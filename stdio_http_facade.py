#!/usr/bin/env python3
"""Explicit local Agent-facing stdio-to-Streamable-HTTP MCP facade.

The Bridge launches stdio MCPs only and the supported agent clients (Codex,
Claude, the DSH stdio overlay) speak stdio.  Some agents can instead consume an
MCP over the MCP 2025-06-18 *Streamable HTTP* transport (``POST /mcp`` with
``Accept: application/json, text/event-stream``, optional ``GET`` server-sent
events, optional ``DELETE`` session teardown).  This module is that explicit
facade::

    HTTP MCP client (this host, loopback)
        <->  Streamable HTTP (this process, one session per MCP session)
        <->  ``<side>-bridge-mcp/bridge.py connect <target>`` subprocess
        <->  local Bridge node (owned registry + peer link)
        <->  peer MCP (stdio) on the other host

The facade is deliberately *not* a native HTTP transport: every HTTP session is
answered by exactly one isolated Bridge ``connect`` stdio subprocess, so each
MCP session gets process isolation, per-session lifecycle, and the exact same
trusted id resolution the stdio connectors already use.  The peer never
supplies a command, argv, environment, credentials, headers, or control
definitions: the facade only ever launches ``bridge.py connect <registered
id>`` against the *local* Bridge control listener (host/port are Operator
arguments), and everything else is resolved by the owned node.  Nothing from a
peer frame crosses into this process.

Transport rules implemented (server side, 2025-06-18):

* one HTTP endpoint ``/mcp``;
* ``initialize`` (no session) spawns one connector subprocess, relays the
  initialize to the backend, and answers with ``Mcp-Session-Id`` plus the
  backend-negotiated ``MCP-Protocol-Version``;
* every later request must carry the session id header (400 when missing,
  404 when unknown, matching the fixture contract);
* a JSON-RPC *request* POST is answered with a single ``application/json``
  response (allowed when the client ``Accept`` includes JSON), or with a
  ``text/event-stream`` response body when the client only accepts SSE;
* a JSON-RPC *notification* or *response* POST is relayed and answered with
  HTTP 202 and no body;
* backend-initiated notifications and server requests are pushed over the
  session ``GET`` SSE stream; a backend server request with no open SSE
  consumer is answered with a structured JSON-RPC error so the backend can
  never hang;
* ``DELETE`` closes the session's backend subprocess and forgets the session
  (HTTP 202);
* bounds: a session cap, a request-body cap, a per-line backend cap, a
  per-session in-flight cap, a request timeout, an idle reaper, bounded SSE
  push queues (drop-newest, counted) and a bounded chunked write per stream.

Explicit ``--protocol-era modern`` selects the bounded 2026-07-28 HTTP binding;
legacy remains the default. Modern mode is POST-only/stateless and rejects
session/resumption headers, legacy initialize and client notification POSTs.
Every request requires version/capability metadata plus matching protocol/method/
name HTTP headers. Discovery, identity, instructions and resultType come only
from the registered stdio backend. No capabilities or handshake are synthesized.

Modern requests each own one ``bridge.py connect <target>`` subprocess; command
and endpoint overrides are unavailable in this mode. tools/call first performs
at most four read-only tools/list preflights on that same proxy, using the
originating metadata. Shared schema helpers validate primitive x-mcp-header
bindings and decoded header/body equality before business forwarding. Unsupported
annotations, unknown Mcp-Param-* metadata, unavailable schemas and unrepresentable
independent backend requests fail closed. Public tools/list excludes definitions
with invalid/unsupported header annotations. MRTR result objects pass through; no
input request is automatically fulfilled. There is no application request replay.

HTTP workers are admitted before header reads. Header/body, stdin/stdout queues,
aggregate backend output (including preflight), HTTP output and elapsed duration
are bounded. A disconnect/deadline sends best-effort stdio cancellation and reaps
only the owned proxy; cleanup never operates on a node or unrelated backend.
Adapter errors report outcomeUnknown after an external request's first pipe-write
attempt, but not for generated schema preflight. This is uncertainty evidence,
not replay advice. This tested profile has a four-page/64-binding schema bound,
requires Content-Length and both JSON/SSE Accept types, closes each HTTP connection
after its response, and does not claim full protocol conformance. HTTP client
half-close is treated as disconnect; session resumption and extension notification
POSTs are not supported. max-inflight is a global modern worker/proxy bound;
max-line-bytes also bounds aggregate modern backend and response bytes.

Loopback only: the HTTP listener host and the Bridge node host must resolve
only to loopback addresses.  Diagnostics are stderr-only and never include
session ids, backend stdout/stderr content, headers, or bodies.  Standard
library only, imports nothing from the Bridge runtime, and is testable and
deployable independently.  This module is not a native HTTP transport and
never claims the peer row is ``native``: use it only for an explicit
``compatibility_route`` of ``stdio-to-http`` on an Agent that consumes MCP over
Streamable HTTP.
"""

from __future__ import annotations

import argparse
import base64
import decimal
import math
import re
import select
import ipaddress
import json
import os
import queue
import secrets
import signal
import socket
import subprocess
import sys
import threading
import time
import urllib.parse
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

FACADE_VERSION = "0.1.0"
PROFILE_NAME = "stdio-http-facade"
DEFAULT_PROTOCOL_VERSION = "2025-06-18"
SESSION_HEADER = "Mcp-Session-Id"
PROTOCOL_HEADER = "MCP-Protocol-Version"
CONTENT_TYPE_JSON = "application/json"
CONTENT_TYPE_EVENT = "text/event-stream"
ENDPOINT = "/mcp"

# Bounds (mirror the runtime's framing limits where relevant).
DEFAULT_MAX_SESSIONS = 32
DEFAULT_REQUEST_BODY_BYTES = 16 * 1024 * 1024
DEFAULT_MAX_LINE_BYTES = 16 * 1024 * 1024
DEFAULT_MAX_INFLIGHT = 8
DEFAULT_REQUEST_TIMEOUT_S = 600.0
DEFAULT_INITIALIZE_TIMEOUT_S = 20.0
DEFAULT_SESSION_IDLE_S = 900.0
DEFAULT_SSE_HEARTBEAT_S = 15.0
MAX_SSE_QUEUE = 256
BACKEND_TERMINATE_GRACE_S = 2.0

ERR_TRANSPORT = -32000
ERR_NOT_FOUND = -32001  # server has no open SSE stream for a server request

_ID_BLOCK = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ-._"


class FacadeError(Exception):
    """Facade failure carrying a stable machine-readable ``category``."""


def _json_bytes(message: Any) -> bytes:
    return json.dumps(message, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def _classify(message: dict[str, Any]) -> str:
    """``request`` / ``notification`` / ``response`` for one JSON-RPC message."""
    if isinstance(message.get("method"), str):
        return "request" if "id" in message else "notification"
    if "id" in message and ("result" in message or "error" in message):
        return "response"
    # Defensive: anything else carrying an id is a request (its id must be
    # preserved in any error answer); id-less objects are notifications.
    return "request" if "id" in message else "notification"


def _canonical_key(request_id: Any) -> str:
    """Canonical pending-key for one JSON-RPC id (numbers/strings/None/bools)."""
    if isinstance(request_id, bool):
        return f"b:{int(request_id)}"
    if isinstance(request_id, (int, float)) and not isinstance(request_id, bool):
        return f"n:{request_id!r}"
    if request_id is None:
        return "z:null"
    return f"s:{request_id}"


def _require_loopback(host: str, what: str) -> None:
    """Refuse any host that resolves to a non-loopback address."""
    try:
        infos = socket.getaddrinfo(host, None, 0, socket.SOCK_STREAM)
    except socket.gaierror:
        raise FacadeError(f"{what} host {host!r} does not resolve")
    addresses = [info[4][0] for info in infos]
    if not addresses:
        raise FacadeError(f"{what} host {host!r} resolves to no addresses")
    for address in addresses:
        try:
            if not ipaddress.ip_address(address).is_loopback:
                raise FacadeError(f"{what} host {host!r} is not loopback-only")
        except ValueError:
            raise FacadeError(f"{what} host {host!r} is not an IP address")


def _launcher_for_side(side: str) -> Path:
    if side not in ("win", "wsl"):
        raise FacadeError(f"side must be 'win' or 'wsl', got {side!r}")
    root = Path(__file__).resolve().parent
    launcher = root / f"{side}-bridge-mcp" / "bridge.py"
    if not launcher.is_file():
        raise FacadeError(f"component launcher not found: {launcher}")
    return launcher


def default_backend_command(
    side: str, node_host: str, node_port: int, target: str
) -> list[str]:
    """The only backend the facade is allowed to launch by default.

    ``bridge.py connect <target>`` against the *local* Bridge control socket:
    the peer never contributes argv, command, environment, or credentials.
    """
    _require_loopback(node_host, "bridge node")
    launcher = _launcher_for_side(side)
    return [
        sys.executable,
        str(launcher),
        "connect",
        "--local-host",
        node_host,
        "--local-port",
        str(int(node_port)),
        target,
    ]


@dataclass
class FacadeOptions:
    side: str
    target: str
    node_host: str = "127.0.0.1"
    node_port: int = 8769
    listen_host: str = "127.0.0.1"
    listen_port: int = 0
    backend_command: list[str] | None = None  # Operator/test override only
    max_sessions: int = DEFAULT_MAX_SESSIONS
    request_body_bytes: int = DEFAULT_REQUEST_BODY_BYTES
    max_line_bytes: int = DEFAULT_MAX_LINE_BYTES
    max_inflight: int = DEFAULT_MAX_INFLIGHT
    request_timeout_s: float = DEFAULT_REQUEST_TIMEOUT_S
    initialize_timeout_s: float = DEFAULT_INITIALIZE_TIMEOUT_S
    session_idle_s: float = DEFAULT_SESSION_IDLE_S
    sse_heartbeat_s: float = DEFAULT_SSE_HEARTBEAT_S
    protocol_era: str = "legacy"

    def __post_init__(self) -> None:
        if self.protocol_era not in ("legacy", "modern"):
            raise FacadeError("protocol-era must be legacy or modern")
        if self.protocol_era == "modern":
            if self.backend_command is not None:
                raise FacadeError("modern mode requires a registered target; backend-command is unavailable")
            if not isinstance(self.target, str) or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", self.target) is None:
                raise FacadeError("modern mode requires a plain registered target id")
            if not math.isfinite(self.request_timeout_s) or not 0 < self.request_timeout_s <= 3600:
                raise FacadeError("modern request timeout must be within (0, 3600] seconds")
            if not math.isfinite(self.sse_heartbeat_s) or self.sse_heartbeat_s < 0:
                raise FacadeError("modern heartbeat must be finite and nonnegative")
        if self.side not in ("win", "wsl"):
            raise FacadeError("side must be 'win' or 'wsl'")
        if not isinstance(self.target, str) or not self.target:
            raise FacadeError("target id is required")
        if not 1 <= self.max_sessions <= 256:
            raise FacadeError("max-sessions must be within 1..256")
        if not 1024 <= self.request_body_bytes <= 64 * 1024 * 1024:
            raise FacadeError("request body cap must be within 1KiB..64MiB")
        if not 1024 <= self.max_line_bytes <= 64 * 1024 * 1024:
            raise FacadeError("backend line cap must be within 1KiB..64MiB")
        if not 1 <= self.max_inflight <= 64:
            raise FacadeError("max-inflight must be within 1..64")
        if self.session_idle_s < 30:
            raise FacadeError("session-idle must be at least 30 seconds")
        _require_loopback(self.listen_host, "listen")
        if self.backend_command is None:
            default_backend_command(self.side, self.node_host, self.node_port, self.target)
        else:
            if not self.backend_command or not all(
                isinstance(item, str) for item in self.backend_command
            ):
                raise FacadeError("backend command must be a non-empty argv list")


# --------------------------------------------------------------------------
# One isolated stdio backend subprocess per MCP session
# --------------------------------------------------------------------------

class BackendGone(Exception):
    """The backend subprocess exited or was closed."""


class BackendProcess:
    """Spawns one ``bridge connect`` (or test) stdio backend and pumps lines."""

    def __init__(
        self,
        argv: list[str],
        inbound: queue.Queue,
        max_line_bytes: int,
        label: str,
        cwd: str | None = None,
    ):
        self.argv = argv
        self.inbound = inbound
        self.max_line_bytes = max_line_bytes
        self.label = label
        self.cwd = cwd
        self.process: subprocess.Popen[bytes] | None = None
        self._write_lock = threading.Lock()
        self._closed = False
        self._reader: threading.Thread | None = None
        self._malformed = 0

    def start(self) -> None:
        env = dict(os.environ)
        env["PYTHONDONTWRITEBYTECODE"] = "1"
        try:
            self.process = subprocess.Popen(
                self.argv,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                env=env,
                cwd=self.cwd,
                start_new_session=os.name == "posix",
            )
        except OSError as exc:
            raise FacadeError(f"failed to start backend for {self.label}: {exc}")
        self._reader = threading.Thread(
            target=self._read_loop, name=f"backend-read-{self.label}", daemon=True
        )
        self._reader.start()

    # -- read side ---------------------------------------------------------

    def _read_loop(self) -> None:
        assert self.process is not None and self.process.stdout is not None
        stream = self.process.stdout
        buffered = b""
        try:
            while True:
                chunk = stream.read1(65536)
                if not chunk:
                    break
                buffered += chunk
                while b"\n" in buffered:
                    raw, _, buffered = buffered.partition(b"\n")
                    if len(raw) > self.max_line_bytes:
                        self._malformed += 1
                        if self._malformed > 20:
                            self.inbound.put(("exit", None))
                            return
                        continue
                    line = raw.strip()
                    if not line:
                        continue
                    try:
                        message = json.loads(line.decode("utf-8"))
                    except (ValueError, UnicodeDecodeError):
                        self._malformed += 1
                        if self._malformed > 20:
                            self.inbound.put(("exit", None))
                            return
                        continue
                    if not isinstance(message, dict):
                        self._malformed += 1
                        continue
                    self.inbound.put(("message", message))
            if len(buffered) > self.max_line_bytes:
                self._malformed += 1
            elif buffered.strip():
                try:
                    message = json.loads(buffered.decode("utf-8"))
                    if isinstance(message, dict):
                        self.inbound.put(("message", message))
                except (ValueError, UnicodeDecodeError):
                    pass
        except (OSError, ValueError):
            pass
        finally:
            self.inbound.put(("exit", self._exit_code()))

    def _exit_code(self) -> int | None:
        if self.process is None:
            return None
        try:
            return self.process.poll()
        except OSError:
            return None

    # -- write side --------------------------------------------------------

    def write_line(self, message: dict[str, Any]) -> None:
        raw = _json_bytes(message) + b"\n"
        if len(raw) > self.max_line_bytes:
            raise FacadeError("outbound message exceeds the backend line cap")
        with self._write_lock:
            if self._closed or self.process is None or self.process.poll() is not None:
                raise BackendGone(f"backend {self.label} is not running")
            assert self.process.stdin is not None
            try:
                self.process.stdin.write(raw)
                self.process.stdin.flush()
            except (OSError, BrokenPipeError, ValueError) as exc:
                raise BackendGone(f"backend {self.label} write failed: {exc}")

    # -- teardown ----------------------------------------------------------

    def close(self, grace_s: float = BACKEND_TERMINATE_GRACE_S) -> None:
        with self._write_lock:
            if self._closed:
                return
            self._closed = True
        process = self.process
        self.process = None
        if process is None:
            return
        try:
            if process.poll() is None:
                if os.name == "posix":
                    try:
                        os.killpg(os.getpgid(process.pid), signal.SIGTERM)
                    except (ProcessLookupError, OSError):
                        process.terminate()
                else:
                    process.terminate()
                try:
                    process.wait(timeout=grace_s)
                except subprocess.TimeoutExpired:
                    if os.name == "posix":
                        try:
                            os.killpg(os.getpgid(process.pid), signal.SIGKILL)
                        except (ProcessLookupError, OSError):
                            process.kill()
                    else:
                        process.kill()
                    process.wait(timeout=grace_s)
            else:
                process.wait(timeout=grace_s)
        except (OSError, subprocess.SubprocessError):
            pass
        # Reap the pipe ends so no file descriptors or ResourceWarnings linger.
        for pipe in (process.stdin, process.stdout):
            if pipe is not None:
                try:
                    pipe.close()
                except OSError:
                    pass
        if self._reader is not None:
            self._reader.join(timeout=grace_s)


# --------------------------------------------------------------------------
# Session state: backend + dispatch + SSE push channels
# --------------------------------------------------------------------------

_CLOSE = object()      # GET SSE stream end
_DONE = object()       # SSE POST response stream end


class _Channel:
    """One SSE consumer queue with bounded drop-newest push."""

    def __init__(self) -> None:
        self.queue: queue.Queue = queue.Queue(maxsize=MAX_SSE_QUEUE)
        self.dropped = 0
        self._lock = threading.Lock()

    def send(self, message: dict[str, Any]) -> int:
        try:
            self.queue.put_nowait(message)
            return 0
        except queue.Full:
            with self._lock:
                self.dropped += 1
            return 1


@dataclass
class _Pending:
    canonical: str
    request_id: Any
    event: threading.Event = field(default_factory=threading.Event)
    result: dict[str, Any] | None = None
    sse_channel: _Channel | None = None


class Session:
    """One HTTP MCP session backed by exactly one isolated stdio backend."""

    def __init__(self, sid: str, argv: list[str], options: FacadeOptions, label: str):
        self.sid = sid
        self.options = options
        self.label = label
        self.inbound: queue.Queue = queue.Queue()
        self.backend = BackendProcess(argv, self.inbound, options.max_line_bytes, label)
        self.lock = threading.Lock()
        self.initialized = False
        self.protocol_version: str | None = None
        self.deleted = False
        self.dead: str | None = None  # set when the backend exits
        self.pending: dict[str, _Pending] = {}
        self.channels: list[_Channel] = []
        self.last_activity = time.monotonic()
        self.dispatcher: threading.Thread | None = None

    def touch(self) -> None:
        self.last_activity = time.monotonic()

    def start(self) -> None:
        self.backend.start()
        self.dispatcher = threading.Thread(
            target=self._dispatch_loop, name=f"dispatch-{self.label}", daemon=True
        )
        self.dispatcher.start()

    def add_channel(self, channel: _Channel) -> None:
        with self.lock:
            if self.deleted:
                raise FacadeError("session deleted")
            self.channels.append(channel)
            self.touch()

    def remove_channel(self, channel: _Channel) -> None:
        with self.lock:
            try:
                self.channels.remove(channel)
            except ValueError:
                pass

    def _broadcast(self, message: dict[str, Any]) -> None:
        with self.lock:
            channels = list(self.channels)
        for channel in channels:
            channel.send(message)

    # -- dispatcher --------------------------------------------------------

    def _dispatch_loop(self) -> None:
        while True:
            item = self.inbound.get()
            if item is None:
                return
            kind = item[0]
            if kind == "exit":
                self._on_backend_exit(item[1] if len(item) > 1 else None)
                return
            message = item[1]  # type: dict[str, Any]
            self._dispatch_message(message)

    def _on_backend_exit(self, code: int | None) -> None:
        with self.lock:
            if self.deleted:
                return
            self.dead = f"backend exited (code {code})" if code is not None else "backend exited"
            pendings = list(self.pending.values())
            self.pending.clear()
            channels = list(self.channels)
            self.channels.clear()
        error = {
            "jsonrpc": "2.0",
            "error": {
                "code": ERR_TRANSPORT,
                "message": f"bridge backend exited (code {code})",
                "data": {"session": "lost"},
            },
        }
        for pending in pendings:
            if pending.sse_channel is not None:
                pending.sse_channel.send({**error, "id": pending.request_id})
                pending.sse_channel.queue.put_nowait(_DONE)
            else:
                pending.result = {**error, "id": pending.request_id}
            pending.event.set()
        for channel in channels:
            channel.queue.put_nowait(_CLOSE)
        self.touch()

    def _dispatch_message(self, message: dict[str, Any]) -> None:
        self.touch()
        kind = _classify(message)
        if kind == "response":
            self._handle_response(message)
            return
        if kind == "request":
            # A backend server request can only be answered by the HTTP client
            # after it is delivered over an open SSE stream.  With no consumer
            # open, refuse it with a structured JSON-RPC error so the backend
            # can never hang on a client that opened no SSE stream.
            with self.lock:
                has_consumer = bool(self.channels)
            if not has_consumer:
                try:
                    self.backend.write_line(
                        {
                            "jsonrpc": "2.0",
                            "id": message.get("id"),
                            "error": {
                                "code": ERR_NOT_FOUND,
                                "message": "no open SSE stream for backend server requests",
                            },
                        }
                    )
                except BackendGone:
                    pass
                return
        # Notifications and deliverable server requests go to SSE consumers.
        self._broadcast(message)

    def _handle_response(self, message: dict[str, Any]) -> None:
        key = _canonical_key(message.get("id"))
        with self.lock:
            pending = self.pending.pop(key, None)
        if pending is None:
            # Late/unsolicited backend response: nothing to answer, drop it.
            return
        if pending.sse_channel is not None:
            pending.sse_channel.send(message)
            try:
                pending.sse_channel.queue.put_nowait(_DONE)
            except queue.Full:
                pass
        else:
            pending.result = message
        pending.event.set()

    # -- request plumbing --------------------------------------------------

    def register_pending(self, request: dict[str, Any]) -> _Pending:
        key = _canonical_key(request.get("id"))
        with self.lock:
            if self.dead is not None:
                raise BackendGone(self.dead)
            if len(self.pending) >= self.options.max_inflight:
                raise FacadeError("per-session in-flight request cap reached")
            pending = self.pending.get(key)
            if pending is not None and not pending.event.is_set():
                raise FacadeError(f"duplicate in-flight request id {request.get('id')!r}")
            pending = _Pending(key, request.get("id"))
            self.pending[key] = pending
            self.touch()
        return pending

    def forward(self, message: dict[str, Any]) -> None:
        with self.lock:
            if self.dead is not None:
                raise BackendGone(self.dead)
        self.backend.write_line(message)
        self.touch()

    def drop_pending(self, pending: _Pending) -> None:
        with self.lock:
            if self.pending.get(pending.canonical) is pending:
                del self.pending[pending.canonical]

    def close(self) -> None:
        with self.lock:
            if self.deleted:
                return
            self.deleted = True
            channels = list(self.channels)
            self.channels.clear()
            pendings = list(self.pending.values())
            self.pending.clear()
        for channel in channels:
            try:
                channel.queue.put_nowait(_CLOSE)
            except queue.Full:
                pass
        for pending in pendings:
            if pending.sse_channel is None:
                pending.result = {
                    "jsonrpc": "2.0",
                    "id": pending.request_id,
                    "error": {
                        "code": ERR_TRANSPORT,
                        "message": "session deleted while request in flight",
                    },
                }
            pending.event.set()
        self.backend.close()
        if self.dispatcher is not None and self.dispatcher is not threading.current_thread():
            self.dispatcher.join(timeout=0.5)


# --------------------------------------------------------------------------
# HTTP server
# --------------------------------------------------------------------------

class FacadeServer(ThreadingHTTPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, options: FacadeOptions, endpoint: str = ENDPOINT):
        if options.protocol_era != "legacy":
            raise FacadeError("Use ModernFacadeServer for protocol-era modern")
        super().__init__((options.listen_host, options.listen_port), _FacadeHandler)
        self.options = options
        self.endpoint = endpoint
        self.lock = threading.Lock()
        self.sessions: dict[str, Session] = {}
        self._closed = False
        self._reaper_stop = threading.Event()

    @property
    def url(self) -> str:
        host, port = self.server_address[:2]
        return f"http://{host}:{port}{self.endpoint}"

    # -- session management -------------------------------------------------

    def create_session(self) -> Session:
        with self.lock:
            if self._closed:
                raise FacadeError("server closed")
            if len(self.sessions) >= self.options.max_sessions:
                raise FacadeError("session capacity reached")
            sid = secrets.token_hex(12)
            argv = self.options.backend_command or default_backend_command(
                self.options.side,
                self.options.node_host,
                self.options.node_port,
                self.options.target,
            )
            session = Session(sid, argv, self.options, sid[:8])
            self.sessions[sid] = session
        session.start()
        return session

    def get_session(self, sid: str | None) -> Session | None:
        if not sid:
            return None
        with self.lock:
            return self.sessions.get(sid)

    def drop_session(self, session: Session) -> None:
        with self.lock:
            self.sessions.pop(session.sid, None)
        session.close()

    def shutdown_all(self) -> None:
        self._closed = True
        with self.lock:
            sessions = list(self.sessions.values())
            self.sessions.clear()
        for session in sessions:
            session.close()
        self._reaper_stop.set()

    def serve(self) -> None:
        reaper = threading.Thread(target=self._reaper_loop, name="facade-reaper", daemon=True)
        reaper.start()
        try:
            super().serve_forever(poll_interval=0.5)
        finally:
            self.shutdown_all()

    def stop(self) -> None:
        self.shutdown_all()
        threading.Thread(target=self.shutdown, daemon=True).start()
        self.server_close()

    def _reaper_loop(self) -> None:
        idle_limit = self.options.session_idle_s
        while not self._reaper_stop.wait(15.0):
            now = time.monotonic()
            with self.lock:
                # Idle sessions are reaped whether or not the backend already
                # exited (a dead session must not occupy the cap forever).
                candidates = [
                    session
                    for session in self.sessions.values()
                    if now - session.last_activity > idle_limit
                ]
            for session in candidates:
                self.drop_session(session)
                print(
                    "stdio-http-facade: reaped idle session",
                    session.sid[:8],
                    file=sys.stderr,
                )


class _FacadeHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server: FacadeServer

    def log_message(self, _format: str, *args: Any) -> None:  # quiet facade
        pass

    # -- helpers -----------------------------------------------------------

    def _send_json(self, status: int, payload: dict[str, Any], extra: dict[str, str] | None = None) -> None:
        body = _json_bytes(payload)
        self.send_response(status)
        self.send_header("Content-Type", CONTENT_TYPE_JSON)
        self.send_header("Content-Length", str(len(body)))
        for name, value in (extra or {}).items():
            self.send_header(name, value)
        self.end_headers()
        try:
            self.wfile.write(body)
        except OSError:
            pass

    def _send_empty(self, status: int, extra: dict[str, str] | None = None) -> None:
        self.send_response(status)
        for name, value in (extra or {}).items():
            self.send_header(name, value)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def _read_message(self) -> dict[str, Any]:
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except (TypeError, ValueError):
            length = 0
        cap = self.server.options.request_body_bytes
        if length > cap:
            self._send_json(
                413,
                {
                    "jsonrpc": "2.0",
                    "id": None,
                    "error": {
                        "code": ERR_TRANSPORT,
                        "message": f"request body exceeds the {cap} byte cap",
                    },
                },
            )
            raise _BodyTooLarge()
        raw = self.rfile.read(length)
        if len(raw) != length:
            raise ValueError("truncated body")
        message = json.loads(raw.decode("utf-8"))
        if not isinstance(message, dict):
            raise ValueError("not an object")
        return message

    def _accepts_json(self) -> bool:
        accept = self.headers.get("Accept", "")
        return "application/json" in accept or not accept.strip()

    def _session_from_header(self) -> Session | None:
        sid = self.headers.get(SESSION_HEADER)
        return self.server.get_session(sid)

    def _channel_stream(
        self,
        channel: _Channel,
        deadline: float | None = None,
        request_id: Any = None,
    ) -> None:
        """Chunked SSE: consume ``channel`` until _CLOSE/_DONE (or deadline)."""
        self.send_response(200)
        self.send_header("Content-Type", CONTENT_TYPE_EVENT)
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Transfer-Encoding", "chunked")
        self.end_headers()
        heartbeat = self.server.options.sse_heartbeat_s
        timeout = heartbeat if heartbeat and heartbeat > 0 else 0.5
        try:
            while True:
                try:
                    item = channel.queue.get(timeout=timeout)
                except queue.Empty:
                    if deadline is not None and time.monotonic() >= deadline:
                        self._write_chunk(
                            b"event: message\ndata: "
                            + _json_bytes(
                                {
                                    "jsonrpc": "2.0",
                                    "id": request_id,
                                    "error": {
                                        "code": ERR_TRANSPORT,
                                        "message": "backend response timed out",
                                    },
                                }
                            )
                            + b"\n\n"
                        )
                        break
                    if heartbeat and heartbeat > 0:
                        self._write_chunk(b": ping\n\n")
                    continue
                if item is _CLOSE or item is _DONE:
                    break
                if isinstance(item, dict):
                    self._write_chunk(b"event: message\ndata: " + _json_bytes(item) + b"\n\n")
        except (OSError, ConnectionError):
            pass
        finally:
            # The chunked terminator is a raw ``0\\r\\n\\r\\n``, never itself a chunk.
            try:
                self.wfile.write(b"0\r\n\r\n")
                self.wfile.flush()
            except (OSError, ConnectionError, ValueError):
                pass

    def _write_chunk(self, data: bytes) -> None:
        self.wfile.write(f"{len(data):x}\r\n".encode("ascii") + data + b"\r\n")
        self.wfile.flush()

    # -- verbs -------------------------------------------------------------

    def do_POST(self) -> None:
        if self.path.split("?", 1)[0] != self.server.endpoint:
            self._send_empty(404)
            return
        try:
            message = self._read_message()
        except _BodyTooLarge:
            return
        except (ValueError, UnicodeDecodeError):
            self._send_json(
                400,
                {
                    "jsonrpc": "2.0",
                    "id": None,
                    "error": {"code": -32700, "message": "parse error"},
                },
            )
            return
        if message.get("method") == "initialize":
            self._handle_initialize(message)
            return
        session = self._session_from_header()
        sid = self.headers.get(SESSION_HEADER)
        if session is None:
            if sid is None:
                self._send_json(
                    400,
                    {
                        "jsonrpc": "2.0",
                        "id": message.get("id"),
                        "error": {"code": -32600, "message": "missing session id header"},
                    },
                )
            else:
                self._send_empty(404)
            return
        if session.deleted:
            self._send_empty(404)
            return
        kind = _classify(message)
        try:
            if kind == "notification":
                session.forward(message)
                self._send_empty(202)
                return
            if kind == "response":
                session.forward(message)
                self._send_empty(202)
                return
            # JSON-RPC request: forward and answer.
            self._handle_request(session, message)
        except BackendGone as exc:
            self._send_json(
                200,
                {
                    "jsonrpc": "2.0",
                    "id": message.get("id") if kind == "request" else None,
                    "error": {
                        "code": ERR_TRANSPORT,
                        "message": str(exc),
                        "data": {"session": "lost"},
                    },
                },
            )
        except FacadeError as exc:
            self._send_json(
                429,
                {
                    "jsonrpc": "2.0",
                    "id": message.get("id") if kind == "request" else None,
                    "error": {"code": ERR_TRANSPORT, "message": str(exc)},
                },
            )

    def _handle_initialize(self, message: dict[str, Any]) -> None:
        session = self._session_from_header()
        if session is not None:
            self._send_json(
                200,
                {
                    "jsonrpc": "2.0",
                    "id": message.get("id"),
                    "error": {
                        "code": -32600,
                        "message": "session already initialized",
                    },
                },
            )
            return
        try:
            session = self.server.create_session()
        except FacadeError as exc:
            self._send_json(
                429,
                {
                    "jsonrpc": "2.0",
                    "id": message.get("id"),
                    "error": {"code": ERR_TRANSPORT, "message": str(exc)},
                },
            )
            return
        try:
            pending = session.register_pending(message)
            session.forward(message)
            if not pending.event.wait(self.server.options.initialize_timeout_s):
                session.drop_pending(pending)
                self.server.drop_session(session)
                self._send_json(
                    200,
                    {
                        "jsonrpc": "2.0",
                        "id": message.get("id"),
                        "error": {
                            "code": ERR_TRANSPORT,
                            "message": "backend initialize timed out",
                        },
                    },
                )
                return
            response = pending.result
        except (BackendGone, FacadeError) as exc:
            self.server.drop_session(session)
            self._send_json(
                200,
                {
                    "jsonrpc": "2.0",
                    "id": message.get("id"),
                    "error": {
                        "code": ERR_TRANSPORT,
                        "message": str(exc),
                        "data": {"session": "lost"},
                    },
                },
            )
            return
        if response is None:
            self.server.drop_session(session)
            self._send_json(
                200,
                {
                    "jsonrpc": "2.0",
                    "id": message.get("id"),
                    "error": {"code": ERR_TRANSPORT, "message": "no initialize response"},
                },
            )
            return
        result = response.get("result")
        if isinstance(result, dict) and isinstance(result.get("protocolVersion"), str):
            session.protocol_version = result["protocolVersion"]
        session.initialized = True
        extra = {
            SESSION_HEADER: session.sid,
        }
        if session.protocol_version:
            extra[PROTOCOL_HEADER] = session.protocol_version
        self._send_json(200, response, extra=extra)

    def _handle_request(self, session: Session, message: dict[str, Any]) -> None:
        try:
            pending = session.register_pending(message)
        except FacadeError as exc:
            self._send_json(
                429,
                {
                    "jsonrpc": "2.0",
                    "id": message.get("id"),
                    "error": {"code": ERR_TRANSPORT, "message": str(exc)},
                },
            )
            return
        sse_mode = not self._accepts_json()
        if sse_mode:
            channel = _Channel()
            pending.sse_channel = channel
            session.add_channel(channel)
        try:
            session.forward(message)
        except BackendGone as exc:
            session.drop_pending(pending)
            error = {
                "jsonrpc": "2.0",
                "id": message.get("id"),
                "error": {
                    "code": ERR_TRANSPORT,
                    "message": str(exc),
                    "data": {"session": "lost"},
                },
            }
            if sse_mode:
                channel.send(error)
                try:
                    channel.queue.put_nowait(_DONE)
                except queue.Full:
                    pass
                try:
                    self._channel_stream(channel)
                finally:
                    session.remove_channel(channel)
                return
            self._send_json(200, error)
            return
        if sse_mode:
            try:
                self._channel_stream(
                    channel,
                    deadline=time.monotonic() + self.server.options.request_timeout_s,
                    request_id=message.get("id"),
                )
            finally:
                session.remove_channel(channel)
            return
        if not pending.event.wait(self.server.options.request_timeout_s):
            session.drop_pending(pending)
            self._send_json(
                200,
                {
                    "jsonrpc": "2.0",
                    "id": message.get("id"),
                    "error": {
                        "code": ERR_TRANSPORT,
                        "message": "backend response timed out",
                    },
                },
            )
            return
        response = pending.result
        if response is None:
            response = {
                "jsonrpc": "2.0",
                "id": message.get("id"),
                "error": {"code": ERR_TRANSPORT, "message": "no backend response"},
            }
        self._send_json(200, response)

    def do_GET(self) -> None:
        if self.path.split("?", 1)[0] != self.server.endpoint:
            self._send_empty(404)
            return
        session = self._session_from_header()
        sid = self.headers.get(SESSION_HEADER)
        if session is None or session.deleted:
            self._send_empty(404 if sid else 400)
            return
        channel = _Channel()
        try:
            session.add_channel(channel)
        except FacadeError:
            self._send_empty(404)
            return
        try:
            self._channel_stream(channel)
        finally:
            session.remove_channel(channel)

    def do_DELETE(self) -> None:
        if self.path.split("?", 1)[0] != self.server.endpoint:
            self._send_empty(404)
            return
        session = self._session_from_header()
        if session is None:
            self._send_empty(404 if self.headers.get(SESSION_HEADER) else 400)
            return
        self.server.drop_session(session)
        self._send_empty(202)


class _BodyTooLarge(Exception):
    pass


# --------------------------------------------------------------------------
# Explicit modern HTTP binding. The legacy session implementation is separate.
# --------------------------------------------------------------------------

from streamable_http_stdio import (
    MODERN_PROTOCOL_VERSION, MODERN_VERSION_META, MODERN_CAPABILITIES_META,
    _modern_json, _modern_request_id, _tool_header_schema, _tool_argument_headers,
    _ModernRpcError as _SchemaHeaderError,
)


def _modern_encode(message: Any) -> bytes:
    return json.dumps(message, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode("utf-8")


class _ModernFacadeError(Exception):
    def __init__(self, status: int, code: int, message: str, data: dict[str, Any] | None = None):
        self.status, self.code, self.message, self.data = status, code, message, data or {}

    def payload(self, request_id: Any, outcome_unknown: bool = False) -> dict[str, Any]:
        return {"jsonrpc": "2.0", "id": request_id, "error": {
            "code": self.code, "message": self.message,
            "data": {**self.data, "outcomeUnknown": outcome_unknown},
        }}


class _HttpPeerGone(Exception):
    pass


def _decode_modern_header(value: str) -> str:
    if (not value or value != value.strip()
            or any(ord(c) < 32 or ord(c) > 126 for c in value)):
        # Empty string is a valid primitive string header value.
        if value == "":
            return value
        raise _ModernFacadeError(400, -32020, "Header mismatch")
    if value.startswith("=?base64?") and value.endswith("?="):
        try:
            return base64.b64decode(value[9:-2], validate=True).decode("utf-8")
        except (ValueError, UnicodeError):
            raise _ModernFacadeError(400, -32020, "Header mismatch") from None
    return value


class _ModernProxy:
    """One owned connect process, two bounded pumps, no persistent session.

    Writes run separately so a backend that stops reading cannot prevent the
    HTTP handler from noticing disconnect/deadline and terminating its proxy.
    Nothing from HTTP headers or params contributes argv/environment.
    """
    def __init__(self, argv: list[str], options: FacadeOptions, deadline: float):
        self.options, self.argv, self.deadline = options, argv, deadline
        self.process: subprocess.Popen[bytes] | None = None
        self.inbound: queue.Queue = queue.Queue(maxsize=32)
        self.outbound: queue.Queue = queue.Queue(maxsize=1)
        self.stopped = threading.Event()
        self.closing = threading.Event()
        self.failure: str | None = None
        self.application_started = False
        self.current_id: Any = None
        self.lock = threading.Lock()
        self.close_lock = threading.Lock()
        self.threads: list[threading.Thread] = []

    def start(self) -> None:
        try:
            self.process = subprocess.Popen(
                self.argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL, bufsize=0,
                env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
                start_new_session=os.name == "posix",
            )
        except OSError:
            raise _ModernFacadeError(502, ERR_TRANSPORT, "Unable to start registered proxy") from None
        for target, name in ((self._read, "modern-proxy-read"), (self._write, "modern-proxy-write")):
            thread = threading.Thread(target=target, name=name, daemon=True)
            self.threads.append(thread)
            thread.start()

    def _fail(self, category: str) -> None:
        with self.lock:
            if self.failure is None:
                self.failure = category

    def _read(self) -> None:
        assert self.process is not None and self.process.stdout is not None
        stream = self.process.stdout
        buffered = bytearray()
        total = 0
        try:
            while not self.stopped.is_set():
                chunk = stream.read(4096)
                if not chunk:
                    if buffered.strip():
                        self._fail("truncated-backend-message")
                    self.inbound.put_nowait(("exit", None))
                    return
                total += len(chunk)
                if total > self.options.max_line_bytes:
                    self._fail("backend-response-bound")
                    return
                buffered.extend(chunk)
                while b"\n" in buffered:
                    end = buffered.index(b"\n")
                    raw = bytes(buffered[:end])
                    del buffered[:end + 1]
                    if not raw.strip():
                        continue
                    message = _modern_json(raw)
                    if not isinstance(message, dict):
                        self._fail("invalid-backend-message")
                        return
                    # Ensure downstream data can be encoded before queueing it.
                    _modern_encode(message)
                    self.inbound.put_nowait(("message", message))
        except queue.Full:
            self._fail("backend-response-queue-bound")
        except (OSError, ValueError, UnicodeError, RecursionError):
            if not self.stopped.is_set():
                self._fail("invalid-backend-message")

    def _write(self) -> None:
        assert self.process is not None and self.process.stdin is not None
        stream = self.process.stdin
        try:
            while not self.stopped.is_set():
                try:
                    job = self.outbound.get(timeout=0.05)
                except queue.Empty:
                    continue
                if job is None:
                    return
                raw, application, cancellation, done = job
                with self.lock:
                    if self.stopped.is_set():
                        return
                    if not cancellation and (self.closing.is_set() or time.monotonic() >= self.deadline):
                        if not self.closing.is_set() and self.failure is None:
                            self.failure = "backend-request-deadline"
                        done.set()
                        continue
                    if application:
                        self.application_started = True
                offset = 0
                while offset < len(raw) and not self.stopped.is_set():
                    written = os.write(stream.fileno(), raw[offset:])
                    if not written:
                        raise OSError("closed pipe")
                    offset += written
                done.set()
        except (OSError, ValueError):
            if not self.stopped.is_set():
                self._fail("backend-write-failed")

    def send(self, message: dict[str, Any], *, application: bool = False) -> threading.Event:
        raw = _modern_encode(message) + b"\n"
        if len(raw) > self.options.max_line_bytes:
            raise _ModernFacadeError(413, -32600, "Backend request line exceeds bound")
        done = threading.Event()
        try:
            self.outbound.put_nowait((raw, application, message.get("method") == "notifications/cancelled", done))
        except queue.Full:
            raise _ModernFacadeError(502, ERR_TRANSPORT, "Backend write queue is full") from None
        return done

    def close(self, cancel: bool = False) -> bool:
        with self.close_lock:
            with self.lock:
                self.closing.set()
            process = self.process
            if process is None:
                return True
            if cancel and not self.stopped.is_set() and process.poll() is None and _modern_request_id(self.current_id):
                try:
                    done = self.send({"jsonrpc": "2.0", "method": "notifications/cancelled",
                                      "params": {"requestId": self.current_id}})
                    done.wait(0.05)
                    if done.is_set():
                        # Pipe write completion is not peer delivery. Allow a
                        # bounded response/drain turn before terminating connect.
                        until = time.monotonic() + 0.1
                        while time.monotonic() < until:
                            try:
                                kind, item = self.inbound.get(timeout=max(0.001, until - time.monotonic()))
                            except queue.Empty:
                                break
                            if kind == "exit" or (isinstance(item, dict) and item.get("id") == self.current_id
                                                   and ("result" in item or "error" in item)):
                                break
                except _ModernFacadeError:
                    pass
            self.stopped.set()
            try:
                if process.poll() is None:
                    if os.name == "posix":
                        os.killpg(process.pid, signal.SIGTERM)
                    else:
                        process.terminate()
                process.wait(timeout=0.3)
            except subprocess.TimeoutExpired:
                try:
                    if os.name == "posix":
                        os.killpg(process.pid, signal.SIGKILL)
                    else:
                        process.kill()
                    process.wait(timeout=0.3)
                except (OSError, subprocess.SubprocessError):
                    pass
            except (OSError, subprocess.SubprocessError):
                pass
            if process.poll() is None:
                return False  # Remains registered against the proxy cap.
            for pipe in (process.stdin, process.stdout):
                if pipe is not None:
                    try:
                        pipe.close()
                    except OSError:
                        pass
            for thread in self.threads:
                thread.join(timeout=0.3)
            return not any(thread.is_alive() for thread in self.threads)


class ModernFacadeServer(ThreadingHTTPServer):
    """Bound HTTP workers before reading headers; each POST owns its proxy."""
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, options: FacadeOptions, endpoint: str = ENDPOINT):
        if options.protocol_era != "modern":
            raise FacadeError("ModernFacadeServer requires protocol-era modern")
        self.options, self.endpoint = options, endpoint
        self.lock = threading.Lock()
        self._slots = threading.BoundedSemaphore(options.max_inflight)
        self._closing = threading.Event()
        self.proxies: set[_ModernProxy] = set()
        self.connections: set[socket.socket] = set()
        self.argv = default_backend_command(options.side, options.node_host, options.node_port, options.target)
        if ":" in options.listen_host:
            self.address_family = socket.AF_INET6
        super().__init__((options.listen_host, options.listen_port), _ModernFacadeHandler)

    @property
    def url(self) -> str:
        host, port = self.server_address[:2]
        host = f"[{host}]" if ":" in host else host
        return f"http://{host}:{port}{self.endpoint}"

    def process_request(self, request: socket.socket, client_address: Any) -> None:
        if self._closing.is_set() or not self._slots.acquire(blocking=False):
            body = _json_bytes(_ModernFacadeError(503, ERR_TRANSPORT, "Modern facade capacity exhausted").payload(None))
            try:
                request.settimeout(0.1)
                request.sendall(b"HTTP/1.1 503 Service Unavailable\r\nContent-Type: application/json\r\nConnection: close\r\nContent-Length: "
                                + str(len(body)).encode() + b"\r\n\r\n" + body)
            except OSError:
                pass
            self.shutdown_request(request)
            return
        with self.lock:
            self.connections.add(request)
        try:
            super().process_request(request, client_address)
        except BaseException:
            with self.lock:
                self.connections.discard(request)
            self._slots.release()
            raise

    def process_request_thread(self, request: socket.socket, client_address: Any) -> None:
        try:
            super().process_request_thread(request, client_address)
        finally:
            with self.lock:
                self.connections.discard(request)
            self._slots.release()

    def handle_error(self, request: Any, client_address: Any) -> None:
        print("stdio-http-facade: modern HTTP handler failed", file=sys.stderr)

    def create_proxy(self, deadline: float) -> _ModernProxy:
        with self.lock:
            if self._closing.is_set() or len(self.proxies) >= self.options.max_inflight:
                raise _ModernFacadeError(503, ERR_TRANSPORT, "Modern proxy capacity exhausted")
            proxy = _ModernProxy(self.argv, self.options, deadline)
            self.proxies.add(proxy)
        try:
            proxy.start()
            return proxy
        except BaseException:
            self.drop_proxy(proxy, cancel=True)
            raise

    def drop_proxy(self, proxy: _ModernProxy, *, cancel: bool = False) -> None:
        if proxy.close(cancel):
            with self.lock:
                self.proxies.discard(proxy)

    def shutdown_all(self) -> None:
        self._closing.set()
        with self.lock:
            connections, proxies = list(self.connections), list(self.proxies)
        for connection in connections:
            try:
                connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
        for proxy in proxies:
            self.drop_proxy(proxy, cancel=True)

    def serve(self) -> None:
        try:
            self.serve_forever(poll_interval=0.1)
        finally:
            self.shutdown_all()

    def stop(self) -> None:
        self.shutdown_all()
        self.shutdown()
        self.server_close()


class _ModernFacadeHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server: ModernFacadeServer

    def setup(self) -> None:
        super().setup()
        self.deadline = time.monotonic() + self.server.options.request_timeout_s
        self.proxy: _ModernProxy | None = None
        self.request_id: Any = None
        self.streamed = False
        self.response_bytes = 0
        self.completed = False
        self.connection.settimeout(min(self.server.options.request_timeout_s, 1.0))
        # Covers slow request lines/headers/body, including byte-at-a-time input.
        self.read_timer = threading.Timer(self.server.options.request_timeout_s, self._interrupt_http)
        self.read_timer.daemon = True
        self.read_timer.start()

    def finish(self) -> None:
        self.read_timer.cancel()
        super().finish()

    def _interrupt_http(self) -> None:
        try:
            self.connection.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass

    def log_message(self, _format: str, *args: Any) -> None:
        pass

    def _single_header(self, name: str, required: bool = True) -> str | None:
        values = self.headers.get_all(name, [])
        if len(values) > 1 or (required and len(values) != 1):
            raise _ModernFacadeError(400, -32020, "Header mismatch")
        return values[0] if values else None

    def _origin(self) -> None:
        port = self.server.server_address[1]
        for name in ("Host", "Origin"):
            value = self._single_header(name, required=name == "Host")
            if value is None:
                continue
            try:
                parsed = urllib.parse.urlsplit("http://" + value if name == "Host" else value)
                host = parsed.hostname
                local = host == "localhost" or bool(host and ipaddress.ip_address(host).is_loopback)
                valid = (local and parsed.scheme == "http" and (parsed.port or 80) == port
                         and parsed.username is None and parsed.password is None
                         and not parsed.path and not parsed.query and not parsed.fragment)
            except ValueError:
                valid = False
            if not valid:
                raise _ModernFacadeError(403, -32600, "HTTP origin is not permitted")

    def _check_live(self) -> None:
        if self.server._closing.is_set():
            raise _ModernFacadeError(503, ERR_TRANSPORT, "Modern facade is stopping")
        if time.monotonic() >= self.deadline:
            raise _ModernFacadeError(504, ERR_TRANSPORT, "Backend request deadline exceeded")
        try:
            ready, _, _ = select.select([self.connection], [], [], 0)
            if ready and self.connection.recv(1, socket.MSG_PEEK) == b"":
                raise _HttpPeerGone()
        except OSError:
            raise _HttpPeerGone() from None
        if self.proxy is not None and self.proxy.failure is not None:
            raise _ModernFacadeError(502, ERR_TRANSPORT, "Registered proxy transport failed", {"category": self.proxy.failure})

    def _read_modern_request(self) -> dict[str, Any]:
        self._origin()
        if self.path.split("?", 1)[0] != self.server.endpoint:
            raise _ModernFacadeError(404, -32601, "Unknown HTTP endpoint")
        if self.headers.get_all("Transfer-Encoding"):
            raise _ModernFacadeError(400, -32600, "Unsupported request framing")
        try:
            length = int(self._single_header("Content-Length") or "")
        except ValueError:
            raise _ModernFacadeError(400, -32600, "Invalid Content-Length") from None
        if length < 1:
            raise _ModernFacadeError(400, -32700, "Empty JSON-RPC body")
        if length > min(self.server.options.request_body_bytes, self.server.options.max_line_bytes - 1):
            raise _ModernFacadeError(413, -32600, "Request body exceeds bound")
        content_type = self._single_header("Content-Type") or ""
        if content_type.split(";", 1)[0].strip().lower() != CONTENT_TYPE_JSON:
            raise _ModernFacadeError(415, -32600, "Content-Type must be application/json")
        accept = ",".join(self.headers.get_all("Accept", []))
        accepted = set()
        for item in accept.split(","):
            pieces = [piece.strip() for piece in item.split(";")]
            quality = 1.0
            for parameter in pieces[1:]:
                key, _, value = parameter.partition("=")
                if key.lower() == "q":
                    if re.fullmatch(r"(?:0(?:\.[0-9]{0,3})?|1(?:\.0{0,3})?)", value) is None:
                        raise _ModernFacadeError(406, -32600, "Invalid Accept quality")
                    quality = float(value)
            if quality > 0:
                accepted.add(pieces[0].lower())
        if not {CONTENT_TYPE_JSON, CONTENT_TYPE_EVENT}.issubset(accepted):
            raise _ModernFacadeError(406, -32600, "Accept must include JSON and SSE")
        raw = self.rfile.read(length)
        if len(raw) != length:
            raise _ModernFacadeError(400, -32700, "Truncated JSON-RPC body")
        try:
            message = _modern_json(raw)
            _modern_encode(message)
        except (ValueError, UnicodeError, RecursionError):
            raise _ModernFacadeError(400, -32700, "Invalid JSON-RPC body") from None
        if not isinstance(message, dict):
            raise _ModernFacadeError(400, -32600, "JSON-RPC request must be an object")
        rid = message.get("id")
        self.request_id = rid if _modern_request_id(rid) else None
        method = message.get("method")
        if (message.get("jsonrpc") != "2.0" or not _modern_request_id(rid)
                or not isinstance(method, str) or not method
                or any(ord(c) < 33 or ord(c) > 126 for c in method)
                or "result" in message or "error" in message):
            raise _ModernFacadeError(400, -32600, "Modern HTTP accepts JSON-RPC requests only")
        if self.headers.get_all(SESSION_HEADER) or self.headers.get_all("Last-Event-ID"):
            raise _ModernFacadeError(400, -32020, "Sessions and response resumption are unavailable")
        version_header = self._single_header(PROTOCOL_HEADER)
        method_header = self._single_header("Mcp-Method")
        params = message.get("params")
        meta = params.get("_meta") if isinstance(params, dict) else None
        if (not isinstance(meta, dict) or not isinstance(meta.get(MODERN_VERSION_META), str)
                or not isinstance(meta.get(MODERN_CAPABILITIES_META), dict)):
            raise _ModernFacadeError(400, -32602, "Modern request metadata is required", {"supported": [MODERN_PROTOCOL_VERSION]})
        version = meta[MODERN_VERSION_META]
        if version_header != version or method_header != method:
            raise _ModernFacadeError(400, -32020, "Header mismatch")
        if version != MODERN_PROTOCOL_VERSION:
            raise _ModernFacadeError(400, -32022, "Unsupported protocol version", {"supported": [MODERN_PROTOCOL_VERSION], "requested": version})
        if method == "initialize":
            raise _ModernFacadeError(404, -32601, "initialize is unavailable in modern mode (2026-07-28)")
        name_header = self._single_header("Mcp-Name", required=method in ("tools/call", "resources/read", "prompts/get"))
        if method in ("tools/call", "resources/read", "prompts/get"):
            value = params.get("uri" if method == "resources/read" else "name")
            if not isinstance(value, str) or not value or _decode_modern_header(name_header or "") != value:
                raise _ModernFacadeError(400, -32020, "Header mismatch")
        elif name_header is not None:
            raise _ModernFacadeError(400, -32020, "Unexpected Mcp-Name header")
        if method != "tools/call" and any(name.lower().startswith("mcp-param-") for name in self.headers):
            raise _ModernFacadeError(400, -32020, "Unsupported HTTP parameter metadata")
        return message

    def _send_json(self, status: int, payload: dict[str, Any]) -> None:
        body = _json_bytes(payload)
        self.connection.settimeout(0.5)
        self.send_response(status)
        self.send_header("Content-Type", CONTENT_TYPE_JSON)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        if status == 405:
            self.send_header("Allow", "POST")
        self.end_headers()
        self.wfile.write(body)
        self.wfile.flush()
        self.completed = True

    def _event(self, payload: dict[str, Any], *, terminal: bool = False) -> None:
        raw = b"event: message\ndata: " + _json_bytes(payload) + b"\n\n"
        framed = f"{len(raw):x}\r\n".encode() + raw + b"\r\n"
        limit = self.server.options.max_line_bytes - (0 if terminal else 512)
        if self.response_bytes + len(framed) + 5 > limit:
            raise _ModernFacadeError(502, ERR_TRANSPORT, "HTTP response exceeds bound")
        if not self.streamed:
            self.connection.settimeout(0.5)
            self.send_response(200)
            self.send_header("Content-Type", CONTENT_TYPE_EVENT)
            self.send_header("Transfer-Encoding", "chunked")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("X-Accel-Buffering", "no")
            self.send_header("Connection", "close")
            self.end_headers()
            self.streamed = True
        self.response_bytes += len(framed)
        self.wfile.write(framed)
        if terminal:
            self.wfile.write(b"0\r\n\r\n")
            self.completed = True
        self.wfile.flush()

    def _backend_exchange(self, message: dict[str, Any], *, preflight: bool = False) -> dict[str, Any]:
        assert self.proxy is not None
        self._check_live()
        self.proxy.current_id = message["id"]
        self.proxy.send(message, application=not preflight)
        while True:
            self._check_live()
            try:
                kind, item = self.proxy.inbound.get(timeout=0.025)
            except queue.Empty:
                continue
            if kind == "exit":
                raise _ModernFacadeError(502, ERR_TRANSPORT, "Registered proxy exited before response")
            if item.get("jsonrpc") != "2.0":
                raise _ModernFacadeError(502, ERR_TRANSPORT, "Invalid backend JSON-RPC envelope")
            if "method" in item:
                if "id" in item:
                    raise _ModernFacadeError(502, ERR_TRANSPORT, "Independent backend requests cannot be expressed over modern HTTP")
                params, meta = item.get("params"), message["params"]["_meta"]
                if not isinstance(params, dict):
                    raise _ModernFacadeError(502, ERR_TRANSPORT, "Invalid backend notification")
                valid = False
                if item["method"] == "notifications/progress":
                    valid = "progressToken" in meta and type(params.get("progressToken")) is type(meta["progressToken"]) and params.get("progressToken") == meta["progressToken"]
                elif item["method"] == "notifications/message":
                    valid = "io.modelcontextprotocol/logLevel" in meta
                elif message["method"] == "subscriptions/listen":
                    notice_meta = params.get("_meta")
                    valid = isinstance(notice_meta, dict) and notice_meta.get("io.modelcontextprotocol/subscriptionId") == message["id"]
                if not valid:
                    raise _ModernFacadeError(502, ERR_TRANSPORT, "Unrelated backend notification")
                if not preflight:
                    self._event(item)
                continue
            rid = item.get("id")
            if (not _modern_request_id(rid) or isinstance(rid, str) != isinstance(message["id"], str)
                    or rid != message["id"] or ("error" in item) == ("result" in item)):
                raise _ModernFacadeError(502, ERR_TRANSPORT, "Unrelated backend response")
            if "error" in item:
                error = item["error"]
                if not isinstance(error, dict) or type(error.get("code")) is not int or not isinstance(error.get("message"), str):
                    raise _ModernFacadeError(502, ERR_TRANSPORT, "Invalid backend error")
            elif not isinstance(item["result"], dict) or not isinstance(item["result"].get("resultType"), str):
                raise _ModernFacadeError(502, ERR_TRANSPORT, "Modern backend resultType is required")
            self.proxy.current_id = None
            return item

    def _validate_tool_headers(self, message: dict[str, Any]) -> None:
        cursor = None
        cursors: set[str] = set()
        for page in range(4):
            params = {"_meta": message["params"]["_meta"]}
            if cursor is not None:
                params["cursor"] = cursor
            probe = {"jsonrpc": "2.0", "id": "facade-schema-" + secrets.token_hex(8), "method": "tools/list", "params": params}
            response = self._backend_exchange(probe, preflight=True)
            result = response.get("result")
            if not isinstance(result, dict) or result.get("resultType") != "complete" or not isinstance(result.get("tools"), list):
                raise _ModernFacadeError(502, ERR_TRANSPORT, "Backend tool schema preflight failed")
            matches = [tool for tool in result["tools"] if isinstance(tool, dict) and tool.get("name") == message["params"]["name"]]
            if len(matches) > 1:
                raise _ModernFacadeError(502, ERR_TRANSPORT, "Ambiguous backend tool schema")
            if matches:
                try:
                    bindings = _tool_header_schema(matches[0])
                    expected = _tool_argument_headers(matches[0], message["params"].get("arguments", {}))
                except _SchemaHeaderError as exc:
                    raise _ModernFacadeError(400, exc.code, exc.message, exc.data) from None
                known = {name.lower(): kind for _path, name, kind in bindings}
                expected = {name.lower(): value for name, value in expected.items()}
                supplied = {name.lower() for name in self.headers if name.lower().startswith("mcp-param-")}
                if supplied - known.keys():
                    raise _ModernFacadeError(400, -32020, "Unsupported HTTP parameter metadata")
                for name, kind in known.items():
                    value = self._single_header(name, required=name in expected)
                    if name not in expected:
                        if value is not None:
                            raise _ModernFacadeError(400, -32020, "Header mismatch")
                        continue
                    actual, wanted = _decode_modern_header(value or ""), _decode_modern_header(expected[name])
                    if kind == "integer":
                        try:
                            equal = len(actual) <= 64 and decimal.Decimal(actual).is_finite() and decimal.Decimal(actual) == decimal.Decimal(wanted)
                        except decimal.InvalidOperation:
                            equal = False
                    else:
                        equal = actual == wanted
                    if not equal:
                        raise _ModernFacadeError(400, -32020, "Header mismatch")
                return
            cursor = result.get("nextCursor")
            if cursor is None:
                raise _ModernFacadeError(400, -32602, "Tool schema unavailable; business request not forwarded")
            if not isinstance(cursor, str) or not cursor or cursor in cursors:
                raise _ModernFacadeError(502, ERR_TRANSPORT, "Invalid schema pagination")
            cursors.add(cursor)
        raise _ModernFacadeError(502, ERR_TRANSPORT, "Tool schema page bound exceeded")

    def do_POST(self) -> None:
        self.close_connection = True
        try:
            message = self._read_modern_request()
            self.read_timer.cancel()
            self._check_live()
            self.proxy = self.server.create_proxy(self.deadline)
            if message["method"] == "tools/call":
                self._validate_tool_headers(message)
            response = self._backend_exchange(message)
            if message["method"] == "tools/list" and response.get("result", {}).get("resultType") == "complete":
                tools = response["result"].get("tools")
                if not isinstance(tools, list):
                    raise _ModernFacadeError(502, ERR_TRANSPORT, "Backend tools/list requires a tools array")
                accepted = []
                for tool in tools:
                    try:
                        _tool_header_schema(tool)
                    except _SchemaHeaderError:
                        continue
                    accepted.append(tool)
                response = dict(response, result=dict(response["result"], tools=accepted))
            if self.streamed:
                self._event(response, terminal=True)
            else:
                code = response.get("error", {}).get("code")
                status = 404 if code == -32601 else 400 if code in (-32020, -32021, -32022) else 200
                self._send_json(status, response)
        except _HttpPeerGone:
            pass
        except _ModernFacadeError as exc:
            self.read_timer.cancel()
            if self.proxy is not None:
                # Stop the writer before snapshotting execution uncertainty:
                # a queued business write cannot race a false outcome report.
                self.server.drop_proxy(self.proxy, cancel=True)
            payload = exc.payload(self.request_id, bool(self.proxy and self.proxy.application_started))
            try:
                if self.streamed:
                    self._event(payload, terminal=True)
                else:
                    self._send_json(exc.status, payload)
            except (OSError, _ModernFacadeError):
                pass
        except (OSError, ValueError, UnicodeError, RecursionError):
            pass  # Transport or invalid framing; no diagnostics may echo data.
        finally:
            self.read_timer.cancel()
            if self.proxy is not None:
                self.server.drop_proxy(self.proxy, cancel=self.proxy.current_id is not None)

    def _unsupported_verb(self) -> None:
        self.close_connection = True
        try:
            self._send_json(405, _ModernFacadeError(405, -32601, "Modern HTTP supports POST only").payload(None))
        except OSError:
            pass

    do_GET = _unsupported_verb
    do_DELETE = _unsupported_verb
    do_PUT = _unsupported_verb
    do_PATCH = _unsupported_verb
    do_OPTIONS = _unsupported_verb
    do_HEAD = _unsupported_verb


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="stdio_http_facade",
        description=(
            "Explicit Agent-facing stdio-to-Streamable-HTTP MCP facade.  Serves "
            "one Streamable HTTP endpoint whose sessions are answered by one "
            "isolated '<side>-bridge-mcp/bridge.py connect <target>' subprocess "
            "against the local Bridge node."
        ),
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {FACADE_VERSION}")
    parser.add_argument("--protocol-era", choices=("legacy", "modern"), default="legacy",
                        help="Explicit HTTP binding: legacy session mode (default) or modern 2026-07-28 request mode.")
    parser.add_argument("--side", choices=("win", "wsl"), required=True)
    parser.add_argument("--target", required=True, help="registered peer MCP id to bridge")
    parser.add_argument("--node-host", default="127.0.0.1", help="local Bridge control host")
    parser.add_argument(
        "--node-port",
        type=int,
        default=None,
        help="local Bridge control port (default 8768 for win, 8769 for wsl)",
    )
    parser.add_argument("--listen-host", default="127.0.0.1", help="loopback HTTP bind host")
    parser.add_argument(
        "--listen-port",
        type=int,
        default=0,
        help="loopback HTTP port (0 picks a free port; actual port prints to stderr)",
    )
    parser.add_argument(
        "--backend-command",
        nargs="+",
        default=None,
        help=(
            "ADVANCED/test override: full argv for the per-session stdio backend "
            "instead of 'bridge.py connect <target>'.  Operator-supplied local "
            "configuration only; never derived from a peer."
        ),
    )
    parser.add_argument("--max-sessions", type=int, default=DEFAULT_MAX_SESSIONS)
    parser.add_argument("--request-body-bytes", type=int, default=DEFAULT_REQUEST_BODY_BYTES)
    parser.add_argument("--max-line-bytes", type=int, default=DEFAULT_MAX_LINE_BYTES)
    parser.add_argument("--max-inflight", type=int, default=DEFAULT_MAX_INFLIGHT)
    parser.add_argument("--request-timeout-s", type=float, default=DEFAULT_REQUEST_TIMEOUT_S)
    parser.add_argument("--initialize-timeout-s", type=float, default=DEFAULT_INITIALIZE_TIMEOUT_S)
    parser.add_argument("--session-idle-s", type=float, default=DEFAULT_SESSION_IDLE_S)
    parser.add_argument("--sse-heartbeat-s", type=float, default=DEFAULT_SSE_HEARTBEAT_S)
    return parser


def _default_node_port(side: str) -> int:
    return 8769 if side == "wsl" else 8768


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        options = FacadeOptions(
            side=args.side,
            protocol_era=args.protocol_era,
            target=args.target,
            node_host=args.node_host,
            node_port=int(args.node_port if args.node_port is not None else _default_node_port(args.side)),
            listen_host=args.listen_host,
            listen_port=int(args.listen_port),
            backend_command=list(args.backend_command) if args.backend_command else None,
            max_sessions=int(args.max_sessions),
            request_body_bytes=int(args.request_body_bytes),
            max_line_bytes=int(args.max_line_bytes),
            max_inflight=int(args.max_inflight),
            request_timeout_s=float(args.request_timeout_s),
            initialize_timeout_s=float(args.initialize_timeout_s),
            session_idle_s=float(args.session_idle_s),
            sse_heartbeat_s=float(args.sse_heartbeat_s),
        )
    except FacadeError as exc:
        print(f"stdio-http-facade: {exc}", file=sys.stderr)
        return 2
    try:
        server_type = ModernFacadeServer if options.protocol_era == "modern" else FacadeServer
        server = server_type(options)
    except (OSError, FacadeError) as exc:
        print(f"stdio-http-facade: failed to bind: {exc}", file=sys.stderr)
        return 2
    print(
        f"stdio-http-facade: listening on {server.url} target={options.target!r} "
        f"side={options.side} node={options.node_host}:{options.node_port} "
        f"(backend argv is not derived from the peer)",
        file=sys.stderr,
    )
    previous_term = None
    if options.protocol_era == "modern" and threading.current_thread() is threading.main_thread():
        previous_term = signal.getsignal(signal.SIGTERM)
        def terminate_modern(_signum: int, _frame: Any) -> None:
            raise KeyboardInterrupt()
        signal.signal(signal.SIGTERM, terminate_modern)
    try:
        server.serve()
    except KeyboardInterrupt:
        pass
    finally:
        server.shutdown_all()
        if options.protocol_era == "modern":
            server.server_close()
        if previous_term is not None:
            signal.signal(signal.SIGTERM, previous_term)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
