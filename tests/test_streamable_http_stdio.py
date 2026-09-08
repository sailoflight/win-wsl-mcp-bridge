#!/usr/bin/env python3
"""Conformance tests for the stdio-to-Streamable-HTTP adapter (P2).

The fixture is a small Streamable HTTP MCP server built on the stdlib
``http.server`` that implements the MCP 2025-06-18 transport rules the adapter
relies on.  Tests drive the adapter as a subprocess over real stdio pipes:

* session establishment and Mcp-Session-Id / MCP-Protocol-Version headers;
* JSON and SSE response modes for POSTed requests, including notifications
  relayed before the response on the same SSE stream;
* notifications/initialized and JSON-RPC responses accepted with HTTP 202;
* GET SSE server-initiated notifications and requests (answered by the local
  client through the adapter);
* cancellation of a long request is delivered mid-flight;
* HTTP error mapping (400/401/403/404/429/5xx) as structured JSON-RPC errors
  without retries, except HTTP 404 which re-initializes and retries once;
* protocol version negotiation (downgrade and unsupported-version error);
* redaction: Authorization/secret headers and response bodies never reach
  adapter stdout or stderr;
* isolation: one failing logical stream does not affect a healthy one.

Runs offline with the standard library only.
"""

from __future__ import annotations

import json
import errno
import io
import os
import queue
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import streamable_http_stdio as shs  # noqa: E402  (in-process client under test)

ADAPTER = ROOT / "streamable_http_stdio.py"
PROTOCOL = "2025-06-18"

SECRET_AUTHORIZATION = "Bearer hunter2-super-secret-token"
SECRET_HEADER = "superdupersecret-value"
LEAKY_BODY = "leaky-internal-server-detail"


def rpc_request(method: str, request_id: Any = None, params: dict[str, Any] | None = None) -> dict[str, Any]:
    message: dict[str, Any] = {"jsonrpc": "2.0", "method": method}
    if request_id is not None:
        message["id"] = request_id
    if params is not None:
        message["params"] = params
    return message


def rpc_response(request_id: Any, result: Any) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "id": request_id, "result": result}


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


# --------------------------------------------------------------------------
# Streamable HTTP fixture server
# --------------------------------------------------------------------------

class SessionState:
    def __init__(self, sid: str, negotiated_version: str):
        self.sid = sid
        self.negotiated_version = negotiated_version
        self.lock = threading.Lock()
        self.requests: list[dict[str, Any]] = []  # posted request records
        self.client_responses: list[dict[str, Any]] = []  # JSON-RPC responses the client POSTs
        self.cancelled_request_ids: list[Any] = []
        self.kill_used = False
        self.sse_writers: list["FixtureSSEWriter"] = []
        self.deleted = False

    def record(self, kind: str, detail: dict[str, Any]) -> None:
        with self.lock:
            self.requests.append({"kind": kind, **detail})

    def snapshot(self) -> dict[str, Any]:
        with self.lock:
            return {
                "negotiated_version": self.negotiated_version,
                "deleted": self.deleted,
                "requests": list(self.requests),
                "client_responses": list(self.client_responses),
                "cancelled_request_ids": list(self.cancelled_request_ids),
            }


class FixtureSSEWriter:
    """One connected GET SSE stream for a session (chunked HTTP/1.1)."""

    def __init__(self, handler: "FixtureHandler"):
        self.handler = handler
        self.closed = threading.Event()
        self._pending: queue.Queue[bytes] = queue.Queue()

    def send(self, message: dict[str, Any]) -> None:
        if self.closed.is_set():
            return
        data = f"event: message\ndata: {json.dumps(message, ensure_ascii=False, separators=(',', ':'))}\n\n"
        self._pending.put(data.encode("utf-8"))

    def run(self) -> None:
        try:
            while not self.closed.is_set():
                try:
                    chunk = self._pending.get(timeout=0.1)
                except queue.Empty:
                    continue
                payload = f"{len(chunk):X}\r\n".encode("ascii") + chunk + b"\r\n"
                self.handler.wfile.write(payload)
                self.handler.wfile.flush()
        except OSError:
            pass
        finally:
            self.closed.set()
            self.handler.close_connection = True
            try:
                self.handler.wfile.write(b"0\r\n\r\n")
                self.handler.wfile.flush()
            except OSError:
                pass


class FixtureConfig:
    def __init__(self) -> None:
        self.required_bearer: str | None = None
        #: None = accept any requested version (echo); a list restricts it.
        self.supported_versions: list[str] | None = None
        #: when the requested version is unsupported: downgrade to the newest
        #: supported version instead of answering with a JSON-RPC error.
        self.downgrade_on_mismatch = True
        self.require_protocol_version_header = False
        #: 'json' returns POST responses as one JSON object; 'sse' streams them.
        self.response_mode = "json"
        self.slow_seconds = 3.0
        self.instructions = "fixture-http-instructions"
        self.accept_deletes = True
        #: optional artificial delay before each initialize is handled, used to
        #: force overlapping re-initialization probes in concurrency tests.
        self.initialize_delay_s = 0.0


class FixtureServer(ThreadingHTTPServer):
    allow_reuse_address = True
    daemon_threads = True
    # Deterministic teardown is done explicitly in stop(); server_close() must
    # not wait for open SSE GET handler threads whose clients are still alive.
    block_on_close = False

    def __init__(self, config: FixtureConfig, endpoint: str = "/mcp"):
        super().__init__(("127.0.0.1", 0), FixtureHandler)
        self.config = config
        self.endpoint = endpoint
        self.lock = threading.Lock()
        self.sessions: dict[str, SessionState] = {}
        self.initialize_count = 0
        self.initialize_requests = 0
        self.delete_count = 0
        self.delete_headers: list[dict[str, str]] = []
        self.kill_sessions_served = False
        self._serve_thread: threading.Thread | None = None
        self._stopped = False

    @property
    def url(self) -> str:
        host, port = self.server_address[:2]
        return f"http://127.0.0.1:{port}{self.endpoint}"

    def create_session(self, negotiated_version: str) -> SessionState:
        sid = os.urandom(12).hex()
        session = SessionState(sid, negotiated_version)
        with self.lock:
            self.initialize_count += 1
            self.sessions[sid] = session
        return session

    def get_session(self, sid: str | None) -> SessionState | None:
        if not sid:
            return None
        with self.lock:
            return self.sessions.get(sid)

    def delete_session(self, sid: str) -> None:
        with self.lock:
            self.delete_count += 1
            session = self.sessions.pop(sid, None)
        if session:
            session.deleted = True

    def push(self, sid: str, message: dict[str, Any]) -> None:
        session = self.get_session(sid)
        if not session:
            return
        for writer in list(session.sse_writers):
            writer.send(message)

    def stop(self, join_timeout: float = 10.0) -> None:
        """Deterministically stop accepting, close the listening socket, and
        join the serve thread.

        Idempotent: repeated stop() calls (e.g. class tearDown after an early
        stop) are no-ops, because a second ``shutdown()`` would wait forever
        for a serve_forever loop that already exited.
        """
        if self._stopped:
            return
        self._stopped = True
        serve_thread = self._serve_thread
        if serve_thread is not None:
            try:
                self.shutdown()
            finally:
                try:
                    self.server_close()
                except OSError:
                    pass
            if serve_thread is not threading.current_thread():
                serve_thread.join(timeout=join_timeout)
        else:
            # Never served (constructed directly): still release the listener.
            try:
                self.server_close()
            except OSError:
                pass

    def handle_error(self, request: Any, client_address: Any) -> None:
        """A peer closing its connection mid-request is expected fixture
        behaviour (the adapter under test kills/abandons sockets): suppress
        only those transport errors.  Anything else still goes to stderr so
        real fixture bugs are not hidden."""
        exc = sys.exc_info()[1]
        if isinstance(exc, (ConnectionResetError, BrokenPipeError,
                            ConnectionAbortedError)):
            return
        if isinstance(exc, OSError) and exc.errno in (
            errno.ECONNRESET, errno.EPIPE, errno.ECONNABORTED, errno.EBADF,
        ):
            return
        if isinstance(exc, ValueError):
            # stdlib BaseHTTPRequestHandler raises a bare ValueError when the
            # request line of a connection aborted mid-request is truncated or
            # otherwise unparseable.  The fixture only ever *receives*
            # requests, so any such failure originates from a client under
            # test closing its socket at the wrong moment -- never from fixture
            # logic -- and is expected noise, not a real error to surface.
            frame = sys.exc_info()[2]
            while frame is not None:
                code = frame.tb_frame.f_code
                if code.co_name in ("parse_request", "handle_one_request") and code.co_filename.endswith(
                    "http" + os.sep + "server.py"
                ):
                    return
                frame = frame.tb_next
        super().handle_error(request, client_address)


class FixtureHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server: FixtureServer

    def log_message(self, format: str, *args: Any) -> None:  # quiet fixture
        pass

    # -- helpers -------------------------------------------------------------

    def _auth_ok(self) -> bool:
        bearer = self.server.config.required_bearer
        if bearer is None:
            return True
        value = self.headers.get("Authorization")
        return value == f"Bearer {bearer}"

    def _send_json(self, status: int, payload: dict[str, Any], extra: dict[str, str] | None = None) -> None:
        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        for name, value in (extra or {}).items():
            self.send_header(name, value)
        self.end_headers()
        self.wfile.write(body)

    def _send_empty(self, status: int) -> None:
        self.send_response(status)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def _read_message(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length)
        message = json.loads(raw.decode("utf-8"))
        if not isinstance(message, dict):
            raise ValueError("not an object")
        return message

    def _force_status(self) -> int | None:
        raw = self.headers.get("X-Fixture-Force")
        if raw is None:
            return None
        try:
            return int(raw)
        except ValueError:
            return None

    # -- verbs ---------------------------------------------------------------

    def do_POST(self) -> None:
        if self.path != self.server.endpoint:
            self._send_empty(404)
            return
        if not self._auth_ok():
            self.send_response(401)
            self.send_header("WWW-Authenticate", 'Bearer realm="fixture"')
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        forced = self._force_status()
        try:
            message = self._read_message()
        except (ValueError, UnicodeDecodeError):
            if forced:
                self._send_json(forced, {"error": LEAKY_BODY})
            else:
                self._send_json(
                    400,
                    {
                        "jsonrpc": "2.0",
                        "error": {"code": -32700, "message": "parse error"},
                    },
                )
            return
        if forced:
            self._send_json(forced, {"error": LEAKY_BODY})
            return
        method = message.get("method")
        sid = self.headers.get("Mcp-Session-Id")
        if method == "initialize":
            self._handle_initialize(message)
            return
        session = self.server.get_session(sid)
        if session is None:
            if sid is None:
                self._send_json(
                    400,
                    {
                        "jsonrpc": "2.0",
                        "error": {"code": -32600, "message": "missing session id header"},
                    },
                )
            else:
                self._send_json(404, {"error": "session not found"})
            return
        if self.server.config.require_protocol_version_header:
            sent = self.headers.get("MCP-Protocol-Version")
            if sent != session.negotiated_version:
                self._send_json(
                    400,
                    {
                        "jsonrpc": "2.0",
                        "error": {
                            "code": -32600,
                            "message": f"invalid MCP-Protocol-Version {sent!r}",
                        },
                    },
                )
                return
        self._dispatch(session, message, sid)

    def do_GET(self) -> None:
        if self.path != self.server.endpoint:
            self._send_empty(404)
            return
        if not self._auth_ok():
            self._send_empty(401)
            return
        sid = self.headers.get("Mcp-Session-Id")
        session = self.server.get_session(sid)
        if session is None:
            self._send_empty(404 if sid else 400)
            return
        session.record("get", {"version_header": self.headers.get("MCP-Protocol-Version")})
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Transfer-Encoding", "chunked")
        self.end_headers()
        writer = FixtureSSEWriter(self)
        with session.lock:
            session.sse_writers.append(writer)
        writer.run()
        with session.lock:
            if writer in session.sse_writers:
                session.sse_writers.remove(writer)

    def do_DELETE(self) -> None:
        if not self.server.config.accept_deletes:
            self._send_empty(405)
            return
        sid = self.headers.get("Mcp-Session-Id")
        session = self.server.get_session(sid)
        if session is None:
            self._send_empty(404)
            return
        with self.server.lock:
            self.server.delete_headers.append({k.lower(): v for k, v in self.headers.items()})
        self.server.delete_session(sid)
        self._send_empty(202)

    # -- message handling -------------------------------------------------------

    def _handle_initialize(self, message: dict[str, Any]) -> None:
        config = self.server.config
        with self.server.lock:
            self.server.initialize_requests += 1
        if config.initialize_delay_s:
            time.sleep(config.initialize_delay_s)
        params = message.get("params") or {}
        requested = params.get("protocolVersion", PROTOCOL)
        request_id = message.get("id")
        negotiated = requested
        supported = config.supported_versions
        if supported is not None and requested not in supported:
            if config.downgrade_on_mismatch:
                negotiated = sorted(supported)[-1]
            else:
                self._send_json(
                    200,
                    {
                        "jsonrpc": "2.0",
                        "id": request_id,
                        "error": {
                            "code": -32602,
                            "message": "Unsupported protocol version",
                            "data": {"supported": supported, "requested": requested},
                        },
                    },
                )
                return
        session = self.server.create_session(negotiated)
        session.record("initialize", {"requested": requested, "negotiated": negotiated})
        result = {
            "protocolVersion": negotiated,
            "capabilities": {"tools": {"listChanged": True}, "logging": {}},
            "serverInfo": {"name": "fixture-http", "title": "Fixture HTTP MCP", "version": "1.0.0"},
            "instructions": config.instructions,
        }
        self._send_json(
            200,
            {"jsonrpc": "2.0", "id": request_id, "result": result},
            extra={
                "Mcp-Session-Id": session.sid,
                "MCP-Protocol-Version": negotiated,
            },
        )

    def _dispatch(self, session: SessionState, message: dict[str, Any], sid: str) -> None:
        method = message.get("method")
        request_id = message.get("id")
        session.record(
            "message",
            {
                "method": method,
                "id": request_id,
                "session": sid,
                "version_header": self.headers.get("MCP-Protocol-Version"),
            },
        )
        params = message.get("params") or {}
        if "id" not in message:
            # notification
            if method == "notifications/cancelled":
                cancelled = (params or {}).get("requestId")
                if cancelled is not None:
                    with session.lock:
                        session.cancelled_request_ids.append(cancelled)
            self._send_empty(202)
            return
        if "result" in message or "error" in message:
            # JSON-RPC response from the client (answering a server request)
            with session.lock:
                session.client_responses.append(message)
            self._send_empty(202)
            return
        # request -----------------------------------------------------------
        if method == "ping":
            self._respond(session, request_id, {})
        elif method == "tools/list":
            tools = [
                {"name": "echo", "description": "Echo text.", "inputSchema": {"type": "object", "properties": {"text": {"type": "string"}}}},
                {"name": "slow", "description": "Slow tool that honours cancellation.", "inputSchema": {"type": "object", "properties": {}}},
                {"name": "progress", "description": "Emits progress notifications.", "inputSchema": {"type": "object", "properties": {}}},
                {"name": "session-echo", "description": "Echo session facts.", "inputSchema": {"type": "object", "properties": {}}},
                {"name": "kill-session", "description": "Ends this session with HTTP 404.", "inputSchema": {"type": "object", "properties": {}}},
                {"name": "http-error", "description": "Returns HTTP 401.", "inputSchema": {"type": "object", "properties": {}}},
                {"name": "bad-version", "description": "Returns HTTP 400 + JSON-RPC error.", "inputSchema": {"type": "object", "properties": {}}},
                {"name": "rate-limit", "description": "Returns HTTP 429.", "inputSchema": {"type": "object", "properties": {}}},
                {"name": "server-error", "description": "Returns HTTP 500 with a leaky body.", "inputSchema": {"type": "object", "properties": {}}},
                {"name": "hang", "description": "Sleeps longer than any idle timeout.", "inputSchema": {"type": "object", "properties": {}}},
            ]
            self._respond(session, request_id, {"tools": tools})
        elif method == "tools/call":
            self._handle_tool_call(session, message)
        else:
            self._respond(
                session,
                request_id,
                None,
                error={"code": -32601, "message": f"method not found: {method}"},
            )

    def _respond(
        self,
        session: SessionState,
        request_id: Any,
        result: Any,
        *,
        error: dict[str, Any] | None = None,
        progress_events: list[dict[str, Any]] | None = None,
        early_messages: list[dict[str, Any]] | None = None,
    ) -> None:
        config = self.server.config
        payload = {"jsonrpc": "2.0", "id": request_id}
        if error is not None:
            payload["error"] = error
        else:
            payload["result"] = result
        if config.response_mode == "json":
            self._send_json(200, payload)
            return
        # SSE response stream
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Transfer-Encoding", "chunked")
        self.end_headers()
        for early in early_messages or []:
            self._write_sse_event(early)
        for progress in progress_events or []:
            self._write_sse_event(
                {"jsonrpc": "2.0", "method": "notifications/progress", "params": progress}
            )
        self._write_sse_event(payload)
        self.wfile.write(b"0\r\n\r\n")
        self.wfile.flush()
        self.close_connection = True

    def _write_sse_event(self, message: dict[str, Any]) -> None:
        data = f"event: message\ndata: {json.dumps(message, ensure_ascii=False, separators=(',', ':'))}\n\n"
        chunk = data.encode("utf-8")
        self.wfile.write(f"{len(chunk):X}\r\n".encode("ascii") + chunk + b"\r\n")
        self.wfile.flush()

    def _handle_tool_call(self, session: SessionState, message: dict[str, Any]) -> None:
        request_id = message.get("id")
        tool_name = ((message.get("params") or {}).get("name") or "")
        arguments = ((message.get("params") or {}).get("arguments") or {})
        progress_token = (((message.get("params") or {}).get("_meta") or {}).get("progressToken"))
        if tool_name == "echo":
            self._respond(session, request_id, {"content": [{"type": "text", "text": f"echo:{arguments.get('text', '')}"}]})
        elif tool_name == "slow":
            deadline = time.monotonic() + self.server.config.slow_seconds
            while time.monotonic() < deadline:
                with session.lock:
                    cancelled = request_id in session.cancelled_request_ids
                if cancelled:
                    self._respond(session, request_id, {"content": [{"type": "text", "text": "cancelled"}]})
                    return
                time.sleep(0.02)
            self._respond(session, request_id, {"content": [{"type": "text", "text": "completed"}]})
        elif tool_name == "progress":
            self._respond(
                session,
                request_id,
                {"content": [{"type": "text", "text": "progressed"}]},
                progress_events=[
                    {"progressToken": progress_token, "progress": 1, "total": 2},
                    {"progressToken": progress_token, "progress": 2, "total": 2},
                ],
            )
        elif tool_name == "session-echo":
            with session.lock:
                count = len(session.requests)
            self._respond(session, request_id, {"content": [{"type": "text", "text": f"session:{session.sid}:requests:{count}"}]})
        elif tool_name == "kill-session":
            if not self.server.kill_sessions_served:
                # First call anywhere ends its session with HTTP 404; the
                # adapter re-initializes and retries, and the retry succeeds.
                self.server.kill_sessions_served = True
                self.server.delete_session(session.sid)
                self._send_json(404, {"error": "session terminated by tool"})
                self.close_connection = True
                return
            self._respond(session, request_id, {"content": [{"type": "text", "text": "ok"}]})
        elif tool_name == "http-error":
            self.send_response(401)
            self.send_header("WWW-Authenticate", 'Bearer realm="fixture"')
            self.send_header("Content-Length", "0")
            self.end_headers()
        elif tool_name == "bad-version":
            self._send_json(
                400,
                {"jsonrpc": "2.0", "error": {"code": -32600, "message": "invalid MCP-Protocol-Version header"}},
            )
        elif tool_name == "rate-limit":
            self.send_response(429)
            self.send_header("Retry-After", "1")
            self.send_header("Content-Length", "0")
            self.end_headers()
        elif tool_name == "server-error":
            self._send_json(500, {"error": LEAKY_BODY})
        elif tool_name == "hang":
            time.sleep(self.server.config.hang_seconds)
            self._respond(session, request_id, {"content": [{"type": "text", "text": "hung-done"}]})
        else:
            self._respond(
                session,
                request_id,
                None,
                error={"code": -32602, "message": f"unknown tool {tool_name}"},
            )


def start_fixture(config: FixtureConfig | None = None) -> FixtureServer:
    server = FixtureServer(config or FixtureConfig())
    thread = threading.Thread(
        target=lambda: server.serve_forever(poll_interval=0.05),
        daemon=True,
        name=f"fixture-http-{server.server_address[1]}",
    )
    server._serve_thread = thread
    thread.start()
    return server


# --------------------------------------------------------------------------
# Subprocess stdio peer driving one adapter instance
# --------------------------------------------------------------------------

class AdapterPeer:
    def __init__(self, url: str, *, headers: list[str] | None = None,
                 env_headers: dict[str, str] | None = None,
                 extra_args: list[str] | None = None,
                 stderr_file: Path):
        self.stderr_file = stderr_file
        self._stderr_handle = stderr_file.open("wb")
        env = os.environ.copy()
        env["PYTHONDONTWRITEBYTECODE"] = "1"
        for key, value in (env_headers or {}).items():
            env[key] = value
        command = [sys.executable, str(ADAPTER), "--url", url]
        for header in headers or []:
            command += ["--header", header]
        command += [
            "--idle-timeout", "5",
            "--sse-idle-timeout", "30",
            "--connect-timeout", "5",
        ] + (extra_args or [])
        self.process = subprocess.Popen(
            command,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=self._stderr_handle,
            env=env,
        )
        self.lines: list[dict[str, Any]] = []
        self._condition = threading.Condition()
        self._error: BaseException | None = None
        self._reader = threading.Thread(target=self._read_loop, daemon=True)
        self._reader.start()

    def _read_loop(self) -> None:
        try:
            assert self.process.stdout is not None
            while True:
                raw = self.process.stdout.readline()
                if not raw:
                    break
                parsed = json.loads(raw.decode("utf-8"))
                with self._condition:
                    self.lines.append(parsed)
                    self._condition.notify_all()
        except BaseException as exc:  # pragma: no cover - defensive
            self._error = exc

    def send(self, message: dict[str, Any]) -> None:
        assert self.process.stdin is not None
        self.process.stdin.write(
            json.dumps(message, ensure_ascii=False, separators=(",", ":")).encode("utf-8") + b"\n"
        )
        self.process.stdin.flush()

    def wait_for(self, predicate: Any, timeout: float = 8.0) -> dict[str, Any]:
        deadline = time.monotonic() + timeout
        with self._condition:
            while True:
                for index, line in enumerate(self.lines):
                    if predicate(line):
                        return self.lines.pop(index)
                if self._error is not None:
                    raise AssertionError(f"peer reader failed: {self._error}")
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise AssertionError(
                        f"timed out waiting for message; got {self.lines!r}"
                    )
                self._condition.wait(remaining)

    def wait_response(self, request_id: Any, timeout: float = 8.0) -> dict[str, Any]:
        return self.wait_for(
            lambda m: m.get("id") == request_id
            and ("result" in m or "error" in m),
            timeout,
        )

    def close(self) -> int:
        if self.process.stdin is not None:
            self.process.stdin.close()
        code = self.process.wait(timeout=15)
        self._release_stderr()
        return code

    def kill(self) -> None:
        if self.process.poll() is None:
            self.process.kill()
            self.process.wait(timeout=10)
        self._release_stderr()

    def _release_stderr(self) -> None:
        try:
            self._stderr_handle.close()
        except OSError:
            pass
        if self.process.stdout is not None:
            try:
                self.process.stdout.close()
            except OSError:
                pass

    @property
    def stderr(self) -> str:
        return self.stderr_file.read_text(encoding="utf-8", errors="replace")


class _FixtureTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self._peers: list[AdapterPeer] = []

    def tearDown(self) -> None:
        for peer in self._peers:
            try:
                peer.kill()
            except Exception:
                pass
        self.temp.cleanup()

    def start_peer(self, url: str, *, headers: list[str] | None = None,
                   env_headers: dict[str, str] | None = None,
                   extra_args: list[str] | None = None) -> AdapterPeer:
        stderr_file = self.root / f"adapter-{len(self._peers)}.stderr"
        peer = AdapterPeer(
            url,
            headers=headers,
            env_headers=env_headers,
            extra_args=extra_args,
            stderr_file=stderr_file,
        )
        self._peers.append(peer)
        return peer


class SessionAndNegotiationTest(_FixtureTestCase):
    def test_json_round_trip_and_session_headers(self) -> None:
        config = FixtureConfig()
        config.response_mode = "json"
        config.require_protocol_version_header = True
        server = start_fixture(config)
        try:
            peer = self.start_peer(server.url)
            peer.send(rpc_request("initialize", 1, {
                "protocolVersion": PROTOCOL,
                "capabilities": {},
                "clientInfo": {"name": "peer-test", "version": "1"},
            }))
            init_response = peer.wait_response(1)
            self.assertIn("result", init_response)
            self.assertEqual(init_response["result"]["protocolVersion"], PROTOCOL)
            self.assertEqual(init_response["result"]["instructions"], "fixture-http-instructions")
            peer.send(rpc_request("notifications/initialized"))
            peer.send(rpc_request("tools/list", 2))
            listed = peer.wait_response(2)
            names = {t["name"] for t in listed["result"]["tools"]}
            self.assertIn("echo", names)
            peer.send(rpc_request("tools/call", 3, {"name": "echo", "arguments": {"text": "hello"}}))
            called = peer.wait_response(3)
            self.assertEqual(called["result"]["content"][0]["text"], "echo:hello")
            # Session id and version headers reached the fixture on every POST.
            session = list(server.sessions.values())[0]
            snapshot = session.snapshot()
            self.assertEqual(snapshot["negotiated_version"], PROTOCOL)
            messages = [r for r in snapshot["requests"] if r["kind"] == "message"]
            self.assertGreaterEqual(len(messages), 2)
            for record in messages:
                self.assertEqual(record["session"], session.sid)
                self.assertEqual(record["version_header"], PROTOCOL)
            # Adapter DELETE on exit ends the session.
            peer.close()
            deadline = time.monotonic() + 5
            while server.get_session(session.sid) is not None and time.monotonic() < deadline:
                time.sleep(0.05)
            self.assertIsNone(server.get_session(session.sid))
            self.assertEqual(server.delete_count, 1)
            # The DELETE carried the session id and the negotiated version.
            self.assertEqual(len(server.delete_headers), 1)
            delete_headers = server.delete_headers[0]
            self.assertEqual(delete_headers.get("mcp-session-id"), session.sid)
            self.assertEqual(delete_headers.get("mcp-protocol-version"), PROTOCOL)
            self.assertEqual(peer.process.returncode, 0)
        finally:
            server.stop()

    def test_sse_response_mode_relays_progress_notifications(self) -> None:
        config = FixtureConfig()
        config.response_mode = "sse"
        server = start_fixture(config)
        try:
            peer = self.start_peer(server.url)
            peer.send(rpc_request("initialize", 1, {"protocolVersion": PROTOCOL}))
            peer.wait_response(1)
            peer.send(rpc_request("notifications/initialized"))
            peer.send(rpc_request(
                "tools/call", 5,
                {"name": "progress", "arguments": {}, "_meta": {"progressToken": "pt-1"}},
            ))
            progress = peer.wait_for(
                lambda m: m.get("method") == "notifications/progress"
                and (m.get("params") or {}).get("progressToken") == "pt-1"
            )
            self.assertEqual(progress["params"]["progress"], 1)
            done = peer.wait_response(5)
            self.assertEqual(done["result"]["content"][0]["text"], "progressed")
            peer.close()
        finally:
            server.stop()

    def test_cancellation_delivered_mid_request(self) -> None:
        config = FixtureConfig()
        config.slow_seconds = 4.0
        server = start_fixture(config)
        try:
            peer = self.start_peer(server.url)
            peer.send(rpc_request("initialize", 1, {"protocolVersion": PROTOCOL}))
            peer.wait_response(1)
            peer.send(rpc_request("notifications/initialized"))
            peer.send(rpc_request("tools/call", 7, {"name": "slow", "arguments": {}}))
            time.sleep(0.5)  # let the request reach the server first
            peer.send(rpc_request("notifications/cancelled", params={"requestId": 7}))
            response = peer.wait_response(7, timeout=10)
            self.assertEqual(response["result"]["content"][0]["text"], "cancelled")
            session = list(server.sessions.values())[0]
            self.assertIn(7, session.snapshot()["cancelled_request_ids"])
            peer.close()
        finally:
            server.stop()

    def test_protocol_downgrade_and_unsupported_version(self) -> None:
        downgraded = FixtureConfig()
        downgraded.supported_versions = ["2025-03-26"]
        downgraded.downgrade_on_mismatch = True
        downgraded.require_protocol_version_header = True
        server = start_fixture(downgraded)
        try:
            peer = self.start_peer(server.url)
            peer.send(rpc_request("initialize", 1, {"protocolVersion": PROTOCOL}))
            response = peer.wait_response(1)
            self.assertEqual(response["result"]["protocolVersion"], "2025-03-26")
            peer.send(rpc_request("notifications/initialized"))
            peer.send(rpc_request("tools/list", 2))
            listed = peer.wait_response(2)  # header must match negotiated version
            self.assertIn("tools", listed["result"])
            peer.close()
            self.assertEqual(peer.process.returncode, 0)
        finally:
            server.stop()

        rejecting = FixtureConfig()
        rejecting.supported_versions = ["2024-11-05"]
        rejecting.downgrade_on_mismatch = False
        server2 = start_fixture(rejecting)
        try:
            peer2 = self.start_peer(server2.url)
            peer2.send(rpc_request("initialize", 9, {"protocolVersion": PROTOCOL}))
            response = peer2.wait_response(9)
            self.assertEqual(response["error"]["code"], -32602)
            peer2.send(rpc_request("ping", 10))
            peer2.send(rpc_request("notifications/initialized"))
            # Without a session the adapter cannot reach the remote; ping fails
            # closed with a structured transport error.
            error = peer2.wait_response(10)
            self.assertEqual(error["error"]["code"], -32000)
            # The rejected initialize reached the server but never created a
            # session, so the adapter has nothing to attach a ping to.
            self.assertEqual(server2.initialize_requests, 1)
            self.assertEqual(server2.initialize_count, 0)
            self.assertFalse(server2.sessions)
            peer2.close()
        finally:
            server2.stop()


class ServerInitiatedTest(_FixtureTestCase):
    def test_get_sse_notifications_and_requests(self) -> None:
        server = start_fixture(FixtureConfig())
        try:
            peer = self.start_peer(server.url)
            peer.send(rpc_request("initialize", 1, {"protocolVersion": PROTOCOL}))
            peer.wait_response(1)
            peer.send(rpc_request("notifications/initialized"))
            peer.send(rpc_request("ping", 2))
            peer.wait_response(2)  # session and GET stream are established
            sid = list(server.sessions.values())[0].sid
            # Wait until the adapter's GET SSE stream is connected.
            deadline = time.monotonic() + 5
            while not server.sessions[sid].sse_writers and time.monotonic() < deadline:
                time.sleep(0.02)
            self.assertTrue(server.sessions[sid].sse_writers)
            # Server-initiated notification over GET.
            server.push(sid, {
                "jsonrpc": "2.0",
                "method": "notifications/message",
                "params": {"level": "info", "data": "pushed-notification"},
            })
            notice = peer.wait_for(
                lambda m: m.get("method") == "notifications/message"
                and (m.get("params") or {}).get("data") == "pushed-notification"
            )
            self.assertEqual(notice["params"]["level"], "info")
            # Server-initiated request over GET; local client answers it.
            server.push(sid, rpc_request("roots/list", "sr-1"))
            request = peer.wait_for(
                lambda m: m.get("id") == "sr-1" and m.get("method") == "roots/list"
            )
            self.assertEqual(request["method"], "roots/list")
            peer.send(rpc_response("sr-1", {"roots": []}))
            deadline = time.monotonic() + 5
            while True:
                snapshot = server.sessions[sid].snapshot()
                if any(
                    r.get("id") == "sr-1" and "result" in r
                    for r in snapshot["client_responses"]
                ):
                    break
                if time.monotonic() > deadline:
                    self.fail("adapter never relayed the JSON-RPC response")
                time.sleep(0.02)
            peer.close()
        finally:
            server.stop()


class HttpErrorMappingTest(_FixtureTestCase):
    def _session_peer(self, server: FixtureServer, request_id_base: int = 0) -> AdapterPeer:
        peer = self.start_peer(server.url)
        peer.send(rpc_request("initialize", 1, {"protocolVersion": PROTOCOL}))
        peer.wait_response(1)
        peer.send(rpc_request("notifications/initialized"))
        return peer

    def test_maps_http_errors_to_structured_jsonrpc_errors(self) -> None:
        server = start_fixture(FixtureConfig())
        try:
            peer = self._session_peer(server)
            cases = [
                ("http-error", 401, "authentication"),
                ("rate-limit", 429, "rate-limited"),
                ("server-error", 500, "server"),
                ("bad-version", 400, "remote-error"),
            ]
            for index, (tool, http_status, category) in enumerate(cases):
                request_id = 100 + index
                peer.send(rpc_request("tools/call", request_id, {"name": tool, "arguments": {}}))
                response = peer.wait_response(request_id)
                self.assertEqual(response["error"]["code"], -32000, tool)
                self.assertIn(category, response["error"]["message"], tool)
                self.assertEqual(response["error"]["data"]["httpStatus"], http_status, tool)
                # The stream stays healthy after each failure.
                ping_id = 200 + index
                peer.send(rpc_request("ping", ping_id))
                self.assertIn("result", peer.wait_response(ping_id))
            peer.close()
        finally:
            server.stop()

    def test_404_reinitializes_and_retries_once(self) -> None:
        server = start_fixture(FixtureConfig())
        try:
            peer = self._session_peer(server)
            peer.send(rpc_request("tools/call", 77, {"name": "kill-session", "arguments": {}}))
            response = peer.wait_response(77, timeout=15)
            # First call lost the session; adapter re-initialized on a fresh
            # session and retried, where the tool succeeds.
            self.assertEqual(response["result"]["content"][0]["text"], "ok")
            self.assertEqual(server.initialize_count, 2)
            peer.send(rpc_request("tools/call", 78, {"name": "echo", "arguments": {"text": "after"}}))
            echoed = peer.wait_response(78)
            self.assertEqual(echoed["result"]["content"][0]["text"], "echo:after")
            peer.close()
        finally:
            server.stop()

    def test_timeout_does_not_affect_concurrent_requests(self) -> None:
        config = FixtureConfig()
        config.hang_seconds = 8.0
        server = start_fixture(config)
        try:
            peer = self.start_peer(
                server.url,
                extra_args=["--request-timeout", "2", "--idle-timeout", "2"],
            )
            peer.send(rpc_request("initialize", 1, {"protocolVersion": PROTOCOL}))
            peer.wait_response(1)
            peer.send(rpc_request("notifications/initialized"))
            # A hung tool call times out on its own connection...
            peer.send(rpc_request("tools/call", 301, {"name": "hang", "arguments": {}}))
            time.sleep(0.2)
            # ...while an unrelated request on the same logical stream still
            # completes through a separate POST connection.
            peer.send(rpc_request("tools/call", 302, {"name": "echo", "arguments": {"text": "quick"}}))
            quick = peer.wait_response(302, timeout=10)
            self.assertEqual(quick["result"]["content"][0]["text"], "echo:quick")
            hung = peer.wait_response(301, timeout=10)
            self.assertEqual(hung["error"]["code"], -32000)
            peer.close()
        finally:
            server.stop()


class ConcurrentSessionLossTest(unittest.TestCase):
    """Deterministic in-process checks of session-loss recovery locking.

    Regression guard for the recovery lock contract: re-initialization must
    serialize on ``_reinit_lock`` and must NEVER hold the client state lock
    ``_lock`` across the ``initialize()`` network probe — ``initialize()``
    re-acquires ``_lock`` at its tail, so holding it across the probe is a
    self-deadlock.  Each test would hang or double-initialize on the buggy
    variants.
    """

    def setUp(self) -> None:
        self._servers: list[FixtureServer] = []

    def tearDown(self) -> None:
        for server in self._servers:
            server.stop()

    def _start(self, config: FixtureConfig) -> FixtureServer:
        server = start_fixture(config)
        self._servers.append(server)
        return server

    def _client(self, server: FixtureServer) -> shs.StreamableHttpClient:
        options = shs.AdapterOptions(
            endpoint_url=server.url,
            connect_timeout_s=5,
            idle_timeout_s=5,
            request_timeout_s=10,
            sse_idle_timeout_s=10,
        )
        return shs.StreamableHttpClient(options, shs._Diag(io.StringIO()))

    def test_concurrent_404_recovery_is_exactly_once_and_never_deadlocks(self) -> None:
        config = FixtureConfig()
        server = self._start(config)
        client = self._client(server)
        client.initialize(rpc_request("initialize", 1, {"protocolVersion": PROTOCOL}))
        sid, _generation = client.session_snapshot()
        # Slow down initialize handling so every concurrent recovery really
        # queues on the re-init lock while the first probe is in flight.
        config.initialize_delay_s = 0.6
        server.delete_session(sid)  # server-side session loss for all threads
        outcomes: dict[int, str] = {}

        def worker(index: int) -> None:
            try:
                client.post(rpc_request("ping", 1000 + index), retry_session_loss=True)
                outcomes[index] = "ok"
            except Exception as exc:  # pragma: no cover - assertion below
                outcomes[index] = f"raised {type(exc).__name__}"

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=15)
            self.assertFalse(thread.is_alive(), "404 recovery deadlocked")
        # Every request retried successfully on the fresh session...
        self.assertEqual(outcomes, {0: "ok", 1: "ok", 2: "ok", 3: "ok"})
        # ...and exactly ONE re-initialization happened despite four 404s.
        self.assertEqual(server.initialize_count, 2)
        self.assertTrue(client.initialized())

    def test_no_state_lock_held_across_probe_initialize(self) -> None:
        config = FixtureConfig()
        server = self._start(config)
        client = self._client(server)
        client.initialize(rpc_request("initialize", 1, {"protocolVersion": PROTOCOL}))
        sid, _generation = client.session_snapshot()
        config.initialize_delay_s = 0.8
        server.delete_session(sid)
        result: dict[str, bool] = {}

        def recover() -> None:
            result["value"] = client._recover_session_loss(sid)

        thread = threading.Thread(target=recover)
        thread.start()
        # Wait until the probe initialize is being handled server-side: the
        # second initialize request is in flight and no new session exists yet.
        deadline = time.monotonic() + 10
        while True:
            with server.lock:
                in_flight = server.initialize_requests == 2 and not server.sessions
            if in_flight:
                break
            self.assertLess(time.monotonic(), deadline, "probe never started")
            time.sleep(0.02)
        # While the probe sleeps server-side, the state lock must be free: an
        # unrelated POST must reach the network promptly (HTTP 404 for the dead
        # session) instead of blocking behind the probing thread.
        started = time.monotonic()
        with self.assertRaises(shs.HttpTransportError):
            client.post(rpc_request("ping", 2000), retry_session_loss=False)
        self.assertLess(
            time.monotonic() - started,
            0.5,
            "self._lock was held across the initialize probe",
        )
        thread.join(timeout=10)
        self.assertFalse(thread.is_alive(), "reinitialize_session deadlocked")
        self.assertTrue(result.get("value"))
        new_sid, _generation = client.session_snapshot()
        self.assertIsNotNone(new_sid)
        self.assertNotEqual(new_sid, sid)
        self.assertEqual(server.initialize_count, 2)

    def test_stale_404_never_tears_down_newer_session(self) -> None:
        server = self._start(FixtureConfig())
        client = self._client(server)
        client.initialize(rpc_request("initialize", 1, {"protocolVersion": PROTOCOL}))
        old_sid, old_generation = client.session_snapshot()
        server.delete_session(old_sid)
        self.assertTrue(client._recover_session_loss(old_sid))
        new_sid, new_generation = client.session_snapshot()
        self.assertNotEqual(new_sid, old_sid)
        self.assertEqual(server.initialize_count, 2)
        # A late 404 whose request used the OLD session id arrives now that a
        # newer session is live: it must be benign and change nothing.
        self.assertTrue(client._recover_session_loss(old_sid))
        after_sid, after_generation = client.session_snapshot()
        self.assertEqual(after_sid, new_sid)
        self.assertEqual(after_generation, new_generation)
        self.assertEqual(server.initialize_count, 2)
        self.assertTrue(client.initialized())


class RedactionTest(_FixtureTestCase):
    def test_secrets_and_bodies_never_reach_stdio(self) -> None:
        config = FixtureConfig()
        config.required_bearer = "good-token"
        server = start_fixture(config)
        try:
            peer = self.start_peer(
                server.url,
                headers=[
                    f"Authorization: {SECRET_AUTHORIZATION}",
                    f"X-Secret: {SECRET_HEADER}",
                ],
            )
            peer.send(rpc_request("initialize", 1, {"protocolVersion": PROTOCOL}))
            error = peer.wait_response(1)
            self.assertEqual(error["error"]["code"], -32000)
            self.assertIn("authentication", error["error"]["message"])
            peer.close()
            stderr = peer.stderr
            self.assertNotIn("hunter2", stderr)
            self.assertNotIn(SECRET_HEADER, stderr)
            self.assertNotIn(SECRET_AUTHORIZATION, stderr)
        finally:
            server.stop()

    def test_server_error_body_is_not_echoed(self) -> None:
        server = start_fixture(FixtureConfig())
        try:
            peer = self._session_peer(server)
            peer.send(rpc_request("tools/call", 500, {"name": "server-error", "arguments": {}}))
            error = peer.wait_response(500)
            self.assertEqual(error["error"]["code"], -32000)
            self.assertNotIn(LEAKY_BODY, json.dumps(error))
            peer.close()
            self.assertNotIn(LEAKY_BODY, peer.stderr)
        finally:
            server.stop()

    def _session_peer(self, server: FixtureServer, extra: None = None) -> AdapterPeer:
        peer = self.start_peer(server.url)
        peer.send(rpc_request("initialize", 1, {"protocolVersion": PROTOCOL}))
        peer.wait_response(1)
        peer.send(rpc_request("notifications/initialized"))
        return peer


class IsolationTest(_FixtureTestCase):
    def test_failing_stream_does_not_affect_healthy_stream(self) -> None:
        healthy_config = FixtureConfig()
        healthy_server = start_fixture(healthy_config)
        auth_config = FixtureConfig()
        auth_config.required_bearer = "right-token"
        auth_server = start_fixture(auth_config)
        try:
            healthy = self._session_peer(healthy_server)
            failing = self.start_peer(
                auth_server.url, headers=["Authorization: Bearer wrong-token"]
            )
            failing.send(rpc_request("initialize", 1, {"protocolVersion": PROTOCOL}))
            failing_error = failing.wait_response(1)
            self.assertIn("authentication", failing_error["error"]["message"])
            # The healthy stream keeps working while the other is failing.
            healthy.send(rpc_request("tools/call", 61, {"name": "echo", "arguments": {"text": "still-here"}}))
            result = healthy.wait_response(61)
            self.assertEqual(result["result"]["content"][0]["text"], "echo:still-here")
            healthy.send(rpc_request("tools/list", 62))
            self.assertIn("tools", healthy.wait_response(62)["result"])
            failing.close()
            healthy.close()
        finally:
            healthy_server.stop()
            auth_server.stop()

    def _session_peer(self, server: FixtureServer) -> AdapterPeer:
        peer = self.start_peer(server.url)
        peer.send(rpc_request("initialize", 1, {"protocolVersion": PROTOCOL}))
        peer.wait_response(1)
        peer.send(rpc_request("notifications/initialized"))
        return peer


class StdioDisciplineTest(_FixtureTestCase):
    def test_stdout_is_protocol_clean_and_process_exits_on_eof(self) -> None:
        server = start_fixture(FixtureConfig())
        try:
            peer = self.start_peer(server.url)
            peer.send(rpc_request("initialize", 1, {"protocolVersion": PROTOCOL}))
            init = peer.wait_response(1)
            self.assertIsInstance(init, dict)
            peer.send(rpc_request("ping", 2))
            ping = peer.wait_response(2)
            self.assertIn("result", ping)
            peer.close()
            self.assertEqual(peer.process.returncode, 0)
            # Every stdout frame was valid JSON-RPC (the reader would have
            # crashed otherwise); stderr carries only adapter diagnostics, and
            # may legitimately be empty.
            for line in peer.stderr.splitlines():
                self.assertTrue(
                    line.startswith("[streamable-http-stdio] "),
                    f"unexpected stderr line: {line!r}",
                )
        finally:
            server.stop()


if __name__ == "__main__":
    unittest.main(verbosity=2)
