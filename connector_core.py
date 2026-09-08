#!/usr/bin/env python3
"""Minimal frozen supervisor for the persistent per-target stdio connector.

The dynamic, reloadable connector engine (``connector_engine.py``) owns every
JSON-RPC-aware recovery decision; this module is deliberately small and
message-agnostic.  It supervises one Agent-side ``connect`` process:

* opens the local node handshake (op + ``connectorVersion``, reading the node's
  ``coreVersion`` reply) and stays alive while the Agent's stdin remains open;
* relays raw MCP bytes in both directions while a node stream is healthy
  (business payloads are byte-preserved);
* reconnects after node/local stream loss with bounded backoff, replays only
  the cached initialize/initialized handshake bytes the engine exposes, absorbs
  the replayed initialize result so it is never forwarded twice, and never
  replays a business call;
* fails each pending business call exactly once with the concise
  Bridge-generated JSON-RPC errors the engine builds;
* exits only when the Agent closes stdin (after a bounded drain) or on a fatal
  stdout write failure.

Everything this module knows about MCP JSON-RPC is delegated to the engine
object it loaded (``engine.ENGINE_VERSION``, ``engine.make_recovery(...)`` and
the returned recovery's methods).  Standard library only; nothing here imports
the bridge runtime, so the module stays independently importable.
"""

from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import socket
import sys
import threading
import time
from collections import deque
from pathlib import Path
from typing import Any

_BUILTIN_ENGINE_FILENAME = "connector_engine.py"
_ENGINE_ENV = "WIN_WSL_MCP_BRIDGE_CONNECTOR_ENGINE"
_ENGINE_DIR_ENV = "WIN_WSL_MCP_BRIDGE_CONNECTOR_ENGINE_DIR"
_ENGINE_MANIFEST_SUFFIX = ".engine.json"
_MAX_ENGINE_CANDIDATES = 32
_MAX_ENGINE_MODULE_BYTES = 2 * 1024 * 1024
_MAX_ENGINE_MANIFEST_BYTES = 64 * 1024

READ_BUFFER = 65536
CONNECT_TIMEOUT = 10.0
SESSION_TIMEOUT = 20.0  # socket timeout once a session is open (reads + sends)
DRAIN_TIMEOUT = 30.0    # bounded drain of the reader after Agent stdin EOF
ABSORB_TIMEOUT = 20.0   # max wait for the replayed initialize result
RECONNECT_MIN_DELAY = 0.2
RECONNECT_MAX_DELAY = 2.0
MAX_QUEUED_BYTES = 4 * 1024 * 1024  # bounded Agent-input queue while reconnecting
MAX_LINE_BYTES = 32 * 1024 * 1024   # generous line bound (stdin scanner safety)
RECV_LINE_LIMIT = 8 * 1024 * 1024   # bound for the one-line node handshake reply

CONNECTOR_VERSION_MAX = 64


class ConnectorError(Exception):
    """Fatal connector-level error (no MCP bytes were or will be written)."""


def _json_bytes(value: object) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, separators=(",", ":"), sort_keys=True
    ).encode("utf-8")


def _recv_line(sock: socket.socket, limit: int = RECV_LINE_LIMIT) -> bytes:
    data = bytearray()
    while len(data) <= limit:
        chunk = sock.recv(1)
        if not chunk:
            raise ConnectorError("node closed before the connect reply")
        data.extend(chunk)
        if chunk == b"\n":
            return bytes(data)
    raise ConnectorError("node connect reply exceeds the size limit")


def _plausible_version(value: object) -> bool:
    """Tiny sanity check for a simple dotted version string (no ordering)."""
    if not isinstance(value, str) or not value or len(value) > CONNECTOR_VERSION_MAX:
        return False
    parts = value.split(".")
    if len(parts) > 4:
        return False
    return all(part.isdigit() and part for part in parts)


# --------------------------------------------------------------------------
# Engine loading (dynamic, reloadable; never statically imported)
#
# Selection order:
#   1. an explicit engine module file (``engine_path`` argument or the
#      ``WIN_WSL_MCP_BRIDGE_CONNECTOR_ENGINE`` environment variable) -- an
#      Operator-chosen file that fails closed when it cannot load;
#   2. an engine directory (``engine_dir`` argument or the
#      ``WIN_WSL_MCP_BRIDGE_CONNECTOR_ENGINE_DIR`` environment variable) --
#      scanned for immutable ``*.engine.json`` manifest bundles whose module
#      bytes are SHA-256 verified before import; the newest verified version is
#      selected and a candidate that fails digest, containment, import, or
#      manifest self-consistency checks is skipped, rolling back to the next
#      candidate and finally to the built-in engine;
#   3. the built-in ``connector_engine.py`` next to this module.
#
# Every module is imported under a unique, immutable name so reloads can never
# collide in ``sys.modules`` regardless of how many engine versions exist on
# disk.
# --------------------------------------------------------------------------


def _import_engine_file(resolved: Path, *, builtin: bool) -> Any:
    digest = hashlib.sha1(str(resolved).encode("utf-8")).hexdigest()[:12]
    module_name = (
        "connector_engine_builtin" if builtin else f"connector_engine_dynamic_{digest}"
    )
    try:
        spec = importlib.util.spec_from_file_location(module_name, resolved)
        if spec is None or spec.loader is None:
            raise ConnectorError(f"cannot create an import spec for {resolved}")
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
    except ConnectorError:
        raise
    except Exception as exc:  # engine code must not break connector startup
        raise ConnectorError(f"connector engine failed to load: {exc}") from exc
    return module


def _validate_engine_module(module: Any, label: str) -> None:
    name = getattr(module, "ENGINE_NAME", None)
    version = getattr(module, "ENGINE_VERSION", None)
    recovery_class = getattr(module, "Recovery", None)
    make_recovery = getattr(module, "make_recovery", None)
    if not isinstance(name, str) or not name:
        raise ConnectorError(f"connector engine {label} has no ENGINE_NAME")
    if not _plausible_version(version):
        raise ConnectorError(f"connector engine {label} has an invalid ENGINE_VERSION")
    core_version = (
        getattr(recovery_class, "CORE_VERSION", None) if recovery_class else None
    )
    if not _plausible_version(core_version):
        raise ConnectorError(f"connector engine {label} has an invalid CORE_VERSION")
    if not callable(make_recovery):
        raise ConnectorError(f"connector engine {label} provides no make_recovery()")


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _version_tuple(value: str) -> tuple[int, int, int, int]:
    parts = value.split(".")
    while len(parts) < 4:
        parts.append("0")
    return tuple(int(part) for part in parts[:4])  # type: ignore[return-value]


def _scan_engine_dir(engine_dir: str | os.PathLike[str]) -> Any:
    """Load the newest verified engine from one scanned engine directory.

    Every candidate is an immutable engine bundle: a module file plus a sibling
    ``<name>.engine.json`` manifest declaring ``module`` (a ``.py`` file name
    inside the directory), ``version``, ``coreVersion``, and the SHA-256 of the
    module bytes.  Candidates are tried in descending version order; a
    candidate that fails any verification step is skipped.  When no candidate
    loads, the built-in engine module is returned so connector startup never
    depends on an Operator-supplied file.
    """
    try:
        resolved_dir = Path(engine_dir).resolve()
    except OSError:
        return _import_engine_file(
            Path(__file__).resolve().with_name(_BUILTIN_ENGINE_FILENAME), builtin=True
        )
    if not resolved_dir.is_dir():
        return _import_engine_file(
            Path(__file__).resolve().with_name(_BUILTIN_ENGINE_FILENAME), builtin=True
        )
    manifests: list[tuple[tuple[int, int, int, int], str, dict[str, Any]]] = []
    try:
        candidates = sorted(resolved_dir.iterdir())
    except OSError:
        return _import_engine_file(
            Path(__file__).resolve().with_name(_BUILTIN_ENGINE_FILENAME), builtin=True
        )
    seen = 0
    for candidate in candidates:
        if not candidate.is_file() or not candidate.name.endswith(
            _ENGINE_MANIFEST_SUFFIX
        ):
            continue
        seen += 1
        if seen > _MAX_ENGINE_CANDIDATES:
            break
        try:
            raw_manifest = candidate.read_bytes()
        except OSError:
            continue
        if len(raw_manifest) > _MAX_ENGINE_MANIFEST_BYTES:
            continue
        try:
            manifest = json.loads(raw_manifest)
        except ValueError:
            continue
        if not isinstance(manifest, dict):
            continue
        version = manifest.get("version")
        core_version = manifest.get("coreVersion")
        if not _plausible_version(version) or not _plausible_version(core_version):
            continue
        manifests.append((_version_tuple(str(version)), candidate.name, manifest))
    # Newest version first; ties break by manifest file name.
    manifests.sort(key=lambda item: (item[0], item[1]), reverse=True)

    for _version, manifest_name, manifest in manifests:
        module_name = manifest.get("module")
        sha256 = manifest.get("sha256")
        if not isinstance(module_name, str) or not module_name.endswith(".py"):
            continue
        module_file = resolved_dir / module_name
        try:
            module_resolved = module_file.resolve()
        except OSError:
            continue
        if module_resolved.parent != resolved_dir or not module_resolved.is_file():
            continue  # modules must live directly inside the engine directory
        try:
            module_bytes = module_resolved.read_bytes()
        except OSError:
            continue
        if len(module_bytes) > _MAX_ENGINE_MODULE_BYTES:
            continue
        if not isinstance(sha256, str) or len(sha256) != 64 or not all(
            character in "0123456789abcdef" for character in sha256
        ):
            continue
        if _sha256_bytes(module_bytes) != sha256:
            continue  # module digest does not match its immutable manifest
        try:
            module = _import_engine_file(module_resolved, builtin=False)
        except ConnectorError:
            continue
        try:
            _validate_engine_module(module, module_resolved.name)
        except ConnectorError:
            continue
        recovery_class = getattr(module, "Recovery", None)
        if str(getattr(module, "ENGINE_VERSION", None)) != str(
            manifest.get("version")
        ) or str(getattr(recovery_class, "CORE_VERSION", None)) != str(
            manifest.get("coreVersion")
        ):
            continue  # module self-declared versions disagree with the manifest
        return module
    return _import_engine_file(
        Path(__file__).resolve().with_name(_BUILTIN_ENGINE_FILENAME), builtin=True
    )


def load_engine(
    engine_path: str | os.PathLike[str] | None = None,
    engine_dir: str | os.PathLike[str] | None = None,
) -> Any:
    """Load and validate the connector engine module.

    Selection order: an explicit ``engine_path`` (argument or the
    ``WIN_WSL_MCP_BRIDGE_CONNECTOR_ENGINE`` environment variable) fails closed;
    otherwise ``engine_dir`` (argument or the
    ``WIN_WSL_MCP_BRIDGE_CONNECTOR_ENGINE_DIR`` environment variable) is
    scanned for immutable manifest+hash engine bundles (newest verified version
    first, rolling back to the built-in engine when none verifies); otherwise
    the built-in ``connector_engine.py`` next to this module is used.  Every
    module is imported under a unique, immutable name so reloads can never
    collide in ``sys.modules`` regardless of how many engine versions exist on
    disk.  Returns the module itself; the core only relies on its documented
    surface (``ENGINE_NAME``, ``ENGINE_VERSION``, ``Recovery`` and
    ``make_recovery(core_version)``).
    """
    if engine_path is None:
        env_value = os.environ.get(_ENGINE_ENV)
        if env_value:
            engine_path = env_value
    if engine_path is not None:
        path = Path(engine_path)
        try:
            resolved = path.resolve()
        except OSError as exc:
            raise ConnectorError(f"connector engine is not readable: {exc}") from exc
        if not resolved.is_file():
            raise ConnectorError(f"connector engine module not found: {resolved}")
        module = _import_engine_file(resolved, builtin=False)
        _validate_engine_module(module, resolved.name)
        return module
    if engine_dir is None:
        engine_dir = os.environ.get(_ENGINE_DIR_ENV)
    if engine_dir is not None:
        return _scan_engine_dir(engine_dir)
    return _import_engine_file(
        Path(__file__).resolve().with_name(_BUILTIN_ENGINE_FILENAME), builtin=True
    )


# --------------------------------------------------------------------------
# Persistent connector (minimal message-agnostic supervisor)
# --------------------------------------------------------------------------


class PersistentConnector:
    """Supervised raw relay that never exits while Agent stdin is open."""

    def __init__(
        self,
        local_host: str,
        local_port: int,
        target: str,
        *,
        engine: object,
        artifact_inbox: str | None = None,
        compatibility_http: bool = False,
        on_stream_id: object | None = None,
        stdin_file: object | None = None,
        stdout_file: object | None = None,
    ) -> None:
        self.local_host = local_host
        self.local_port = local_port
        self.target = target
        self.engine = engine
        self.artifact_inbox = artifact_inbox
        self.compatibility_http = compatibility_http
        self.on_stream_id = on_stream_id
        self.stdin_file = stdin_file if stdin_file is not None else sys.stdin
        self.stdout_file = stdout_file if stdout_file is not None else sys.stdout.buffer

        self.recovery = engine.make_recovery(None)  # type: ignore[attr-defined]
        self._engine_dir = os.environ.get(_ENGINE_DIR_ENV)
        self._explicit_engine = bool(os.environ.get(_ENGINE_ENV))

        self._lock = threading.RLock()
        self._stdout_lock = threading.Lock()
        self._sock: socket.socket | None = None
        self._session_up = False
        self._shutdown = False
        self._eof_close = False  # Agent stdin EOF: intentional drain, then exit
        self._fatal: str | None = None
        self._last_open_error: str | None = None
        self._ever_established = False

        # Bounded Agent-input queue while the session is down (raw bytes only).
        self._queued: deque[bytes] = deque()
        self._queued_bytes = 0

        self._lost = threading.Event()
        self._ready = threading.Event()
        self._input_done = threading.Event()
        self._reader: threading.Thread | None = None

    # -- diagnostics --------------------------------------------------------

    def _diag(self, message: str) -> None:
        try:
            print(f"bridge proxy: {message}", file=sys.stderr)
            sys.stderr.flush()
        except OSError:
            pass

    # -- stdout -------------------------------------------------------------

    def _write_stdout(self, data: bytes) -> bool:
        try:
            with self._stdout_lock:
                self.stdout_file.write(data)
                self.stdout_file.flush()
            return True
        except OSError as exc:
            with self._lock:
                if self._fatal is None:
                    self._fatal = f"stdout write failed: {exc}"
                self._shutdown = True
            return False

    def _write_payloads(self, payloads: list[bytes]) -> None:
        for payload in payloads:
            if not self._write_stdout(payload):
                return

    # -- engine handover -----------------------------------------------------

    def _maybe_reload_engine(self) -> bool:
        """Adopt a newer verified engine at a quiescent session boundary."""
        if self._explicit_engine or not self._engine_dir:
            return False
        try:
            candidate = load_engine(engine_dir=self._engine_dir)
            identity = (
                candidate.ENGINE_NAME,
                candidate.ENGINE_VERSION,
                candidate.Recovery.CORE_VERSION,
            )
            current = (
                self.engine.ENGINE_NAME,
                self.engine.ENGINE_VERSION,
                self.engine.Recovery.CORE_VERSION,
            )
            if identity == current or not self.recovery.quiescent():
                return False
            replacement = candidate.make_recovery(self.recovery.core_version)
            replacement.import_handshake(self.recovery.export_handshake())
        except Exception:
            return False  # verified current engine remains the rollback generation
        self.engine = candidate
        self.recovery = replacement
        return True

    # -- session open / handshake -------------------------------------------

    def _open_session(self) -> socket.socket:
        request: dict[str, object] = {
            "op": "connect-http" if self.compatibility_http else "connect",
            "target": self.target,
            "connectorVersion": self.engine.ENGINE_VERSION,  # type: ignore[attr-defined]
        }
        if self.artifact_inbox:
            request["artifactInbox"] = self.artifact_inbox
        sock = socket.create_connection(
            (self.local_host, self.local_port), timeout=CONNECT_TIMEOUT
        )
        try:
            sock.settimeout(CONNECT_TIMEOUT)
            sock.sendall(_json_bytes(request) + b"\n")
            reply = json.loads(_recv_line(sock))
        except Exception as exc:
            sock.close()
            if isinstance(exc, ConnectorError):
                raise
            raise ConnectorError(str(exc)) from exc
        if not reply.get("ok"):
            message = str(reply.get("message", "bridge open failed"))
            sock.close()
            raise ConnectorError(message)
        if self.on_stream_id is not None:
            stream_id = reply.get("stream")
            self.on_stream_id(str(stream_id) if isinstance(stream_id, str) else "")
        self.recovery.set_core(reply.get("coreVersion"))
        sock.settimeout(SESSION_TIMEOUT)
        return sock

    # -- Agent (client) -> node ---------------------------------------------

    @staticmethod
    def _send_raw(sock: socket.socket, raw: bytes) -> bool:
        try:
            sock.sendall(raw)
            return True
        except (OSError, socket.timeout):
            return False

    def _publish_agent_line(self, raw: bytes) -> None:
        role = self.recovery.handle_agent(raw)
        with self._lock:
            sock = self._sock
            up = self._session_up
        if up and sock is not None:
            if self._send_raw(sock, raw):
                if role.kind in ("request", "initialize"):
                    with self._lock:
                        current = self._sock is sock and self._session_up
                    self.recovery.remember_sent(role.id, role.method, current=current)
            elif role.kind in ("request", "initialize"):
                # Send failed: the id (if any) becomes uncertain and the loss
                # handler fails it once if no response ever arrives.
                self.recovery.remember_sent(role.id, role.method, current=False)
            return
        # Session down: retain only the MCP handshake needed to reconstruct the
        # session. A new business request is failed immediately and is never
        # delayed for execution under a later node generation.
        if role.kind == "initialize":
            with self._lock:
                self._queued.append(raw)
                self._queued_bytes += len(raw)
            return
        if role.kind == "request":
            self._write_payloads(
                self.recovery.fail_once(
                    [(role.id, role.method)],
                    self.recovery.UNAVAILABLE_REASON,
                    outcome_unknown=False,
                )
            )
        # notifications/responses for the lost session are not replayed.

    # -- Node -> Agent (client) ----------------------------------------------

    def _session_loop(self, sock: socket.socket, absorb: bool) -> None:
        buffer = bytearray()
        absorb_out = bytearray()
        absorb_state = [absorb]
        absorb_started = time.monotonic()
        while not self._shutdown:
            try:
                chunk = sock.recv(READ_BUFFER)
            except socket.timeout:
                # Idle sessions are legal; only a closed socket ends the session.
                if absorb_state[0] and time.monotonic() - absorb_started > ABSORB_TIMEOUT:
                    break  # replayed initialize never answered: session failed
                with self._lock:
                    if self._sock is not sock:
                        return
                continue
            except OSError:
                break
            if not chunk:
                break
            buffer.extend(chunk)
            while True:
                index = buffer.find(b"\n")
                if index < 0:
                    break
                line = bytes(buffer[: index + 1])
                del buffer[: index + 1]
                if len(line) <= MAX_LINE_BYTES:
                    self._handle_node_line(sock, line, absorb_state, absorb_out)
        # Drain complete lines that already arrived before the close so late
        # responses are delivered and pending bookkeeping is cleared.
        if not absorb_state[0]:
            while True:
                index = buffer.find(b"\n")
                if index < 0:
                    break
                line = bytes(buffer[: index + 1])
                del buffer[: index + 1]
                if len(line) <= MAX_LINE_BYTES:
                    self._handle_node_line(sock, line, absorb_state, absorb_out)
        self._on_session_lost(sock)

    def _handle_node_line(
        self, sock: socket.socket, raw: bytes, absorb_state: list[bool], absorb_out: bytearray
    ) -> None:
        if absorb_state[0]:
            decision = self.recovery.handle_node(raw, absorbing=True)
            if decision.kind == "absorb_done":
                absorb_state[0] = False
                # Flush lines that arrived before the replay's init result, then
                # send the cached initialized notification and open the session.
                if absorb_out:
                    self._write_stdout(bytes(absorb_out))
                    absorb_out.clear()
                initialized = self.recovery.cached_initialized()
                if initialized is not None:
                    self._send_raw(sock, initialized)
                self._ready.set()
            else:
                absorb_out.extend(raw)
            return
        decision = self.recovery.handle_node(raw, absorbing=False)
        if decision.kind == "agent" and decision.payload is not None:
            self._write_stdout(decision.payload)

    def _on_session_lost(self, sock: socket.socket) -> None:
        with self._lock:
            if self._sock is sock:
                self._sock = None
            self._session_up = False
            graceful = self._eof_close or self._shutdown
            try:
                sock.close()
            except OSError:
                pass
        if not graceful:
            lost = self.recovery.lost_requests()
            if lost:
                payloads = self.recovery.fail_once(lost, self.recovery.LOST_REASON)
                self._write_payloads(payloads)
            self._lost.set()

    # -- Agent stdin writer --------------------------------------------------

    def _stdin_loop(self) -> None:
        partial = bytearray()
        try:
            fd = self.stdin_file.fileno()
        except (OSError, AttributeError):
            self._input_done.set()
            return
        while True:
            try:
                data = os.read(fd, READ_BUFFER)
            except OSError:
                break
            if not data:
                break
            partial.extend(data)
            while True:
                index = partial.find(b"\n")
                if index < 0:
                    break
                raw = bytes(partial[: index + 1])
                del partial[: index + 1]
                if len(raw) <= MAX_LINE_BYTES:
                    self._publish_agent_line(raw)
        self._input_done.set()

    # -- reconnect ------------------------------------------------------------

    def _establish(self) -> bool:
        """Open + handshake + replay + absorb + drain queue. True when up."""
        self._maybe_reload_engine()
        sock: socket.socket | None = None
        try:
            sock = self._open_session()
        except ConnectorError as exc:
            self._last_open_error = str(exc)
            return False  # silent: reconnection is best-effort while stdin open
        replay = self.recovery.replay_available()
        if replay:
            cached = self.recovery.cached_initialize()
            if cached is None or not self._send_raw(sock, cached):
                try:
                    sock.close()
                except OSError:
                    pass
                return False
        with self._lock:
            self._sock = sock
        self._ready.clear()
        self._lost.clear()
        reader = threading.Thread(
            target=self._session_loop, args=(sock, replay), daemon=True
        )
        self._reader = reader
        reader.start()
        if not replay:
            self._ready.set()
        while not self._ready.is_set() and not self._lost.is_set():
            if self._input_done.is_set() or self._shutdown:
                with self._lock:
                    if self._sock is sock:
                        self._sock = None
                try:
                    sock.close()
                except OSError:
                    pass
                return False
            time.sleep(0.05)
        if self._lost.is_set():
            return False
        # Drain the bounded inbound queue, then mark the session live.
        while True:
            with self._lock:
                item = self._queued.popleft() if self._queued else None
                if item is not None:
                    self._queued_bytes -= len(item)
            if item is None:
                break
            role = self.recovery.handle_agent(item)
            if not self._send_raw(sock, item):
                # The fresh session died mid-handover.  The popped item was
                # never delivered, so put it back in front of the queue (never
                # drop an undelivered request silently) and let the reconnect
                # loop retry it on the next session; queue overflow still
                # fails requests explicitly with a Bridge error.
                with self._lock:
                    self._queued.appendleft(item)
                    self._queued_bytes += len(item)
                    if self._sock is sock:
                        self._sock = None
                try:
                    sock.close()
                except OSError:
                    pass
                return False
            if role.kind in ("request", "initialize"):
                self.recovery.remember_sent(role.id, role.method, current=True)
        with self._lock:
            self._session_up = True
        self._ever_established = True
        self._last_open_error = None
        return True

    # -- main lifecycle -------------------------------------------------------

    def run(self) -> int:
        # The Agent owns this process through stdin. Even if the node is absent
        # at startup, keep the stdio endpoint alive and retry with capped delay;
        # only deterministic CLI validation or engine-load failure happens
        # before this point.
        # Preserve ordinary startup ordering: establish once before consuming
        # Agent requests so an immediately-following initialize/tools/list batch
        # reaches the first session. If the node is absent, start consuming only
        # to observe stdin EOF and retry forever without exiting.
        initially_up = self._establish()
        writer = threading.Thread(target=self._stdin_loop, daemon=True)
        writer.start()
        if not initially_up:
            self._reconnect_until_up()

        next_engine_scan = time.monotonic() + 1.0
        while not self._shutdown:
            if self._input_done.is_set():
                break
            if time.monotonic() >= next_engine_scan:
                next_engine_scan = time.monotonic() + 1.0
                if self._maybe_reload_engine():
                    # A verified quiescent handover uses a fresh node session so
                    # its connectorVersion handshake matches the new engine.
                    self._close_current()
                    self._lost.set()
            if self._lost.is_set():
                self._lost.clear()
                self._reconnect_until_up()
                continue
            self._lost.wait(0.1)

        # Agent stdin EOF (or shutdown): graceful drain, then exit.
        if self._shutdown:
            self._close_current()
            return 1 if self._fatal else 0
        self._eof_close = True
        with self._lock:
            self._session_up = False
        sock = self._sock
        if sock is not None:
            try:
                sock.shutdown(socket.SHUT_WR)
            except OSError:
                pass
            # Bounded drain so the final downstream responses reach stdout.
            reader = self._reader
            if reader is not None:
                deadline = time.monotonic() + DRAIN_TIMEOUT
                while reader.is_alive() and time.monotonic() < deadline:
                    time.sleep(0.05)
        self._close_current()
        if self._fatal:
            return 1
        # With no Agent environment left to preserve, retain the historical
        # startup diagnostic/exit contract for deterministic open refusal.
        if not self._ever_established and self._last_open_error:
            self._diag(self._last_open_error)
            return 1
        return 0

    def _reconnect_until_up(self) -> None:
        delay = RECONNECT_MIN_DELAY
        while not self._input_done.is_set() and not self._shutdown:
            if self._establish():
                return
            if self._input_done.is_set() or self._shutdown:
                return
            time.sleep(delay)
            delay = min(delay * 2, RECONNECT_MAX_DELAY)

    def _close_current(self) -> None:
        with self._lock:
            sock = self._sock
            self._sock = None
            self._session_up = False
        if sock is not None:
            try:
                sock.close()
            except OSError:
                pass


# --------------------------------------------------------------------------
# Entry point used by the bridge runtime `connect` / `connect-http` commands.
# --------------------------------------------------------------------------


def run_connector(
    local_host: str,
    local_port: int,
    target: str,
    *,
    engine: object | None = None,
    artifact_inbox: str | None = None,
    compatibility_http: bool = False,
    on_stream_id: object | None = None,
    stdin_file: object | None = None,
    stdout_file: object | None = None,
) -> int:
    """Run one persistent connector session; returns the process exit code."""
    if engine is None:
        try:
            engine = load_engine()
        except ConnectorError as exc:
            print(f"bridge proxy: {exc}", file=sys.stderr)
            return 1
    connector = PersistentConnector(
        local_host,
        local_port,
        target,
        engine=engine,
        artifact_inbox=artifact_inbox,
        compatibility_http=compatibility_http,
        on_stream_id=on_stream_id,
        stdin_file=stdin_file,
        stdout_file=stdout_file,
    )
    return connector.run()
