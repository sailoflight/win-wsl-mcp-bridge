#!/usr/bin/env python3
"""Standalone reverse-era projection: a LEGACY Agent's stdio MCP client is
bridged to a MODERN-only (2026-07-28) stdio backend that speaks per-request
``_meta`` metadata and ``server/discover`` and never runs a legacy
``initialize`` handshake.

This is the P10 reverse-era projection: the mirror image of the modern-facing
tools-only surface in ``bridge_runtime``.  It covers only the tools
intersection and is deliberately NOT a raw protocol stack:

* The legacy-facing side speaks the frozen legacy profiles ``2024-11-05``,
  ``2025-03-26``, ``2025-06-18`` and ``2025-11-25`` exactly as the official
  schemas define them (instructions/listChanged/cancelled/progress/pagination
  exist in every profile; ``Tool.annotations`` >= 2025-03-26;
  ``structuredContent``, ``Tool.outputSchema`` and ``Tool.title`` >=
  2025-06-18; ``Tool.icons`` (plural: a list of ``Icon`` objects each carrying
  a ``src``) >= 2025-11-25).  A negotiated revision never leaks a field or
  capability unavailable in that revision.
* The backend-facing side speaks exactly one modern revision (2026-07-28):
  every request carries the required ``_meta`` protocolVersion and
  clientCapabilities, ``server/discover`` bootstraps the backend once, and
  the required modern result envelope (``resultType``, ``ttlMs``,
  ``cacheScope``, ``_meta.serverInfo``) is stripped before a result is shaped
  back to the negotiated legacy profile.
* There is no modern->legacy auto-fallback, no invented optional capability
  family, and no hidden conversion: a modern backend that does not support the
  single modern revision we speak is reported as a clean session-unavailable
  failure, never silently downgraded.
* Requests are serialized one-at-a-time to the backend (one in flight), like
  the shared-backend projection.  Legacy ``notifications/cancelled`` and
  progress tokens are forwarded onto the mapped in-flight backend request, and
  backend ``notifications/progress`` are relayed back with the token
  unchanged.  A cancellation for a request that is still *queued* purges it
  from the queue so it is never executed at all (a request whose cancellation
  races with dispatch start is dropped before any backend bytes are written).
  Cancellation state exists only while a request is live (queued / dispatch-
  pending / in-flight): cancels for unknown or already-completed ids and for
  non-scalar ``requestId`` values (dict/list) are ignored, so markers stay
  bounded and a future reuse of the same numeric id is never poisoned.
* Business calls are never replayed: a transport failure or a deadline after a
  ``tools/call`` was written surfaces ``outcomeUnknown`` (true only when the
  call bytes were actually sent to the backend) with retryable=true, and the
  adapter never resends it.  ``server/discover`` startup and every backend
  request are bounded by operator-set timeouts; a deadline unblocks the
  exchange and tears the backend process down so no late duplicate side
  effect can follow.
* The backend's stderr is drained onto a bounded diagnostics ring (last 64
  KiB, discarded beyond that), so a noisy backend can never deadlock the
  adapter's pipe; the ring is never forwarded to the legacy client.
* Every stream is read with bounded chunk reads (never ``readline``): backend
  stderr is chunked straight onto the ring, and backend stdout / legacy stdin
  frames longer than the configured limit are refused whole -- never truncated
  and forwarded as a business call.  The pending legacy request queue is
  bounded; an overflowing client receives a structured JSON-RPC refusal, so a
  flood cannot grow adapter memory without bound.
* Closing the legacy stdin stops the server: queued work is dropped, an
  in-flight exchange is abandoned at the next checkpoint, and the owned
  backend process is stopped before the adapter exits.

Explicit opt-in only: this program is a stdio server that is launched by a
local CLI / operator and is told the backend launch command on its own argv.
It binds no listener and never accepts a remote command/args/env.  It is
standard-library only and imports from ``bridge_protocol`` only its pure
schema constants plus the frozen legacy content-variant/_meta gates
(``project_legacy_content_item``), which it shares with the runtime projection
so both directions apply identical per-profile content acceptance; stdout
stays protocol-clean newline-delimited JSON.

Usage (typical operator input, e.g. an Agent MCP config entry):

    legacy_modern_stdio.py --backend-command <executable> \
        [--startup-timeout-seconds <seconds>] \
        [--request-timeout-seconds <seconds>] \
        [--backend-cwd <dir>] --backend-args <arg> ...

The legacy Agent spawns this module as its stdio MCP server.  The modern-only
backend is itself spawned on first legacy ``initialize`` with the operator's
command/args/cwd (inheriting the operator's environment) and is kept for the
lifetime of this process.
"""

from __future__ import annotations

import argparse
import calendar
import json
import re
import subprocess
import sys
import threading
import time
from typing import Any, Deque, Iterator, Optional
from collections import deque

# Pure schema constants and the frozen legacy content gates from the modern
# protocol module.  The content-variant/_meta gates (project_legacy_content_item)
# are shared with the runtime's own legacy projection so both directions apply
# the same per-profile content-kind acceptance; they are read-only imports.
from bridge_protocol import (
    META_CLIENT_CAPABILITIES_KEY,
    META_CLIENT_INFO_KEY,
    META_PROGRESS_TOKEN_KEY,
    META_PROTOCOL_VERSION_KEY,
    META_SERVER_INFO_KEY,
    MODERN_MCP_PROTOCOL_VERSION,
    RESULT_TYPE_COMPLETE,
    RESULT_TYPE_INPUT_REQUIRED,
    RETIRED_MCP_ERROR_CODES,
    project_legacy_content_item,
)

ADAPTER_NAME = "legacy-modern-stdio"
ADAPTER_VERSION = "0.1.0"
ADAPTER_SERVER_INFO = {"name": ADAPTER_NAME, "version": ADAPTER_VERSION}

#: Bounded backend-stderr diagnostics ring (bytes kept, oldest discarded).
_STDERR_RING_MAX_BYTES = 65536

#: Legacy revisions this adapter serves, oldest first.  Dates compare
#: lexicographically, which equals chronological order for this fixed set.
LEGACY_PROFILES = ("2024-11-05", "2025-03-26", "2025-06-18", "2025-11-25")
LEGACY_CANONICAL = LEGACY_PROFILES[-1]  # 2025-11-25: newest verified legacy.

#: JSON-RPC / MCP error codes reused on the legacy-facing side.
CODE_PARSE_ERROR = -32700
CODE_INVALID_REQUEST = -32600
CODE_METHOD_NOT_FOUND = -32601
CODE_INVALID_PARAMS = -32602
CODE_INTERNAL_ERROR = -32603
#: Implementation-defined server error: incoming request queue at capacity.
CODE_SERVER_OVERLOAD = -32000
#: Modern-era codes that a legacy client must never receive verbatim.
MCP_UNSUPPORTED_PROTOCOL_VERSION = -32022

#: Bounded I/O.  ``_IO_CHUNK_BYTES`` is the raw read size for every stream;
#: frames longer than ``_MAX_FRAME_BYTES`` (configurable) are refused whole
#: (never truncated and forwarded).  Defaults live in ``main()``.
_IO_CHUNK_BYTES = 16384
_DEFAULT_MAX_FRAME_BYTES = 16 * 1024 * 1024
_DEFAULT_QUEUE_CAPACITY = 256


def _read_chunk(stream: Any) -> bytes:
    """Read one bounded chunk without ever waiting for a full buffer.

    ``BufferedReader.read(n)`` on a pipe blocks until ``n`` bytes arrive (or
    EOF), which would stall on any short frame; ``read1(n)`` returns with the
    first available data after at most one raw read.  Text fallback (no
    ``.buffer``) uses a plain chunked read.
    """
    reader = getattr(stream, "read1", None)
    if reader is not None:
        chunk = reader(_IO_CHUNK_BYTES)
    else:
        chunk = stream.read(_IO_CHUNK_BYTES)
    return chunk

_VERSION_RE = re.compile(r"^(\d{4})-(\d{2})-(\d{2})$")

# --------------------------------------------------------------------------
# Frozen legacy field profiles
# --------------------------------------------------------------------------


def _well_formed_legacy_version(version: str) -> bool:
    match = _VERSION_RE.match(version)
    if not match:
        return False
    year, month, day = (int(part) for part in match.groups())
    if not 1 <= month <= 12:
        return False
    return 1 <= day <= calendar.monthrange(year, month)[1]


def _ge(version: str, anchor: str) -> bool:
    return version >= anchor


def _json_scalar_request_id(request_id: Any) -> bool:
    """True for a JSON-RPC scalar request id (string/number) that a legacy
    client could legitimately cancel.

    Objects/arrays (dict/list), booleans, missing (None), and other exotic
    values are never valid cancellation targets: they cannot match a queued or
    in-flight request, so they are ignored outright and are never hashed or
    stored (a dict/list requestId must not raise TypeError on the reader).
    """
    return isinstance(request_id, (str, int, float)) and not isinstance(
        request_id, bool
    )


def negotiate_legacy_version(requested: Any) -> tuple[str, str]:
    """Map a legacy client's requested protocol version to a negotiated one.

    Returns ``("ok", negotiated)`` when the request is one of the frozen
    profiles, ``("downgrade", LEGACY_CANONICAL)`` for a well-formed newer
    unverified revision (per the MCP versioning rule a server never claims a
    revision it does not implement and answers with the newest verified
    revision not exceeding the request), and ``("reject", reason)`` for an
    unrecognised, malformed, or older-than-supported revision.
    """
    if isinstance(requested, str) and requested in LEGACY_PROFILES:
        return "ok", requested
    if isinstance(requested, str) and _well_formed_legacy_version(requested):
        if requested > LEGACY_CANONICAL:
            return "downgrade", LEGACY_CANONICAL
        return "reject", f"unsupported legacy protocol version {requested!r}"
    return "reject", f"unrecognised legacy protocol version {requested!r}"


class _LegacyProfile:
    """Field gates for one negotiated legacy revision (tools-only surface)."""

    def __init__(self, version: str) -> None:
        self.version = version
        #: Present in every legacy profile (schema since 2024-11-05).
        self.annotations = _ge(version, "2025-03-26")
        self.structured_content = _ge(version, "2025-06-18")
        self.output_schema = _ge(version, "2025-06-18")
        self.title = _ge(version, "2025-06-18")
        #: Tool.icons (plural, list of Icon objects) was added 2025-11-25.
        self.icons = _ge(version, "2025-11-25")

    # Result-envelope / tool allow-lists -----------------------------------
    _TOOL_BASE_KEYS = ("name", "description", "inputSchema")

    def project_tool(self, tool: Any) -> Optional[dict[str, Any]]:
        """Project one modern tool entry onto this legacy profile.

        Modern-only and future-era keys (e.g. ``execution``, tool-level
        ``_meta``, unknown extension keys) are withheld.  Returns None for a
        malformed entry so a broken catalog fails cleanly instead of leaking.
        """
        if not isinstance(tool, dict) or not isinstance(tool.get("name"), str):
            return None
        out: dict[str, Any] = {"name": tool["name"]}
        if isinstance(tool.get("description"), str):
            out["description"] = tool["description"]
        if isinstance(tool.get("inputSchema"), dict):
            out["inputSchema"] = tool["inputSchema"]
        if self.annotations and isinstance(tool.get("annotations"), dict):
            out["annotations"] = tool["annotations"]
        if self.output_schema and isinstance(tool.get("outputSchema"), dict):
            out["outputSchema"] = tool["outputSchema"]
        if self.title and isinstance(tool.get("title"), str):
            out["title"] = tool["title"]
        # Tool.icons is a list of Icon objects (schema 2025-11-25+); forwarded
        # verbatim when the negotiated profile can carry it.
        if self.icons and isinstance(tool.get("icons"), list):
            out["icons"] = tool["icons"]
        return out

    def project_tools_result(self, result: Any) -> Optional[dict[str, Any]]:
        """Strip the modern CacheableResult envelope from a tools/list result.

        Keeps only ``tools`` (projected per profile) and ``nextCursor`` (valid
        in every legacy profile).  Returns None when the backend list is
        structurally unusable.
        """
        if not isinstance(result, dict) or not isinstance(result.get("tools"), list):
            return None
        tools: list[dict[str, Any]] = []
        for entry in result["tools"]:
            projected = self.project_tool(entry)
            if projected is None:
                return None
            tools.append(projected)
        out: dict[str, Any] = {"tools": tools}
        if isinstance(result.get("nextCursor"), str):
            out["nextCursor"] = result["nextCursor"]
        return out

    def project_call_result(self, result: Any) -> Optional[dict[str, Any]]:
        """Strip the modern result envelope from a tools/call result.

        Legacy CallToolResult is ``{content, structuredContent?, isError?}``.
        ``structuredContent`` is withheld from profiles older than 2025-06-18.
        Every content item is projected through the shared frozen content
        gates (``bridge_protocol.project_legacy_content_item``): kinds a
        profile cannot represent (audio before 2025-03-26, resource_link
        before 2025-06-18, or any unknown kind) and item/resource ``_meta``
        fields absent from the profile are handled there.  An unrepresentable
        item makes the WHOLE result unprojectable (None), which the caller
        surfaces as an explicit error -- business output is never silently
        dropped and a success is never fabricated.  An ``input_required``
        modern result cannot be expressed on the legacy side and must be
        surfaced as an error by the caller, never silently converted; the
        caller checks ``resultType`` before calling here.
        """
        if not isinstance(result, dict) or not isinstance(result.get("content"), list):
            return None
        try:
            projected_items = [
                project_legacy_content_item(self.version, item)
                for item in result["content"]
            ]
        except ValueError:
            return None
        out: dict[str, Any] = {"content": projected_items}
        if self.structured_content and "structuredContent" in result:
            out["structuredContent"] = result["structuredContent"]
        if "isError" in result:
            out["isError"] = result["isError"]
        return out


# --------------------------------------------------------------------------
# JSON-RPC frame helpers (legacy side)
# --------------------------------------------------------------------------


def _legacy_result(request_id: Any, result: dict[str, Any]) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "id": request_id, "result": result}


def _legacy_error(
    request_id: Any,
    code: int,
    message: str,
    data: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    error: dict[str, Any] = {"code": code, "message": message}
    if data is not None:
        error["data"] = data
    return {"jsonrpc": "2.0", "id": request_id, "error": error}


def _session_error(
    request_id: Any,
    *,
    retryable: bool,
    outcome_unknown: bool,
    detail: str,
    code: int = CODE_INTERNAL_ERROR,
) -> dict[str, Any]:
    """Structured adapter error: the backend session was or became unusable."""
    return _legacy_error(
        request_id,
        code,
        "backend session unavailable",
        {
            "code": "backend_session_unavailable",
            "retryable": retryable,
            "outcomeUnknown": outcome_unknown,
            "detail": detail,
        },
    )


def _legacy_initialize_result(
    negotiated: str,
    *,
    tools_list_changed: bool,
    instructions: Optional[str],
) -> dict[str, Any]:
    capabilities: dict[str, Any] = {"tools": {"listChanged": tools_list_changed}}
    result: dict[str, Any] = {
        "protocolVersion": negotiated,
        "capabilities": capabilities,
        "serverInfo": dict(ADAPTER_SERVER_INFO),
    }
    # instructions exist in every frozen legacy profile (schema since
    # 2024-11-05) and are preserved from the backend verbatim.
    if instructions is not None:
        result["instructions"] = instructions
    return result


# --------------------------------------------------------------------------
# Modern request/notification construction toward the backend
# --------------------------------------------------------------------------


def _modern_meta(*, progress_token: Any = None) -> dict[str, Any]:
    meta: dict[str, Any] = {
        META_PROTOCOL_VERSION_KEY: MODERN_MCP_PROTOCOL_VERSION,
        META_CLIENT_CAPABILITIES_KEY: {},
        META_CLIENT_INFO_KEY: dict(ADAPTER_SERVER_INFO),
    }
    if progress_token is not None:
        # progressToken is an un-namespaced ``_meta`` key in this era.
        meta[META_PROGRESS_TOKEN_KEY] = progress_token
    return meta


def _modern_request(method: str, params: dict[str, Any], request_id: str) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "id": request_id, "method": method, "params": params}


def _modern_discover_request(request_id: str) -> dict[str, Any]:
    return _modern_request(
        "server/discover", {"_meta": _modern_meta()}, request_id
    )


def _modern_tools_list_request(
    request_id: str, cursor: Any, progress_token: Any
) -> dict[str, Any]:
    params: dict[str, Any] = {"_meta": _modern_meta(progress_token=progress_token)}
    if cursor is not None:
        params["cursor"] = cursor
    return _modern_request("tools/list", params, request_id)


def _modern_tools_call_request(
    request_id: str, name: Any, arguments: Any, progress_token: Any
) -> dict[str, Any]:
    params: dict[str, Any] = {"name": name, "_meta": _modern_meta(progress_token=progress_token)}
    if isinstance(arguments, dict):
        params["arguments"] = arguments
    elif arguments is not None:
        raise ValueError("tools/call arguments must be an object when present")
    return _modern_request("tools/call", params, request_id)


# --------------------------------------------------------------------------
# The serialized stdio server
# --------------------------------------------------------------------------


class LegacyModernServer:
    """Serve one legacy Agent over stdin/stdout, backed by one modern backend.

    Threading model (stdlib, portable):
      * a legacy reader thread decodes stdin lines and queues requests; it
        also handles ``notifications/cancelled``: a cancellation for a queued
        request purges it (that request is never executed) and a cancellation
        for the *in-flight* request is forwarded onto the mapped backend
        request id while the main thread is awaiting it;
      * a backend reader thread decodes backend stdout: responses wake the
        awaiting main thread; ``notifications/progress`` are relayed to the
        legacy client immediately with the token unchanged;
      * a backend stderr thread drains stderr onto a bounded ring so a noisy
        backend can never deadlock the adapter;
      * the main thread handles legacy requests strictly one at a time: each
        request that needs the backend performs exactly one backend exchange
        (write + await response id, bounded by a deadline), so backend
        requests are serialized.
    """

    def __init__(
        self,
        backend_command: str,
        backend_args: Optional[list[str]] = None,
        backend_cwd: Optional[str] = None,
        startup_timeout: float = 15.0,
        request_timeout: float = 60.0,
        max_frame_bytes: int = _DEFAULT_MAX_FRAME_BYTES,
        queue_capacity: int = _DEFAULT_QUEUE_CAPACITY,
    ) -> None:
        if not isinstance(backend_command, str) or not backend_command:
            raise ValueError("--backend-command must be a non-empty executable")
        for label, value in (
            ("startup_timeout", startup_timeout),
            ("request_timeout", request_timeout),
        ):
            if not isinstance(value, (int, float)) or value <= 0:
                raise ValueError(f"{label} must be a positive number")
        if not isinstance(max_frame_bytes, int) or max_frame_bytes <= 0:
            raise ValueError("max_frame_bytes must be a positive integer")
        if not isinstance(queue_capacity, int) or queue_capacity <= 0:
            raise ValueError("queue_capacity must be a positive integer")
        self._backend_command = backend_command
        self._backend_args = list(backend_args or [])
        self._backend_cwd = backend_cwd
        self._startup_timeout = float(startup_timeout)
        self._request_timeout = float(request_timeout)
        self._max_frame_bytes = max_frame_bytes
        self._queue_capacity = queue_capacity

        self._legacy_in = sys.stdin
        self._legacy_out = sys.stdout
        self._legacy_write_lock = threading.Lock()

        # Backend lifecycle / I/O.
        self._backend: Optional[subprocess.Popen] = None
        self._backend_io_lock = threading.Lock()  # guards backend process + writes
        self._backend_started = False
        self._backend_start_error: Optional[str] = None
        self._backend_dead = False

        # Discovery result observed from the backend (single bootstrap).
        self._discovery: Optional[dict[str, Any]] = None

        # Serialized request state.
        self._request_queue: Deque[dict[str, Any]] = deque()
        self._queue_condition = threading.Condition()
        self._backend_requests: dict[str, dict[str, Any]] = {}
        self._response_condition = threading.Condition()
        self._backend_seq = 0
        # In-flight mapping: legacy request id -> backend request id, plus
        # whether the business bytes for the in-flight request were written.
        self._inflight_legacy_id: Any = None
        self._inflight_backend_id: Optional[str] = None
        self._inflight_written = False
        self._state_lock = threading.Lock()

        self._legacy_reader: Optional[threading.Thread] = None
        self._backend_reader: Optional[threading.Thread] = None
        self._backend_stderr_reader: Optional[threading.Thread] = None
        self._shutdown = False
        self._client_closed = False
        # Negotiated session state (updated by each successful initialize).
        self._initialized = False
        self._tools_offered = False
        self._profile: Optional[_LegacyProfile] = None

        # Cancellation state, guarded by the queue condition.  Markers exist
        # ONLY while a request is live: queued (purged directly on cancel),
        # dequeued-but-not-yet-written (``_pending_legacy_id``; a marker makes
        # the dispatch-time check skip the write), or in-flight (forwarded to
        # the mapped backend id).  An unknown or already-completed id is
        # ignored, so markers stay bounded to pending work and a future reuse
        # of the same numeric id is never poisoned.  Equality-based list (no
        # hashing): a dict/list requestId can never raise TypeError.
        self._cancelled_legacy_ids: list[Any] = []
        self._pending_legacy_id: Any = None
        # Bounded backend-stderr diagnostics ring.
        self._stderr_ring: Deque[str] = deque()
        self._stderr_bytes = 0
        self._stderr_lock = threading.Lock()

    # -- process plumbing ------------------------------------------------

    def _write_legacy(self, message: dict[str, Any]) -> None:
        with self._legacy_write_lock:
            self._legacy_out.write(
                json.dumps(message, separators=(",", ":")) + "\n"
            )
            self._legacy_out.flush()

    def _write_backend(self, message: dict[str, Any]) -> bool:
        """Write one frame to the backend under the I/O lock.

        Returns False when the backend is not usable (never started, dead, or
        its stdin is closed) and nothing was written.
        """
        with self._backend_io_lock:
            backend = self._backend
            if backend is None or backend.poll() is not None or backend.stdin is None:
                return False
            try:
                backend.stdin.write(
                    json.dumps(message, separators=(",", ":"), ensure_ascii=True).encode(
                        "utf-8"
                    )
                    + b"\n"
                )
                backend.stdin.flush()
            except (BrokenPipeError, OSError, ValueError):
                self._mark_backend_dead_unlocked("backend stdin write failed")
                return False
            return True

    def _mark_backend_dead_unlocked(self, detail: str) -> None:
        if not self._backend_dead:
            self._backend_dead = True
            self._backend_start_error = detail
        with self._response_condition:
            self._response_condition.notify_all()

    def _ensure_backend_started(self) -> Optional[str]:
        """Spawn the backend subprocess exactly once on first demand.

        Returns None on success or an error string when startup failed (the
        process could not be launched).  Never retried automatically; a later
        legacy initialize may retry once by calling this again after a failure
        (no business request is ever involved).
        """
        with self._backend_io_lock:
            if self._backend is not None and self._backend.poll() is None:
                return None
            if self._backend_started and self._backend_dead:
                # The process died after a successful start; do not respawn
                # automatically (no hidden replay path).
                return self._backend_start_error or "backend process exited"
            if self._backend_start_error is not None and self._backend is None:
                # A prior launch attempt failed outright.
                return self._backend_start_error
            try:
                process = subprocess.Popen(
                    [self._backend_command, *self._backend_args],
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    cwd=self._backend_cwd,
                )
            except (OSError, ValueError) as exc:
                self._backend_started = True
                self._backend_dead = True
                self._backend_start_error = f"backend launch failed: {exc}"
                self._backend = None
                return self._backend_start_error
            self._backend = process
            self._backend_started = True
            self._backend_dead = False
            self._backend_start_error = None
            self._backend_reader = threading.Thread(
                target=self._backend_read_loop,
                name="backend-reader",
                daemon=True,
            )
            self._backend_reader.start()
            self._backend_stderr_reader = threading.Thread(
                target=self._backend_stderr_loop,
                name="backend-stderr",
                daemon=True,
            )
            self._backend_stderr_reader.start()
            return None

    def _backend_stderr_loop(self) -> None:
        """Drain backend stderr onto a bounded ring (never forwarded).

        Reads use bounded chunk reads -- never ``readline()``, which would
        accumulate an arbitrarily long line that lacks a newline.  Ring
        eviction keeps memory bounded regardless of how much the backend
        writes.
        """
        while True:
            with self._backend_io_lock:
                backend = self._backend
                if backend is None or backend.stderr is None:
                    return
                stream = backend.stderr
            chunk = _read_chunk(stream)
            if not chunk:
                return
            with self._stderr_lock:
                self._stderr_ring.append(chunk)
                self._stderr_bytes += len(chunk)
                while self._stderr_bytes > _STDERR_RING_MAX_BYTES and self._stderr_ring:
                    dropped = self._stderr_ring.popleft()
                    self._stderr_bytes -= len(dropped)

    def _iter_bounded_frames(self, stream: Any) -> Iterator[tuple[str, Any]]:
        """Yield ``("line", bytes)`` / ``("oversize", None)`` from a binary
        stream without ever buffering more than ``_max_frame_bytes`` per line.

        A frame that exceeds the cap is reported once as ``("oversize", None)``
        (never split, truncated, or forwarded), and the remainder of that line
        is discarded chunk-by-chunk so a flood cannot grow memory.  Line bytes
        are left encoded; callers decode.
        """
        pending = bytearray()
        discarding = False
        while True:
            chunk = _read_chunk(stream)
            if not chunk:
                # EOF: a final unterminated line is still a frame.
                if pending:
                    if len(pending) > self._max_frame_bytes:
                        yield ("oversize", None)
                    else:
                        yield ("line", bytes(pending))
                elif discarding:
                    yield ("oversize", None)
                return
            pos = 0
            while pos < len(chunk):
                nl = chunk.find(b"\n", pos)
                if nl == -1:
                    if discarding:
                        pos = len(chunk)
                        continue
                    pending += chunk[pos:]
                    pos = len(chunk)
                    if len(pending) > self._max_frame_bytes:
                        yield ("oversize", None)
                        pending.clear()
                        discarding = True
                    continue
                if discarding:
                    discarding = False
                    pos = nl + 1
                    continue
                pending += chunk[pos:nl]
                pos = nl + 1
                if len(pending) > self._max_frame_bytes:
                    yield ("oversize", None)
                else:
                    yield ("line", bytes(pending))
                pending.clear()

    def _backend_stderr_snippet(self, max_chars: int = 500) -> str:
        """Tail of the bounded stderr ring for operator diagnostics.

        Never wired into any MCP error payload; operators may call it only
        through explicit local introspection.
        """
        with self._stderr_lock:
            joined = b"".join(self._stderr_ring).decode("utf-8", errors="replace").strip()
        if len(joined) > max_chars:
            joined = "..." + joined[-max_chars:]
        return joined

    def _discover(self) -> Optional[str]:
        """One modern ``server/discover`` bootstrap exchange.

        Returns None on success (``self._discovery`` populated) or an error
        string describing why discovery failed (protocol error, crash,
        unsupported modern revision, malformed result).
        """
        with self._backend_io_lock:
            backend = self._backend
            if backend is None:
                return "backend process is not running"
        request_id = self._next_backend_id()
        request = _modern_discover_request(request_id)
        self._register_backend_request(request_id, "server/discover")
        if not self._write_backend(request):
            self._unregister_backend_request(request_id)
            return "backend unavailable while sending server/discover"
        response, reason = self._await_backend_response(
            request_id, self._startup_timeout
        )
        self._unregister_backend_request(request_id)
        if reason == "timeout":
            # A hanging bootstrap must unblock: tear the backend down so no
            # late discover response can confuse a later exchange, then report
            # a clean session failure (nothing business was sent).
            self._terminate_backend(
                f"server/discover timed out after {self._startup_timeout:.1f}s"
            )
            return (
                f"server/discover timed out after {self._startup_timeout:.1f}s; "
                "the backend was stopped and no request is replayed"
            )
        if response is None:
            if reason == "client_closed":
                return "client closed stdin while waiting for server/discover"
            return "backend exited before answering server/discover"
        if "error" in response:
            return self._describe_backend_error(response["error"], "server/discover")
        result = response.get("result")
        if not isinstance(result, dict):
            return "server/discover returned a non-object result"
        capabilities = result.get("capabilities")
        if not isinstance(capabilities, dict):
            return "server/discover did not advertise capabilities"
        supported = result.get("supportedVersions")
        if not isinstance(supported, list) or MODERN_MCP_PROTOCOL_VERSION not in supported:
            return (
                "backend does not support the only modern revision this adapter "
                f"speaks ({MODERN_MCP_PROTOCOL_VERSION})"
            )
        self._discovery = result
        return None

    # -- backend request lifecycle --------------------------------------

    def _next_backend_id(self) -> str:
        with self._state_lock:
            self._backend_seq += 1
            return f"lm-{self._backend_seq}"

    def _register_backend_request(self, request_id: str, method: str) -> None:
        with self._response_condition:
            self._backend_requests[request_id] = {"method": method, "response": None}

    def _unregister_backend_request(self, request_id: str) -> None:
        with self._response_condition:
            self._backend_requests.pop(request_id, None)

    def _await_backend_response(
        self, request_id: str, timeout: float
    ) -> tuple[Optional[dict[str, Any]], str]:
        """Block until the backend answers ``request_id`` or the exchange ends.

        Returns ``(response, reason)`` where reason is one of ``"ok"``,
        ``"dead"`` (backend process gone), ``"timeout"`` (the bounded deadline
        elapsed with no answer) or ``"client_closed"`` (legacy stdin closed,
        so the result is no longer wanted).  A deadline never replays the
        request; callers decide what (if anything) was sent.
        """
        deadline = time.monotonic() + timeout
        with self._response_condition:
            while True:
                record = self._backend_requests.get(request_id)
                if record is not None and record["response"] is not None:
                    return record["response"], "ok"
                if self._client_closed:
                    return None, "client_closed"
                if self._backend_dead or (
                    self._backend is not None and self._backend.poll() is not None
                ):
                    return None, "dead"
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return None, "timeout"
                self._response_condition.wait(timeout=min(0.2, remaining))

    def _backend_read_loop(self) -> None:
        """Read backend stdout frames: responses wake awaiters; progress
        notifications are relayed to the legacy client immediately.

        Frames are read through ``_iter_bounded_frames``: a line longer than
        the configured cap is rejected whole -- the backend session is closed
        and the frame is never partially parsed or forwarded.
        """
        while True:
            with self._backend_io_lock:
                backend = self._backend
                if backend is None or backend.stdout is None:
                    return
                stdout = backend.stdout
            for kind, payload in self._iter_bounded_frames(stdout):
                if kind == "oversize":
                    # Structured closure: this backend violates the frame
                    # contract, so it cannot be trusted for further exchange.
                    # Awaited business calls surface outcomeUnknown (their
                    # bytes were already sent); nothing is replayed.
                    self._terminate_backend(
                        "backend sent a frame larger than the "
                        f"{self._max_frame_bytes}-byte limit; the frame was "
                        "rejected whole (never truncated or forwarded) and the "
                        "backend session was closed"
                    )
                    return
                raw = payload.decode("utf-8", errors="replace").strip()
                if not raw:
                    continue
                try:
                    message = json.loads(raw)
                except ValueError:
                    continue
                if not isinstance(message, dict):
                    continue
                if message.get("method") is None and "id" in message:
                    request_id = message.get("id")
                    with self._response_condition:
                        record = self._backend_requests.get(request_id)
                        if record is not None and record["response"] is None:
                            record["response"] = message
                            self._response_condition.notify_all()
                    continue
                method = message.get("method")
                if method == "notifications/progress":
                    params = message.get("params")
                    if isinstance(params, dict):
                        self._write_legacy(
                            {"jsonrpc": "2.0", "method": method, "params": params}
                        )
                    continue
                # Other backend notifications are ignored: the adapter never
                # opts into subscriptions, and request-scoped notifications
                # other than progress have no legacy mapping.
                continue
            # Backend stdout reached EOF (process exited or was stopped).
            with self._backend_io_lock:
                if self._backend is backend:
                    self._mark_backend_dead_unlocked("backend process exited")
            return

    # -- legacy reader ---------------------------------------------------

    def _legacy_read_loop(self) -> None:
        """Read legacy stdin frames with a hard per-frame size cap and a
        bounded pending-request queue.

        An oversized incoming frame cannot be trusted to be a well-formed
        request (its id is unreadable), so it is answered once with a parse
        error and the rest of the line is discarded -- it is never truncated
        into a partial request.  When the pending queue is at capacity a new
        request gets a structured JSON-RPC refusal and is never executed.
        """
        stdin_buffer = getattr(self._legacy_in, "buffer", None)
        if stdin_buffer is None:
            stdin_buffer = self._legacy_in
        for kind, payload in self._iter_bounded_frames(stdin_buffer):
            if kind == "oversize":
                self._write_legacy(
                    _legacy_error(
                        None,
                        CODE_PARSE_ERROR,
                        f"incoming frame exceeds the {self._max_frame_bytes}"
                        "-byte limit; the request was rejected whole and not "
                        "executed (resend a smaller frame)",
                    )
                )
                continue
            raw = payload.decode("utf-8", errors="replace").strip()
            if not raw:
                continue
            try:
                message = json.loads(raw)
            except ValueError:
                self._write_legacy(
                    _legacy_error(None, CODE_PARSE_ERROR, "Parse error")
                )
                continue
            if not isinstance(message, dict):
                self._write_legacy(
                    _legacy_error(None, CODE_INVALID_REQUEST, "Invalid Request")
                )
                continue
            if message.get("method") is None:
                # A response frame from the client is invalid on the server
                # side; answered as invalid request when it has an id.
                if "id" in message:
                    self._write_legacy(
                        _legacy_error(
                            message.get("id"),
                            CODE_INVALID_REQUEST,
                            "Invalid Request",
                        )
                    )
                continue
            method = message.get("method")
            if method == "notifications/cancelled":
                self._handle_cancelled(message.get("params"))
                continue
            if "id" not in message:
                # Other notifications are fire-and-forget; there is no legacy
                # mapping that needs a side effect (initialized is a no-op for
                # the stateless backend).
                continue
            with self._queue_condition:
                if len(self._request_queue) >= self._queue_capacity:
                    overflow = True
                else:
                    overflow = False
                    self._request_queue.append(message)
                    self._queue_condition.notify()
            if overflow:
                # Structured refusal: never executed, so the client may resend
                # after earlier requests complete.  Memory stays bounded.
                self._write_legacy(
                    _legacy_error(
                        message.get("id"),
                        CODE_SERVER_OVERLOAD,
                        f"request queue is full (capacity "
                        f"{self._queue_capacity}); the request was not "
                        "executed - resend it after earlier requests complete",
                    )
                )
        # Client closed stdin: tell the main loop to stop (queued work is
        # dropped by ``_pop_next_request`` so shutdown is prompt).
        with self._queue_condition:
            self._client_closed = True
            self._queue_condition.notify_all()

    def _handle_cancelled(self, params: Any) -> None:
        """Handle a legacy ``notifications/cancelled``.

        Cancellation is tracked ONLY while the targeted request is live, so a
        cancel never poisons a later reuse of the same numeric id and the
        marker list stays bounded to pending work:

        * queued request      -> purged from the queue under the queue lock
                                 (never executed; no marker retained);
        * in-flight request   -> forwarded onto the mapped backend request id
                                 (that call already started; best effort);
        * dequeued-but-not-yet-written request (``_pending_legacy_id``) ->
                                 recorded so the dispatch-time pre-write check
                                 drops it before any backend bytes are written
                                 (the dequeue->write race guarantee);
        * unknown, completed, or non-scalar requestId -> ignored entirely.
        """
        if not isinstance(params, dict):
            return
        legacy_request_id = params.get("requestId")
        if not _json_scalar_request_id(legacy_request_id):
            return
        with self._queue_condition:
            # 1) Purge every queued occurrence of the id (never executed).
            self._request_queue = deque(
                message
                for message in self._request_queue
                if message.get("id") != legacy_request_id
            )
        with self._state_lock:
            inflight_legacy = self._inflight_legacy_id
            inflight_backend = self._inflight_backend_id
            inflight_written = self._inflight_written
        if (
            inflight_backend is not None
            and legacy_request_id == inflight_legacy
            and inflight_written
        ):
            # 2) In-flight with the business bytes already written: forward
            # the cancellation onto the mapped backend request id (best
            # effort; the call already started).
            reason = params.get("reason")
            cancelled: dict[str, Any] = {
                "jsonrpc": "2.0",
                "method": "notifications/cancelled",
                "params": {"requestId": inflight_backend},
            }
            if isinstance(reason, str):
                cancelled["params"]["reason"] = reason
            self._write_backend(cancelled)
            return
        # 3) Dequeued-but-not-yet-written, or in-flight-but-not-yet-written?
        # Record the marker ONLY while the id is still pending/in-flight so the
        # pre-write check drops it before any backend bytes are written (the
        # dequeue->write race guarantee).  Unknown/completed ids and non-scalar
        # requestIds fall through unrecorded; markers never outlive the request.
        with self._queue_condition:
            if self._pending_legacy_id == legacy_request_id or (
                inflight_legacy == legacy_request_id and not inflight_written
            ):
                if legacy_request_id not in self._cancelled_legacy_ids:
                    self._cancelled_legacy_ids.append(legacy_request_id)

    # -- request handlers (main thread, serialized) ---------------------

    def _pop_next_request(self) -> Optional[dict[str, Any]]:
        """Pop the next legacy request and mark it dispatch-pending.

        ``_pending_legacy_id`` (queue-lock guarded) is the id that has been
        dequeued but whose backend write has not happened yet; a cancellation
        arriving in this window records a marker that the dispatch-time check
        consults before any backend bytes are written (the dequeue->write race
        guarantee).  Queued cancellations purge directly, so nothing marked is
        ever still in the queue.

        Once the client closed stdin no further request is executed: queued
        work is dropped so shutdown stops promptly instead of draining it.
        """
        with self._queue_condition:
            while True:
                if self._shutdown or self._client_closed:
                    self._request_queue.clear()
                    return None
                if not self._request_queue:
                    self._queue_condition.wait(timeout=0.2)
                    continue
                message = self._request_queue.popleft()
                self._pending_legacy_id = message.get("id")
                return message

    def _run_main(self) -> None:
        while not self._shutdown:
            message = self._pop_next_request()
            if message is None:
                break
            request_id = message.get("id")
            try:
                method = message.get("method")
                if method == "initialize":
                    self._write_legacy(
                        self._handle_initialize(request_id, message.get("params"))
                    )
                elif method == "ping":
                    self._write_legacy(_legacy_result(request_id, {}))
                elif method not in ("tools/list", "tools/call"):
                    self._write_legacy(
                        _legacy_error(
                            request_id,
                            CODE_METHOD_NOT_FOUND,
                            f"method {method!r} is not part of the advertised "
                            "tools-only legacy projection",
                        )
                    )
                else:
                    # Serialized backend-backed request.
                    self._handle_backend_request(
                        request_id, method, message.get("params")
                    )
            finally:
                # The request is no longer live: clear the pending marker and
                # any cancellation marker so a later reuse of the same numeric
                # id is never poisoned and markers stay bounded to pending work.
                with self._queue_condition:
                    if self._pending_legacy_id == request_id:
                        self._pending_legacy_id = None
                    self._cancelled_legacy_ids = [
                        marker
                        for marker in self._cancelled_legacy_ids
                        if marker != request_id
                    ]

    def _handle_initialize(
        self, request_id: Any, params: Any
    ) -> dict[str, Any]:
        if not isinstance(params, dict):
            return _legacy_error(
                request_id, CODE_INVALID_PARAMS, "initialize requires object params"
            )
        status, negotiated = negotiate_legacy_version(params.get("protocolVersion"))
        if status == "reject":
            return _legacy_error(
                request_id,
                CODE_INVALID_PARAMS,
                negotiated,
                {
                    "code": "unsupported_protocol_version",
                    "requested": params.get("protocolVersion"),
                    "supported": list(LEGACY_PROFILES),
                },
            )
        # Backend bootstrap (single discover) on first need.  This is the only
        # point where the backend is started, so a failed launch/unsupported
        # backend surfaces here as a clean session error with nothing sent.
        if self._discovery is None:
            if self._ensure_backend_started() is not None:
                return _session_error(
                    request_id,
                    retryable=True,
                    outcome_unknown=False,
                    detail=self._backend_start_error or "backend start failed",
                )
            error = self._discover()
            if error is not None:
                return _session_error(
                    request_id,
                    retryable=False,
                    outcome_unknown=False,
                    detail=error,
                )
        discovery = self._discovery
        if discovery is None:
            return _session_error(
                request_id,
                retryable=False,
                outcome_unknown=False,
                detail="backend discovery did not complete",
            )
        tools_capability = discovery.get("capabilities", {}).get("tools")
        tools_offered = isinstance(tools_capability, dict)
        list_changed = (
            bool(tools_capability.get("listChanged"))
            if isinstance(tools_capability, dict)
            else False
        )
        instructions = discovery.get("instructions")
        instructions = (
            instructions if isinstance(instructions, str) and instructions else None
        )
        self._initialized = True
        self._tools_offered = tools_offered
        if tools_offered:
            # Tools capability is the only family the adapter can represent;
            # ``listChanged`` is the intersection of the backend's flag and
            # what every frozen legacy profile can carry (listChanged exists
            # in every profile, so no further gating applies).
            self._profile = _LegacyProfile(negotiated)
            result = _legacy_initialize_result(
                negotiated,
                tools_list_changed=list_changed,
                instructions=instructions,
            )
        else:
            # Intersection: with no backend tools capability, advertise no
            # capability family at all (never an invented empty claim).
            self._profile = None
            result: dict[str, Any] = {
                "protocolVersion": negotiated,
                "capabilities": {},
                "serverInfo": dict(ADAPTER_SERVER_INFO),
            }
            if instructions is not None:
                result["instructions"] = instructions
        return _legacy_result(request_id, result)

    def _handle_backend_request(
        self, request_id: Any, method: str, params: Any
    ) -> None:
        profile = self._profile
        if not self._initialized:
            self._write_legacy(
                _legacy_error(
                    request_id,
                    CODE_INVALID_REQUEST,
                    f"{method} before a successful initialize",
                )
            )
            return
        if not self._tools_offered or profile is None:
            self._write_legacy(
                _legacy_error(
                    request_id,
                    CODE_METHOD_NOT_FOUND,
                    "server does not offer tools",
                )
            )
            return
        if not isinstance(params, dict):
            params = {}
        progress_token = None
        meta = params.get("_meta")
        if isinstance(meta, dict) and "progressToken" in meta:
            progress_token = meta["progressToken"]

        if method == "tools/list":
            cursor = params.get("cursor")
            backend_id = self._next_backend_id()
            request = _modern_tools_list_request(backend_id, cursor, progress_token)
            self._dispatch_serialized(request_id, backend_id, request, profile, method)
            return

        # tools/call
        name = params.get("name")
        if not isinstance(name, str) or not name:
            self._write_legacy(
                _legacy_error(request_id, CODE_INVALID_PARAMS, "tools/call requires a name")
            )
            return
        arguments = params.get("arguments")
        if arguments is not None and not isinstance(arguments, dict):
            self._write_legacy(
                _legacy_error(
                    request_id,
                    CODE_INVALID_PARAMS,
                    "tools/call arguments must be an object when present",
                )
            )
            return
        try:
            request = _modern_tools_call_request(
                self._next_backend_id(), name, arguments, progress_token
            )
        except ValueError as exc:
            self._write_legacy(
                _legacy_error(request_id, CODE_INVALID_PARAMS, str(exc))
            )
            return
        self._dispatch_serialized(request_id, request["id"], request, profile, method)

    def _dispatch_serialized(
        self,
        legacy_id: Any,
        backend_id: str,
        request: dict[str, Any],
        profile: _LegacyProfile,
        legacy_method: str,
    ) -> None:
        """One serialized backend exchange for a legacy tools request.

        Exactly one write happens per exchange, and a deadline or transport
        failure never resends the request.  ``outcomeUnknown`` becomes true
        only once the business ``tools/call`` bytes were written to the
        backend.
        """
        outcome_unknown = False
        with self._state_lock:
            self._inflight_legacy_id = legacy_id
            self._inflight_backend_id = backend_id
            self._inflight_written = False
        try:
            self._register_backend_request(backend_id, legacy_method)
            # Dispatch-time cancellation check: a cancel that raced between
            # dequeue and here must prevent any backend bytes from being
            # written (the request never executes).
            with self._queue_condition:
                cancelled_early = legacy_id in self._cancelled_legacy_ids
                if cancelled_early:
                    self._cancelled_legacy_ids = [
                        marker
                        for marker in self._cancelled_legacy_ids
                        if marker != legacy_id
                    ]
            if not cancelled_early:
                # Second pre-write check: a cancel that landed after the
                # dispatch-time check but before the write (in-flight
                # registered, bytes not yet sent) must also stop the write.
                with self._queue_condition:
                    cancelled_early = legacy_id in self._cancelled_legacy_ids
                    if cancelled_early:
                        self._cancelled_legacy_ids = [
                            marker
                            for marker in self._cancelled_legacy_ids
                            if marker != legacy_id
                        ]
            if cancelled_early:
                return  # never executed; no response (client signalled unused)
            wrote = self._write_backend(request)
            if wrote:
                with self._state_lock:
                    self._inflight_written = True
            reason = "dead"
            response: Optional[dict[str, Any]] = None
            if not wrote:
                pass
            else:
                if legacy_method == "tools/call":
                    # The business call bytes were written; if the backend now
                    # dies or times out we cannot know whether it ran.
                    outcome_unknown = True
                response, reason = self._await_backend_response(
                    backend_id, self._request_timeout
                )
        finally:
            self._unregister_backend_request(backend_id)
            with self._state_lock:
                self._inflight_legacy_id = None
                self._inflight_backend_id = None
                self._inflight_written = False
            with self._queue_condition:
                # A post-write cancellation (already forwarded to the backend)
                # must not poison a later reuse of the same numeric id.
                self._cancelled_legacy_ids = [
                    marker
                    for marker in self._cancelled_legacy_ids
                    if marker != legacy_id
                ]

        if reason == "timeout":
            # Bounded deadline reached: tear the backend down so a late answer
            # cannot execute or echo anything further; the request was sent at
            # most once and is never replayed.
            self._terminate_backend(
                f"backend {legacy_method} timed out after "
                f"{self._request_timeout:.1f}s"
            )
            self._write_legacy(
                _session_error(
                    legacy_id,
                    retryable=True,
                    outcome_unknown=outcome_unknown,
                    detail=(
                        f"backend {legacy_method} timed out after "
                        f"{self._request_timeout:.1f}s; the request was sent "
                        "at most once and is not replayed"
                    ),
                )
            )
            return
        if response is None:
            if reason == "client_closed":
                # Stdin closed while awaiting: the client is gone, so no
                # response is written; serve_forever tears the backend down.
                return
            self._write_legacy(
                _session_error(
                    legacy_id,
                    retryable=True,
                    outcome_unknown=outcome_unknown,
                    detail=(
                        "backend transport failed"
                        if outcome_unknown
                        else "backend unavailable before any side effect"
                    ),
                )
            )
            return
        if "error" in response:
            self._write_legacy(
                self._project_backend_error(legacy_id, response["error"])
            )
            return
        result = response.get("result")
        if not isinstance(result, dict):
            self._write_legacy(
                _session_error(
                    legacy_id,
                    retryable=False,
                    outcome_unknown=outcome_unknown,
                    detail="backend returned a non-object result",
                )
            )
            return
        if result.get("resultType") == RESULT_TYPE_INPUT_REQUIRED:
            # MRTR interim results cannot be expressed on the legacy side and
            # are never silently converted (no hidden modern->legacy fallback).
            self._write_legacy(
                _legacy_error(
                    legacy_id,
                    CODE_INTERNAL_ERROR,
                    "backend requested input the legacy client cannot provide",
                    {
                        "code": "input_required_unrepresentable",
                        "retryable": False,
                        "outcomeUnknown": False,
                    },
                )
            )
            return
        if legacy_method == "tools/list":
            projected = profile.project_tools_result(result)
        else:
            projected = profile.project_call_result(result)
        if projected is None:
            self._write_legacy(
                _legacy_error(
                    legacy_id,
                    CODE_INTERNAL_ERROR,
                    "backend result could not be projected to the legacy profile",
                    {
                        "code": "unprojectable_result",
                        "retryable": False,
                        "outcomeUnknown": False,
                    },
                )
            )
            return
        self._write_legacy(_legacy_result(legacy_id, projected))

    @staticmethod
    def _describe_backend_error(error: Any, method: str) -> str:
        if not isinstance(error, dict):
            return f"backend answered {method} with a non-object error"
        message = error.get("message")
        code = error.get("code")
        return f"backend {method} failed: code={code!r} message={message!r}"

    @staticmethod
    def _project_backend_error(
        request_id: Any, error: Any
    ) -> dict[str, Any]:
        """Project a backend JSON-RPC error onto the legacy profile.

        The code and message pass through verbatim except for modern-era or
        retired codes a legacy client must never receive; those are mapped to
        a plain internal error with the original preserved in ``data``.
        """
        if not isinstance(error, dict):
            return _session_error(
                request_id,
                retryable=False,
                outcome_unknown=False,
                detail="backend answered with a non-object error",
            )
        code = error.get("code")
        message = error.get("message")
        if not isinstance(code, int):
            return _session_error(
                request_id,
                retryable=False,
                outcome_unknown=False,
                detail="backend error has no numeric code",
            )
        data = error.get("data")
        if code in RETIRED_MCP_ERROR_CODES or code == MCP_UNSUPPORTED_PROTOCOL_VERSION:
            return _legacy_error(
                request_id,
                CODE_INTERNAL_ERROR,
                "backend protocol error",
                {
                    "originalCode": code,
                    "originalMessage": message,
                    "code": "backend_protocol_error",
                    "retryable": False,
                    "outcomeUnknown": False,
                },
            )
        legacy_error: dict[str, Any] = {
            "code": code,
            "message": message if isinstance(message, str) else "backend error",
        }
        if data is not None:
            legacy_error["data"] = data
        return {"jsonrpc": "2.0", "id": request_id, "error": legacy_error}

    # -- lifecycle --------------------------------------------------------

    def serve_forever(self) -> int:
        self._legacy_reader = threading.Thread(
            target=self._legacy_read_loop,
            name="legacy-reader",
            daemon=True,
        )
        self._legacy_reader.start()
        self._run_main()
        self._shutdown = True
        with self._queue_condition:
            self._queue_condition.notify_all()
        self._teardown_backend()
        return 0

    def _teardown_backend(self) -> None:
        with self._backend_io_lock:
            backend = self._backend
            self._backend = None
        if backend is not None:
            self._stop_backend_process(backend, wait_seconds=5)

    def _terminate_backend(self, detail: str) -> None:
        """Stop the backend process after a deadline/transport failure.

        Marks the session dead (unblocking any awaiter), then closes stdin and
        kills the process so no late response or duplicate side effect can
        follow.  Never respawned automatically.
        """
        with self._backend_io_lock:
            backend = self._backend
            self._backend = None
            if backend is not None:
                self._mark_backend_dead_unlocked(detail)
        if backend is not None:
            self._stop_backend_process(backend, wait_seconds=2)

    @staticmethod
    def _stop_backend_process(
        backend: subprocess.Popen, wait_seconds: float
    ) -> None:
        try:
            if backend.stdin is not None:
                backend.stdin.close()
        except OSError:
            pass
        try:
            backend.wait(timeout=wait_seconds)
        except subprocess.TimeoutExpired:
            try:
                backend.kill()
            except OSError:
                pass
            try:
                backend.wait(timeout=wait_seconds)
            except subprocess.TimeoutExpired:
                pass
        for stream in (backend.stdout, backend.stderr):
            if stream is not None:
                try:
                    stream.close()
                except OSError:
                    pass


# --------------------------------------------------------------------------


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="legacy_modern_stdio",
        description=(
            "Reverse-era stdio projection: serve a frozen legacy MCP profile "
            "to an Agent while bridging to a modern-only (2026-07-28) stdio "
            "backend. Local CLI/operator opt-in only; binds no listener."
        ),
    )
    parser.add_argument(
        "--backend-command",
        required=True,
        help="Executable to launch the modern-only backend subprocess with.",
    )
    parser.add_argument(
        "--backend-cwd",
        default=None,
        help="Optional working directory for the backend subprocess.",
    )
    parser.add_argument(
        "--startup-timeout-seconds",
        type=float,
        default=15.0,
        help=(
            "Bounded wait for the backend server/discover bootstrap exchange "
            "(default 15).  A hang tears the backend down and fails initialize "
            "cleanly; nothing is replayed."
        ),
    )
    parser.add_argument(
        "--request-timeout-seconds",
        type=float,
        default=60.0,
        help=(
            "Bounded wait for each backend tools/list / tools/call exchange "
            "(default 60).  A deadline unblocks the exchange and stops the "
            "backend; a timed-out tools/call reports outcomeUnknown=true only "
            "because its bytes were sent, and is never replayed."
        ),
    )
    parser.add_argument(
        "--max-frame-bytes",
        type=int,
        default=_DEFAULT_MAX_FRAME_BYTES,
        help=(
            "Hard cap on one newline-delimited frame on legacy stdin and "
            "backend stdout (default 16 MiB).  An oversized frame is refused "
            "whole - never truncated and forwarded as a business call."
        ),
    )
    parser.add_argument(
        "--queue-capacity",
        type=int,
        default=_DEFAULT_QUEUE_CAPACITY,
        help=(
            "Bounded number of pending legacy requests awaiting the serialized "
            "backend (default 256).  At capacity, further requests get a "
            "structured JSON-RPC refusal and are never executed."
        ),
    )
    parser.add_argument(
        "--backend-args",
        nargs=argparse.REMAINDER,
        default=[],
        help=(
            "Arguments for the backend subprocess, taken verbatim from this "
            "point to the end of the command line (values may begin with '-', "
            "so this option must be listed last). Operator supplied."
        ),
    )
    args = parser.parse_args(argv)
    server = LegacyModernServer(
        backend_command=args.backend_command,
        backend_args=args.backend_args,
        backend_cwd=args.backend_cwd,
        startup_timeout=args.startup_timeout_seconds,
        request_timeout=args.request_timeout_seconds,
        max_frame_bytes=args.max_frame_bytes,
        queue_capacity=args.queue_capacity,
    )
    return server.serve_forever()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise SystemExit(0)
