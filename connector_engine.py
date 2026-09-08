#!/usr/bin/env python3
"""Connector engine: the dynamically reloadable JSON-RPC-aware recovery layer.

The frozen connector core (``connector_core``) stays a minimal,
message-agnostic supervisor: it owns sockets, framing, threads, the reconnect
loop, buffering, and process exit, and it writes exactly the bytes this engine
returns.  Every JSON-RPC-aware recovery decision lives in this module so that
recovery policy can be updated (or replaced entirely, e.g. via
``WIN_WSL_MCP_BRIDGE_CONNECTOR_ENGINE``) without touching the frozen core:

* which Agent messages are handshake and may therefore be cached and replayed
  after a reconnect, and which are ordinary requests that are never replayed;
* which node response closes the replay-absorb phase (and is therefore never
  forwarded a second time to the Agent);
* which pending business calls are failed exactly once with a concise
  Bridge-generated JSON-RPC error when a stream dies;
* where the stale-core warning may appear (initialize result instructions and
  Bridge-generated errors only, plus an optional single-line tool-result note).

Versions are simple strings compared with direct equality (no semver ordering):
the node's ``coreVersion`` must equal ``CORE_VERSION`` or the connector treats
the core as stale.  Standard library only; this module never imports the core.
"""

from __future__ import annotations

import json
import threading
from types import SimpleNamespace

ENGINE_NAME = "connector-engine"
ENGINE_VERSION = "1.0.0"
CORE_VERSION = "0.4.0"  # exact node core release this engine pairs with

# Optional single-line tool-result annotation.  When True and the connected node
# core does not equal CORE_VERSION, the connector appends one concise text line
# to the first forwarded business tool result.  Off by default so business
# payloads stay byte-preserved during connected operation.
NOTE_RESULTS_WHEN_STALE = False

INITIALIZE_METHOD = "initialize"
INITIALIZED_NOTIFICATION = "notifications/initialized"
CALL_METHOD = "tools/call"

BRIDGE_ERROR_CODE = -32000

LOST_REASON = (
    "bridge: node stream lost before the MCP answered; "
    "request failed once and will not be replayed"
)
OVERFLOW_REASON = (
    "bridge: node stream unavailable and the connector input queue exceeded "
    "its limit; request not delivered and not replayed"
)


def parse(raw: bytes) -> dict | None:
    try:
        message = json.loads(raw)
    except ValueError:
        return None
    return message if isinstance(message, dict) else None


def _json_bytes(value: object) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, separators=(",", ":"), sort_keys=True
    ).encode("utf-8")


def annotate_initialize_result(raw: bytes, warning: str) -> bytes:
    """Append the stale-core warning line to an initialize result's instructions."""
    try:
        message = json.loads(raw)
        if not isinstance(message.get("result"), dict):
            return raw
        clone = json.loads(raw)
        result = clone["result"]
        existing = result.get("instructions")
        if isinstance(existing, str) and existing:
            result["instructions"] = existing.rstrip() + "\n" + warning
        else:
            result["instructions"] = warning
        return _json_bytes(clone) + b"\n"
    except Exception:
        return raw


def annotate_tool_result(raw: bytes, warning: str) -> bytes:
    """Append one concise text line to a business tool result's content."""
    try:
        message = json.loads(raw)
        if not isinstance(message.get("result"), dict):
            return raw
        clone = json.loads(raw)
        content = clone["result"].get("content")
        if not isinstance(content, list):
            content = []
        content = list(content)
        content.append({"type": "text", "text": warning})
        clone["result"]["content"] = content
        return _json_bytes(clone) + b"\n"
    except Exception:
        return raw


def error_payload(
    request_id: object, message: str, *, outcome_unknown: bool = True
) -> bytes:
    """One concise Bridge-generated JSON-RPC error response (once-only callsite)."""
    return _json_bytes(
        {
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {
                "code": BRIDGE_ERROR_CODE,
                "message": message,
                "data": (
                    {"bridge": True, "outcomeUnknown": True}
                    if outcome_unknown
                    else {"bridge": True, "outcomeUnknown": False}
                ),
            },
        }
    ) + b"\n"


class Recovery:
    """JSON-RPC-aware recovery policy for one persistent connector session.

    Pure logic: it never touches sockets, files, or stdio.  The core feeds it
    raw lines from both directions and applies whatever payload bytes it
    returns.  An engine override may subclass ``Recovery`` and change the class
    attributes below (for example ``CORE_VERSION`` or
    ``NOTE_RESULTS_WHEN_STALE``) without copying any logic.
    """

    NAME = ENGINE_NAME
    VERSION = ENGINE_VERSION
    CORE_VERSION = CORE_VERSION
    NOTE_RESULTS_WHEN_STALE = NOTE_RESULTS_WHEN_STALE
    INITIALIZE_METHOD = INITIALIZE_METHOD
    INITIALIZED_NOTIFICATION = INITIALIZED_NOTIFICATION
    CALL_METHOD = CALL_METHOD
    LOST_REASON = LOST_REASON
    OVERFLOW_REASON = OVERFLOW_REASON
    UNAVAILABLE_REASON = "bridge: node stream unavailable; request not delivered"

    def __init__(self, core_version: str | None = None) -> None:
        self.core_version = core_version if isinstance(core_version, str) else None
        self._lock = threading.RLock()
        # Cached Agent->node handshake, replayed verbatim on reconnect.
        self._init_raw: bytes | None = None
        self._init_id: object = None
        self._initialized_raw: bytes | None = None
        # Outstanding Agent-initiated requests (id -> method); the uncertain set
        # holds ids whose send may or may not have reached the node.
        self._pending: dict[object, str | None] = {}
        self._uncertain: set[object] = set()
        self._failed: set[object] = set()
        # One-shot warning placement.
        self._init_annotated = False
        self._result_noted = False

    # -- staleness (simple direct version equality, no semver) ---------------

    def stale(self) -> bool:
        core = self.core_version
        return bool(core) and core != type(self).CORE_VERSION

    def set_core(self, core_version: object) -> None:
        with self._lock:
            self.core_version = (
                core_version if isinstance(core_version, str) and core_version else None
            )

    def warning_line(self) -> str:
        return (
            f"Bridge core mismatch: connector engine {type(self).NAME} "
            f"v{type(self).VERSION} expects node core {type(self).CORE_VERSION}, "
            f"node reports {self.core_version}."
        )

    def failed_message(self, plain: str) -> str:
        if self.stale():
            return f"{plain} ({self.warning_line()})"
        return plain

    # -- Agent (client) direction -------------------------------------------

    def handle_agent(self, raw: bytes) -> SimpleNamespace:
        """Classify one Agent line and cache handshake bytes when present.

        Returns a ``SimpleNamespace(kind, id, method)`` where kind is one of
        ``'initialize'``, ``'initialized'``, ``'request'``, ``'response'`` or
        ``'other'``.  Only initialize/request kinds may be forwarded or queued.
        """
        message = parse(raw)
        kind = "other"
        request_id: object = None
        method: str | None = None
        if message is not None:
            candidate = message.get("id")
            message_method = message.get("method")
            if message_method == type(self).INITIALIZE_METHOD and "id" in message:
                kind = "initialize"
                request_id = candidate
                method = type(self).INITIALIZE_METHOD
                with self._lock:
                    self._init_raw = raw
                    self._init_id = candidate
            elif message_method == type(self).INITIALIZED_NOTIFICATION:
                kind = "initialized"
                with self._lock:
                    self._initialized_raw = raw
            elif (
                isinstance(message_method, str)
                and "id" in message
                and not ("result" in message or "error" in message)
            ):
                kind = "request"
                request_id = candidate
                method = message_method
            elif "id" in message and ("result" in message or "error" in message):
                kind = "response"
                request_id = candidate
        return SimpleNamespace(kind=kind, id=request_id, method=method)

    def remember_sent(self, request_id: object, method: str | None, current: bool) -> None:
        """Record a forwarded Agent request id.

        ``current`` True means the send provably happened on the live session;
        False means the session died around the send, so the id is uncertain and
        must be failed once at the next loss unless a response already arrived.
        """
        with self._lock:
            if current:
                self._pending[request_id] = method
            elif request_id not in self._failed:
                self._uncertain.add(request_id)

    def export_handshake(self) -> tuple[bytes | None, object, bytes | None]:
        """Export only replay-safe session state for a verified engine handover."""
        with self._lock:
            return self._init_raw, self._init_id, self._initialized_raw

    def import_handshake(
        self, state: tuple[bytes | None, object, bytes | None]
    ) -> None:
        """Restore only initialize/initialized bytes; business calls never migrate."""
        with self._lock:
            self._init_raw, self._init_id, self._initialized_raw = state

    def quiescent(self) -> bool:
        """True when an engine generation can be replaced without losing a call."""
        with self._lock:
            return not self._pending and not self._uncertain

    def replay_available(self) -> bool:
        with self._lock:
            return self._init_raw is not None

    def cached_initialize(self) -> bytes | None:
        with self._lock:
            return self._init_raw

    def cached_initialized(self) -> bytes | None:
        with self._lock:
            return self._initialized_raw

    # -- Node (server) direction ---------------------------------------------

    def handle_node(self, raw: bytes, *, absorbing: bool) -> SimpleNamespace:
        """Classify one node line during/after the replay-absorb phase.

        Returns ``SimpleNamespace(kind, payload)``: while ``absorbing`` the kind
        is ``'absorb'`` (core buffers the raw line) until the replayed
        initialize result arrives, which yields ``'absorb_done'`` with payload
        ``None`` (that response is swallowed, never re-forwarded).  Once live,
        kind is ``'agent'`` and payload is the raw line, annotated exactly when
        this engine's policy says a stale-core warning belongs on it.
        """
        message = parse(raw)
        request_id = message.get("id") if message is not None else None
        is_response = (
            message is not None
            and request_id is not None
            and ("result" in message or "error" in message)
        )
        if absorbing:
            if (
                is_response
                and request_id == self._init_id
                and isinstance(message.get("result"), dict)
                and bool(message["result"])
            ):
                return SimpleNamespace(kind="absorb_done", payload=None)
            return SimpleNamespace(kind="absorb", payload=None)

        payload = raw
        if is_response:
            method = self._consume(request_id)
            init_result = (
                request_id == self._init_id
                and isinstance(message.get("result"), dict)
                and bool(message["result"])
            )
            if init_result and self.stale() and not self._init_annotated:
                with self._lock:
                    self._init_annotated = True
                payload = annotate_initialize_result(raw, self.warning_line())
            elif (
                method == type(self).CALL_METHOD
                and self.stale()
                and type(self).NOTE_RESULTS_WHEN_STALE
                and not self._result_noted
            ):
                with self._lock:
                    self._result_noted = True
                payload = annotate_tool_result(raw, self.warning_line())
        return SimpleNamespace(kind="agent", payload=payload)

    def _consume(self, request_id: object) -> str | None:
        with self._lock:
            method = self._pending.pop(request_id, None)
            self._uncertain.discard(request_id)
            return method

    # -- loss / fail-once ----------------------------------------------------

    def lost_requests(self) -> list[tuple[object, str | None]]:
        """Drain the outstanding request ids for a stream loss."""
        with self._lock:
            items = list(self._pending.items())
            items.extend((request_id, None) for request_id in self._uncertain)
            self._pending.clear()
            self._uncertain.clear()
            return items

    def fail_once(
        self,
        request_ids: list[tuple[object, str | None]],
        reason: str,
        *,
        outcome_unknown: bool = True,
    ) -> list[bytes]:
        """Return Bridge error payload bytes for each id not yet failed once."""
        with self._lock:
            fresh = [
                (request_id, _method)
                for request_id, _method in request_ids
                if request_id not in self._failed
            ]
            for request_id, _method in fresh:
                self._failed.add(request_id)
        if not fresh:
            return []
        message = self.failed_message(reason)
        return [
            error_payload(request_id, message, outcome_unknown=outcome_unknown)
            for request_id, _method in fresh
        ]


def make_recovery(core_version: str | None = None) -> Recovery:
    """Factory the frozen core uses to build one recovery policy instance."""
    return Recovery(core_version)
