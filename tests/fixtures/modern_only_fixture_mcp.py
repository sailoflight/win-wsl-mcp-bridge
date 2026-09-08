#!/usr/bin/env python3
"""Modern-only (2026-07-28) stdio MCP fixture used by the reverse-era
``legacy_modern_stdio`` projection tests.

This fixture deliberately implements ONLY the modern stateless contract: every
request must carry the required per-request ``_meta`` (protocolVersion +
clientCapabilities), there is NO legacy ``initialize`` handshake (a legacy
initialize is answered as method-not-found ``-32601``), and all results
carry the modern envelope (``resultType``, ``ttlMs``/``cacheScope`` on list
results, ``_meta.serverInfo``).  A legacy client cannot talk to it directly;
``legacy_modern_stdio.py`` exists to bridge one.

Protocol behaviour:

* ``server/discover`` -> DiscoverResult with supportedVersions/capabilities/
  instructions/identity (or ``-32022`` when the requested version is not the
  single supported modern revision).
* ``tools/list`` -> modern ListToolsResult whose tools carry modern-era fields
  (``title``, ``outputSchema``, ``annotations``, ``icons`` - a plural list of
  ``Icon`` objects) that the projection must strip back to the negotiated
  legacy profile.
* ``tools/call``:
  - ``echo``       -> content + ``structuredContent`` (+ modern envelope).
  - ``tool_error`` -> ``isError: true`` result.
  - ``bad_call``   -> JSON-RPC ``-32602`` protocol error (tests clean relay).
  - ``retired_error`` -> JSON-RPC ``-32042`` retired code (tests normalization).
  - ``slow``       -> worker thread emits ``notifications/progress`` for the
    request's ``_meta.progressToken`` then completes; honours a matching
    ``notifications/cancelled`` (answers ``-32800`` when cancelled in flight).
    With config ``hang_call`` it never completes (adapter deadline tests).
  - ``crash_on_call`` -> worker logs the call and exits the process
    immediately (tests no-replay / outcomeUnknown).

The advertised tools subset is deliberately bounded to those six tools; it is
the fixture's whole catalog and never grows.

* Every incoming frame and synthesized lifecycle event is appended as one JSON
  line to the ``--state`` log file so tests can count calls from the fixture's
  own perspective.

Invocation (operator/test use):

    python3 tests/fixtures/modern_only_fixture_mcp.py --state <log.jsonl> [--config <cfg.json>]

Config keys (all optional): ``server_name``, ``instructions`` (string; a
missing/empty value suppresses the field), ``list_changed`` (bool, default
true), ``supported_versions`` (list, default [\"2026-07-28\"]),
``discover_error`` (\"unsupported\"), ``no_instructions``,
``hang_discover`` (bool: never answer server/discover - startup-timeout
tests), ``hang_call`` (bool: ``slow`` never completes - request-timeout
tests), ``stderr_flood_bytes`` (int: write that many bytes to stderr at
startup before serving - bounded-drain deadlock tests; the flood contains
no newline so a line-buffering reader would grow without bound),
``oversized_result_bytes`` (int: ``echo`` answers with one newline-free text
blob of that size - oversized-backend-frame rejection tests),
``stderr_token`` (str: one newline-terminated marker line written to stderr
at startup - proves stderr content never leaks into MCP errors).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import threading
import time
from typing import Any, Optional

MODERN_VERSION = "2026-07-28"
SERVER_INFO_KEY = "io.modelcontextprotocol/serverInfo"

_log_lock = threading.Lock()


def log_event(state_path: str, event: dict) -> None:
    if not state_path:
        return
    with _log_lock:
        try:
            with open(state_path, "a", encoding="utf-8") as handle:
                handle.write(json.dumps(event, separators=(",", ":")) + "\n")
        except OSError:
            pass


def _text(text: str) -> list:
    return [{"type": "text", "text": text}]


#: A schema-shaped Icon object (Icon = {src, mimeType?, sizes?}) shared by the
#: bounded six-tool catalog so profile tests can assert Tool.icons handling.
_ICON = {
    "src": "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwC"
    "AAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII=",
    "mimeType": "image/png",
}


class ModernOnlyFixture:
    def __init__(self, config: dict, state_path: str) -> None:
        self.state = state_path
        self.server_name = config.get("server_name", "modern-only-fixture")
        self.instructions = config.get("instructions")
        if config.get("no_instructions"):
            self.instructions = None
        self.list_changed = bool(config.get("list_changed", True))
        versions = config.get("supported_versions")
        self.supported_versions = (
            list(versions) if isinstance(versions, list) and versions else [MODERN_VERSION]
        )
        self.discover_error = config.get("discover_error")
        self.hang_discover = bool(config.get("hang_discover"))
        self.hang_call = bool(config.get("hang_call"))
        raw_flood = config.get("stderr_flood_bytes", 0)
        self.stderr_flood_bytes = (
            int(raw_flood) if isinstance(raw_flood, (int, float)) and raw_flood > 0 else 0
        )
        raw_oversize = config.get("oversized_result_bytes", 0)
        self.oversized_result_bytes = (
            int(raw_oversize)
            if isinstance(raw_oversize, (int, float)) and raw_oversize > 0
            else 0
        )
        self.stderr_token = config.get("stderr_token", "")
        self._respond_lock = threading.Lock()
        self._cancelled: dict[Any, bool] = {}
        self._cancelled_lock = threading.Lock()
        self._tools = [
            {
                "name": "echo",
                "title": "Echo tool title",
                "description": "Return the supplied value.",
                "inputSchema": {
                    "type": "object",
                    "properties": {"value": {"type": "string"}},
                    "required": ["value"],
                    "additionalProperties": False,
                },
                "outputSchema": {
                    "type": "object",
                    "properties": {"echo": {}},
                    "required": [],
                    "additionalProperties": False,
                },
                "annotations": {
                    "title": "Echo",
                    "readOnlyHint": True,
                    "destructiveHint": False,
                    "idempotentHint": True,
                    "openWorldHint": False,
                },
                "icons": [dict(_ICON)],
            },
            {
                "name": "tool_error",
                "title": "Tool error title",
                "description": "Always ends with isError true.",
                "inputSchema": {
                    "type": "object",
                    "properties": {},
                    "additionalProperties": False,
                },
                "outputSchema": {
                    "type": "object",
                    "properties": {},
                    "required": [],
                    "additionalProperties": False,
                },
                "annotations": {
                    "title": "Tool error",
                    "readOnlyHint": True,
                    "destructiveHint": False,
                },
                "icons": [dict(_ICON)],
            },
            {
                "name": "bad_call",
                "title": "Bad call title",
                "description": "Answers a protocol-level -32602 error.",
                "inputSchema": {
                    "type": "object",
                    "properties": {},
                    "additionalProperties": False,
                },
                "icons": [dict(_ICON)],
            },
            {
                "name": "retired_error",
                "title": "Retired error title",
                "description": "Answers a retired -32042 error.",
                "inputSchema": {
                    "type": "object",
                    "properties": {},
                    "additionalProperties": False,
                },
                "icons": [dict(_ICON)],
            },
            {
                "name": "slow",
                "title": "Slow tool title",
                "description": "Progress then completion; honour cancellation.",
                "inputSchema": {
                    "type": "object",
                    "properties": {},
                    "additionalProperties": False,
                },
                "outputSchema": {
                    "type": "object",
                    "properties": {},
                    "required": [],
                    "additionalProperties": False,
                },
                "annotations": {
                    "title": "Slow",
                    "readOnlyHint": True,
                    "destructiveHint": False,
                },
                "icons": [dict(_ICON)],
            },
            {
                "name": "crash_on_call",
                "title": "Crash tool title",
                "description": "Exits the process after logging the call.",
                "inputSchema": {
                    "type": "object",
                    "properties": {},
                    "additionalProperties": False,
                },
                "icons": [dict(_ICON)],
            },
        ]
        self._server_info = {"name": self.server_name, "version": "1.0.0"}

    # -- plumbing ---------------------------------------------------------

    def respond(self, request_id: Any, result: Optional[dict] = None, error: Optional[dict] = None) -> None:
        message: dict[str, Any] = {"jsonrpc": "2.0", "id": request_id}
        if error is not None:
            message["error"] = error
        else:
            message["result"] = result if result is not None else {}
        with self._respond_lock:
            sys.stdout.write(json.dumps(message, separators=(",", ":")) + "\n")
            sys.stdout.flush()

    def notify(self, method: str, params: dict) -> None:
        with self._respond_lock:
            sys.stdout.write(
                json.dumps(
                    {"jsonrpc": "2.0", "method": method, "params": params},
                    separators=(",", ":"),
                )
                + "\n"
            )
            sys.stdout.flush()

    # -- validation -------------------------------------------------------

    def modern_meta(self, message: dict) -> tuple[Optional[str], Optional[str]]:
        """Return ``(None, None)`` on valid modern meta else ``(problem, None)``.

        Also enforces the single supported modern revision, returning the
        ``-32022``-style failure as ``("unsupported", requested)``.
        """
        params = message.get("params")
        if not isinstance(params, dict):
            return "_meta is required on every modern request", None
        meta = params.get("_meta")
        if not isinstance(meta, dict):
            return "_meta is required on every modern request", None
        version = meta.get("io.modelcontextprotocol/protocolVersion")
        capabilities = meta.get("io.modelcontextprotocol/clientCapabilities")
        if not isinstance(version, str) or not version:
            return "protocolVersion must be a non-empty string", None
        if not isinstance(capabilities, dict):
            return "clientCapabilities must be an object", None
        if version not in self.supported_versions:
            return "unsupported", version
        return None, None

    def meta_progress_token(self, message: dict) -> Any:
        params = message.get("params")
        if not isinstance(params, dict):
            return None
        meta = params.get("_meta")
        if not isinstance(meta, dict):
            return None
        return meta.get("progressToken")

    # -- handlers ---------------------------------------------------------

    def handle(self, message: dict) -> None:
        method = message.get("method")
        request_id = message.get("id")
        params = message.get("params")
        if not isinstance(params, dict):
            params = {}
        is_request = "id" in message

        if method == "server/discover":
            problem, _requested = self.modern_meta(message)
            if problem == "unsupported":
                self.respond(
                    request_id,
                    error={
                        "code": -32022,
                        "message": "Unsupported protocol version",
                        "data": {
                            "supported": self.supported_versions,
                            "requested": self.meta_version(message),
                        },
                    },
                )
                return
            if problem is not None:
                self.respond(request_id, error={"code": -32602, "message": problem})
                return
            if self.discover_error == "unsupported":
                self.respond(
                    request_id,
                    error={
                        "code": -32022,
                        "message": "Unsupported protocol version",
                        "data": {
                            "supported": ["2027-01-01"],
                            "requested": MODERN_VERSION,
                        },
                    },
                )
                return
            if self.hang_discover:
                # Never answer: exercises the adapter's bounded startup
                # timeout (no business request is involved).
                log_event(self.state, {"event": "discover-hang", "id": request_id})
                return
            result = {
                "resultType": "complete",
                "supportedVersions": list(self.supported_versions),
                "capabilities": {"tools": {"listChanged": self.list_changed}},
                "ttlMs": 0,
                "cacheScope": "private",
                "_meta": {SERVER_INFO_KEY: self._server_info},
            }
            if self.instructions is not None:
                result["instructions"] = self.instructions
            self.respond(request_id, result=result)
            return

        if method == "tools/list":
            problem, _requested = self.modern_meta(message)
            if problem == "unsupported":
                self.respond(
                    request_id,
                    error={
                        "code": -32022,
                        "message": "Unsupported protocol version",
                        "data": {
                            "supported": self.supported_versions,
                            "requested": self.meta_version(message),
                        },
                    },
                )
                return
            if problem is not None:
                self.respond(request_id, error={"code": -32602, "message": problem})
                return
            self.respond(
                request_id,
                result={
                    "resultType": "complete",
                    "tools": self._tools,
                    "ttlMs": 5000,
                    "cacheScope": "private",
                    "_meta": {SERVER_INFO_KEY: self._server_info},
                },
            )
            return

        if method == "tools/call":
            problem, _requested = self.modern_meta(message)
            if problem == "unsupported":
                self.respond(
                    request_id,
                    error={
                        "code": -32022,
                        "message": "Unsupported protocol version",
                        "data": {
                            "supported": self.supported_versions,
                            "requested": self.meta_version(message),
                        },
                    },
                )
                return
            if problem is not None:
                self.respond(request_id, error={"code": -32602, "message": problem})
                return
            name = params.get("name")
            token = self.meta_progress_token(message)
            if name == "slow":
                self._start_slow(request_id, token, hang=self.hang_call)
                return
            if name == "crash_on_call":
                self._start_crash(request_id)
                return
            self._call_sync(request_id, name, params, token)
            return

        if method == "notifications/cancelled":
            cancelled_params = params
            request_id_value = cancelled_params.get("requestId")
            log_event(self.state, {"event": "cancelled", "params": cancelled_params})
            with self._cancelled_lock:
                self._cancelled[request_id_value] = True
            return

        if is_request:
            self.respond(
                request_id,
                error={
                    "code": -32601,
                    "message": f"Method not found: {method}",
                },
            )
            return
        # Unknown notification: ignore.
        return

    def meta_version(self, message: dict) -> Any:
        params = message.get("params")
        if isinstance(params, dict):
            meta = params.get("_meta")
            if isinstance(meta, dict):
                return meta.get("io.modelcontextprotocol/protocolVersion")
        return None

    def _call_sync(self, request_id: Any, name: Any, params: dict, token: Any) -> None:
        if name == "echo":
            value = params.get("arguments", {}).get("value")
            if self.oversized_result_bytes:
                # One single-line, newline-free text blob far larger than any
                # adapter frame cap under test: the response frame itself is
                # the oversized payload.  Never truncated by the fixture.
                self.respond(
                    request_id,
                    result={
                        "resultType": "complete",
                        "content": _text("E" * self.oversized_result_bytes),
                        "structuredContent": {"echo": value},
                        "_meta": {SERVER_INFO_KEY: self._server_info},
                    },
                )
                return
            self.respond(
                request_id,
                result={
                    "resultType": "complete",
                    "content": _text(f"echo:{value}"),
                    "structuredContent": {"echo": value},
                    "_meta": {SERVER_INFO_KEY: self._server_info},
                },
            )
            return
        if name == "tool_error":
            self.respond(
                request_id,
                result={
                    "resultType": "complete",
                    "content": _text("tool exploded"),
                    "isError": True,
                    "structuredContent": {"ok": False},
                    "_meta": {SERVER_INFO_KEY: self._server_info},
                },
            )
            return
        if name == "bad_call":
            self.respond(
                request_id,
                error={
                    "code": -32602,
                    "message": "invalid parameters for bad_call",
                    "data": {"detail": "fixture-sent"},
                },
            )
            return
        if name == "retired_error":
            self.respond(
                request_id,
                error={"code": -32042, "message": "connection closed"},
            )
            return
        self.respond(
            request_id,
            error={
                "code": -32602,
                "message": f"unknown tool {name!r}",
            },
        )

    def _is_cancelled(self, request_id: Any) -> bool:
        with self._cancelled_lock:
            return bool(self._cancelled.get(request_id))

    def _start_slow(self, request_id: Any, token: Any, hang: bool = False) -> None:
        def work() -> None:
            log_event(self.state, {"event": "call-start", "tool": "slow", "id": request_id})
            deadline = None if hang else time.monotonic() + 1.0
            if token is not None:
                self.notify(
                    "notifications/progress",
                    {"progressToken": token, "progress": 1, "total": 3, "message": "one"},
                )
            while True:
                if self._is_cancelled(request_id):
                    log_event(self.state, {"event": "call-cancelled-observed", "id": request_id})
                    self.respond(
                        request_id,
                        error={"code": -32800, "message": "cancelled by client"},
                    )
                    return
                if deadline is not None and time.monotonic() >= deadline:
                    break
                time.sleep(0.05)
            if hang:
                # Never completes (adapter request-timeout tests); the worker
                # is a daemon and dies with the process.
                return
            if token is not None:
                self.notify(
                    "notifications/progress",
                    {"progressToken": token, "progress": 3, "total": 3, "message": "three"},
                )
            log_event(self.state, {"event": "call-end", "tool": "slow", "id": request_id})
            self.respond(
                request_id,
                result={
                    "resultType": "complete",
                    "content": _text("slow-done"),
                    "_meta": {SERVER_INFO_KEY: self._server_info},
                },
            )

        threading.Thread(target=work, name="fixture-slow", daemon=True).start()

    def _start_crash(self, request_id: Any) -> None:
        def work() -> None:
            log_event(self.state, {"event": "call-start", "tool": "crash_on_call", "id": request_id})
            os._exit(3)

        threading.Thread(target=work, name="fixture-crash", daemon=True).start()


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(prog="modern_only_fixture_mcp")
    parser.add_argument("--state", default="", help="JSONL state log path.")
    parser.add_argument("--config", default="", help="Optional JSON config path.")
    args = parser.parse_args(argv)
    config: dict = {}
    if args.config:
        with open(args.config, encoding="utf-8") as handle:
            loaded = json.load(handle)
        if isinstance(loaded, dict):
            config = loaded
    fixture = ModernOnlyFixture(config, args.state)
    if fixture.stderr_token:
        sys.stderr.write(f"{fixture.stderr_token}\n")
        sys.stderr.flush()
    if fixture.stderr_flood_bytes > 0:
        # Write well past the OS pipe buffer before serving: exercises the
        # adapter's bounded stderr drain (an undrained PIPE would deadlock the
        # backend here and the first request would never be answered).  The
        # flood contains NO newline, so a reader that accumulates whole lines
        # before capping would buffer without bound.
        chunk = "S" * 32768
        remaining = fixture.stderr_flood_bytes
        while remaining > 0:
            take = min(len(chunk), remaining)
            sys.stderr.write(chunk[:take])
            remaining -= take
        sys.stderr.flush()
    for raw in sys.stdin:
        raw = raw.strip()
        if not raw:
            continue
        try:
            message = json.loads(raw)
        except ValueError:
            continue
        if not isinstance(message, dict):
            continue
        log_event(
            args.state,
            {
                "event": "recv",
                "kind": "request" if "id" in message else "notification",
                "method": message.get("method"),
                "id": message.get("id"),
            },
        )
        fixture.handle(message)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
