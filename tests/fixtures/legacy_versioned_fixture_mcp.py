#!/usr/bin/env python3
"""Era-graded LEGACY stdio MCP fixture for shared-backend physical-baseline
tests (canonical legacy request ``2025-11-25`` with server-choose answers).

The Bridge opens its physical shared session by requesting the canonical
legacy revision and the backend answers with its own supported revision at or
below the request.  This fixture models that server-choose behaviour: it owns
exactly one legacy revision (``actual_version``, one of the four frozen
profiles) and answers ``initialize`` with that revision regardless of the
request, exactly as a real legacy MCP does.  Its catalog and call results are
era-graded by ``actual_version`` so tests can prove both directions:

* the Bridge *requests* ``2025-11-25`` (recorded in the state log) and
  *accepts/records* whichever verified legacy revision the fixture answers;
* a ``2025-11-25``-actual backend emits era fields (``annotations``,
  ``title``, ``outputSchema``, plural ``icons``, ``structuredContent``) and the
  Bridge's four-profile projection strips them back for older logical clients.

Protocol behaviour:

* ``initialize`` -> result with ``protocolVersion = actual_version``,
  ``capabilities = {"tools": {"listChanged": true}}``, serverInfo and
  instructions.  Configurable failure modes prove fail-closed validation of a
  malicious/missing/modern backend:
  - ``mode: "modern_only"``       -> backend answers ``-32601`` (no legacy
    initialize, like a 2026-07-28-only server).
  - ``mode: "missing_version"``   -> success-shaped result without
    ``protocolVersion``.
  - ``mode: "garbage_version"``   -> success result whose ``protocolVersion``
    is a modern revision (``2026-07-28``).
* ``tools/list`` -> one page whose tools carry the era fields their
  ``actual_version`` can express (kept self-contained: same era anchors the
  runtime's pure projection uses).  Catalog is bounded to ``echo`` and
  ``variants``.
* ``tools/call``:
  - ``echo``        -> content + ``structuredContent`` (the latter only when
    actual >= 2025-06-18), echoing ``value``/``requestId``/``backendPid``.
  - ``variants``    -> mixed content exercising every content kind its actual
    can emit: text (with item ``_meta`` once actual >= 2025-06-18), image,
    EmbeddedResource, audio (actual >= 2025-03-26) and resource_link
    (actual >= 2025-06-18).  Lets tests prove per-profile content-kind
    projection and the explicit failure for unrepresentable kinds.
  - ``fail``        -> JSON-RPC ``-32042`` retired-code error.
  - ``crash``       -> logs the event and exits the process immediately
    (restart/recovery tests).
  - ``notify``      -> returns ``notifications/tools/list_changed`` before the
    result (exercise of the shared list_changed broadcast).

Every frame and lifecycle event is appended as one JSON line to ``--state`` so
tests read the fixture's own perspective (spawns, initialize request params,
business calls, crashes).

Invocation (operator/test use)::

    python3 tests/fixtures/legacy_versioned_fixture_mcp.py --state <file> [--config <file>]

Config keys (all optional): ``actual_version`` (one of the four frozen legacy
revisions; default ``2025-11-25``), ``server_name`` (default
``legacy-versioned-fixture``), ``instructions`` (default ``Synthetic versioned
shared-backend instructions.``; empty string suppresses the field),
``mode`` (``legacy`` | ``modern_only`` | ``missing_version`` |
``garbage_version``), ``structured_content`` (bool override, default follows
actual >= 2025-06-18), and ``extra_capabilities`` (object merged into the
initialize result's ``capabilities`` -- used by acceptance tests to have a
2025-11-25 actual advertise families the bridge must never replay, such as
``tasks`` or unknown keys).  The advertised tools subset is deliberately
bounded: echo/variants/fail/crash/notify.  It is the fixture's whole catalog
and never grows.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import threading
import time
from typing import Any

LEGACY_VERSIONS = ("2024-11-05", "2025-03-26", "2025-06-18", "2025-11-25")
ANNOTATIONS_ANCHOR = "2025-03-26"
TITLE_OUTPUT_SCHEMA_ANCHOR = "2025-06-18"
STRUCTURED_CONTENT_ANCHOR = "2025-06-18"
ICONS_ANCHOR = "2025-11-25"

_log_lock = threading.Lock()
_children: list[Any] = []


def log_event(state_path: str, event: dict) -> None:
    if not state_path:
        return
    with _log_lock:
        try:
            with open(state_path, "a", encoding="utf-8") as handle:
                handle.write(json.dumps(event, ensure_ascii=False) + "\n")
                handle.flush()
        except OSError:
            pass


def at_least(version: str, anchor: str) -> bool:
    return version >= anchor


def era_graded_tool(version: str, name: str, description: str,
                    schema: dict) -> dict[str, Any]:
    """One tool entry carrying every era field its version can express."""
    tool: dict[str, Any] = {
        "name": name,
        "description": description,
        "inputSchema": schema,
    }
    if at_least(version, ANNOTATIONS_ANCHOR):
        tool["annotations"] = {
            "title": f"{name} tool",
            "readOnlyHint": True,
        }
    if at_least(version, TITLE_OUTPUT_SCHEMA_ANCHOR):
        tool["title"] = f"{name} tool"
        tool["outputSchema"] = {"type": "object"}
    if at_least(version, ICONS_ANCHOR):
        tool["icons"] = [
            {
                "src": f"https://example.invalid/{name}.png",
                "mimeType": "image/png",
            }
        ]
    return tool


def era_graded_catalog(version: str) -> list[dict[str, Any]]:
    """The bounded fixture catalog: echo and variants, both era-graded."""
    return [
        era_graded_tool(
            version,
            "echo",
            "echo a value back",
            {
                "type": "object",
                "properties": {"value": {"type": "string"}},
                "required": ["value"],
            },
        ),
        era_graded_tool(
            version,
            "variants",
            "return mixed content kinds this revision can emit",
            {"type": "object"},
        ),
    ]


def mixed_variants_content(version: str, request_id: Any) -> dict[str, Any]:
    """One mixed-content result: every content kind the actual can emit.

    Kept faithful to the real schema introduction dates (text/image/resource
    always; audio from 2025-03-26; resource_link from 2025-06-18; item-level
    ``_meta`` from 2025-06-18), so a server at the canonical actual emits the
    full set and older logical profiles must either represent or explicitly
    reject it.
    """
    content: list[dict[str, Any]] = [
        {
            "type": "text",
            "text": f"mixed-{request_id}",
        },
        {"type": "image", "data": "AA==", "mimeType": "image/png"},
        {
            "type": "resource",
            "resource": {
                "uri": "file:///blob.bin",
                "mimeType": "application/octet-stream",
                "blob": "AAEC",
            },
        },
    ]
    if at_least(version, STRUCTURED_CONTENT_ANCHOR):
        # Item-level _meta exists from 2025-06-18; surface it so projections
        # that must strip or preserve it are exercised end to end.
        content[0]["_meta"] = {"fixture": True}
    if at_least(version, "2025-03-26"):
        content.append(
            {"type": "audio", "data": "QUJD", "mimeType": "audio/wav"}
        )
    if at_least(version, "2025-06-18"):
        content.append({"type": "resource_link", "uri": "file:///ref.txt"})
    return {"content": content}


def parse_config(config_path: str | None) -> dict[str, Any]:
    config: dict[str, Any] = {"actual_version": "2025-11-25", "mode": "legacy"}
    if config_path:
        with open(config_path, encoding="utf-8") as handle:
            loaded = json.load(handle)
            if isinstance(loaded, dict):
                config.update(loaded)
    actual = config.get("actual_version")
    if actual not in LEGACY_VERSIONS:
        raise SystemExit(
            f"legacy-versioned fixture: invalid actual_version {actual!r}"
        )
    config["actual_version"] = actual
    return config


def respond(request_id: Any, *, result: Any = None, error: Any = None) -> None:
    message: dict[str, Any] = {"jsonrpc": "2.0", "id": request_id}
    if error is None:
        message["result"] = result
    else:
        message["error"] = error
    print(json.dumps(message, ensure_ascii=False, separators=(",", ":")), flush=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", required=True, help="state JSONL log path")
    parser.add_argument("--config", default=None, help="optional config JSON path")
    args = parser.parse_args()
    config = parse_config(args.config)
    actual = config["actual_version"]
    mode = config.get("mode", "legacy")
    name = config.get("server_name", "legacy-versioned-fixture")
    instructions = config.get(
        "instructions", "Synthetic versioned shared-backend instructions."
    )
    if not isinstance(instructions, str) or not instructions:
        instructions = None
    include_structured = bool(
        config.get("structured_content", at_least(actual, STRUCTURED_CONTENT_ANCHOR))
    )
    pid = os.getpid()
    log_event(args.state, {"event": "start", "pid": pid, "actual": actual,
                           "mode": mode, "at": time.monotonic_ns()})
    try:
        for raw in sys.stdin:
            raw = raw.strip()
            if not raw:
                continue
            try:
                message = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if not isinstance(message, dict):
                continue
            method = message.get("method")
            request_id = message.get("id")
            params = message.get("params")
            params = params if isinstance(params, dict) else {}
            log_event(
                args.state,
                {"event": "frame", "method": method, "params": params,
                 "pid": pid, "at": time.monotonic_ns()},
            )
            if method == "notifications/initialized":
                continue
            if method == "initialize":
                if mode == "modern_only":
                    respond(
                        request_id,
                        error={
                            "code": -32601,
                            "message": "Method not found: initialize",
                        },
                    )
                    continue
                result: dict[str, Any] = {
                    "protocolVersion": actual,
                    "capabilities": {"tools": {"listChanged": True}},
                    "serverInfo": {"name": name, "version": "1.0.0"},
                }
                extra = config.get("extra_capabilities")
                if isinstance(extra, dict) and extra:
                    merged = dict(result["capabilities"])
                    merged.update(extra)
                    result["capabilities"] = merged
                if mode == "missing_version":
                    result.pop("protocolVersion", None)
                elif mode == "garbage_version":
                    result["protocolVersion"] = "2026-07-28"
                if instructions is not None:
                    result["instructions"] = instructions
                respond(request_id, result=result)
                continue
            if method == "tools/list":
                respond(
                    request_id,
                    result={"tools": era_graded_catalog(actual)},
                )
                continue
            if method == "tools/call":
                tool_name = params.get("name")
                if tool_name == "echo":
                    arguments = params.get("arguments")
                    arguments = arguments if isinstance(arguments, dict) else {}
                    result: dict[str, Any] = {
                        "content": [
                            {
                                "type": "text",
                                "text": json.dumps(arguments.get("value")),
                            }
                        ]
                    }
                    if include_structured:
                        result["structuredContent"] = {
                            "value": arguments.get("value"),
                            "requestId": request_id,
                            "backendPid": pid,
                        }
                    respond(request_id, result=result)
                    continue
                if tool_name == "notify":
                    log_event(
                        args.state, {"event": "notify", "pid": pid,
                                     "at": time.monotonic_ns()}
                    )
                    print(
                        json.dumps(
                            {
                                "jsonrpc": "2.0",
                                "method": "notifications/tools/list_changed",
                                "params": {},
                            },
                            separators=(",", ":"),
                        ),
                        flush=True,
                    )
                    respond(request_id, result={"content": []})
                    continue
                if tool_name == "variants":
                    respond(request_id, result=mixed_variants_content(actual, request_id))
                    continue
                if tool_name == "fail":
                    respond(
                        request_id,
                        error={"code": -32042, "message": "fixture failure"},
                    )
                    continue
                if tool_name == "crash":
                    log_event(
                        args.state,
                        {"event": "crash", "pid": pid, "at": time.monotonic_ns()},
                    )
                    os._exit(23)
                respond(
                    request_id,
                    error={
                        "code": -32602,
                        "message": f"unknown tool: {tool_name!r}",
                    },
                )
                continue
            if method == "ping":
                respond(request_id, result={})
                continue
            respond(
                request_id,
                error={"code": -32601, "message": f"method not found: {method}"},
            )
    finally:
        log_event(args.state, {"event": "exit", "pid": pid, "at": time.monotonic_ns()})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
