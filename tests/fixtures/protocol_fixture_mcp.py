#!/usr/bin/env python3
"""Tiny legacy-era (2025-06-18) stdio MCP used by the P10 dual-era projection tests.

This fixture plays the *physical shared backend* role: it speaks the legacy
initialize handshake and plain tools/list / tools/call with no modern ``_meta``
semantics, exactly like ``fixture_mcp.py`` but with a few extra tools that the
modern-projection tests need:

* ``echo``       - returns the supplied value (content + structuredContent).
* ``probe``      - echoes back the request params it actually received
                  (including ``_meta``), so tests can prove no
                  ``io.modelcontextprotocol/*`` capability/identity claims leak
                  to the backend.
* ``slow``       - emits ``notifications/progress`` for a ``progressToken`` the
                  client placed in ``params._meta.progressToken``, sleeps, then
                  returns; used for modern progress and cancellation mapping.
* ``tool_error`` - returns a tool-execution error result (``isError`` true).

It also appends one JSON line per lifecycle/tool event to the file named by the
``PROTO_FIXTURE_LOG`` environment variable (when set), so integration tests can
count initialize handshakes and business calls to prove "one physical legacy
generation, no business replay".
"""

from __future__ import annotations

import json
import os
import sys
import time

NAME = os.environ.get("FIXTURE_MCP_NAME", "protocol-fixture")
LOG_PATH = os.environ.get("PROTO_FIXTURE_LOG", "")
#: Observation knobs for the P10 follow-up regression tests:
#:  - NO_TOOLS=1      initialize advertises no tools capability (capabilities {}).
#:  - INSTRUCTIONS    initialize ``instructions`` to preserve downstream verbatim.
#:  - TASK_AUGMENTED  tools/list emits legacy task-augmented top-level fields
#:                    that the modern tools-only projection must withhold.
NO_TOOLS = os.environ.get("PROTO_FIXTURE_NO_TOOLS") == "1"
DOWNSTREAM_INSTRUCTIONS = os.environ.get("PROTO_FIXTURE_INSTRUCTIONS", "")
TASK_AUGMENTED = os.environ.get("PROTO_FIXTURE_TASK_AUGMENTED") == "1"


def _log(event: dict) -> None:
    if not LOG_PATH:
        return
    try:
        with open(LOG_PATH, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(event, separators=(",", ":")) + "\n")
    except OSError:
        pass


def _response(request_id, result=None, error=None) -> dict:
    value = {"jsonrpc": "2.0", "id": request_id}
    if error is not None:
        value["error"] = error
    else:
        value["result"] = result
    return value


def _text_content(text: str) -> list:
    return [{"type": "text", "text": text}]


for raw in sys.stdin:
    raw = raw.strip()
    if not raw:
        continue
    try:
        message = json.loads(raw)
        method = message.get("method")
        params = message.get("params")
        if not isinstance(params, dict):
            params = {}
        if method == "notifications/initialized":
            _log({"event": "initialized-notification"})
            continue
        if method == "notifications/cancelled":
            _log({"event": "cancelled-notification", "params": params})
            continue
        if method == "initialize":
            _log({"event": "initialize"})
            capabilities: dict = {}
            if not NO_TOOLS:
                capabilities = {"tools": {"listChanged": False}}
            result = {
                "protocolVersion": "2025-06-18",
                "capabilities": capabilities,
                "serverInfo": {"name": NAME, "version": "1.0.0"},
                "instructions": (
                    DOWNSTREAM_INSTRUCTIONS
                    if DOWNSTREAM_INSTRUCTIONS
                    else f"Protocol dual-era fixture for {NAME}."
                ),
            }
        elif method == "tools/list":
            _log({"event": "tools-list"})
            result = {
                "tools": [
                    {
                        "name": "echo",
                        "description": "Return the supplied value.",
                        "inputSchema": {
                            "type": "object",
                            "properties": {"value": {}},
                            "required": ["value"],
                            "additionalProperties": False,
                        },
                    },
                    {
                        "name": "probe",
                        "description": "Echo the params this tool actually received.",
                        "inputSchema": {
                            "type": "object",
                            "properties": {},
                            "additionalProperties": True,
                        },
                    },
                    {
                        "name": "slow",
                        "description": "Emit progress then return after a delay.",
                        "inputSchema": {
                            "type": "object",
                            "properties": {"delay": {"type": "number"}},
                            "additionalProperties": False,
                        },
                    },
                    {
                        "name": "tool_error",
                        "description": "Return a tool-execution error result.",
                        "inputSchema": {
                            "type": "object",
                            "properties": {"message": {"type": "string"}},
                            "additionalProperties": False,
                        },
                    },
                    {
                        "name": "retired_error",
                        "description": "Return an error carrying a retired MCP code.",
                        "inputSchema": {
                            "type": "object",
                            "properties": {"message": {"type": "string"}},
                            "additionalProperties": False,
                        },
                    },
                ]
            }
            if TASK_AUGMENTED:
                # Legacy (2025-11-25-era) task augmentation that the modern
                # tools-only projection does not advertise and must withhold.
                result["task"] = {
                    "description": "augmented legacy task",
                    "readMore": [],
                }
                result["unadvertisedLegacyField"] = {"keepOutOfModern": True}
        elif method == "tools/call":
            tool_name = params.get("name")
            arguments = params.get("arguments") or {}
            _log({"event": "tools-call", "tool": tool_name})
            if tool_name == "echo":
                value = arguments.get("value")
                result = {
                    "content": _text_content(json.dumps(value, ensure_ascii=False)),
                    "structuredContent": {"value": value, "servedBy": NAME},
                }
            elif tool_name == "probe":
                payload = {
                    "tool": tool_name,
                    "params": params,
                }
                result = {
                    "content": _text_content(
                        json.dumps(payload, ensure_ascii=False, sort_keys=True)
                    ),
                    "structuredContent": payload,
                }
            elif tool_name == "slow":
                try:
                    delay = float(arguments.get("delay") or 0.2)
                except (TypeError, ValueError):
                    delay = 0.2
                delay = max(0.0, min(delay, 3.0))
                meta = params.get("_meta") or {}
                token = meta.get("progressToken")
                steps = max(1, int(delay / 0.1))
                total = steps
                for step in range(1, steps + 1):
                    if token is not None:
                        print(
                            json.dumps(
                                {
                                    "jsonrpc": "2.0",
                                    "method": "notifications/progress",
                                    "params": {
                                        "progressToken": token,
                                        "progress": step,
                                        "total": total,
                                        "message": f"step {step}/{total}",
                                    },
                                },
                                separators=(",", ":"),
                            ),
                            flush=True,
                        )
                    time.sleep(0.1)
                result = {
                    "content": _text_content(json.dumps({"done": True})),
                    "structuredContent": {"done": True, "steps": steps, "servedBy": NAME},
                }
            elif tool_name == "tool_error":
                error_text = arguments.get("message") or "tool failed"
                result = {
                    "content": _text_content(error_text),
                    "structuredContent": {"ok": False, "error": error_text},
                    "isError": True,
                }
            elif tool_name == "retired_error":
                error_text = arguments.get("message") or "retired transport error"
                print(
                    json.dumps(
                        _response(
                            message.get("id"),
                            error={
                                "code": -32042,
                                "message": error_text,
                                "data": {"fixture": True},
                            },
                        ),
                        separators=(",", ":"),
                    ),
                    flush=True,
                )
                continue
            else:
                raise ValueError(f"unknown tool: {tool_name}")
        else:
            raise ValueError(f"method not found: {method}")
        print(json.dumps(_response(message.get("id"), result=result), separators=(",", ":")), flush=True)
    except Exception as exc:  # noqa: BLE001 - fixture keeps serving on malformed input
        rid = message.get("id") if isinstance(locals().get("message"), dict) else None
        print(
            json.dumps(
                _response(rid, error={"code": -32603, "message": str(exc)}),
                separators=(",", ":"),
            ),
            flush=True,
        )
