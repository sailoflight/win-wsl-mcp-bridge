#!/usr/bin/env python3
"""Tiny standard stdio MCP used only by the bridge integration tests."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

NAME = os.environ.get("FIXTURE_MCP_NAME", "fixture-mcp")
EXIT_AFTER_CALL = os.environ.get("FIXTURE_EXIT_AFTER_CALL") == "1"


def response(request_id, result=None, error=None):
    value = {"jsonrpc": "2.0", "id": request_id}
    if error is not None:
        value["error"] = error
    else:
        value["result"] = result
    return value


for raw in sys.stdin:
    raw = raw.strip()
    if not raw:
        continue
    try:
        message = json.loads(raw)
        method = message.get("method")
        if method == "notifications/initialized":
            continue
        if method == "initialize":
            result = {
                "protocolVersion": "2025-06-18",
                "capabilities": {"tools": {"listChanged": False}},
                "serverInfo": {"name": NAME, "version": "1.0.0"},
                "instructions": f"Read-only integration fixture for {NAME}.",
            }
        elif method == "tools/list":
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
                        "name": "create_artifact",
                        "description": "Create and push one text artifact into the client workspace.",
                        "inputSchema": {
                            "type": "object",
                            "properties": {"text": {"type": "string"}},
                            "required": ["text"],
                            "additionalProperties": False,
                        },
                    },
                ]
            }
        elif method == "tools/call":
            params = message.get("params") or {}
            tool_name = params.get("name")
            arguments = params.get("arguments") or {}
            if tool_name == "echo":
                value = arguments.get("value")
                result = {
                    "content": [{"type": "text", "text": json.dumps(value)}],
                    "structuredContent": {"value": value, "servedBy": NAME},
                }
            elif tool_name == "create_artifact":
                text = arguments.get("text")
                if not isinstance(text, str):
                    raise ValueError("create_artifact requires text")
                stage = Path(os.environ["WIN_WSL_MCP_BRIDGE_ARTIFACT_STAGE"])
                filename = "fixture-result.txt"
                (stage / filename).write_text(text, encoding="utf-8")
                publisher = subprocess.run(
                    [
                        os.environ["WIN_WSL_MCP_BRIDGE_ARTIFACT_PYTHON"],
                        os.environ["WIN_WSL_MCP_BRIDGE_ARTIFACT_PUBLISHER"],
                        "publish",
                        filename,
                        "--name",
                        filename,
                        "--media-type",
                        "text/plain",
                    ],
                    text=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    timeout=320,
                    check=False,
                )
                if publisher.returncode != 0:
                    raise ValueError(publisher.stderr.strip() or "artifact publish failed")
                published = json.loads(publisher.stdout)
                result = {
                    "content": [published["artifact"]],
                    "structuredContent": {
                        "artifact": published["artifact"],
                        "servedBy": NAME,
                    },
                }
            else:
                raise ValueError("unknown tool")
        else:
            raise ValueError(f"method not found: {method}")
        output = response(message.get("id"), result=result)
    except Exception as exc:
        output = response(
            message.get("id") if isinstance(locals().get("message"), dict) else None,
            error={"code": -32603, "message": str(exc)},
        )
    print(json.dumps(output, separators=(",", ":")), flush=True)
    if method == "tools/call" and EXIT_AFTER_CALL:
        break
