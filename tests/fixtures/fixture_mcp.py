#!/usr/bin/env python3
"""Tiny standard stdio MCP used only by the bridge integration tests."""

from __future__ import annotations

import base64
import hashlib
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
                    {
                        "name": "read_input",
                        "description": (
                            "Read one staged Agent-local input file by its local path and "
                            "return its bytes and digest."
                        ),
                        "inputSchema": {
                            "type": "object",
                            "properties": {"path": {"type": "string"}},
                            "required": ["path"],
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
            elif tool_name == "read_input":
                path = arguments.get("path")
                if not isinstance(path, str) or not path:
                    raise ValueError("read_input requires a path")
                stage = os.environ.get("WIN_WSL_MCP_BRIDGE_INPUT_STAGE")
                if not stage:
                    raise ValueError("read_input has no bridge input stage configured")
                target = Path(path).resolve()
                stage_root = Path(stage).resolve()
                if target == stage_root or not target.is_relative_to(stage_root):
                    raise ValueError("read_input path is outside the staged input area")
                if not target.is_file() or target.is_symlink():
                    raise ValueError("read_input staged path is not a regular file")
                payload = target.read_bytes()
                digest = hashlib.sha256(payload).hexdigest()
                result = {
                    "content": [
                        {
                            "type": "text",
                            "text": json.dumps(
                                {
                                    "name": target.name,
                                    "size": len(payload),
                                    "sha256": digest,
                                    "data": base64.b64encode(payload).decode("ascii"),
                                },
                                separators=(",", ":"),
                            ),
                        }
                    ],
                    "structuredContent": {
                        "size": len(payload),
                        "sha256": digest,
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
