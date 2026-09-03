#!/usr/bin/env python3
"""Stateful stdio MCP fixture for shared-backend lifecycle tests."""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

NAME = os.environ.get("FIXTURE_MCP_NAME", "shared-fixture-mcp")
SPAWN_LOG = Path(os.environ["FIXTURE_SPAWN_LOG"])
EVENT_LOG = Path(os.environ["FIXTURE_EVENT_LOG"])
CHILD_PID_FILE = Path(os.environ["FIXTURE_CHILD_PID_FILE"])
child: subprocess.Popen[Any] | None = None


def append(path: Path, value: str) -> None:
    with path.open("a", encoding="utf-8") as handle:
        handle.write(value + "\n")
        handle.flush()


def response(request_id: Any, *, result: Any = None, error: Any = None) -> dict[str, Any]:
    message: dict[str, Any] = {"jsonrpc": "2.0", "id": request_id}
    if error is None:
        message["result"] = result
    else:
        message["error"] = error
    return message


def emit(message: dict[str, Any]) -> None:
    print(json.dumps(message, ensure_ascii=False, separators=(",", ":")), flush=True)


def stop_child() -> bool:
    global child
    process = child
    if process is None:
        return True
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)
    append(EVENT_LOG, f"profile-released:{process.pid}")
    child = None
    return True


append(SPAWN_LOG, str(os.getpid()))
append(EVENT_LOG, f"backend-start:{os.getpid()}:{time.monotonic_ns()}")
try:
    for raw in sys.stdin:
        if not raw.strip():
            continue
        message = json.loads(raw)
        method = message.get("method")
        request_id = message.get("id")
        if method == "notifications/initialized":
            append(EVENT_LOG, "initialized")
            continue
        if method == "notifications/cancelled":
            params = message.get("params") or {}
            append(EVENT_LOG, f"cancel:{params.get('requestId')}")
            continue
        if method == "initialize":
            append(EVENT_LOG, "initialize")
            result = {
                "protocolVersion": "2025-06-18",
                "capabilities": {"tools": {"listChanged": True}},
                "serverInfo": {"name": NAME, "version": "1.0.0"},
            }
        elif method == "tools/list":
            result = {
                "tools": [
                    {"name": name, "description": name, "inputSchema": {"type": "object"}}
                    for name in (
                        "echo",
                        "browser_start",
                        "browser_session",
                        "view_change",
                        "notify",
                        "server_roundtrip",
                        "exit_after_response",
                        "fail",
                        "crash",
                    )
                ]
            }
        elif method == "tools/call":
            params = message.get("params") or {}
            name = params.get("name")
            arguments = params.get("arguments") or {}
            append(EVENT_LOG, f"call:{name}:{request_id}")
            if name == "echo":
                delay = float(arguments.get("delay", 0))
                if delay:
                    time.sleep(delay)
                metadata = params.get("_meta")
                if isinstance(metadata, dict) and "progressToken" in metadata:
                    emit(
                        {
                            "jsonrpc": "2.0",
                            "method": "notifications/progress",
                            "params": {
                                "progressToken": metadata["progressToken"],
                                "progress": 1,
                                "total": 1,
                            },
                        }
                    )
                value = arguments.get("value")
                result = {
                    "content": [{"type": "text", "text": json.dumps(value)}],
                    "structuredContent": {
                        "value": value,
                        "backendPid": os.getpid(),
                        "requestId": request_id,
                    },
                }
            elif name == "browser_start":
                if child is None or child.poll() is not None:
                    child = subprocess.Popen(
                        [sys.executable, "-c", "import time; time.sleep(300)"],
                        stdin=subprocess.DEVNULL,
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                    )
                    CHILD_PID_FILE.write_text(str(child.pid), encoding="ascii")
                    append(EVENT_LOG, f"profile-started:{child.pid}")
                result = {
                    "content": [{"type": "text", "text": "started"}],
                    "structuredContent": {"profileStarted": True, "childPid": child.pid},
                }
            elif name == "browser_session" and arguments.get("action") == "release":
                released = stop_child()
                result = {
                    "content": [{"type": "text", "text": "released"}],
                    "structuredContent": {"profileReleased": released},
                }
            elif name == "view_change":
                result = {"structuredContent": {"changed": True}, "content": []}
            elif name == "notify":
                emit(
                    {
                        "jsonrpc": "2.0",
                        "method": "notifications/tools/list_changed",
                        "params": {},
                    }
                )
                result = {"structuredContent": {"notified": True}, "content": []}
            elif name == "server_roundtrip":
                server_id = f"server-{os.getpid()}-{time.monotonic_ns()}"
                emit(
                    {
                        "jsonrpc": "2.0",
                        "id": server_id,
                        "method": "sampling/createMessage",
                        "params": {"messages": []},
                    }
                )
                nested = json.loads(sys.stdin.readline())
                result = {
                    "structuredContent": {
                        "nestedResult": nested.get("result"),
                        "nestedId": nested.get("id"),
                    },
                    "content": [],
                }
            elif name == "exit_after_response":
                emit(
                    response(
                        request_id,
                        result={
                            "content": [{"type": "text", "text": "final"}],
                            "structuredContent": {"finalResponse": True},
                        },
                    )
                )
                break
            elif name == "fail":
                emit(
                    response(
                        request_id,
                        error={"code": -32042, "message": "fixture failure"},
                    )
                )
                continue
            elif name == "crash":
                append(EVENT_LOG, f"backend-crash:{os.getpid()}")
                os._exit(23)
            else:
                raise ValueError(f"unknown tool: {name}")
        else:
            raise ValueError(f"method not found: {method}")
        emit(response(request_id, result=result))
finally:
    stop_child()
    append(EVENT_LOG, f"backend-exit:{os.getpid()}:{time.monotonic_ns()}")
