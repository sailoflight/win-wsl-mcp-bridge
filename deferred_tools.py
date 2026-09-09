"""Opt-in per-target deferred tools facade for the verified legacy MCP profiles.

One invocation owns one upstream MCP connection and its reconnectable downstream.
Expansion is connection-scoped: agents sharing a host-mounted MCP process share
its tool view. Independent agent-session isolation is not provided. It advertises
only tools, starts collapsed, and never retries a business call. Catalog changes
request client re-listing; notification delivery is not a model-readiness barrier.
The parent CLI owns enrollment/transport selection. No private registry fields,
process launch definitions, resources, prompts, subscriptions, or modern discovery
are consumed here. Reconnection is demand-driven and preserves expansion intent.
"""
from __future__ import annotations

import copy
import json
import socket
import sys
import threading
import time
import uuid
from typing import Any, Callable

import bridge_protocol
import bridge_runtime

LIBRARY_TOOL_NAME = "bridge_library"
MAX_MESSAGE_BYTES = 16 * 1024 * 1024
MAX_CATALOG_BYTES = 4 * 1024 * 1024
MAX_SCHEMA_BYTES = 256 * 1024
MAX_TOOLS = 1000
MAX_PAGES = 64
MAX_CURSOR_BYTES = 1024
MAX_NAME_CHARS = 256
MAX_INSTRUCTIONS_BYTES = 64 * 1024
CONNECT_TIMEOUT = 10.0
REQUEST_TIMEOUT = 60.0
CATALOG_TIMEOUT = 120.0
PROFILE_NOTE = (
    "\nBridge deferred-tools facade (legacy, tools-only): initially only "
    "bridge_library is listed. Call bridge_library with action expand to expose "
    "this target's tools, collapse to hide them, or status to inspect this connection. "
    "The view is connection-scoped: agents sharing this MCP process share its "
    "expansion state; it is not isolated per agent session. "
    "Expand only when needed, then keep the library expanded across related work "
    "and follow-up turns. Do not collapse after each call or routinely at turn end. "
    "Collapse only on user request or when a sustained context reduction is worth "
    "the prompt-cache rebuild cost; consider other agents sharing the connection. "
    "View changes can turn cached prefix tokens into uncached input. "
    "Expand/collapse changes the bridge directory in this turn and requests client "
    "re-listing. Added tools can be called in a subsequent model request after client refresh. "
    "Removed tools cannot be called until re-exposed. The bridge_library entry remains "
    "available: if the remaining task needs this library, expand it and then continue; "
    "collapse does not make the task impossible. A later step of the same "
    "conversation turn is sufficient; a new user message is not required. Never "
    "batch a view switch with a downstream call or infer failure from the old "
    "request's schema snapshot. "
    "If definitions are still stale, report pending client refresh rather than "
    "guessing tool names or repeatedly switching. Tool names remembered from "
    "conversation history are not current callable definitions. refreshRequested describes this "
    "response's notification request, not an acknowledgement of client/model readiness; "
    "status and toolCount describe only the bridge. Resources, prompts, subscriptions, "
    "tasks, sampling, elicitation, and roots are not forwarded. Client cancellation "
    "notifications are not forwarded; a timeout does not prove execution stopped. "
    "A disconnected call has an unknown outcome and is never replayed automatically."
)


class _Fault(Exception):
    def __init__(self, reason: str, *, code: int = -32001,
                 unknown: bool = False, retryable: bool = False):
        super().__init__(reason)
        self.error = {
            "code": code, "message": reason,
            "data": {"reason": reason, "outcomeUnknown": unknown,
                     "retryable": retryable},
        }


class _DownstreamFault(Exception):
    def __init__(self, error: dict[str, Any]):
        super().__init__("downstream MCP rejected request")
        self.error = error


def _encode(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"),
                      allow_nan=False).encode("utf-8")


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def _decode(raw: bytes) -> Any:
    def invalid_constant(_value: str) -> None:
        raise ValueError("non-finite JSON number")
    value = json.loads(raw, object_pairs_hook=_unique_object,
                       parse_constant=invalid_constant)
    # JSON exponents can overflow without parse_constant being called.
    _encode(value)
    return value


def _valid_id(value: Any) -> bool:
    return ((isinstance(value, str) and len(value) <= 256)
            or (type(value) is int and abs(value) <= 2**53 - 1))


def _short_text(value: Any, limit: int) -> str:
    if not isinstance(value, str):
        return ""
    return " ".join(value.split())[:limit]


def _validate_schema(schema: Any, *, root: bool = True, depth: int = 0) -> None:
    """Validate bounded schema structure, not instances or arbitrary dialects.

    Preserve extension keywords and references verbatim; never fetch/resolve a
    schema. This is deliberately not a replacement JSON Schema implementation.
    """
    if depth > 64 or (root and (not isinstance(schema, dict)
                               or schema.get("type") != "object")):
        raise _Fault("invalid_tool_schema")
    if type(schema) is bool and not root:
        return
    if not isinstance(schema, dict):
        raise _Fault("invalid_tool_schema")
    types = {"null", "boolean", "object", "array", "number", "integer", "string"}
    if "type" in schema:
        declared = schema["type"]
        values = declared if isinstance(declared, list) else [declared]
        if (not values or any(not isinstance(v, str) or v not in types for v in values)
                or len(set(values)) != len(values)):
            raise _Fault("invalid_tool_schema")
    for key in ("properties", "patternProperties", "$defs", "definitions", "dependentSchemas"):
        if key in schema:
            if not isinstance(schema[key], dict):
                raise _Fault("invalid_tool_schema")
            for value in schema[key].values():
                _validate_schema(value, root=False, depth=depth + 1)
    for key in ("allOf", "anyOf", "oneOf", "prefixItems"):
        if key in schema:
            if not isinstance(schema[key], list) or not schema[key]:
                raise _Fault("invalid_tool_schema")
            for value in schema[key]:
                _validate_schema(value, root=False, depth=depth + 1)
    for key in ("additionalProperties", "additionalItems", "unevaluatedProperties",
                "unevaluatedItems", "contains", "not", "if", "then", "else", "propertyNames"):
        if key in schema:
            _validate_schema(schema[key], root=False, depth=depth + 1)
    if "items" in schema:
        items = schema["items"]
        for value in (items if isinstance(items, list) else [items]):
            _validate_schema(value, root=False, depth=depth + 1)
    if "required" in schema:
        required = schema["required"]
        if (not isinstance(required, list) or any(not isinstance(v, str) for v in required)
                or len(set(required)) != len(required)):
            raise _Fault("invalid_tool_schema")
    for key in ("$ref", "$id", "$schema", "pattern", "format"):
        if key in schema and not isinstance(schema[key], str):
            raise _Fault("invalid_tool_schema")
    if "enum" in schema and (not isinstance(schema["enum"], list) or not schema["enum"]):
        raise _Fault("invalid_tool_schema")
    for key in ("minLength", "maxLength", "minItems", "maxItems", "minProperties",
                "maxProperties", "minContains", "maxContains"):
        if key in schema and (type(schema[key]) is not int or schema[key] < 0):
            raise _Fault("invalid_tool_schema")
    for key in ("minimum", "maximum", "multipleOf"):
        if key in schema and (type(schema[key]) not in (int, float)
                              or (key == "multipleOf" and schema[key] <= 0)):
            raise _Fault("invalid_tool_schema")
    for key in ("readOnly", "writeOnly", "deprecated", "uniqueItems"):
        if key in schema and type(schema[key]) is not bool:
            raise _Fault("invalid_tool_schema")


def _validate_tool(tool: Any, version: str) -> dict[str, Any]:
    if not isinstance(tool, dict):
        raise _Fault("invalid_tool_definition")
    name = tool.get("name")
    if (not isinstance(name, str) or not name.strip() or len(name) > MAX_NAME_CHARS
            or any(ord(c) < 32 for c in name)):
        raise _Fault("invalid_tool_name")
    if name == LIBRARY_TOOL_NAME:
        raise _Fault("reserved_tool_collision")
    for field in ("inputSchema", "outputSchema"):
        if field == "outputSchema" and field not in tool:
            continue
        schema = tool.get(field)
        if len(_encode(schema)) > MAX_SCHEMA_BYTES:
            raise _Fault("tool_schema_too_large")
        _validate_schema(schema)
    for field in ("description", "title"):
        if field in tool and not isinstance(tool[field], str):
            raise _Fault("invalid_tool_definition")
    if "annotations" in tool:
        annotations = tool["annotations"]
        if not isinstance(annotations, dict):
            raise _Fault("invalid_tool_annotations")
        for field in ("readOnlyHint", "destructiveHint", "idempotentHint", "openWorldHint"):
            if field in annotations and type(annotations[field]) is not bool:
                raise _Fault("invalid_tool_annotations")
        if "title" in annotations and not isinstance(annotations["title"], str):
            raise _Fault("invalid_tool_annotations")
    if "icons" in tool and (not isinstance(tool["icons"], list) or any(
            not isinstance(icon, dict) or not isinstance(icon.get("src"), str)
            for icon in tool["icons"])):
        raise _Fault("invalid_tool_icons")
    execution = tool.get("execution", {})
    if not isinstance(execution, dict):
        raise _Fault("invalid_tool_execution")
    if execution.get("taskSupport") == "required":
        raise _Fault("required_tasks_not_supported")
    result = bridge_protocol.project_legacy_tool(version, tool)
    assert result is not None
    return copy.deepcopy(result)


class _Pending:
    def __init__(self, request_id: str, method: str, params: dict[str, Any]):
        self.request_id = request_id
        self.method = method
        self.progress_token = params.get("_meta", {}).get("progressToken")
        self.event = threading.Event()
        self.message: dict[str, Any] | None = None
        self.failure: _Fault | None = None


class _Peer:
    """One bounded in-flight exchange plus an always-running downstream reader."""
    def __init__(self, sock: socket.socket,
                 changed: Callable[[str], None], emit: Callable[[dict[str, Any]], None]):
        self.sock = sock
        self.changed = changed
        self.emit = emit
        self.lock = threading.Lock()
        self.write_lock = threading.Lock()
        self.pending: _Pending | None = None
        self.counter = 0
        self.closed = False
        self.reader = threading.Thread(target=self._read, name="deferred-mcp-reader", daemon=True)
        self.reader.start()

    def _send(self, message: dict[str, Any]) -> None:
        raw = _encode(message) + b"\n"
        if len(raw) > MAX_MESSAGE_BYTES:
            raise _Fault("downstream_message_too_large")
        with self.write_lock:
            self.sock.sendall(raw)

    def _fail(self, reason: str) -> None:
        with self.lock:
            if self.closed:
                return
            self.closed = True
            if self.pending is not None and not self.pending.event.is_set():
                self.pending.failure = _Fault(
                    reason, unknown=self.pending.method == "tools/call", retryable=True)
                self.pending.event.set()
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self.sock.close()
        self.changed(reason)

    def close(self) -> None:
        self._fail("downstream_unavailable")
        if threading.current_thread() is not self.reader:
            self.reader.join(timeout=1)

    def notify(self, method: str, params: dict[str, Any]) -> None:
        try:
            self._send({"jsonrpc": "2.0", "method": method, "params": params})
        except (OSError, ValueError, _Fault):
            self._fail("downstream_unavailable")
            raise _Fault("downstream_unavailable", retryable=True)

    def request(self, method: str, params: dict[str, Any], timeout: float | None = None) -> Any:
        if timeout is None:
            timeout = REQUEST_TIMEOUT
        with self.lock:
            if self.closed:
                raise _Fault("downstream_unavailable", retryable=True)
            if self.pending is not None:
                raise _Fault("downstream_busy")
            self.counter += 1
            pending = _Pending(f"deferred-{self.counter}", method, params)
            self.pending = pending
        try:
            try:
                self._send({"jsonrpc": "2.0", "id": pending.request_id,
                            "method": method, "params": params})
            except (ValueError, _Fault):
                # Encoding/size validation happens before any write. This local
                # refusal must not claim that a business outcome is unknown.
                pending.failure = _Fault("downstream_request_not_sent", code=-32602)
                pending.event.set()
            except OSError:
                self._fail("downstream_send_failed")
            if not pending.event.wait(timeout):
                self._fail("downstream_timeout")
            if pending.failure is not None:
                raise pending.failure
            message = pending.message
            if message is None:
                raise _Fault("downstream_unavailable", unknown=method == "tools/call")
            if "error" in message:
                raise _DownstreamFault(message["error"])
            return message["result"]
        finally:
            with self.lock:
                self.pending = None

    def _read(self) -> None:
        buffer = bytearray()
        try:
            while not self.closed:
                try:
                    chunk = self.sock.recv(65536)
                except socket.timeout:
                    continue  # Idle connections remain open; exchanges have explicit deadlines.
                if not chunk:
                    raise EOFError
                buffer.extend(chunk)
                while b"\n" in buffer:
                    offset = buffer.index(b"\n") + 1
                    if offset > MAX_MESSAGE_BYTES:
                        raise ValueError
                    raw = bytes(buffer[:offset])
                    del buffer[:offset]
                    self._receive(_decode(raw))
                if len(buffer) > MAX_MESSAGE_BYTES:
                    raise ValueError
        except (OSError, EOFError, ValueError, TypeError, RecursionError, _Fault):
            self._fail("downstream_unavailable")

    def _receive(self, message: Any) -> None:
        if not isinstance(message, dict) or message.get("jsonrpc") != "2.0":
            raise ValueError
        method = message.get("method")
        if method is not None:
            if not isinstance(method, str) or "result" in message or "error" in message:
                raise ValueError
            if "id" in message:
                if not _valid_id(message["id"]):
                    raise ValueError
                # Do not queue server requests behind an outstanding client request.
                # No client capability is advertised. Ping is capability-free.
                answer: dict[str, Any] = {"jsonrpc": "2.0", "id": message["id"]}
                if method == "ping":
                    answer["result"] = {}
                else:
                    answer["error"] = {"code": -32601, "message": "tools-only facade: server request unsupported"}
                self._send(answer)
                return
            if method == "notifications/tools/list_changed":
                self.changed("list_changed")
            elif method == "notifications/progress":
                params = message.get("params")
                with self.lock:
                    pending = self.pending
                    token = pending.progress_token if pending is not None else None
                if (token is not None and isinstance(params, dict)
                        and type(params.get("progressToken")) is type(token)
                        and params.get("progressToken") == token):
                    self.emit(message)
            return
        if (not _valid_id(message.get("id"))
                or ("result" in message) == ("error" in message)):
            raise ValueError
        if "error" in message:
            error = message["error"]
            if (not isinstance(error, dict) or type(error.get("code")) is not int
                    or not isinstance(error.get("message"), str)):
                raise ValueError
        with self.lock:
            pending = self.pending
            if pending is None or message["id"] != pending.request_id or pending.event.is_set():
                raise ValueError  # Unknown/duplicate responses never accumulate in a queue.
            pending.message = message
            pending.event.set()


class DeferredSession:
    """Upstream connection engine; all agents using this connection share its state.

    ``emit`` must serialize JSON-RPC messages to client output. ``connection_id``
    identifies this facade lifetime, not a downstream transport generation or
    an agent session, and survives demand-driven downstream reconnects.
    """
    def __init__(self, local_host: str, local_port: int, target: str,
                 emit: Callable[[dict[str, Any]], None]):
        self.local_host, self.local_port, self.target = local_host, local_port, target
        self.connection_id = uuid.uuid4().hex
        self.state_revision = 0
        self.emit = emit
        self.lock = threading.Lock()
        self.peer: _Peer | None = None
        self.metadata: dict[str, str] | None = None
        self.initialize_result: dict[str, Any] | None = None
        self.requested_version: str | None = None
        self.version: str | None = None
        self.client_ready = False
        self.closed = False
        self.expanded = False
        self.catalog: list[dict[str, Any]] | None = None
        # Last bridge-published names, retained across invalidation only for
        # directory deltas. Never used to authorize calls or serve stale schemas.
        self.view_names: tuple[str, ...] = ()
        self.epoch = 0
        self.revision = 0
        self.reason = "unobserved"

    def close(self) -> None:
        self.closed = True
        self.client_ready = False
        if self.peer is not None:
            self.peer.close()
        with self.lock:
            self.catalog = None
            self.expanded = False
            self.view_names = ()

    def _notice(self) -> None:
        if self.client_ready and not self.closed:
            self.emit({"jsonrpc": "2.0", "method": "notifications/tools/list_changed"})

    def _invalidate(self, reason: str) -> None:
        with self.lock:
            self.epoch += 1
            self.state_revision += 1
            self.catalog = None
            self.reason = reason
            notify = self.expanded
        if notify:
            self._notice()

    def _metadata(self) -> None:
        if self.metadata is not None:
            return
        if (not isinstance(self.target, str) or not bridge_runtime.ID_PATTERN.fullmatch(self.target)
                or not bridge_runtime._is_loopback(self.local_host)):
            raise _Fault("invalid_target_or_host", code=-32602)
        try:
            entry = bridge_runtime.local_registry_query(
                self.local_host, self.local_port, "remote", "describe", {"id": self.target})
        except (OSError, ValueError, bridge_runtime.BridgeError):
            raise _Fault("registry_describe_unavailable", retryable=True) from None
        if not isinstance(entry, dict) or entry.get("id") != self.target:
            raise _Fault("invalid_registry_description")
        transport = entry.get("transport", {"type": "stdio"})
        if not isinstance(transport, dict) or transport.get("type") != "stdio":
            raise _Fault("deferred_facade_requires_stdio_target")
        self.metadata = {
            "id": self.target,
            "name": _short_text(entry.get("name"), 128) or self.target,
            "description": _short_text(entry.get("summary", entry.get("description")), 400),
        }

    def _connect(self) -> None:
        if self.peer is not None and not self.peer.closed:
            return
        if self.peer is not None:
            self.peer.close()
        self._metadata()
        sock = None
        try:
            sock = socket.create_connection((self.local_host, self.local_port), timeout=CONNECT_TIMEOUT)
            sock.sendall(_encode({"op": "connect", "target": self.target}) + b"\n")
            ack = _decode(bridge_runtime._recv_line(sock))
            if not isinstance(ack, dict) or ack.get("ok") is not True:
                raise _Fault("bridge_connect_refused", retryable=True)
        except (OSError, ValueError, bridge_runtime.BridgeError, _Fault):
            if sock is not None:
                sock.close()
            self._invalidate("downstream_unavailable")
            raise _Fault("downstream_unavailable", retryable=True) from None
        peer = _Peer(sock, self._invalidate, self.emit)
        self.peer = peer
        try:
            result = peer.request("initialize", {
                "protocolVersion": self.requested_version,
                "capabilities": {},
                "clientInfo": {"name": "win-wsl-mcp-bridge-deferred",
                               "version": bridge_runtime.SERVER_VERSION},
            })
            if (not isinstance(result, dict)
                    or not bridge_protocol.is_verified_legacy_protocol_version(result.get("protocolVersion"))
                    or not isinstance(result.get("capabilities"), dict)
                    or not isinstance(result["capabilities"].get("tools"), dict)
                    or not isinstance(result.get("serverInfo"), dict)
                    or not isinstance(result["serverInfo"].get("name"), str)
                    or not isinstance(result["serverInfo"].get("version"), str)):
                raise _Fault("unverified_downstream_initialize")
            instructions = result.get("instructions", "")
            if not isinstance(instructions, str) or len(instructions.encode("utf-8")) > MAX_INSTRUCTIONS_BYTES:
                raise _Fault("invalid_downstream_instructions")
            version = result["protocolVersion"]
            if self.initialize_result is not None and (
                    version != self.version or instructions != self.initialize_result.get("instructions", "")):
                raise _Fault("downstream_contract_changed_restart_required")
            peer.notify("notifications/initialized", {})
            if peer.closed:
                raise _Fault("downstream_unavailable", retryable=True)
            self.version = version
            self.initialize_result = copy.deepcopy(result)
            self._invalidate("reconnected" if self.client_ready else "unobserved")
        except Exception:
            peer.close()
            raise

    def _library_tool(self) -> dict[str, Any]:
        assert self.metadata is not None
        description = (
            f"{self.metadata['name']}: {self.metadata['description']} "
            "Expand/collapse this connection's tool list now. Added tools can be called in a subsequent model "
            "request after client refresh. A later step in the same "
            "turn is sufficient. Do not batch a switch with downstream calls. "
            "Shared across agents on this connection. Expand only when needed; keep "
            "expanded for related work. Avoid routine collapse: switching can rebuild "
            "the prompt cache and increase uncached input cost. History is not the callable list. "
            "Status/toolCount are bridge state; refreshRequested is not an acknowledgement "
            "of client/model readiness."
        )
        return {"name": LIBRARY_TOOL_NAME, "description": description.strip(),
                "inputSchema": {"type": "object", "properties": {
                    "action": {"type": "string", "enum": ["expand", "collapse", "status"]}},
                    "required": ["action"], "additionalProperties": False}}

    def _catalog(self) -> list[dict[str, Any]]:
        self._connect()
        with self.lock:
            if self.catalog is not None:
                return copy.deepcopy(self.catalog)
            epoch = self.epoch
        assert self.peer is not None and self.version is not None
        peer = self.peer
        tools: list[dict[str, Any]] = []
        names: set[str] = set()
        cursors: set[str] = set()
        cursor = None
        size = 0
        deadline = time.monotonic() + CATALOG_TIMEOUT
        try:
            for _page in range(MAX_PAGES):
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise _Fault("catalog_timeout", retryable=True)
                result = peer.request("tools/list", {} if cursor is None else {"cursor": cursor},
                                      timeout=min(REQUEST_TIMEOUT, remaining))
                if not isinstance(result, dict) or not isinstance(result.get("tools"), list):
                    raise _Fault("invalid_catalog_page")
                size += len(_encode(result))
                if size > MAX_CATALOG_BYTES or len(tools) + len(result["tools"]) > MAX_TOOLS:
                    raise _Fault("catalog_limit_exceeded")
                for raw_tool in result["tools"]:
                    tool = _validate_tool(raw_tool, self.version)
                    if tool["name"] in names:
                        raise _Fault("duplicate_tool_name")
                    names.add(tool["name"])
                    tools.append(tool)
                if "nextCursor" not in result:
                    break
                cursor = result["nextCursor"]
                if (not isinstance(cursor, str) or not cursor
                        or len(cursor.encode("utf-8")) > MAX_CURSOR_BYTES or cursor in cursors):
                    raise _Fault("invalid_catalog_cursor")
                cursors.add(cursor)
            else:
                raise _Fault("catalog_page_limit_exceeded")
            with self.lock:
                if self.epoch != epoch or peer.closed:
                    raise _Fault("catalog_changed_during_refresh", retryable=True)
                self.catalog = tools
                if self.expanded:
                    self.view_names = tuple(tool["name"] for tool in tools)
                self.revision += 1
                self.state_revision += 1
                self.reason = "verified"
                return copy.deepcopy(tools)
        except Exception:
            self._invalidate("catalog_refresh_failed")
            raise

    def status(self, *, refresh_requested: bool = False) -> dict[str, Any]:
        with self.lock:
            valid = self.catalog is not None and self.peer is not None and not self.peer.closed
            return {"target": self.target, "expanded": self.expanded,
                    "scope": "connection", "connectionId": self.connection_id,
                    "stateRevision": self.state_revision,
                    "state": "unknown" if self.expanded and not valid else (
                        "expanded" if self.expanded else "collapsed"),
                    "catalogStatus": "verified" if valid else self.reason,
                    "catalogRevision": self.revision,
                    "toolCount": len(self.catalog) if valid else 0,
                    "refreshRequested": refresh_requested}

    def _library(self, params: dict[str, Any]) -> dict[str, Any]:
        args = params.get("arguments")
        if (not isinstance(args, dict) or set(args) != {"action"}
                or args["action"] not in ("expand", "collapse", "status")):
            raise _Fault("invalid_library_action", code=-32602)
        action = args["action"]
        with self.lock:
            before_names = self.view_names
        if action == "expand":
            self._catalog()
            with self.lock:
                if self.catalog is None:
                    raise _Fault("catalog_changed_during_refresh", retryable=True)
                if not self.expanded:
                    self.state_revision += 1
                self.expanded = True
                after_names = tuple(tool["name"] for tool in self.catalog)
                self.view_names = after_names
            self._notice()
        elif action == "collapse":
            with self.lock:
                changed = self.expanded
                if changed or self.catalog is not None or self.reason != "unobserved":
                    self.state_revision += 1
                self.expanded = False
                self.view_names = ()
                self.catalog = None
                self.reason = "unobserved"
                self.epoch += 1
            if changed:
                self._notice()
        value = self.status(refresh_requested=action == "expand" or (action == "collapse" and changed))
        if action == "expand":
            before_set, after_set = set(before_names), set(after_names)
            value["addedTools"] = [name for name in after_names if name not in before_set]
            removed = [name for name in before_names if name not in after_set]
            if removed:
                value["removedTools"] = removed
            value["nextStep"] = (
                "上述新增工具将在客户端刷新后的下一次请求可用，可直接调用以继续完成任务。"
                if value["addedTools"] else
                "本次没有新增工具；目录刷新后可继续使用已展开的工具完成任务。"
            )
            if removed:
                value["nextStep"] += " removedTools 中的工具已移除，不得继续调用。"
        elif action == "collapse":
            value["removedTools"] = list(before_names)
            value["nextStep"] = (
                "上述工具已移除，再次展开前不得调用。"
                "bridge_library入口仍可用；若剩余任务需要上述工具，请先调用入口的expand，"
                "客户端刷新后的下一次请求即可继续使用。收起不表示任务无法完成。"
            )
        result: dict[str, Any] = {"content": [{"type": "text", "text": _encode(value).decode("utf-8")}]}
        if self.version >= "2025-06-18":
            result["structuredContent"] = value
        return result

    def handle(self, message: Any) -> dict[str, Any] | None:
        """Process one upstream frame. Call serially; reader notifications run concurrently."""
        request_id = None
        try:
            if (not isinstance(message, dict) or message.get("jsonrpc") != "2.0"
                    or not isinstance(message.get("method"), str)
                    or "result" in message or "error" in message):
                raise _Fault("invalid_request", code=-32600)
            if "id" in message:
                if not _valid_id(message["id"]):
                    raise _Fault("invalid_request_id", code=-32600)
                request_id = message["id"]
            method = message["method"]
            params = message.get("params", {})
            if not isinstance(params, dict) or ("_meta" in params and not isinstance(params["_meta"], dict)):
                raise _Fault("invalid_params", code=-32602)
            if method == "server/discover" or bridge_protocol.protocol_version_entry(message)[0]:
                raise _Fault("legacy_deferred_only_modern_discovery_unsupported", code=-32601)
            if "id" not in message:
                if method == "notifications/initialized" and self.initialize_result is not None:
                    self.client_ready = True
                # Unsupported notification families have no effect and no response.
                return None
            if method == "initialize":
                if self.initialize_result is not None:
                    raise _Fault("already_initialized", code=-32600)
                version = params.get("protocolVersion")
                if not bridge_protocol.is_verified_legacy_protocol_version(version):
                    raise _Fault("legacy_deferred_requires_verified_revision", code=-32602)
                if not isinstance(params.get("capabilities"), dict) or not isinstance(params.get("clientInfo"), dict):
                    raise _Fault("invalid_initialize", code=-32602)
                self.requested_version = version
                self._connect()
                assert self.initialize_result is not None
                result = {
                    "protocolVersion": self.version,
                    "capabilities": {"tools": {"listChanged": True}},
                    "serverInfo": {"name": "win-wsl-mcp-bridge-deferred",
                                   "version": bridge_runtime.SERVER_VERSION},
                    "instructions": self.initialize_result.get("instructions", "") + PROFILE_NOTE,
                }
            elif method == "ping":
                result = {}
            elif not self.client_ready:
                raise _Fault("initialize_and_initialized_required", code=-32600)
            elif method == "tools/list":
                if params.keys() - {"_meta"}:
                    raise _Fault("facade_catalog_is_not_paginated", code=-32602)
                result = {"tools": [self._library_tool()] + (self._catalog() if self.expanded else [])}
            elif method == "tools/call":
                if (params.keys() - {"name", "arguments", "_meta"}
                        or not isinstance(params.get("name"), str)
                        or ("arguments" in params and not isinstance(params["arguments"], dict))):
                    raise _Fault("invalid_tool_call", code=-32602)
                if params["name"] == LIBRARY_TOOL_NAME:
                    result = self._library(params)
                else:
                    if not self.expanded:
                        raise _Fault("tool_library_collapsed", code=-32602)
                    tools = self._catalog()
                    if params["name"] not in {tool["name"] for tool in tools}:
                        raise _Fault("unknown_tool", code=-32602)
                    assert self.peer is not None and self.version is not None
                    # Original name/arguments/_meta survive unchanged; IDs alone are private.
                    downstream = self.peer.request("tools/call", params)
                    try:
                        if (not isinstance(downstream, dict)
                                or not isinstance(downstream.get("content"), list)
                                or ("isError" in downstream and type(downstream["isError"]) is not bool)
                                or ("structuredContent" in downstream
                                    and not isinstance(downstream["structuredContent"], dict))):
                            raise ValueError("invalid call result")
                        result = bridge_protocol.project_legacy_tools_call_result(self.version, downstream)
                    except ValueError:
                        raise _Fault("unrepresentable_downstream_result", unknown=True) from None
            else:
                raise _Fault("tools_only_method_unsupported", code=-32601)
            response = {"jsonrpc": "2.0", "id": request_id, "result": result}
            if len(_encode(response)) + 1 > MAX_MESSAGE_BYTES:
                raise _Fault("facade_output_too_large", unknown=method == "tools/call")
            return response
        except (_Fault, _DownstreamFault) as exc:
            if isinstance(message, dict) and "id" not in message and isinstance(message.get("method"), str):
                return None
            return {"jsonrpc": "2.0", "id": request_id, "error": exc.error}
        except (ValueError, TypeError, RecursionError, OSError):
            return {"jsonrpc": "2.0", "id": request_id,
                    "error": _Fault("invalid_or_unavailable_downstream").error}


def run_deferred_mcp(local_host: str, local_port: int, target: str) -> int:
    """Serve one opt-in target over newline-framed stdin/stdout; never launch a server."""
    output_lock = threading.Lock()

    def emit(message: dict[str, Any]) -> None:
        raw = _encode(message) + b"\n"
        if len(raw) > MAX_MESSAGE_BYTES:
            raise _Fault("facade_output_too_large")
        with output_lock:
            sys.stdout.buffer.write(raw)
            sys.stdout.buffer.flush()

    session = DeferredSession(local_host, local_port, target, emit)
    try:
        while True:
            raw = sys.stdin.buffer.readline(MAX_MESSAGE_BYTES + 1)
            if not raw:
                return 0
            if len(raw) > MAX_MESSAGE_BYTES:
                emit({"jsonrpc": "2.0", "id": None,
                      "error": {"code": -32700, "message": "MCP message exceeds limit"}})
                return 2  # Do not resynchronize an unbounded hostile input stream.
            try:
                message = _decode(raw)
            except (ValueError, UnicodeError, RecursionError):
                emit({"jsonrpc": "2.0", "id": None,
                      "error": {"code": -32700, "message": "invalid JSON"}})
                continue
            response = session.handle(message)
            if response is not None:
                emit(response)
    except (BrokenPipeError, OSError, _Fault):
        return 1
    finally:
        session.close()
