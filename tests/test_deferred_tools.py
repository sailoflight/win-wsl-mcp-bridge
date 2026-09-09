"""Offline socket/subprocess fixtures for the opt-in legacy deferred-tools facade."""
from __future__ import annotations

import copy
import json
import os
from pathlib import Path
import queue
import socket
import subprocess
import sys
import threading
import time
import unittest

import bridge_protocol
import deferred_tools

ROOT = Path(__file__).resolve().parents[1]
TARGET = "fixture-library"
VERSION = "2025-11-25"
INSTRUCTIONS = "Canonical fixture policy: confirmation gates remain downstream."


def tool(name: str = "fixture_echo") -> dict:
    return {
        "name": name, "description": "Return exact fixture arguments", "title": "Fixture Echo",
        "inputSchema": {"type": "object", "properties": {
            "value": {"anyOf": [{"type": "string"}, {"type": "null"}]},
            "literal": {"$ref": "#/$defs/payload"}},
            "$defs": {"payload": {"type": "object", "additionalProperties": True}},
            "required": ["value"], "additionalProperties": False},
        "outputSchema": {"type": "object", "additionalProperties": True},
        "annotations": {"readOnlyHint": False, "destructiveHint": True,
                        "idempotentHint": False, "openWorldHint": True},
        "icons": [{"src": "https://example.invalid/icon.png", "mimeType": "image/png"}],
    }


class FakeNode:
    """Redacted registry + bridge connect + scripted downstream on local sockets."""
    def __init__(self):
        self.catalog = [tool()]
        self.pages = None
        self.page_size = 1
        self.version = None
        self.instructions = INSTRUCTIONS
        self.capabilities = {"tools": {"listChanged": True}, "resources": {}, "prompts": {}, "logging": {}}
        self.fail_initialize = False
        self.refuse_connect = False
        self.close_on_call = False
        self.close_after_call_result = False
        self.call_error = None
        self.call_result = None
        self.hold_call = False
        self.server_request = False
        self.notice_during_list = False
        self.close_list_page = None
        self.sessions = []
        self.registry_queries = []
        self.errors = []
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        self.listener = socket.socket()
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen(16)
        self.listener.settimeout(.1)
        self.port = self.listener.getsockname()[1]
        self.threads = []
        self.thread = threading.Thread(target=self._accept, daemon=True)
        self.thread.start()

    def _accept(self):
        while not self.stop_event.is_set():
            try:
                sock, _ = self.listener.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            thread = threading.Thread(target=self._serve, args=(sock,), daemon=True)
            self.threads.append(thread)
            thread.start()

    def send(self, session, message):
        with session["write_lock"]:
            session["sock"].sendall(json.dumps(message, ensure_ascii=False).encode() + b"\n")

    def _serve(self, sock):
        session = {"sock": sock, "write_lock": threading.Lock(), "requests": [], "responses": [], "alive": True}
        try:
            with sock, sock.makefile("rb") as stream:
                control = json.loads(stream.readline())
                if control.get("op") == "registry":
                    with self.lock:
                        self.registry_queries.append(control)
                    self.send(session, {"ok": True, "result": {
                        "id": TARGET, "name": "Fixture Library", "summary": "Bounded redacted fixture summary",
                        "transport": {"type": "stdio"},
                        # An injected extra field must not enter the facade's catalog/metadata.
                        "command": "PRIVATE-COMMAND-SENTINEL", "env": {"TOKEN": "PRIVATE-SECRET"},
                    }})
                    return
                if control != {"op": "connect", "target": TARGET}:
                    raise AssertionError(f"unexpected control: {control}")
                if self.refuse_connect:
                    self.send(session, {"ok": False, "message": "fixture offline"})
                    return
                with self.lock:
                    self.sessions.append(session)
                self.send(session, {"ok": True})
                for raw in stream:
                    request = json.loads(raw)
                    with self.lock:
                        if "method" in request:
                            session["requests"].append(request)
                        else:
                            session["responses"].append(request)
                    if "method" not in request:
                        # Complete a business request after its unsupported server request is refused.
                        if session.get("waiting") is not None:
                            original = session.pop("waiting")
                            self.send(session, {"jsonrpc": "2.0", "id": original["id"],
                                                "result": self._call_result(original)})
                        continue
                    if "id" not in request:
                        continue
                    method = request["method"]
                    if method == "initialize":
                        if self.fail_initialize:
                            self.send(session, {"jsonrpc": "2.0", "id": request["id"],
                                                "error": {"code": -32602, "message": "fixture initialize refused"}})
                            continue
                        result = {"protocolVersion": self.version or request["params"]["protocolVersion"],
                                  "capabilities": copy.deepcopy(self.capabilities),
                                  "serverInfo": {"name": "Fixture Backend", "version": "7"},
                                  "instructions": self.instructions}
                    elif method == "tools/list":
                        cursor = request.get("params", {}).get("cursor")
                        if self.close_list_page is not None and cursor == self.close_list_page:
                            return
                        if self.pages is not None:
                            result = copy.deepcopy(self.pages.get(cursor, {"tools": []}))
                        else:
                            start = int(cursor or 0)
                            snapshot = copy.deepcopy(self.catalog)
                            result = {"tools": snapshot[start:start + self.page_size]}
                            if start + self.page_size < len(snapshot):
                                result["nextCursor"] = str(start + self.page_size)
                        if self.notice_during_list:
                            self.send(session, {"jsonrpc": "2.0", "method": "notifications/tools/list_changed"})
                    elif method == "tools/call":
                        if self.close_on_call:
                            return
                        if self.hold_call:
                            continue
                        if self.call_error is not None:
                            self.send(session, {"jsonrpc": "2.0", "id": request["id"],
                                                "error": copy.deepcopy(self.call_error)})
                            continue
                        if self.server_request:
                            session["waiting"] = request
                            # Same ID as the active client request is legal in the opposite direction.
                            self.send(session, {"jsonrpc": "2.0", "id": request["id"],
                                                "method": "sampling/createMessage", "params": {}})
                            continue
                        result = (copy.deepcopy(self.call_result) if self.call_result is not None
                                  else self._call_result(request))
                    else:
                        raise AssertionError(f"unexpected downstream method {method}")
                    self.send(session, {"jsonrpc": "2.0", "id": request["id"], "result": result})
                    if method == "tools/call" and self.close_after_call_result:
                        return
        except (OSError, ValueError):
            pass
        except Exception as exc:
            self.errors.append(exc)
        finally:
            session["alive"] = False

    @staticmethod
    def _call_result(request):
        return {"content": [{"type": "text", "text": "fixture result"}],
                "structuredContent": copy.deepcopy(request["params"]), "isError": False,
                "_meta": {"fixture": "preserved"}}

    def broadcast(self):
        for session in list(self.sessions):
            if session["alive"]:
                try:
                    self.send(session, {"jsonrpc": "2.0", "method": "notifications/tools/list_changed"})
                except OSError:
                    pass

    def drop(self):
        for session in list(self.sessions):
            try:
                session["sock"].shutdown(socket.SHUT_RDWR)
            except OSError:
                pass

    def requests(self, method):
        with self.lock:
            return [copy.deepcopy(request) for session in self.sessions
                    for request in session["requests"] if request["method"] == method]

    def close(self):
        self.stop_event.set()
        self.listener.close()
        self.drop()
        self.thread.join(2)
        for thread in self.threads:
            thread.join(2)


class FacadeClient:
    def __init__(self, node, **settings):
        code = "import deferred_tools as d; "
        code += "; ".join(f"d.{name}={value!r}" for name, value in settings.items())
        code += f"; raise SystemExit(d.run_deferred_mcp('127.0.0.1', {node.port}, {TARGET!r}))"
        # Avoid a double separator when there are no settings.
        code = code.replace("; ;", ";")
        env = dict(os.environ, PYTHONDONTWRITEBYTECODE="1")
        self.process = subprocess.Popen([sys.executable, "-u", "-c", code], cwd=ROOT,
                                        stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE, env=env)
        self.messages = queue.Queue()
        self.notices = []
        self.counter = 0
        self.raw_lines = []
        self.reader = threading.Thread(target=self._read, daemon=True)
        self.reader.start()

    def _read(self):
        for raw in self.process.stdout:
            self.raw_lines.append(raw)
            try:
                self.messages.put(json.loads(raw))
            except ValueError:
                self.messages.put({"invalid_stdout": raw.decode(errors="replace")})
        self.messages.put({"eof": True})

    def send(self, message):
        self.process.stdin.write(json.dumps(message).encode() + b"\n")
        self.process.stdin.flush()

    def request(self, method, params=None, request_id=None):
        if request_id is None:
            self.counter += 1
            request_id = self.counter
        message = {"jsonrpc": "2.0", "id": request_id, "method": method}
        if params is not None:
            message["params"] = params
        self.send(message)
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            incoming = self.messages.get(timeout=max(.01, deadline - time.monotonic()))
            if incoming.get("id") == request_id:
                return incoming
            if "method" in incoming:
                self.notices.append(incoming)
            else:
                raise AssertionError(incoming)
        raise AssertionError("no facade response")

    def initialize(self, version=VERSION):
        response = self.request("initialize", {"protocolVersion": version,
                                "capabilities": {"sampling": {}, "roots": {"listChanged": True}},
                                "clientInfo": {"name": "fixture-client", "version": "1"}})
        if "result" in response:
            self.send({"jsonrpc": "2.0", "method": "notifications/initialized"})
        return response

    def library(self, action):
        return self.request("tools/call", {"name": "bridge_library", "arguments": {"action": action}})

    def notice(self):
        if self.notices:
            return self.notices.pop(0)
        message = self.messages.get(timeout=3)
        if message.get("method") != "notifications/tools/list_changed":
            raise AssertionError(message)
        return message

    def clear_notices(self):
        self.notices.clear()
        while True:
            try:
                message = self.messages.get_nowait()
            except queue.Empty:
                break
            if "method" not in message:
                raise AssertionError(message)

    def close(self):
        if self.process.poll() is None:
            self.process.stdin.close()
            try:
                self.process.wait(3)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(3)
        self.reader.join(2)
        stderr = self.process.stderr.read().decode()
        self.process.stdout.close()
        self.process.stderr.close()
        return stderr


def value(response):
    result = response["result"]
    return result.get("structuredContent") or json.loads(result["content"][0]["text"])


class DeferredToolsTests(unittest.TestCase):
    def setUp(self):
        self.node = FakeNode()
        self.clients = []

    def tearDown(self):
        for client in self.clients:
            self.assertEqual(client.close(), "", "protocol implementation wrote unexpected stderr")
            self.assertTrue(all("jsonrpc" in json.loads(raw) for raw in client.raw_lines))
        self.node.close()
        self.assertEqual(self.node.errors, [])

    def client(self, **settings):
        client = FacadeClient(self.node, **settings)
        self.clients.append(client)
        return client

    def initialized(self, **settings):
        client = self.client(**settings)
        self.assertIn("result", client.initialize())
        return client

    def test_small_initial_catalog_canonical_instructions_and_redacted_metadata(self):
        self.node.catalog = [tool(f"tool_{i}") for i in range(50)]
        client = self.client()
        initialized = client.initialize()["result"]
        self.assertEqual(initialized["protocolVersion"], VERSION)
        self.assertEqual(initialized["capabilities"], {"tools": {"listChanged": True}})
        self.assertTrue(initialized["instructions"].startswith(INSTRUCTIONS))
        self.assertIn("tools-only", initialized["instructions"])
        listed = client.request("tools/list")["result"]["tools"]
        self.assertEqual([item["name"] for item in listed], ["bridge_library"])
        self.assertIn("Fixture Library", listed[0]["description"])
        self.assertIn("Bounded redacted", listed[0]["description"])
        self.assertNotIn("PRIVATE", json.dumps(listed))
        self.assertEqual(self.node.requests("tools/list"), [])
        self.assertEqual(self.node.requests("initialize")[0]["params"]["capabilities"], {})
        self.assertEqual(self.node.registry_queries, [{"op": "registry", "scope": "remote",
                         "action": "describe", "arguments": {"id": TARGET}}])

    def test_view_switch_guides_next_turn_and_status_is_not_refresh_ack(self):
        client = self.client()
        initialized = client.initialize()["result"]
        self.assertIn("subsequent model request", initialized["instructions"])
        initial = client.request("tools/list")["result"]["tools"]
        self.assertIn("subsequent model request", initial[0]["description"])
        self.assertIn("not an acknowledgement", initial[0]["description"])
        # A client may retain its already-built request snapshot until it
        # processes list_changed. The facade cannot turn a status call into ack.
        expanded = value(client.library("expand"))
        self.assertIn("下一次请求", expanded["nextStep"])
        self.assertEqual(expanded["addedTools"], [t["name"] for t in self.node.catalog])
        self.assertEqual([t["name"] for t in initial], ["bridge_library"])
        before_relist = value(client.library("status"))
        self.assertFalse(before_relist["refreshRequested"])
        client.notice()
        next_turn = client.request("tools/list")["result"]["tools"]
        self.assertEqual(next_turn[1:], self.node.catalog)
        self.assertEqual(value(client.library("status")), before_relist)
        collapsed = value(client.library("collapse"))
        self.assertIn("下一次请求", collapsed["nextStep"])
        self.assertIn("再次展开前不得调用", collapsed["nextStep"])
        self.assertIn("bridge_library入口仍可用", collapsed["nextStep"])
        self.assertIn("收起不表示任务无法完成", collapsed["nextStep"])
        self.assertEqual(collapsed["removedTools"], expanded["addedTools"])
        # Even an old request snapshot retaining schemas cannot authorize a
        # hidden business call after the bridge has collapsed its directory.
        count = len(self.node.requests("tools/call"))
        refused = client.request("tools/call", {"name": self.node.catalog[0]["name"], "arguments": {}})
        self.assertEqual(refused["error"]["message"], "tool_library_collapsed")
        self.assertEqual(len(self.node.requests("tools/call")), count)
        client.notice()
        self.assertEqual([t["name"] for t in client.request("tools/list")["result"]["tools"]], ["bridge_library"])
        self.assertFalse(value(client.library("status"))["refreshRequested"])
        self.assertNotIn("nextStep", value(client.library("status")))

    def test_name_deltas_are_visible_in_text_for_every_legacy_profile(self):
        for version in bridge_protocol.LEGACY_PROTOCOL_VERSIONS:
            with self.subTest(version=version):
                self.node.version = version
                self.node.catalog = [tool("first"), tool("second")]
                client = self.client()
                client.initialize(version)
                result = client.library("expand")["result"]
                body = json.loads(result["content"][0]["text"])
                self.assertEqual(body["addedTools"], ["first", "second"])
                self.assertNotIn("description", json.dumps(body))
                self.assertNotIn("inputSchema", json.dumps(body))
                self.assertNotIn("查看", body["nextStep"])
                if version >= "2025-06-18":
                    self.assertEqual(result["structuredContent"], body)
                else:
                    self.assertNotIn("structuredContent", result)
                client.notice()
                self.assertEqual(value(client.library("expand"))["addedTools"], [])
                client.notice()
                self.assertEqual(value(client.library("collapse"))["removedTools"], ["first", "second"])
                client.notice()
                self.assertEqual(value(client.library("collapse"))["removedTools"], [])
                status = value(client.library("status"))
                self.assertNotIn("addedTools", status)
                self.assertNotIn("removedTools", status)
                client.close()
                self.clients.remove(client)

    def test_collapse_reports_last_names_after_catalog_invalidation(self):
        self.node.catalog = [tool("old")]
        client = self.initialized()
        client.library("expand")
        client.notice()
        self.node.broadcast()
        client.notice()
        self.assertEqual(value(client.library("status"))["state"], "unknown")
        requests = len(self.node.requests("tools/list"))
        self.assertEqual(value(client.library("collapse"))["removedTools"], ["old"])
        self.assertEqual(len(self.node.requests("tools/list")), requests)
        client.notice()
        self.assertEqual(value(client.library("collapse"))["removedTools"], [])

    def test_expanded_refresh_delta_tracks_replacements_and_relisted_names(self):
        self.node.catalog = [tool("old"), tool("retained")]
        client = self.initialized()
        client.library("expand")
        client.notice()
        self.node.catalog = [tool("retained"), tool("new")]
        self.node.broadcast()
        client.notice()
        refreshed = value(client.library("expand"))
        self.assertEqual(refreshed["addedTools"], ["new"])
        self.assertEqual(refreshed["removedTools"], ["old"])
        client.notice()
        self.node.catalog = [tool("latest")]
        self.node.broadcast()
        client.notice()
        self.assertEqual(client.request("tools/list")["result"]["tools"][1:], self.node.catalog)
        self.assertEqual(value(client.library("collapse"))["removedTools"], ["latest"])
        client.notice()

    def test_expand_paging_exact_schema_and_exact_business_params(self):
        self.node.catalog = [tool("one"), tool("two"), tool("three")]
        client = self.initialized()
        expanded = client.library("expand")
        status = value(expanded)
        self.assertTrue(status["expanded"])
        self.assertTrue(status["refreshRequested"])
        self.assertEqual(status["toolCount"], 3)
        self.assertNotIn("inputSchema", json.dumps(expanded))
        self.assertNotIn("modelReady", json.dumps(expanded))
        client.notice()
        listed = client.request("tools/list")["result"]["tools"]
        self.assertEqual(listed[1:], self.node.catalog)
        self.assertEqual([r["params"] for r in self.node.requests("tools/list")],
                         [{}, {"cursor": "1"}, {"cursor": "2"}])
        params = {"name": "two", "arguments": {"value": None, "literal": {
            "nested": [False, 0, "原样", {"confirm": False}]}}, "_meta": {"progressToken": "p"}}
        result = client.request("tools/call", params, request_id="deferred-5")["result"]
        self.assertEqual(result, FakeNode._call_result({"params": params}))
        self.assertEqual(self.node.requests("tools/call")[0]["params"], params)
        # Equal-looking upstream IDs have no association with private downstream IDs.
        self.assertEqual(value(client.library("status"))["state"], "expanded")

    def test_connections_independent_and_collapse_idempotent(self):
        one, two = self.initialized(), self.initialized()
        one.library("expand")
        one.notice()
        self.assertEqual(len(one.request("tools/list")["result"]["tools"]), 2)
        self.assertEqual(len(two.request("tools/list")["result"]["tools"]), 1)
        self.assertFalse(value(two.library("status"))["expanded"])
        first = value(one.library("collapse"))
        self.assertFalse(first["expanded"])
        self.assertTrue(first["refreshRequested"])
        one.notice()
        second = value(one.library("collapse"))
        self.assertFalse(second["refreshRequested"])
        self.assertEqual(second["stateRevision"], first["stateRevision"])
        self.assertEqual(len(one.request("tools/list")["result"]["tools"]), 1)
        self.assertEqual(one.request("tools/call", {"name": "fixture_echo", "arguments": {}})
                         ["error"]["data"]["reason"], "tool_library_collapsed")
        self.assertEqual(self.node.requests("tools/call"), [])

    def test_agents_sharing_connection_share_view_and_status_revision(self):
        client = self.initialized()
        initial = value(client.library("status"))
        self.assertEqual(initial["scope"], "connection")
        self.assertEqual(len(initial["connectionId"]), 32)

        def caller(agent, action):
            # Caller-supplied labels are not a new MCP connection or isolation grant.
            return value(client.request("tools/call", {
                "name": "bridge_library", "arguments": {"action": action},
                "_meta": {"agentSessionId": agent}}))

        expanded = caller("agent-a", "expand")
        observed = caller("agent-b", "status")
        self.assertTrue(observed["expanded"])
        self.assertEqual(observed["connectionId"], initial["connectionId"])
        self.assertEqual(observed["stateRevision"], expanded["stateRevision"])
        self.assertEqual(observed["catalogRevision"], expanded["catalogRevision"])
        self.assertGreater(observed["stateRevision"], initial["stateRevision"])
        self.assertFalse(observed["refreshRequested"])
        collapsed = caller("agent-b", "collapse")
        self.assertGreater(collapsed["stateRevision"], observed["stateRevision"])
        self.assertFalse(caller("agent-a", "status")["expanded"])
        other = self.initialized()
        self.assertNotEqual(value(other.library("status"))["connectionId"], initial["connectionId"])

    def test_idle_notification_invalidates_then_eventual_relist_refreshes(self):
        client = self.initialized()
        client.library("expand")
        client.notice()
        self.node.catalog = [tool("replacement")]
        self.node.broadcast()  # No client request is running while this arrives.
        client.notice()
        status = value(client.library("status"))
        self.assertEqual(status["state"], "unknown")
        self.assertEqual(status["catalogStatus"], "list_changed")
        self.assertEqual(client.request("tools/list")["result"]["tools"][1:], self.node.catalog)
        self.assertEqual(value(client.library("status"))["state"], "expanded")

    def test_invalid_pages_never_publish_partial_catalog(self):
        malformed_schema = tool()
        malformed_schema["inputSchema"]["properties"] = []
        cases = [
            ({None: {"tools": [tool("bridge_library")]}}, "reserved_tool_collision"),
            ({None: {"tools": [tool("same")], "nextCursor": "a"},
              "a": {"tools": [tool("same")]}}, "duplicate_tool_name"),
            ({None: {"tools": [malformed_schema]}}, "invalid_tool_schema"),
            ({None: {"tools": [tool()], "nextCursor": "a"},
              "a": {"tools": [], "nextCursor": "a"}}, "invalid_catalog_cursor"),
            ({None: {"tools": [tool()], "nextCursor": 1}}, "invalid_catalog_cursor"),
            ({None: {"tools": [tool()], "nextCursor": None}}, "invalid_catalog_cursor"),
            ({None: {"tools": [tool()], "nextCursor": "a"},
              "a": {"tools": "bad"}}, "invalid_catalog_page"),
            ({None: {"tools": [None]}}, "invalid_tool_definition"),
        ]
        for pages, reason in cases:
            with self.subTest(reason=reason):
                self.node.pages = pages
                client = self.initialized()
                self.assertEqual(client.library("expand")["error"]["data"]["reason"], reason)
                self.assertFalse(value(client.library("status"))["expanded"])
                self.assertEqual(len(client.request("tools/list")["result"]["tools"]), 1)
                client.close()
                self.clients.remove(client)

    def test_bounded_catalog_schema_page_count_and_cursor(self):
        cases = [({"MAX_TOOLS": 1}, {None: {"tools": [tool("a"), tool("b")]}}, "catalog_limit_exceeded"),
                 ({"MAX_SCHEMA_BYTES": 8}, {None: {"tools": [tool()]}}, "tool_schema_too_large"),
                 ({"MAX_CATALOG_BYTES": 32}, {None: {"tools": [tool()]}}, "catalog_limit_exceeded"),
                 ({"MAX_PAGES": 2}, {None: {"tools": [], "nextCursor": "a"},
                                      "a": {"tools": [], "nextCursor": "b"}}, "catalog_page_limit_exceeded"),
                 ({"MAX_CURSOR_BYTES": 2}, {None: {"tools": [], "nextCursor": "long"}}, "invalid_catalog_cursor")]
        for settings, pages, reason in cases:
            with self.subTest(reason=reason):
                self.node.pages = pages
                client = self.initialized(**settings)
                self.assertEqual(client.library("expand")["error"]["data"]["reason"], reason)
                client.close()
                self.clients.remove(client)

    def test_dirty_refresh_failure_is_explicit_and_never_stale(self):
        client = self.initialized()
        client.library("expand")
        client.notice()
        self.node.pages = {None: {"tools": [tool("new")], "nextCursor": "next"},
                           "next": {"tools": [False]}}
        self.node.broadcast()
        client.notice()
        reply = client.request("tools/list")
        self.assertIn("error", reply)
        status = value(client.library("status"))
        self.assertEqual(status["state"], "unknown")
        self.assertEqual(status["toolCount"], 0)
        self.assertTrue(status["expanded"])
        self.assertIn("error", client.request("tools/call", {"name": "fixture_echo", "arguments": {}}))
        self.assertEqual(self.node.requests("tools/call"), [])

    def test_catalog_change_during_fetch_rejects_generation(self):
        client = self.initialized()
        self.node.notice_during_list = True
        self.assertEqual(client.library("expand")["error"]["data"]["reason"],
                         "catalog_changed_during_refresh")
        self.assertEqual(len(client.request("tools/list")["result"]["tools"]), 1)

    def test_reconnect_preserves_intent_without_replaying_business(self):
        client = self.initialized()
        client.library("expand")
        client.notice()
        self.node.close_on_call = True
        result = client.request("tools/call", {"name": "fixture_echo", "arguments": {"value": "once"}})
        self.assertTrue(result["error"]["data"]["outcomeUnknown"])
        self.assertEqual(len(self.node.requests("tools/call")), 1)
        self.assertEqual(value(client.library("status"))["state"], "unknown")
        self.node.close_on_call = False
        self.node.catalog = [tool("recovered")]
        self.assertEqual(client.request("tools/list")["result"]["tools"][1:], self.node.catalog)
        self.assertTrue(value(client.library("status"))["expanded"])
        self.assertEqual(len(self.node.requests("initialize")), 2)
        self.assertEqual(len(self.node.requests("notifications/initialized")), 2)
        self.assertEqual(len(self.node.requests("tools/call")), 1)

    def test_timeout_has_unknown_outcome_and_no_replay(self):
        client = self.initialized(REQUEST_TIMEOUT=.2, CONNECT_TIMEOUT=.2)
        client.library("expand")
        client.notice()
        self.node.hold_call = True
        reply = client.request("tools/call", {"name": "fixture_echo", "arguments": {"value": "once"}})
        self.assertEqual(reply["error"]["data"]["reason"], "downstream_timeout")
        self.assertTrue(reply["error"]["data"]["outcomeUnknown"])
        self.node.hold_call = False
        self.assertIn("result", client.request("tools/list"))
        self.assertEqual(len(self.node.requests("tools/call")), 1)

    def test_server_requests_refused_without_deadlock_or_id_collision(self):
        self.node.server_request = True
        client = self.initialized()
        client.library("expand")
        client.notice()
        reply = client.request("tools/call", {"name": "fixture_echo", "arguments": {"value": "ok"}})
        self.assertIn("result", reply)
        responses = self.node.sessions[0]["responses"]
        self.assertEqual(responses[0]["error"]["code"], -32601)
        self.assertEqual(responses[0]["id"], self.node.requests("tools/call")[0]["id"])

    def test_modern_and_unsupported_methods_fail_explicitly(self):
        client = self.client()
        self.assertEqual(client.request("server/discover")["error"]["code"], -32601)
        self.assertIn("error", client.initialize("2026-07-28"))
        self.assertEqual(self.node.sessions, [])
        self.assertIn("result", client.initialize())
        for method in ("resources/list", "prompts/list", "subscriptions/listen", "tasks/get"):
            self.assertEqual(client.request(method)["error"]["code"], -32601)
        self.assertEqual(client.request("tools/list", {"_meta": {
            "io.modelcontextprotocol/protocolVersion": "2026-07-28"}})["error"]["code"], -32601)

    def test_actual_verified_backend_revision_is_negotiated(self):
        for version in bridge_protocol.LEGACY_PROTOCOL_VERSIONS:
            with self.subTest(version=version):
                self.node.version = version
                client = self.client()
                self.assertEqual(client.initialize()["result"]["protocolVersion"], version)
                client.library("expand")
                expected = bridge_protocol.project_legacy_tool(version, tool())
                self.assertEqual(client.request("tools/list")["result"]["tools"][1], expected)
                self.assertNotIn("structuredContent", client.library("status")["result"]
                                 if version < "2025-06-18" else {})
                client.close()
                self.clients.remove(client)

    def test_unverified_backend_and_changed_reconnect_contract_fail_closed(self):
        client = self.client()
        self.node.version = "2026-07-28"
        self.assertEqual(client.initialize()["error"]["data"]["reason"], "unverified_downstream_initialize")
        self.node.version = VERSION
        self.assertIn("result", client.initialize())
        client.library("expand")
        client.notice()
        self.node.drop()
        client.notice()
        self.node.instructions = "Changed policy cannot silently replace the initialized policy."
        reply = client.request("tools/list")
        self.assertEqual(reply["error"]["data"]["reason"], "downstream_contract_changed_restart_required")
        self.assertEqual(value(client.library("status"))["state"], "unknown")
        self.assertEqual(self.node.requests("tools/call"), [])

    def test_mid_page_disconnect_does_not_expand_and_can_recover(self):
        self.node.catalog = [tool("one"), tool("two")]
        self.node.close_list_page = "1"
        client = self.initialized()
        failed = client.library("expand")
        self.assertIn("error", failed)
        self.assertFalse(failed["error"]["data"]["outcomeUnknown"])
        self.assertEqual(len(client.request("tools/list")["result"]["tools"]), 1)
        self.node.close_list_page = None
        self.assertEqual(value(client.library("expand"))["toolCount"], 2)
        self.assertEqual(len(self.node.requests("tools/call")), 0)

    def test_downstream_errors_and_confirmation_result_are_preserved(self):
        client = self.initialized()
        client.library("expand")
        client.notice()
        self.node.call_error = {"code": -32005, "message": "approval required",
                                "data": {"requiresConfirmation": True, "dryRun": True}}
        params = {"name": "fixture_echo", "arguments": {"value": "x", "confirm": False}}
        self.assertEqual(client.request("tools/call", params)["error"], self.node.call_error)
        self.node.call_error = None
        self.node.call_result = {"content": [{"type": "text", "text": "preview only"}],
                                 "isError": True, "structuredContent": {"mutated": False}}
        self.assertEqual(client.request("tools/call", params)["result"], self.node.call_result)
        self.assertEqual([r["params"] for r in self.node.requests("tools/call")], [params, params])
        self.node.call_result = {"content": None}
        self.assertEqual(client.request("tools/call", params)["error"]["data"]["reason"],
                         "unrepresentable_downstream_result")

    def test_final_response_is_not_lost_when_downstream_closes_immediately(self):
        client = self.initialized()
        client.library("expand")
        client.notice()
        self.node.close_after_call_result = True
        params = {"name": "fixture_echo", "arguments": {"value": "settled"}}
        self.assertEqual(client.request("tools/call", params)["result"],
                         FakeNode._call_result({"params": params}))
        self.assertEqual(len(self.node.requests("tools/call")), 1)

    def test_fresh_process_starts_collapsed_after_another_process_expanded(self):
        client = self.initialized()
        client.library("expand")
        client.notice()
        client.close()
        self.clients.remove(client)
        fresh = self.initialized()
        self.assertEqual(value(fresh.library("status"))["state"], "collapsed")
        self.assertEqual([entry["name"] for entry in fresh.request("tools/list")["result"]["tools"]],
                         ["bridge_library"])

    def test_input_validation_and_stdout_are_protocol_clean(self):
        client = self.initialized()
        client.process.stdin.write(b'{"jsonrpc":"2.0","id":77,"id":78,"method":"ping"}\n')
        client.process.stdin.flush()
        parsed = client.messages.get(timeout=3)
        self.assertEqual(parsed["error"]["code"], -32700)
        for params in ({"name": "bridge_library", "arguments": {}},
                       {"name": "bridge_library", "arguments": {"action": "expand", "extra": 1}},
                       {"name": "bridge_library", "arguments": {"action": "status"}, "task": {}}):
            self.assertEqual(client.request("tools/call", params)["error"]["code"], -32602)
        self.assertIn("result", client.request("ping", request_id="still-clean"))


if __name__ == "__main__":
    unittest.main()
