"""Modern HTTP façade fixtures: real HTTP -> registered bridge -> synthetic stdio.

All registries, node listeners, logs and backend scripts live in a TemporaryDirectory.
No production services, backend-command override, credentials or external network.
"""
from __future__ import annotations

import base64
import http.client
import json
import os
import select
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

import stdio_http_facade as facade
from bridge_runtime import Registry, BridgeError, local_registry_query

ROOT = Path(__file__).resolve().parents[1]
VERSION = "2026-07-28"
VERSION_META = "io.modelcontextprotocol/protocolVersion"
CAP_META = "io.modelcontextprotocol/clientCapabilities"
SECRET = "synthetic-private-http-header"

BACKEND = r'''
import json, os, signal, sys, threading, time
state = sys.argv[1]
blocked = len(sys.argv) > 2
write_lock = threading.Lock()
def log(event, **data):
    with open(state, "a", encoding="utf-8") as out:
        out.write(json.dumps({"event": event, "pid": os.getpid(), **data}) + "\n")
def stop(*args):
    log("exit")
    raise SystemExit(0)
signal.signal(signal.SIGTERM, stop)
log("spawn")
if blocked:
    while True: time.sleep(1)
def send(message):
    with write_lock:
        print(json.dumps(message, separators=(",", ":")), flush=True)
def result(rid, **data):
    send({"jsonrpc": "2.0", "id": rid, "result": {"resultType": "complete", **data}})
def error(rid, code, message):
    send({"jsonrpc": "2.0", "id": rid, "error": {"code": code, "message": message}})
annotated = {"type":"object", "properties": {
    "text":{"type":"string", "x-mcp-header":"Text"},
    "nested":{"type":"object", "properties": {
        "count":{"type":"integer", "x-mcp-header":"Count"},
        "flag":{"type":"boolean", "x-mcp-header":"Flag"}}},
    "missing":{"type":"string", "x-mcp-header":"Missing"}}}
names = ["echo", "slow", "crash", "big", "push", "bad_token", "needs_input", "flood", "annotated", "invalid"]
tools = [{"name": n, "inputSchema": annotated if n == "annotated" else {"type":"object"}} for n in names]
tools[-1]["inputSchema"] = {"type":"object", "properties":{"v":{"type":"number", "x-mcp-header":"Bad"}}}
cancelled = {}
def slow(rid, token, bad=False):
    send({"jsonrpc":"2.0", "method":"notifications/progress", "params": {
        "progressToken": "unrelated" if bad else token, "progress":1, "total":2}})
    while not cancelled.get(rid): time.sleep(.02)
    log("cancel-observed", id=rid)
    error(rid, -32800, "cancelled")
try:
    for line in sys.stdin:
        message = json.loads(line)
        log("recv", message=message)
        method, rid = message.get("method"), message.get("id")
        params = message.get("params", {})
        if method == "notifications/cancelled":
            cancelled[params.get("requestId")] = True
            continue
        meta = params.get("_meta", {})
        if meta.get("io.modelcontextprotocol/protocolVersion") != "2026-07-28" or not isinstance(meta.get("io.modelcontextprotocol/clientCapabilities"), dict):
            error(rid, -32602, "metadata required")
            continue
        if method == "server/discover":
            result(rid, supportedVersions=["2026-07-28"], capabilities={"tools":{}},
                instructions="Actual registered synthetic guidance", _meta={"io.modelcontextprotocol/serverInfo":{"name":"registered-modern","version":"1"}})
        elif method == "tools/list":
            result(rid, tools=tools)
        elif method == "tools/call":
            name = params["name"]
            log("executed", id=rid, name=name)
            if name in ("slow", "bad_token"):
                threading.Thread(target=slow, args=(rid, meta.get("progressToken"), name == "bad_token"), daemon=True).start()
            elif name == "crash":
                os._exit(7)
            elif name == "big":
                result(rid, content=[{"type":"text", "text":"x" * 100000}])
            elif name == "push":
                send({"jsonrpc":"2.0", "id":"backend-input", "method":"sampling/createMessage", "params":{}})
            elif name == "flood":
                for i in range(2000):
                    send({"jsonrpc":"2.0", "method":"notifications/progress", "params":{"progressToken":meta.get("progressToken"),"progress":i}})
            elif name == "needs_input":
                send({"jsonrpc":"2.0", "id":rid, "result":{"resultType":"input_required","requestState":"opaque-state","inputRequests":{"i":{"method":"roots/list"}}}})
            else:
                result(rid, content=[{"type":"text","text":"registered-result"}], echoed=params, backendPid=os.getpid())
        elif method == "fixture/extension-hang":
            log("executed", id=rid, name="extension-hang")
        elif method == "fixture/missing-result-type":
            send({"jsonrpc":"2.0", "id":rid, "result":{"value":1}})
        elif method == "fixture/wrong-id":
            result("wrong-id")
        else:
            error(rid, -32601, "Unknown fixture method")
finally:
    log("exit")
'''


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def request(method="server/discover", rid=1, **params):
    meta = {VERSION_META: VERSION, CAP_META: {}}
    meta.update(params.pop("meta", {}))
    return {"jsonrpc":"2.0", "id":rid, "method":method, "params":{"_meta":meta, **params}}


def header_value(value):
    if (value != value.strip() or any(ord(c) < 32 or ord(c) > 126 for c in value)
            or value.startswith("=?base64?") and value.endswith("?=")):
        return "=?base64?" + base64.b64encode(value.encode()).decode() + "?="
    return value


class ModernFacadeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory()
        cls.root = Path(cls.temp.name)
        cls.state = cls.root / "backend.jsonl"
        script = cls.root / "registered_modern.py"
        script.write_text(BACKEND, encoding="utf-8")
        cls.state.write_text("")
        cls.win_port, cls.wsl_port, cls.link_port = free_port(), free_port(), free_port()
        cls.nodes = []
        entries = [{"id": target, "name": target, "summary":"Synthetic local modern stdio",
                    "command":sys.executable, "args":[str(script),str(cls.state)] + (["blocked"] if target == "blocked" else []),
                    "cwd":str(ROOT), "process":{"multiProcessAllowed":True}, "artifactDelivery":{"enabled":False}}
                   for target in ("modern-fixture", "blocked")]
        for side, port, servers in (("win", cls.win_port, entries), ("wsl", cls.wsl_port, [])):
            manifest = cls.root / (side + ".json")
            manifest.write_text(json.dumps({"servers":servers}))
            database = cls.root / (side + ".sqlite3")
            Registry.initialize_database(database, manifest, replace=True)
            workspace = cls.root / (side + "-workspace")
            workspace.mkdir()
            cls.nodes.append(subprocess.Popen([
                sys.executable, str(ROOT / (side + "-bridge-mcp") / "bridge.py"), "serve",
                "--registry",str(database), "--local-port",str(port), "--link-port",str(cls.link_port),
                "--allow-artifact-root",str(workspace), "--artifact-spool-root",str(cls.root / (side + "-spool"))],
                cwd=ROOT, env={**os.environ,"PYTHONDONTWRITEBYTECODE":"1"}, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            try:
                row = local_registry_query("127.0.0.1", cls.wsl_port, "remote", "describe", {"id":"modern-fixture"})
                if row.get("id") == "modern-fixture":
                    return
            except (OSError, BridgeError):
                pass
            threading.Event().wait(.05)
        cls.tearDownClass()
        raise RuntimeError("Synthetic bridge link did not become ready")

    @classmethod
    def tearDownClass(cls):
        for node in reversed(cls.nodes):
            if node.poll() is None:
                node.terminate()
            try:
                node.wait(timeout=5)
            except subprocess.TimeoutExpired:
                node.kill()
                node.wait(timeout=3)
        cls.temp.cleanup()

    def setUp(self):
        self.servers = []
        self.mark = len(self.all_events())
        self.server = self.start_server()

    def tearDown(self):
        for server, thread in self.servers:
            server.shutdown_all()
            server.shutdown()
            server.server_close()
            thread.join(3)
            self.assertFalse(thread.is_alive())
            self.assertEqual(server.proxies, set())
        self.wait_until(lambda: all(self.pid_gone(e["pid"]) for e in self.events() if e.get("event") == "spawn"), timeout=6)

    def start_server(self, **changes):
        options = dict(side="wsl", target="modern-fixture", node_port=self.wsl_port,
                       protocol_era="modern", request_timeout_s=3, sse_heartbeat_s=.05)
        options.update(changes)
        server = facade.ModernFacadeServer(facade.FacadeOptions(**options))
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.servers.append((server, thread))
        return server

    @staticmethod
    def pid_gone(pid):
        try:
            os.kill(pid, 0)
            return False
        except ProcessLookupError:
            return True

    def all_events(self):
        events = []
        for line in self.state.read_text().splitlines():
            try:
                events.append(json.loads(line))
            except ValueError:
                pass
        return events

    def events(self):
        return self.all_events()[self.mark:]

    def wait_until(self, predicate, timeout=4):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if predicate():
                return
            threading.Event().wait(.02)
        self.fail("Timed out waiting for bounded fixture cleanup")

    def headers(self, message):
        headers = {"Content-Type":"application/json", "Accept":"application/json, text/event-stream",
                   "MCP-Protocol-Version":message["params"]["_meta"][VERSION_META], "Mcp-Method":message["method"]}
        if message["method"] in ("tools/call", "resources/read", "prompts/get"):
            headers["Mcp-Name"] = header_value(message["params"].get("uri", message["params"].get("name", "")))
        return headers

    def post(self, message, *, headers=None, server=None):
        server = server or self.server
        connection = http.client.HTTPConnection(*server.server_address[:2], timeout=7)
        try:
            connection.request("POST", "/mcp", body=json.dumps(message).encode(), headers=self.headers(message) if headers is None else headers)
            response = connection.getresponse()
            body = response.read()
            return response.status, dict(response.getheaders()), json.loads(body)
        finally:
            connection.close()

    def test_discover_list_call_actual_metadata_and_independent_registered_proxies(self):
        replies = []
        for rid, method in enumerate(("server/discover", "tools/list", "tools/call"), 1):
            message = request(method, rid, **({"name":"echo","arguments":{"value":"hi"}} if method == "tools/call" else {}))
            message["params"]["_meta"][CAP_META] = {"roots":{}} if rid == 3 else {}
            headers = self.headers(message)
            headers["Authorization"] = SECRET
            status, response_headers, body = self.post(message, headers=headers)
            self.assertEqual(status, 200)
            self.assertEqual(body["result"]["resultType"], "complete")
            self.assertNotIn("mcp-session-id", {k.lower() for k in response_headers})
            self.assertNotIn(SECRET, json.dumps(body))
            replies.append(body)
        self.assertEqual(replies[0]["result"]["instructions"], "Actual registered synthetic guidance")
        self.assertEqual(replies[0]["result"]["_meta"]["io.modelcontextprotocol/serverInfo"]["name"], "registered-modern")
        self.assertNotIn("invalid", [tool["name"] for tool in replies[1]["result"]["tools"]])
        self.assertIn("annotated", [tool["name"] for tool in replies[1]["result"]["tools"]])
        self.assertEqual(replies[2]["result"]["echoed"], message["params"])
        self.wait_until(lambda: not self.server.proxies)
        frames = [e["message"] for e in self.events() if e["event"] == "recv"]
        self.assertEqual([m["method"] for m in frames], ["server/discover", "tools/list", "tools/list", "tools/call"])
        self.assertEqual(frames[-1], message)
        self.assertEqual(len({e["pid"] for e in self.events() if e["event"] == "spawn"}), 3)
        self.assertNotIn(SECRET, json.dumps(frames))

    def test_post_only_no_session_or_initialized_and_precise_versions(self):
        for verb in ("GET", "DELETE"):
            connection = http.client.HTTPConnection(*self.server.server_address[:2], timeout=3)
            connection.request(verb, "/mcp")
            response = connection.getresponse()
            self.assertEqual(response.status, 405)
            self.assertEqual(response.getheader("Allow"), "POST")
            response.read()
            connection.close()
        for version in ("2025-06-18", "2025-11-25", "2099-01-01"):
            message = request(meta={VERSION_META:version})
            status, _, body = self.post(message)
            self.assertEqual(status, 400)
            self.assertEqual(body["error"]["code"], -32022)
            self.assertEqual(body["error"]["data"]["supported"], [VERSION])
            self.assertEqual(body["error"]["data"]["requested"], version)
            self.assertFalse(body["error"]["data"]["outcomeUnknown"])
        message = request("initialize")
        self.assertEqual(self.post(message)[2]["error"]["code"], -32601)
        message = request()
        for forbidden in ("Mcp-Session-Id", "Last-Event-ID"):
            headers = self.headers(message)
            headers[forbidden] = "forbidden"
            self.assertEqual(self.post(message, headers=headers)[2]["error"]["code"], -32020)
        notification = request("notifications/initialized")
        del notification["id"]
        self.assertEqual(self.post(notification)[2]["error"]["code"], -32600)
        self.assertEqual(self.events(), [])

    def test_header_body_origin_and_duplicate_header_validation_before_spawn(self):
        message = request("tools/call", name="echo")
        for key, value in (("Mcp-Method","tools/list"), ("Mcp-Name","other"), ("MCP-Protocol-Version","2025-06-18")):
            headers = self.headers(message)
            headers[key] = value
            status, _, body = self.post(message, headers=headers)
            self.assertEqual(status, 400)
            self.assertEqual(body["error"]["code"], -32020)
        headers = self.headers(message)
        headers["Origin"] = "http://attacker.invalid"
        self.assertEqual(self.post(message, headers=headers)[0], 403)
        headers = self.headers(message)
        del headers["Mcp-Method"]
        self.assertEqual(self.post(message, headers=headers)[2]["error"]["code"], -32020)
        connection = http.client.HTTPConnection(*self.server.server_address[:2], timeout=3)
        body = json.dumps(message).encode()
        connection.putrequest("POST", "/mcp")
        for key, value in self.headers(message).items():
            connection.putheader(key, value)
        connection.putheader("Mcp-Method", "tools/call")
        connection.putheader("Content-Length", str(len(body)))
        connection.endheaders(body)
        response = connection.getresponse()
        self.assertEqual(response.status, 400)
        response.read()
        connection.close()
        self.assertEqual(self.events(), [])

    def test_annotated_headers_decode_and_validate_without_changing_body(self):
        message = request("tools/call", name="annotated", arguments={"text":" padded 世界 ","nested":{"count":42,"flag":True},"missing":None})
        headers = self.headers(message)
        headers.update({"Mcp-Param-Text":header_value(" padded 世界 "), "mcp-param-count":"42.0", "MCP-PARAM-FLAG":"true"})
        status, _, body = self.post(message, headers=headers)
        self.assertEqual(status, 200)
        self.assertEqual(body["result"]["echoed"], message["params"])
        for bad in ("wrong", "=?base64?invalid!?="):
            rejected = dict(headers, **{"Mcp-Param-Text":bad})
            status, _, body = self.post(message, headers=rejected)
            self.assertEqual(status, 400)
            self.assertEqual(body["error"]["code"], -32020)
            self.assertFalse(body["error"]["data"]["outcomeUnknown"])
        missing = self.headers(message)
        self.assertEqual(self.post(message, headers=missing)[2]["error"]["code"], -32020)
        unknown = dict(headers, **{"Mcp-Param-Unknown":"not-forwardable"})
        self.assertEqual(self.post(message, headers=unknown)[2]["error"]["code"], -32020)
        invalid = request("tools/call", 9, name="invalid", arguments={"v":1})
        failure = self.post(invalid)[2]["error"]
        self.assertEqual(failure["data"]["requiredFeature"], "x-mcp-header")
        self.assertFalse(failure["data"]["outcomeUnknown"])
        self.assertEqual(len([e for e in self.events() if e["event"] == "executed"]), 1)

    def test_sse_progress_disconnect_cancels_and_reaps_only_owned_proxy(self):
        message = request("tools/call", 71, name="slow", meta={"progressToken":"request-token"})
        connection = http.client.HTTPConnection(*self.server.server_address[:2], timeout=4)
        connection.request("POST", "/mcp", body=json.dumps(message).encode(), headers=self.headers(message))
        response = connection.getresponse()
        self.assertEqual(response.getheader("Content-Type"), "text/event-stream")
        self.assertNotIn("Mcp-Session-Id", dict(response.getheaders()))
        self.assertEqual(response.readline(), b"event: message\n")
        progress = json.loads(response.readline()[6:])
        self.assertEqual(progress["params"]["progressToken"], "request-token")
        with self.server.lock:
            proxies = list(self.server.proxies)
        response.close()
        connection.close()
        self.wait_until(lambda: not self.server.proxies)
        self.assertTrue(all(p.process.poll() is not None and all(not t.is_alive() for t in p.threads) for p in proxies))
        self.wait_until(lambda: any(e["event"] == "recv" and e["message"].get("method") == "notifications/cancelled" for e in self.events()))
        self.assertEqual(len([e for e in self.events() if e["event"] == "executed"]), 1)
        self.assertEqual(self.post(request(rid=72))[0], 200)

    def test_timeout_and_crash_have_uncertainty_no_replay_and_proxy_cleanup(self):
        server = self.start_server(request_timeout_s=.7)
        # No progress token: synthetic slow emits unrelated progress, so use
        # a known business that exits after execution for transport uncertainty.
        status, _, body = self.post(request("tools/call", 80, name="crash"), server=server)
        self.assertIn(status, (200, 502), body)
        self.assertTrue(body["error"]["data"]["outcomeUnknown"])
        blocked = self.start_server(target="blocked", request_timeout_s=.5)
        status, _, body = self.post(request("tools/call", 81, name="echo"), server=blocked)
        self.assertEqual(status, 504)
        self.assertFalse(body["error"]["data"]["outcomeUnknown"])
        self.wait_until(lambda: not server.proxies and not blocked.proxies)
        self.assertEqual(len([e for e in self.events() if e["event"] == "executed" and e.get("id") == 80]), 1)
        self.assertFalse(any(e["event"] == "executed" and e.get("id") == 81 for e in self.events()))

    def test_result_type_mrtr_and_unrepresentable_backend_messages(self):
        status, _, body = self.post(request("tools/call", 91, name="needs_input"))
        self.assertEqual(status, 200)
        self.assertEqual(body["result"]["resultType"], "input_required")
        self.assertEqual(body["result"]["requestState"], "opaque-state")
        for rid, name in ((92,"push"), (93,"bad_token")):
            status, _, body = self.post(request("tools/call", rid, name=name, meta={"progressToken":"t"}))
            self.assertEqual(status, 502)
            self.assertTrue(body["error"]["data"]["outcomeUnknown"])
        for method in ("fixture/missing-result-type", "fixture/wrong-id"):
            self.assertEqual(self.post(request(method))[0], 502)
        frames = [e["message"] for e in self.events() if e["event"] == "recv"]
        self.assertFalse(any(m.get("id") == "backend-input" for m in frames))
        self.assertEqual(len([e for e in self.events() if e["event"] == "executed" and e.get("id") == 91]), 1)

    def test_bounded_request_response_and_http_worker_lifecycle(self):
        server = self.start_server(max_line_bytes=4096, request_body_bytes=2048)
        message = request("tools/call", name="echo", arguments={"value":"x"*3000})
        self.assertEqual(self.post(message, server=server)[0], 413)
        self.assertEqual(self.events(), [])
        status, _, body = self.post(request("tools/call", 31, name="big"), server=server)
        self.assertEqual(status, 502)
        self.assertTrue(body["error"]["data"]["outcomeUnknown"])
        limited = self.start_server(max_inflight=1, request_timeout_s=.4)
        slow_header = socket.create_connection(limited.server_address[:2])
        self.wait_until(lambda: len(limited.connections) == 1)
        self.assertEqual(self.post(request(), server=limited)[0], 503)
        self.wait_until(lambda: not limited.connections)
        slow_header.close()
        self.assertEqual(limited.proxies, set())
        self.assertEqual(self.post(request(), server=limited)[0], 200)

    def test_positive_accept_quality_and_zero_quality_are_distinguished(self):
        message = request()
        headers = self.headers(message)
        headers["Accept"] = "application/json;q=0.5, text/event-stream;q=0.9"
        self.assertEqual(self.post(message, headers=headers)[0], 200)
        before = len([e for e in self.events() if e["event"] == "spawn"])
        headers["Accept"] = "application/json;q=0, text/event-stream;q=1"
        self.assertEqual(self.post(message, headers=headers)[0], 406)
        self.assertEqual(len([e for e in self.events() if e["event"] == "spawn"]), before)

    def test_deadline_or_disconnect_after_schema_never_queues_business_call(self):
        original = facade._ModernFacadeHandler._validate_tool_headers
        server = self.start_server(request_timeout_s=.6)
        def expire_after_schema(handler, message):
            original(handler, message)
            threading.Event().wait(max(0, handler.deadline - time.monotonic()) + .03)
        with patch.object(facade._ModernFacadeHandler, "_validate_tool_headers", expire_after_schema):
            status, _, body = self.post(request("tools/call", 101, name="echo"), server=server)
        self.assertEqual(status, 504)
        self.assertFalse(body["error"]["data"]["outcomeUnknown"])
        validated, release = threading.Event(), threading.Event()
        def disconnect_after_schema(handler, message):
            original(handler, message)
            validated.set()
            release.wait(2)
        message = request("tools/call", 102, name="echo")
        connection = http.client.HTTPConnection(*self.server.server_address[:2], timeout=3)
        with patch.object(facade._ModernFacadeHandler, "_validate_tool_headers", disconnect_after_schema):
            connection.request("POST", "/mcp", body=json.dumps(message).encode(), headers=self.headers(message))
            self.assertTrue(validated.wait(2))
            connection.close()
            with self.server.lock:
                accepted = list(self.server.connections)
            # Resume only after FIN is observable at the server, so this tests
            # the pre-send guard rather than TCP delivery scheduling.
            self.assertTrue(select.select(accepted, [], [], 1)[0])
            self.assertTrue(any(sock.recv(1, socket.MSG_PEEK) == b"" for sock in accepted))
            release.set()
            self.wait_until(lambda: not self.server.proxies)
        executed = [e for e in self.events() if e["event"] == "executed"]
        self.assertEqual(executed, [])
        self.assertFalse(any(e["event"] == "recv" and e["message"].get("method") == "tools/call" for e in self.events()))

    def test_dispatched_extension_and_sse_deadline_report_uncertain_outcome(self):
        server = self.start_server(request_timeout_s=.7)
        status, _, body = self.post(request("fixture/extension-hang", 111), server=server)
        self.assertEqual(status, 504)
        self.assertTrue(body["error"]["data"]["outcomeUnknown"])
        message = request("tools/call", 112, name="slow", meta={"progressToken":"t"})
        connection = http.client.HTTPConnection(*server.server_address[:2], timeout=3)
        connection.request("POST", "/mcp", body=json.dumps(message).encode(), headers=self.headers(message))
        response = connection.getresponse()
        self.assertEqual(response.getheader("Content-Type"), "text/event-stream")
        payloads = [json.loads(line[6:]) for line in response.read().splitlines() if line.startswith(b"data: ")]
        connection.close()
        self.assertEqual(payloads[0]["method"], "notifications/progress")
        self.assertTrue(payloads[-1]["error"]["data"]["outcomeUnknown"])
        self.assertEqual(len([e for e in self.events() if e["event"] == "executed" and e.get("id") == 112]), 1)

    def test_modern_cli_serves_registered_target_and_sigterm_reaps_active_proxy(self):
        port = free_port()
        process = subprocess.Popen([
            sys.executable, str(ROOT / "stdio_http_facade.py"), "--protocol-era", "modern",
            "--side", "wsl", "--target", "modern-fixture", "--node-port", str(self.wsl_port),
            "--listen-port", str(port), "--request-timeout-s", "3"], cwd=ROOT,
            env={**os.environ, "PYTHONDONTWRITEBYTECODE":"1"}, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        connection = None
        response = None
        try:
            def listening():
                try:
                    with socket.create_connection(("127.0.0.1", port), timeout=.1):
                        return True
                except OSError:
                    return False
            self.wait_until(listening)
            message = request("tools/call", 121, name="slow", meta={"progressToken":"cli-token"})
            headers = self.headers(message)
            headers["Authorization"] = SECRET
            connection = http.client.HTTPConnection("127.0.0.1", port, timeout=4)
            connection.request("POST", "/mcp", body=json.dumps(message).encode(), headers=headers)
            response = connection.getresponse()
            self.assertEqual(response.getheader("Content-Type"), "text/event-stream")
            self.assertEqual(response.readline(), b"event: message\n")
            self.assertEqual(json.loads(response.readline()[6:])["params"]["progressToken"], "cli-token")
            process.terminate()
            stdout, stderr = process.communicate(timeout=5)
            self.assertEqual(process.returncode, 0, stderr.decode())
            self.assertEqual(stdout, b"")
            self.assertNotIn(SECRET, stderr.decode())
            self.wait_until(lambda: all(self.pid_gone(e["pid"]) for e in self.events() if e["event"] == "spawn"))
            self.assertEqual(len([e for e in self.events() if e["event"] == "executed" and e.get("id") == 121]), 1)
        finally:
            if response is not None:
                response.close()
            if connection is not None:
                connection.close()
            if process.poll() is None:
                process.kill()
            process.communicate(timeout=3)

    def test_cli_era_is_explicit_and_modern_command_override_is_rejected(self):
        parser = facade.build_parser()
        args = parser.parse_args(["--side","wsl","--target","modern-fixture"])
        self.assertEqual(args.protocol_era, "legacy")
        self.assertEqual(parser.parse_args(["--side","wsl","--target","modern-fixture","--protocol-era","modern"]).protocol_era, "modern")
        with self.assertRaises(facade.FacadeError):
            facade.FacadeOptions(side="wsl", target="modern-fixture", protocol_era="modern", backend_command=[sys.executable,"arbitrary.py"])
        self.assertEqual(self.server.argv[2:], ["connect","--local-host","127.0.0.1","--local-port",str(self.wsl_port),"modern-fixture"])
        self.assertNotIn("--url", self.server.argv)
        self.assertEqual(self.events(), [])


if __name__ == "__main__":
    unittest.main()
