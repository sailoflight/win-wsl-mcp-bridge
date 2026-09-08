"""Fixture-only checks for explicit MCP 2026-07-28 owner-host conversion.

Uses real loopback HTTP sockets. No installed MCP, credentials or bridge node.
"""
from __future__ import annotations

import base64
import io
import json
import os
import queue
import socket
import subprocess
import sys
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch

import streamable_http_stdio as adapter

VERSION = "2026-07-28"
SECRET = "fixture-private-header-secret"
ROOT = Path(__file__).resolve().parents[1]


def request(method="tools/list", request_id=1, **params):
    meta = {adapter.MODERN_VERSION_META: VERSION, adapter.MODERN_CAPABILITIES_META: {}}
    meta.update(params.pop("meta", {}))
    return {"jsonrpc": "2.0", "id": request_id, "method": method,
            "params": {"_meta": meta, **params}}


def result(request_id, **fields):
    return {"jsonrpc": "2.0", "id": request_id,
            "result": {"resultType": "complete", **fields}}


class Output:
    def __init__(self):
        self.messages = queue.Queue()

    def write(self, data):
        self.messages.put(json.loads(data))

    def flush(self):
        pass

    def get(self, timeout=3):
        return self.messages.get(timeout=timeout)


class Fixture(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self):
        super().__init__(("127.0.0.1", 0), Handler)
        self.records = []
        self.tools = [{"name": "echo", "inputSchema": {"type": "object"}}]
        self.catalog_pages = None
        self.catalog_status = 200
        self.catalog_sse = False
        self.catalog_hold = False
        self.executed_call_ids = []
        self.started = threading.Event()
        self.disconnected = threading.Event()
        self.release = threading.Event()
        self.thread = threading.Thread(target=self.serve_forever, daemon=True)
        self.thread.start()

    def handle_error(self, request, client_address):
        # Closing a bounded/cancelled exchange may reset an unread HTTP body.
        # Suppress only this expected fixture disconnect, not handler defects.
        if isinstance(sys.exc_info()[1], (ConnectionResetError, BrokenPipeError)):
            return
        super().handle_error(request, client_address)

    def finish(self):
        self.release.set()
        self.shutdown()
        self.server_close()
        self.thread.join(2)


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def do_GET(self):
        self.server.records.append(("GET", {}, None))
        self.send_error(405)

    def do_DELETE(self):
        self.server.records.append(("DELETE", {}, None))
        self.send_error(405)

    def send_json(self, body, status=200):
        raw = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("Mcp-Session-Id", "must-never-be-reused")
        self.send_header("X-Private", SECRET)
        self.end_headers()
        self.wfile.write(raw)
        self.wfile.flush()

    def event(self, body):
        self.wfile.write(("event: message\ndata: " + json.dumps(body) + "\n\n").encode())
        self.wfile.flush()

    def do_POST(self):
        message = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        self.server.records.append(("POST", dict(self.headers), message))
        method, params, rid = message["method"], message["params"], message["id"]
        mode = params.get("mode")
        try:
            if method == "tools/call" and mode in ("execute-disconnect", "execute-deadline"):
                # This completed fixture mutation is deliberately never rolled
                # back when its response disappears.
                self.server.executed_call_ids.append(rid)
                self.close_connection = True
                if mode == "execute-deadline":
                    self.connection.settimeout(3)
                    self.connection.recv(1)
                else:
                    self.connection.shutdown(socket.SHUT_RDWR)
                    self.connection.close()
                return
            if mode in ("deep-json", "deep-sse"):
                raw = b'[' * 20000 + b'0' + b']' * 20000
                if mode == "deep-sse":
                    raw = b"data: " + raw + b"\n\n"
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream" if mode == "deep-sse" else "application/json")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)
                self.wfile.flush()
            elif mode == "status":
                code = params.get("code", -32000)
                self.send_json({"jsonrpc": "2.0", "error": {
                    "code": code, "message": SECRET,
                    "data": {"supported": [VERSION], "requested": VERSION, "secret": SECRET}}}, params["status"])
            elif mode in ("sse", "cancel", "timeout", "wrong-token", "server-request", "sse-big"):
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
                self.send_header("Connection", "close")
                self.end_headers()
                self.close_connection = True
                self.wfile.write(b": heartbeat\n\n")
                self.wfile.flush()
                if mode == "sse-big":
                    self.wfile.write(b":" + b"x" * 3000 + b"\n\n")
                    self.wfile.flush()
                    return
                if mode == "server-request":
                    self.event({"jsonrpc": "2.0", "id": "server", "method": "sampling/createMessage", "params": {}})
                    return
                if mode != "timeout":
                    self.event({"jsonrpc": "2.0", "method": "notifications/progress", "params": {
                        "progressToken": "wrong" if mode == "wrong-token" else params["_meta"]["progressToken"],
                        "progress": 1, "total": 2}})
                self.server.started.set()
                if mode in ("cancel", "timeout"):
                    self.connection.settimeout(3)
                    if self.connection.recv(1) == b"":
                        self.server.disconnected.set()
                    return
                self.server.release.wait(2)
                self.event(result(rid, content=[{"type": "text", "text": "sse-done"}]))
            elif mode == "slow-headers":
                self.server.started.set()
                self.connection.settimeout(3)
                if self.connection.recv(1) == b"":
                    self.server.disconnected.set()
            elif mode == "large":
                self.send_json(result(rid, content="x" * 3000))
            elif mode == "wrong-id":
                self.send_json(result("unrelated"))
            elif mode == "missing-result-type":
                self.send_json({"jsonrpc": "2.0", "id": rid, "result": {}})
            elif mode == "input-required":
                body = result(rid, requestState="opaque-state", inputRequests={"i": {"method": "roots/list"}})
                body["result"]["resultType"] = "input_required"
                self.send_json(body)
            elif method == "server/discover":
                self.send_json(result(rid, supportedVersions=[VERSION], capabilities={"tools": {}},
                    instructions="Fixture guidance before tool decisions", _meta={
                        "io.modelcontextprotocol/serverInfo": {"name": "actual-fixture", "version": "2"}}))
            elif method == "tools/list":
                if self.server.catalog_hold:
                    self.server.started.set()
                    self.connection.settimeout(3)
                    if self.connection.recv(1) == b"":
                        self.server.disconnected.set()
                    return
                fields = {"tools": self.server.tools}
                if self.server.catalog_pages is not None:
                    fields = self.server.catalog_pages.get(params.get("cursor"), {"tools": []})
                body = result(rid, **fields)
                if self.server.catalog_status != 200:
                    body = {"jsonrpc": "2.0", "id": rid, "error": {"code": -32000, "message": SECRET}}
                if self.server.catalog_sse:
                    self.send_response(200)
                    self.send_header("Content-Type", "text/event-stream")
                    self.send_header("Connection", "close")
                    self.end_headers()
                    self.close_connection = True
                    self.event(body)
                else:
                    self.send_json(body, self.server.catalog_status)
            else:
                self.send_json(result(rid, content=[{"type": "text", "text": "actual-call"}]))
        except (OSError, TimeoutError):
            pass


class ModernAdapterTests(unittest.TestCase):
    def setUp(self):
        self.server = Fixture()
        self.output = Output()
        self.diag = io.StringIO()
        self.active = []
        self.client = self.make_adapter()

    def tearDown(self):
        for client in self.active:
            client.shutdown()
        self.server.finish()

    def make_adapter(self, **kwargs):
        opts = dict(endpoint_url=f"http://127.0.0.1:{self.server.server_port}/mcp",
                    protocol_era="modern", request_timeout_s=2, idle_timeout_s=2,
                    headers={"Authorization": SECRET, "Mcp-Session-Id": SECRET,
                             "MCP-Protocol-Version": "wrong", "Mcp-Method": "wrong",
                             "Mcp-Name": "wrong", "Last-Event-ID": SECRET})
        opts.update(kwargs)
        client = adapter.ModernStdioStreamableHttpAdapter(adapter.AdapterOptions(**opts), self.output, adapter._Diag(self.diag))
        client.start()
        self.active.append(client)
        return client

    def test_discover_list_call_forward_exact_metadata_identity_and_instructions(self):
        for index, method in enumerate(("server/discover", "tools/list", "tools/call")):
            message = request(method, index, **({"name": "echo", "arguments": {"a": 3}} if index == 2 else {}))
            message["params"]["_meta"][adapter.MODERN_CAPABILITIES_META] = {"roots": {}} if index == 2 else {}
            self.client.handle(message)
            reply = self.output.get()
            if index == 0:
                self.assertEqual(reply["result"]["instructions"], "Fixture guidance before tool decisions")
                self.assertEqual(reply["result"]["_meta"]["io.modelcontextprotocol/serverInfo"]["name"], "actual-fixture")
            self.assertEqual(reply["result"]["resultType"], "complete")
            verb, headers, actual = self.server.records[-1]
            lower = {k.lower(): v for k, v in headers.items()}
            self.assertEqual(verb, "POST")
            self.assertEqual(actual, message)
            self.assertEqual(lower["mcp-protocol-version"], VERSION)
            self.assertEqual(lower["mcp-method"], method)
            self.assertEqual(lower["accept"], adapter.ACCEPT_JSON_AND_EVENT)
            self.assertEqual(lower["authorization"], SECRET)
            self.assertNotIn("mcp-session-id", lower)
            self.assertNotIn("last-event-id", lower)
            if index == 2:
                self.assertEqual(lower["mcp-name"], "echo")
            else:
                self.assertNotIn("mcp-name", lower)
            self.assertNotIn(SECRET, json.dumps(reply))
        self.client.shutdown()
        self.assertEqual([record[0] for record in self.server.records], ["POST"] * 4)
        self.assertEqual([record[2]["method"] for record in self.server.records],
                         ["server/discover", "tools/list", "tools/list", "tools/call"])
        self.assertEqual(self.server.records[-2][2]["params"]["_meta"], message["params"]["_meta"])
        self.assertNotIn(SECRET, self.diag.getvalue())

    def test_explicit_mode_cli_and_default_legacy(self):
        parser = adapter.build_parser()
        self.assertEqual(parser.parse_args(["--url", "http://localhost/mcp"]).protocol_era, "legacy")
        message = {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}}
        process = subprocess.run([sys.executable, str(ROOT / "streamable_http_stdio.py"),
            "--url", f"http://127.0.0.1:{self.server.server_port}/mcp", "--protocol-era", "modern"],
            input=json.dumps(message)+"\n", text=True, capture_output=True, timeout=3,
            env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"})
        self.assertEqual(process.returncode, 0, process.stderr)
        reply = json.loads(process.stdout)
        self.assertEqual(reply["error"]["code"], -32601)
        self.assertIn(VERSION, reply["error"]["message"])
        self.assertNotIn("result", reply)
        self.assertEqual(self.server.records, [])

    def test_invalid_static_header_diagnostic_never_echoes_value(self):
        process = subprocess.run([sys.executable, str(ROOT / "streamable_http_stdio.py"),
            "--url", "http://127.0.0.1:1/mcp", "--protocol-era", "modern", "--header", SECRET],
            input="", text=True, capture_output=True, timeout=3,
            env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"})
        self.assertEqual(process.returncode, 2)
        self.assertEqual(process.stdout, "")
        self.assertNotIn(SECRET, process.stderr)

    def test_metadata_required_on_every_request_no_fabricated_handshake(self):
        bad = request()
        del bad["params"]["_meta"][adapter.MODERN_CAPABILITIES_META]
        self.client.handle(bad)
        self.assertEqual(self.output.get()["error"]["code"], -32602)
        self.client.handle(request(meta={adapter.MODERN_VERSION_META: "2025-06-18"}))
        self.assertEqual(self.output.get()["error"]["code"], -32022)
        self.client.handle({"jsonrpc": "2.0", "method": "notifications/initialized"})
        self.client.handle({"jsonrpc": "2.0", "id": 5, "result": {}})
        self.assertEqual(self.output.get()["error"]["code"], -32600)
        self.assertEqual(self.server.records, [])

    def test_name_headers_encode_unicode_controls_and_sentinels(self):
        for index, (method, name) in enumerate((("tools/call", "世界"), ("resources/read", "file:///a\nb"),
                                               ("prompts/get", "=?base64?literal?="), ("tools/call", " padded "))):
            self.server.tools = [{"name": name, "inputSchema": {"type": "object"}}]
            self.client.handle(request(method, index, **{"uri" if method == "resources/read" else "name": name}))
            self.assertIn("result", self.output.get())
            headers = {k.lower(): v for k, v in self.server.records[-1][1].items()}
            self.assertEqual(headers["mcp-name"], "=?base64?" + base64.b64encode(name.encode()).decode() + "?=")

    def test_sse_progress_is_immediate_and_scoped_while_other_request_completes(self):
        self.client.handle(request("tools/call", 1, name="echo", mode="sse", meta={"progressToken": "token-1"}))
        progress = self.output.get()
        self.assertEqual(progress["params"]["progressToken"], "token-1")
        self.client.handle(request(request_id=2))
        self.assertEqual(self.output.get()["id"], 2)
        self.server.release.set()
        self.assertEqual(self.output.get()["id"], 1)

    def test_cancel_closes_socket_without_notification_post_and_isolates_requests(self):
        self.client.handle(request("tools/call", 1, name="echo", mode="cancel", meta={"progressToken": "t"}))
        self.output.get()
        self.assertTrue(self.server.started.wait(2))
        self.client.handle({"jsonrpc": "2.0", "method": "notifications/cancelled", "params": {"requestId": 1}})
        self.assertTrue(self.server.disconnected.wait(2))
        self.client.handle(request(request_id=2))
        self.assertEqual(self.output.get()["id"], 2)
        self.assertTrue(self.output.messages.empty())
        self.assertEqual(len(self.server.records), 3)
        self.assertEqual([r[2]["method"] for r in self.server.records], ["tools/list", "tools/call", "tools/list"])

    def test_deadline_closes_headers_and_sse_without_replay(self):
        for mode in ("timeout", "slow-headers"):
            with self.subTest(mode=mode):
                self.server.disconnected.clear()
                client = self.make_adapter(request_timeout_s=0.15)
                client.handle(request(mode=mode))
                reply = self.output.get()
                self.assertIn("timeout", reply["error"]["message"])
                self.assertTrue(self.server.disconnected.wait(2))
        self.assertEqual(len(self.server.records), 2)

    def test_http_failures_preserve_modern_codes_redact_and_never_replay(self):
        cases = [(400, -32020), (400, -32022), (400, -32021), (404, -32601),
                 (401, -32000), (403, -32000), (404, -32000), (429, -32000), (503, -32000)]
        for index, (status, code) in enumerate(cases):
            self.client.handle(request(request_id=index, mode="status", status=status, code=code))
            reply = self.output.get()
            self.assertEqual(reply["error"]["code"], code)
            self.assertEqual(reply["error"]["data"]["httpStatus"], status)
            self.assertNotIn(SECRET, json.dumps(reply))
        self.assertEqual(len(self.server.records), len(cases))
        self.assertNotIn(SECRET, self.diag.getvalue())

    def test_bounds_wrong_id_result_type_and_unrelated_sse_rejected(self):
        client = self.make_adapter(max_message_bytes=1024)
        for index, mode in enumerate(("large", "sse-big", "wrong-id", "missing-result-type", "wrong-token", "server-request")):
            client.handle(request(request_id=index, mode=mode, meta={"progressToken": "t"}))
            self.assertIn("error", self.output.get())
        count = len(self.server.records)
        client.handle(request(request_id=9, huge="x" * 2000))
        self.assertIn("request-too-large", self.output.get()["error"]["message"])
        self.assertEqual(len(self.server.records), count)

    def test_input_required_is_transparent_without_automatic_followup(self):
        self.client.handle(request(mode="input-required"))
        reply = self.output.get()
        self.assertEqual(reply["result"]["resultType"], "input_required")
        self.assertEqual(reply["result"]["requestState"], "opaque-state")
        self.assertEqual(len(self.server.records), 1)

    def test_malformed_unicode_is_local_error_without_interrupting_active_request(self):
        self.client.handle(request(request_id=1, mode="sse", meta={"progressToken": "t"}))
        self.output.get()
        self.client.handle(request("tools/call", 2, name="\ud800"))
        self.assertEqual(self.output.get()["error"]["code"], -32600)
        self.client.handle(request(request_id="\ud800"))
        malformed_id = self.output.get()
        self.assertIsNone(malformed_id["id"])
        self.assertEqual(malformed_id["error"]["code"], -32600)
        self.server.release.set()
        self.assertEqual(self.output.get()["id"], 1)
        self.assertEqual(len(self.server.records), 1)

    def test_deep_json_and_sse_return_terminal_errors(self):
        for index, mode in enumerate(("deep-json", "deep-sse")):
            self.client.handle(request(request_id=index, mode=mode))
            reply = self.output.get()
            self.assertEqual(reply["id"], index)
            self.assertIn("bad-response", reply["error"]["message"])
        self.client.handle(request(request_id=3))
        self.assertIn("result", self.output.get())

    def test_shutdown_interrupts_tls_handshake_before_response_exists(self):
        listener = socket.socket()
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        listener.settimeout(2)
        started, disconnected = threading.Event(), threading.Event()
        def stall_tls():
            try:
                sock, _ = listener.accept()
                with sock:
                    sock.settimeout(2)
                    sock.recv(4096)  # TLS ClientHello; never send ServerHello.
                    started.set()
                    if sock.recv(4096) == b"":
                        disconnected.set()
            except OSError:
                pass
        thread = threading.Thread(target=stall_tls, daemon=True)
        thread.start()
        try:
            client = self.make_adapter(endpoint_url=f"https://127.0.0.1:{listener.getsockname()[1]}/mcp",
                                       connect_timeout_s=5, request_timeout_s=5)
            client.handle(request())
            self.assertTrue(started.wait(2))
            before = time.monotonic()
            client.shutdown()
            self.assertLess(time.monotonic() - before, 0.7)
            self.assertTrue(disconnected.wait(1))
            self.assertTrue(self.output.messages.empty())
        finally:
            listener.close()
            thread.join(2)

    def test_dns_wait_is_cancellable_and_one_resolver_is_shared(self):
        started, release = threading.Event(), threading.Event()
        calls = []
        def stalled_resolver(*args, **kwargs):
            calls.append(args)
            started.set()
            release.wait(2)
            return []
        client = self.make_adapter(endpoint_url="http://fixture.invalid/mcp", request_timeout_s=5)
        try:
            with patch.object(adapter.socket, "getaddrinfo", stalled_resolver):
                client.handle(request(request_id=1))
                self.assertTrue(started.wait(1))
                client.handle(request(request_id=2))
                before = time.monotonic()
                client.shutdown()
                self.assertLess(time.monotonic() - before, 0.7)
                self.assertEqual(len(calls), 1)
                self.assertTrue(self.output.messages.empty())
        finally:
            release.set()
            self.assertTrue(client.client._resolved.wait(2))

    def test_capacity_bound_cancellation_not_queued_behind_workers(self):
        client = self.make_adapter(max_inflight=1)
        client.handle(request(request_id=1, mode="cancel", meta={"progressToken": "t"}))
        self.output.get()
        client.handle(request(request_id=2))
        self.assertIn("capacity", self.output.get()["error"]["message"])
        client.handle({"jsonrpc": "2.0", "method": "notifications/cancelled", "params": {"requestId": 1}})
        self.assertTrue(self.server.disconnected.wait(2))
        self.assertEqual(len(self.server.records), 1)


    def annotated_tool(self, properties):
        return {"name": "echo", "inputSchema": {"type": "object", "properties": properties}}

    def test_direct_annotated_call_preflights_and_maps_exact_primitive_headers(self):
        self.server.tools = [self.annotated_tool({
            "region": {"type": "string", "x-mcp-header": "Region"},
            "nested": {"type": "object", "properties": {
                "flag": {"type": "boolean", "x-mcp-header": "Enabled"},
                "count": {"type": "integer", "x-mcp-header": "Count"}}},
            "absent": {"type": "string", "x-mcp-header": "Absent"},
            "null": {"type": "string", "x-mcp-header": "Null"},
            "authority": {"type": "string", "x-mcp-header": "Host"},
            "auth": {"type": "string", "x-mcp-header": "Authorization"},
        })]
        client = self.make_adapter(headers={"Authorization": SECRET, "Mcp-Param-Region": "spoofed",
                                           "Host": "attacker.invalid", "Mcp-Name": "spoofed"})
        message = request("tools/call", 19, name="echo", arguments={
            "region": "世界", "nested": {"flag": False, "count": 42.0},
            "null": None, "authority": "attacker.invalid", "auth": "untrusted"},
            meta={adapter.MODERN_CAPABILITIES_META: {"roots": {}}, "progressToken": "t"})
        client.handle(message)
        self.assertIn("result", self.output.get())
        self.assertEqual([r[2]["method"] for r in self.server.records], ["tools/list", "tools/call"])
        self.assertEqual(self.server.records[0][2]["params"]["_meta"], message["params"]["_meta"])
        self.assertEqual(self.server.records[1][2], message)
        headers = {k.lower(): v for k, v in self.server.records[1][1].items()}
        self.assertEqual(headers["mcp-param-region"], "=?base64?" + base64.b64encode("世界".encode()).decode() + "?=")
        self.assertEqual(headers["mcp-param-enabled"], "false")
        self.assertEqual(headers["mcp-param-count"], "42")
        self.assertNotIn("mcp-param-null", headers)
        self.assertNotIn("mcp-param-absent", headers)
        self.assertEqual(headers["mcp-param-host"], "attacker.invalid")
        self.assertEqual(headers["mcp-param-authorization"], "untrusted")
        self.assertEqual(headers["authorization"], SECRET)
        self.assertEqual(headers["host"], f"127.0.0.1:{self.server.server_port}")
        self.assertEqual(headers["mcp-name"], "echo")
        self.assertNotIn(SECRET, self.diag.getvalue())

    def test_annotated_string_encoding_null_absent_and_safe_integer_boundaries(self):
        self.server.tools = [self.annotated_tool({
            "text": {"type": "string", "x-mcp-header": "Text"},
            "number": {"type": "integer", "x-mcp-header": "Number"}})]
        for rid, text in enumerate((" padded ", "a\nb", "=?base64?literal?=", "plain")):
            self.client.handle(request("tools/call", rid, name="echo", arguments={"text": text, "number": -(2 ** 53 - 1)}))
            self.assertIn("result", self.output.get())
            headers = {k.lower(): v for k, v in self.server.records[-1][1].items()}
            expected = text if text == "plain" else "=?base64?" + base64.b64encode(text.encode()).decode() + "?="
            self.assertEqual(headers["mcp-param-text"], expected)
            self.assertEqual(headers["mcp-param-number"], str(-(2 ** 53 - 1)))
        for rid, value in enumerate((2 ** 53, -(2 ** 53), 1.5, True, "42"), 10):
            before = len(self.server.records)
            self.client.handle(request("tools/call", rid, name="echo", arguments={"number": value}))
            self.assertEqual(self.output.get()["error"]["data"]["category"], "invalid-tool-header-value")
            self.assertEqual(len(self.server.records), before + 1)
            self.assertEqual(self.server.records[-1][2]["method"], "tools/list")

    def test_invalid_annotation_definitions_filtered_and_direct_calls_fail_closed(self):
        invalid = [
            {"value": {"type": "string", "x-mcp-header": value}} for value in ("", "a\nb", "bad name", "世界", 4)
        ] + [
            {"value": {"type": kind, "x-mcp-header": "V"}} for kind in ("number", "object", "array", ["string", "null"])
        ] + [
            {"a": {"type": "string", "x-mcp-header": "same"}, "b": {"type": "string", "x-mcp-header": "SAME"}},
            {"a": {"type": "array", "items": {"type": "string", "x-mcp-header": "Bad"}}},
            {"a": {"oneOf": [{"type": "string", "x-mcp-header": "Bad"}]}},
            {"a": {"allOf": [{"type": "string", "x-mcp-header": "Bad"}]}},
            {"a": {"if": {"type": "string", "x-mcp-header": "Bad"}}},
            {"a": {"$defs": {"T": {"type": "string", "x-mcp-header": "Bad"}}, "$ref": "#/$defs/T"}},
        ]
        for index, properties in enumerate(invalid):
            with self.subTest(index=index):
                self.server.tools = [self.annotated_tool(properties), {"name": "safe", "inputSchema": {"type": "object"}}]
                self.client.handle(request(request_id=f"list-{index}"))
                self.assertEqual([t["name"] for t in self.output.get()["result"]["tools"]], ["safe"])
                before = len(self.server.records)
                self.client.handle(request("tools/call", f"call-{index}", name="echo", arguments={"value": "x"}))
                reply = self.output.get()
                self.assertEqual(reply["error"]["data"]["requiredFeature"], "x-mcp-header")
                self.assertEqual(len(self.server.records), before + 1)
                self.assertEqual(self.server.records[-1][2]["method"], "tools/list")

    def test_catalog_preflight_paginates_sse_and_has_hard_page_bound(self):
        target = self.annotated_tool({"v": {"type": "string", "x-mcp-header": "Value"}})
        self.server.catalog_sse = True
        self.server.catalog_pages = {None: {"tools": [], "nextCursor": "page2"}, "page2": {"tools": [target]}}
        self.client.handle(request("tools/call", 1, name="echo", arguments={"v": "yes"}))
        self.assertIn("result", self.output.get())
        self.assertEqual([r[2]["method"] for r in self.server.records], ["tools/list", "tools/list", "tools/call"])
        self.assertEqual(self.server.records[1][2]["params"]["cursor"], "page2")
        self.server.catalog_pages = {None: {"tools": [], "nextCursor": "1"}, **{
            str(i): {"tools": [], "nextCursor": str(i + 1)} for i in range(1, 5)}}
        before = len(self.server.records)
        self.client.handle(request("tools/call", 2, name="missing"))
        self.assertIn("page limit", self.output.get()["error"]["message"])
        self.assertEqual(len(self.server.records), before + 4)
        self.assertTrue(all(r[2]["method"] == "tools/list" for r in self.server.records[before:]))

    def test_executed_call_disconnect_and_deadline_report_uncertainty_without_replay(self):
        client = self.make_adapter(request_timeout_s=0.2)
        for index, mode in enumerate(("execute-disconnect", "execute-deadline"), 100):
            with self.subTest(mode=mode):
                before = len(self.server.records)
                client.handle(request("tools/call", index, name="echo", mode=mode))
                reply = self.output.get()
                self.assertTrue(reply["error"]["data"]["outcomeUnknown"])
                self.assertNotIn("retry", reply["error"]["message"].lower())
                self.assertEqual(self.server.executed_call_ids.count(index), 1)
                sent = self.server.records[before:]
                self.assertEqual([r[2]["method"] for r in sent], ["tools/list", "tools/call"])
                self.assertEqual(sum(r[2]["method"] == "tools/call" for r in sent), 1)
        client.shutdown()
        self.assertEqual(len(self.server.executed_call_ids), 2)
        self.assertEqual(len(self.server.records), 4)

    def test_transport_error_itself_serializes_outcome_without_stdio_wrapper(self):
        transport = self.make_adapter(request_timeout_s=0.2).client
        self.server.catalog_status = 503
        with self.assertRaises(adapter.HttpTransportError) as failure:
            transport.request(request("tools/call", 200, name="echo"), adapter._ModernPending(), lambda item: None)
        self.assertFalse(failure.exception.to_jsonrpc(200)["error"]["data"]["outcomeUnknown"])
        self.assertEqual([r[2]["method"] for r in self.server.records], ["tools/list"])
        self.assertEqual(self.server.executed_call_ids, [])
        self.server.catalog_status = 200
        for rid, mode in enumerate(("execute-disconnect", "execute-deadline"), 201):
            with self.subTest(mode=mode):
                before = len(self.server.records)
                with self.assertRaises(adapter.HttpTransportError) as failure:
                    transport.request(request("tools/call", rid, name="echo", mode=mode),
                                      adapter._ModernPending(), lambda item: None)
                reply = failure.exception.to_jsonrpc(rid)
                self.assertTrue(reply["error"]["data"]["outcomeUnknown"])
                self.assertEqual(self.server.executed_call_ids.count(rid), 1)
                self.assertEqual([r[2]["method"] for r in self.server.records[before:]], ["tools/list", "tools/call"])
        self.assertEqual(len(self.server.records), 5)
        self.assertEqual(len(self.server.executed_call_ids), 2)
        self.assertTrue(self.output.messages.empty())  # This never used the stdio error writer.

    def test_preflight_and_local_refusal_have_known_nonexecution_outcome(self):
        self.server.catalog_status = 503
        self.client.handle(request("tools/call", 100, name="echo"))
        self.assertFalse(self.output.get()["error"]["data"]["outcomeUnknown"])
        self.assertEqual([r[2]["method"] for r in self.server.records], ["tools/list"])
        self.server.catalog_status = 200
        self.server.catalog_hold = True
        client = self.make_adapter(request_timeout_s=0.1)
        client.handle(request("tools/call", 101, name="echo"))
        self.assertFalse(self.output.get()["error"]["data"]["outcomeUnknown"])
        self.server.catalog_hold = False
        self.server.tools = [self.annotated_tool({"v": {"type": "number", "x-mcp-header": "Bad"}})]
        self.client.handle(request("tools/call", 102, name="echo", arguments={"v": 2}))
        self.assertFalse(self.output.get()["error"]["data"]["outcomeUnknown"])
        malformed = request("tools/call", 103, name="echo")
        del malformed["params"]["_meta"]
        self.client.handle(malformed)
        self.assertFalse(self.output.get()["error"]["data"]["outcomeUnknown"])
        self.assertTrue(all(r[2]["method"] == "tools/list" for r in self.server.records))
        self.assertEqual(self.server.executed_call_ids, [])

    def test_preflight_failure_cancel_and_schema_changes_never_send_unverified_call(self):
        self.server.catalog_status = 503
        self.client.handle(request("tools/call", 1, name="echo"))
        self.assertIn("error", self.output.get())
        self.assertEqual([r[2]["method"] for r in self.server.records], ["tools/list"])
        self.server.catalog_status = 200
        self.server.tools = [self.annotated_tool({"v": {"type": "string", "x-mcp-header": "Before"}})]
        self.client.handle(request("tools/call", 2, name="echo", arguments={"v": "a"}))
        self.assertIn("result", self.output.get())
        self.server.tools = [self.annotated_tool({"v": {"type": "string", "x-mcp-header": "After"}})]
        self.client.handle(request("tools/call", 3, name="echo", arguments={"v": "b"}))
        self.assertIn("result", self.output.get())
        headers = {k.lower(): v for k, v in self.server.records[-1][1].items()}
        self.assertEqual(headers["mcp-param-after"], "b")
        self.assertNotIn("mcp-param-before", headers)
        self.server.catalog_hold = True
        before = len(self.server.records)
        self.client.handle(request("tools/call", 4, name="echo"))
        self.assertTrue(self.server.started.wait(2))
        self.client.handle({"jsonrpc": "2.0", "method": "notifications/cancelled", "params": {"requestId": 4}})
        self.assertTrue(self.server.disconnected.wait(2))
        self.client.shutdown()
        self.assertEqual(len(self.server.records), before + 1)
        self.assertEqual(self.server.records[-1][2]["method"], "tools/list")
        self.assertTrue(self.output.messages.empty())


if __name__ == "__main__":
    unittest.main()
