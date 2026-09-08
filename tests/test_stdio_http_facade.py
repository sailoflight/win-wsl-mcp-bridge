#!/usr/bin/env python3
"""Tests for the explicit stdio-to-Streamable-HTTP Agent-facing facade.

The facade is a real Streamable HTTP MCP *server* whose sessions are answered by
one isolated stdio backend each (the production route is ``bridge.py connect
<target>``; these tests use an Operator-supplied local backend override pointed
at the existing ``fixture_mcp.py`` stdio MCP or at small scripted backends).

Coverage:

* initialize session issuance (Mcp-Session-Id / MCP-Protocol-Version headers)
  and request/notification/response forwarding to the stdio backend;
* tools/list and tools/call round-trips through fixture_mcp.py;
* session header enforcement (400 missing / 404 unknown / duplicate
  initialize), DELETE teardown, and stale-session 404 after DELETE;
* one isolated backend subprocess per session (distinct pids) with bounded
  request bodies and a session capacity 429;
* backend notifications relayed over the session GET SSE stream;
* a backend server request with no open SSE stream is refused with a
  structured JSON-RPC error so the backend never hangs;
* an SSE-only POST (Accept: text/event-stream) receives a streamed response;
* a backend that exits surfaces a structured JSON-RPC transport error;
* the default connector argv is exactly ``bridge.py connect <target>`` against
  the local loopback node (no peer command/argv/env ever enters the argv);
* CLI subprocess smoke test on a real port.

Runs offline with the standard library only.
"""

from __future__ import annotations

import errno
import http.client
import ipaddress
import json
import os
import queue
import socket
import subprocess
import sys
import tempfile
import threading
import time
import types
import unittest
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import stdio_http_facade as shf  # noqa: E402  (facade under test)

FACADE = ROOT / "stdio_http_facade.py"
FIXTURE_MCP = ROOT / "tests" / "fixtures" / "fixture_mcp.py"
PROTOCOL = "2025-06-18"
SESSION_HEADER = "Mcp-Session-Id"
PROTOCOL_HEADER = "MCP-Protocol-Version"


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _quiet_facade_handle_error(server: "shf.FacadeServer") -> None:
    """Bind a filtered handle_error onto one in-process facade instance.

    The facade is a real server: clients under test deliberately abort sockets
    (session DELETE teardown, backend-loss recovery, cancelled SSE streams), so
    the stdlib handler routinely observes ConnectionReset/BrokenPipe on a
    connection it is still reading or answering.  Those expected peer resets
    would otherwise spam stderr via socketserver's default handle_error.  Only
    the transport-error classes and the stdlib "truncated request line" ValueError
    raised when a peer dies mid-request are suppressed; every other failure
    still reaches the runtime's original handle_error so genuine bugs surface.
    """
    original = server.handle_error  # bound method; never touched by filter path

    def _filtered(self: Any, request: Any, client_address: Any) -> None:
        exc = sys.exc_info()[1]
        if isinstance(exc, (ConnectionResetError, BrokenPipeError,
                            ConnectionAbortedError)):
            return
        if isinstance(exc, OSError) and exc.errno in (
            errno.ECONNRESET, errno.EPIPE, errno.ECONNABORTED, errno.EBADF,
        ):
            return
        if isinstance(exc, ValueError):
            frame = sys.exc_info()[2]
            while frame is not None:
                code = frame.tb_frame.f_code
                if code.co_name in ("parse_request", "handle_one_request") and code.co_filename.endswith(
                    "http" + os.sep + "server.py"
                ):
                    return
                frame = frame.tb_next
        original(request, client_address)

    server.handle_error = types.MethodType(_filtered, server)


def rpc_request(method: str, request_id: Any = None, params: dict[str, Any] | None = None) -> dict[str, Any]:
    message: dict[str, Any] = {"jsonrpc": "2.0", "method": method}
    if request_id is not None:
        message["id"] = request_id
    if params is not None:
        message["params"] = params
    return message


def wait_for(queue_: queue.Queue, predicate: Any, timeout: float = 5.0) -> Any:
    deadline = time.monotonic() + timeout
    while True:
        try:
            item = queue_.get(timeout=0.1)
        except queue.Empty:
            if time.monotonic() > deadline:
                raise AssertionError("timed out waiting for queue item")
            continue
        if predicate(item):
            return item
        if time.monotonic() > deadline:
            raise AssertionError(f"timed out; last item {item!r}")


# --------------------------------------------------------------------------
# Minimal direct Streamable HTTP client (JSON mode) + SSE GET reader
# --------------------------------------------------------------------------

class RpcClient:
    """Small synchronous HTTP client speaking the 2025-06-18 client role."""

    def __init__(self, host: str, port: int, endpoint: str = "/mcp"):
        self.host = host
        self.port = port
        self.endpoint = endpoint
        self.session_id: str | None = None
        self.protocol: str | None = None

    def _headers(self, sid: str | None, extra: dict[str, str] | None = None) -> dict[str, str]:
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
        }
        if sid is not None:
            headers[SESSION_HEADER] = sid
        if self.protocol:
            headers[PROTOCOL_HEADER] = self.protocol
        for name, value in (extra or {}).items():
            headers[name] = value
        return headers

    def post(
        self,
        message: dict[str, Any],
        *,
        sid: str | None = None,
        accept: str = "application/json, text/event-stream",
        headers: dict[str, str] | None = None,
    ) -> tuple[int, dict[str, str], bytes]:
        use_sid = self.session_id if sid is None else sid
        connection = http.client.HTTPConnection(self.host, self.port, timeout=15)
        try:
            body = json.dumps(message, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
            request_headers = self._headers(use_sid, headers)
            if accept:
                request_headers["Accept"] = accept
            connection.request("POST", self.endpoint, body=body, headers=request_headers)
            response = connection.getresponse()
            data = response.read()
            lowered = {name.lower(): value for name, value in response.getheaders()}
            return response.status, lowered, data
        finally:
            connection.close()

    def delete(self) -> tuple[int, dict[str, str], bytes]:
        connection = http.client.HTTPConnection(self.host, self.port, timeout=15)
        try:
            connection.request("DELETE", self.endpoint, headers=self._headers(self.session_id))
            response = connection.getresponse()
            data = response.read()
            lowered = {name.lower(): value for name, value in response.getheaders()}
            return response.status, lowered, data
        finally:
            connection.close()

    def initialize(self) -> dict[str, Any]:
        status, headers, data = self.post(rpc_request("initialize", 1, {"protocolVersion": PROTOCOL}))
        if status != 200:
            raise AssertionError(f"initialize HTTP {status}: {data[:400]!r}")
        sid = headers.get(SESSION_HEADER.lower())
        self.session_id = sid
        self.protocol = headers.get(PROTOCOL_HEADER.lower())
        return json.loads(data.decode("utf-8"))


class SseReader(threading.Thread):
    """Reads one session GET SSE stream, pushing (event, payload) tuples."""

    def __init__(self, host: str, port: int, endpoint: str, sid: str, protocol: str):
        super().__init__(daemon=True)
        self.host = host
        self.port = port
        self.endpoint = endpoint
        self.sid = sid
        self.protocol = protocol
        self.events: queue.Queue = queue.Queue()
        self.status: int | None = None
        self.error: str | None = None

    def run(self) -> None:
        connection = http.client.HTTPConnection(self.host, self.port, timeout=30)
        try:
            headers = {
                "Accept": "text/event-stream",
                SESSION_HEADER: self.sid,
                PROTOCOL_HEADER: self.protocol,
            }
            connection.request("GET", self.endpoint, headers=headers)
            response = connection.getresponse()
            self.status = response.status
            buffer = b""
            while True:
                # read1() returns decoded bytes as each chunk arrives; read()
                # would buffer until 64 KiB or EOF and never return for an
                # open-ended SSE stream.
                chunk = response.read1(65536)
                if not chunk:
                    break
                buffer += chunk
                while b"\n\n" in buffer:
                    block, _, buffer = buffer.partition(b"\n\n")
                    self._parse_block(block)
        except OSError as exc:
            self.error = str(exc)
        finally:
            connection.close()

    def _parse_block(self, block: bytes) -> None:
        lines = block.split(b"\n")
        kind = "message"
        data_lines: list[bytes] = []
        for line in lines:
            if not line or line.startswith(b":"):
                continue
            if line.startswith(b"event:"):
                kind = line[len(b"event:"):].strip().decode("ascii", "replace")
            elif line.startswith(b"data:"):
                data_lines.append(line[len(b"data:"):].strip())
        if not data_lines:
            return
        text = b"\n".join(data_lines).decode("utf-8", "replace")
        try:
            payload = json.loads(text)
        except ValueError:
            payload = text
        self.events.put((kind, payload))


# --------------------------------------------------------------------------
# Scripted backends (written to a temporary directory per test class)
# --------------------------------------------------------------------------

SCRIPTED_BACKEND = r'''#!/usr/bin/env python3
"""Scripted stdio MCP backend used only by the facade tests."""
from __future__ import annotations
import json
import os
import sys

NAME = os.environ.get("FACADE_BACKEND_NAME", "scripted")
EXIT_AFTER_CALL = os.environ.get("FACADE_EXIT_AFTER_CALL") == "1"


def send(message: dict):
    print(json.dumps(message, separators=(",", ":")), flush=True)


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
    except ValueError:
        continue
    method = message.get("method")
    rid = message.get("id")
    if method == "notifications/initialized":
        continue
    if method == "initialize":
        send(response(rid, {
            "protocolVersion": "2025-06-18",
            "capabilities": {"tools": {"listChanged": False}},
            "serverInfo": {"name": NAME, "version": "1.0.0"},
            "instructions": "scripted backend",
        }))
        continue
    if method == "ping":
        send(response(rid, {}))
        continue
    if method == "tools/call":
        params = message.get("params") or {}
        tool = params.get("name")
        if tool == "push-notification":
            send({"jsonrpc": "2.0", "method": "notifications/progress",
                  "params": {"progress": 1, "total": 1}})
            send(response(rid, {
                "content": [{"type": "text", "text": "pushed"}],
                "structuredContent": {"ok": True, "servedBy": NAME},
            }))
        elif tool == "server-request":
            send({"jsonrpc": "2.0", "id": "sr-1", "method": "sampling/createMessage",
                  "params": {"messages": []}})
            answer = sys.stdin.readline()
            try:
                parsed = json.loads(answer)
            except ValueError:
                parsed = {"error": {"code": -32000, "message": "unparseable answer"}}
            send(response(rid, {
                "content": [{"type": "text", "text": json.dumps({
                    "answerId": parsed.get("id"),
                    "errorCode": (parsed.get("error") or {}).get("code"),
                })}],
                "structuredContent": {"answerId": parsed.get("id"),
                                      "errorCode": (parsed.get("error") or {}).get("code"),
                                      "servedBy": NAME},
            }))
        elif tool == "echo":
            value = (params.get("arguments") or {}).get("value")
            send(response(rid, {
                "content": [{"type": "text", "text": json.dumps(value)}],
                "structuredContent": {"value": value, "servedBy": NAME},
            }))
        else:
            send(response(rid, error={"code": -32602, "message": f"no such tool {tool}"}))
        if EXIT_AFTER_CALL and tool == "echo":
            break
        continue
    send(response(rid, error={"code": -32601, "message": f"method not found: {method}"}))
'''

EXIT_BACKEND = r'''#!/usr/bin/env python3
"""Exits right after answering one tools/call (transport-loss test)."""
from __future__ import annotations
import json
import sys

for raw in sys.stdin:
    raw = raw.strip()
    if not raw:
        continue
    message = json.loads(raw)
    rid = message.get("id")
    method = message.get("method")
    if method == "notifications/initialized":
        continue
    if method == "initialize":
        print(json.dumps({"jsonrpc": "2.0", "id": rid, "result": {
            "protocolVersion": "2025-06-18",
            "capabilities": {"tools": {"listChanged": False}},
            "serverInfo": {"name": "exit-backend", "version": "1.0.0"},
        }}, separators=(",", ":")), flush=True)
        continue
    if method == "tools/call":
        print(json.dumps({"jsonrpc": "2.0", "id": rid, "result": {
            "content": [{"type": "text", "text": "done"}],
            "structuredContent": {"ok": True},
        }}, separators=(",", ":")), flush=True)
        break
'''


def write_script(name: str, source: str, directory: Path) -> Path:
    path = directory / name
    path.write_text(source, encoding="utf-8")
    os.chmod(path, 0o755)
    return path


# --------------------------------------------------------------------------
# Shared server harness
# --------------------------------------------------------------------------

class _FacadeTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.tempdir = Path(self.temp.name)
        self.server: shf.FacadeServer | None = None
        self._threads: list[threading.Thread] = []

    def tearDown(self) -> None:
        for thread in self._threads:
            if thread.is_alive():
                try:
                    thread.join(timeout=1.0)
                except RuntimeError:
                    pass
        if self.server is not None:
            self.server.shutdown_all()
            try:
                self.server.shutdown()
            except OSError:
                pass
            self.server.server_close()
        self.temp.cleanup()

    def start_server(self, options: shf.FacadeOptions) -> shf.FacadeServer:
        server = shf.FacadeServer(options)
        _quiet_facade_handle_error(server)
        self.server = server
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self._threads.append(thread)
        return server

    @staticmethod
    def client(server: shf.FacadeServer) -> RpcClient:
        host, port = server.server_address[:2]
        return RpcClient(host, port, server.endpoint)

    def session_count(self, server: shf.FacadeServer) -> int:
        with server.lock:
            return len(server.sessions)

    def wait_until(self, predicate: Any, timeout: float = 5.0) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if predicate():
                return
            time.sleep(0.02)
        raise AssertionError("condition never became true")


# --------------------------------------------------------------------------
# Pure unit tests
# --------------------------------------------------------------------------

class PureFacadeTest(unittest.TestCase):
    def test_default_backend_command_is_local_connect_only(self) -> None:
        argv = shf.default_backend_command("wsl", "127.0.0.1", 8769, "my-server")
        self.assertEqual(argv[0], sys.executable)
        self.assertTrue(argv[1].endswith(os.path.join("wsl-bridge-mcp", "bridge.py")))
        self.assertEqual(argv[2:], ["connect", "--local-host", "127.0.0.1",
                                    "--local-port", "8769", "my-server"])
        self.assertNotIn("peer", argv)
        self.assertTrue(all(isinstance(item, str) for item in argv))

    def test_loopback_refusals(self) -> None:
        for bad in ("0.0.0.0", "192.168.1.10", "example.com"):
            with self.assertRaises(shf.FacadeError):
                shf._require_loopback(bad, "listen")
        shf._require_loopback("127.0.0.1", "listen")
        shf._require_loopback("localhost", "listen")

    def test_classify_and_key(self) -> None:
        self.assertEqual(shf._classify({"jsonrpc": "2.0", "method": "a", "id": 1}), "request")
        self.assertEqual(shf._classify({"jsonrpc": "2.0", "method": "a"}), "notification")
        self.assertEqual(shf._classify({"jsonrpc": "2.0", "id": 1, "result": {}}), "response")
        self.assertEqual(shf._classify({"jsonrpc": "2.0", "id": "x"}), "request")
        self.assertEqual(shf._canonical_key(1), shf._canonical_key(1))
        self.assertNotEqual(shf._canonical_key(1), shf._canonical_key("1"))

    def test_options_validation(self) -> None:
        with self.assertRaises(shf.FacadeError):
            shf.FacadeOptions(side="mac", target="x")
        with self.assertRaises(shf.FacadeError):
            shf.FacadeOptions(side="wsl", target="")
        with self.assertRaises(shf.FacadeError):
            shf.FacadeOptions(side="wsl", target="x", max_sessions=0)
        with self.assertRaises(shf.FacadeError):
            shf.FacadeOptions(side="wsl", target="x", listen_host="0.0.0.0")
        options = shf.FacadeOptions(side="wsl", target="x", node_port=9999)
        self.assertEqual(options.node_port, 9999)
        self.assertIsNone(options.backend_command)


# --------------------------------------------------------------------------
# HTTP behaviour against fixture_mcp.py
# --------------------------------------------------------------------------

class FacadeRoundTripTest(_FacadeTestCase):
    def _options(self, **overrides: Any) -> shf.FacadeOptions:
        base: dict[str, Any] = dict(
            side="wsl", target="fixture-mcp",
            backend_command=[sys.executable, str(FIXTURE_MCP)],
        )
        base.update(overrides)
        return shf.FacadeOptions(**base)

    def test_initialize_list_and_call_tool(self) -> None:
        server = self.start_server(self._options())
        client = self.client(server)
        initialize = client.initialize()
        self.assertEqual(initialize["result"]["protocolVersion"], PROTOCOL)
        self.assertEqual(initialize["result"]["serverInfo"]["name"], "fixture-mcp")
        self.assertIsNotNone(client.session_id)
        self.assertEqual(client.protocol, PROTOCOL)
        self.assertTrue(client.session_id)
        # tools/list
        status, _headers, data = client.post(rpc_request("tools/list", 2))
        self.assertEqual(status, 200)
        names = [tool["name"] for tool in json.loads(data)["result"]["tools"]]
        self.assertIn("echo", names)
        # notifications/initialized is a notification -> HTTP 202
        status, _headers, _data = client.post(rpc_request("notifications/initialized"))
        self.assertEqual(status, 202)
        # tools/call echo round trip
        status, _headers, data = client.post(
            rpc_request("tools/call", 3, {"name": "echo", "arguments": {"value": "hi"}})
        )
        self.assertEqual(status, 200)
        body = json.loads(data.decode("utf-8"))
        self.assertEqual(body["id"], 3)
        self.assertEqual(body["result"]["structuredContent"]["value"], "hi")
        # DELETE tears the session down
        status, _headers, _data = client.delete()
        self.assertEqual(status, 202)
        self.assertEqual(self.session_count(server), 0)
        # stale session id is now unknown
        status, _headers, _data = client.post(rpc_request("ping", 4))
        self.assertEqual(status, 404)

    def test_session_header_enforcement_and_duplicate_initialize(self) -> None:
        server = self.start_server(self._options())
        client = self.client(server)
        client.initialize()
        # missing session id header -> 400 JSON-RPC error (fresh, session-less client)
        anonymous = RpcClient(*server.server_address[:2], server.endpoint)
        status, _headers, data = anonymous.post(rpc_request("ping", 7))
        self.assertEqual(status, 400)
        self.assertEqual(json.loads(data)["error"]["code"], -32600)
        # unknown session id -> 404
        status, _headers, _data = client.post(rpc_request("ping", 8), sid="does-not-exist")
        self.assertEqual(status, 404)
        # duplicate initialize within the same session -> JSON-RPC error
        status, _headers, data = client.post(rpc_request("initialize", 9, {"protocolVersion": PROTOCOL}))
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(data)["error"]["code"], -32600)

    def test_one_isolated_backend_per_session(self) -> None:
        server = self.start_server(self._options())
        first = self.client(server)
        second = self.client(server)
        first.initialize()
        second.initialize()
        self.assertNotEqual(first.session_id, second.session_id)
        with server.lock:
            sessions = list(server.sessions.values())
        self.assertEqual(len(sessions), 2)
        pids = {session.backend.process.pid for session in sessions}
        self.assertEqual(len(pids), 2)
        # First session keeps working while the second is torn down.
        status, _headers, data = first.post(rpc_request("tools/call", 11, {"name": "echo", "arguments": {"value": "a"}}))
        self.assertEqual(status, 200)
        second.delete()
        self.assertEqual(self.session_count(server), 1)
        status, _headers, data = first.post(rpc_request("tools/call", 12, {"name": "echo", "arguments": {"value": "b"}}))
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(data)["result"]["structuredContent"]["value"], "b")

    def test_oversize_body_is_rejected(self) -> None:
        server = self.start_server(self._options(request_body_bytes=2048))
        client = self.client(server)
        client.initialize()
        connection = http.client.HTTPConnection(*server.server_address[:2], timeout=10)
        try:
            payload = json.dumps({"jsonrpc": "2.0", "method": "tools/call", "id": 1,
                                  "params": {"x": "y" * 3000}}).encode("utf-8")
            connection.request("POST", server.endpoint, body=payload,
                               headers={"Content-Type": "application/json",
                                        SESSION_HEADER: client.session_id or ""})
            response = connection.getresponse()
            self.assertEqual(response.status, 413)
        finally:
            connection.close()

    def test_capacity_reached_returns_429(self) -> None:
        server = self.start_server(self._options(max_sessions=1))
        first = self.client(server)
        first.initialize()
        second = self.client(server)
        status, _headers, data = second.post(rpc_request("initialize", 1, {"protocolVersion": PROTOCOL}))
        self.assertEqual(status, 429)
        self.assertIn("capacity", json.loads(data)["error"]["message"])


class FacadeScriptedBackendTest(_FacadeTestCase):
    def _options(self, script: Path | None = None, **overrides: Any) -> shf.FacadeOptions:
        backend = script or write_script("scripted_backend.py", SCRIPTED_BACKEND, self.tempdir)
        base: dict[str, Any] = dict(
            side="wsl",
            target="scripted",
            backend_command=[sys.executable, str(backend)],
        )
        base.update(overrides)
        return shf.FacadeOptions(**base)

    def test_backend_notification_relayed_over_get_sse(self) -> None:
        server = self.start_server(self._options())
        client = self.client(server)
        client.initialize()
        sid = client.session_id or ""
        reader = SseReader(*server.server_address[:2], server.endpoint, sid, PROTOCOL)
        reader.start()
        self._threads.append(reader)

        def sse_ready() -> bool:
            session = server.get_session(sid)
            return bool(session and session.channels and reader.status == 200)

        self.wait_until(sse_ready)
        status, _headers, data = client.post(
            rpc_request("tools/call", 21, {"name": "push-notification", "arguments": {}})
        )
        self.assertEqual(status, 200)
        self.assertTrue(json.loads(data)["result"]["structuredContent"]["ok"])
        kind, payload = wait_for(
            reader.events,
            lambda item: isinstance(item[1], dict)
            and item[1].get("method") == "notifications/progress",
        )
        self.assertEqual(kind, "message")
        self.assertEqual(payload["params"]["progress"], 1)
        client.delete()

    def test_backend_server_request_without_sse_is_refused(self) -> None:
        server = self.start_server(self._options())
        client = self.client(server)
        client.initialize()
        # No GET SSE stream is open: the backend's server request must be
        # refused by the facade so the backend unblocks and answers the call.
        status, _headers, data = client.post(
            rpc_request("tools/call", 31, {"name": "server-request", "arguments": {}})
        )
        self.assertEqual(status, 200)
        result = json.loads(data)["result"]["structuredContent"]
        self.assertEqual(result["answerId"], "sr-1")
        self.assertEqual(result["errorCode"], shf.ERR_NOT_FOUND)

    def test_sse_only_post_receives_streamed_response(self) -> None:
        server = self.start_server(self._options())
        client = self.client(server)
        client.initialize()
        status, headers, data = client.post(
            rpc_request("ping", 41), accept="text/event-stream"
        )
        self.assertEqual(status, 200)
        self.assertEqual(headers.get("content-type"), "text/event-stream")
        text = data.decode("utf-8")
        self.assertIn("event: message", text)
        self.assertIn('"result"', text)
        self.assertIn('"id":41', text)
        # every SSE block ends with a blank line
        self.assertIn("\n\n", text)

    def test_backend_exit_surfaces_transport_error(self) -> None:
        script = write_script("exit_backend.py", EXIT_BACKEND, self.tempdir)
        server = self.start_server(shf.FacadeOptions(
            side="wsl", target="exit", backend_command=[sys.executable, str(script)],
        ))
        client = self.client(server)
        client.initialize()
        session = server.get_session(client.session_id)
        backend_process = session.backend.process
        self.assertIsNotNone(backend_process)
        status, _headers, _data = client.post(
            rpc_request("tools/call", 51, {"name": "echo", "arguments": {"value": "x"}})
        )
        self.assertEqual(status, 200)
        status, _headers, data = client.post(rpc_request("ping", 52))
        self.assertEqual(status, 200)
        body = json.loads(data)
        self.assertEqual(body["error"]["code"], shf.ERR_TRANSPORT)
        # EOF and process-poll detection can win in either order. Both must
        # return the same transport-error class, never a successful reply or
        # an implicit replacement backend.
        self.assertRegex(body["error"]["message"], r"exited|is not running")
        self.assertNotIn("result", body)
        backend_process.wait(timeout=3)
        self.assertIsNotNone(backend_process.returncode)
        self.assertIn(session.backend.process, (None, backend_process))

    def test_bad_json_body_returns_parse_error(self) -> None:
        server = self.start_server(self._options())
        client = self.client(server)
        client.initialize()
        connection = http.client.HTTPConnection(*server.server_address[:2], timeout=10)
        try:
            connection.request("POST", server.endpoint, body=b"{not json",
                               headers={"Content-Type": "application/json",
                                        SESSION_HEADER: client.session_id or ""})
            response = connection.getresponse()
            data = response.read()
            self.assertEqual(response.status, 400)
            self.assertEqual(json.loads(data)["error"]["code"], -32700)
        finally:
            connection.close()


class FacadeCliTest(unittest.TestCase):
    def test_cli_subprocess_serves_http(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            port = free_port()
            process = subprocess.Popen(
                [
                    sys.executable, str(FACADE), "--side", "wsl", "--target", "cli-fixture",
                    "--node-port", "9999", "--listen-port", str(port),
                    "--backend-command", sys.executable, str(FIXTURE_MCP),
                    "--session-idle-s", "60",
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=str(ROOT),
            )
            try:
                client = RpcClient("127.0.0.1", port)
                deadline = time.monotonic() + 10
                last_error: Exception | None = None
                while time.monotonic() < deadline:
                    try:
                        initialize = client.initialize()
                        last_error = None
                        break
                    except (OSError, http.client.HTTPException) as exc:
                        last_error = exc
                        time.sleep(0.1)
                if last_error is not None:
                    self.fail(f"CLI facade never became reachable: {last_error}")
                self.assertEqual(initialize["result"]["serverInfo"]["name"], "fixture-mcp")
                self.assertIsNotNone(client.session_id)
                client.delete()
            finally:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
                for pipe in (process.stdout, process.stderr):
                    if pipe is not None:
                        try:
                            pipe.close()
                        except OSError:
                            pass


if __name__ == "__main__":
    unittest.main(verbosity=2)
