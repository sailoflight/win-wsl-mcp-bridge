"""Resilience tests for the constant two-tool compatibility MCP facade.

Scope: ``bridge_runtime._CompatibilitySession`` + ``compatibility_mcp`` only.
These tests exercise the *real* facade over a real stdio subprocess (no mock of
the session or the handler), talking to an in-process scripted "bridge node"
that plays both the local control surface (the ``connect`` ack) and the raw
downstream business MCP (initialize / tools/list / tools/call / list_changed)
over one socket per connection.  Each accepted connection is one downstream
session generation, which lets the tests script outages, refused reconnects,
mid-refresh drops, list_changed notifications, and call accounting exactly the
way a transient backend outage would behave on a real deployment.

Behavioural contract under test (all inside a single facade process):

* startup with the peer/backend unavailable still yields a valid constant
  two-tool initialize + tools/list, with no invented downstream identity;
* a failed reconnect never discards the last-known-good downstream
  initialize result or catalog (instructions survive outages);
* every bridge_capabilities result carries explicit cache evidence
  (observed / revision / verified / reason) so clients can see staleness;
* ``refresh=true`` preserves the previous full catalog on failure and swaps a
  complete bounded catalog atomically on success (no partial page mixes);
* a failed downstream business call is never replayed, and
  ``outcomeUnknown`` is True only when a business tools/call was written to
  the downstream before the transport loss;
* a downstream ``notifications/tools/list_changed`` marks the cached catalog
  stale (``verified: False``) until a refresh re-observes it.

A second class adds a real two-node ``connect-http`` end-to-end route (WSL
node -> peer link -> WIN node -> streamable_http_stdio adapter subprocess ->
in-process streamable-http MCP fixture) to prove the HTTP compatibility path
end to end.  Both classes stay stdlib-only; helper imports from the existing
test modules happen inside method bodies so unittest discovery never
double-collects their test classes.
"""

import json
import os
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WSL_SCRIPT = ROOT / "wsl-bridge-mcp" / "bridge.py"
WIN_SCRIPT = ROOT / "win-bridge-mcp" / "bridge.py"

SHARED_PROTOCOL = "2025-06-18"
COMPAT_NOTE = (
    "\nCompatibility mode exposes a stable two-tool surface. Discover once with "
    "bridge_capabilities, then invoke with bridge_call."
)
SURFACE_TOOLS = ["bridge_capabilities", "bridge_call"]


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def tool(name: str, description: str = "fixture tool") -> dict:
    return {
        "name": name,
        "description": description,
        "inputSchema": {"type": "object", "properties": {}},
    }


def mcp_request(request_id, method: str, params: dict | None = None) -> dict:
    message = {"jsonrpc": "2.0", "id": request_id, "method": method}
    if params is not None:
        message["params"] = params
    return message


def capabilities_request(request_id: int, **arguments) -> dict:
    return mcp_request(
        request_id, "tools/call", {"name": "bridge_capabilities", "arguments": arguments}
    )


def bridge_call_request(request_id: int, downstream_tool: str, arguments: dict) -> dict:
    return mcp_request(
        request_id,
        "tools/call",
        {
            "name": "bridge_call",
            "arguments": {"tool": downstream_tool, "arguments": arguments},
        },
    )


def tool_result_value(response: dict) -> dict:
    result = response.get("result")
    if not isinstance(result, dict) or not isinstance(result.get("structuredContent"), dict):
        raise AssertionError(f"no structured tool result: {response!r}")
    return result["structuredContent"]


class FakeBridgeNode:
    """One loopback TCP server that plays a bridge node + business MCP.

    One accepted connection = one downstream session/generation: the facade
    first sends its local ``connect`` op (answered with ok on the same socket)
    and then raw newline-delimited MCP JSON-RPC, exactly like the real bridge
    node relays.  Behaviours (catalog, instructions, drops, list_changed) are
    plain attributes the test toggles between exchanges; session accounting is
    appended under a lock so the test can assert call counts later.
    """

    def __init__(
        self,
        catalog: list | None = None,
        instructions: str = "fixture-backend-instructions",
        server_name: str = "fixture-backend",
        page_size: int | None = None,
        port: int | None = None,
    ) -> None:
        self.catalog = list(catalog) if catalog is not None else []
        self.instructions = instructions
        self.server_name = server_name
        self.page_size = page_size
        self.port = port
        # Behaviour knobs (toggled by the test between exchanges).
        self.accept_connections = True
        self.drop_on_call = False
        self.drop_next_tools_list = False
        self.push_list_changed = False
        self._listener: socket.socket | None = None
        self._thread: threading.Thread | None = None
        self._sessions: list[dict] = []
        self._lock = threading.Lock()

    # -- lifecycle ---------------------------------------------------------
    def start(self) -> int:
        if self._listener is not None:
            return int(self.port)
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", self.port if self.port is not None else 0))
        # A short timeout lets the accept loop notice stop() instead of
        # parking forever on a closed listener.
        listener.settimeout(1.0)
        listener.listen(16)
        self._listener = listener
        self.port = int(listener.getsockname()[1])
        self._thread = threading.Thread(target=self._accept_loop, daemon=True)
        self._thread.start()
        return int(self.port)

    def stop(self) -> None:
        listener, self._listener = self._listener, None
        if listener is not None:
            try:
                listener.close()
            except OSError:
                pass
        self.accept_connections = False
        self.drop_all_sessions()
        if self._thread is not None:
            self._thread.join(timeout=5)

    def drop_all_sessions(self) -> None:
        with self._lock:
            sessions = list(self._sessions)
        for record in sessions:
            self._close_record(record)

    @staticmethod
    def _close_record(record: dict) -> None:
        connection = record.get("connection")
        if connection is not None:
            try:
                # shutdown() reliably interrupts the server thread blocked in
                # readline on this socket; close() alone can leave the parked
                # read alive on Linux.
                connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                connection.close()
            except OSError:
                pass

    # -- accounting --------------------------------------------------------
    @property
    def session_count(self) -> int:
        with self._lock:
            return len(self._sessions)

    @property
    def calls(self) -> list[tuple[str, dict]]:
        with self._lock:
            return [item for record in self._sessions for item in record["calls"]]

    def call_count(self) -> int:
        return len(self.calls)

    def session_messages(self, session_index: int) -> list[str]:
        with self._lock:
            if session_index >= len(self._sessions):
                return []
            return list(self._sessions[session_index]["messages"])

    def session_calls(self, session_index: int) -> list[tuple[str, dict]]:
        with self._lock:
            if session_index >= len(self._sessions):
                return []
            return list(self._sessions[session_index]["calls"])

    def session_initialize_seen(self, session_index: int) -> int:
        with self._lock:
            if session_index >= len(self._sessions):
                return 0
            return self._sessions[session_index].get("initialize_seen", 0)

    # -- serving -----------------------------------------------------------
    def _accept_loop(self) -> None:
        while self._listener is not None:
            listener = self._listener
            try:
                connection, _address = listener.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            if not self.accept_connections:
                try:
                    connection.close()
                except OSError:
                    pass
                continue
            threading.Thread(
                target=self._serve, args=(connection,), daemon=True
            ).start()

    def _page(self, cursor: str | None) -> tuple[list, str | None]:
        tools = self.catalog
        if self.page_size is None:
            return list(tools), None
        index = int(cursor) if cursor is not None else 0
        page = tools[index : index + self.page_size]
        next_cursor = str(index + len(page)) if index + len(page) < len(tools) else None
        return page, next_cursor

    def _respond(self, connection: socket.socket, message: dict) -> None:
        payload = json.dumps(message, separators=(",", ":")) + "\n"
        connection.sendall(payload.encode("utf-8"))

    def _serve(self, connection: socket.socket) -> None:
        with self._lock:
            record = {
                "connection": connection,
                "messages": [],
                "calls": [],
                "initialize_seen": 0,
            }
            self._sessions.append(record)
        reader = None
        try:
            connection.settimeout(60)
            reader = connection.makefile("rb")
            for raw in iter(reader.readline, b""):
                if not raw:
                    break
                message = json.loads(raw.decode("utf-8"))
                if message.get("op") == "connect":
                    self._respond(connection, {"ok": True})
                    continue
                if not isinstance(message.get("method"), str):
                    continue
                method = message["method"]
                with self._lock:
                    record["messages"].append(method)
                if method == "notifications/initialized":
                    continue
                request_id = message.get("id")
                if method == "initialize":
                    with self._lock:
                        record["initialize_seen"] += 1
                    result = {
                        "protocolVersion": SHARED_PROTOCOL,
                        "capabilities": {"tools": {"listChanged": False}},
                        "serverInfo": {
                            "name": self.server_name,
                            "version": "1.0.0",
                        },
                        "instructions": self.instructions,
                    }
                    self._respond(
                        connection, {"jsonrpc": "2.0", "id": request_id, "result": result}
                    )
                    continue
                if method == "tools/list":
                    if self.drop_next_tools_list:
                        self.drop_next_tools_list = False
                        break  # close without responding: mid-refresh loss
                    page, next_cursor = self._page((message.get("params") or {}).get("cursor"))
                    payload: dict = {"tools": page}
                    if next_cursor is not None:
                        payload["nextCursor"] = next_cursor
                    self._respond(
                        connection, {"jsonrpc": "2.0", "id": request_id, "result": payload}
                    )
                    continue
                if method == "tools/call":
                    params = message.get("params") or {}
                    name = params.get("name")
                    arguments = params.get("arguments") or {}
                    if not isinstance(arguments, dict):
                        arguments = {}
                    with self._lock:
                        record["calls"].append((str(name), dict(arguments)))
                    if self.drop_on_call:
                        break  # backend accepted the call then vanished
                    if self.push_list_changed:
                        self.push_list_changed = False
                        self._respond(
                            connection,
                            {"jsonrpc": "2.0", "method": "notifications/tools/list_changed"},
                        )
                    text = "called:%s:%s" % (name, json.dumps(arguments, sort_keys=True))
                    result = {
                        "content": [{"type": "text", "text": text}],
                        "structuredContent": {"called": name, "arguments": arguments},
                    }
                    self._respond(
                        connection, {"jsonrpc": "2.0", "id": request_id, "result": result}
                    )
                    continue
                self._respond(
                    connection,
                    {
                        "jsonrpc": "2.0",
                        "id": request_id,
                        "error": {"code": -32601, "message": f"method not found: {method}"},
                    },
                )
        except (OSError, ValueError, socket.timeout):
            pass
        finally:
            if reader is not None:
                try:
                    reader.close()
                except OSError:
                    pass
            self._close_record(record)


class FacadeProcess:
    """One real ``compatibility-mcp`` facade subprocess over stdio pipes."""

    def __init__(self, node_port: int, target: str = "sample"):
        environment = os.environ.copy()
        environment["PYTHONDONTWRITEBYTECODE"] = "1"
        self.proc = subprocess.Popen(
            [
                sys.executable,
                str(WSL_SCRIPT),
                "compatibility-mcp",
                target,
                "--local-port",
                str(node_port),
            ],
            cwd=ROOT,
            env=environment,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

    def exchange(self, message: dict) -> dict:
        if self.proc.poll() is not None:
            raise AssertionError(f"facade exited early rc={self.proc.poll()}")
        assert self.proc.stdin is not None and self.proc.stdout is not None
        self.proc.stdin.write(json.dumps(message, separators=(",", ":")) + "\n")
        self.proc.stdin.flush()
        line = self.proc.stdout.readline()
        if not line:
            raise AssertionError(
                "facade closed stdout without answering (stderr: %r)"
                % (self.stderr_tail(),)
            )
        return json.loads(line)

    def stderr_tail(self) -> str:
        if self.proc.stderr is None:
            return ""
        try:
            self.proc.stderr.flush()
        except OSError:
            pass
        return ""

    def close(self) -> None:
        if self.proc.poll() is None:
            try:
                if self.proc.stdin is not None:
                    self.proc.stdin.close()
            except OSError:
                pass
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                try:
                    self.proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    pass
        for stream in (self.proc.stdin, self.proc.stdout, self.proc.stderr):
            if stream is not None:
                try:
                    stream.close()
                except OSError:
                    pass


class CompatibilityFacadeResilienceTest(unittest.TestCase):
    """Whole-loop resilience contract of the constant two-tool facade."""

    def setUp(self) -> None:
        self._facades: list[FacadeProcess] = []
        self._nodes: list[FakeBridgeNode] = []

    def tearDown(self) -> None:
        for facade in self._facades:
            facade.close()
        for node in self._nodes:
            node.stop()

    # -- helpers -----------------------------------------------------------
    def start_node(self, **kwargs) -> FakeBridgeNode:
        node = FakeBridgeNode(**kwargs)
        self._nodes.append(node)
        return node

    def start_facade(self, node_port: int) -> FacadeProcess:
        facade = FacadeProcess(node_port)
        self._facades.append(facade)
        return facade

    def initialize(self, facade: FacadeProcess, rid: int = 1) -> dict:
        response = facade.exchange(
            mcp_request(rid, "initialize", {"protocolVersion": SHARED_PROTOCOL})
        )
        self.assertEqual(response.get("id"), rid)
        return response["result"]

    def assert_initialize_shape(self, result: dict, *, target: str = "sample") -> None:
        self.assertEqual(result["protocolVersion"], SHARED_PROTOCOL)
        self.assertEqual(result["capabilities"], {"tools": {"listChanged": False}})
        self.assertEqual(result["serverInfo"]["name"], f"{target}-bridge-compatibility")
        self.assertIsInstance(result["serverInfo"]["version"], str)
        self.assertTrue(result["serverInfo"]["version"])
        self.assertEqual(
            set(result), {"protocolVersion", "capabilities", "serverInfo", "instructions"}
        )

    # -- tests -------------------------------------------------------------
    def test_startup_backend_unavailable_then_recovers(self) -> None:
        # The peer/backend is not listening at all when the facade starts.
        port = free_port()
        node = self.start_node(
            catalog=[tool("echo")],
            instructions="fixture-instructions-v1",
            server_name="backend-one",
            port=port,
        )
        facade = self.start_facade(port)

        # Startup initialize must still succeed with the constant surface and
        # no invented downstream identity (nothing observed yet).
        result = self.initialize(facade, 1)
        self.assert_initialize_shape(result)
        self.assertEqual(result["instructions"], COMPAT_NOTE)

        # tools/list is the constant two-tool surface even while unavailable.
        listed = facade.exchange(mcp_request(2, "tools/list", {}))
        names = [item["name"] for item in listed["result"]["tools"]]
        self.assertEqual(names, SURFACE_TOOLS)

        # A discovery attempt reports a retryable, known-outcome error.
        response = facade.exchange(capabilities_request(3, tool="echo"))
        content = tool_result_value(response)
        self.assertTrue(response["result"].get("isError"))
        self.assertEqual(content["code"], "backend_unavailable")
        self.assertTrue(content["retryable"])
        self.assertFalse(content["outcomeUnknown"])
        self.assertNotIn("tools", content)

        # Backend appears on the same port: full discovery now works, rev 1.
        self.assertEqual(node.start(), port)
        response = facade.exchange(capabilities_request(4, tool="echo"))
        content = tool_result_value(response)
        self.assertEqual([item["name"] for item in content["tools"]], ["echo"])
        self.assertEqual(
            content["cache"],
            {"observed": True, "revision": 1, "verified": True, "reason": "live"},
        )

        # A later upstream initialize stays on the live session: instructions
        # come from the downstream but the identity stays the compat facade.
        result = self.initialize(facade, 5)
        self.assert_initialize_shape(result)
        self.assertEqual(result["instructions"], "fixture-instructions-v1" + COMPAT_NOTE)
        self.assertEqual(node.session_count, 1)
        self.assertEqual(node.session_initialize_seen(0), 1)

    def test_last_known_good_survives_outage_and_refresh_recovers(self) -> None:
        port = free_port()
        node = self.start_node(
            catalog=[tool("echo")],
            instructions="instruction-one",
            server_name="backend-one",
            port=port,
        )
        node.start()
        facade = self.start_facade(port)

        self.assertIn("instruction-one", self.initialize(facade, 1)["instructions"])
        content = tool_result_value(facade.exchange(capabilities_request(2, tool="echo")))
        self.assertEqual(content["cache"]["reason"], "live")

        # Outage: node stops; facade discovers it on the next refresh attempt.
        node.stop()
        response = facade.exchange(capabilities_request(3, tool="echo", refresh=True))
        content = tool_result_value(response)
        self.assertTrue(response["result"].get("isError"))
        self.assertEqual(content["code"], "backend_unavailable")
        self.assertFalse(content["outcomeUnknown"])

        # Cache still answers with explicit stale evidence (sock now dropped).
        content = tool_result_value(facade.exchange(capabilities_request(4, tool="echo")))
        self.assertEqual([item["name"] for item in content["tools"]], ["echo"])
        self.assertEqual(
            content["cache"],
            {"observed": True, "revision": 1, "verified": False, "reason": "downstream_unavailable"},
        )

        # Re-initialize while down: LKG instructions survive (never wiped).
        result = self.initialize(facade, 5)
        self.assert_initialize_shape(result)
        self.assertIn("instruction-one", result["instructions"])

        # Backend returns with a different catalog/instructions generation.
        node2 = self.start_node(
            catalog=[tool("echo"), tool("alpha")],
            instructions="instruction-two",
            server_name="backend-two",
            port=port,
        )
        node2.start()
        # Fresh downstream initialize happens on reconnect; LKG instructions
        # are replaced, but the old catalog is not yet re-observed.
        result = self.initialize(facade, 6)
        self.assertIn("instruction-two", result["instructions"])
        content = tool_result_value(facade.exchange(capabilities_request(7, tool="echo")))
        self.assertEqual(content["cache"]["reason"], "reconnected")
        self.assertEqual(content["cache"]["revision"], 1)
        self.assertFalse(content["cache"]["verified"])

        # refresh reconnects and atomically installs the full new catalog.
        content = tool_result_value(facade.exchange(capabilities_request(8, refresh=True)))
        self.assertEqual(
            [item["name"] for item in content["tools"]], ["echo", "alpha"]
        )
        self.assertEqual(
            content["cache"],
            {"observed": True, "revision": 2, "verified": True, "reason": "live"},
        )
        # Two downstream sessions total across the whole process life.
        self.assertEqual(node2.session_count + node.session_count, 2)

    def test_refresh_failure_keeps_old_catalog_and_swap_is_atomic(self) -> None:
        port = free_port()
        node = self.start_node(catalog=[tool("echo")], port=port)
        node.start()
        facade = self.start_facade(port)
        self.initialize(facade, 1)
        content = tool_result_value(facade.exchange(capabilities_request(2, tool="echo")))
        self.assertEqual(content["cache"]["revision"], 1)

        node.stop()
        # Failed refresh -> error, known outcome, old cache preserved.
        response = facade.exchange(capabilities_request(3, refresh=True))
        content = tool_result_value(response)
        self.assertTrue(response["result"].get("isError"))
        self.assertEqual(content["code"], "backend_unavailable")
        self.assertFalse(content["outcomeUnknown"])

        # New generation has four tools paged two at a time, and it will drop
        # the socket right after serving the first page of a refresh.
        node2 = self.start_node(
            catalog=[tool("a"), tool("b"), tool("c"), tool("d")],
            page_size=2,
            port=port,
        )
        node2.start()
        node2.drop_next_tools_list = True
        response = facade.exchange(capabilities_request(4, refresh=True))
        content = tool_result_value(response)
        self.assertTrue(response["result"].get("isError"))
        self.assertEqual(content["code"], "backend_unavailable")
        self.assertFalse(content["outcomeUnknown"])

        # The interrupted refresh must NOT leave a partial page behind.
        content = tool_result_value(facade.exchange(capabilities_request(5, tool="echo")))
        self.assertEqual([item["name"] for item in content["tools"]], ["echo"])
        self.assertEqual(content["cache"]["revision"], 1)
        self.assertEqual(content["cache"]["reason"], "downstream_unavailable")

        # A later full refresh installs the complete four-tool catalog as one
        # atomic swap across both pages.
        content = tool_result_value(facade.exchange(capabilities_request(6, refresh=True)))
        self.assertEqual(
            [item["name"] for item in content["tools"]], ["a", "b", "c", "d"]
        )
        self.assertEqual(content["cache"]["revision"], 2)
        self.assertEqual(content["cache"]["verified"], True)
        self.assertEqual(content["cache"]["reason"], "live")

    def test_business_call_failure_is_never_replayed(self) -> None:
        node = self.start_node(catalog=[tool("echo")])
        node.start()
        facade = self.start_facade(node.port)
        self.initialize(facade, 1)
        content = tool_result_value(facade.exchange(capabilities_request(2, tool="echo")))
        self.assertEqual(content["cache"]["revision"], 1)

        # The backend accepts the business call and then vanishes.
        node.drop_on_call = True
        response = facade.exchange(bridge_call_request(3, "echo", {"text": "once"}))
        content = tool_result_value(response)
        self.assertTrue(response["result"].get("isError"))
        self.assertEqual(content["code"], "backend_unavailable")
        self.assertTrue(content["retryable"])
        self.assertTrue(content["outcomeUnknown"])  # call was delivered, outcome lost

        # No automatic replay: exactly one call reached the first generation.
        time.sleep(0.4)
        self.assertEqual(node.call_count(), 1)
        self.assertEqual(node.session_calls(0), [("echo", {"text": "once"})])

        # A later explicit retry runs on a fresh downstream generation and is
        # exactly the retried request -- the old one is never replayed.
        node.drop_on_call = False
        response = facade.exchange(bridge_call_request(4, "echo", {"text": "twice"}))
        self.assertFalse(response["result"].get("isError"))
        content = tool_result_value(response)
        self.assertEqual(content["called"], "echo")
        self.assertEqual(content["arguments"], {"text": "twice"})
        self.assertEqual(node.session_count, 2)
        self.assertEqual(node.session_calls(1), [("echo", {"text": "twice"})])
        self.assertEqual(node.session_initialize_seen(1), 1)
        # Session 2 message order: downstream initialize handshake (initialize
        # request + initialized notification), then exactly one business call.
        self.assertEqual(
            node.session_messages(1),
            ["initialize", "notifications/initialized", "tools/call"],
        )

    def test_list_changed_marks_catalog_stale_until_refresh(self) -> None:
        node = self.start_node(catalog=[tool("echo")])
        node.start()
        facade = self.start_facade(node.port)
        self.initialize(facade, 1)
        content = tool_result_value(facade.exchange(capabilities_request(2, tool="echo")))
        self.assertEqual(content["cache"], {"observed": True, "revision": 1, "verified": True, "reason": "live"})

        # The backend pushes list_changed right before answering the next call.
        node.push_list_changed = True
        response = facade.exchange(bridge_call_request(3, "echo", {"text": "x"}))
        self.assertFalse(response["result"].get("isError"))

        # Catalog pages still answer but are explicitly flagged stale.
        content = tool_result_value(facade.exchange(capabilities_request(4, tool="echo")))
        self.assertEqual([item["name"] for item in content["tools"]], ["echo"])
        self.assertEqual(
            content["cache"],
            {"observed": True, "revision": 1, "verified": False, "reason": "list_changed"},
        )

        # A refresh re-observes and clears the stale flag, bumping the revision.
        content = tool_result_value(facade.exchange(capabilities_request(5, refresh=True)))
        self.assertEqual(content["cache"]["revision"], 2)
        self.assertEqual(content["cache"]["verified"], True)
        self.assertEqual(content["cache"]["reason"], "live")

    def test_unknown_tool_discovery_answers_from_cache_with_evidence(self) -> None:
        node = self.start_node(catalog=[tool("echo"), tool("alpha")])
        node.start()
        facade = self.start_facade(node.port)
        self.initialize(facade, 1)
        tool_result_value(facade.exchange(capabilities_request(2, tool="echo")))

        # Unknown exact name: empty match list, not an error, plus evidence.
        content = tool_result_value(facade.exchange(capabilities_request(3, tool="missing")))
        self.assertEqual(content["tools"], [])
        self.assertEqual(
            content["cache"],
            {"observed": True, "revision": 1, "verified": True, "reason": "live"},
        )

        # Summaries list all names with the same evidence attached.
        content = tool_result_value(facade.exchange(capabilities_request(4)))
        self.assertEqual([item["name"] for item in content["tools"]], ["echo", "alpha"])
        self.assertEqual(content["total"], 2)
        self.assertEqual(content["cache"]["observed"], True)


class HttpConnectRouteE2ETest(unittest.TestCase):
    """Real connect-http route across a WIN/WSL node pair.

    WSL node -> peer link -> WIN node -> streamable_http_stdio adapter
    subprocess -> in-process streamable-http MCP fixture.  The raw MCP bytes
    that come back (fixture serverInfo / instructions / tools / echo) prove
    the entire HTTP compatibility path end to end.
    """

    @classmethod
    def setUpClass(cls) -> None:
        # Imported here (not at module level) so discovery never collects the
        # fixture module's own unittest classes twice.
        from tests.test_streamable_http_stdio import FixtureConfig, FixtureServer

        cls._temp = tempfile.TemporaryDirectory()
        temp = Path(cls._temp.name)

        config = FixtureConfig()
        config.slow_seconds = 0.1
        cls.fixture = FixtureServer(config)
        cls.fixture_thread = threading.Thread(
            target=cls.fixture.serve_forever, daemon=True
        )
        cls.fixture_thread.start()

        # WIN registry carries one external streamable-http row pointing at
        # the in-process fixture; WSL registry carries an unused filler.
        win_registry = temp / "win.sqlite3"
        wsl_registry = temp / "wsl.sqlite3"
        win_manifest = temp / "win.manifest.json"
        wsl_manifest = temp / "wsl.manifest.json"
        win_manifest.write_text(
            json.dumps(
                {
                    "servers": [
                        {
                            "id": "http-echo",
                            "name": "HTTP echo fixture",
                            "summary": "streamable-http external row for connect-http route",
                            "transport": {
                                "type": "streamable-http",
                                "endpoint": cls.fixture.url,
                                "headers": {},
                            },
                            "management": {"ownership": "external"},
                        }
                    ]
                },
                indent=2,
            ),
            encoding="utf-8",
        )
        wsl_manifest.write_text(
            json.dumps(
                {
                    "servers": [
                        {
                            "id": "wsl-filler",
                            "name": "WSL filler",
                            "summary": "never opened in this route test",
                            "command": sys.executable,
                            "args": ["-c", "import sys; sys.stdin.read()"],
                        }
                    ]
                },
                indent=2,
            ),
            encoding="utf-8",
        )
        from bridge_runtime import Registry

        Registry.initialize_database(win_registry, win_manifest, replace=True)
        Registry.initialize_database(wsl_registry, wsl_manifest, replace=True)

        cls.link_port = free_port()
        cls.win_local_port = free_port()
        cls.wsl_local_port = free_port()
        win_home = temp / "win-app-data"
        wsl_home = temp / "wsl-state"
        win_home.mkdir()
        wsl_home.mkdir()
        environment = os.environ.copy()
        environment["PYTHONDONTWRITEBYTECODE"] = "1"
        # The CLI resolves side-specific default paths from these variables;
        # on Linux without them the win side would touch a Windows-style
        # AppData path under the real home directory.
        environment["LOCALAPPDATA"] = str(win_home)
        environment["XDG_STATE_HOME"] = str(wsl_home)
        cls._environment = environment
        cls.win_process = subprocess.Popen(
            [
                sys.executable,
                str(WIN_SCRIPT),
                "serve",
                "--registry",
                str(win_registry),
                "--local-port",
                str(cls.win_local_port),
                "--link-port",
                str(cls.link_port),
            ],
            cwd=ROOT,
            env=cls._environment,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        cls.wsl_process = subprocess.Popen(
            [
                sys.executable,
                str(WSL_SCRIPT),
                "serve",
                "--registry",
                str(wsl_registry),
                "--local-port",
                str(cls.wsl_local_port),
                "--link-port",
                str(cls.link_port),
            ],
            cwd=ROOT,
            env=cls._environment,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        try:
            cls._wait_for_link()
        except Exception:
            cls.tearDownClass()
            raise

    @classmethod
    def _wait_for_link(cls) -> None:
        from bridge_runtime import BridgeError, local_registry_query

        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            try:
                result = local_registry_query(
                    "127.0.0.1",
                    cls.wsl_local_port,
                    "remote",
                    "describe",
                    {"id": "http-echo"},
                )
                if result.get("id") == "http-echo":
                    return
            except (OSError, BridgeError):
                pass
            time.sleep(0.15)
        raise RuntimeError("bridge nodes did not establish their peer link")

    @classmethod
    def tearDownClass(cls) -> None:
        for name in ("wsl_process", "win_process"):
            process = getattr(cls, name, None)
            if process is not None and process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=10)
        fixture = getattr(cls, "fixture", None)
        if fixture is not None:
            fixture.stop()
        temp = getattr(cls, "_temp", None)
        if temp is not None:
            temp.cleanup()

    def test_connect_http_reaches_fixture_and_echoes(self) -> None:
        messages = "".join(
            json.dumps(item, separators=(",", ":")) + "\n"
            for item in [
                mcp_request(1, "initialize", {"protocolVersion": SHARED_PROTOCOL}),
                {"jsonrpc": "2.0", "method": "notifications/initialized"},
                mcp_request(2, "tools/list", {}),
                mcp_request(
                    3,
                    "tools/call",
                    {"name": "echo", "arguments": {"text": "route-ok"}},
                ),
            ]
        )
        environment = self.__class__._environment
        completed = subprocess.run(
            [
                sys.executable,
                str(WSL_SCRIPT),
                "connect-http",
                "http-echo",
                "--local-port",
                str(self.wsl_local_port),
            ],
            input=messages,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=ROOT,
            env=environment,
            timeout=60,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr[-2000:])
        responses = [
            json.loads(line) for line in completed.stdout.splitlines() if line
        ]
        # Each client message becomes its own HTTP POST through the adapter,
        # so response arrival order is not guaranteed; look responses up by id.
        self.assertEqual(sorted(item.get("id") for item in responses), [1, 2, 3])

        initialize = next(item for item in responses if item.get("id") == 1)
        result = initialize["result"]
        # The raw downstream initialize proves the adapter->fixture path: the
        # fixture identity and instructions arrive unmodified.
        self.assertEqual(result["serverInfo"]["name"], "fixture-http")
        self.assertEqual(result["instructions"], "fixture-http-instructions")
        self.assertEqual(result["protocolVersion"], SHARED_PROTOCOL)

        tools_list = next(item for item in responses if item.get("id") == 2)
        names = [item["name"] for item in tools_list["result"]["tools"]]
        self.assertIn("echo", names)

        called = next(item for item in responses if item.get("id") == 3)
        text = called["result"]["content"][0]["text"]
        self.assertEqual(text, "echo:route-ok")

        # The fixture really did serve the session on this route.
        self.assertGreaterEqual(self.fixture.initialize_count, 1)


if __name__ == "__main__":
    unittest.main()
