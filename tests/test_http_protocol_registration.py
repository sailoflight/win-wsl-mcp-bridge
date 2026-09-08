"""Owner-registry protocol-era selection for explicit HTTP conversion."""

import asyncio
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import AsyncMock, patch

from bridge_runtime import BridgeError, BridgeNode, Registry
from bridge_protocol import META_CLIENT_CAPABILITIES_KEY, META_PROTOCOL_VERSION_KEY


class HttpProtocolRegistrationTest(unittest.TestCase):
    def row(self, era="modern"):
        return {"id": "modern-http", "name": "Modern HTTP", "summary": "fixture",
                "transport": {"type": "streamable-http", "endpoint": "http://127.0.0.1:39999/mcp",
                              "protocolEra": era}}

    def test_era_is_validated_and_private(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            manifest, database = root / "manifest.json", root / "registry.sqlite3"
            manifest.write_text(json.dumps({"servers": [self.row()]}))
            Registry.initialize_database(database, manifest, replace=True)
            registry = Registry(database)
            self.assertEqual(registry.launch("modern-http")["transport"]["protocolEra"], "modern")
            self.assertEqual(registry.public("modern-http")["transport"], {"type": "streamable-http"})
        for invalid in (None, True, "future", 1, {}):
            with self.subTest(invalid=invalid), self.assertRaises(BridgeError):
                Registry._validate_manifest_row(self.row(invalid))
        row = self.row()
        row["transport"]["type"] = "stdio"
        row["command"] = "python"
        with self.assertRaises(BridgeError):
            Registry._validate_manifest_row(row)


class HttpProtocolSpawnTest(unittest.IsolatedAsyncioTestCase):
    async def test_owner_spawns_actual_modern_http_adapter_from_private_registration(self):
        from tests.test_modern_http_adapter import Fixture
        fixture = Fixture()
        process = None
        try:
            with tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                manifest, database = root / "manifest.json", root / "registry.sqlite3"
                row = {"id": "modern-http", "name": "Modern HTTP", "summary": "fixture",
                       "transport": {"type": "streamable-http", "protocolEra": "modern",
                                     "endpoint": f"http://127.0.0.1:{fixture.server_port}/mcp"}}
                manifest.write_text(json.dumps({"servers": [row]}))
                Registry.initialize_database(database, manifest, replace=True)
                node = BridgeNode(side="win", registry=Registry(database), local_host="127.0.0.1",
                                  local_port=1, link_mode="listen", link_host="127.0.0.1", link_port=2)
                # Isolate node pipe pumps so this gate can inspect the spawned
                # adapter's real stdio. The peer byte route has its own E2E gate.
                with patch.object(node, "_send_frame", new_callable=AsyncMock), \
                     patch.object(node, "_consume_stream_input", new_callable=AsyncMock), \
                     patch.object(node, "_pump_process_output", new_callable=AsyncMock), \
                     patch.object(node, "_pump_process_stderr", new_callable=AsyncMock), \
                     patch.object(node, "_wait_process", new_callable=AsyncMock):
                    await node._start_http_stdio_compatibility("fixture-stream", "modern-http",
                                                             node.registry.launch("modern-http"))
                    process = node.streams["fixture-stream"].process
                    self.assertIsNotNone(process)
                    message = {"jsonrpc": "2.0", "id": 1, "method": "server/discover",
                               "params": {"_meta": {META_PROTOCOL_VERSION_KEY: "2026-07-28",
                                                   META_CLIENT_CAPABILITIES_KEY: {}}}}
                    process.stdin.write((json.dumps(message) + "\n").encode())
                    await process.stdin.drain()
                    result = json.loads(await asyncio.wait_for(process.stdout.readline(), 10))
                    self.assertEqual(result["result"]["resultType"], "complete")
                    self.assertEqual(result["result"]["_meta"]["io.modelcontextprotocol/serverInfo"]["name"],
                                     "actual-fixture")
                    self.assertEqual(fixture.records[0][0], "POST")
                    headers = {key.lower(): value for key, value in fixture.records[0][1].items()}
                    self.assertEqual(headers["mcp-protocol-version"], "2026-07-28")
                    self.assertNotIn("mcp-session-id", headers)
                    process.stdin.close()
                    await asyncio.wait_for(process.wait(), 10)
                    await asyncio.gather(*node.streams["fixture-stream"].tasks)
        finally:
            if process is not None and process.returncode is None:
                process.kill()
                await process.wait()
            await asyncio.to_thread(fixture.finish)


if __name__ == "__main__":
    unittest.main()
