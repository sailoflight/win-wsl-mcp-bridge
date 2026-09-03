#!/usr/bin/env python3
"""Offline integration tests for the bidirectional bridge prototype."""

from __future__ import annotations

import asyncio
import base64
import concurrent.futures
import hashlib
import json
import os
import socket
import sqlite3
import stat
import subprocess
import sys
import tempfile
import time
import tomllib
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from bridge_runtime import (
    BridgeError,
    BridgeNode,
    ArtifactReceiveState,
    ArtifactTransferWaiter,
    Registry,
    StreamState,
    _is_loopback,
    default_registry_path,
    local_registry_query,
    proxy_stdio,
    publish_artifact,
)

WIN = ROOT / "win-bridge-mcp" / "bridge.py"
WSL = ROOT / "wsl-bridge-mcp" / "bridge.py"
FIXTURE = ROOT / "fixture_mcp.py"
SHARED_FIXTURE = ROOT / "shared_fixture_mcp.py"


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def write_registry(
    path: Path,
    server_id: str,
    fixture_name: str,
    *,
    multi_process_allowed: bool = False,
) -> None:
    manifest = path.with_name(path.name + ".manifest.json")
    manifest.write_text(
        json.dumps(
            {
                "servers": [
                    {
                        "id": server_id,
                        "name": fixture_name,
                        "summary": f"Integration fixture hosted on {fixture_name}.",
                        "command": sys.executable,
                        "args": [str(FIXTURE)],
                        "cwd": str(ROOT),
                        "env": {
                            "FIXTURE_MCP_NAME": fixture_name,
                            "FIXTURE_EXIT_AFTER_CALL": "1",
                        },
                        "token": "must-not-leak",
                        "process": {
                            "multiProcessAllowed": multi_process_allowed,
                            "enforcement": "business-mcp" if multi_process_allowed else "bridge-shared-backend",
                        },
                        "capabilityGroups": ["test", "echo", "artifact-delivery"],
                        "artifactDelivery": {
                            "enabled": True,
                            "maxBytes": 1048576,
                        },
                    }
                ]
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    Registry.initialize_database(path, manifest, replace=True)


def write_shared_registry(path: Path, state: Path) -> None:
    manifest = path.with_name(path.name + ".manifest.json")
    manifest.write_text(
        json.dumps(
            {
                "servers": [
                    {
                        "id": "shared-browser",
                        "name": "Shared browser fixture",
                        "summary": "Shared backend lifecycle fixture.",
                        "command": sys.executable,
                        "args": [str(SHARED_FIXTURE)],
                        "cwd": str(ROOT),
                        "env": {
                            "FIXTURE_MCP_NAME": "shared-browser-fixture",
                            "FIXTURE_SPAWN_LOG": str(state / "spawns.log"),
                            "FIXTURE_EVENT_LOG": str(state / "events.log"),
                            "FIXTURE_CHILD_PID_FILE": str(state / "child.pid"),
                        },
                        "process": {
                            "multiProcessAllowed": False,
                            "clientLease": {
                                "toolPatterns": ["browser_*"],
                                "releaseTool": "browser_session",
                                "releaseArguments": {"action": "release"},
                                "releasedResultPath": [
                                    "structuredContent",
                                    "profileReleased",
                                ],
                                "cleanupTimeoutSeconds": 3,
                            },
                            "sharedState": {
                                "mode": "fixed",
                                "rejectTools": ["view_change"],
                            },
                        },
                        "capabilityGroups": ["test", "shared-backend"],
                        "artifactDelivery": {"enabled": False},
                    },
                    {
                        "id": "other-mcp",
                        "name": "Independent fixture",
                        "summary": "Dedicated process isolation fixture.",
                        "command": sys.executable,
                        "args": [str(FIXTURE)],
                        "cwd": str(ROOT),
                        "env": {
                            "FIXTURE_MCP_NAME": "independent-fixture",
                            "FIXTURE_EXIT_AFTER_CALL": "1",
                        },
                        "process": {
                            "multiProcessAllowed": True,
                            "enforcement": "business-mcp",
                        },
                        "capabilityGroups": ["test", "isolation"],
                        "artifactDelivery": {"enabled": False},
                    },
                ]
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    Registry.initialize_database(path, manifest, replace=True)


class RawBridgeClient:
    def __init__(self, port: int, target: str):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=10)
        self.sock.settimeout(10)
        self.buffer = bytearray()
        self.sock.sendall(
            json.dumps(
                {"op": "connect", "target": target}, separators=(",", ":")
            ).encode("utf-8")
            + b"\n"
        )
        reply = self._recv_json()
        if not reply.get("ok"):
            self.close()
            raise BridgeError(str(reply.get("message", "connect failed")))

    def send(self, message: dict) -> None:
        self.sock.sendall(
            json.dumps(message, separators=(",", ":")).encode("utf-8") + b"\n"
        )

    def receive(self) -> dict:
        return self._recv_json()

    def request(self, request_id: object, method: str, params: dict) -> dict:
        self.send(
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "method": method,
                "params": params,
            }
        )
        while True:
            message = self.receive()
            if message.get("id") == request_id and (
                "result" in message or "error" in message
            ):
                return message

    def initialize(self, request_id: object = 1) -> dict:
        response = self.request(
            request_id,
            "initialize",
            {
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": {"name": "raw-test", "version": "1"},
            },
        )
        self.send({"jsonrpc": "2.0", "method": "notifications/initialized"})
        return response

    def call(self, request_id: object, name: str, arguments: dict | None = None) -> dict:
        return self.request(
            request_id,
            "tools/call",
            {"name": name, "arguments": arguments or {}},
        )

    def _recv_json(self) -> dict:
        while b"\n" not in self.buffer:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise EOFError("bridge client stream closed")
            self.buffer.extend(chunk)
        raw, _, remainder = self.buffer.partition(b"\n")
        self.buffer = bytearray(remainder)
        return json.loads(raw)

    def close(self) -> None:
        try:
            self.sock.close()
        except OSError:
            pass


def rpc_messages(value: object = "through-bridge") -> str:
    messages = [
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": {"name": "bridge-test", "version": "1"},
            },
        },
        {"jsonrpc": "2.0", "method": "notifications/initialized"},
        {"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}},
        {
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": {"name": "echo", "arguments": {"value": value}},
        },
    ]
    return "".join(json.dumps(item, separators=(",", ":")) + "\n" for item in messages)


def artifact_rpc_messages(text: str) -> str:
    messages = [
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": {"name": "artifact-test", "version": "1"},
            },
        },
        {"jsonrpc": "2.0", "method": "notifications/initialized"},
        {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {"name": "create_artifact", "arguments": {"text": text}},
        },
    ]
    return "".join(json.dumps(item, separators=(",", ":")) + "\n" for item in messages)


def invoke_proxy(
    script: Path,
    local_port: int,
    target: str,
    *,
    messages: str | None = None,
    artifact_inbox: Path | None = None,
) -> list[dict]:
    command = [sys.executable, str(script), "connect", target, "--local-port", str(local_port)]
    if artifact_inbox is not None:
        command.extend(["--artifact-inbox", str(artifact_inbox)])
    process = subprocess.run(
        command,
        input=messages if messages is not None else rpc_messages(),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        cwd=ROOT,
        timeout=30,
        check=True,
    )
    return [json.loads(line) for line in process.stdout.splitlines() if line]


class RegistryTest(unittest.TestCase):
    def test_public_metadata_never_exposes_launch_fields(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "registry.sqlite3"
            write_registry(path, "private-test", "Private Test")
            public = Registry(path).public("private-test")
            self.assertEqual(public["process"]["multiProcessAllowed"], False)
            for field in ("command", "args", "cwd", "env", "token"):
                self.assertNotIn(field, public)
            with sqlite3.connect(path) as connection:
                self.assertEqual(connection.execute("PRAGMA user_version").fetchone()[0], 2)
                self.assertEqual(connection.execute("PRAGMA journal_mode").fetchone()[0], "wal")

    def test_shared_process_policy_is_enforced_privately_and_redacted_publicly(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            database = root / "registry.sqlite3"
            state = root / "state"
            state.mkdir()
            write_shared_registry(database, state)
            registry = Registry(database)
            private_process = registry.launch("shared-browser")["process"]
            self.assertEqual(
                private_process["enforcement"], "bridge-shared-backend"
            )
            self.assertEqual(
                private_process["clientLease"]["toolPatterns"], ["browser_*"]
            )
            public_process = registry.public("shared-browser")["process"]
            self.assertEqual(
                public_process["clientLease"],
                {
                    "enabled": True,
                    "busyPolicy": "error",
                    "releaseOnDisconnect": True,
                },
            )
            self.assertEqual(public_process["sharedState"], {"mode": "fixed"})
            serialized = json.dumps(public_process)
            for private in (
                "toolPatterns",
                "releaseTool",
                "releaseArguments",
                "releasedResultPath",
                "browser_*",
                "view_change",
            ):
                self.assertNotIn(private, serialized)

        with self.assertRaisesRegex(BridgeError, "clientLease requires"):
            Registry._validate_manifest_row(
                {
                    "id": "invalid-lease",
                    "command": "python",
                    "process": {
                        "multiProcessAllowed": True,
                        "clientLease": {},
                    },
                }
            )

    @unittest.skipIf(os.name == "nt", "POSIX mode bits are not Windows ACLs")
    def test_registry_database_uses_owner_only_permissions(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            state = Path(temp) / "private-state"
            database = state / "registry.sqlite3"
            manifest = Path(temp) / "manifest.json"
            manifest.write_text('{"servers": []}', encoding="utf-8")
            Registry.initialize_database(database, manifest, replace=True)
            self.assertEqual(stat.S_IMODE(state.stat().st_mode), 0o700)
            self.assertEqual(stat.S_IMODE(database.stat().st_mode), 0o600)
            database.chmod(0o644)
            Registry(database)
            self.assertEqual(stat.S_IMODE(database.stat().st_mode), 0o600)

    def test_default_registries_are_separate_host_local_databases(self) -> None:
        with mock.patch.dict(
            os.environ,
            {
                "LOCALAPPDATA": "/tmp/windows-local-app-data",
                "XDG_STATE_HOME": "/tmp/wsl-local-state",
            },
            clear=False,
        ):
            self.assertEqual(
                default_registry_path("win"),
                Path("/tmp/windows-local-app-data/WinWslMcpBridge/registry.sqlite3"),
            )
            self.assertEqual(
                default_registry_path("wsl"),
                Path("/tmp/wsl-local-state/win-wsl-mcp-bridge/registry.sqlite3"),
            )
            self.assertNotEqual(default_registry_path("win"), default_registry_path("wsl"))

    def test_registry_init_cli_creates_versioned_sqlite_database(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "registry.sqlite3"
            process = subprocess.run(
                [
                    sys.executable,
                    str(WSL),
                    "registry-init",
                    "--registry",
                    str(database),
                    "--manifest",
                    str(ROOT / "wsl-bridge-mcp" / "registry.example.json"),
                    "--replace",
                ],
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=ROOT,
                timeout=20,
                check=True,
            )
            self.assertIn(str(database), process.stdout)
            public = Registry(database).public("example-wsl-mcp")
            self.assertEqual(public["id"], "example-wsl-mcp")
            self.assertIsNone(public["process"]["multiProcessAllowed"])

    def test_doctor_reports_deployment_readiness_and_clean_cli_errors(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            database = root / "registry.sqlite3"
            workspace = root / "workspace"
            workspace.mkdir()
            write_registry(database, "doctor-test", "Doctor Test")
            healthy = subprocess.run(
                [
                    sys.executable,
                    str(WSL),
                    "doctor",
                    "--registry",
                    str(database),
                    "--allow-artifact-root",
                    str(workspace),
                ],
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=ROOT,
                timeout=20,
                check=True,
            )
            report = json.loads(healthy.stdout)
            self.assertTrue(report["ok"])
            self.assertEqual(report["bridgeProtocol"], "win-wsl-mcp-bridge/0.2")
            unhealthy = subprocess.run(
                [
                    sys.executable,
                    str(WSL),
                    "doctor",
                    "--registry",
                    str(database),
                    "--link-host",
                    "192.0.2.1",
                ],
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=ROOT,
                timeout=20,
                check=False,
            )
            self.assertEqual(unhealthy.returncode, 1)
            self.assertFalse(json.loads(unhealthy.stdout)["ok"])
            missing = subprocess.run(
                [
                    sys.executable,
                    str(WSL),
                    "registry-init",
                    "--registry",
                    str(root / "missing.sqlite3"),
                    "--manifest",
                    str(root / "missing.json"),
                ],
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=ROOT,
                timeout=20,
                check=False,
            )
            self.assertEqual(missing.returncode, 1)
            self.assertNotIn("Traceback", missing.stderr)
            self.assertIn("wsl bridge:", missing.stderr)

    def test_registry_v1_migrates_to_artifact_schema_v2(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "registry.sqlite3"
            manifest = Path(temp) / "empty.json"
            manifest.write_text('{"servers": []}', encoding="utf-8")
            with sqlite3.connect(database) as connection:
                connection.execute(
                    """
                    CREATE TABLE servers (
                        id TEXT PRIMARY KEY, name TEXT NOT NULL, summary TEXT NOT NULL,
                        command TEXT NOT NULL, args_json TEXT NOT NULL, cwd TEXT,
                        env_json TEXT NOT NULL, process_json TEXT NOT NULL,
                        capability_groups_json TEXT NOT NULL, server_info_json TEXT,
                        enabled INTEGER NOT NULL, updated_at_ns INTEGER NOT NULL
                    )
                    """
                )
                connection.execute(
                    "INSERT INTO servers VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        "legacy",
                        "Legacy",
                        "v1 entry",
                        "python",
                        "[]",
                        None,
                        "{}",
                        '{"multiProcessAllowed":null}',
                        "[]",
                        None,
                        1,
                        1,
                    ),
                )
                connection.execute("PRAGMA user_version = 1")
            Registry.initialize_database(database, manifest)
            registry = Registry(database)
            self.assertEqual(registry.public("legacy")["id"], "legacy")
            self.assertNotIn("artifactDelivery", registry.public("legacy"))
            with sqlite3.connect(database) as connection:
                self.assertEqual(connection.execute("PRAGMA user_version").fetchone()[0], 2)

    def test_registry_rejects_duplicate_and_invalid_ids(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "registry.sqlite3"
            manifest = Path(temp) / "invalid.json"
            manifest.write_text(
                json.dumps(
                    {
                        "servers": [
                            {"id": "bad id", "command": "python"},
                        ]
                    }
                ),
                encoding="utf-8",
            )
            with self.assertRaises(BridgeError):
                Registry.initialize_database(database, manifest)

    def test_registry_rejects_reserved_artifact_environment(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "registry.sqlite3"
            manifest = Path(temp) / "reserved-env.json"
            manifest.write_text(
                json.dumps(
                    {
                        "servers": [
                            {
                                "id": "reserved-env",
                                "command": "python",
                                "env": {
                                    "WIN_WSL_MCP_BRIDGE_ARTIFACT_TOKEN": "must-not-enter-child"
                                },
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(BridgeError, "must not set bridge artifact"):
                Registry.initialize_database(database, manifest)

    def test_registry_rejects_non_boolean_enabled_and_casefolded_reserved_env(self) -> None:
        with self.assertRaisesRegex(BridgeError, "enabled must be boolean"):
            Registry._validate_manifest_row(
                {"id": "bad-enabled", "command": "python", "enabled": "false"}
            )
        with self.assertRaisesRegex(BridgeError, "must not set bridge artifact"):
            Registry._validate_manifest_row(
                {
                    "id": "reserved-env-case",
                    "command": "python",
                    "env": {"win_wsl_mcp_bridge_artifact_token": "shadow"},
                }
            )

    def test_listener_security_boundary_is_loopback(self) -> None:
        self.assertTrue(_is_loopback("127.0.0.1"))
        self.assertTrue(_is_loopback("::1"))
        self.assertFalse(_is_loopback("0.0.0.0"))
        self.assertEqual(proxy_stdio("192.0.2.1", 1, "valid-target"), 2)
        with self.assertRaisesRegex(BridgeError, "publisher host"):
            publish_artifact("192.0.2.1", 1, "token", "result.txt", None, None)
        with self.assertRaisesRegex(BridgeError, "registry host"):
            local_registry_query("192.0.2.1", 1, "remote", "list", {})
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "registry.sqlite3"
            write_registry(path, "loopback-test", "Loopback Test")
            with self.assertRaisesRegex(BridgeError, "connector must use a loopback"):
                BridgeNode(
                    side="wsl",
                    registry=Registry(path),
                    local_host="127.0.0.1",
                    local_port=free_port(),
                    link_mode="connect",
                    link_host="192.0.2.1",
                    link_port=free_port(),
                )

    def test_listener_treats_peer_eof_as_a_clean_link_end(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "registry.sqlite3"
            write_registry(path, "listener-eof", "Listener EOF")
            node = BridgeNode(
                side="win",
                registry=Registry(path),
                local_host="127.0.0.1",
                local_port=free_port(),
                link_mode="listen",
                link_host="127.0.0.1",
                link_port=free_port(),
            )

            class Writer:
                def __init__(self) -> None:
                    self.closed = False

                def close(self) -> None:
                    self.closed = True

                async def wait_closed(self) -> None:
                    return None

            async def exercise() -> None:
                reader = asyncio.StreamReader()
                reader.feed_eof()
                writer = Writer()
                messages: list[str] = []
                node.log = messages.append  # type: ignore[method-assign]
                await node._accept_link(reader, writer)  # type: ignore[arg-type]
                self.assertTrue(writer.closed)
                self.assertFalse(node.link_installing)
                self.assertIn("peer link ended: peer closed", messages)

            asyncio.run(exercise())

    def test_registry_mcp_supports_ping_and_standard_jsonrpc_errors(self) -> None:
        messages = [
            "{not-json}",
            json.dumps({"jsonrpc": "1.0", "id": 1, "method": "ping"}),
            json.dumps(
                {
                    "jsonrpc": "2.0",
                    "id": 2,
                    "method": "initialize",
                    "params": {
                        "protocolVersion": "2025-06-18",
                        "capabilities": {},
                    },
                }
            ),
            json.dumps({"jsonrpc": "2.0", "id": 3, "method": "ping", "params": {}}),
            json.dumps({"jsonrpc": "2.0", "id": 4, "method": "unknown", "params": {}}),
            json.dumps(
                {
                    "jsonrpc": "2.0",
                    "id": 5,
                    "method": "tools/call",
                    "params": [],
                }
            ),
        ]
        process = subprocess.run(
            [sys.executable, str(WSL), "registry-mcp", "--local-port", "1"],
            input="\n".join(messages) + "\n",
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=ROOT,
            timeout=20,
            check=True,
        )
        responses = [json.loads(line) for line in process.stdout.splitlines()]
        self.assertEqual(responses[0]["error"]["code"], -32700)
        self.assertEqual(responses[1]["error"]["code"], -32600)
        self.assertEqual(responses[2]["result"]["protocolVersion"], "2025-06-18")
        self.assertEqual(responses[3]["result"], {})
        self.assertEqual(responses[4]["error"]["code"], -32601)
        self.assertEqual(responses[5]["error"]["code"], -32602)
        self.assertEqual(process.stderr, "")

    def test_oversized_registry_response_fails_without_dropping_link(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "registry.sqlite3"
            write_registry(path, "response-limit", "Response Limit")
            node = BridgeNode(
                side="win",
                registry=Registry(path),
                local_host="127.0.0.1",
                local_port=free_port(),
                link_mode="listen",
                link_host="127.0.0.1",
                link_port=free_port(),
            )

            async def exercise() -> None:
                sent: list[dict] = []

                async def capture(frame: dict) -> None:
                    sent.append(frame)

                node._send_frame = capture  # type: ignore[method-assign]
                node.registry.query = lambda _action, _arguments: "x" * (1024 * 1024)  # type: ignore[method-assign]
                await node._handle_registry_request(
                    {
                        "type": "registry_request",
                        "request": "large-response",
                        "action": "list",
                        "arguments": {},
                    }
                )
                self.assertFalse(sent[0]["ok"])
                self.assertIn("narrow the query", sent[0]["message"])

            asyncio.run(exercise())

    def test_remote_open_rejects_missing_and_duplicate_stream_ids(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "registry.sqlite3"
            write_registry(path, "stream-test", "Stream Test")
            node = BridgeNode(
                side="win",
                registry=Registry(path),
                local_host="127.0.0.1",
                local_port=free_port(),
                link_mode="listen",
                link_host="127.0.0.1",
                link_port=free_port(),
            )
            sent: list[dict] = []

            async def exercise() -> None:
                async def capture(frame: dict) -> None:
                    sent.append(frame)

                node._send_frame = capture  # type: ignore[method-assign]
                await node._handle_remote_open({"type": "open", "target": "stream-test"})
                duplicate = "wsl-0123456789abcdef"
                node.streams[duplicate] = None  # type: ignore[assignment]
                await node._handle_remote_open(
                    {"type": "open", "stream": duplicate, "target": "stream-test"}
                )

            asyncio.run(exercise())
            self.assertEqual(sent[0]["message"], "invalid open frame")
            self.assertEqual(sent[1]["message"], "duplicate stream id")

    def test_process_start_failure_is_redacted_from_peer(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "registry.sqlite3"
            manifest = Path(temp) / "manifest.json"
            private_command = "/private/bridge-test/missing-mcp"
            manifest.write_text(
                json.dumps(
                    {
                        "servers": [
                            {
                                "id": "bad-start",
                                "command": private_command,
                                "summary": "Expected startup failure",
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            Registry.initialize_database(database, manifest, replace=True)
            node = BridgeNode(
                side="win",
                registry=Registry(database),
                local_host="127.0.0.1",
                local_port=free_port(),
                link_mode="listen",
                link_host="127.0.0.1",
                link_port=free_port(),
            )
            sent: list[dict] = []

            async def exercise() -> None:
                async def capture(frame: dict) -> None:
                    sent.append(frame)

                node._send_frame = capture  # type: ignore[method-assign]
                await node._handle_remote_open(
                    {"type": "open", "stream": "wsl-bad-start", "target": "bad-start"}
                )

            asyncio.run(exercise())
            self.assertEqual(
                sent[0]["message"],
                "registered MCP failed to start; inspect local bridge diagnostics",
            )
            self.assertNotIn(private_command, json.dumps(sent))

    def test_link_reader_queues_stream_data_without_waiting_for_downstream_drain(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "registry.sqlite3"
            write_registry(path, "queue-test", "Queue Test")
            node = BridgeNode(
                side="win",
                registry=Registry(path),
                local_host="127.0.0.1",
                local_port=free_port(),
                link_mode="listen",
                link_host="127.0.0.1",
                link_port=free_port(),
            )
            stream = StreamState(stream_id="wsl-queue-test")
            node.streams[stream.stream_id] = stream

            asyncio.run(
                asyncio.wait_for(
                    node._handle_stream_data(
                        {
                            "type": "data",
                            "stream": stream.stream_id,
                            "sequence": 0,
                            "data": base64.b64encode(b"payload").decode("ascii"),
                        }
                    ),
                    timeout=0.1,
                )
            )
            self.assertEqual(stream.inbound.get_nowait(), (0, b"payload"))

    def test_stream_sender_waits_for_matching_downstream_acknowledgement(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "registry.sqlite3"
            write_registry(path, "ack-test", "Ack Test")
            node = BridgeNode(
                side="win",
                registry=Registry(path),
                local_host="127.0.0.1",
                local_port=free_port(),
                link_mode="listen",
                link_host="127.0.0.1",
                link_port=free_port(),
            )

            async def exercise() -> None:
                sent: list[dict] = []

                async def capture(frame: dict) -> None:
                    sent.append(frame)

                node._send_frame = capture  # type: ignore[method-assign]
                stream = StreamState(stream_id="wsl-ack-test")
                node.streams[stream.stream_id] = stream
                sending = asyncio.create_task(node._send_stream_data(stream, b"payload"))
                await asyncio.sleep(0)
                self.assertFalse(sending.done())
                self.assertEqual(sent[0]["sequence"], 0)
                node._handle_stream_data_ok(
                    {
                        "type": "data_ok",
                        "stream": stream.stream_id,
                        "sequence": 0,
                    }
                )
                await asyncio.wait_for(sending, timeout=0.1)
                self.assertEqual(stream.outbound_sequence, 1)

            asyncio.run(exercise())

    def test_registry_request_handler_does_not_block_peer_data_frames(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "registry.sqlite3"
            write_registry(path, "dispatch-test", "Dispatch Test")
            node = BridgeNode(
                side="win",
                registry=Registry(path),
                local_host="127.0.0.1",
                local_port=free_port(),
                link_mode="listen",
                link_host="127.0.0.1",
                link_port=free_port(),
            )

            async def exercise() -> None:
                started = asyncio.Event()
                release = asyncio.Event()

                async def slow_registry(_frame: dict) -> None:
                    started.set()
                    await release.wait()

                node._handle_registry_request = slow_registry  # type: ignore[method-assign]
                stream = StreamState(stream_id="wsl-dispatch-test")
                node.streams[stream.stream_id] = stream
                await node._handle_frame(
                    {
                        "type": "registry_request",
                        "request": "slow-registry",
                        "action": "list",
                        "arguments": {},
                    }
                )
                await asyncio.wait_for(started.wait(), timeout=0.1)
                await node._handle_frame(
                    {
                        "type": "data",
                        "stream": stream.stream_id,
                        "sequence": 0,
                        "data": base64.b64encode(b"still flowing").decode("ascii"),
                    }
                )
                self.assertEqual(stream.inbound.get_nowait(), (0, b"still flowing"))
                release.set()
                await asyncio.gather(*node.background_tasks)

            asyncio.run(exercise())

    def test_local_client_eof_closes_stream_after_bounded_grace(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "registry.sqlite3"
            write_registry(path, "eof-test", "EOF Test")
            node = BridgeNode(
                side="wsl",
                registry=Registry(path),
                local_host="127.0.0.1",
                local_port=free_port(),
                link_mode="connect",
                link_host="127.0.0.1",
                link_port=free_port(),
            )

            class Writer:
                def __init__(self) -> None:
                    self.buffer = bytearray()
                    self.closing = False

                def write(self, data: bytes) -> None:
                    self.buffer.extend(data)

                async def drain(self) -> None:
                    return None

                def is_closing(self) -> bool:
                    return self.closing

                def close(self) -> None:
                    self.closing = True

                async def wait_closed(self) -> None:
                    return None

                def write_eof(self) -> None:
                    return None

            async def exercise() -> None:
                frames: list[dict] = []
                reader = asyncio.StreamReader()
                reader.feed_eof()
                writer = Writer()
                node.link_ready.set()

                async def capture(frame: dict) -> None:
                    frames.append(frame)
                    if frame.get("type") == "open":
                        stream = node.streams[frame["stream"]]
                        assert stream.opened is not None
                        stream.opened.set_result(None)

                node._send_frame = capture  # type: ignore[method-assign]
                with mock.patch("bridge_runtime.STREAM_EOF_GRACE_SECONDS", 0.01):
                    await asyncio.wait_for(
                        node._serve_local_stream(  # type: ignore[arg-type]
                            reader,
                            writer,
                            "eof-test",
                            artifact_inbox=None,
                        ),
                        timeout=0.5,
                    )
                self.assertTrue(writer.closing)
                self.assertEqual(
                    [frame["type"] for frame in frames],
                    ["open", "eof", "close"],
                )
                self.assertEqual(node.streams, {})

            asyncio.run(exercise())

    def test_node_shutdown_terminates_active_business_process(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            path = root / "registry.sqlite3"
            write_registry(path, "shutdown-test", "Shutdown Test")
            node = BridgeNode(
                side="wsl",
                registry=Registry(path),
                local_host="127.0.0.1",
                local_port=free_port(),
                link_mode="connect",
                link_host="127.0.0.1",
                link_port=free_port(),
                artifact_spool_root=root / "spool",
            )

            async def exercise() -> None:
                process = await asyncio.create_subprocess_exec(
                    sys.executable,
                    "-c",
                    "import time; time.sleep(60)",
                )
                stream = StreamState(
                    stream_id="wsl-shutdown-test",
                    process=process,
                )
                node.streams[stream.stream_id] = stream
                running = asyncio.create_task(node.run())
                deadline = asyncio.get_running_loop().time() + 2
                while node.local_server is None:
                    if asyncio.get_running_loop().time() >= deadline:
                        self.fail("node did not start its local listener")
                    await asyncio.sleep(0.01)
                running.cancel()
                await asyncio.gather(running, return_exceptions=True)
                self.assertIsNotNone(process.returncode)
                self.assertEqual(node.streams, {})

            asyncio.run(exercise())

    def test_artifact_inbox_must_stay_under_operator_root(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            database = root / "registry.sqlite3"
            allowed = root / "allowed"
            outside = root / "outside"
            allowed.mkdir()
            outside.mkdir()
            write_registry(database, "inbox-test", "Inbox Test")
            node = BridgeNode(
                side="wsl",
                registry=Registry(database),
                local_host="127.0.0.1",
                local_port=free_port(),
                link_mode="connect",
                link_host="127.0.0.1",
                link_port=free_port(),
                allowed_artifact_roots=[allowed],
            )
            self.assertEqual(node._validate_artifact_inbox(str(allowed)), allowed.resolve())
            with self.assertRaisesRegex(BridgeError, "outside Operator-authorized"):
                node._validate_artifact_inbox(str(outside))

    def test_artifact_begin_send_failure_aborts_receive_state_immediately(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            database = root / "registry.sqlite3"
            workspace = root / "workspace"
            workspace.mkdir()
            write_registry(database, "begin-failure", "Begin Failure")
            node = BridgeNode(
                side="wsl",
                registry=Registry(database),
                local_host="127.0.0.1",
                local_port=free_port(),
                link_mode="connect",
                link_host="127.0.0.1",
                link_port=free_port(),
                allowed_artifact_roots=[workspace],
                artifact_spool_root=root / "spool",
            )

            async def exercise() -> None:
                stream = StreamState(
                    stream_id="wsl-begin-failure",
                    artifact_inbox=workspace,
                )
                node.streams[stream.stream_id] = stream

                async def fail_send(_frame: dict) -> None:
                    raise BridgeError("peer link unavailable")

                node._send_frame = fail_send  # type: ignore[method-assign]
                await node._handle_artifact_begin(
                    {
                        "type": "artifact_begin",
                        "stream": stream.stream_id,
                        "artifact": "begin-failure-artifact",
                        "name": "result.bin",
                        "mediaType": "application/octet-stream",
                        "size": 3,
                        "sha256": hashlib.sha256(b"abc").hexdigest(),
                    }
                )
                self.assertEqual(node.receiving_artifacts, {})
                self.assertFalse(
                    (workspace / ".mcp-artifacts" / "begin-failure-artifact").exists()
                )

            asyncio.run(exercise())

    def test_startup_janitor_removes_only_stale_workspace_partials(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            database = root / "registry.sqlite3"
            workspace = root / "workspace"
            artifact_root = workspace / ".mcp-artifacts"
            stale = artifact_root / "stale-transfer"
            committed = artifact_root / "committed-transfer"
            stale.mkdir(parents=True)
            committed.mkdir()
            (stale / ".partial").write_bytes(b"incomplete")
            final = committed / "result.bin"
            final.write_bytes(b"committed")
            write_registry(database, "janitor-test", "Janitor Test")
            node = BridgeNode(
                side="wsl",
                registry=Registry(database),
                local_host="127.0.0.1",
                local_port=free_port(),
                link_mode="connect",
                link_host="127.0.0.1",
                link_port=free_port(),
                allowed_artifact_roots=[workspace],
                artifact_spool_root=root / "spool",
            )
            node._prepare_artifact_spool()
            self.assertFalse(stale.exists())
            self.assertEqual(final.read_bytes(), b"committed")

    @unittest.skipIf(os.name == "nt", "POSIX mode bits are not Windows ACLs")
    def test_artifact_partial_uses_owner_only_permissions(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            database = root / "registry.sqlite3"
            workspace = root / "workspace"
            workspace.mkdir()
            write_registry(database, "partial-mode", "Partial Mode")
            node = BridgeNode(
                side="wsl",
                registry=Registry(database),
                local_host="127.0.0.1",
                local_port=free_port(),
                link_mode="connect",
                link_host="127.0.0.1",
                link_port=free_port(),
                allowed_artifact_roots=[workspace],
            )

            async def exercise() -> None:
                async def capture(_frame: dict) -> None:
                    return None

                node._send_frame = capture  # type: ignore[method-assign]
                stream = StreamState(
                    stream_id="win-partial-mode",
                    artifact_inbox=workspace.resolve(),
                )
                node.streams[stream.stream_id] = stream
                artifact_id = "artifact-partial-mode"
                await node._handle_artifact_begin(
                    {
                        "type": "artifact_begin",
                        "stream": stream.stream_id,
                        "artifact": artifact_id,
                        "name": "result.bin",
                        "mediaType": None,
                        "size": 0,
                        "sha256": hashlib.sha256(b"").hexdigest(),
                    }
                )
                state = node.receiving_artifacts[artifact_id]
                self.assertEqual(stat.S_IMODE(state.temp_path.stat().st_mode), 0o600)
                await node._abort_received_artifact(state, "test cleanup")

            asyncio.run(exercise())

    def test_artifact_commit_never_overwrites_existing_file(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            partial = root / ".result.partial"
            final = root / "result.txt"
            partial.write_bytes(b"new")
            final.write_bytes(b"existing")
            with self.assertRaises(FileExistsError):
                BridgeNode._commit_artifact_no_overwrite(partial, final)
            self.assertEqual(final.read_bytes(), b"existing")
            self.assertEqual(partial.read_bytes(), b"new")

    def test_artifact_publish_rejects_unknown_session_token(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            database = root / "registry.sqlite3"
            write_registry(database, "token-test", "Token Test")
            node = BridgeNode(
                side="wsl",
                registry=Registry(database),
                local_host="127.0.0.1",
                local_port=free_port(),
                link_mode="connect",
                link_host="127.0.0.1",
                link_port=free_port(),
            )
            node.peer_artifacts = True
            with self.assertRaisesRegex(BridgeError, "session is unavailable"):
                asyncio.run(
                    node._publish_local_artifact(
                        {
                            "op": "publish",
                            "token": "guessed-token",
                            "relativePath": "result.txt",
                        }
                    )
                )

    def test_artifact_receiver_aborts_chunk_after_end_once(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            database = root / "registry.sqlite3"
            write_registry(database, "receive-phase", "Receive Phase")
            node = BridgeNode(
                side="win",
                registry=Registry(database),
                local_host="127.0.0.1",
                local_port=free_port(),
                link_mode="listen",
                link_host="127.0.0.1",
                link_port=free_port(),
            )
            artifact_dir = root / "artifact"
            artifact_dir.mkdir()
            partial = artifact_dir / ".partial"
            final = artifact_dir / "result.bin"

            async def exercise() -> None:
                state = ArtifactReceiveState(
                    stream_id="wsl-receive-phase",
                    artifact_id="artifact-receive-phase",
                    name="result.bin",
                    media_type=None,
                    expected_size=0,
                    temp_path=partial,
                    final_path=final,
                    handle=open(partial, "xb"),
                    phase="ending",
                )
                node.receiving_artifacts[state.artifact_id] = state
                node._handle_artifact_chunk(
                    {
                        "type": "artifact_chunk",
                        "stream": state.stream_id,
                        "artifact": state.artifact_id,
                        "sequence": 0,
                        "data": "",
                    }
                )
                self.assertEqual(state.phase, "aborting")
                assert state.abort_task is not None
                await state.abort_task
                self.assertNotIn(state.artifact_id, node.receiving_artifacts)
                self.assertFalse(partial.exists())

            asyncio.run(exercise())

    def test_artifact_sender_rejects_premature_commit_receipt(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "registry.sqlite3"
            write_registry(database, "phase-test", "Phase Test")
            node = BridgeNode(
                side="wsl",
                registry=Registry(database),
                local_host="127.0.0.1",
                local_port=free_port(),
                link_mode="connect",
                link_host="127.0.0.1",
                link_port=free_port(),
            )

            async def exercise() -> None:
                loop = asyncio.get_running_loop()
                waiter = ArtifactTransferWaiter(
                    stream_id="wsl-phase-test",
                    ready=loop.create_future(),
                    done=loop.create_future(),
                    expected_size=4,
                    expected_sha256="0" * 64,
                )
                node.pending_artifacts["artifact-phase-test"] = waiter
                node._handle_artifact_reply(
                    {
                        "type": "artifact_ok",
                        "stream": waiter.stream_id,
                        "artifact": "artifact-phase-test",
                        "uri": "file:///tmp/result",
                        "path": "/tmp/result",
                        "size": 4,
                        "sha256": "0" * 64,
                    }
                )
                self.assertEqual(waiter.phase, "failed")
                self.assertIsInstance(waiter.ready.exception(), BridgeError)
                waiter.done.cancel()
                node.pending_artifacts.clear()

            asyncio.run(exercise())

    def test_stream_close_immediately_fails_active_artifact_chunk_ack(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "registry.sqlite3"
            write_registry(database, "close-ack-test", "Close Ack Test")
            node = BridgeNode(
                side="wsl",
                registry=Registry(database),
                local_host="127.0.0.1",
                local_port=free_port(),
                link_mode="connect",
                link_host="127.0.0.1",
                link_port=free_port(),
            )

            async def exercise() -> None:
                loop = asyncio.get_running_loop()
                stream = StreamState(stream_id="wsl-close-ack-test")
                node.streams[stream.stream_id] = stream
                waiter = ArtifactTransferWaiter(
                    stream_id=stream.stream_id,
                    ready=loop.create_future(),
                    done=loop.create_future(),
                    expected_size=1,
                    expected_sha256="0" * 64,
                    phase="sending",
                    chunk_ack=loop.create_future(),
                )
                waiter.ready.set_result({})
                node.pending_artifacts["artifact-close-ack-test"] = waiter
                await node._close_stream(stream.stream_id, remote=True)
                assert waiter.chunk_ack is not None
                self.assertIsInstance(waiter.chunk_ack.exception(), BridgeError)
                waiter.done.cancel()

            asyncio.run(exercise())

    def test_artifact_names_reject_path_and_windows_device_syntax(self) -> None:
        for value in (
            "../secret",
            r"C:\\secret.txt",
            r"\\server\\share",
            "file:name.txt",
            "bad?.txt",
            "CON.txt",
            "result. ",
            "%2e%2e%2fsecret",
            "nested/file.txt",
            "界" * 100,
        ):
            with self.subTest(value=value), self.assertRaises(BridgeError):
                BridgeNode._safe_artifact_name(value)
        self.assertEqual(BridgeNode._safe_artifact_name("report 结果.step"), "report 结果.step")

    def test_artifact_snapshot_rejects_symlink_hardlink_and_oversize(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            database = root / "registry.sqlite3"
            write_registry(database, "artifact-test", "Artifact Test")
            node = BridgeNode(
                side="win",
                registry=Registry(database),
                local_host="127.0.0.1",
                local_port=free_port(),
                link_mode="listen",
                link_host="127.0.0.1",
                link_port=free_port(),
                artifact_spool_root=root / "spool",
            )
            node._prepare_artifact_spool()
            stage = root / "stage"
            stage.mkdir()
            source = stage / "result.bin"
            source.write_bytes(b"1234")
            with self.assertRaisesRegex(BridgeError, "size limit"):
                node._snapshot_artifact(source, 3)
            hardlink = stage / "hardlink.bin"
            os.link(source, hardlink)
            with self.assertRaisesRegex(BridgeError, "regular unlinked file"):
                node._snapshot_artifact(source, 100)
            hardlink.unlink()
            replaceable = stage / "replaceable.bin"
            replaceable.write_bytes(b"old")
            old_metadata = replaceable.lstat()
            old_identity = (
                old_metadata.st_dev,
                old_metadata.st_ino,
                old_metadata.st_ctime_ns,
            )
            replaceable.unlink()
            replaceable.write_bytes(b"new")
            node._unlink_source_if_same(replaceable, old_identity)
            self.assertEqual(replaceable.read_bytes(), b"new")
            outside = root / "outside.bin"
            outside.write_bytes(b"outside")
            symlink = stage / "symlink.bin"
            try:
                symlink.symlink_to(outside)
            except OSError:
                self.skipTest("symlinks are unavailable in this environment")
            with self.assertRaisesRegex(BridgeError, "symbolic link"):
                node._snapshot_artifact(symlink, 100)

    def test_distribution_metadata_and_cli_version_are_consistent(self) -> None:
        project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
        self.assertEqual(project["project"]["dynamic"], ["version"])
        self.assertEqual(
            project["tool"]["setuptools"]["py-modules"],
            ["bridge_runtime", "bridge_publisher"],
        )
        self.assertEqual(
            project["project"]["scripts"],
            {
                "win-wsl-mcp-win": "bridge_runtime:win_main",
                "win-wsl-mcp-wsl": "bridge_runtime:wsl_main",
            },
        )
        for script in (WIN, WSL):
            process = subprocess.run(
                [sys.executable, str(script), "--version"],
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=ROOT,
                timeout=20,
                check=True,
            )
            self.assertIn("0.4.0", process.stdout)
        direct = subprocess.run(
            [sys.executable, str(ROOT / "bridge_runtime.py")],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=ROOT,
            timeout=20,
            check=False,
        )
        self.assertNotEqual(direct.returncode, 0)
        self.assertIn("entry point", direct.stderr)

    def test_project_has_only_two_component_directories(self) -> None:
        directories = sorted(
            path.name for path in ROOT.iterdir() if path.is_dir() and not path.name.startswith(".")
        )
        self.assertEqual(directories, ["win-bridge-mcp", "wsl-bridge-mcp"])


class SharedBackendAcceptanceTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.temp = tempfile.TemporaryDirectory()
        root = Path(cls.temp.name)
        cls.root = root
        cls.state = root / "state"
        cls.state.mkdir()
        for name in ("spawns.log", "events.log"):
            (cls.state / name).touch()
        cls.win_registry = root / "win.sqlite3"
        cls.wsl_registry = root / "wsl.sqlite3"
        write_shared_registry(cls.win_registry, cls.state)
        empty_manifest = root / "empty.json"
        empty_manifest.write_text('{"servers": []}', encoding="utf-8")
        Registry.initialize_database(cls.wsl_registry, empty_manifest, replace=True)
        cls.link_port = free_port()
        cls.win_local_port = free_port()
        cls.wsl_local_port = free_port()
        cls.environment = os.environ.copy()
        cls.environment["PYTHONDONTWRITEBYTECODE"] = "1"
        cls._start_win_node()
        cls.wsl_process = subprocess.Popen(
            [
                sys.executable,
                str(WSL),
                "serve",
                "--registry",
                str(cls.wsl_registry),
                "--local-port",
                str(cls.wsl_local_port),
                "--link-port",
                str(cls.link_port),
                "--artifact-spool-root",
                str(root / "wsl-spool"),
            ],
            cwd=ROOT,
            env=cls.environment,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        cls._wait_for_link()

    @classmethod
    def _start_win_node(cls) -> None:
        cls.win_process = subprocess.Popen(
            [
                sys.executable,
                str(WIN),
                "serve",
                "--registry",
                str(cls.win_registry),
                "--local-port",
                str(cls.win_local_port),
                "--link-port",
                str(cls.link_port),
                "--artifact-spool-root",
                str(cls.root / "win-spool"),
            ],
            cwd=ROOT,
            env=cls.environment,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

    @classmethod
    def _wait_for_link(cls) -> None:
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            try:
                result = local_registry_query(
                    "127.0.0.1",
                    cls.wsl_local_port,
                    "remote",
                    "describe",
                    {"id": "shared-browser"},
                )
                if result.get("id") == "shared-browser":
                    return
            except (OSError, BridgeError):
                time.sleep(0.1)
        raise RuntimeError("shared-backend test nodes did not connect")

    @classmethod
    def tearDownClass(cls) -> None:
        for name in ("wsl_process", "win_process"):
            process = getattr(cls, name, None)
            if process and process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
        cls.temp.cleanup()

    def _log_lines(self, name: str) -> list[str]:
        return (self.state / name).read_text(encoding="utf-8").splitlines()

    def _wait_pid_exit(self, pid: int, timeout: float = 10) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                os.kill(pid, 0)
            except OSError:
                return
            time.sleep(0.05)
        self.fail(f"owned child process {pid} did not exit")

    def _wait_event(self, prefix: str, start: int = 0, timeout: float = 10) -> str:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            rows = self._log_lines("events.log")
            for row in rows[start:]:
                if row.startswith(prefix):
                    return row
            time.sleep(0.05)
        self.fail(f"event {prefix!r} was not recorded")

    def test_01_concurrent_clients_share_spawn_and_route_ids(self) -> None:
        spawn_start = len(self._log_lines("spawns.log"))
        event_start = len(self._log_lines("events.log"))
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            clients = list(
                executor.map(
                    lambda _index: RawBridgeClient(
                        self.wsl_local_port, "shared-browser"
                    ),
                    range(2),
                )
            )
        first, second = clients
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
                initialized = list(
                    executor.map(
                        lambda client: client.initialize(1),
                        clients,
                    )
                )
            self.assertTrue(all("result" in item for item in initialized))
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
                one = executor.submit(
                    first.call, "", "echo", {"value": "first", "delay": 0.2}
                )
                two = executor.submit(
                    second.call, "", "echo", {"value": "second"}
                )
                first_result = one.result(timeout=10)
                second_result = two.result(timeout=10)
            first_content = first_result["result"]["structuredContent"]
            second_content = second_result["result"]["structuredContent"]
            self.assertEqual(first_result["id"], "")
            self.assertEqual(second_result["id"], "")
            self.assertEqual(first_content["value"], "first")
            self.assertEqual(second_content["value"], "second")
            self.assertEqual(first_content["backendPid"], second_content["backendPid"])
            self.assertEqual(
                len(self._log_lines("spawns.log")) - spawn_start,
                1,
            )
            self.assertEqual(
                sum(
                    row == "initialize"
                    for row in self._log_lines("events.log")[event_start:]
                ),
                1,
            )

            first.send(
                {
                    "jsonrpc": "2.0",
                    "id": 9,
                    "method": "tools/call",
                    "params": {"name": "server_roundtrip", "arguments": {}},
                }
            )
            nested = first.receive()
            self.assertEqual(nested["method"], "sampling/createMessage")
            first.send(
                {
                    "jsonrpc": "2.0",
                    "id": nested["id"],
                    "result": {"model": "fixture"},
                }
            )
            final = first.receive()
            self.assertEqual(final["id"], 9)
            self.assertEqual(
                final["result"]["structuredContent"]["nestedResult"],
                {"model": "fixture"},
            )

            first.send(
                {
                    "jsonrpc": "2.0",
                    "id": 0,
                    "method": "tools/call",
                    "params": {
                        "name": "echo",
                        "arguments": {"value": "progress"},
                        "_meta": {"progressToken": "client-token"},
                    },
                }
            )
            progress = first.receive()
            self.assertEqual(progress["method"], "notifications/progress")
            self.assertEqual(progress["params"]["progressToken"], "client-token")
            progressed = first.receive()
            self.assertEqual(progressed["id"], 0)

            cancellation_start = len(self._log_lines("events.log"))
            first.send(
                {
                    "jsonrpc": "2.0",
                    "id": 55,
                    "method": "tools/call",
                    "params": {
                        "name": "echo",
                        "arguments": {"value": "cancelled", "delay": 0.2},
                    },
                }
            )
            self._wait_event("call:echo:", cancellation_start)
            first.send(
                {
                    "jsonrpc": "2.0",
                    "method": "notifications/cancelled",
                    "params": {"requestId": 55, "reason": "test"},
                }
            )
            cancelled_response = first.receive()
            self.assertEqual(cancelled_response["id"], 55)
            cancellation = self._wait_event("cancel:", cancellation_start)
            self.assertIn("cancel:bridge:", cancellation)
            self.assertNotEqual(cancellation, "cancel:55")

            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
                success_future = executor.submit(
                    first.call, 77, "echo", {"value": "not an error"}
                )
                error_future = executor.submit(second.call, 77, "fail", {})
                success = success_future.result(timeout=10)
                failure = error_future.result(timeout=10)
            self.assertEqual(success["result"]["structuredContent"]["value"], "not an error")
            self.assertEqual(failure["error"]["code"], -32042)
        finally:
            first.close()
            second.close()
        backend_pid = int(self._log_lines("spawns.log")[-1])
        self._wait_pid_exit(backend_pid)

    def test_02_browser_lease_busy_release_and_disconnect_cleanup(self) -> None:
        first = RawBridgeClient(self.wsl_local_port, "shared-browser")
        second = RawBridgeClient(self.wsl_local_port, "shared-browser")
        first.initialize()
        second.initialize()
        try:
            started = first.call(2, "browser_start")
            first_child = started["result"]["structuredContent"]["childPid"]
            busy = second.call(2, "browser_start")
            self.assertTrue(busy["result"]["isError"])
            self.assertEqual(
                busy["result"]["structuredContent"]["error"]["code"],
                "client_lease_busy",
            )
            fixed = second.call(3, "view_change")
            self.assertEqual(
                fixed["result"]["structuredContent"]["error"]["code"],
                "shared_view_fixed",
            )
            released = first.call(4, "browser_session", {"action": "release"})
            self.assertTrue(
                released["result"]["structuredContent"]["profileReleased"]
            )
            self._wait_pid_exit(first_child)
            second_started = second.call(5, "browser_start")
            second_child = second_started["result"]["structuredContent"]["childPid"]
            second.close()
            self._wait_pid_exit(second_child)
            reacquired = first.call(6, "browser_start")
            third_child = reacquired["result"]["structuredContent"]["childPid"]
            first.call(7, "browser_session", {"action": "release"})
            self._wait_pid_exit(third_child)
        finally:
            first.close()
            second.close()

    def test_03_backend_crash_cleans_tree_and_other_target_survives(self) -> None:
        independent_before = invoke_proxy(
            WSL,
            self.wsl_local_port,
            "other-mcp",
            messages=rpc_messages("before crash"),
        )
        self.assertEqual(
            independent_before[2]["result"]["structuredContent"]["servedBy"],
            "independent-fixture",
        )
        client = RawBridgeClient(self.wsl_local_port, "shared-browser")
        client.initialize()
        started = client.call(2, "browser_start")
        child_pid = started["result"]["structuredContent"]["childPid"]
        client.send(
            {
                "jsonrpc": "2.0",
                "id": 3,
                "method": "tools/call",
                "params": {"name": "crash", "arguments": {}},
            }
        )
        with self.assertRaises((EOFError, OSError)):
            client.receive()
        client.close()
        self._wait_pid_exit(child_pid)

        replacement = RawBridgeClient(self.wsl_local_port, "shared-browser")
        try:
            replacement.initialize()
            result = replacement.call(2, "echo", {"value": "after crash"})
            self.assertEqual(
                result["result"]["structuredContent"]["value"], "after crash"
            )
        finally:
            replacement.close()
        independent_after = invoke_proxy(
            WSL,
            self.wsl_local_port,
            "other-mcp",
            messages=rpc_messages("after crash"),
        )
        self.assertEqual(
            independent_after[2]["result"]["structuredContent"]["value"],
            "after crash",
        )

    def test_04_bridge_restart_has_no_generation_overlap(self) -> None:
        client = RawBridgeClient(self.wsl_local_port, "shared-browser")
        client.initialize()
        started = client.call(2, "browser_start")
        child_pid = started["result"]["structuredContent"]["childPid"]
        old_backend_pid = int(self._log_lines("spawns.log")[-1])
        event_start = len(self._log_lines("events.log"))
        self.win_process.terminate()
        self.win_process.wait(timeout=15)
        self._wait_pid_exit(child_pid)
        self._wait_pid_exit(old_backend_pid)
        client.close()

        self._start_win_node()
        self._wait_for_link()
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            clients = list(
                executor.map(
                    lambda _index: RawBridgeClient(
                        self.wsl_local_port, "shared-browser"
                    ),
                    range(8),
                )
            )
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
                results = list(executor.map(lambda item: item.initialize(), clients))
            self.assertTrue(all("result" in item for item in results))
            with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
                echoes = list(
                    executor.map(
                        lambda pair: pair[1].call(
                            2, "echo", {"value": pair[0]}
                        ),
                        enumerate(clients),
                    )
                )
            backend_pids = {
                item["result"]["structuredContent"]["backendPid"]
                for item in echoes
            }
            self.assertEqual(len(backend_pids), 1)
            self.assertNotIn(old_backend_pid, backend_pids)
        finally:
            for item in clients:
                item.close()
        rows = self._log_lines("events.log")[event_start:]
        old_exit = next(
            index
            for index, row in enumerate(rows)
            if row.startswith(f"backend-exit:{old_backend_pid}:")
        )
        new_start = next(
            index
            for index, row in enumerate(rows)
            if row.startswith("backend-start:") and f":{old_backend_pid}:" not in row
        )
        self.assertLess(old_exit, new_start)

    def test_05_repeated_concurrent_connects_never_overlap_generations(self) -> None:
        observed_pids: list[int] = []
        for wave in range(6):
            with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
                clients = list(
                    executor.map(
                        lambda _index: RawBridgeClient(
                            self.wsl_local_port, "shared-browser"
                        ),
                        range(6),
                    )
                )
            try:
                with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
                    list(executor.map(lambda item: item.initialize(), clients))
                with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
                    responses = list(
                        executor.map(
                            lambda pair: pair[1].call(
                                wave,
                                "echo",
                                {"value": f"{wave}:{pair[0]}"},
                            ),
                            enumerate(clients),
                        )
                    )
                pids = {
                    response["result"]["structuredContent"]["backendPid"]
                    for response in responses
                }
                self.assertEqual(len(pids), 1)
                observed_pids.append(next(iter(pids)))
            finally:
                with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
                    list(executor.map(lambda item: item.close(), clients))
        for pid in set(observed_pids):
            self._wait_pid_exit(pid)
        rows = self._log_lines("events.log")
        intervals: list[tuple[int, int, int]] = []
        for pid in dict.fromkeys(observed_pids):
            start = next(
                index
                for index, row in enumerate(rows)
                if row.startswith(f"backend-start:{pid}:")
            )
            exit_index = next(
                index
                for index, row in enumerate(rows)
                if index > start and row.startswith(f"backend-exit:{pid}:")
            )
            intervals.append((start, exit_index, pid))
        intervals.sort()
        for previous, current in zip(intervals, intervals[1:]):
            self.assertLess(
                previous[1],
                current[0],
                f"backend generations overlapped: {previous[2]} and {current[2]}",
            )

    def test_06_final_response_is_drained_before_backend_exit(self) -> None:
        client = RawBridgeClient(self.wsl_local_port, "shared-browser")
        client.initialize()
        response = client.call(2, "exit_after_response")
        self.assertTrue(response["result"]["structuredContent"]["finalResponse"])
        with self.assertRaises((EOFError, OSError)):
            client.receive()
        client.close()

        replacement = RawBridgeClient(self.wsl_local_port, "shared-browser")
        try:
            replacement.initialize()
            echoed = replacement.call(2, "echo", {"value": "replacement"})
            self.assertEqual(
                echoed["result"]["structuredContent"]["value"], "replacement"
            )
        finally:
            replacement.close()


class BidirectionalIntegrationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.temp = tempfile.TemporaryDirectory()
        temp = Path(cls.temp.name)
        cls.win_registry = temp / "win.sqlite3"
        cls.wsl_registry = temp / "wsl.sqlite3"
        cls.win_workspace = temp / "win-workspace"
        cls.wsl_workspace = temp / "wsl-workspace"
        cls.win_spool = temp / "win-spool"
        cls.wsl_spool = temp / "wsl-spool"
        cls.win_workspace.mkdir()
        cls.wsl_workspace.mkdir()
        write_registry(
            cls.win_registry,
            "win-echo",
            "windows-fixture-mcp",
            multi_process_allowed=True,
        )
        write_registry(
            cls.wsl_registry,
            "wsl-echo",
            "wsl-fixture-mcp",
            multi_process_allowed=True,
        )
        cls.link_port = free_port()
        cls.win_local_port = free_port()
        cls.wsl_local_port = free_port()
        environment = os.environ.copy()
        environment["PYTHONDONTWRITEBYTECODE"] = "1"
        cls.win_process = subprocess.Popen(
            [
                sys.executable,
                str(WIN),
                "serve",
                "--registry",
                str(cls.win_registry),
                "--local-port",
                str(cls.win_local_port),
                "--link-port",
                str(cls.link_port),
                "--allow-artifact-root",
                str(cls.win_workspace),
                "--artifact-spool-root",
                str(cls.win_spool),
            ],
            cwd=ROOT,
            env=environment,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        cls.wsl_process = subprocess.Popen(
            [
                sys.executable,
                str(WSL),
                "serve",
                "--registry",
                str(cls.wsl_registry),
                "--local-port",
                str(cls.wsl_local_port),
                "--link-port",
                str(cls.link_port),
                "--allow-artifact-root",
                str(cls.wsl_workspace),
                "--artifact-spool-root",
                str(cls.wsl_spool),
            ],
            cwd=ROOT,
            env=environment,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            try:
                result = local_registry_query(
                    "127.0.0.1",
                    cls.wsl_local_port,
                    "remote",
                    "describe",
                    {"id": "win-echo"},
                )
                if result.get("id") == "win-echo":
                    return
            except (OSError, BridgeError):
                time.sleep(0.1)
        cls.tearDownClass()
        raise RuntimeError("bridge nodes did not establish their peer link")

    @classmethod
    def tearDownClass(cls) -> None:
        for name in ("wsl_process", "win_process"):
            process = getattr(cls, name, None)
            if process and process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
        temp = getattr(cls, "temp", None)
        if temp:
            temp.cleanup()

    def test_wsl_agent_calls_windows_mcp(self) -> None:
        responses = invoke_proxy(WSL, self.wsl_local_port, "win-echo")
        self.assertEqual(responses[0]["result"]["serverInfo"]["name"], "windows-fixture-mcp")
        self.assertEqual(responses[2]["result"]["structuredContent"]["servedBy"], "windows-fixture-mcp")

    def test_windows_agent_calls_wsl_mcp_over_same_link(self) -> None:
        responses = invoke_proxy(WIN, self.win_local_port, "wsl-echo")
        self.assertEqual(responses[0]["result"]["serverInfo"]["name"], "wsl-fixture-mcp")
        self.assertEqual(responses[2]["result"]["structuredContent"]["servedBy"], "wsl-fixture-mcp")

    def test_proxy_exits_when_remote_mcp_closes_before_client_stdin(self) -> None:
        process = subprocess.Popen(
            [
                sys.executable,
                str(WSL),
                "connect",
                "win-echo",
                "--local-port",
                str(self.wsl_local_port),
            ],
            text=True,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=ROOT,
        )
        try:
            assert process.stdin is not None
            process.stdin.write(rpc_messages("remote closes first"))
            process.stdin.flush()
            process.wait(timeout=10)
            assert process.stdout is not None
            assert process.stderr is not None
            responses = [json.loads(line) for line in process.stdout.read().splitlines()]
            self.assertEqual(process.stderr.read(), "")
            self.assertEqual(process.returncode, 0)
            self.assertEqual(len(responses), 3)
            self.assertEqual(
                responses[2]["result"]["structuredContent"]["value"],
                "remote closes first",
            )
        finally:
            if process.stdin is not None:
                process.stdin.close()
            if process.stdout is not None:
                process.stdout.close()
            if process.stderr is not None:
                process.stderr.close()
            if process.poll() is None:
                process.kill()
                process.wait(timeout=5)

    def test_path_looking_business_result_never_triggers_artifact_delivery(self) -> None:
        before = {path.relative_to(self.wsl_workspace) for path in self.wsl_workspace.rglob("*")}
        suspicious = [
            "/etc/passwd",
            r"C:\\Users\\example\\secret.txt",
            r"\\server\\share\\file.bin",
            "file:///private/result.step",
        ]
        responses = invoke_proxy(
            WSL,
            self.wsl_local_port,
            "win-echo",
            messages=rpc_messages(suspicious),
            artifact_inbox=self.wsl_workspace,
        )
        after = {path.relative_to(self.wsl_workspace) for path in self.wsl_workspace.rglob("*")}
        self.assertEqual(responses[2]["result"]["structuredContent"]["value"], suspicious)
        self.assertEqual(after, before)

    def test_mcp_message_larger_than_one_bridge_frame_remains_transparent(self) -> None:
        value = "x" * 600000
        responses = invoke_proxy(
            WSL,
            self.wsl_local_port,
            "win-echo",
            messages=rpc_messages(value),
        )
        self.assertEqual(responses[2]["result"]["structuredContent"]["value"], value)

    def test_windows_mcp_pushes_artifact_into_wsl_workspace(self) -> None:
        content = "artifact delivered from Windows-role MCP\n" * 6000
        responses = invoke_proxy(
            WSL,
            self.wsl_local_port,
            "win-echo",
            messages=artifact_rpc_messages(content),
            artifact_inbox=self.wsl_workspace,
        )
        artifact = responses[1]["result"]["content"][0]
        local_path = Path(artifact["_meta"]["io.win-wsl-mcp-bridge/artifact"]["localPath"])
        self.assertTrue(local_path.is_relative_to(self.wsl_workspace))
        self.assertEqual(local_path.read_text(encoding="utf-8"), content)
        self.assertEqual(
            artifact["_meta"]["io.win-wsl-mcp-bridge/artifact"]["sha256"],
            hashlib.sha256(content.encode("utf-8")).hexdigest(),
        )
        self.assertEqual(artifact["type"], "resource_link")
        self.assertEqual(artifact["mimeType"], "text/plain")
        self.assertNotIn("spool", json.dumps(responses))

    def test_wsl_mcp_pushes_artifact_into_windows_workspace_on_same_link(self) -> None:
        content = "artifact delivered from WSL-role MCP"
        responses = invoke_proxy(
            WIN,
            self.win_local_port,
            "wsl-echo",
            messages=artifact_rpc_messages(content),
            artifact_inbox=self.win_workspace,
        )
        artifact = responses[1]["result"]["content"][0]
        local_path = Path(artifact["_meta"]["io.win-wsl-mcp-bridge/artifact"]["localPath"])
        self.assertTrue(local_path.is_relative_to(self.win_workspace))
        self.assertEqual(local_path.read_text(encoding="utf-8"), content)

    def test_bidirectional_artifacts_and_tool_call_share_one_link_concurrently(self) -> None:
        windows_content = "concurrent Windows artifact"
        wsl_content = "concurrent WSL artifact"
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            windows_artifact_future = executor.submit(
                invoke_proxy,
                WSL,
                self.wsl_local_port,
                "win-echo",
                messages=artifact_rpc_messages(windows_content),
                artifact_inbox=self.wsl_workspace,
            )
            wsl_artifact_future = executor.submit(
                invoke_proxy,
                WIN,
                self.win_local_port,
                "wsl-echo",
                messages=artifact_rpc_messages(wsl_content),
                artifact_inbox=self.win_workspace,
            )
            echo_future = executor.submit(
                invoke_proxy,
                WSL,
                self.wsl_local_port,
                "win-echo",
                messages=rpc_messages("concurrent echo"),
            )
            windows_responses = windows_artifact_future.result(timeout=30)
            wsl_responses = wsl_artifact_future.result(timeout=30)
            echo_responses = echo_future.result(timeout=30)
        windows_path = Path(
            windows_responses[1]["result"]["content"][0]["_meta"]
            ["io.win-wsl-mcp-bridge/artifact"]["localPath"]
        )
        wsl_path = Path(
            wsl_responses[1]["result"]["content"][0]["_meta"]
            ["io.win-wsl-mcp-bridge/artifact"]["localPath"]
        )
        self.assertEqual(windows_path.read_text(encoding="utf-8"), windows_content)
        self.assertEqual(wsl_path.read_text(encoding="utf-8"), wsl_content)
        self.assertEqual(
            echo_responses[2]["result"]["structuredContent"]["value"],
            "concurrent echo",
        )

    def test_artifact_publish_fails_closed_without_authorized_inbox(self) -> None:
        before = {path.relative_to(self.wsl_workspace) for path in self.wsl_workspace.rglob("*")}
        responses = invoke_proxy(
            WSL,
            self.wsl_local_port,
            "win-echo",
            messages=artifact_rpc_messages("must not be delivered"),
        )
        after = {path.relative_to(self.wsl_workspace) for path in self.wsl_workspace.rglob("*")}
        self.assertIn("error", responses[1])
        self.assertEqual(after, before)

    def test_unknown_target_is_rejected_before_process_start(self) -> None:
        process = subprocess.run(
            [
                sys.executable,
                str(WSL),
                "connect",
                "not-registered",
                "--local-port",
                str(self.wsl_local_port),
            ],
            input="",
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=ROOT,
            timeout=20,
            check=False,
        )
        self.assertEqual(process.returncode, 1)
        self.assertEqual(process.stdout, "")
        self.assertIn("unknown registry id", process.stderr)

    def test_registry_mcp_exposes_remote_summary_without_launch_data(self) -> None:
        messages = [
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {"protocolVersion": "2025-06-18", "capabilities": {}},
            },
            {
                "jsonrpc": "2.0",
                "method": "notifications/cancelled",
                "params": {"requestId": 999, "reason": "test notification"},
            },
            {"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}},
            {
                "jsonrpc": "2.0",
                "id": 3,
                "method": "tools/call",
                "params": {
                    "name": "bridge_registry_list",
                    "arguments": {},
                },
            },
        ]
        process = subprocess.run(
            [sys.executable, str(WSL), "registry-mcp", "--local-port", str(self.wsl_local_port)],
            input="".join(json.dumps(item) + "\n" for item in messages),
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=ROOT,
            timeout=20,
            check=True,
        )
        responses = [json.loads(line) for line in process.stdout.splitlines() if line]
        names = {tool["name"] for tool in responses[1]["result"]["tools"]}
        self.assertTrue(
            all(
                "scope" not in tool["inputSchema"].get("properties", {})
                for tool in responses[1]["result"]["tools"]
            )
        )
        self.assertEqual(
            names,
            {
                "bridge_registry_list",
                "bridge_registry_search",
                "bridge_registry_describe",
                "bridge_registry_status",
            },
        )
        rows = responses[2]["result"]["structuredContent"]["result"]
        self.assertEqual([row["id"] for row in rows], ["win-echo"])
        self.assertFalse(rows[0]["artifactDelivery"]["agentFetchRequired"])
        self.assertNotIn("wsl-echo", {row["id"] for row in rows})
        serialized = json.dumps(rows)
        for private in ("command", "args", "cwd", "env", "token", "must-not-leak", str(FIXTURE)):
            self.assertNotIn(private, serialized)


if __name__ == "__main__":
    unittest.main(verbosity=2)
