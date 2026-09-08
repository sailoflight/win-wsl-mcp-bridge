#!/usr/bin/env python3
"""Offline integration tests for the bidirectional bridge prototype."""

from __future__ import annotations

import asyncio
import base64
import concurrent.futures
import hashlib
import json
import os
import select
import signal
import socket
import sqlite3
import stat
import subprocess
import sys
import tempfile
import threading
import time
import tomllib
import unittest
import zipfile
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from connector_core import ConnectorError, load_engine
from connector_engine import (
    CORE_VERSION,
    ENGINE_NAME,
    ENGINE_VERSION,
    Recovery,
    error_payload,
    make_recovery,
)

from bridge_runtime import (
    BridgeError,
    JsonRpcError,
    BridgeNode,
    EventJournal,
    ArtifactReceiveState,
    ArtifactTransferWaiter,
    Registry,
    SharedBackend,
    StreamState,
    _is_loopback,
    _event_journal_path,
    default_registry_path,
    local_registry_query,
    compatibility_mcp,
    _json_bytes,
    proxy_stdio,
    publish_artifact,
    stage_input,
    ProjectionDatabase,
    _looks_bridge_owned,
    default_projection_path,
    projection_enroll,
    projection_reconcile,
    projection_scan_candidates,
    projection_status,
    projection_sync_peer,
    projection_unenroll,
    projection_watch,
    _parse_capability_index,
    capability_index_dedupe,
    capability_index_load,
    capability_index_search,
    capability_install_plan,
    _control_tools,
    _control_mcp_dispatch,
    _desired_entry_descriptors,
    build_parser,
    CONTROL_INSTRUCTIONS,
    negotiate_mcp_protocol_version,
)

WIN = ROOT / "win-bridge-mcp" / "bridge.py"
WSL = ROOT / "wsl-bridge-mcp" / "bridge.py"
FIXTURE = ROOT / "tests" / "fixtures" / "fixture_mcp.py"
SHARED_FIXTURE = ROOT / "tests" / "fixtures" / "shared_fixture_mcp.py"


_ALLOCATED_TEST_PORTS: set[int] = set()
_ALLOCATED_TEST_PORTS_LOCK = threading.Lock()


def free_port() -> int:
    # A bind-to-zero probe releases the socket before the subprocess binds it.
    # Never hand the same ephemeral port to two harness roles in this process;
    # otherwise local-control and peer-link listeners can race intermittently.
    while True:
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = int(sock.getsockname()[1])
        with _ALLOCATED_TEST_PORTS_LOCK:
            if port not in _ALLOCATED_TEST_PORTS:
                _ALLOCATED_TEST_PORTS.add(port)
                return port


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
                        "inputDelivery": {
                            "enabled": multi_process_allowed,
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
        self.stream_id = reply.get("stream") if isinstance(reply.get("stream"), str) else None

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

    def initialize(
        self,
        request_id: object = 1,
        *,
        protocol_version: str = "2025-06-18",
        capabilities: dict | None = None,
        client_name: str = "raw-test",
    ) -> dict:
        response = self.request(
            request_id,
            "initialize",
            {
                "protocolVersion": protocol_version,
                "capabilities": capabilities or {},
                "clientInfo": {"name": client_name, "version": "1"},
            },
        )
        if "result" in response:
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


class FdLineReader:
    """Line reader over a binary pipe using select, so tests never hang."""

    def __init__(self, raw_stream: object) -> None:
        self.fd = raw_stream.fileno()  # type: ignore[attr-defined]
        self.buffer = bytearray()

    def read_line(self, timeout: float = 10.0) -> bytes | None:
        deadline = time.monotonic() + timeout
        while b"\n" not in self.buffer:
            if time.monotonic() >= deadline:
                return None
            ready, _, _ = select.select([self.fd], [], [], 0.2)
            if not ready:
                continue
            try:
                chunk = os.read(self.fd, 65536)
            except OSError:
                chunk = b""
            if not chunk:
                return None
            self.buffer.extend(chunk)
        index = self.buffer.find(b"\n")
        line = bytes(self.buffer[: index + 1])
        del self.buffer[: index + 1]
        return line

    def drain(self, timeout: float = 1.0) -> bytes:
        out = bytearray()
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            ready, _, _ = select.select([self.fd], [], [], 0.1)
            if not ready:
                break
            try:
                chunk = os.read(self.fd, 65536)
            except OSError:
                break
            if not chunk:
                break
            out.extend(chunk)
        return bytes(out)


def _spawn_connect(local_port: int, *, extra_env: dict[str, str] | None = None) -> subprocess.Popen:
    environment = os.environ.copy()
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    if extra_env:
        environment.update(extra_env)
    return subprocess.Popen(
        [
            sys.executable,
            str(WSL),
            "connect",
            "fake-target",
            "--local-port",
            str(local_port),
        ],
        cwd=ROOT,
        env=environment,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


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
                self.assertEqual(connection.execute("PRAGMA user_version").fetchone()[0], Registry.SCHEMA_VERSION)
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

    def test_registry_v1_migrates_to_artifact_input_schema_v3(self) -> None:
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
            self.assertNotIn("inputDelivery", registry.public("legacy"))
            with sqlite3.connect(database) as connection:
                self.assertEqual(connection.execute("PRAGMA user_version").fetchone()[0], Registry.SCHEMA_VERSION)

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
        with self.assertRaisesRegex(BridgeError, "must not set bridge artifact"):
            Registry._validate_manifest_row(
                {
                    "id": "reserved-input-env-case",
                    "command": "python",
                    "env": {"WIN_WSL_MCP_BRIDGE_INPUT_STAGE": "shadow"},
                }
            )

    def test_registry_rejects_input_delivery_on_shared_backends_and_bad_values(self) -> None:
        with self.assertRaisesRegex(BridgeError, "requires a dedicated business process"):
            Registry._validate_manifest_row(
                {
                    "id": "shared-input",
                    "command": "python",
                    "process": {"multiProcessAllowed": False},
                    "inputDelivery": {"enabled": True, "maxBytes": 1024},
                }
            )
        with self.assertRaisesRegex(BridgeError, "inputDelivery.maxBytes"):
            Registry._validate_manifest_row(
                {
                    "id": "bad-input-size",
                    "command": "python",
                    "inputDelivery": {"enabled": True, "maxBytes": -1},
                }
            )
        with self.assertRaisesRegex(BridgeError, "inputDelivery must be an object"):
            Registry._validate_manifest_row(
                {"id": "bad-input-type", "command": "python", "inputDelivery": "yes"}
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

    def test_compatibility_mcp_has_constant_two_tool_surface(self) -> None:
        with mock.patch("bridge_runtime._CompatibilitySession._connect"):
            with mock.patch.object(sys, "stdin") as stdin, mock.patch.object(sys, "stdout") as stdout:
                stdin.buffer.readline.side_effect = [
                    _json_bytes(
                        {
                            "jsonrpc": "2.0",
                            "id": 1,
                            "method": "tools/list",
                            "params": {},
                        }
                    )
                    + b"\n",
                    b"",
                ]
                stdout.write = mock.Mock()
                compatibility_mcp("127.0.0.1", 1, "sample")
                response = json.loads(stdout.write.call_args_list[0].args[0])
                self.assertEqual(
                    [tool["name"] for tool in response["result"]["tools"]],
                    ["bridge_capabilities", "bridge_call"],
                )

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

    def test_input_source_must_stay_under_operator_root_and_be_a_plain_file(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            database = root / "registry.sqlite3"
            allowed = root / "allowed"
            outside = root / "outside"
            allowed.mkdir()
            outside.mkdir()
            write_registry(database, "input-root", "Input Root")
            node = BridgeNode(
                side="wsl",
                registry=Registry(database),
                local_host="127.0.0.1",
                local_port=free_port(),
                link_mode="connect",
                link_host="127.0.0.1",
                link_port=free_port(),
                allowed_artifact_roots=[allowed],
                artifact_spool_root=root / "spool",
            )
            inside = allowed / "payload.txt"
            inside.write_bytes(b"payload")
            self.assertEqual(
                node._validate_input_source(str(inside)), inside.resolve()
            )
            with self.assertRaisesRegex(BridgeError, "outside Operator-authorized"):
                (outside / "payload.txt").write_bytes(b"payload")
                node._validate_input_source(str(outside / "payload.txt"))
            with self.assertRaisesRegex(BridgeError, "must not be a symbolic link"):
                target = allowed / "real-target.txt"
                target.write_bytes(b"x")
                link = allowed / "linked.txt"
                try:
                    link.symlink_to(target)
                except (OSError, NotImplementedError):
                    self.skipTest("symlinks unavailable")
                node._validate_input_source(str(link))
            with self.assertRaisesRegex(BridgeError, "existing local absolute file"):
                node._validate_input_source(str(allowed / "missing.txt"))
            with self.assertRaisesRegex(BridgeError, "existing local absolute file"):
                (allowed / "a-directory").mkdir()
                node._validate_input_source(str(allowed / "a-directory"))

    def test_input_descriptor_rewrite_is_exact_and_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            database = root / "registry.sqlite3"
            workspace = root / "workspace"
            stage = root / "input-stage"
            workspace.mkdir()
            stage.mkdir()
            write_registry(database, "rewrite-test", "Rewrite Test")
            node = BridgeNode(
                side="win",
                registry=Registry(database),
                local_host="127.0.0.1",
                local_port=free_port(),
                link_mode="listen",
                link_host="127.0.0.1",
                link_port=free_port(),
                allowed_artifact_roots=[workspace],
            )
            staged = stage / "payload.txt"
            staged.write_bytes(b"payload")
            stream = StreamState(
                stream_id="rewrite-stream",
                input_stage=stage,
                input_handles={"input-aaaa": staged},
            )
            descriptor = f"bridge-input://input-aaaa"
            message = {
                "jsonrpc": "2.0",
                "id": 7,
                "method": "tools/call",
                "params": {
                    "name": "read_input",
                    "arguments": {"path": descriptor},
                },
            }
            line = json.dumps(message, separators=(",", ":")).encode("utf-8")
            rewritten, error = node._rewrite_input_descriptor_line(stream, line)
            self.assertIsNone(error)
            self.assertEqual(
                json.loads(rewritten)["params"]["arguments"]["path"], str(staged)
            )
            # Ordinary arguments stay byte-for-byte identical.
            ordinary = (
                b'{"jsonrpc":"2.0","id":8,"method":"tools/call",'
                b'"params":{"name":"echo","arguments":{"value":{" a " : 1}}}}'
            )
            self.assertEqual(
                node._rewrite_input_descriptor_line(stream, ordinary)[0], ordinary
            )
            # A foreign/expired handle fails closed without reaching the MCP.
            foreign_line = json.dumps(
                {
                    "jsonrpc": "2.0",
                    "id": 9,
                    "method": "tools/call",
                    "params": {
                        "name": "read_input",
                        "arguments": {"path": "bridge-input://input-zzzz"},
                    },
                },
                separators=(",", ":"),
            ).encode("utf-8")
            rewritten, error = node._rewrite_input_descriptor_line(stream, foreign_line)
            self.assertIsNone(rewritten)
            self.assertIsNotNone(error)
            self.assertEqual(error["error"]["code"], -32602)
            # A descriptor literal embedded in longer text is never rewritten.
            embedded = json.dumps(
                {
                    "jsonrpc": "2.0",
                    "id": 10,
                    "method": "tools/call",
                    "params": {
                        "name": "echo",
                        "arguments": {"value": f"see {descriptor} later"},
                    },
                },
                separators=(",", ":"),
            ).encode("utf-8")
            self.assertEqual(
                node._rewrite_input_descriptor_line(stream, embedded)[0], embedded
            )

    def test_input_begin_rejected_without_negotiated_stage(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            database = root / "registry.sqlite3"
            workspace = root / "workspace"
            workspace.mkdir()
            write_registry(database, "begin-input", "Begin Input")
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
            node.peer_artifact_inputs = True

            async def exercise() -> None:
                sent = []

                async def capture(_frame: dict) -> None:
                    sent.append(_frame)

                node._send_frame = capture  # type: ignore[method-assign]
                await node._handle_input_begin(
                    {
                        "type": "input_begin",
                        "stream": "missing-stream",
                        "input": "input-begin-rejected",
                        "name": "payload.bin",
                        "size": 3,
                        "sha256": hashlib.sha256(b"abc").hexdigest(),
                    }
                )
                self.assertEqual(node.receiving_inputs, {})
                self.assertEqual(
                    sent[0]["message"], "input delivery was rejected"
                )

            asyncio.run(exercise())

    def test_distribution_metadata_and_cli_version_are_consistent(self) -> None:
        project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
        self.assertEqual(project["project"]["dynamic"], ["version"])
        self.assertEqual(
            project["tool"]["setuptools"]["py-modules"],
            [
                "bridge_runtime",
                "bridge_publisher",
                "archive_profile",
                "streamable_http_stdio",
                "stdio_http_facade",
                "connector_core",
                "connector_engine",
                "bridge_protocol",
                "journal_maintenance",
                "journal_evidence",
                "legacy_modern_stdio",
                "stream_evidence",
            ],
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

    def test_cli_exposes_stage_input_and_stream_notice(self) -> None:
        for script in (WIN, WSL):
            stage_help = subprocess.run(
                [sys.executable, str(script), "stage-input", "--help"],
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=ROOT,
                timeout=20,
                check=True,
            )
            self.assertIn("--stream", stage_help.stdout)
            connect_help = subprocess.run(
                [sys.executable, str(script), "connect", "--help"],
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=ROOT,
                timeout=20,
                check=True,
            )
            self.assertIn("--print-stream-id", connect_help.stdout)

    def test_project_has_only_two_component_directories(self) -> None:
        directories = sorted(
            path.name
            for path in ROOT.iterdir()
            if path.is_dir()
            and not path.name.startswith(".")
            # Explicitly approved support directories and generated build outputs
            # are not additional runtime components.
            and path.name not in {"docs", "tests", "release-artifacts", "build", "dist"}
            and not path.name.endswith(".egg-info")
        )
        self.assertEqual(directories, ["win-bridge-mcp", "wsl-bridge-mcp"])


class SharedProtocolNegotiationUnitTest(unittest.TestCase):
    """Generic MCP initialize negotiation decisions and journal rows."""

    def test_verified_logical_revisions_are_accepted_verbatim(self) -> None:
        # ``2025-11-25`` is a verified *logical* revision for the shared
        # surface: the JSON-RPC/tools core it shares with the normalized
        # ``2025-06-18`` backend is wire-compatible, and its optional additions
        # (server tasks, tool ``execution.taskSupport``, task-augmented
        # sampling/elicitation, URL elicitation) are only used when a party
        # advertises them, which the Bridge and the normalized backend never
        # do.  A future well-formed revision still downgrades (next test)
        # instead of receiving a fabricated echo.
        for version in ("2025-11-25", "2025-06-18", "2025-03-26", "2024-11-05"):
            with self.subTest(version=version):
                self.assertEqual(
                    negotiate_mcp_protocol_version(version),
                    ("accepted", version),
                )

    def test_future_revisions_are_downgraded_not_falsely_claimed(self) -> None:
        # The rule is purely version-driven (not client-specific): a future
        # well-formed revision steps down to the newest verified revision that
        # does not exceed the request (now ``2025-11-25``).
        for requested, expected in (
            ("2026-01-01", "2025-11-25"),
            ("2025-12-01", "2025-11-25"),
        ):
            with self.subTest(requested=requested):
                self.assertEqual(
                    negotiate_mcp_protocol_version(requested),
                    ("downgraded", expected),
                )

    def test_malformed_or_unusable_versions_are_rejected(self) -> None:
        for requested in (
            "1999-01-01",      # well-formed date older than every supported revision
            "banana",          # not a protocol revision date
            "2025-11-25-rc1",  # malformed date
            "",
            "   ",
            None,
            5,
            [],
        ):
            with self.subTest(requested=requested):
                self.assertEqual(
                    negotiate_mcp_protocol_version(requested),
                    ("rejected", None),
                )

    def test_journal_rows_are_metadata_only_and_carry_versions(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            journal = EventJournal(root / "events.sqlite3", max_events=100)

            class _JournalNode:
                def __init__(self, side: str, store: object) -> None:
                    self.side = side
                    self.journal = store

            backend = SharedBackend(
                node=_JournalNode("win", journal),
                target="shared-browser",
                entry={"process": {}},
            )
            # A verified 2025-11-25 request journals ``accepted`` (its logical
            # session keeps 2025-11-25); only a future well-formed revision is
            # a downgrade, to the newest verified revision (2025-11-25).
            backend._journal_initialize_negotiation(
                "accepted", "2025-11-25", "2025-11-25"
            )
            backend._journal_initialize_negotiation(
                "downgraded", "2026-01-01", "2025-11-25"
            )
            backend._journal_initialize_negotiation("accepted", "2025-06-18", "2025-06-18")
            backend._journal_initialize_negotiation("rejected", "1999-01-01", None)
            backend._journal_initialize_negotiation("rejected", None, None)
            rows = journal.recent(20)
            by_requested = {row["metadata"]["requestedVersion"]: row for row in rows}
            accepted_latest = by_requested["2025-11-25"]
            self.assertEqual(accepted_latest["category"], "shared-initialize")
            self.assertEqual(accepted_latest["outcome"], "accepted")
            self.assertEqual(accepted_latest["target"], "shared-browser")
            self.assertEqual(accepted_latest["side"], "win")
            self.assertEqual(
                accepted_latest["metadata"],
                {
                    "requestedVersion": "2025-11-25",
                    "negotiatedVersion": "2025-11-25",
                    "backendVersion": "2025-06-18",
                },
            )
            downgraded = by_requested["2026-01-01"]
            self.assertEqual(downgraded["outcome"], "downgraded")
            self.assertEqual(
                downgraded["metadata"],
                {
                    "requestedVersion": "2026-01-01",
                    "negotiatedVersion": "2025-11-25",
                    "backendVersion": "2025-06-18",
                },
            )
            accepted = by_requested["2025-06-18"]
            self.assertEqual(accepted["outcome"], "accepted")
            self.assertEqual(accepted["metadata"]["negotiatedVersion"], "2025-06-18")
            rejected = by_requested["1999-01-01"]
            self.assertEqual(rejected["outcome"], "rejected")
            self.assertIsNone(rejected["metadata"]["negotiatedVersion"])
            # Metadata-only: no payload column content and no trace session.
            with sqlite3.connect(journal.path) as connection:
                trace_count = connection.execute(
                    "SELECT COUNT(*) FROM traces"
                ).fetchone()[0]
            self.assertEqual(trace_count, 0)
            for row in rows:
                self.assertEqual(
                    set(row["metadata"]),
                    {"requestedVersion", "negotiatedVersion", "backendVersion"},
                )

    def test_missing_journal_is_a_silent_noop(self) -> None:
        class _BareNode:
            side = "win"

        backend = SharedBackend(
            node=_BareNode(), target="shared-browser", entry={"process": {}}
        )
        # A node with no journal must never raise on the negotiation path.
        backend._journal_initialize_negotiation("accepted", "2025-06-18", "2025-06-18")
        backend._journal_initialize_negotiation(
            "downgraded", "2026-01-01", "2025-11-25"
        )
        backend._journal_initialize_negotiation("rejected", "2025-13-99", None)

    def test_task_capabilities_never_substitute_for_flow_capabilities(self) -> None:
        # Under 2025-11-25, sampling/elicitation requests may only be
        # task-augmented toward a client that declared the matching base
        # capability in addition to the tasks leaf.  A task-only advertisement
        # (``capabilities.tasks`` with no ``sampling``/``elicitation``/``roots``)
        # must therefore never act as the base capability, or a backend request
        # could be falsely enabled toward a client that never declared the
        # flow.  The runtime routes on the exact base capability keys only.
        class _BareNode:
            side = "win"

        backend = SharedBackend(
            node=_BareNode(), target="shared-browser", entry={"process": {}}
        )

        class _CapabilityClient:
            def __init__(self, capabilities: dict) -> None:
                self.capabilities = capabilities

        tasks_only = {
            "tasks": {
                "list": {},
                "cancel": {},
                "requests": {
                    "sampling": {"createMessage": {}},
                    "elicitation": {"create": {}},
                },
            }
        }
        tasks_plus_base = {
            **tasks_only,
            "sampling": {},
            "elicitation": {},
            "roots": {"listChanged": True},
        }
        backend.clients["a"] = _CapabilityClient(tasks_only)
        backend.clients["b"] = _CapabilityClient(tasks_plus_base)
        backend.clients["c"] = _CapabilityClient({"sampling": {}})
        for method in ("sampling/createMessage", "elicitation/create", "roots/list"):
            with self.subTest(method=method):
                # tasks-only advertisement: nothing is routable.
                self.assertFalse(
                    backend._client_supports_backend_method("a", method)
                )
                # Only the declared base capability keys (not the tasks
                # umbrella) enable routing.
                self.assertTrue(
                    backend._client_supports_backend_method("b", method)
                )
                self.assertEqual(
                    backend._client_supports_backend_method("c", method),
                    method == "sampling/createMessage",
                )
        # The 2025-11-25 task-only namespace never maps to a client flow
        # capability that would substitute for sampling/elicitation/roots.
        self.assertIsNone(SharedBackend._required_client_capability("tasks/get"))
        self.assertIsNone(SharedBackend._required_client_capability("tasks/list"))
        self.assertEqual(
            SharedBackend._required_client_capability("sampling/createMessage"),
            "sampling",
        )
        self.assertEqual(
            SharedBackend._required_client_capability("elicitation/create"),
            "elicitation",
        )


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

    def test_00_compatibility_facade_discovers_and_calls_target(self) -> None:
        process = subprocess.Popen(
            [
                sys.executable,
                str(WSL),
                "compatibility-mcp",
                "shared-browser",
                "--local-port",
                str(self.wsl_local_port),
            ],
            cwd=ROOT,
            env=self.environment,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        assert process.stdin is not None and process.stdout is not None

        def exchange(message: dict) -> dict:
            process.stdin.write(json.dumps(message, separators=(",", ":")) + "\n")
            process.stdin.flush()
            line = process.stdout.readline()
            self.assertTrue(line)
            return json.loads(line)

        try:
            initialized = exchange(
                {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "initialize",
                    "params": {"protocolVersion": "2025-06-18", "capabilities": {}},
                }
            )
            self.assertIn("Synthetic shared-backend instructions.", initialized["result"]["instructions"])
            listed = exchange({"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}})
            self.assertEqual(
                [item["name"] for item in listed["result"]["tools"]],
                ["bridge_capabilities", "bridge_call"],
            )
            catalog = exchange(
                {
                    "jsonrpc": "2.0",
                    "id": 3,
                    "method": "tools/call",
                    "params": {"name": "bridge_capabilities", "arguments": {"tool": "echo"}},
                }
            )
            self.assertEqual(catalog["result"]["structuredContent"]["tools"][0]["name"], "echo")
            called = exchange(
                {
                    "jsonrpc": "2.0",
                    "id": 4,
                    "method": "tools/call",
                    "params": {
                        "name": "bridge_call",
                        "arguments": {"tool": "echo", "arguments": {"value": "compat"}},
                    },
                }
            )
            self.assertEqual(called["result"]["structuredContent"]["value"], "compat")
        finally:
            process.terminate()
            process.wait(timeout=10)
            for handle in (process.stdin, process.stdout, process.stderr):
                if handle is not None:
                    handle.close()

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
                        lambda client: client.initialize(
                            1, capabilities={"sampling": {}}
                        ),
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
                    row.startswith("initialize:")
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

    def test_02_heterogeneous_clients_virtualize_initialize_and_catalog(self) -> None:
        vectors = [
            (
                "dsh-like",
                "2025-06-18",
                {
                    "sampling": {},
                    "roots": {"listChanged": True},
                    "elicitation": {},
                },
            ),
            ("codex-like", "2025-03-26", {}),
        ]
        for order in (vectors, list(reversed(vectors))):
            spawn_start = len(self._log_lines("spawns.log"))
            event_start = len(self._log_lines("events.log"))
            clients = [
                RawBridgeClient(self.wsl_local_port, "shared-browser") for _ in order
            ]
            try:
                responses = [
                    client.initialize(
                        request_id=index + 10,
                        protocol_version=version,
                        capabilities=capabilities,
                        client_name=name,
                    )
                    for index, (client, (name, version, capabilities)) in enumerate(
                        zip(clients, order)
                    )
                ]
                self.assertTrue(all("result" in response for response in responses))
                self.assertEqual(
                    [response["result"]["protocolVersion"] for response in responses],
                    [item[1] for item in order],
                )
                self.assertTrue(
                    all(
                        response["result"]["instructions"]
                        == "Synthetic shared-backend instructions."
                        for response in responses
                    )
                )
                catalogs = []
                backend_pids = set()
                for index, client in enumerate(clients):
                    first_page = client.request(index + 20, "tools/list", {})
                    second_page = client.request(
                        index + 30,
                        "tools/list",
                        {"cursor": first_page["result"]["nextCursor"]},
                    )
                    catalog = first_page["result"]["tools"] + second_page["result"]["tools"]
                    catalogs.append([tool["name"] for tool in catalog])
                    echoed = client.call(index + 40, "echo", {"value": index})
                    version = order[index][1]
                    if version >= "2025-06-18":
                        # structuredContent exists from 2025-06-18 onward: the
                        # browser-era profile still carries the backend pid.
                        backend_pids.add(
                            echoed["result"]["structuredContent"]["backendPid"]
                        )
                    else:
                        # The 2025-03-26 profile predates structuredContent:
                        # the era projection strips it; content still round-trips.
                        self.assertNotIn("structuredContent", echoed["result"])
                        self.assertEqual(
                            json.loads(echoed["result"]["content"][0]["text"]), index
                        )
                self.assertTrue(catalogs[0])
                self.assertEqual(catalogs[0], catalogs[1])
                self.assertEqual(len(backend_pids), 1)
                self.assertEqual(
                    len(self._log_lines("spawns.log")) - spawn_start, 1
                )
                init_rows = [
                    row
                    for row in self._log_lines("events.log")[event_start:]
                    if row.startswith("initialize:")
                ]
                self.assertEqual(len(init_rows), 1)
                physical_params = json.loads(init_rows[0].split(":", 1)[1])
                self.assertEqual(
                    physical_params["protocolVersion"], "2025-11-25"
                )
                self.assertEqual(physical_params["capabilities"], {})
                self.assertEqual(
                    physical_params["clientInfo"]["name"],
                    "win-wsl-mcp-bridge-shared-backend",
                )
            finally:
                for client in clients:
                    client.close()
            self._wait_pid_exit(next(iter(backend_pids)))

    def test_03_unsupported_protocol_is_diagnostic_and_does_not_poison_backend(self) -> None:
        client = RawBridgeClient(self.wsl_local_port, "shared-browser")
        try:
            rejected = client.initialize(
                protocol_version="1999-01-01", client_name="unsupported"
            )
            self.assertEqual(rejected["error"]["code"], -32602)
            self.assertEqual(
                rejected["error"]["data"]["requestedProtocolVersion"],
                "1999-01-01",
            )
            self.assertIn(
                "2025-06-18",
                rejected["error"]["data"]["supportedProtocolVersions"],
            )
            accepted = client.initialize(
                request_id="supported",
                capabilities={"roots": {"listChanged": True}},
            )
            self.assertIn("result", accepted)
        finally:
            client.close()

    def test_04_backend_request_requires_advertised_client_capability(self) -> None:
        client = RawBridgeClient(self.wsl_local_port, "shared-browser")
        try:
            client.initialize(capabilities={})
            response = client.call(2, "server_roundtrip")
            nested = response["result"]["structuredContent"]["nestedError"]
            self.assertEqual(nested["code"], -32000)
            self.assertEqual(
                nested["data"]["requiredClientCapability"],
                "sampling",
            )
        finally:
            client.close()

    def test_05_browser_lease_busy_release_and_disconnect_cleanup(self) -> None:
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
        lost = client.receive()
        self.assertEqual(lost["id"], 3)
        self.assertEqual(lost["error"]["code"], -32001)
        self.assertTrue(lost["error"]["data"]["retryable"])
        self.assertTrue(lost["error"]["data"]["outcomeUnknown"])
        recovered = client.call(4, "echo", {"value": "after crash"})
        self.assertEqual(
            recovered["result"]["structuredContent"]["value"], "after crash"
        )
        client.close()
        self._wait_pid_exit(child_pid)
        backend_starts = [
            row for row in self._log_lines("events.log") if row.startswith("backend-start:")
        ]
        self.assertGreaterEqual(len(backend_starts), 2)
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

    def test_03b_stale_server_response_after_recovery_is_ignored(self) -> None:
        client = RawBridgeClient(self.wsl_local_port, "shared-browser")
        try:
            client.initialize(capabilities={"sampling": {}})
            # Generation zero is strictly older after initialize started the
            # first physical backend generation.
            old_generation = 0
            client.send(
                {
                    "jsonrpc": "2.0",
                    "id": f"bridge-server:{old_generation}:stale",
                    "result": {"role": "assistant"},
                }
            )
            result = client.call(99, "echo", {"value": "stream-still-open"})
            self.assertEqual(
                result["result"]["structuredContent"]["value"], "stream-still-open"
            )
        finally:
            client.close()

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
        notice = client.receive()
        self.assertEqual(notice["method"], "notifications/tools/list_changed")
        echoed = client.call(3, "echo", {"value": "replacement"})
        self.assertEqual(
            echoed["result"]["structuredContent"]["value"], "replacement"
        )
        client.close()


    def _journal_files(self) -> list[Path]:
        return [
            _event_journal_path(self.win_registry),
            _event_journal_path(self.wsl_registry),
        ]

    def _max_shared_initialize_seq(self) -> int:
        maximum = 0
        for path in self._journal_files():
            if not path.exists():
                continue
            try:
                with sqlite3.connect(path) as connection:
                    row = connection.execute(
                        "SELECT COALESCE(MAX(seq), 0) FROM events "
                        "WHERE category = 'shared-initialize'"
                    ).fetchone()
                    maximum = max(maximum, int(row[0]))
            except sqlite3.Error:
                continue
        return maximum

    def _shared_initialize_rows_since(self, after_seq: int) -> list[dict]:
        rows: list[dict] = []
        for path in self._journal_files():
            if not path.exists():
                continue
            try:
                with sqlite3.connect(path) as connection:
                    connection.row_factory = sqlite3.Row
                    for row in connection.execute(
                        "SELECT side, category, target, outcome, metadata_json "
                        "FROM events WHERE category = 'shared-initialize' AND seq > ? "
                        "ORDER BY seq ASC",
                        (after_seq,),
                    ):
                        rows.append(
                            {
                                "side": row["side"],
                                "category": row["category"],
                                "target": row["target"],
                                "outcome": row["outcome"],
                                "metadata": json.loads(row["metadata_json"]),
                            }
                        )
            except sqlite3.Error:
                continue
        return rows

    def test_20_dsh_sdk_2025_11_25_initialize_is_accepted_and_journaled(self) -> None:
        # The verified logical 2025-11-25 tools subset is accepted verbatim,
        # while the physical backend remains normalized at 2025-06-18. Mixed
        # older clients share the generation and continue safely.
        spawn_start = len(self._log_lines("spawns.log"))
        event_start = len(self._log_lines("events.log"))
        journal_start = self._max_shared_initialize_seq()
        clients = [
            RawBridgeClient(self.wsl_local_port, "shared-browser")
            for _index in range(4)
        ]
        backend_pid: int | None = None
        try:
            dsh_latest, dsh_verified, older, unsupported = clients
            accepted_latest = dsh_latest.initialize(
                request_id="init-dsh-2025-11-25",
                protocol_version="2025-11-25",
                capabilities={
                    "roots": {"listChanged": True},
                    "sampling": {},
                    "elicitation": {},
                },
                client_name="dsh-2025-11-25-shape",
            )
            self.assertIn("result", accepted_latest)
            result = accepted_latest["result"]
            self.assertEqual(result["protocolVersion"], "2025-11-25")
            self.assertNotIn("tasks", result["capabilities"])
            self.assertEqual(
                result["instructions"], "Synthetic shared-backend instructions."
            )
            self.assertIsInstance(result["capabilities"], dict)
            # Client continuation under the verified logical revision: the
            # catalog and a real backend round trip work on the same stream.
            listed = dsh_latest.request("init-dsh-list", "tools/list", {})
            self.assertTrue(listed["result"]["tools"])
            echoed = dsh_latest.call(
                "init-dsh-echo", "echo", {"value": "accepted-ok"}
            )
            self.assertEqual(
                echoed["result"]["structuredContent"]["value"], "accepted-ok"
            )
            backend_pid = echoed["result"]["structuredContent"]["backendPid"]

            accepted_new = dsh_verified.initialize(
                request_id="init-2025-06-18", client_name="dsh-like"
            )
            self.assertEqual(
                accepted_new["result"]["protocolVersion"], "2025-06-18"
            )
            accepted_old = older.initialize(
                request_id="init-2025-03-26",
                protocol_version="2025-03-26",
                client_name="codex-like",
            )
            self.assertEqual(
                accepted_old["result"]["protocolVersion"], "2025-03-26"
            )
            rejected = unsupported.initialize(
                request_id="init-1999-01-01",
                protocol_version="1999-01-01",
                client_name="unsupported",
            )
            self.assertEqual(rejected["error"]["code"], -32602)
            self.assertEqual(
                rejected["error"]["data"]["requestedProtocolVersion"],
                "1999-01-01",
            )
            self.assertIn(
                "2025-06-18",
                rejected["error"]["data"]["supportedProtocolVersions"],
            )

            # One physical backend generation for all four logical clients.  Its
            # initialize requests the canonical 2025-11-25 revision with the
            # Bridge-owned client profile; this fixture answers its own actual
            # 2025-06-18 revision (server-choose), which the bridge observes.
            self.assertEqual(len(self._log_lines("spawns.log")) - spawn_start, 1)
            init_rows = [
                row
                for row in self._log_lines("events.log")[event_start:]
                if row.startswith("initialize:")
            ]
            self.assertEqual(len(init_rows), 1)
            physical = json.loads(init_rows[0].split(":", 1)[1])
            self.assertEqual(physical["protocolVersion"], "2025-11-25")
            self.assertEqual(physical["capabilities"], {})
            self.assertEqual(
                physical["clientInfo"]["name"],
                "win-wsl-mcp-bridge-shared-backend",
            )

            # Metadata-only journal evidence for each negotiation outcome.
            rows = self._shared_initialize_rows_since(journal_start)
            by_key = {
                (row["outcome"], row["metadata"]["requestedVersion"]): row
                for row in rows
            }
            self.assertIn(("accepted", "2025-11-25"), by_key)
            accepted_latest_row = by_key[("accepted", "2025-11-25")]
            self.assertEqual(accepted_latest_row["target"], "shared-browser")
            self.assertEqual(
                accepted_latest_row["metadata"],
                {
                    "requestedVersion": "2025-11-25",
                    "negotiatedVersion": "2025-11-25",
                    "backendVersion": "2025-06-18",
                },
            )
            self.assertEqual(
                by_key[("accepted", "2025-06-18")]["metadata"]["negotiatedVersion"],
                "2025-06-18",
            )
            self.assertEqual(
                by_key[("accepted", "2025-03-26")]["metadata"]["negotiatedVersion"],
                "2025-03-26",
            )
            rejected_row = by_key[("rejected", "1999-01-01")]
            self.assertIsNone(rejected_row["metadata"]["negotiatedVersion"])
            # The logical revision is accepted without changing the observed
            # actual physical backend revision recorded above.
            for row in rows:
                if row["metadata"]["requestedVersion"] == "2025-11-25":
                    self.assertEqual(row["outcome"], "accepted")
        finally:
            for client in clients:
                client.close()
        if backend_pid is not None:
            self._wait_pid_exit(backend_pid)

    def test_21_2025_11_25_initialize_result_never_invents_tasks(self) -> None:
        # A 2025-11-25 logical client advertises a full client profile
        # (sampling, elicitation, roots, and the 2025-11-25 client tasks
        # umbrella).  initialize is accepted at exactly 2025-11-25, and the
        # replayed downstream result never fabricates the optional *server*
        # task surface: the physical backend is normalized at 2025-06-18 and
        # advertised no ``capabilities.tasks`` and no tool
        # ``execution.taskSupport``, so the echo carries only what it actually
        # sent.  The client's own claims are never forwarded to the physical
        # backend (fixed empty capability profile), so nothing is solicited
        # from it.
        spawn_start = len(self._log_lines("spawns.log"))
        event_start = len(self._log_lines("events.log"))
        client = RawBridgeClient(self.wsl_local_port, "shared-browser")
        backend_pid: int | None = None
        try:
            initialized = client.initialize(
                request_id="init-2025-11-25-no-tasks",
                protocol_version="2025-11-25",
                capabilities={
                    "sampling": {},
                    "elicitation": {},
                    "roots": {"listChanged": True},
                    "tasks": {
                        "list": {},
                        "cancel": {},
                        "requests": {
                            "sampling": {"createMessage": {}},
                            "elicitation": {"create": {}},
                        },
                    },
                },
                client_name="2025-11-25-task-capable-client",
            )
            self.assertIn("result", initialized)
            result = initialized["result"]
            self.assertEqual(result["protocolVersion"], "2025-11-25")
            # Downstream capability replay is verbatim: no tasks capability is
            # invented even though 2025-11-25 defines the optional server task
            # surface and this client itself advertised the client umbrella.
            self.assertEqual(
                result["capabilities"], {"tools": {"listChanged": True}}
            )
            self.assertNotIn("tasks", result["capabilities"])
            first_page = client.request("init-no-tasks-list-1", "tools/list", {})
            second_page = client.request(
                "init-no-tasks-list-2",
                "tools/list",
                {"cursor": first_page["result"]["nextCursor"]},
            )
            tools = (
                first_page["result"]["tools"] + second_page["result"]["tools"]
            )
            self.assertTrue(tools)
            for tool in tools:
                # Tool-level task support (2025-11-25 ``execution.taskSupport``)
                # is never added to a replayed catalog.
                self.assertNotIn("execution", tool)
            echoed = client.call(
                "init-no-tasks-echo", "echo", {"value": "no-invented-tasks"}
            )
            self.assertEqual(
                echoed["result"]["structuredContent"]["value"], "no-invented-tasks"
            )
            backend_pid = echoed["result"]["structuredContent"]["backendPid"]
            self.assertEqual(len(self._log_lines("spawns.log")) - spawn_start, 1)
            # The physical backend saw the Bridge-owned profile only: the
            # logical client's sampling/elicitation/tasks advertisement was
            # never forwarded, so no such flow was solicited from the backend.
            init_rows = [
                row
                for row in self._log_lines("events.log")[event_start:]
                if row.startswith("initialize:")
            ]
            self.assertEqual(len(init_rows), 1)
            physical = json.loads(init_rows[0].split(":", 1)[1])
            self.assertEqual(physical["protocolVersion"], "2025-11-25")
            self.assertEqual(physical["capabilities"], {})
        finally:
            client.close()
        if backend_pid is not None:
            self._wait_pid_exit(backend_pid)

    def test_22_task_only_advertisement_cannot_false_enable_flows(self) -> None:
        # Under 2025-11-25 a server may only task-augment sampling/elicitation
        # toward a client that declared the matching base capability in
        # addition to the tasks leaf.  A task-only advertisement
        # (``capabilities.tasks`` with no ``sampling``/``elicitation``/``roots``)
        # must not be treated as a flow capability: the backend's
        # sampling/createMessage request is refused with
        # ``requiredClientCapability: sampling`` rather than routed to a client
        # that only claimed the tasks umbrella, so task-augmented flows cannot
        # be falsely enabled through advertised capabilities.
        client = RawBridgeClient(self.wsl_local_port, "shared-browser")
        backend_pid: int | None = None
        try:
            initialized = client.initialize(
                request_id="init-tasks-only",
                protocol_version="2025-11-25",
                capabilities={
                    "tasks": {
                        "list": {},
                        "cancel": {},
                        "requests": {
                            "sampling": {"createMessage": {}},
                            "elicitation": {"create": {}},
                        },
                    }
                },
                client_name="tasks-only-client",
            )
            self.assertIn("result", initialized)
            self.assertEqual(
                initialized["result"]["protocolVersion"], "2025-11-25"
            )
            response = client.call("tasks-only-roundtrip", "server_roundtrip")
            nested = response["result"]["structuredContent"]["nestedError"]
            self.assertEqual(nested["code"], -32000)
            self.assertEqual(
                nested["data"]["requiredClientCapability"], "sampling"
            )
            echoed = client.call(
                "tasks-only-echo", "echo", {"value": "tasks-only-ok"}
            )
            self.assertEqual(
                echoed["result"]["structuredContent"]["value"], "tasks-only-ok"
            )
            backend_pid = echoed["result"]["structuredContent"]["backendPid"]
        finally:
            client.close()
        if backend_pid is not None:
            self._wait_pid_exit(backend_pid)


class SharedBackendStopEscalationTest(unittest.IsolatedAsyncioTestCase):
    """Stop-machine escalation evidence without full bridge nodes.

    A resisting child that ignores SIGTERM forces the bounded stop path
    through wait:timeout -> terminate:requested -> force-kill:used, and a
    short-lived child exercises the graceful clean-exit phases.  POSIX only:
    the escalation path relies on process-group signals.
    """

    RESIST_SCRIPT = (
        "import signal, time\n"
        "signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
        "time.sleep(30)\n"
    )

    def _backend(self) -> SharedBackend:
        backend = SharedBackend(
            node=mock.Mock(), target="stubborn", entry={"process": {}}
        )
        backend.state = "running"
        backend.generation = 1
        return backend

    async def _spawn_child(self, script: str) -> asyncio.subprocess.Process:
        return await asyncio.create_subprocess_exec(
            sys.executable,
            "-c",
            script,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            start_new_session=True,
        )

    async def test_resisting_child_records_forced_escalation_phases(self) -> None:
        if os.name == "nt":
            self.skipTest("process-group stop escalation is POSIX-only")
        backend = self._backend()
        child = await self._spawn_child(self.RESIST_SCRIPT)
        backend.process = child
        child_pid = child.pid
        with mock.patch("bridge_runtime.SHARED_BACKEND_STOP_TIMEOUT_SECONDS", 0.2):
            await backend._stop_generation(1, "forced-stop test")
        phases = backend.last_stop_phases
        self.assertEqual(
            [item["phase"] for item in phases],
            ["drain", "protocol-close", "wait", "terminate", "force-kill"],
        )
        self.assertEqual(phases[2]["outcome"], "timeout")
        self.assertEqual(phases[3]["outcome"], "requested")
        # The graceful request was ignored, so the escalation inside the
        # terminate path is surfaced truthfully as an actual force kill.
        self.assertEqual(phases[4]["outcome"], "used")
        self.assertEqual(backend.state, "exited")
        self.assertIsNone(backend.process)
        with self.assertRaises(OSError):
            os.kill(child_pid, 0)

    async def test_clean_exit_records_graceful_phases_without_force(self) -> None:
        if os.name == "nt":
            self.skipTest("process-group stop escalation is POSIX-only")
        backend = self._backend()
        child = await self._spawn_child("import time; time.sleep(0.2)")
        backend.process = child
        child_pid = child.pid
        with mock.patch("bridge_runtime.SHARED_BACKEND_STOP_TIMEOUT_SECONDS", 10):
            await backend._stop_generation(1, "clean-stop test")
        phases = backend.last_stop_phases
        self.assertEqual(
            [item["phase"] for item in phases],
            ["drain", "protocol-close", "wait", "force-kill"],
        )
        self.assertEqual(phases[2]["outcome"], "exited")
        self.assertEqual(phases[3]["outcome"], "not-needed")
        self.assertEqual(backend.state, "exited")
        with self.assertRaises(OSError):
            os.kill(child_pid, 0)


class BridgeLifecycleUnitTest(unittest.TestCase):
    """Offline lifecycle semantics: status, redaction, drain guard, previews."""

    def _node(self, database: Path) -> BridgeNode:
        return BridgeNode(
            side="win",
            registry=Registry(database),
            local_host="127.0.0.1",
            local_port=free_port(),
            link_mode="listen",
            link_host="127.0.0.1",
            link_port=free_port(),
        )

    def test_status_and_registry_enrichment_are_compact_and_redacted(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            state = root / "state"
            state.mkdir()
            database = root / "registry.sqlite3"
            write_shared_registry(database, state)
            node = self._node(database)

            status = node._lifecycle_status(None)
            self.assertTrue(status["ok"])
            servers = {entry["id"]: entry for entry in status["servers"]}
            self.assertEqual(servers["shared-browser"]["mode"], "shared")
            self.assertEqual(
                servers["shared-browser"]["lifecycle"]["state"], "exited"
            )
            self.assertEqual(
                servers["shared-browser"]["lifecycle"]["ownedGeneration"], 0
            )
            self.assertFalse(servers["shared-browser"]["lifecycle"]["drain"])
            self.assertEqual(servers["other-mcp"]["mode"], "dedicated")
            self.assertEqual(
                servers["other-mcp"]["lifecycle"]["activeStreams"], 0
            )
            serialized = json.dumps(status)
            for private in ("command", "args", "cwd", "env", str(ROOT)):
                self.assertNotIn(private, serialized)

            # Registry describe is enriched only by a live owning node, and the
            # merged lifecycle summary never leaks pids or launch fields.
            describe = node._with_lifecycle_fields(
                "describe",
                Registry(database).query("describe", {"id": "shared-browser"}),
            )
            lifecycle = describe["lifecycle"]
            self.assertEqual(lifecycle["mode"], "shared")
            self.assertEqual(lifecycle["state"], "exited")
            self.assertNotIn("pid", lifecycle)
            status_all = node._with_lifecycle_fields(
                "status", Registry(database).query("status", {})
            )
            by_id = {row["id"]: row for row in status_all}
            self.assertIn("lifecycle", by_id["shared-browser"])
            self.assertEqual(by_id["other-mcp"]["lifecycle"]["mode"], "dedicated")
            # list rows are intentionally unchanged (compactness).
            rows = node._with_lifecycle_fields(
                "list", Registry(database).query("list", {})
            )
            for row in rows:
                self.assertNotIn("lifecycle", row)

    def test_registry_request_answer_carries_redacted_lifecycle_fields(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            state = root / "state"
            state.mkdir()
            database = root / "registry.sqlite3"
            write_shared_registry(database, state)
            node = self._node(database)

            async def exercise() -> None:
                sent: list[dict] = []

                async def capture(frame: dict) -> None:
                    sent.append(frame)

                node._send_frame = capture  # type: ignore[method-assign]
                await node._handle_registry_request(
                    {
                        "type": "registry_request",
                        "request": "lifecycle-describe",
                        "action": "describe",
                        "arguments": {"id": "shared-browser"},
                    }
                )
                await node._handle_registry_request(
                    {
                        "type": "registry_request",
                        "request": "lifecycle-status",
                        "action": "status",
                        "arguments": {"id": "shared-browser"},
                    }
                )
                self.assertTrue(sent[0]["ok"])
                describe = sent[0]["result"]
                self.assertEqual(describe["lifecycle"]["mode"], "shared")
                self.assertIn("state", describe["lifecycle"])
                status = sent[1]["result"]
                self.assertTrue(status["registered"])
                self.assertEqual(status["lifecycle"]["mode"], "shared")
                serialized = json.dumps(sent)
                self.assertNotIn("pid", serialized)
                for private in ("command", "args", "cwd", "env", str(ROOT)):
                    self.assertNotIn(private, serialized)

            asyncio.run(exercise())

    def test_previews_never_mutate_and_drain_arms_without_spawning(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            state = root / "state"
            state.mkdir()
            database = root / "registry.sqlite3"
            write_shared_registry(database, state)
            node = self._node(database)

            async def exercise() -> None:
                for action in ("drain", "restart", "stop"):
                    if action == "drain":
                        result = await node._lifecycle_drain(
                            "shared-browser", None, False, ""
                        )
                    elif action == "restart":
                        result = await node._lifecycle_restart(
                            "shared-browser", None, False, ""
                        )
                    else:
                        result = await node._lifecycle_stop(
                            "shared-browser", None, False, ""
                        )
                    self.assertFalse(result["applied"], result)
                    self.assertTrue(result["confirmRequired"], result)
                    self.assertEqual(result["id"], "shared-browser")
                self.assertFalse(node._lifecycle_drain_armed("shared-browser"))
                self.assertIsNone(
                    node.lifecycle_desired.get("shared-browser")
                )

                applied = await node._lifecycle_drain(
                    "shared-browser", None, True, ""
                )
                self.assertTrue(applied["applied"])
                self.assertTrue(applied["result"]["drain"])
                backend = node.shared_backends["shared-browser"]
                self.assertTrue(backend.drain_requested)
                # A refused attach never starts a generation (no process spawn).
                stream = StreamState(
                    stream_id="win-refused001", target="shared-browser"
                )
                with self.assertRaises(BridgeError) as caught:
                    await backend.attach(stream)
                self.assertIn("draining", str(caught.exception))
                self.assertEqual(backend.generation, 0)
                self.assertEqual(backend.state, "exited")
                restart = await node._lifecycle_restart(
                    "shared-browser", None, True, ""
                )
                self.assertTrue(restart["result"]["clearedDrain"])
                self.assertFalse(node._lifecycle_drain_armed("shared-browser"))
                self.assertFalse(backend.drain_requested)
                # stop also disarms an armed drain (in-memory intent; no warm).
                rearmed = await node._lifecycle_drain(
                    "shared-browser", None, True, ""
                )
                self.assertTrue(rearmed["applied"])
                self.assertTrue(node._lifecycle_drain_armed("shared-browser"))
                stopped = await node._lifecycle_stop(
                    "shared-browser", None, True, ""
                )
                self.assertTrue(stopped["applied"])
                self.assertTrue(stopped["result"]["clearedDrain"])
                self.assertTrue(stopped["result"]["reconnectRequired"])
                self.assertFalse(node._lifecycle_drain_armed("shared-browser"))
                self.assertFalse(backend.drain_requested)

            asyncio.run(exercise())

    def test_dedicated_and_unknown_targets_refuse_control_cleanly(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            state = root / "state"
            state.mkdir()
            database = root / "registry.sqlite3"
            write_shared_registry(database, state)
            node = self._node(database)

            async def exercise() -> None:
                with self.assertRaises(BridgeError) as dedicated:
                    await node._lifecycle_stop("other-mcp", None, True, "")
                self.assertIn(
                    "dedicated registration", str(dedicated.exception)
                )
                with self.assertRaises(BridgeError) as missing:
                    await node._lifecycle_stop("missing-id", None, True, "")
                self.assertIn("unknown registry id", str(missing.exception))
                # status keeps working for dedicated registrations.
                status = node._lifecycle_status("other-mcp")
                self.assertEqual(status["server"]["mode"], "dedicated")

            asyncio.run(exercise())


class BridgeLifecycleControlTest(unittest.TestCase):
    """End-to-end Operator lifecycle control against live bridge nodes.

    The Windows node owns the shared/dedicated registrations; the WSL node is
    the Agent side that opens logical streams. All lifecycle mutations go
    through the local Operator CLI on the owning node with preview --confirm.
    """

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp = tempfile.TemporaryDirectory()
        root = Path(cls.temp.name)
        cls.root = root
        cls.state = root / "state"
        cls.state.mkdir()
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
                str(root / "win-spool"),
            ],
            cwd=ROOT,
            env=cls.environment,
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
        raise RuntimeError("lifecycle test nodes did not connect")

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

    # -- helpers ----------------------------------------------------------

    def _spawn_pids(self) -> list[int]:
        rows = (self.state / "spawns.log").read_text(encoding="utf-8").splitlines()
        return [int(row) for row in rows if row.strip().isdigit()]

    def _log_lines(self, name: str) -> list[str]:
        path = self.state / name
        return path.read_text(encoding="utf-8").splitlines() if path.exists() else []

    def _wait_for(self, predicate, timeout: float = 15, what: str = "condition") -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if predicate():
                return
            time.sleep(0.05)
        self.fail(f"timed out waiting for {what}")

    def _wait_pid_exit(self, pid: int, timeout: float = 10) -> None:
        def gone() -> bool:
            try:
                os.kill(pid, 0)
            except OSError:
                return True
            return False

        self._wait_for(gone, timeout=timeout, what=f"child pid {pid} to exit")

    def _cli(
        self,
        action: str,
        *,
        target: str | None = None,
        generation: int | None = None,
        confirm: bool = False,
    ) -> tuple[int, dict | None]:
        command = [
            sys.executable,
            str(WIN),
            "lifecycle",
            action,
            "--local-port",
            str(self.win_local_port),
        ]
        if target is not None:
            command += ["--id", target]
        if generation is not None:
            command += ["--generation", str(generation)]
        if confirm:
            command.append("--confirm")
        process = subprocess.run(
            command,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=ROOT,
            timeout=25,
        )
        result: dict | None = None
        text = process.stdout.strip()
        if text:
            try:
                result = json.loads(text)
            except json.JSONDecodeError:
                result = None
        return process.returncode, result

    def _status_server(self, target: str) -> dict:
        code, result = self._cli("status", target=target)
        self.assertEqual(code, 0, result)
        assert result is not None
        return result["server"]

    def _wait_state(self, target: str, expected: str, timeout: float = 12) -> None:
        self._wait_for(
            lambda: self._status_server(target)["lifecycle"]["state"] == expected,
            timeout=timeout,
            what=f"{target} state {expected}",
        )

    def _open_shared(self) -> RawBridgeClient:
        client = RawBridgeClient(self.wsl_local_port, "shared-browser")
        client.initialize()
        return client

    # -- lifecycle status, preview, and confirm --------------------------

    def test_01_status_reports_idle_registrations_with_modes(self) -> None:
        code, result = self._cli("status")
        self.assertEqual(code, 0, result)
        assert result is not None and result["ok"]
        servers = {entry["id"]: entry for entry in result["servers"]}
        self.assertEqual(servers["shared-browser"]["mode"], "shared")
        lifecycle = servers["shared-browser"]["lifecycle"]
        self.assertEqual(lifecycle["state"], "exited")
        self.assertEqual(lifecycle["ownedGeneration"], 0)
        self.assertFalse(lifecycle["drain"])
        self.assertEqual(servers["other-mcp"]["mode"], "dedicated")
        self.assertEqual(servers["other-mcp"]["lifecycle"]["activeStreams"], 0)
        serialized = json.dumps(result)
        for private in ("command", "args", "cwd", "env", str(FIXTURE)):
            self.assertNotIn(private, serialized)

    def test_02_preview_is_read_only_until_confirm(self) -> None:
        client = self._open_shared()
        try:
            self.assertEqual(
                client.call(2, "echo", {"value": "before"})["result"][
                    "structuredContent"
                ]["value"],
                "before",
            )
            pid = self._spawn_pids()[-1]
            for action in ("drain", "restart", "stop"):
                code, preview = self._cli(action, target="shared-browser")
                self.assertEqual(code, 0, preview)
                assert preview is not None
                self.assertFalse(preview["applied"])
                self.assertTrue(preview["confirmRequired"])
                self.assertEqual(preview["observed"]["state"], "running")
            self.assertEqual(self._spawn_pids()[-1], pid)
            self.assertEqual(
                client.call(3, "echo", {"value": "after"})["result"][
                    "structuredContent"
                ]["value"],
                "after",
            )
        finally:
            client.close()
        self._wait_pid_exit(self._spawn_pids()[-1])

    def test_03_stop_confirmed_terminates_owned_generation(self) -> None:
        client = self._open_shared()
        pid = self._spawn_pids()[-1]
        status = self._status_server("shared-browser")
        self.assertEqual(status["lifecycle"]["state"], "running")
        owned = status["lifecycle"]["ownedGeneration"]
        code, applied = self._cli(
            "stop", target="shared-browser", generation=owned, confirm=True
        )
        self.assertEqual(code, 0, applied)
        assert applied is not None and applied["applied"]
        self.assertEqual(applied["result"]["stoppedGeneration"], owned)
        self.assertTrue(applied["result"]["reconnectRequired"])
        with self.assertRaises((EOFError, OSError)):
            client.receive()
        client.close()
        self._wait_pid_exit(pid)
        self._wait_state("shared-browser", "exited")
        # The stopped stream was closed; a fresh connect starts one new
        # generation with no overlap.
        before = len(self._spawn_pids())
        replacement = self._open_shared()
        try:
            echoed = replacement.call(2, "echo", {"value": "replacement"})
            self.assertEqual(
                echoed["result"]["structuredContent"]["value"], "replacement"
            )
        finally:
            replacement.close()
        self.assertEqual(len(self._spawn_pids()), before + 1)
        new_pid = self._spawn_pids()[-1]
        self.assertNotEqual(new_pid, pid)
        self._wait_state("shared-browser", "exited")
        self._wait_pid_exit(new_pid)

    def test_04_drain_refuses_new_streams_and_restart_clears(self) -> None:
        client = self._open_shared()
        try:
            tools_before = client.request(2, "tools/list", {})
            self.assertTrue("result" in tools_before)
            code, drained = self._cli("drain", target="shared-browser", confirm=True)
            self.assertEqual(code, 0, drained)
            assert drained is not None and drained["applied"]
            server = self._status_server("shared-browser")
            self.assertEqual(server["lifecycle"]["state"], "running")
            self.assertTrue(server["lifecycle"]["drain"])
            spawn_count = len(self._spawn_pids())
            # A new attach is refused without spawning another backend.
            with self.assertRaisesRegex(BridgeError, "draining"):
                RawBridgeClient(self.wsl_local_port, "shared-browser")
            self.assertEqual(len(self._spawn_pids()), spawn_count)
            # The active client is unaffected and its catalog stays identical.
            echoed = client.call(3, "echo", {"value": "still active"})
            self.assertEqual(
                echoed["result"]["structuredContent"]["value"], "still active"
            )
            # Lifecycle control is out-of-band: it adds no business tools or
            # schemas, as separately asserted by the fixed Registry tool-list
            # and pre-drain catalog checks.
            self.assertTrue(tools_before["result"]["tools"])
        finally:
            client.close()
        pid = self._spawn_pids()[-1]
        self._wait_pid_exit(pid)
        self._wait_state("shared-browser", "exited")
        server = self._status_server("shared-browser")
        self.assertTrue(server["lifecycle"]["drain"])
        with self.assertRaisesRegex(BridgeError, "draining"):
            RawBridgeClient(self.wsl_local_port, "shared-browser")
        code, restarted = self._cli("restart", target="shared-browser", confirm=True)
        self.assertEqual(code, 0, restarted)
        assert restarted is not None and restarted["applied"]
        self.assertTrue(restarted["result"]["clearedDrain"])
        self.assertFalse(
            self._status_server("shared-browser")["lifecycle"]["drain"]
        )
        fresh = self._open_shared()
        try:
            echoed = fresh.call(2, "echo", {"value": "after restart"})
            self.assertEqual(
                echoed["result"]["structuredContent"]["value"], "after restart"
            )
        finally:
            fresh.close()
        self._wait_pid_exit(self._spawn_pids()[-1])

    def test_05_stale_generation_guard_refuses_stale_ownership(self) -> None:
        first = self._open_shared()
        first_pid = self._spawn_pids()[-1]
        first_generation = self._status_server("shared-browser")["lifecycle"][
            "ownedGeneration"
        ]
        first.close()
        self._wait_pid_exit(first_pid)
        self._wait_state("shared-browser", "exited")

        second = self._open_shared()
        try:
            server = self._status_server("shared-browser")
            self.assertEqual(
                server["lifecycle"]["ownedGeneration"], first_generation + 1
            )
            code, refused = self._cli(
                "stop",
                target="shared-browser",
                generation=first_generation,
                confirm=True,
            )
            self.assertEqual(code, 1, refused)
            self.assertIsNone(refused)
            # The newer generation is untouched and still serves.
            self.assertEqual(
                self._status_server("shared-browser")["lifecycle"][
                    "ownedGeneration"
                ],
                first_generation + 1,
            )
            echoed = second.call(2, "echo", {"value": "still alive"})
            self.assertEqual(
                echoed["result"]["structuredContent"]["value"], "still alive"
            )
            code, applied = self._cli(
                "stop",
                target="shared-browser",
                generation=first_generation + 1,
                confirm=True,
            )
            self.assertEqual(code, 0, applied)
            assert applied is not None and applied["applied"]
        finally:
            second.close()
        self._wait_state("shared-browser", "exited")
        self._wait_pid_exit(self._spawn_pids()[-1])

    def test_06_restart_preserves_stream_without_generation_overlap(self) -> None:
        client = self._open_shared()
        before = client.call(2, "echo", {"value": "one"})
        pid = before["result"]["structuredContent"]["backendPid"]
        owned = self._status_server("shared-browser")["lifecycle"][
            "ownedGeneration"
        ]
        spawn_count = len(self._spawn_pids())
        code, restarted = self._cli("restart", target="shared-browser", confirm=True)
        self.assertEqual(code, 0, restarted)
        assert restarted is not None and restarted["applied"]
        self.assertFalse(restarted["result"]["clearedDrain"])
        self.assertEqual(restarted["result"]["stoppedGeneration"], owned)
        self.assertEqual(restarted["result"]["startedGeneration"], owned + 1)
        self.assertEqual(restarted["result"]["preservedClients"], 1)
        self.assertEqual(restarted["result"]["toolsListChangedNotifiedClients"], 1)
        self.assertFalse(restarted["result"]["reconnectRequired"])
        self.assertEqual(
            [item["phase"] for item in restarted["result"]["phases"]],
            ["drain", "protocol-close", "wait", "force-kill"],
        )
        self.assertEqual(restarted["result"]["phases"][-1]["outcome"], "not-needed")
        self._wait_pid_exit(pid)
        self.assertEqual(len(self._spawn_pids()), spawn_count + 1)
        changed = client.receive()
        self.assertEqual(changed["method"], "notifications/tools/list_changed")
        after = client.call(3, "echo", {"value": "two"})
        new_pid = after["result"]["structuredContent"]["backendPid"]
        self.assertNotEqual(new_pid, pid)
        self.assertEqual(after["result"]["structuredContent"]["value"], "two")
        server = self._status_server("shared-browser")["lifecycle"]
        self.assertEqual(server["state"], "running")
        self.assertEqual(server["ownedGeneration"], owned + 1)
        self.assertEqual(server["activeClients"], 1)
        events = self._log_lines("events.log")
        old_exit = next(i for i, row in enumerate(events) if row.startswith(f"backend-exit:{pid}:"))
        new_start = next(i for i, row in enumerate(events) if row.startswith(f"backend-start:{new_pid}:"))
        self.assertLess(old_exit, new_start)
        client.close()
        self._wait_pid_exit(new_pid)
        self._wait_state("shared-browser", "exited")

    def test_06b_refresh_broadcasts_without_restarting_backend(self) -> None:
        client = self._open_shared()
        before = client.call(2, "echo", {"value": "before"})
        pid = before["result"]["structuredContent"]["backendPid"]
        owned = self._status_server("shared-browser")["lifecycle"]["ownedGeneration"]
        code, preview = self._cli("refresh", target="shared-browser")
        self.assertEqual(code, 0, preview)
        assert preview is not None
        self.assertFalse(preview["applied"])
        self.assertTrue(preview["confirmRequired"])
        code, refreshed = self._cli("refresh", target="shared-browser", confirm=True)
        self.assertEqual(code, 0, refreshed)
        assert refreshed is not None and refreshed["applied"]
        self.assertGreaterEqual(
            refreshed["result"]["toolsListChangedNotifiedClients"], 1
        )
        self.assertFalse(refreshed["result"]["backendStarted"])
        self.assertFalse(refreshed["result"]["backendRestarted"])
        changed = client.receive()
        self.assertEqual(changed["method"], "notifications/tools/list_changed")
        after = client.call(3, "echo", {"value": "after"})
        self.assertEqual(after["result"]["structuredContent"]["backendPid"], pid)
        self.assertEqual(
            self._status_server("shared-browser")["lifecycle"]["ownedGeneration"],
            owned,
        )
        client.close()
        self._wait_pid_exit(pid)

    def test_07_dedicated_status_aggregates_and_control_refuses(self) -> None:
        dedicated = RawBridgeClient(self.wsl_local_port, "other-mcp")
        dedicated.initialize()
        try:
            server = self._status_server("other-mcp")
            self.assertEqual(server["mode"], "dedicated")
            self.assertEqual(server["lifecycle"]["state"], "running")
            self.assertGreaterEqual(server["lifecycle"]["activeStreams"], 1)
            for action in ("drain", "restart", "stop"):
                code, refused = self._cli(action, target="other-mcp", confirm=True)
                self.assertEqual(code, 1, refused)
                self.assertIsNone(refused)
            # The dedicated Agent stream is untouched: lifecycle control never
            # synthesizes or replays a dedicated session.
            self.assertEqual(
                self._status_server("other-mcp")["lifecycle"]["activeStreams"], 1
            )
        finally:
            dedicated.close()
        self._wait_for(
            lambda: self._status_server("other-mcp")["lifecycle"][
                "activeStreams"
            ]
            == 0,
            timeout=12,
            what="dedicated stream aggregation to drain to zero",
        )

    def test_08_peer_registry_describe_and_status_include_lifecycle(self) -> None:
        client = self._open_shared()
        try:
            describe = local_registry_query(
                "127.0.0.1",
                self.wsl_local_port,
                "remote",
                "describe",
                {"id": "shared-browser"},
            )
            lifecycle = describe["lifecycle"]
            self.assertEqual(lifecycle["mode"], "shared")
            self.assertEqual(lifecycle["state"], "running")
            self.assertGreaterEqual(lifecycle["ownedGeneration"], 1)
            self.assertNotIn("pid", lifecycle)
            serialized = json.dumps(describe)
            for private in ("command", "args", "cwd", "env", "must-not-leak"):
                self.assertNotIn(private, serialized)

            status_one = local_registry_query(
                "127.0.0.1",
                self.wsl_local_port,
                "remote",
                "status",
                {"id": "shared-browser"},
            )
            self.assertTrue(status_one["registered"])
            self.assertIn("lifecycle", status_one)

            status_all = local_registry_query(
                "127.0.0.1", self.wsl_local_port, "remote", "status", {}
            )
            by_id = {row["id"]: row for row in status_all}
            self.assertEqual(by_id["other-mcp"]["lifecycle"]["mode"], "dedicated")
            # list stays compact and unchanged (lifecycle only on describe/status).
            rows = local_registry_query(
                "127.0.0.1", self.wsl_local_port, "remote", "list", {}
            )
            for row in rows:
                self.assertNotIn("lifecycle", row)
        finally:
            client.close()
        self._wait_pid_exit(self._spawn_pids()[-1])


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

    def test_connector_stays_alive_and_reconnects_after_remote_close(self) -> None:
        process = subprocess.Popen(
            [
                sys.executable,
                str(WSL),
                "connect",
                "win-echo",
                "--local-port",
                str(self.wsl_local_port),
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=ROOT,
        )
        try:
            assert process.stdin is not None and process.stdout is not None
            assert process.stderr is not None
            reader = FdLineReader(process.stdout)

            def exchange(text: str) -> list[dict]:
                assert process.stdin is not None
                process.stdin.write(rpc_messages(text).encode("utf-8"))
                process.stdin.flush()
                responses: list[dict] = []
                deadline = time.monotonic() + 10
                while time.monotonic() < deadline:
                    raw = reader.read_line(timeout=0.5)
                    if raw is None:
                        break
                    try:
                        message = json.loads(raw)
                    except ValueError:
                        continue
                    if isinstance(message, dict) and "id" in message and (
                        "result" in message or "error" in message
                    ):
                        responses.append(message)
                        if len(responses) >= 3:
                            break
                return responses

            # Round 1 completes; the win-echo fixture exits after the call, so
            # the node closes the stream while Agent stdin is still open.
            first = exchange("remote closes first")
            self.assertEqual(len(first), 3)
            self.assertEqual(
                first[2]["result"]["structuredContent"]["value"],
                "remote closes first",
            )
            time.sleep(1.0)
            self.assertIsNone(
                process.poll(),
                "connect exited while Agent stdin was still open after remote close",
            )
            # Round 2 has no fresh initialize: the reconnected connector must
            # replay only the cached initialize/initialized handshake and never
            # replay the business call.
            assert process.stdin is not None
            process.stdin.write(
                "".join(
                    json.dumps(item, separators=(",", ":")) + "\n"
                    for item in [
                        {"jsonrpc": "2.0", "id": 4, "method": "tools/list", "params": {}},
                        {
                            "jsonrpc": "2.0",
                            "id": 5,
                            "method": "tools/call",
                            "params": {
                                "name": "echo",
                                "arguments": {"value": "after reconnect"},
                            },
                        },
                    ]
                ).encode("utf-8")
            )
            process.stdin.flush()
            second: list[dict] = []
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                raw = reader.read_line(timeout=0.5)
                if raw is None:
                    break
                try:
                    message = json.loads(raw)
                except ValueError:
                    continue
                if isinstance(message, dict) and message.get("id") in {4, 5}:
                    second.append(message)
                    if {item.get("id") for item in second} >= {4, 5}:
                        break
            by_id = {item.get("id"): item for item in second}
            self.assertIn(4, by_id, second)
            self.assertIn(5, by_id, second)
            self.assertEqual(
                by_id[5]["result"]["structuredContent"]["value"],
                "after reconnect",
            )
            # The only sanctioned exit is Agent stdin EOF.
            process.stdin.close()
            process.wait(timeout=10)
            self.assertEqual(process.returncode, 0)
            self.assertEqual(process.stderr.read(), b"")
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

    def _assert_input_roundtrip(self, local_port: int, target: str, source: Path) -> None:
        payload = bytes(range(256)) + b"artifact-inputs-roundtrip\x00\xff"
        source.write_bytes(payload)
        client = RawBridgeClient(local_port, target)
        try:
            initialize = client.initialize()
            self.assertIn("result", initialize)
            self.assertIsNotNone(client.stream_id)
            receipt = stage_input(
                "127.0.0.1",
                local_port,
                client.stream_id,
                str(source),
                name="agent-payload.bin",
                media_type="application/octet-stream",
            )
            self.assertTrue(receipt["handle"].startswith("bridge-input://"))
            self.assertEqual(receipt["size"], len(payload))
            self.assertEqual(receipt["sha256"], hashlib.sha256(payload).hexdigest())
            response = client.call(
                2,
                "read_input",
                {"path": receipt["handle"]},
            )
            self.assertIn("result", response, response)
            result = response["result"]
            self.assertFalse(result.get("isError", False))
            self.assertEqual(result["structuredContent"]["size"], len(payload))
            self.assertEqual(
                result["structuredContent"]["sha256"],
                hashlib.sha256(payload).hexdigest(),
            )
        finally:
            client.close()

    def test_wsl_agent_stages_local_input_into_windows_mcp(self) -> None:
        self._assert_input_roundtrip(
            self.wsl_local_port,
            "win-echo",
            self.wsl_workspace / "wsl-agent-input.bin",
        )

    def test_windows_agent_stages_local_input_into_wsl_mcp_on_same_link(self) -> None:
        self._assert_input_roundtrip(
            self.win_local_port,
            "wsl-echo",
            self.win_workspace / "win-agent-input.bin",
        )

    def test_input_staging_rejects_sources_outside_authorized_root(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            outside = Path(temp) / "outside-root"
            outside.mkdir()
            source = outside / "sneaky.txt"
            source.write_bytes(b"sneaky")
            client = RawBridgeClient(self.wsl_local_port, "win-echo")
            try:
                self.assertIsNotNone(client.stream_id)
                with self.assertRaisesRegex(BridgeError, "outside Operator-authorized"):
                    stage_input(
                        "127.0.0.1",
                        self.wsl_local_port,
                        client.stream_id,
                        str(source),
                    )
            finally:
                client.close()

    def test_staged_input_expires_when_the_session_closes(self) -> None:
        client = RawBridgeClient(self.wsl_local_port, "win-echo")
        try:
            self.assertIsNotNone(client.stream_id)
            source = self.wsl_workspace / "expiring-input.bin"
            source.write_bytes(b"will-expire")
            receipt = stage_input(
                "127.0.0.1",
                self.wsl_local_port,
                client.stream_id,
                str(source),
            )
            self.assertIn("handle", receipt)
            expired_stream = client.stream_id
        finally:
            client.close()
        # The local client EOF closes the logical stream (bounded grace period);
        # staging against the expired stream id must then fail closed.
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            try:
                stage_input(
                    "127.0.0.1",
                    self.wsl_local_port,
                    expired_stream,
                    str(self.wsl_workspace / "expiring-input.bin"),
                )
            except BridgeError:
                return
            time.sleep(0.2)
        self.fail("staged input did not expire after the session closed")

    def test_duplicate_staged_input_name_is_rejected_fail_closed(self) -> None:
        payload = b"first-bytes"
        source = self.wsl_workspace / "dup-input.bin"
        source.write_bytes(payload)
        client = RawBridgeClient(self.wsl_local_port, "win-echo")
        try:
            self.assertIsNotNone(client.stream_id)
            first = stage_input(
                "127.0.0.1",
                self.wsl_local_port,
                client.stream_id,
                str(source),
                name="dup-input.bin",
            )
            self.assertIn("handle", first)
            with self.assertRaisesRegex(BridgeError, "input delivery was rejected"):
                stage_input(
                    "127.0.0.1",
                    self.wsl_local_port,
                    client.stream_id,
                    str(source),
                    name="dup-input.bin",
                )
        finally:
            client.close()

    def test_stage_input_cli_is_explicit_and_self_contained(self) -> None:
        payload = b"cli-input-" + bytes(range(96))
        source = self.wsl_workspace / "cli-source.bin"
        source.write_bytes(payload)
        environment = os.environ.copy()
        environment["PYTHONDONTWRITEBYTECODE"] = "1"
        client = RawBridgeClient(self.wsl_local_port, "win-echo")
        try:
            self.assertIsNotNone(client.stream_id)
            process = subprocess.run(
                [
                    sys.executable,
                    str(WSL),
                    "stage-input",
                    str(source),
                    "--stream",
                    str(client.stream_id),
                    "--local-host",
                    "127.0.0.1",
                    "--local-port",
                    str(self.wsl_local_port),
                    "--name",
                    "cli-payload.bin",
                    "--media-type",
                    "application/octet-stream",
                ],
                cwd=ROOT,
                env=environment,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=60,
                check=True,
            )
            receipt = json.loads(process.stdout)
            self.assertTrue(receipt["handle"].startswith("bridge-input://"))
            self.assertEqual(receipt["size"], len(payload))
            self.assertEqual(receipt["sha256"], hashlib.sha256(payload).hexdigest())
            response = client.call(2, "read_input", {"path": receipt["handle"]})
            self.assertIn("result", response, response)
            result = response["result"]
            self.assertFalse(result.get("isError", False))
            self.assertEqual(result["structuredContent"]["size"], len(payload))
            self.assertEqual(
                result["structuredContent"]["sha256"],
                hashlib.sha256(payload).hexdigest(),
            )
        finally:
            client.close()

    def test_stage_input_cli_rejects_sources_outside_authorized_root(self) -> None:
        environment = os.environ.copy()
        environment["PYTHONDONTWRITEBYTECODE"] = "1"
        client = RawBridgeClient(self.wsl_local_port, "win-echo")
        try:
            with tempfile.TemporaryDirectory() as temp:
                outside = Path(temp) / "outside-cli"
                outside.mkdir()
                source = outside / "sneaky-cli.txt"
                source.write_bytes(b"sneaky")
                process = subprocess.run(
                    [
                        sys.executable,
                        str(WSL),
                        "stage-input",
                        str(source),
                        "--stream",
                        str(client.stream_id),
                        "--local-port",
                        str(self.wsl_local_port),
                    ],
                    cwd=ROOT,
                    env=environment,
                    text=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    timeout=60,
                )
                self.assertNotEqual(process.returncode, 0)
                self.assertIn("outside Operator-authorized", process.stderr)
        finally:
            client.close()

    def test_connect_cli_can_print_stream_id_for_input_adapters(self) -> None:
        environment = os.environ.copy()
        environment["PYTHONDONTWRITEBYTECODE"] = "1"
        process = subprocess.Popen(
            [
                sys.executable,
                str(WSL),
                "connect",
                "win-echo",
                "--local-host",
                "127.0.0.1",
                "--local-port",
                str(self.wsl_local_port),
                "--print-stream-id",
            ],
            cwd=ROOT,
            env=environment,
            text=True,
            bufsize=1,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        stdout_lines: list[str] = []
        stderr_lines: list[str] = []

        def pump(pipe, sink) -> None:
            for line in pipe:
                sink.append(line)

        stdout_thread = threading.Thread(target=pump, args=(process.stdout, stdout_lines), daemon=True)
        stderr_thread = threading.Thread(target=pump, args=(process.stderr, stderr_lines), daemon=True)
        stdout_thread.start()
        stderr_thread.start()
        try:
            deadline = time.monotonic() + 10
            stream_line = None
            while time.monotonic() < deadline:
                if any(item.startswith("bridge stream: ") for item in stderr_lines):
                    stream_line = next(
                        item for item in stderr_lines if item.startswith("bridge stream: ")
                    )
                    break
                if process.poll() is not None:
                    break
                time.sleep(0.05)
            self.assertIsNotNone(stream_line, stderr_lines)
            stream_id = stream_line.split(":", 1)[1].strip()
            self.assertTrue(stream_id)
            assert process.stdin is not None
            process.stdin.write(
                json.dumps(
                    {
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "initialize",
                        "params": {
                            "protocolVersion": "2025-06-18",
                            "capabilities": {},
                            "clientInfo": {"name": "cli-adapter", "version": "1"},
                        },
                    },
                    separators=(",", ":"),
                )
                + "\n"
            )
            process.stdin.write(
                json.dumps({"jsonrpc": "2.0", "method": "notifications/initialized"}, separators=(",", ":"))
                + "\n"
            )
            process.stdin.flush()
            source = self.wsl_workspace / "adapter-stream-input.bin"
            source.write_bytes(b"adapter-input-" + bytes(range(48)))
            staged = subprocess.run(
                [
                    sys.executable,
                    str(WSL),
                    "stage-input",
                    str(source),
                    "--stream",
                    stream_id,
                    "--local-port",
                    str(self.wsl_local_port),
                ],
                cwd=ROOT,
                env=environment,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=60,
                check=True,
            )
            receipt = json.loads(staged.stdout)
            handle = receipt["handle"]
            assert process.stdin is not None
            process.stdin.write(
                json.dumps(
                    {
                        "jsonrpc": "2.0",
                        "id": 2,
                        "method": "tools/call",
                        "params": {"name": "read_input", "arguments": {"path": handle}},
                    },
                    separators=(",", ":"),
                )
                + "\n"
            )
            process.stdin.flush()
            deadline = time.monotonic() + 10
            response = None
            while time.monotonic() < deadline:
                for raw in list(stdout_lines):
                    try:
                        message = json.loads(raw)
                    except (ValueError, TypeError):
                        continue
                    if message.get("id") == 2 and "result" in message:
                        response = message
                        break
                if response is not None:
                    break
                if process.poll() is not None:
                    break
                time.sleep(0.05)
            self.assertIsNotNone(response, stdout_lines)
            self.assertEqual(response["result"]["structuredContent"]["size"], source.stat().st_size)
        finally:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=10)
            for stream in (process.stdin, process.stdout, process.stderr):
                if stream is not None:
                    try:
                        stream.close()
                    except OSError:
                        pass


import shutil

import bridge_runtime as bridge_runtime

FAKE_CLI_SRC = r'''#!@PYBIN@
import json, os, sys
STATE = os.environ["FAKE_STATE"]
KIND = os.environ.get("FAKE_KIND", "generic")
def load():
    try:
        return json.load(open(STATE))
    except (OSError, ValueError):
        return {"mcpServers": {}}
def save(doc):
    json.dump(doc, open(STATE, "w"), ensure_ascii=False, indent=1)
args = sys.argv[1:]
if not args or args[0] != "mcp":
    print("expected mcp subcommand", file=sys.stderr)
    sys.exit(2)
cmd = args[1]
def first_positional(start):
    for index in range(start, len(args)):
        token = args[index]
        if token.startswith("-") or token == "--":
            continue
        return index
    return None
if cmd == "list":
    doc = load()
    out = {
        "mcpServers": [
            {"name": key, **value} for key, value in doc["mcpServers"].items()
        ]
    }
    print(json.dumps(out))
    sys.exit(0)
if cmd == "get":
    doc = load()
    index = first_positional(2)
    if index is None:
        sys.exit(1)
    name = args[index]
    if name not in doc["mcpServers"]:
        sys.exit(1)
    value = doc["mcpServers"][name]
    if KIND == "codex":
        # Real Codex `mcp get --json` nests the transport object.
        if value.get("type") == "http":
            transport = {"type": "streamable_http", "url": value.get("url")}
        else:
            transport = {"type": "stdio", "command": value.get("command"),
                         "args": value.get("args", []), "env": value.get("env", {})}
        print(json.dumps({"name": name, "transport": transport}))
        sys.exit(0)
    print(json.dumps({"name": name, **value}))
    sys.exit(0)
if cmd == "remove":
    doc = load()
    index = first_positional(2)
    if index is None:
        sys.exit(1)
    doc["mcpServers"].pop(args[index], None)
    save(doc)
    sys.exit(0)
if cmd == "add":
    env = {}
    headers = {}
    name = None
    url = None
    transport_http = False
    after_dash = False
    rest = []
    index = 2
    while index < len(args):
        token = args[index]
        if after_dash:
            rest.append(token)
            index += 1
            continue
        if token == "--":
            after_dash = True
            index += 1
            continue
        if token in ("-e", "--env"):
            key, value = args[index + 1].split("=", 1)
            env[key] = value
            index += 2
            continue
        if token == "--url":
            url = args[index + 1]
            index += 2
            continue
        if token == "--transport":
            transport_http = (args[index + 1] == "http")
            index += 2
            continue
        if token in ("-H", "--header"):
            key, value = args[index + 1].split(":", 1)
            headers[key.strip()] = value.strip()
            index += 2
            continue
        if token in ("-s", "--scope"):
            index += 2
            continue
        if name is None:
            name = token
        else:
            rest.append(token)
        index += 1
    if name is None:
        print("add requires a name", file=sys.stderr)
        sys.exit(1)
    doc = load()
    if url is not None or transport_http:
        if url is None and rest:
            url = rest[0]
        if url is None:
            print("http add requires a url", file=sys.stderr)
            sys.exit(1)
        doc["mcpServers"][name] = {
            "type": "http", "url": url,
            "headers": headers if headers else {},
        }
    else:
        if not rest:
            print("add requires a command", file=sys.stderr)
            sys.exit(1)
        command = rest[0]
        extra = list(rest[1:])
        doc["mcpServers"][name] = {
            "type": "stdio", "command": command, "args": extra, "env": env,
        }
    save(doc)
    sys.exit(0)
print("unexpected arguments", args, file=sys.stderr)
sys.exit(2)
'''



class ProjectionHarness(unittest.TestCase):
    """Hermetic per-test environment: fake HOME, pinned PATH, temp registries."""

    SIDE = "wsl"

    def setUp(self) -> None:
        self._env_snapshot = {
            key: os.environ.get(key)
            for key in ("HOME", "CODEX_HOME", "DSH_HOME", "PATH", "FAKE_STATE", "FAKE_KIND")
        }
        self.root = Path(tempfile.mkdtemp(prefix="p0b-test-"))
        self.addCleanup(self._restore_environment)
        self.home = self.root / "home"
        (self.home / ".codex").mkdir(parents=True)
        self.dsh_home = self.home / ".dsh"
        profiles = self.dsh_home / "profiles" / "main"
        profiles.mkdir(parents=True)
        (profiles / "cordis.patch.yml").write_bytes(b"# empty bridge cordis patch\n")
        os.environ["HOME"] = str(self.home)
        os.environ["CODEX_HOME"] = str(self.home / ".codex")
        os.environ["DSH_HOME"] = str(self.dsh_home)
        os.environ.pop("FAKE_STATE", None)
        os.environ.pop("FAKE_KIND", None)
        self.set_path([])
        self.claude_json = self.home / ".claude.json"
        self.claude_json.write_text(
            json.dumps({"mcpServers": {}, "userKey": "preserved"}, ensure_ascii=False)
        )

    def _restore_environment(self) -> None:
        for key, value in self._env_snapshot.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        shutil.rmtree(self.root, ignore_errors=True)

    def set_path(self, extra_dirs) -> None:
        os.environ["PATH"] = os.pathsep.join(
            [str(item) for item in extra_dirs] + ["/usr/bin", "/bin"]
        )

    @staticmethod
    def server(server_id: str, name: str | None = None, enabled: bool = True) -> dict:
        return {
            "id": server_id,
            "name": name or server_id,
            "summary": f"summary for {server_id}",
            "command": "python",
            "args": ["-m", "memcp"],
            "process": {"multiProcessAllowed": True},
            "capabilityGroups": ["g"],
            "artifactDelivery": {"enabled": False},
            "enabled": enabled,
        }

    def make_peer(self, servers, name: str = "peer.sqlite3") -> Path:
        peer = self.root / name
        manifest = self.root / (name + ".json")
        manifest.write_text(json.dumps({"servers": servers}, ensure_ascii=False))
        Registry.initialize_database(
            peer,
            manifest,
            replace=True,
            projection_path=self.root / (name + ".proj.sqlite3"),
        )
        return peer

    def projection(self, name: str = "local.sqlite3") -> Path:
        return self.root / name

    def sync_mirror(self, peer: Path) -> None:
        projection_sync_peer(
            projection=self.projection(),
            source="registry-path",
            side=self.SIDE,
            peer_registry=peer,
        )

    def install_fake_cli(self, kind: str) -> Path:
        bindir = self.root / "bin"
        bindir.mkdir(exist_ok=True)
        target = bindir / kind
        state = self.root / f"{kind}-state.json"
        source = FAKE_CLI_SRC.replace("@PYBIN@", sys.executable)
        target.write_text(source)
        target.chmod(0o755)
        self.set_path([bindir])
        os.environ["FAKE_STATE"] = str(state)
        os.environ["FAKE_KIND"] = kind
        return state

    def scan(self, side: str | None = None):
        return projection_scan_candidates(side or self.SIDE)

    def candidate(self, kind: str, side: str | None = None) -> dict:
        for candidate in self.scan(side):
            if candidate["clientKind"] == kind:
                return candidate
        self.fail(f"no scanned candidate of kind {kind}")

    def enroll(self, candidate: dict, **kwargs) -> dict:
        return projection_enroll(
            projection=self.projection(),
            side=self.SIDE,
            candidate_id=candidate["candidateId"],
            extra_paths=(),
            launcher_command=None,
            launcher_args=None,
            confirm=True,
            **kwargs,
        )

    @staticmethod
    def with_marker(command: str, args: list, env_extra: dict | None = None):
        env = {
            "WIN_WSL_MCP_BRIDGE_OWNED": "1",
            "WIN_WSL_MCP_BRIDGE_SERVER": "unused",
        }
        if env_extra:
            env.update(env_extra)
        return {"command": command, "args": list(args), "env": env}


class ProjectionScannerOutboxEnrollTest(ProjectionHarness):
    def test_scanner_never_returns_values_or_launch_definitions(self) -> None:
        user_secret = "hunter2-do-not-leak"
        user_command = "user-mcp-cmd"
        self.claude_json.write_text(
            json.dumps(
                {
                    "mcpServers": {
                        "user-server": {
                            "type": "stdio",
                            "command": user_command,
                            "args": ["--flag"],
                            "env": {"USER_SECRET": user_secret, "TOKEN": "abc"},
                        }
                    },
                    "oauthAccount": {"tokens": "secret-material"},
                },
                ensure_ascii=False,
            )
        )
        codex_config = self.home / ".codex" / "config.toml"
        codex_config.write_text(
            "[mcp_servers]\n"
            "[mcp_servers.user-codex]\n"
            'command = "codex-mcp-cmd"\n'
            'args = ["-x"]\n'
            'env = { CODE_TOKEN = "codex-secret" }\n'
        )
        serialized = json.dumps(self.scan())
        self.assertNotIn(user_secret, serialized)
        self.assertNotIn("secret-material", serialized)
        self.assertNotIn(user_command, serialized)
        self.assertNotIn("codex-mcp-cmd", serialized)
        names = set()
        for candidate in self.scan():
            names.update(candidate["existingMcpNames"])
        self.assertIn("user-server", names)
        self.assertIn("user-codex", names)
        claude = self.candidate("claude")
        self.assertIn("may-contain-credentials", claude["notes"])
        self.assertIs(claude["mayContainSecrets"], True)

    def test_candidate_id_is_stable_and_stale_id_rejected(self) -> None:
        first = self.candidate("claude")
        second = self.candidate("claude")
        self.assertEqual(first["candidateId"], second["candidateId"])
        self.claude_json.write_text(
            json.dumps({"mcpServers": {"x": {"command": "y"}}}, ensure_ascii=False)
        )
        changed = self.candidate("claude")
        self.assertNotEqual(first["candidateId"], changed["candidateId"])
        with self.assertRaisesRegex(BridgeError, "stale or not from this scan"):
            self.enroll(first)

    def test_enrollment_requires_confirm(self) -> None:
        candidate = self.candidate("claude")
        with self.assertRaisesRegex(BridgeError, "confirm"):
            projection_enroll(
                projection=self.projection(),
                side=self.SIDE,
                candidate_id=candidate["candidateId"],
                extra_paths=(),
                launcher_command=None,
                launcher_args=None,
                confirm=False,
            )

    def test_duplicate_enrollment_is_rejected(self) -> None:
        candidate = self.candidate("claude")
        self.enroll(candidate)
        with self.assertRaisesRegex(BridgeError, "already enrolled"):
            self.enroll(candidate)

    def test_registry_init_events_and_runtime_only_noop(self) -> None:
        projection = self.root / "peer-proj.sqlite3"
        manifest = self.root / "manifest.json"
        manifest.write_text(json.dumps({"servers": [self.server("alpha"), self.server("beta")]}))
        registry = self.root / "peer.sqlite3"
        Registry.initialize_database(registry, manifest, replace=True, projection_path=projection)
        with sqlite3.connect(projection) as connection:
            events = connection.execute(
                "SELECT event_type FROM registry_projection_outbox"
            ).fetchall()
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0][0], "registry_changed")
        # runtime-only change (command/args) must not churn projection state
        manifest.write_text(
            json.dumps(
                {
                    "servers": [
                        {**self.server("alpha"), "command": "pythonX", "args": ["-o"]},
                        self.server("beta"),
                    ]
                }
            )
        )
        Registry.initialize_database(registry, manifest, replace=True, projection_path=projection)
        with sqlite3.connect(projection) as connection:
            count = connection.execute(
                "SELECT COUNT(*) FROM registry_projection_outbox"
            ).fetchone()[0]
        self.assertEqual(count, 1)
        # projection-affecting display-name change appends a second event
        manifest.write_text(
            json.dumps({"servers": [self.server("alpha", "Alpha Renamed"), self.server("beta")]})
        )
        Registry.initialize_database(registry, manifest, replace=True, projection_path=projection)
        with sqlite3.connect(projection) as connection:
            count = connection.execute(
                "SELECT COUNT(*) FROM registry_projection_outbox"
            ).fetchone()[0]
        self.assertEqual(count, 2)

    def test_failed_registry_init_leaves_projection_and_registry_unchanged(self) -> None:
        projection = self.root / "peer-proj.sqlite3"
        manifest = self.root / "manifest.json"
        manifest.write_text(json.dumps({"servers": [self.server("alpha")]}))
        registry = self.root / "peer.sqlite3"
        Registry.initialize_database(registry, manifest, replace=True, projection_path=projection)
        with sqlite3.connect(projection) as connection:
            revision = connection.execute(
                "SELECT MAX(revision) FROM registry_projection_outbox"
            ).fetchone()[0]
        manifest.write_text("{not valid json")
        with self.assertRaises(Exception):
            Registry.initialize_database(registry, manifest, replace=True, projection_path=projection)
        with sqlite3.connect(projection) as connection:
            after = connection.execute(
                "SELECT MAX(revision) FROM registry_projection_outbox"
            ).fetchone()[0]
        self.assertEqual(after, revision)
        with sqlite3.connect(registry) as connection:
            servers = connection.execute("SELECT id FROM servers").fetchall()
        self.assertEqual([row[0] for row in servers], ["alpha"])

    def test_enroll_revalidates_candidate_and_records_redacted_environment(self) -> None:
        candidate = self.candidate("claude")
        enrolled = self.enroll(candidate)
        environment = enrolled["environment"]
        self.assertTrue(environment["environmentId"].startswith("env-"))
        self.assertEqual(environment["clientKind"], "claude")
        self.assertEqual(environment["hostSide"], self.SIDE)
        self.assertEqual(environment["configPath"], str(self.claude_json))
        self.assertEqual(environment["enabled"], True)
        self.assertEqual(environment["applyAdapter"], "bridge-file")
        # stale candidate from before enrollment cannot be enrolled twice
        with self.assertRaisesRegex(BridgeError, "already enrolled"):
            self.enroll(self.candidate("claude"))


class ProjectionReconcileTest(ProjectionHarness):
    def _reconcile(self, **kwargs):
        return projection_reconcile(
            projection=self.projection(), side=self.SIDE, **kwargs
        )

    def _claude_servers(self) -> dict:
        document = json.loads(self.claude_json.read_text(encoding="utf-8"))
        return document["mcpServers"]

    def test_unsupported_transport_preserves_each_environment_and_recovers(self) -> None:
        self.sync_mirror(self.make_peer([self.server("alpha"), self.server("obsolete")]))
        paths = []
        for kind in ("claude", "codex", "dsh"):
            enrolled = self.enroll(self.candidate(kind))
            paths.append(Path(enrolled["environment"]["configPath"]))
        self.assertTrue(self._reconcile()["ok"])
        before = {path: path.read_bytes() for path in paths}
        with sqlite3.connect(self.projection()) as connection:
            fingerprints = connection.execute(
                "SELECT environment_id, server_id, entry_fingerprint "
                "FROM agent_mcp_projections ORDER BY environment_id, server_id"
            ).fetchall()

        http = {
            "id": "alpha", "name": "Alpha HTTP", "summary": "transport transition",
            "transport": {
                "type": "streamable-http", "endpoint": "http://127.0.0.1:39001/mcp",
                "headers": {"Authorization": "private-transition-secret"},
            },
        }
        self.sync_mirror(self.make_peer([http, self.server("new-server")]))
        with sqlite3.connect(self.projection()) as connection:
            before_preview = list(connection.iterdump())
        for dry_run in (True, False):
            with self.subTest(dry_run=dry_run):
                with mock.patch(
                    "bridge_runtime._apply_environment_entries",
                    return_value={"actions": [], "mode": "bridge-file"},
                ) as apply:
                    result = self._reconcile(dry_run=dry_run)
                self.assertFalse(result["ok"])
                self.assertTrue(all("unsupported_transport" in error for error in result["errors"]))
                apply.assert_not_called()
                self.assertEqual(len(result["environments"]), 3)
                for environment in result["environments"]:
                    self.assertEqual(environment["status"], "error")
                    self.assertEqual(environment["actions"], [])
                    self.assertEqual(environment["unsupportedTransports"], [{
                        "serverId": "alpha", "transport": "streamable-http",
                        "code": "unsupported_transport",
                    }])
                    self.assertIn("unsupported_transport", environment["errorDetail"])
                self.assertNotIn("private-transition-secret", json.dumps(result))
                self.assertNotIn("39001", json.dumps(result))
                self.assertEqual({path: path.read_bytes() for path in paths}, before)
                with sqlite3.connect(self.projection()) as connection:
                    self.assertEqual(connection.execute(
                        "SELECT environment_id, server_id, entry_fingerprint "
                        "FROM agent_mcp_projections ORDER BY environment_id, server_id"
                    ).fetchall(), fingerprints)
                    if dry_run:
                        self.assertEqual(list(connection.iterdump()), before_preview)
        status = projection_status(projection=self.projection())
        for projection in status["projections"]:
            self.assertEqual(projection["status"], "error")
            self.assertIn("unsupported_transport", projection["errorDetail"])

        self.sync_mirror(self.make_peer([self.server("alpha"), self.server("new-server")]))
        self.assertTrue(self._reconcile()["ok"])
        status = projection_status(projection=self.projection())
        self.assertEqual({item["serverId"] for item in status["projections"]},
                         {"alpha", "new-server"})
        self.assertTrue(all(item["errorDetail"] is None for item in status["projections"]))

    def test_http_to_stdio_projection_remains_explicit_and_supported(self) -> None:
        self.sync_mirror(self.make_peer([{
            "id": "http-one", "name": "HTTP", "summary": "converted fixture",
            "transport": {"type": "streamable-http", "endpoint": "http://127.0.0.1:39001/mcp"},
        }, self.server("stdio-one")]))
        self.enroll(self.candidate("claude"), compatibility_route="http-to-stdio")
        result = self._reconcile()
        self.assertTrue(result["ok"])
        self.assertEqual(result["environments"][0]["unsupportedTransports"], [])
        servers = self._claude_servers()
        self.assertEqual(servers["http-one"]["args"][-2:], ["connect-http", "http-one"])
        self.assertEqual(servers["stdio-one"]["args"][-2:], ["connect", "stdio-one"])

    def test_constant_two_tool_route_keeps_one_entry_per_stdio_target(self) -> None:
        self.sync_mirror(self.make_peer([self.server("alpha"), self.server("beta")]))
        self.enroll(
            self.candidate("claude"),
            compatibility_route="constant-two-tool",
        )
        result = self._reconcile()
        self.assertTrue(result["ok"])
        servers = self._claude_servers()
        self.assertEqual(sorted(servers), ["alpha", "beta"])
        for server_id, entry in servers.items():
            self.assertEqual(
                entry["args"][-2:], ["compatibility-mcp", server_id]
            )

    def test_file_mode_adds_preserves_and_removes_claude_entries(self) -> None:
        self.sync_mirror(self.make_peer([self.server("alpha"), self.server("beta")]))
        self.enroll(self.candidate("claude"))
        result = self._reconcile()
        self.assertTrue(result["ok"])
        self.assertEqual(result["environments"][0]["status"], "configured")
        servers = self._claude_servers()
        self.assertEqual(sorted(servers), ["alpha", "beta"])
        document = json.loads(self.claude_json.read_text(encoding="utf-8"))
        self.assertEqual(document["userKey"], "preserved")
        alpha = servers["alpha"]
        self.assertEqual(alpha["type"], "stdio")
        self.assertIn("WIN_WSL_MCP_BRIDGE_OWNED", alpha["env"])
        self.assertEqual(alpha["env"]["WIN_WSL_MCP_BRIDGE_SERVER"], "alpha")
        # command is the local bridge connect launcher, never the peer command
        self.assertNotEqual(alpha["command"], "python")
        self.assertIn("connect", alpha["args"])
        # idempotent second run is a no-op
        again = self._reconcile()
        self.assertTrue(again["ok"])
        self.assertEqual(again["environments"][0]["actions"], [])
        # peer entry disabled -> entry removed; unrelated data preserved
        peer = self.root / "peer.sqlite3"
        manifest = self.root / "peer.sqlite3.json"
        manifest.write_text(json.dumps({"servers": [self.server("beta"), self.server("alpha", enabled=False)]}))
        Registry.initialize_database(peer, manifest, replace=True, projection_path=self.root / "peer.sqlite3.proj.sqlite3")
        self.sync_mirror(peer)
        removed = self._reconcile()
        self.assertTrue(removed["ok"])
        self.assertEqual(sorted(self._claude_servers()), ["beta"])
        self.assertEqual(json.loads(self.claude_json.read_text())["userKey"], "preserved")

    def test_crash_at_boundary_converges_without_duplicates(self) -> None:
        # Simulate a crashed previous apply: entries already present but no DB state.
        self.sync_mirror(self.make_peer([self.server("alpha")]))
        launcher_command, launcher_args = bridge_runtime._default_launcher(self.SIDE)
        descriptor = bridge_runtime._agent_connect_entry(
            launcher_command=launcher_command,
            launcher_args=launcher_args,
            server_id="alpha",
        )
        marker = {
            "type": "stdio",
            "command": descriptor["command"],
            "args": descriptor["args"],
            "env": descriptor["env"],
        }
        document = json.loads(self.claude_json.read_text(encoding="utf-8"))
        document["mcpServers"]["alpha"] = marker
        self.claude_json.write_text(json.dumps(document, ensure_ascii=False))
        self.enroll(self.candidate("claude"))
        result = self._reconcile()
        self.assertTrue(result["ok"])
        self.assertEqual(result["environments"][0]["actions"], [])
        servers = self._claude_servers()
        self.assertEqual(list(servers), ["alpha"])

    def test_drift_is_detected_and_never_removes_user_change(self) -> None:
        self.sync_mirror(self.make_peer([self.server("alpha"), self.server("beta")]))
        self.enroll(self.candidate("claude"))
        first = self._reconcile()
        self.assertTrue(first["ok"])
        document = json.loads(self.claude_json.read_text(encoding="utf-8"))
        document["mcpServers"]["beta"]["args"] = ["--user-tampered"]
        self.claude_json.write_text(json.dumps(document, ensure_ascii=False))
        peer = self.root / "peer.sqlite3"
        manifest = self.root / "peer.sqlite3.json"
        manifest.write_text(json.dumps({"servers": [self.server("alpha"), self.server("beta", enabled=False)]}))
        Registry.initialize_database(peer, manifest, replace=True, projection_path=self.root / "peer.sqlite3.proj.sqlite3")
        self.sync_mirror(peer)
        drifted = self._reconcile()
        self.assertFalse(drifted["ok"])
        self.assertIn("drift", drifted["environments"][0]["status"])
        # user-tampered entry is retained, unmanaged value untouched
        remaining = self._claude_servers()
        self.assertIn("beta", remaining)
        self.assertEqual(remaining["beta"]["args"], ["--user-tampered"])

    def test_rollback_keeps_file_unchanged_on_write_failure(self) -> None:
        peer = self.make_peer([self.server("alpha")])
        self.sync_mirror(peer)
        self.enroll(self.candidate("claude"))
        self._reconcile()
        before = self.claude_json.read_bytes()
        # introduce a second desired server so the next reconcile must write
        manifest = self.root / "peer.sqlite3.json"
        manifest.write_text(
            json.dumps(
                {"servers": [self.server("alpha"), self.server("gamma")]}
            )
        )
        Registry.initialize_database(
            peer,
            manifest,
            replace=True,
            projection_path=self.root / "peer.sqlite3.proj.sqlite3",
        )
        self.sync_mirror(peer)
        real_replace = bridge_runtime._atomic_replace_bytes

        state = {"corrupted": False}

        def flaky_replace(target: Path, data: bytes) -> None:
            real_replace(target, data)
            if not state["corrupted"]:
                target.write_bytes(b"corrupted-after-write")
                state["corrupted"] = True

        with mock.patch(
            "bridge_runtime._atomic_replace_bytes", side_effect=flaky_replace
        ):
            result = self._reconcile()
        self.assertFalse(result["ok"])
        self.assertEqual(result["environments"][0]["status"], "error")
        # failed write was rolled back to the previous document
        self.assertEqual(self.claude_json.read_bytes(), before)

    def test_official_cli_add_list_remove_with_fake_binaries(self) -> None:
        state = self.install_fake_cli("claude")
        peer = self.make_peer([self.server("alpha"), self.server("beta")])
        self.sync_mirror(peer)
        candidate = self.candidate("claude")
        self.assertEqual(candidate["existingMcpNames"], [])
        environment = self.enroll(candidate)["environment"]
        self.assertEqual(environment["applyAdapter"], "official-cli")
        result = self._reconcile()
        self.assertTrue(result["ok"], result["errors"])
        doc = json.loads(state.read_text())
        self.assertEqual(sorted(doc["mcpServers"]), ["alpha", "beta"])
        entry = doc["mcpServers"]["alpha"]
        self.assertEqual(entry["env"]["WIN_WSL_MCP_BRIDGE_SERVER"], "alpha")
        # peer removal drives official remove after fingerprint verification
        manifest = self.root / "peer.sqlite3.json"
        manifest.write_text(json.dumps({"servers": [self.server("alpha", enabled=False), self.server("beta")]}))
        Registry.initialize_database(peer, manifest, replace=True, projection_path=self.root / "peer.sqlite3.proj.sqlite3")
        self.sync_mirror(peer)
        removed = self._reconcile()
        self.assertTrue(removed["ok"], removed["errors"])
        self.assertEqual(sorted(json.loads(state.read_text())["mcpServers"]), ["beta"])

    def test_official_cli_drift_never_removes_tampered_entry(self) -> None:
        state = self.install_fake_cli("claude")
        self.sync_mirror(self.make_peer([self.server("alpha")]))
        self.enroll(self.candidate("claude"))
        self._reconcile()
        doc = json.loads(state.read_text())
        doc["mcpServers"]["alpha"]["args"] = ["--user-edited"]
        state.write_text(json.dumps(doc, ensure_ascii=False))
        peer = self.root / "peer.sqlite3"
        manifest = self.root / "peer.sqlite3.json"
        manifest.write_text(json.dumps({"servers": [self.server("alpha", enabled=False)]}))
        Registry.initialize_database(peer, manifest, replace=True, projection_path=self.root / "peer.sqlite3.proj.sqlite3")
        self.sync_mirror(peer)
        drifted = self._reconcile()
        self.assertFalse(drifted["ok"])
        final = json.loads(state.read_text())
        self.assertIn("alpha", final["mcpServers"])
        self.assertEqual(final["mcpServers"]["alpha"]["args"], ["--user-edited"])

    def test_codex_owned_document_mode_when_cli_absent(self) -> None:
        config = self.home / ".codex" / "config.toml"
        self.sync_mirror(self.make_peer([self.server("alpha")]))
        candidate = self.candidate("codex")
        self.enroll(candidate)
        result = self._reconcile()
        self.assertTrue(result["ok"], result["errors"])
        self.assertTrue(config.is_file())
        text = config.read_text(encoding="utf-8")
        self.assertIn("# WIN-WSL-MCP-BRIDGE-MANAGED", text)
        self.assertIn("alpha", text)
        # regeneration preserves the whole owned document and can remove entries
        peer = self.root / "peer.sqlite3"
        manifest = self.root / "peer.sqlite3.json"
        manifest.write_text(json.dumps({"servers": [self.server("alpha", enabled=False)]}))
        Registry.initialize_database(peer, manifest, replace=True, projection_path=self.root / "peer.sqlite3.proj.sqlite3")
        self.sync_mirror(peer)
        removed = self._reconcile()
        self.assertTrue(removed["ok"])
        self.assertNotIn("alpha", config.read_text(encoding="utf-8"))

    def test_unmanaged_codex_config_without_cli_is_per_environment_error(self) -> None:
        config = self.home / ".codex" / "config.toml"
        config.write_text('[mcp_servers]\n[mcp_servers.user-thing]\ncommand = "keep"\n')
        self.sync_mirror(self.make_peer([self.server("alpha")]))
        candidate = self.candidate("codex")
        self.enroll(candidate)
        result = self._reconcile()
        self.assertFalse(result["ok"])
        self.assertEqual(result["environments"][0]["status"], "error")
        original = config.read_text(encoding="utf-8")
        self.assertNotIn("alpha", original)
        self.assertIn("user-thing", original)

    def test_dsh_overlay_projection_and_removal(self) -> None:
        from bridge_runtime import CLIENT_KIND_DSH

        profile = self.dsh_home / "profiles" / "main"
        self.sync_mirror(self.make_peer([self.server("alpha"), self.server("beta")]))
        candidates = self.scan()
        overlay = [c for c in candidates if c["clientKind"] == CLIENT_KIND_DSH]
        dsh_candidates = [c for c in overlay if c["configPath"].endswith("cordis-bridge-overlay.json")]
        self.assertEqual(len(dsh_candidates), 1, overlay)
        candidate = dsh_candidates[0]
        self.assertFalse(candidate["exists"])
        self.enroll(candidate)
        result = self._reconcile()
        self.assertTrue(result["ok"], result["errors"])
        self.assertEqual(result["environments"][0]["status"], "next_session")
        overlay_file = profile / "cordis-bridge-overlay.json"
        self.assertTrue(overlay_file.is_file())
        patches = json.loads(overlay_file.read_text(encoding="utf-8"))
        inserted = []
        for patch in patches:
            for item in patch.get("insert", []):
                inserted.append(item["config"]["serverName"])
        self.assertEqual(sorted(inserted), ["alpha", "beta"])
        self.assertTrue((profile / "cordis.patch.yml").is_file())
        # peer disable removes the overlay entry
        peer = self.root / "peer.sqlite3"
        manifest = self.root / "peer.sqlite3.json"
        manifest.write_text(json.dumps({"servers": [self.server("alpha", enabled=False), self.server("beta")]}))
        Registry.initialize_database(peer, manifest, replace=True, projection_path=self.root / "peer.sqlite3.proj.sqlite3")
        self.sync_mirror(peer)
        removed = self._reconcile()
        self.assertTrue(removed["ok"])
        patches = json.loads(overlay_file.read_text(encoding="utf-8"))
        inserted = [
            item["config"]["serverName"]
            for patch in patches
            for item in patch.get("insert", [])
        ]
        self.assertEqual(inserted, ["beta"])

    def test_per_environment_failure_isolation(self) -> None:
        config = self.home / ".codex" / "config.toml"
        config.write_text('[mcp_servers]\n[mcp_servers.user-thing]\ncommand = "keep"\n')
        self.sync_mirror(self.make_peer([self.server("alpha")]))
        self.enroll(self.candidate("codex"))
        self.enroll(self.candidate("claude"))
        result = self._reconcile()
        statuses = [environment["status"] for environment in result["environments"]]
        self.assertIn("error", statuses)  # codex unowned, no CLI
        self.assertIn("configured", statuses)  # claude file mode succeeded
        self.assertTrue("alpha" in self._claude_servers())

    def test_two_environment_secrets_never_surface_in_status(self) -> None:
        self.claude_json.write_text(
            json.dumps(
                {
                    "mcpServers": {
                        "user-server": {
                            "type": "stdio",
                            "command": "private-cmd",
                            "env": {"SECRET": "do-not-leak-me"},
                        }
                    }
                },
                ensure_ascii=False,
            )
        )
        self.sync_mirror(self.make_peer([self.server("alpha")]))
        self.enroll(self.candidate("claude"))
        result = self._reconcile()
        self.assertTrue(result["ok"])
        self.assertIn("user-server", self._claude_servers())
        status = json.dumps(projection_status(projection=self.projection()))
        result_json = json.dumps(result)
        self.assertNotIn("do-not-leak-me", status)
        self.assertNotIn("private-cmd", status)
        self.assertNotIn("do-not-leak-me", result_json)
        self.assertNotIn("private-cmd", result_json)

    def test_watch_polling_runs_bounded_rounds(self) -> None:
        self.sync_mirror(self.make_peer([self.server("alpha")]))
        self.enroll(self.candidate("claude"))
        code = projection_watch(
            projection=self.projection(),
            side=self.SIDE,
            refresh_source="registry-path",
            peer_registry=self.root / "peer.sqlite3",
            local_host="127.0.0.1",
            local_port=8769,
            interval_seconds=0.01,
            max_rounds=2,
        )
        self.assertEqual(code, 0)
        self.assertIn("alpha", self._claude_servers())

    def test_unenroll_keep_entries_stops_sync(self) -> None:
        self.sync_mirror(self.make_peer([self.server("alpha")]))
        environment = self.enroll(self.candidate("claude"))["environment"]
        self._reconcile()
        environment_id = environment["environmentId"]
        result = projection_unenroll(
            projection=self.projection(),
            environment_id=environment_id,
            remove_entries=False,
            confirm=True,
        )
        self.assertTrue(result["ok"])
        # reconciliation no longer touches the disabled environment
        outcome = self._reconcile()
        self.assertEqual(len(outcome["environments"]), 0)
        self.assertIn("alpha", self._claude_servers())

    def test_unenroll_remove_entries_deletes_owned_only(self) -> None:
        self.sync_mirror(self.make_peer([self.server("alpha")]))
        environment = self.enroll(self.candidate("claude"))["environment"]
        self._reconcile()
        environment_id = environment["environmentId"]
        result = projection_unenroll(
            projection=self.projection(),
            environment_id=environment_id,
            remove_entries=True,
            confirm=True,
        )
        self.assertTrue(result["ok"])
        self.assertEqual(self._claude_servers(), {})
        self.assertEqual(json.loads(self.claude_json.read_text())["userKey"], "preserved")

    def test_status_reports_environments_mirror_and_outbox(self) -> None:
        peer = self.make_peer([self.server("alpha")])
        self.sync_mirror(peer)
        self.enroll(self.candidate("claude"))
        self._reconcile()
        status = projection_status(projection=self.projection())
        self.assertEqual([item["serverId"] for item in status["mirrorServers"]], ["alpha"])
        self.assertEqual(status["outboxUnprocessed"], 0)
        self.assertEqual(len(status["environments"]), 1)
        projections = status["projections"]
        self.assertEqual(len(projections), 1)
        self.assertEqual(projections[0]["serverId"], "alpha")
        self.assertEqual(projections[0]["exposedName"], "alpha")



class ProjectionNativeHttpTest(ProjectionHarness):
    """P6 native Streamable HTTP projection: relay enrollment, per-client
    descriptors, ownership, collisions, drift, and transport transitions."""

    RELAY = "http://127.0.0.1:8877"

    def http_server(self, server_id, name=None, enabled=True) -> dict:
        return {
            "id": server_id,
            "name": name or server_id,
            "summary": f"http summary for {server_id}",
            "transport": {
                "type": "streamable-http",
                "endpoint": f"http://127.0.0.1:39001/mcp/{server_id}",
            },
            "enabled": enabled,
        }

    def _reconcile(self, **kwargs):
        return projection_reconcile(
            projection=self.projection(), side=self.SIDE, **kwargs
        )

    def enroll_http(self, kind: str, candidate: dict | None = None, relay: str = RELAY) -> dict:
        return self.enroll(
            candidate if candidate is not None else self.candidate(kind),
            transport_capabilities={"stdio": True, "streamable-http": True},
            compatibility_route="native",
            relay_base_url=relay,
        )

    def _set_env(self, sql, environment_id: str, **columns) -> None:
        with sqlite3.connect(self.projection()) as connection:
            assignments = ", ".join(f"{key} = ?" for key in columns)
            connection.execute(
                f"UPDATE agent_environments SET {assignments}, updated_at_ns = ? "
                "WHERE environment_id = ?",
                (*columns.values(), time.time_ns(), environment_id),
            )

    def test_enroll_native_http_requires_and_validates_relay_url(self) -> None:
        codex = self.candidate("codex")
        http_caps = {"stdio": True, "streamable-http": True}
        with self.assertRaisesRegex(BridgeError, "--relay-url"):
            self.enroll(
                codex,
                transport_capabilities=http_caps,
                compatibility_route="native",
            )
        with self.assertRaisesRegex(BridgeError, "only meaningful"):
            # relay base without native-http capability is rejected too
            self.enroll(codex, relay_base_url=self.RELAY)
        for bad in (
            "http://10.0.0.5:8877",
            "https://example.com:8877",
            "http://127.0.0.1",
            "http://127.0.0.1:70000",
            "http://127.0.0.1:8877/path",
            "http://127.0.0.1:8877?q=1",
            "ftp://127.0.0.1:8877",
        ):
            with self.subTest(bad=bad):
                with self.assertRaises(BridgeError):
                    self.enroll(
                        codex,
                        transport_capabilities=http_caps,
                        compatibility_route="native",
                        relay_base_url=bad,
                    )
        # trailing slash and localhost are accepted and normalized
        enrolled = self.enroll_http("codex", codex, relay="http://localhost:8877/")
        self.assertEqual(enrolled["environment"]["relayBaseUrl"], "http://localhost:8877")

    def test_codex_owned_document_native_http_mixed_projection(self) -> None:
        config = self.home / ".codex" / "config.toml"
        self.sync_mirror(self.make_peer(
            [self.http_server("alpha"), self.server("beta")]
        ))
        self.enroll_http("codex")
        result = self._reconcile()
        self.assertTrue(result["ok"], result["errors"])
        self.assertEqual(result["environments"][0]["status"], "configured")
        text = config.read_text(encoding="utf-8")
        self.assertIn("# WIN-WSL-MCP-BRIDGE-MANAGED", text)
        # native HTTP entry: url only, exact deterministic relay mount
        self.assertIn(f'url = "http://127.0.0.1:8877/mcp/alpha"', text)
        # stdio entry keeps command/args/env in the same owned document
        self.assertIn("[mcp_servers.beta]", text)
        self.assertIn("command =", text)
        entries = bridge_runtime._codex_owned_document_entries(text)
        self.assertEqual(entries["alpha"]["url"], "http://127.0.0.1:8877/mcp/alpha")
        self.assertIn("connect", entries["beta"]["args"])
        # idempotent second run changes nothing
        before = config.read_bytes()
        again = self._reconcile()
        self.assertTrue(again["ok"])
        self.assertEqual(again["environments"][0]["actions"], [])
        self.assertEqual(config.read_bytes(), before)

    def test_native_http_transport_transition_rewrites_atomically(self) -> None:
        config = self.home / ".codex" / "config.toml"
        peer = self.make_peer([self.http_server("alpha")])
        self.sync_mirror(peer)
        self.enroll_http("codex")
        self._reconcile()
        self.assertIn('url = "http://127.0.0.1:8877/mcp/alpha"', config.read_text())

        # peer switches alpha to stdio: one rewrite drops the url section and
        # writes the launcher entry under the same name (no name lost)
        manifest = self.root / "peer.sqlite3.json"
        manifest.write_text(json.dumps({"servers": [self.server("alpha")]}))
        Registry.initialize_database(
            peer, manifest, replace=True,
            projection_path=self.root / "peer.sqlite3.proj.sqlite3",
        )
        self.sync_mirror(peer)
        transition = self._reconcile()
        self.assertTrue(transition["ok"], transition["errors"])
        entries = bridge_runtime._codex_owned_document_entries(
            config.read_text(encoding="utf-8")
        )
        self.assertEqual(list(entries), ["alpha"])
        self.assertNotIn("url", entries["alpha"])
        self.assertIn("connect", entries["alpha"]["args"])
        again = self._reconcile()
        self.assertEqual(again["environments"][0]["actions"], [])

        # and back to native http over the same name
        manifest.write_text(json.dumps({"servers": [self.http_server("alpha")]}))
        Registry.initialize_database(
            peer, manifest, replace=True,
            projection_path=self.root / "peer.sqlite3.proj.sqlite3",
        )
        self.sync_mirror(peer)
        back = self._reconcile()
        self.assertTrue(back["ok"], back["errors"])
        entries = bridge_runtime._codex_owned_document_entries(
            config.read_text(encoding="utf-8")
        )
        self.assertEqual(list(entries), ["alpha"])
        self.assertEqual(entries["alpha"]["url"], "http://127.0.0.1:8877/mcp/alpha")
        self.assertNotIn("command", entries["alpha"])
        again = self._reconcile()
        self.assertEqual(again["environments"][0]["actions"], [])

    def test_url_drift_blocks_removal_and_preserves_stored_fingerprint(self) -> None:
        config = self.home / ".codex" / "config.toml"
        peer = self.make_peer([self.http_server("alpha")])
        self.sync_mirror(peer)
        self.enroll_http("codex")
        self._reconcile()
        with sqlite3.connect(self.projection()) as connection:
            stored = dict(connection.execute(
                "SELECT server_id, entry_fingerprint FROM agent_mcp_projections"
            ).fetchall())
        self.assertEqual(list(stored), ["alpha"])
        # user edits the URL away from the deterministic relay mount
        edited = config.read_text(encoding="utf-8").replace(
            "http://127.0.0.1:8877/mcp/alpha", "http://127.0.0.1:9999/custom"
        )
        config.write_text(edited, encoding="utf-8")
        manifest = self.root / "peer.sqlite3.json"
        manifest.write_text(json.dumps({"servers": [self.http_server("alpha", enabled=False)]}))
        Registry.initialize_database(
            peer, manifest, replace=True,
            projection_path=self.root / "peer.sqlite3.proj.sqlite3",
        )
        self.sync_mirror(peer)
        # codex owned documents are read whole: the tampered URL mismatches the
        # stored fingerprint, so removal is blocked as drift, never performed
        result = self._reconcile()
        self.assertFalse(result["ok"])
        self.assertEqual(result["environments"][0]["status"], "drift")
        self.assertIn("alpha", result["environments"][0]["drift"])
        self.assertIn("http://127.0.0.1:9999/custom", config.read_text())
        with sqlite3.connect(self.projection()) as connection:
            self.assertEqual(dict(connection.execute(
                "SELECT server_id, entry_fingerprint FROM agent_mcp_projections"
            ).fetchall()), stored)

    def test_rollback_keeps_transition_file_unchanged_on_write_failure(self) -> None:
        config = self.home / ".codex" / "config.toml"
        peer = self.make_peer([self.http_server("alpha")])
        self.sync_mirror(peer)
        self.enroll_http("codex")
        self._reconcile()
        before = config.read_bytes()
        manifest = self.root / "peer.sqlite3.json"
        manifest.write_text(json.dumps({"servers": [self.server("alpha")]}))
        Registry.initialize_database(
            peer, manifest, replace=True,
            projection_path=self.root / "peer.sqlite3.proj.sqlite3",
        )
        self.sync_mirror(peer)
        with mock.patch(
            "bridge_runtime._atomic_replace_bytes",
            side_effect=BridgeError("readback mismatch"),
        ):
            failed = self._reconcile()
        self.assertFalse(failed["ok"])
        self.assertEqual(config.read_bytes(), before)

    def test_claude_file_native_http_preserves_unmanaged_fields_and_round_trips(self) -> None:
        self.sync_mirror(self.make_peer([self.http_server("alpha")]))
        self.enroll_http("claude")
        result = self._reconcile()
        self.assertTrue(result["ok"], result["errors"])
        self.assertEqual(result["environments"][0]["status"], "configured")
        document = json.loads(self.claude_json.read_text(encoding="utf-8"))
        self.assertEqual(document["userKey"], "preserved")
        alpha = document["mcpServers"]["alpha"]
        self.assertEqual(alpha["type"], "http")
        self.assertEqual(alpha["url"], "http://127.0.0.1:8877/mcp/alpha")
        self.assertNotIn("headers", alpha)
        again = self._reconcile()
        self.assertTrue(again["ok"])
        self.assertEqual(again["environments"][0]["actions"], [])

    def test_unmanaged_same_name_http_server_is_a_collision_never_overwritten(self) -> None:
        user_http = {
            "type": "http",
            "url": "http://example.com/mcp",
            "headers": {"Authorization": "user-secret-header"},
        }
        self.claude_json.write_text(json.dumps({
            "mcpServers": {"alpha": user_http},
            "userKey": "preserved",
        }, ensure_ascii=False))
        self.sync_mirror(self.make_peer([self.http_server("alpha")]))
        self.enroll_http("claude")
        result = self._reconcile()
        self.assertFalse(result["ok"])
        environment = result["environments"][0]
        self.assertEqual(environment["status"], "error")
        self.assertIn("alpha", environment["conflicts"])
        document = json.loads(self.claude_json.read_text(encoding="utf-8"))
        self.assertEqual(document["mcpServers"]["alpha"], user_http)
        self.assertEqual(document["userKey"], "preserved")
        self.assertNotIn("user-secret-header", json.dumps(result))

    def test_claude_http_url_drift_keeps_user_edit_when_peer_drops(self) -> None:
        self.sync_mirror(self.make_peer([self.http_server("alpha")]))
        self.enroll_http("claude")
        self._reconcile()
        document = json.loads(self.claude_json.read_text(encoding="utf-8"))
        document["mcpServers"]["alpha"]["url"] = "http://127.0.0.1:9999/user-url"
        self.claude_json.write_text(json.dumps(document, ensure_ascii=False))
        peer = self.root / "peer.sqlite3"
        manifest = self.root / "peer.sqlite3.json"
        manifest.write_text(json.dumps({"servers": [self.http_server("alpha", enabled=False)]}))
        Registry.initialize_database(
            peer, manifest, replace=True,
            projection_path=self.root / "peer.sqlite3.proj.sqlite3",
        )
        self.sync_mirror(peer)
        with sqlite3.connect(self.projection()) as connection:
            stored = dict(connection.execute(
                "SELECT server_id, entry_fingerprint FROM agent_mcp_projections"
            ).fetchall())
        result = self._reconcile()
        self.assertTrue(result["ok"], result["errors"])
        final = json.loads(self.claude_json.read_text(encoding="utf-8"))
        self.assertEqual(
            final["mcpServers"]["alpha"]["url"], "http://127.0.0.1:9999/user-url"
        )
        with sqlite3.connect(self.projection()) as connection:
            self.assertEqual(dict(connection.execute(
                "SELECT server_id, entry_fingerprint FROM agent_mcp_projections"
            ).fetchall()), stored)

    def test_official_cli_native_http_argv_shapes_and_round_trip(self) -> None:
        from bridge_runtime import (
            _cli_add_argv, _cli_get_argv, _cli_remove_argv,
            _cli_get_entry, _cli_parse_claude_text,
        )
        # evidence-backed real client syntax (Codex/Claude verified flags)
        self.assertEqual(
            _cli_add_argv(
                "codex",
                {"name": "alpha", "url": "http://127.0.0.1:8877/mcp/alpha", "headers": {}},
            ),
            ["codex", "mcp", "add", "alpha", "--url", "http://127.0.0.1:8877/mcp/alpha"],
        )
        self.assertEqual(
            _cli_add_argv(
                "claude",
                {"name": "alpha", "url": "http://127.0.0.1:8877/mcp/alpha", "headers": {}},
                scope="user",
            ),
            ["claude", "mcp", "add", "--scope", "user", "--transport", "http",
             "alpha", "http://127.0.0.1:8877/mcp/alpha"],
        )
        self.assertEqual(_cli_get_argv("codex", "alpha"),
                         ["codex", "mcp", "get", "--json", "alpha"])
        self.assertEqual(_cli_get_argv("claude", "alpha"), ["claude", "mcp", "get", "alpha"])
        self.assertEqual(_cli_remove_argv("claude", "alpha", scope="user"),
                         ["claude", "mcp", "remove", "alpha", "--scope", "user"])
        # codex nested-transport get parse
        parsed = _cli_get_entry(
            json.dumps({"name": "alpha", "transport": {
                "type": "streamable_http", "url": "http://127.0.0.1:8877/mcp/alpha",
            }}),
            "alpha",
        )
        self.assertEqual(parsed["url"], "http://127.0.0.1:8877/mcp/alpha")
        # real Claude text get: Type/URL lines parse to the canonical http entry
        text = "alpha:\n  Type: http\n  URL: http://127.0.0.1:8877/mcp/alpha\n"
        self.assertEqual(
            _cli_parse_claude_text(text, "alpha"),
            {"url": "http://127.0.0.1:8877/mcp/alpha", "headers": {}},
        )

    def test_official_cli_native_http_projection_with_fake_claude(self) -> None:
        state = self.install_fake_cli("claude")
        self.sync_mirror(self.make_peer([self.http_server("alpha")]))
        candidate = self.candidate("claude")
        environment = self.enroll_http("claude", candidate)["environment"]
        self.assertEqual(environment["applyAdapter"], "official-cli")
        result = self._reconcile()
        self.assertTrue(result["ok"], result["errors"])
        doc = json.loads(state.read_text())
        entry = doc["mcpServers"]["alpha"]
        self.assertEqual(entry["type"], "http")
        self.assertEqual(entry["url"], "http://127.0.0.1:8877/mcp/alpha")
        again = self._reconcile()
        self.assertTrue(again["ok"])
        self.assertEqual(again["environments"][0]["actions"], [])
        # peer drop removes the owned HTTP entry after fingerprint verification
        peer = self.root / "peer.sqlite3"
        manifest = self.root / "peer.sqlite3.json"
        manifest.write_text(json.dumps({"servers": [self.http_server("alpha", enabled=False)]}))
        Registry.initialize_database(
            peer, manifest, replace=True,
            projection_path=self.root / "peer.sqlite3.proj.sqlite3",
        )
        self.sync_mirror(peer)
        removed = self._reconcile()
        self.assertTrue(removed["ok"], removed["errors"])
        self.assertEqual(json.loads(state.read_text())["mcpServers"], {})

    def test_native_http_missing_relay_or_unsupported_target_fails_closed(self) -> None:
        # env records http capability but no relay base: http targets unsupported,
        # whole environment errors before any add/remove and preserves the file
        config = self.home / ".codex" / "config.toml"
        self.sync_mirror(self.make_peer([self.http_server("alpha")]))
        environment = self.enroll_http("codex")["environment"]
        # drop the relay base to simulate a v2-era / operator omission
        self._set_env(
            self.projection(), environment["environmentId"], relay_base_url=""
        )
        result = self._reconcile()
        self.assertFalse(result["ok"])
        env_result = result["environments"][0]
        self.assertEqual(env_result["status"], "error")
        self.assertEqual(env_result["unsupportedTransports"], [{
            "serverId": "alpha", "transport": "streamable-http",
            "code": "unsupported_transport",
        }])
        self.assertFalse(config.exists())
        self.assertEqual(env_result["actions"], [])

    def test_dsh_overlay_native_http_render_and_removal(self) -> None:
        from bridge_runtime import CLIENT_KIND_DSH

        profile = self.dsh_home / "profiles" / "main"
        self.sync_mirror(self.make_peer([self.http_server("alpha")]))
        candidates = self.scan()
        overlay = [
            c for c in candidates
            if c["clientKind"] == CLIENT_KIND_DSH
            and c["configPath"].endswith("cordis-bridge-overlay.json")
        ]
        self.assertEqual(len(overlay), 1)
        self.enroll_http(CLIENT_KIND_DSH, overlay[0])
        result = self._reconcile()
        self.assertTrue(result["ok"], result["errors"])
        overlay_file = profile / "cordis-bridge-overlay.json"
        patches = json.loads(overlay_file.read_text(encoding="utf-8"))
        items = [
            item
            for patch in patches
            for item in patch.get("insert", [])
        ]
        self.assertEqual(len(items), 1)
        config = items[0]["config"]
        self.assertEqual(items[0]["id"], "mcp-alpha")
        self.assertEqual(config["serverName"], "alpha")
        self.assertEqual(config["transport"], "streamable-http")
        self.assertEqual(config["url"], "http://127.0.0.1:8877/mcp/alpha")
        self.assertNotIn("headers", config)
        before = overlay_file.read_bytes()
        again = self._reconcile()
        self.assertTrue(again["ok"])
        self.assertEqual(again["environments"][0]["actions"], [])
        self.assertEqual(overlay_file.read_bytes(), before)
        # removal of the peer entry empties the overlay
        peer = self.root / "peer.sqlite3"
        manifest = self.root / "peer.sqlite3.json"
        manifest.write_text(json.dumps({"servers": [self.http_server("alpha", enabled=False)]}))
        Registry.initialize_database(
            peer, manifest, replace=True,
            projection_path=self.root / "peer.sqlite3.proj.sqlite3",
        )
        self.sync_mirror(peer)
        removed = self._reconcile()
        self.assertTrue(removed["ok"], removed["errors"])
        self.assertEqual(json.loads(overlay_file.read_text(encoding="utf-8")), [])

    def test_schema_v2_to_v4_migration_preserves_enrolled_rows(self) -> None:
        # a fully enrolled environment survives the v3 relay_base_url and the
        # v4 stdio-http-endpoints migrations (drop both, rewind to v2)
        self.sync_mirror(self.make_peer([self.server("alpha")]))
        environment = self.enroll(self.candidate("claude"))["environment"]
        environment_id = environment["environmentId"]
        self._reconcile()
        path = self.projection()
        with sqlite3.connect(path) as connection:
            connection.execute(
                "ALTER TABLE agent_environments DROP COLUMN relay_base_url"
            )
            connection.execute(
                "ALTER TABLE agent_environments DROP COLUMN stdio_http_endpoints_json"
            )
            connection.execute("PRAGMA user_version = 2")
        ProjectionDatabase.ensure(path)
        with ProjectionDatabase(path)._connect() as connection:
            row = ProjectionDatabase(path).environment_row(connection, environment_id)
        self.assertIsNotNone(row)
        summary = bridge_runtime._environment_summary(row)
        self.assertEqual(summary["relayBaseUrl"], "")
        self.assertEqual(summary["stdioHttpEndpoints"], {})
        self.assertEqual(summary["environmentId"], environment_id)
        self.assertEqual(summary["clientKind"], "claude")
        self.assertEqual(summary["transportCapabilities"]["stdio"], True)
        # migration leaves the environment reconcilable
        result = self._reconcile()
        self.assertTrue(result["ok"], result["errors"])
        self.assertEqual(result["environments"][0]["actions"], [])

    def test_unmanaged_fields_survive_claude_http_apply(self) -> None:
        # unrelated mcpServers (stdio and http) plus top-level keys round-trip
        self.claude_json.write_text(json.dumps({
            "mcpServers": {
                "user-stdio": {"type": "stdio", "command": "keep-me", "args": []},
                "user-http": {"type": "http", "url": "http://user.example/mcp",
                              "headers": {"X-User": "1"}},
            },
            "userKey": "preserved",
            "machines": {"deep": {"nested": True}},
        }, ensure_ascii=False))
        self.sync_mirror(self.make_peer([self.http_server("alpha")]))
        # cli absent -> shared file is not bridge-owned onboarding: official
        # requires the CLI; instead run the file path through the adapter
        # directly to prove only alpha is added and user servers are untouched
        from bridge_runtime import _claude_json_document, _claude_server_value
        entries, document = _claude_json_document(
            self.claude_json.read_text(encoding="utf-8")
        )
        self.assertIn("user-http", entries)
        servers = document["mcpServers"]
        servers["alpha"] = _claude_server_value(
            {"url": "http://127.0.0.1:8877/mcp/alpha", "headers": {}}
        )
        self.claude_json.write_text(json.dumps(document, ensure_ascii=False))
        document = json.loads(self.claude_json.read_text(encoding="utf-8"))
        self.assertEqual(document["mcpServers"]["user-stdio"]["command"], "keep-me")
        self.assertEqual(document["mcpServers"]["user-http"]["url"], "http://user.example/mcp")
        self.assertEqual(document["mcpServers"]["user-http"]["headers"], {"X-User": "1"})
        self.assertEqual(document["userKey"], "preserved")
        self.assertEqual(document["machines"], {"deep": {"nested": True}})
        self.assertEqual(document["mcpServers"]["alpha"]["url"],
                         "http://127.0.0.1:8877/mcp/alpha")



    def test_same_name_header_edit_fails_closed_then_transition_after_revert(self) -> None:
        # user adds a header to an otherwise expected relay-URL http entry while
        # the mirror keeps the same http target: update is blocked as drift and
        # the user edit plus the stored fingerprint survive.
        peer = self.make_peer([self.http_server("alpha")])
        self.sync_mirror(peer)
        self.enroll_http("claude")
        self._reconcile()
        original = self.claude_json.read_bytes()
        with sqlite3.connect(self.projection()) as connection:
            stored_before = dict(connection.execute(
                "SELECT server_id, entry_fingerprint FROM agent_mcp_projections"
            ).fetchall())
        self.assertEqual(list(stored_before), ["alpha"])
        document = json.loads(original.decode("utf-8"))
        document["mcpServers"]["alpha"]["headers"] = {"X-User": "edited"}
        self.claude_json.write_text(json.dumps(document, ensure_ascii=False))
        # same mirror target: desired entry unchanged, current differs -> drift
        drifted = self._reconcile()
        self.assertFalse(drifted["ok"])
        env_result = drifted["environments"][0]
        self.assertEqual(env_result["status"], "drift")
        self.assertIn("alpha", env_result["drift"])
        self.assertEqual(env_result["actions"], [])
        kept = json.loads(self.claude_json.read_text(encoding="utf-8"))
        self.assertEqual(kept["mcpServers"]["alpha"]["headers"], {"X-User": "edited"})
        with sqlite3.connect(self.projection()) as connection:
            self.assertEqual(dict(connection.execute(
                "SELECT server_id, entry_fingerprint FROM agent_mcp_projections"
            ).fetchall()), stored_before)
        # revert the header, then the transport transition may proceed
        self.claude_json.write_bytes(original)
        self.sync_mirror(peer)  # unchanged mirror, idempotent recovery first
        recovered = self._reconcile()
        self.assertTrue(recovered["ok"], recovered["errors"])
        self.assertEqual(recovered["environments"][0]["actions"], [])
        manifest = self.root / "peer.sqlite3.json"
        manifest.write_text(json.dumps({"servers": [self.server("alpha")]}))
        Registry.initialize_database(
            peer, manifest, replace=True,
            projection_path=self.root / "peer.sqlite3.proj.sqlite3",
        )
        self.sync_mirror(peer)
        transition = self._reconcile()
        self.assertTrue(transition["ok"], transition["errors"])
        final = json.loads(self.claude_json.read_text(encoding="utf-8"))
        self.assertEqual(final["mcpServers"]["alpha"]["type"], "stdio")
        self.assertIn("connect", final["mcpServers"]["alpha"]["args"])
        self.assertEqual(final["userKey"], "preserved")

    def test_same_name_stdio_env_edit_blocks_transition_then_transition(self) -> None:
        # user edits args/env of a Bridge stdio entry; the mirror switches that
        # server to native http -> drift, never a blind overwrite
        peer = self.make_peer([self.server("beta")])
        self.sync_mirror(peer)
        self.enroll_http("claude")
        self._reconcile()
        original = self.claude_json.read_bytes()
        with sqlite3.connect(self.projection()) as connection:
            stored_before = dict(connection.execute(
                "SELECT server_id, entry_fingerprint FROM agent_mcp_projections"
            ).fetchall())
        document = json.loads(original.decode("utf-8"))
        document["mcpServers"]["beta"]["args"] = document["mcpServers"]["beta"]["args"] + ["--user"]
        self.claude_json.write_text(json.dumps(document, ensure_ascii=False))
        manifest = self.root / "peer.sqlite3.json"
        manifest.write_text(json.dumps({"servers": [self.http_server("beta")]}))
        Registry.initialize_database(
            peer, manifest, replace=True,
            projection_path=self.root / "peer.sqlite3.proj.sqlite3",
        )
        self.sync_mirror(peer)
        drifted = self._reconcile()
        self.assertFalse(drifted["ok"])
        env_result = drifted["environments"][0]
        self.assertEqual(env_result["status"], "drift")
        self.assertIn("beta", env_result["drift"])
        kept = json.loads(self.claude_json.read_text(encoding="utf-8"))
        self.assertIn("--user", kept["mcpServers"]["beta"]["args"])
        with sqlite3.connect(self.projection()) as connection:
            self.assertEqual(dict(connection.execute(
                "SELECT server_id, entry_fingerprint FROM agent_mcp_projections"
            ).fetchall()), stored_before)
        # revert the user edit: transport transition now applies cleanly
        self.claude_json.write_bytes(original)
        transition = self._reconcile()
        self.assertTrue(transition["ok"], transition["errors"])
        final = json.loads(self.claude_json.read_text(encoding="utf-8"))
        self.assertEqual(final["mcpServers"]["beta"]["type"], "http")
        self.assertEqual(
            final["mcpServers"]["beta"]["url"], "http://127.0.0.1:8877/mcp/beta"
        )

    def test_official_cli_unparseable_get_blocks_add_not_missing(self) -> None:
        # a listed name whose official `get` output cannot be parsed must never
        # be treated as missing: reconcile fails closed instead of blind adding
        state = self.install_fake_cli("claude")
        target = self.root / "bin" / "claude"
        target.write_text(
            '#!/usr/bin/env python3\n'
            'import json, os, sys\n'
            'STATE = os.environ["FAKE_STATE"]\n'
            'def load():\n'
            '    try:\n'
            '        return json.load(open(STATE))\n'
            '    except Exception:\n'
            '        return {"mcpServers": {}}\n'
            'args = sys.argv[1:]\n'
            'if args[:2] == ["mcp", "list"]:\n'
            '    print(json.dumps({"mcpServers": [{"name": "alpha"}]}))\n'
            '    sys.exit(0)\n'
            'if args[:2] == ["mcp", "get"]:\n'
            '    print("Type: http\\nURL: (unparseable garbage)")\n'
            '    sys.exit(0)\n'
            'sys.exit(2)\n'
        )
        target.chmod(0o755)
        self.sync_mirror(self.make_peer([self.http_server("alpha")]))
        self.enroll_http("claude")
        result = self._reconcile()
        self.assertFalse(result["ok"])
        env_result = result["environments"][0]
        self.assertEqual(env_result["status"], "error")
        self.assertIn("alpha", env_result["conflicts"])
        self.assertEqual(env_result["actions"], [])
        # nothing was added to the client's own store (the state file is only
        # created by a mutation, so its absence proves no add/update happened)
        self.assertFalse(state.exists())
        with sqlite3.connect(self.projection()) as connection:
            rows = connection.execute(
                "SELECT entry_fingerprint FROM agent_mcp_projections"
            ).fetchall()
        self.assertTrue(all(row[0] == "" for row in rows))

    def test_collision_probe_read_error_fails_environment_closed(self) -> None:
        peer = self.make_peer([self.http_server("alpha")])
        self.sync_mirror(peer)
        self.enroll_http("claude")
        self._reconcile()
        before_doc = self.claude_json.read_bytes()
        with sqlite3.connect(self.projection()) as connection:
            stored_before = dict(connection.execute(
                "SELECT server_id, entry_fingerprint FROM agent_mcp_projections"
            ).fetchall())
        real_parse = bridge_runtime._claude_json_document
        calls = {"count": 0}

        def flaky_parse(text: str):
            calls["count"] += 1
            if calls["count"] == 1:
                # first call = the current-entries read (succeeds)
                return real_parse(text)
            # second call = the unowned-name collision probe (fails)
            raise BridgeError("claude configuration is not valid JSON (probe)")

        with mock.patch(
            "bridge_runtime._claude_json_document", side_effect=flaky_parse
        ):
            result = self._reconcile()
        self.assertFalse(result["ok"])
        env_result = result["environments"][0]
        self.assertEqual(env_result["status"], "error")
        self.assertIn("not valid JSON", env_result["errorDetail"])
        self.assertEqual(env_result["actions"], [])
        self.assertEqual(self.claude_json.read_bytes(), before_doc)
        with sqlite3.connect(self.projection()) as connection:
            self.assertEqual(dict(connection.execute(
                "SELECT server_id, entry_fingerprint FROM agent_mcp_projections"
            ).fetchall()), stored_before)
        # probe failure is transient: the next reconcile recovers
        recovered = self._reconcile()
        self.assertTrue(recovered["ok"], recovered["errors"])
        self.assertEqual(recovered["environments"][0]["actions"], [])

class ProjectionStdioToHttpTest(ProjectionHarness):
    """Explicit Operator-supplied per-target stdio-to-HTTP facade projection.

    The environment's Agent consumes MCP over Streamable HTTP (route
    ``stdio-to-http``); stdio peers are converted entries pointed at the
    Operator's already-provisioned loopback facade ``/mcp`` URL (mapping
    registered id -> URL). No port is guessed, no process is launched or
    supervised, readiness is reported as configured not live, and a missing
    mapping fails the whole environment closed. Native HTTP peers in the same
    environment reach the Agent through the explicitly supplied relay base.
    """

    RELAY = "http://127.0.0.1:8877"
    ALPHA_ENDPOINT = "http://127.0.0.1:9011/mcp"
    BETA_ENDPOINT = "http://127.0.0.1:9012/mcp"

    def enroll_s2h(
        self,
        kind: str,
        candidate: dict | None = None,
        mapping: dict[str, str] | None = None,
        relay: str | None = None,
        capabilities=None,
    ) -> dict:
        kwargs = dict(
            transport_capabilities=capabilities
            if capabilities is not None
            else {"stdio": True, "streamable-http": True},
            compatibility_route="stdio-to-http",
        )
        if mapping is not None:
            kwargs["stdio_http_endpoints"] = (
                mapping
                if isinstance(mapping, str)
                else json.dumps(mapping, ensure_ascii=False)
            )
        if relay is not None:
            kwargs["relay_base_url"] = relay
        return self.enroll(
            candidate if candidate is not None else self.candidate(kind), **kwargs
        )

    def http_server(self, server_id, name=None, enabled=True) -> dict:
        return {
            "id": server_id,
            "name": name or server_id,
            "summary": f"http summary for {server_id}",
            "transport": {
                "type": "streamable-http",
                "endpoint": f"http://127.0.0.1:39001/mcp/{server_id}",
            },
            "enabled": enabled,
        }

    def _set_env(self, sql, environment_id: str, **columns) -> None:
        with sqlite3.connect(sql) as connection:
            assignments = ", ".join(f"{key} = ?" for key in columns)
            connection.execute(
                f"UPDATE agent_environments SET {assignments}, updated_at_ns = ? "
                "WHERE environment_id = ?",
                (*columns.values(), time.time_ns(), environment_id),
            )

    def _reconcile(self, **kwargs):
        return projection_reconcile(
            projection=self.projection(), side=self.SIDE, **kwargs
        )

    def _doc(self) -> dict:
        return json.loads(self.claude_json.read_text(encoding="utf-8"))

    def test_enroll_endpoint_mapping_validation_and_normalization(self) -> None:
        parse = bridge_runtime._parse_stdio_http_endpoints
        # inline JSON must be an object of bounded, valid entries
        for bad_mapping, fragment in (
            ("[1,2]", "must decode to a JSON object"),
            ('{"alpha": 42}', "endpoint URL must be a non-empty string"),
            ('{"alpha": "http://10.1.2.3:9011/mcp"}', "is not loopback"),
            ('{"alpha": "https://example.com:9011/mcp"}', "is not loopback"),
            ('{"alpha": "http://127.0.0.1/mcp"}', "explicit loopback port"),
            ('{"alpha": "http://127.0.0.1:0/mcp"}', "out of range"),
            ('{"alpha": "http://127.0.0.1:70000/mcp"}', "invalid port"),
            ('{"alpha": "http://127.0.0.1:9011/mcp/extra"}', "path must be the facade"),
            ('{"alpha": "http://127.0.0.1:9011/x"}', "path must be the facade"),
            ('{"alpha": "http://127.0.0.1:9011?q=1"}', "query or fragment"),
            ('{"alpha": "ftp://127.0.0.1:9011/mcp"}', "must use http or https"),
            ('{"alpha": "http://user:pw@127.0.0.1:9011/mcp"}', "userinfo"),
            ('{"../alpha": "http://127.0.0.1:9011/mcp"}', "not a valid registered id"),
            ('{"a/b": "http://127.0.0.1:9011/mcp"}', "not a valid registered id"),
            ('{"": "http://127.0.0.1:9011/mcp"}', "must be non-empty strings"),
        ):
            with self.subTest(bad_mapping=bad_mapping):
                with self.assertRaisesRegex(BridgeError, fragment):
                    parse(bad_mapping)
        # count bound
        too_many = {f"srv{i:03d}": "http://127.0.0.1:9100/mcp" for i in range(65)}
        with self.assertRaisesRegex(BridgeError, "bounded to 64"):
            parse(json.dumps(too_many))
        # a file whose contents are not JSON is rejected
        invalid_file = self.root / "invalid-endpoints.json"
        invalid_file.write_text("{oops not json", encoding="utf-8")
        with self.assertRaisesRegex(BridgeError, "is not valid JSON"):
            parse(str(invalid_file))
        # accepted forms normalize: bare base gains /mcp, localhost kept
        self.assertEqual(
            parse('{"alpha": "http://localhost:9011"}'),
            {"alpha": "http://localhost:9011/mcp"},
        )
        self.assertEqual(
            parse(json.dumps({"alpha": self.ALPHA_ENDPOINT})),
            {"alpha": self.ALPHA_ENDPOINT},
        )
        # a local file path is accepted in place of inline JSON
        mapping_file = self.root / "facade-endpoints.json"
        mapping_file.write_text(
            json.dumps({"alpha": self.ALPHA_ENDPOINT}), encoding="utf-8"
        )
        self.assertEqual(
            parse(str(mapping_file)), {"alpha": self.ALPHA_ENDPOINT}
        )
        # mapping is only meaningful for the stdio-to-http route
        codex = self.candidate("codex")
        with self.assertRaisesRegex(BridgeError, "only meaningful"):
            self.enroll(
                codex,
                transport_capabilities={"stdio": True, "streamable-http": True},
                compatibility_route="native",
                stdio_http_endpoints=json.dumps({"alpha": self.ALPHA_ENDPOINT}),
            )
        # stdio-to-http without the streamable-http client capability is rejected
        with self.assertRaisesRegex(BridgeError, "stdio-to-http conversion requires"):
            self.enroll(
                codex,
                transport_capabilities={"stdio": True, "streamable-http": False},
                compatibility_route="stdio-to-http",
                stdio_http_endpoints=json.dumps({"alpha": self.ALPHA_ENDPOINT}),
            )
        # relay stays optional for stdio-to-http and normalizes when supplied;
        # both are persisted and surfaced in the environment summary
        with_relay = self.enroll_s2h(
            "codex",
            candidate=codex,
            mapping={"alpha": "http://localhost:9011"},
            relay=self.RELAY,
        )
        self.assertEqual(with_relay["environment"]["relayBaseUrl"], self.RELAY)
        self.assertEqual(
            with_relay["environment"]["stdioHttpEndpoints"],
            {"alpha": "http://localhost:9011/mcp"},
        )

    def test_claude_file_converted_projection_idempotence_and_removal(self) -> None:
        self.sync_mirror(
            self.make_peer([self.server("alpha"), self.server("beta")])
        )
        self.enroll_s2h(
            "claude",
            mapping={"alpha": self.ALPHA_ENDPOINT, "beta": self.BETA_ENDPOINT},
        )
        first = self._reconcile()
        self.assertTrue(first["ok"], first["errors"])
        document = self._doc()
        self.assertEqual(document["mcpServers"]["alpha"], {
            "type": "http", "url": self.ALPHA_ENDPOINT,
        })
        self.assertEqual(document["mcpServers"]["beta"], {
            "type": "http", "url": self.BETA_ENDPOINT,
        })
        self.assertEqual(document["userKey"], "preserved")
        before = self.claude_json.read_bytes()
        again = self._reconcile()
        self.assertTrue(again["ok"], again["errors"])
        self.assertEqual(again["environments"][0]["actions"], [])
        self.assertEqual(self.claude_json.read_bytes(), before)
        # mirror drops both peers: converted http entries are removed
        peer = self.root / "peer.sqlite3"
        manifest = self.root / "peer.sqlite3.json"
        manifest.write_text(json.dumps({
            "servers": [
                self.server("alpha", enabled=False),
                self.server("beta", enabled=False),
            ]
        }))
        Registry.initialize_database(
            peer, manifest, replace=True,
            projection_path=self.root / "peer.sqlite3.proj.sqlite3",
        )
        self.sync_mirror(peer)
        removed = self._reconcile()
        self.assertTrue(removed["ok"], removed["errors"])
        final = self._doc()
        self.assertEqual(final["mcpServers"], {})
        self.assertEqual(final["userKey"], "preserved")

    def test_missing_mapping_fails_whole_environment_without_partial_apply(self) -> None:
        # three stdio peers, mapping covers only beta: the whole environment
        # errors before any add, so beta is NOT applied alone either
        self.sync_mirror(
            self.make_peer(
                [self.server("alpha"), self.server("beta"), self.server("gamma")]
            )
        )
        self.enroll_s2h("claude", mapping={"beta": self.BETA_ENDPOINT})
        result = self._reconcile()
        self.assertFalse(result["ok"])
        env_result = result["environments"][0]
        self.assertEqual(env_result["status"], "error")
        self.assertEqual(
            sorted(item["serverId"] for item in env_result["unsupportedTransports"]),
            ["alpha", "gamma"],
        )
        for item in env_result["unsupportedTransports"]:
            self.assertEqual(item["code"], "unsupported_transport")
            self.assertEqual(item["transport"], "stdio")
        self.assertEqual(env_result["actions"], [])
        self.assertEqual(self._doc()["mcpServers"], {})

    def test_mixed_converted_entries_and_native_http_peer_via_explicit_relay(self) -> None:
        # stdio peer alpha -> facade mapping (converted); native HTTP peer
        # delta -> this host's explicitly supplied relay base
        self.sync_mirror(
            self.make_peer([self.server("alpha"), self.http_server("delta")])
        )
        self.enroll_s2h(
            "claude",
            mapping={"alpha": self.ALPHA_ENDPOINT},
            relay=self.RELAY,
        )
        first = self._reconcile()
        self.assertTrue(first["ok"], first["errors"])
        document = self._doc()
        self.assertEqual(document["mcpServers"]["alpha"]["url"], self.ALPHA_ENDPOINT)
        self.assertEqual(
            document["mcpServers"]["delta"]["url"], f"{self.RELAY}/mcp/delta"
        )
        before = self.claude_json.read_bytes()
        # relay removal (operator omission) leaves delta unprojectable: the
        # whole environment errors with no action and the file is preserved
        self._set_env(self.projection(), self._env_id(first), relay_base_url="")
        blocked = self._reconcile()
        self.assertFalse(blocked["ok"])
        env_result = blocked["environments"][0]
        self.assertEqual(env_result["status"], "error")
        self.assertEqual(
            env_result["unsupportedTransports"],
            [{"serverId": "delta", "transport": "streamable-http",
              "code": "unsupported_transport"}],
        )
        self.assertEqual(env_result["actions"], [])
        self.assertEqual(self.claude_json.read_bytes(), before)
        # restoring the relay reconciles idempotently again
        self._set_env(self.projection(), self._env_id(first), relay_base_url=self.RELAY)
        recovered = self._reconcile()
        self.assertTrue(recovered["ok"], recovered["errors"])
        self.assertEqual(recovered["environments"][0]["actions"], [])
        self.assertEqual(self.claude_json.read_bytes(), before)

    def _env_id(self, result: dict) -> str:
        return result["environments"][0]["environmentId"]

    def test_codex_owned_document_converted_projection_and_drift(self) -> None:
        config = self.home / ".codex" / "config.toml"
        self.sync_mirror(self.make_peer([self.server("alpha")]))
        self.enroll_s2h("codex", mapping={"alpha": self.ALPHA_ENDPOINT})
        first = self._reconcile()
        self.assertTrue(first["ok"], first["errors"])
        text = config.read_text(encoding="utf-8")
        self.assertIn(f'url = "{self.ALPHA_ENDPOINT}"', text)
        # idempotent
        again = self._reconcile()
        self.assertTrue(again["ok"], again["errors"])
        self.assertEqual(again["environments"][0]["actions"], [])
        # user edits the projected URL in the owned document: removal on peer
        # drop is blocked as drift and the user edit is preserved
        tampered = text.replace(self.ALPHA_ENDPOINT, "http://127.0.0.1:9999/mcp")
        config.write_text(tampered, encoding="utf-8")
        peer = self.root / "peer.sqlite3"
        manifest = self.root / "peer.sqlite3.json"
        manifest.write_text(json.dumps({"servers": [self.server("alpha", enabled=False)]}))
        Registry.initialize_database(
            peer, manifest, replace=True,
            projection_path=self.root / "peer.sqlite3.proj.sqlite3",
        )
        self.sync_mirror(peer)
        drifted = self._reconcile()
        self.assertFalse(drifted["ok"])
        env_result = drifted["environments"][0]
        self.assertEqual(env_result["status"], "drift")
        self.assertIn("alpha", env_result["drift"])
        self.assertEqual(config.read_text(encoding="utf-8"), tampered)

    def test_dsh_overlay_converted_projection_render_and_removal(self) -> None:
        from bridge_runtime import CLIENT_KIND_DSH

        profile = self.dsh_home / "profiles" / "main"
        self.sync_mirror(self.make_peer([self.server("alpha")]))
        candidates = self.scan()
        overlay = [
            c for c in candidates
            if c["clientKind"] == CLIENT_KIND_DSH
            and c["configPath"].endswith("cordis-bridge-overlay.json")
        ]
        self.assertEqual(len(overlay), 1)
        self.enroll_s2h(CLIENT_KIND_DSH, overlay[0], mapping={"alpha": self.ALPHA_ENDPOINT})
        result = self._reconcile()
        self.assertTrue(result["ok"], result["errors"])
        overlay_file = profile / "cordis-bridge-overlay.json"
        patches = json.loads(overlay_file.read_text(encoding="utf-8"))
        items = [
            item for patch in patches for item in patch.get("insert", [])
        ]
        self.assertEqual(len(items), 1)
        config = items[0]["config"]
        self.assertEqual(items[0]["id"], "mcp-alpha")
        self.assertEqual(config["serverName"], "alpha")
        self.assertEqual(config["transport"], "streamable-http")
        self.assertEqual(config["url"], self.ALPHA_ENDPOINT)
        before = overlay_file.read_bytes()
        again = self._reconcile()
        self.assertTrue(again["ok"])
        self.assertEqual(again["environments"][0]["actions"], [])
        self.assertEqual(overlay_file.read_bytes(), before)
        peer = self.root / "peer.sqlite3"
        manifest = self.root / "peer.sqlite3.json"
        manifest.write_text(json.dumps({"servers": [self.server("alpha", enabled=False)]}))
        Registry.initialize_database(
            peer, manifest, replace=True,
            projection_path=self.root / "peer.sqlite3.proj.sqlite3",
        )
        self.sync_mirror(peer)
        removed = self._reconcile()
        self.assertTrue(removed["ok"], removed["errors"])
        self.assertEqual(json.loads(overlay_file.read_text(encoding="utf-8")), [])

    def test_user_edited_converted_url_is_a_collision_never_overwritten(self) -> None:
        self.sync_mirror(self.make_peer([self.server("alpha")]))
        environment = self.enroll_s2h(
            "claude", mapping={"alpha": self.ALPHA_ENDPOINT}
        )["environment"]
        self._reconcile()
        with sqlite3.connect(self.projection()) as connection:
            stored_before = dict(connection.execute(
                "SELECT server_id, entry_fingerprint FROM agent_mcp_projections"
            ).fetchall())
        # user points the same-name server at their own loopback URL
        document = self._doc()
        document["mcpServers"]["alpha"]["url"] = "http://127.0.0.1:9999/mcp"
        self.claude_json.write_text(json.dumps(document, ensure_ascii=False))
        conflicted = self._reconcile()
        self.assertFalse(conflicted["ok"])
        env_result = conflicted["environments"][0]
        self.assertEqual(env_result["status"], "error")
        self.assertIn("alpha", env_result["conflicts"])
        self.assertEqual(env_result["actions"], [])
        kept = self._doc()
        self.assertEqual(kept["mcpServers"]["alpha"]["url"], "http://127.0.0.1:9999/mcp")
        # stored fingerprint is preserved through the conflict
        with sqlite3.connect(self.projection()) as connection:
            self.assertEqual(dict(connection.execute(
                "SELECT server_id, entry_fingerprint FROM agent_mcp_projections"
            ).fetchall()), stored_before)
        # reverting to the mapped endpoint converges again
        document = self._doc()
        document["mcpServers"]["alpha"]["url"] = self.ALPHA_ENDPOINT
        self.claude_json.write_text(json.dumps(document, ensure_ascii=False))
        recovered = self._reconcile()
        self.assertTrue(recovered["ok"], recovered["errors"])
        self.assertEqual(recovered["environments"][0]["actions"], [])
        self.assertEqual(
            self._doc()["mcpServers"]["alpha"]["url"], self.ALPHA_ENDPOINT
        )

    def test_official_cli_converted_projection_idempotent_and_remove(self) -> None:
        state = self.install_fake_cli("claude")
        self.sync_mirror(self.make_peer([self.server("alpha")]))
        self.enroll_s2h("claude", mapping={"alpha": self.ALPHA_ENDPOINT})
        result = self._reconcile()
        self.assertTrue(result["ok"], result["errors"])
        stored = json.loads(state.read_text())["mcpServers"]
        self.assertEqual(stored["alpha"], {
            "type": "http", "url": self.ALPHA_ENDPOINT, "headers": {},
        })
        again = self._reconcile()
        self.assertTrue(again["ok"], again["errors"])
        self.assertEqual(again["environments"][0]["actions"], [])
        peer = self.root / "peer.sqlite3"
        manifest = self.root / "peer.sqlite3.json"
        manifest.write_text(json.dumps({"servers": [self.server("alpha", enabled=False)]}))
        Registry.initialize_database(
            peer, manifest, replace=True,
            projection_path=self.root / "peer.sqlite3.proj.sqlite3",
        )
        self.sync_mirror(peer)
        removed = self._reconcile()
        self.assertTrue(removed["ok"], removed["errors"])
        self.assertEqual(json.loads(state.read_text())["mcpServers"], {})

    def test_schema_v3_to_v4_migration_preserves_enrolled_rows(self) -> None:
        # a fully enrolled stdio-to-http environment survives the v4 column
        # migration: rows persist, the column defaults to empty, and restoring
        # the mapping reconciles idempotently
        self.sync_mirror(self.make_peer([self.server("alpha")]))
        environment = self.enroll_s2h(
            "claude", mapping={"alpha": self.ALPHA_ENDPOINT}
        )["environment"]
        environment_id = environment["environmentId"]
        self._reconcile()
        path = self.projection()
        with sqlite3.connect(path) as connection:
            connection.execute(
                "ALTER TABLE agent_environments DROP COLUMN stdio_http_endpoints_json"
            )
            connection.execute("PRAGMA user_version = 3")
        ProjectionDatabase.ensure(path)
        with ProjectionDatabase(path)._connect() as connection:
            row = ProjectionDatabase(path).environment_row(connection, environment_id)
        self.assertIsNotNone(row)
        summary = bridge_runtime._environment_summary(row)
        self.assertEqual(summary["stdioHttpEndpoints"], {})
        self.assertEqual(summary["relayBaseUrl"], "")
        self.assertEqual(summary["environmentId"], environment_id)
        # after restoring the mapping the environment stays reconcilable
        self._set_env(
            path,
            environment_id,
            stdio_http_endpoints_json=json.dumps({"alpha": self.ALPHA_ENDPOINT}),
        )
        recovered = self._reconcile()
        self.assertTrue(recovered["ok"], recovered["errors"])
        self.assertEqual(recovered["environments"][0]["actions"], [])

class ArtifactV2DeliveryTest(unittest.TestCase):
    """artifacts/2: content-addressed durable receipts, resume, idempotence."""

    def _node(self, root: Path, workspace: Path, retention: float = 300.0) -> BridgeNode:
        database = root / "registry.sqlite3"
        write_registry(database, "v2-test", "V2 Test")
        node = BridgeNode(
            side="win",
            registry=Registry(database),
            local_host="127.0.0.1",
            local_port=free_port(),
            link_mode="listen",
            link_host="127.0.0.1",
            link_port=free_port(),
            allowed_artifact_roots=[workspace],
            artifact_resume_retention_seconds=retention,
        )
        node.peer_artifact_protocol = 2
        return node

    @staticmethod
    def _chunk_frame(stream_id: str, artifact_id: str, sequence: int, data: bytes) -> dict:
        return {
            "type": "artifact_chunk",
            "stream": stream_id,
            "artifact": artifact_id,
            "sequence": sequence,
            "sha256": hashlib.sha256(data).hexdigest(),
            "data": base64.b64encode(data).decode("ascii"),
        }

    def _content(self) -> bytes:
        return (b"artifacts/2 resume delivery\n" * 9000) + b"tail"

    async def _receive_all(
        self,
        node: BridgeNode,
        stream_id: str,
        artifact_id: str,
        frames: list[dict],
        content: bytes,
    ) -> None:
        state = node.receiving_artifacts[artifact_id]
        for frame in frames:
            node._handle_artifact_chunk_v2(state, frame)
        node._handle_artifact_end_v2(
            state,
            {
                "type": "artifact_end",
                "stream": stream_id,
                "artifact": artifact_id,
                "chunks": len(frames),
                "size": len(content),
                "sha256": hashlib.sha256(content).hexdigest(),
            },
        )
        for _ in range(2000):
            if artifact_id not in node.receiving_artifacts:
                return
            await asyncio.sleep(0.001)
        raise AssertionError("v2 receive did not settle")

    async def _settle_receipt(self, sent: list[dict]) -> None:
        """Wait until the durable receipt frame (ok/committed) is captured."""
        for _ in range(4000):
            if any(
                frame.get("type") in ("artifact_ok", "artifact_committed")
                for frame in sent
            ):
                return
            await asyncio.sleep(0.001)
        raise AssertionError("delivery receipt was not captured")

    def test_v2_negotiation_helper_fallbacks(self) -> None:
        from bridge_runtime import _artifact_max_chunk, _parse_artifact_extension

        self.assertEqual(
            _parse_artifact_extension({"version": 1, "maxChunk": 1024}),
            1,
        )
        self.assertEqual(
            _parse_artifact_extension({"version": 1, "highestVersion": 2, "maxChunk": 1024}),
            2,
        )
        self.assertEqual(_parse_artifact_extension({"version": 2}), 2)
        # A version-1-compatible offer is always speakable; only unknowable
        # shapes (absent, non-1/2 versions, non-objects) disable delivery.
        self.assertEqual(
            _parse_artifact_extension({"version": 1, "highestVersion": 3}),
            1,
        )
        for bad in (None, {}, {"version": 3}, "artifacts/2"):
            self.assertIsNone(_parse_artifact_extension(bad))
        self.assertEqual(_artifact_max_chunk({"version": 1, "maxChunk": 4096}), 4096)
        for bad in (0, -1, 1 << 40, True, "big"):
            self.assertIsNone(_artifact_max_chunk({"version": 1, "maxChunk": bad}))

    def test_v2_begin_replies_committed_fast_path_with_matching_token(self) -> None:
        content = self._content()
        sha256 = hashlib.sha256(content).hexdigest()
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            workspace = root / "workspace"
            workspace.mkdir()
            node = self._node(root, workspace)
            sent: list[dict] = []

            async def exercise() -> None:
                async def capture(_frame: dict) -> None:
                    sent.append(_frame)

                node._send_frame = capture  # type: ignore[method-assign]
                stream = StreamState(stream_id="wsl-v2-fast", artifact_inbox=workspace.resolve())
                node.streams[stream.stream_id] = stream
                frames: list[dict] = []
                chunk_bytes = 12288
                data = content
                sequence = 0
                while data:
                    piece, data = data[:chunk_bytes], data[chunk_bytes:]
                    frames.append(self._chunk_frame(stream.stream_id, "artifact-v2-fast", sequence, piece))
                    sequence += 1
                await node._handle_artifact_begin_v2({
                    "type": "artifact_begin", "stream": stream.stream_id,
                    "artifact": "artifact-v2-fast", "name": "fast.bin",
                    "mediaType": None, "size": len(content), "sha256": sha256,
                })
                await self._receive_all(
                    node, stream.stream_id, "artifact-v2-fast", frames, content
                )
                state = node.receiving_artifacts
                self.assertEqual(state, {})
                # Wait for the durable committed receipt before probing it.
                await self._settle_receipt(sent)
                ready = sent[0]
                token = ready["resumeToken"]
                # A retry that carries the committed token is answered without
                # re-sending a single byte: no duplicate overwrite, no probe leak.
                sent.clear()
                await node._handle_artifact_begin_v2({
                    "type": "artifact_begin", "stream": stream.stream_id,
                    "artifact": "artifact-v2-again", "name": "fast.bin",
                    "mediaType": None, "size": len(content), "sha256": sha256,
                    "resumeToken": token,
                })
                self.assertEqual(len(sent), 1)
                committed = sent[0]
                self.assertEqual(committed["type"], "artifact_committed")
                final = Path(committed["path"])
                self.assertEqual(final.read_bytes(), content)
                # Exactly one final artifact exists.
                finals = list((workspace / ".mcp-artifacts" / sha256).glob("fast.bin"))
                self.assertEqual(len(finals), 1)

            asyncio.run(exercise())

    def test_v2_fresh_full_resend_is_idempotent_at_end(self) -> None:
        content = self._content()
        sha256 = hashlib.sha256(content).hexdigest()
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            workspace = root / "workspace"
            workspace.mkdir()
            node = self._node(root, workspace)
            sent: list[dict] = []

            async def exercise() -> None:
                async def capture(_frame: dict) -> None:
                    sent.append(_frame)

                node._send_frame = capture  # type: ignore[method-assign]
                stream = StreamState(stream_id="wsl-v2-idem", artifact_inbox=workspace.resolve())
                node.streams[stream.stream_id] = stream

                async def deliver(artifact_id: str) -> dict:
                    frames: list[dict] = []
                    data = content
                    sequence = 0
                    while data:
                        piece, data = data[:12288], data[12288:]
                        frames.append(self._chunk_frame(stream.stream_id, artifact_id, sequence, piece))
                        sequence += 1
                    await node._handle_artifact_begin_v2({
                        "type": "artifact_begin", "stream": stream.stream_id,
                        "artifact": artifact_id, "name": "idem.bin",
                        "mediaType": None, "size": len(content), "sha256": sha256,
                    })
                    await self._receive_all(
                        node, stream.stream_id, artifact_id, frames, content
                    )
                    await self._settle_receipt(sent)
                    sent.clear()
                    return {}

                await deliver("artifact-v2-idem-a")
                finals = list((workspace / ".mcp-artifacts" / sha256).glob("idem.bin"))
                self.assertEqual(len(finals), 1)
                first_stat = finals[0].stat()
                # A tokenless full re-delivery (a lost artifact_ok) must converge
                # on the existing file without overwriting it.
                await deliver("artifact-v2-idem-b")
                finals = list((workspace / ".mcp-artifacts" / sha256).glob("idem.bin"))
                self.assertEqual(len(finals), 1)
                self.assertEqual(finals[0].stat().st_ino, first_stat.st_ino)
                self.assertEqual(finals[0].read_bytes(), content)

            asyncio.run(exercise())

    def test_v2_park_then_resume_has_no_byte_duplication(self) -> None:
        content = self._content()
        sha256 = hashlib.sha256(content).hexdigest()
        chunk_bytes = 12288
        pieces = [content[i : i + chunk_bytes] for i in range(0, len(content), chunk_bytes)]
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            workspace = root / "workspace"
            workspace.mkdir()
            node = self._node(root, workspace)
            sent: list[dict] = []

            async def exercise() -> None:
                async def capture(_frame: dict) -> None:
                    sent.append(_frame)

                node._send_frame = capture  # type: ignore[method-assign]
                stream = StreamState(stream_id="wsl-v2-resume", artifact_inbox=workspace.resolve())
                node.streams[stream.stream_id] = stream
                await node._handle_artifact_begin_v2({
                    "type": "artifact_begin", "stream": stream.stream_id,
                    "artifact": "artifact-v2-resume", "name": "resume.bin",
                    "mediaType": None, "size": len(content), "sha256": sha256,
                })
                state = node.receiving_artifacts["artifact-v2-resume"]
                # Feed half of the chunks, let each be acked, then park the
                # receive exactly as a link loss would.
                half = len(pieces) // 2
                for index in range(half):
                    node._handle_artifact_chunk_v2(state, self._chunk_frame(stream.stream_id, state.artifact_id, index, pieces[index]))
                for _ in range(400):
                    if state.acked_bytes == sum(len(piece) for piece in pieces[:half]) and state.next_sequence == half:
                        break
                    await asyncio.sleep(0.001)
                self.assertEqual(state.next_sequence, half)
                await node._park_artifact_v2(state, "test link loss")
                self.assertNotIn(state.artifact_id, node.receiving_artifacts)
                parked_offset = state.acked_bytes
                token = state.token
                # Resume: begin with the parked token and deliver only the tail.
                sent.clear()
                await node._handle_artifact_begin_v2({
                    "type": "artifact_begin", "stream": stream.stream_id,
                    "artifact": "artifact-v2-resume-2", "name": "resume.bin",
                    "mediaType": None, "size": len(content), "sha256": sha256,
                    "resumeToken": token,
                })
                resumed = node.receiving_artifacts["artifact-v2-resume-2"]
                ready = sent[0]
                self.assertEqual(ready["resumeOffset"], parked_offset)
                self.assertEqual(resumed.received, parked_offset)
                for index in range(half, len(pieces)):
                    node._handle_artifact_chunk_v2(resumed, self._chunk_frame(stream.stream_id, resumed.artifact_id, index, pieces[index]))
                node._handle_artifact_end_v2(resumed, {
                    "type": "artifact_end", "stream": stream.stream_id,
                    "artifact": resumed.artifact_id, "chunks": len(pieces),
                    "size": len(content), "sha256": sha256,
                })
                for _ in range(400):
                    if resumed.artifact_id not in node.receiving_artifacts:
                        break
                    await asyncio.sleep(0.001)
                self.assertNotIn(resumed.artifact_id, node.receiving_artifacts)
                finals = list((workspace / ".mcp-artifacts" / sha256).glob("resume.bin"))
                self.assertEqual(len(finals), 1)
                self.assertEqual(finals[0].read_bytes(), content)

            asyncio.run(exercise())

    def test_v2_foreign_tokenless_begin_never_reveals_state(self) -> None:
        content = b"probe-me"
        sha256 = hashlib.sha256(content).hexdigest()
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            workspace = root / "workspace"
            workspace.mkdir()
            node = self._node(root, workspace)
            sent: list[dict] = []

            async def exercise() -> None:
                async def capture(_frame: dict) -> None:
                    sent.append(_frame)

                node._send_frame = capture  # type: ignore[method-assign]
                stream = StreamState(stream_id="wsl-v2-probe", artifact_inbox=workspace.resolve())
                node.streams[stream.stream_id] = stream
                # Complete one delivery so a committed receipt exists.
                await node._handle_artifact_begin_v2({
                    "type": "artifact_begin", "stream": stream.stream_id,
                    "artifact": "artifact-v2-probe-a", "name": "probe.bin",
                    "mediaType": None, "size": len(content), "sha256": sha256,
                })
                node._handle_artifact_chunk_v2(node.receiving_artifacts["artifact-v2-probe-a"], self._chunk_frame(stream.stream_id, "artifact-v2-probe-a", 0, content))
                node._handle_artifact_end_v2(node.receiving_artifacts["artifact-v2-probe-a"], {"type": "artifact_end", "stream": stream.stream_id, "artifact": "artifact-v2-probe-a", "chunks": 1, "size": len(content), "sha256": sha256})
                for _ in range(400):
                    if "artifact-v2-probe-a" not in node.receiving_artifacts:
                        break
                    await asyncio.sleep(0.001)
                self.assertNotIn("artifact-v2-probe-a", node.receiving_artifacts)
                await self._settle_receipt(sent)
                # A tokenless probe must be answered as a fresh receiving state,
                # never with the committed receipt: existence is not revealed.
                sent.clear()
                await node._handle_artifact_begin_v2({
                    "type": "artifact_begin", "stream": stream.stream_id,
                    "artifact": "artifact-v2-probe-b", "name": "probe.bin",
                    "mediaType": None, "size": len(content), "sha256": sha256,
                })
                self.assertEqual(len(sent), 1)
                self.assertEqual(sent[0]["type"], "artifact_ready")
                self.assertEqual(sent[0].get("resumeOffset"), 0)
                # An unknown claimed token behaves like a fresh attempt too.
                sent.clear()
                await node._handle_artifact_begin_v2({
                    "type": "artifact_begin", "stream": stream.stream_id,
                    "artifact": "artifact-v2-probe-c", "name": "probe.bin",
                    "mediaType": None, "size": len(content), "sha256": sha256,
                    "resumeToken": "f" * 48,
                })
                self.assertEqual(len(sent), 1)
                self.assertEqual(sent[0]["type"], "artifact_ready")
                # Abort both live probes so no partial clutter is left behind.
                for leftover in ("artifact-v2-probe-b", "artifact-v2-probe-c"):
                    state = node.receiving_artifacts.pop(leftover, None)
                    if state is not None:
                        await node._abort_artifact_v2(state, "test cleanup")
                self.assertEqual(node.receiving_artifacts, {})

            asyncio.run(exercise())

    def test_v2_retention_expiry_and_janitor_preserve_partials(self) -> None:
        content = b"expiry-content"
        sha256 = hashlib.sha256(content).hexdigest()
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            workspace = root / "workspace"
            workspace.mkdir()
            node = self._node(root, workspace, retention=0.05)
            sent: list[dict] = []

            async def exercise() -> None:
                async def capture(_frame: dict) -> None:
                    sent.append(_frame)

                node._send_frame = capture  # type: ignore[method-assign]
                stream = StreamState(stream_id="wsl-v2-ret", artifact_inbox=workspace.resolve())
                node.streams[stream.stream_id] = stream
                await node._handle_artifact_begin_v2({
                    "type": "artifact_begin", "stream": stream.stream_id,
                    "artifact": "artifact-v2-ret-a", "name": "ret.bin",
                    "mediaType": None, "size": len(content), "sha256": sha256,
                })
                state = node.receiving_artifacts["artifact-v2-ret-a"]
                node._handle_artifact_chunk_v2(state, self._chunk_frame(stream.stream_id, state.artifact_id, 0, content))
                for _ in range(400):
                    if state.acked_bytes == len(content):
                        break
                    await asyncio.sleep(0.001)
                token = state.token
                await node._park_artifact_v2(state, "test park")
                journal_path = workspace / ".mcp-artifacts" / ".v2-journal.json"
                self.assertTrue(journal_path.exists())
                # A parked (resumable) partial survives the startup janitor.
                self.assertEqual(node._cleanup_workspace_partials(), 0)
                partial = state.partial_path
                self.assertTrue(partial.exists())
                # Expiry prunes the durable record (and the parked partial) so
                # the token can no longer resume.
                await asyncio.sleep(0.15)
                await node._handle_artifact_begin_v2({
                    "type": "artifact_begin", "stream": stream.stream_id,
                    "artifact": "artifact-v2-ret-b", "name": "ret.bin",
                    "mediaType": None, "size": len(content), "sha256": sha256,
                    "resumeToken": token,
                })
                sent.clear()
                await node._handle_artifact_begin_v2({
                    "type": "artifact_begin", "stream": stream.stream_id,
                    "artifact": "artifact-v2-ret-c", "name": "ret.bin",
                    "mediaType": None, "size": len(content), "sha256": sha256,
                })
                self.assertEqual(sent[0]["type"], "artifact_ready")
                self.assertEqual(sent[0]["resumeOffset"], 0)
                self.assertFalse(partial.exists())
                for leftover in ("artifact-v2-ret-b", "artifact-v2-ret-c"):
                    state = node.receiving_artifacts.pop(leftover, None)
                    if state is not None:
                        await node._abort_artifact_v2(state, "test cleanup")

            asyncio.run(exercise())

    def test_v2_reply_router_accepts_committed_during_begin_phase(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "registry.sqlite3"
            write_registry(database, "v2-reply", "V2 Reply")
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
                    stream_id="wsl-v2-reply",
                    ready=loop.create_future(),
                    done=loop.create_future(),
                    expected_size=4,
                    expected_sha256="0" * 64,
                )
                node.pending_artifacts["artifact-v2-reply"] = waiter
                node._handle_artifact_reply(
                    {
                        "type": "artifact_committed",
                        "stream": waiter.stream_id,
                        "artifact": "artifact-v2-reply",
                        "uri": "file:///tmp/result",
                        "path": "/tmp/result",
                        "size": 4,
                        "sha256": "0" * 64,
                    }
                )
                self.assertEqual(waiter.phase, "completed")
                self.assertEqual(waiter.done.result()["path"], "/tmp/result")
                self.assertEqual(waiter.ready.result().get("status"), "committed")
                waiter.done.cancel()
                node.pending_artifacts.clear()

            asyncio.run(exercise())

    def test_v2_reply_router_rejects_bad_committed_receipt(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "registry.sqlite3"
            write_registry(database, "v2-reply-bad", "V2 Reply Bad")
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
                    stream_id="wsl-v2-reply-bad",
                    ready=loop.create_future(),
                    done=loop.create_future(),
                    expected_size=4,
                    expected_sha256="0" * 64,
                )
                node.pending_artifacts["artifact-v2-reply-bad"] = waiter
                node._handle_artifact_reply(
                    {
                        "type": "artifact_committed",
                        "stream": waiter.stream_id,
                        "artifact": "artifact-v2-reply-bad",
                        "uri": "file:///tmp/other",
                        "path": "/tmp/other",
                        "size": 99,
                        "sha256": "f" * 64,
                    }
                )
                self.assertEqual(waiter.phase, "failed")
                # Consume the rejection (raised on the ready future) so no
                # orphaned-future noise is left.
                with self.assertRaises(BridgeError):
                    waiter.ready.result()
                waiter.done.cancel()
                node.pending_artifacts.clear()

            asyncio.run(exercise())


class ProjectionFinalCoverageTest(ProjectionHarness):
    """Closes the live-query and unenroll-drift paths hermetic-ally."""

    def _reconcile(self, **kwargs):
        return projection_reconcile(
            projection=self.projection(), side=self.SIDE, **kwargs
        )

    def _claude_servers(self) -> dict:
        document = json.loads(self.claude_json.read_text(encoding="utf-8"))
        return document["mcpServers"]

    def test_sync_remote_source_uses_live_peer_query(self) -> None:
        summaries = [
            {"id": "alpha", "name": "Alpha MCP"},
            {"id": "beta", "name": "Beta MCP"},
        ]
        with mock.patch(
            "bridge_runtime.local_registry_query",
            return_value=list(summaries),
        ) as query:
            synced = projection_sync_peer(
                projection=self.projection(),
                source="registry-remote",
                side=self.SIDE,
                local_host="127.0.0.1",
                local_port=8769,
            )
            query.assert_called_once_with("127.0.0.1", 8769, "remote", "list", {})
        self.assertTrue(synced["changed"])
        self.assertIn("registry-remote:127.0.0.1:8769", synced["source"])
        self.assertEqual(synced["serverCount"], 2)
        self.enroll(self.candidate("claude"))
        result = projection_reconcile(projection=self.projection(), side=self.SIDE)
        self.assertTrue(result["ok"], result["errors"])
        self.assertEqual(sorted(self._claude_servers()), ["alpha", "beta"])
        # a later remote refresh sees one server disabled -> one entry removed
        with mock.patch(
            "bridge_runtime.local_registry_query",
            return_value=[{"id": "beta", "name": "Beta MCP"}],
        ):
            projection_sync_peer(
                projection=self.projection(),
                source="registry-remote",
                side=self.SIDE,
                local_host="127.0.0.1",
                local_port=8769,
            )
        result = projection_reconcile(projection=self.projection(), side=self.SIDE)
        self.assertTrue(result["ok"], result["errors"])
        self.assertEqual(sorted(self._claude_servers()), ["beta"])

    def test_remote_query_rejects_invalid_ids_conservatively(self) -> None:
        with mock.patch(
            "bridge_runtime.local_registry_query",
            return_value=[
                {"id": "alpha", "name": "Alpha"},
                {"id": "../evil", "name": "Evil"},
                {"id": "ok-id-2", "name": "Two"},
                "not-a-summary",
            ],
        ):
            synced = projection_sync_peer(
                projection=self.projection(),
                source="registry-remote",
                side=self.SIDE,
                local_host="127.0.0.1",
                local_port=8769,
            )
        self.assertEqual(synced["servers"], ["alpha", "ok-id-2"])

    def test_unenroll_remove_entries_with_drift_records_error_and_keeps_tampered(self) -> None:
        self.sync_mirror(
            self.make_peer([self.server("alpha"), self.server("beta")])
        )
        environment = self.enroll(self.candidate("claude"))["environment"]
        environment_id = environment["environmentId"]
        result = self._reconcile()
        self.assertTrue(result["ok"])
        document = json.loads(self.claude_json.read_text(encoding="utf-8"))
        document["mcpServers"]["beta"]["args"] = ["--user-tampered"]
        self.claude_json.write_text(json.dumps(document, ensure_ascii=False))
        with self.assertRaisesRegex(BridgeError, "drift"):
            projection_unenroll(
                projection=self.projection(),
                environment_id=environment_id,
                remove_entries=True,
                confirm=True,
            )
        # the tampered entry survives; the environment is not deleted/disabled
        remaining = self._claude_servers()
        self.assertIn("beta", remaining)
        self.assertEqual(remaining["beta"]["args"], ["--user-tampered"])
        status = projection_status(projection=self.projection())
        self.assertEqual(len(status["environments"]), 1)
        self.assertIs(status["environments"][0]["enabled"], True)


class CapabilityWarehouseTest(unittest.TestCase):
    """P5 local slice: capability inventory + static index primitives."""

    # ------------------------------------------------------------------
    # fixtures
    # ------------------------------------------------------------------

    @staticmethod
    def _index(items: list[dict]) -> dict:
        # Normalize through the same validation every public path uses, so the
        # discovery/dedup/plan tests exercise the canonical item shape.
        return {"version": 1, "items": _parse_capability_index({"version": 1, "items": items})}

    @staticmethod
    def _item(
        publisher: str = "acme",
        name: str = "Code Metrics",
        version: str = "2.1.0",
        **extra: object,
    ) -> dict:
        item: dict = {
            "identity": {"publisher": publisher, "name": name, "version": version},
            "summary": "Aggregates repository metrics for CI dashboards.",
            "capabilityGroups": ["code", "metrics"],
        }
        item.update(extra)
        return item

    # ------------------------------------------------------------------
    # local inventory: Registry.identities is redacted and stable
    # ------------------------------------------------------------------

    def test_registry_identities_returns_redacted_stable_rows(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "registry.sqlite3"
            write_registry(database, "warehouse-side", "Warehouse Host")
            rows = Registry(database).identities()
            self.assertIsInstance(rows, list)
            self.assertTrue(rows)
            for row in rows:
                self.assertIn("id", row)
                self.assertIn("name", row)
                self.assertIn("summary", row)
                self.assertIn("capabilityGroups", row)
                self.assertIn("enabled", row)
                # Identity rows never carry launch/installation authority.
                for secret in (
                    "command",
                    "args",
                    "cwd",
                    "env",
                    "token",
                    "process",
                    "serverInfo",
                    "artifactDelivery",
                    "inputDelivery",
                ):
                    self.assertNotIn(secret, row)

    def test_identities_correspond_to_public_registry_rows(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "registry.sqlite3"
            write_registry(database, "warehouse-side", "Warehouse Host")
            registry = Registry(database)
            identity_ids = {row["id"] for row in registry.identities()}
            public_ids = {row["id"] for row in registry.query("list", {})}
            self.assertEqual(identity_ids, public_ids)

    # ------------------------------------------------------------------
    # static index validation fails closed
    # ------------------------------------------------------------------

    def test_capability_index_validation_rejects_launch_authority(self) -> None:
        for forbidden in ("command", "args", "cwd", "env", "script", "install", "launcher", "token", "credential"):
            item = self._item()
            item[forbidden] = "anything"
            with self.assertRaises(BridgeError) as caught:
                _parse_capability_index(self._index([item]))
            self.assertIn("launch/install authority", str(caught.exception))

    def test_capability_index_validation_structural_errors(self) -> None:
        # Unknown index version.
        with self.assertRaises(BridgeError):
            _parse_capability_index({"version": 2, "items": [self._item()]})
        # Empty / non-list items.
        with self.assertRaises(BridgeError):
            _parse_capability_index({"version": 1, "items": []})
        with self.assertRaises(BridgeError):
            _parse_capability_index({"version": 1})
        # Duplicate identity keys are rejected.
        with self.assertRaises(BridgeError):
            _parse_capability_index(
                self._index([self._item(), self._item(name="Code Metrics")])
            )
        # Broken identity / bounded fields.
        broken = self._item()
        del broken["identity"]
        with self.assertRaises(BridgeError):
            _parse_capability_index(self._index([broken]))
        oversized = self._item(summary="x" * 401)
        with self.assertRaises(BridgeError):
            _parse_capability_index(self._index([oversized]))
        bad_digest = self._item(manifestDigest="not-hex")
        with self.assertRaises(BridgeError):
            _parse_capability_index(self._index([bad_digest]))
        bad_id = self._item(id="../escape")
        with self.assertRaises(BridgeError):
            _parse_capability_index(self._index([bad_id]))
        # Empty capability groups are invalid.
        empty_groups = self._item(capabilityGroups=[])
        with self.assertRaises(BridgeError):
            _parse_capability_index(self._index([empty_groups]))

    def test_capability_index_load_accepts_valid_local_file(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "index.json"
            path.write_text(
                json.dumps(
                    self._index(
                        [
                            self._item(
                                manifestDigest="ab" * 32,
                                id="acme-code-metrics",
                                description="Longer prose about the capability.",
                            )
                        ]
                    )
                ),
                encoding="utf-8",
            )
            loaded = capability_index_load(path)
            self.assertEqual(loaded["version"], 1)
            item = loaded["items"][0]
            self.assertEqual(item["key"], "acme/Code Metrics@2.1.0")
            self.assertEqual(item["identity"]["version"], "2.1.0")
            self.assertEqual(item["manifestDigest"], "ab" * 32)
            self.assertEqual(item["id"], "acme-code-metrics")
            # The raw launch-forbidden content could never have entered.
            self.assertNotIn("command", item)

    # ------------------------------------------------------------------
    # bounded discovery over public summaries only
    # ------------------------------------------------------------------

    def test_capability_index_search_is_bounded_and_public_only(self) -> None:
        index = self._index(
            [
                self._item(name="Code Metrics"),
                self._item(name="Branch Cable Trophy Display", capabilityGroups=["cad", "generative"], summary="Generates a branch cable trophy display model."),
                self._item(name="Log Shipping", publisher="corp", capabilityGroups=["ops"], summary="Ships logs between hosts."),
            ]
        )
        results = capability_index_search(index, "cable trophy")
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["key"], "acme/Branch Cable Trophy Display@2.1.0")
        # Results expose only public metadata, never schemas or launch data.
        for entry in results:
            self.assertNotIn("command", entry)
            self.assertNotIn("schema", entry)
        # Token match crosses groups too.
        self.assertEqual(len(capability_index_search(index, "ops")), 1)
        # Bounded empty query lists from the top.
        self.assertEqual(len(capability_index_search(index, "", limit=2)), 2)
        self.assertEqual(len(capability_index_search(index, "", limit=999)), 3)

    # ------------------------------------------------------------------
    # dedup against the local direct registration inventory
    # ------------------------------------------------------------------

    def test_capability_dedupe_hides_directly_registered_names(self) -> None:
        index = self._index(
            [
                self._item(name="Code Metrics"),
                self._item(name="Branch Cable Trophy Display", capabilityGroups=["cad", "generative"], summary="Generates a branch cable trophy display model."),
            ]
        )
        # The local registry directly registers a server with the same name
        # (case differs on purpose): it must not be offered twice.
        local = [
            {"id": "cand-local-metrics", "name": "code metrics", "summary": "x", "capabilityGroups": [], "enabled": True},
            {"id": "other-thing", "name": "Something Else", "summary": "y", "capabilityGroups": [], "enabled": True},
        ]
        remaining = capability_index_dedupe(index, local)
        self.assertEqual(len(remaining), 1)
        self.assertEqual(remaining[0]["key"], "acme/Branch Cable Trophy Display@2.1.0")
        # A catalog-suggested id that is already registered also dedupes.
        index_with_id = self._index(
            [self._item(name="Code Metrics", id="cand-local-metrics")]
        )
        self.assertEqual(capability_index_dedupe(index_with_id, local), [])

    # ------------------------------------------------------------------
    # install plans never invent launch configuration
    # ------------------------------------------------------------------

    def test_capability_plan_is_read_only_and_never_launches(self) -> None:
        index = self._index([self._item()])
        plan = capability_install_plan(index, "acme/Code Metrics@2.1.0")
        self.assertEqual(plan["action"], "import")
        self.assertIsNone(plan["local"])
        self.assertIn("Operator", plan["nextStep"])
        self.assertNotIn("command", json.dumps(plan))
        # Registry-aware: a local same-name registration yields no action.
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "registry.sqlite3"
            write_registry(database, "warehouse-side", "Warehouse Host")
            registry = Registry(database)
            # Name does not collide, so plan remains import.
            self.assertEqual(capability_install_plan(index, "acme/Code Metrics@2.1.0", registry)["action"], "import")
        # Unknown key is a clean error, not a partial plan.
        with self.assertRaises(BridgeError):
            capability_install_plan(index, "acme/Missing@1.0.0")
        with self.assertRaises(BridgeError):
            capability_install_plan(index, "")



class TransportControlJournalTest(unittest.TestCase):
    def test_typed_http_registry_is_private_and_observation_only(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            manifest = root / "manifest.json"
            manifest.write_text(json.dumps({"servers": [{
                "id": "http-example", "name": "HTTP Example", "summary": "typed",
                "transport": {"type": "streamable-http", "endpoint": "http://127.0.0.1:39001/mcp", "headers": {"Authorization": "secret"}},
                "management": {"ownership": "external", "agentControl": {"enabled": False}},
                "capabilityGroups": ["example"],
            }]}), encoding="utf-8")
            database = root / "registry.sqlite3"
            Registry.initialize_database(database, manifest, replace=True)
            private = Registry(database).launch("http-example")
            self.assertEqual(private["transport"]["type"], "streamable-http")
            self.assertEqual(private["transport"]["headers"]["Authorization"], "secret")
            public = Registry(database).public("http-example")
            self.assertEqual(public["transport"], {"type": "streamable-http"})
            serialized = json.dumps(public)
            self.assertNotIn("39001", serialized)
            self.assertNotIn("secret", serialized)
            node = BridgeNode(side="wsl", registry=Registry(database), local_host="127.0.0.1", local_port=free_port(), link_mode="connect", link_host="127.0.0.1", link_port=free_port())
            status = node._lifecycle_server_status("http-example")
            self.assertFalse(status["lifecycle"]["ready"])
            self.assertEqual(status["lifecycle"]["state"], "not-probed")

    def test_connect_http_cli_is_explicit(self) -> None:
        parser = build_parser("wsl", 8766, "connect")
        args = parser.parse_args(["connect-http", "http-example"])
        self.assertEqual(args.command, "connect-http")
        self.assertEqual(args.target, "http-example")

    def test_projection_enroll_parser_accepts_relay_url(self) -> None:
        parser = build_parser("wsl", 8766, "connect")
        args = parser.parse_args(
            ["projection", "enroll", "cand-123",
             "--native-http", "--relay-url", "http://127.0.0.1:8877",
             "--compatibility-route", "native", "--confirm"]
        )
        self.assertEqual(args.projection_command, "enroll")
        self.assertEqual(args.candidate_id, "cand-123")
        self.assertIs(args.native_http, True)
        self.assertEqual(args.relay_url, "http://127.0.0.1:8877")
        self.assertEqual(args.compatibility_route, "native")
        self.assertIs(args.confirm, True)

    def test_projection_enroll_parser_relay_defaults_to_none(self) -> None:
        parser = build_parser("wsl", 8766, "connect")
        args = parser.parse_args(["projection", "enroll", "cand-123"])
        self.assertIsNone(args.relay_url)
        self.assertIs(args.native_http, False)
        self.assertEqual(args.compatibility_route, "native")

    def test_projection_skips_unsupported_without_blocking_stdio(self) -> None:
        row = {
            "launcher_command": "python3",
            "launcher_args_json": "[]",
            "transport_capabilities_json": json.dumps({"stdio": True, "streamable-http": False}),
            "compatibility_route": "native",
        }
        descriptors = _desired_entry_descriptors(
            row, [("stdio-one", "Stdio", "stdio"), ("http-one", "HTTP", "streamable-http")]
        )
        self.assertEqual([item["name"] for item in descriptors], ["stdio-one"])

    def test_control_surface_and_token_budget_are_constant(self) -> None:
        tools = _control_tools()
        self.assertEqual({tool["name"] for tool in tools}, {"bridge_control", "bridge_diagnostics"})
        serialized = json.dumps({"instructions": CONTROL_INSTRUCTIONS, "tools": tools}, separators=(",", ":"))
        self.assertLess(len(serialized.encode("utf-8")), 8192)
        self.assertNotIn("http-example", serialized)

    def test_event_journal_is_bounded_and_sensitive_trace_needs_confirmation(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            journal = EventJournal(Path(temp) / "events.sqlite3", max_events=100)
            for index in range(140):
                journal.record(side="wsl", category="test", metadata={"index": index})
            self.assertEqual(len(journal.recent(100)), 100)
            preview = journal.start_trace("example", "complete", 60, 4096)
            self.assertFalse(preview["applied"])
            applied = journal.start_trace("example", "complete", 60, 4096, confirm_sensitive=True)
            self.assertTrue(applied["applied"])
            self.assertTrue(applied["traceId"].startswith("trace-"))


class ControlMcpDispatchTest(unittest.TestCase):
    """Control MCP tools/call validation and error-surface behavior."""

    def _call(self, method: str, params: dict) -> dict:
        return _control_mcp_dispatch(
            {"method": method, "params": params}, "127.0.0.1", 1
        )

    def test_unknown_tool_is_a_validation_error(self) -> None:
        with self.assertRaises(JsonRpcError) as caught:
            self._call("tools/call", {"name": "not-a-tool", "arguments": {}})
        self.assertEqual(caught.exception.code, -32602)
        self.assertIn("unknown control tool", str(caught.exception))

    def test_missing_tool_name_is_a_validation_error(self) -> None:
        with self.assertRaises(JsonRpcError) as caught:
            self._call("tools/call", {"arguments": {}})
        self.assertEqual(caught.exception.code, -32602)

    def test_non_object_arguments_are_rejected(self) -> None:
        with self.assertRaises(JsonRpcError) as caught:
            self._call(
                "tools/call",
                {"name": "bridge_control", "arguments": ["not", "an", "object"]},
            )
        self.assertEqual(caught.exception.code, -32602)
        self.assertIn("arguments must be an object", str(caught.exception))

    def test_non_object_params_are_rejected(self) -> None:
        with self.assertRaises(JsonRpcError) as caught:
            _control_mcp_dispatch(
                {"method": "tools/call", "params": ["bad"]}, "127.0.0.1", 1
            )
        self.assertEqual(caught.exception.code, -32602)

    def test_unknown_jsonrpc_method_is_not_found(self) -> None:
        with self.assertRaises(JsonRpcError) as caught:
            self._call("resources/list", {})
        self.assertEqual(caught.exception.code, -32601)
        self.assertIn("method not found", str(caught.exception))

    def test_initialize_requires_protocol_version(self) -> None:
        with self.assertRaises(JsonRpcError) as caught:
            self._call("initialize", {})
        self.assertEqual(caught.exception.code, -32602)
        self.assertIn("protocolVersion", str(caught.exception))

    def test_control_failure_is_surfaced_as_iserror_with_structured_content(self) -> None:
        refusal = {
            "ok": False,
            "code": "agent_control_disabled",
            "summary": "registration did not opt in to agent control",
        }
        with mock.patch(
            "bridge_runtime.local_control_query", return_value=refusal
        ) as query:
            reply = self._call(
                "tools/call",
                {"name": "bridge_control", "arguments": {"action": "restart", "id": "x"}},
            )
        query.assert_called_once_with(
            "127.0.0.1",
            1,
            {
                "op": "control",
                "action": "restart",
                "target": "x",
                "generation": None,
                "confirm": False,
                "impactOverride": False,
                "reason": "",
            },
        )
        self.assertTrue(reply["isError"])
        self.assertEqual(reply["structuredContent"], refusal)
        self.assertEqual(reply["content"][0]["type"], "text")
        self.assertIn("agent_control_disabled", reply["content"][0]["text"])

    def test_control_arguments_are_mapped_and_non_error_results_are_clean(self) -> None:
        captured: dict[str, dict] = {}

        def fake_query(_host: str, _port: int, request: dict) -> dict:
            captured["request"] = request
            return {
                "ok": True,
                "applied": False,
                "confirmRequired": True,
                "action": request.get("action"),
            }

        with mock.patch("bridge_runtime.local_control_query", side_effect=fake_query):
            reply = self._call(
                "tools/call",
                {
                    "name": "bridge_control",
                    "arguments": {
                        "action": "restart",
                        "id": "shared-browser",
                        "expectedGeneration": 3,
                        "confirm": True,
                        "impactOverride": True,
                        "reason": "maintenance window",
                    },
                },
            )
        self.assertNotIn("isError", reply)
        self.assertFalse(reply["structuredContent"]["applied"])
        request = captured["request"]
        self.assertEqual(request["op"], "control")
        self.assertEqual(request["action"], "restart")
        self.assertEqual(request["target"], "shared-browser")
        self.assertEqual(request["generation"], 3)
        self.assertIs(request["confirm"], True)
        self.assertIs(request["impactOverride"], True)
        self.assertEqual(request["reason"], "maintenance window")

    def test_diagnostics_defaults_limit_and_returns_structured_result(self) -> None:
        captured: dict[str, dict] = {}

        def fake_query(_host: str, _port: int, request: dict) -> dict:
            captured["request"] = request
            return {"ok": True, "healthy": True, "recentErrors": []}

        with mock.patch("bridge_runtime.local_control_query", side_effect=fake_query):
            reply = self._call("tools/call", {"name": "bridge_diagnostics", "arguments": {}})
        self.assertNotIn("isError", reply)
        self.assertEqual(captured["request"], {"op": "diagnostics", "limit": 20})
        self.assertTrue(reply["structuredContent"]["healthy"])


class EventJournalTraceRecordTest(unittest.TestCase):
    """Trace-record slice: schema v2, capture levels, filters, budgets, expiry."""

    REQUEST = b'{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"secret":"TOPSECRETVALUE"}}\n'

    @staticmethod
    def _column_names(database: Path, table: str) -> set[str]:
        with sqlite3.connect(database) as connection:
            return {
                str(row[1])
                for row in connection.execute(f"PRAGMA table_info({table})").fetchall()
            }

    def test_age_and_logical_byte_retention_prune_oldest_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "events.sqlite3"
            journal = EventJournal(
                database,
                max_age_seconds=60,
                max_logical_bytes=64 * 1024,
            )
            for index in range(200):
                journal.record(side="wsl", category="large", metadata={"index": index, "data": "x" * 1000})
            recent = journal.recent(100)
            self.assertLess(len(recent), 100)
            self.assertEqual(recent[0]["metadata"]["index"], 199)
            self.assertTrue(database.is_file(), database)
            self.assertTrue(journal.recent(10))
            with mock.patch(
                "bridge_runtime.time.time_ns", side_effect=[0, time.time_ns()]
            ):
                journal.record(side="wsl", category="expired", metadata={"old": True})
            self.assertFalse(any(item["category"] == "expired" for item in journal.recent(100)))

    def test_metadata_bundle_has_digests_and_excludes_trace_payloads(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            journal = EventJournal(root / "events.sqlite3")
            journal.record(side="wsl", category="test", metadata={"safe": True})
            trace = journal.start_trace("srv", "complete", 60, 4096, confirm_sensitive=True)
            journal.capture(target="srv", direction="inbound", data=self.REQUEST)
            destination = root / "diagnostics.zip"
            result = journal.export_bundle(destination, event_limit=10)
            self.assertEqual(result["sha256"], hashlib.sha256(destination.read_bytes()).hexdigest())
            self.assertFalse(result["payloadsIncluded"])
            with zipfile.ZipFile(destination) as archive:
                self.assertEqual(set(archive.namelist()), {"events.json", "manifest.json"})
                events = archive.read("events.json")
                manifest = json.loads(archive.read("manifest.json"))
            self.assertEqual(manifest["members"]["events.json"]["sha256"], hashlib.sha256(events).hexdigest())
            self.assertNotIn(b"TOPSECRETVALUE", destination.read_bytes())
            self.assertTrue(trace["applied"])
        self.assertEqual(_event_journal_path(Path("/tmp/win.sqlite3")).name, "win.events.sqlite3")

    def test_schema_is_v2_and_migrates_v1_database(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            fresh = EventJournal(root / "fresh.sqlite3")
            self.assertEqual(fresh.SCHEMA_VERSION, 2)
            with sqlite3.connect(fresh.path) as connection:
                version = int(connection.execute("PRAGMA user_version").fetchone()[0])
                self.assertEqual(version, 2)
            traces = self._column_names(fresh.path, "traces")
            self.assertIn("direction_filter", traces)
            self.assertIn("method_filter_json", traces)
            records = self._column_names(fresh.path, "trace_records")
            self.assertIn("kind", records)
            self.assertIn("payload_bytes", records)
            # A legacy v1 database (old traces + old trace_records, no producer
            # ever existed) upgrades in place to the v2 record shape.
            legacy = root / "legacy.sqlite3"
            with sqlite3.connect(legacy) as connection:
                connection.executescript(
                    "CREATE TABLE traces (trace_id TEXT PRIMARY KEY, target TEXT NOT NULL, "
                    "level TEXT NOT NULL, byte_budget INTEGER NOT NULL, bytes_used INTEGER NOT NULL "
                    "DEFAULT 0, expires_at_ns INTEGER NOT NULL, confirmed INTEGER NOT NULL, "
                    "created_at_ns INTEGER NOT NULL);"
                    "CREATE TABLE trace_records (seq INTEGER PRIMARY KEY AUTOINCREMENT, "
                    "trace_id TEXT NOT NULL, occurred_at_ns INTEGER NOT NULL, direction TEXT NOT NULL, "
                    "method_class TEXT NOT NULL, payload BLOB NOT NULL);"
                    "PRAGMA user_version = 1;"
                )
            upgraded = EventJournal(legacy)
            self.assertEqual(self._column_names(upgraded.path, "traces") >= {"direction_filter", "method_filter_json"}, True)
            self.assertEqual(self._column_names(upgraded.path, "trace_records") >= {"kind", "payload_bytes"}, True)

    def test_capture_levels_and_readback_without_accidental_payload(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            journal = EventJournal(Path(temp) / "events.sqlite3")
            envelope = journal.start_trace("example", "envelope", 60, 100000)
            snippet = journal.start_trace("example", "snippet", 60, 100000, confirm_sensitive=True)
            complete = journal.start_trace("example", "complete", 60, 100000, confirm_sensitive=True)
            self.assertEqual(journal.capture(target="example", direction="outbound", data=self.REQUEST), 3)
            for trace_id in (envelope["traceId"], snippet["traceId"], complete["traceId"]):
                metadata = journal.trace_records(trace_id=trace_id)
                self.assertEqual(len(metadata), 1)
                self.assertNotIn("payload", metadata[0])
                self.assertGreater(metadata[0]["payload_bytes"], 0)
                self.assertEqual(metadata[0]["direction"], "outbound")
            # envelope stores metadata only: class/method/size, never content.
            envelope_rows = journal.trace_records(trace_id=envelope["traceId"], include_payload=True)
            envelope_payload = base64.b64decode(envelope_rows[0]["payload"])
            envelope_json = json.loads(envelope_payload)
            self.assertEqual(envelope_json["kind"], "request")
            self.assertEqual(envelope_json["method"], "tools/call")
            self.assertNotIn(b"TOPSECRETVALUE", envelope_payload)
            self.assertLess(len(envelope_payload), 200)
            # snippet and complete keep the payload at this small size.
            for trace_id in (snippet["traceId"], complete["traceId"]):
                rows = journal.trace_records(trace_id=trace_id, include_payload=True)
                self.assertEqual(base64.b64decode(rows[0]["payload"]), self.REQUEST)
            # method_class is recorded for requests.
            complete_rows = journal.trace_records(trace_id=complete["traceId"])
            self.assertEqual(complete_rows[0]["method_class"], "tools/call")
            self.assertEqual(complete_rows[0]["kind"], "request")

    def test_capture_snippet_stores_only_bounded_prefix(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            journal = EventJournal(Path(temp) / "events.sqlite3")
            self.assertEqual(journal.SNIPPET_PREFIX_BYTES, 1024)
            session = journal.start_trace("example", "snippet", 60, 1024 * 1024, confirm_sensitive=True)
            data = bytes(range(256)) * 40  # 10240 bytes, repeating pattern.
            self.assertEqual(journal.capture(target="example", direction="inbound", data=data), 1)
            rows = journal.trace_records(trace_id=session["traceId"], include_payload=True)
            stored = base64.b64decode(rows[0]["payload"])
            self.assertEqual(len(stored), 1024)
            self.assertEqual(stored, data[:1024])
            self.assertEqual(rows[0]["payload_bytes"], 1024)
            # A later, already-bounded chunk is stored whole.
            small = b'{"jsonrpc":"2.0","method":"notifications/initialized"}\n'
            journal.capture(target="example", direction="inbound", data=small)
            rows = journal.trace_records(trace_id=session["traceId"], limit=5)
            self.assertEqual(len(rows), 2)

    def test_capture_enforces_budget_and_expiry_atomically(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            journal = EventJournal(root / "events.sqlite3")
            session = journal.start_trace("budgeted", "complete", 3600, 128, confirm_sensitive=True)
            # A 200-byte message does not fit the 128-byte budget: no partial write.
            self.assertEqual(journal.capture(target="budgeted", direction="outbound", data=b"x" * 200), 0)
            self.assertEqual(journal.trace_records(trace_id=session["traceId"]), [])
            # A 100-byte message fits and consumes budget.
            self.assertEqual(journal.capture(target="budgeted", direction="outbound", data=b"y" * 100), 1)
            # Remaining budget (28) cannot fit another 100-byte message.
            self.assertEqual(journal.capture(target="budgeted", direction="outbound", data=b"z" * 100), 0)
            rows = journal.trace_records(trace_id=session["traceId"])
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["payload_bytes"], 100)
            # Expiry ends capture and prunes the session.
            expired = journal.start_trace("shortlived", "envelope", 3600, 4096)
            with sqlite3.connect(journal.path) as connection:
                connection.execute(
                    "UPDATE traces SET expires_at_ns = ? WHERE trace_id = ?",
                    (time.time_ns() - 1, expired["traceId"]),
                )
            self.assertNotIn("shortlived", journal.active_targets())
            self.assertEqual(journal.capture(target="shortlived", direction="outbound", data=self.REQUEST), 0)
            with sqlite3.connect(journal.path) as connection:
                remaining = connection.execute(
                    "SELECT COUNT(*) FROM traces WHERE trace_id = ?", (expired["traceId"],)
                ).fetchone()[0]
            self.assertEqual(remaining, 0)

    def test_capture_honors_direction_and_method_filters(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            journal = EventJournal(Path(temp) / "events.sqlite3")
            outbound_only = journal.start_trace("dir", "envelope", 60, 4096, direction="outbound")
            by_method = journal.start_trace(
                "methods", "envelope", 60, 4096, methods=["tools/call"]
            )
            by_class = journal.start_trace(
                "class", "envelope", 60, 4096, methods=["response"]
            )
            # Direction filter ignores the inbound capture.
            self.assertEqual(journal.capture(target="dir", direction="inbound", data=self.REQUEST), 0)
            self.assertEqual(journal.capture(target="dir", direction="outbound", data=self.REQUEST), 1)
            # Method filter: tools/call request records; initialize does not.
            initialize = b'{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}\n'
            self.assertEqual(journal.capture(target="methods", direction="outbound", data=initialize), 0)
            self.assertEqual(journal.capture(target="methods", direction="outbound", data=self.REQUEST), 1)
            # Response-class messages are requests with no method field.
            response = b'{"jsonrpc":"2.0","id":7,"result":{"ok":true}}\n'
            self.assertEqual(journal.capture(target="methods", direction="outbound", data=response), 0)
            # Reserved class token matches responses.
            self.assertEqual(journal.capture(target="class", direction="outbound", data=response), 1)
            self.assertEqual(journal.capture(target="class", direction="outbound", data=self.REQUEST), 0)
            self.assertEqual(len(journal.trace_records(trace_id=by_method["traceId"])), 1)
            self.assertEqual(len(journal.trace_records(trace_id=by_class["traceId"])), 1)
            # Invalid filter arguments are refused at session start.
            with self.assertRaises(BridgeError):
                journal.start_trace("x", "envelope", 60, 4096, direction="sideways")
            with self.assertRaises(BridgeError):
                journal.start_trace("x", "envelope", 60, 4096, methods=[""])
            with self.assertRaises(BridgeError):
                journal.start_trace("x", "envelope", 60, 4096, methods=["ok"] * 65)

    def test_trace_record_readback_is_bounded(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            journal = EventJournal(Path(temp) / "events.sqlite3")
            session = journal.start_trace("bulk", "complete", 60, 1000000, confirm_sensitive=True)
            for index in range(300):
                line = f'{{"jsonrpc":"2.0","id":{index},"method":"m/{index}","params":{{}}}}\n'.encode()
                journal.capture(target="bulk", direction="outbound", data=line)
            # Default readback is metadata only and limited.
            recent = journal.trace_records(trace_id=session["traceId"], limit=1000)
            self.assertLessEqual(len(recent), 200)
            self.assertNotIn("payload", recent[0])

    def test_complete_stores_exact_full_payload_within_budget(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            journal = EventJournal(Path(temp) / "events.sqlite3")
            data = bytes(range(251)) * 8  # 2008 bytes, well past the snippet prefix
            # Complete with budget exactly equal to payload stores the full
            # payload (not a snippet prefix).
            exact = journal.start_trace("exact", "complete", 60, len(data), confirm_sensitive=True)
            self.assertEqual(journal.capture(target="exact", direction="outbound", data=data), 1)
            rows = journal.trace_records(trace_id=exact["traceId"], include_payload=True)
            self.assertEqual(rows[0]["payload_bytes"], len(data))
            self.assertEqual(base64.b64decode(rows[0]["payload"]), data)
            # One byte over budget: no partial write at all.
            over = journal.start_trace("over", "complete", 60, len(data) - 1, confirm_sensitive=True)
            self.assertEqual(journal.capture(target="over", direction="outbound", data=data), 0)
            self.assertEqual(journal.trace_records(trace_id=over["traceId"]), [])

    def test_envelope_records_metadata_only_for_responses_and_diagnostics_payload_free(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            journal = EventJournal(Path(temp) / "events.sqlite3")
            session = journal.start_trace("demo", "envelope", 60, 4096)
            response = b'{"jsonrpc":"2.0","id":9,"result":{"secret":"SENSITIVE-RESULT"}}\n'
            journal.record(
                side="wsl", category="runtime", target="demo",
                outcome="observed", metadata={"messageClass": "runtime"},
            )
            self.assertEqual(journal.capture(target="demo", direction="inbound", data=response), 1)
            # Ordinary diagnostics carry no payload key.
            metadata = journal.trace_records(trace_id=session["traceId"])
            self.assertEqual(metadata[0]["kind"], "response")
            self.assertNotIn("payload", metadata[0])
            for event in journal.recent(10):
                self.assertNotIn("payload", event)
            # Even explicit envelope payload readback is tiny metadata only.
            rows = journal.trace_records(trace_id=session["traceId"], include_payload=True)
            envelope_payload = base64.b64decode(rows[0]["payload"])
            self.assertNotIn(b"SENSITIVE-RESULT", envelope_payload)
            self.assertLess(len(envelope_payload), 200)


class RemoteControlJournalTest(unittest.IsolatedAsyncioTestCase):
    async def test_requester_records_returned_operation_id(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            database = root / "registry.sqlite3"
            write_registry(database, "local", "Local")
            journal = EventJournal(root / "events.sqlite3")
            node = BridgeNode(
                side="wsl", registry=Registry(database), local_host="127.0.0.1",
                local_port=free_port(), link_mode="connect", link_host="127.0.0.1",
                link_port=free_port(), journal=journal,
            )
            node.link_ready.set()

            async def fake_send(frame: dict[str, object]) -> None:
                node.pending_control[str(frame["request"])].set_result(
                    {"ok": True, "applied": True, "operationId": "op-shared", "id": "peer"}
                )

            node._send_frame = fake_send  # type: ignore[method-assign]
            result = await node._remote_control({"action": "restart", "target": "peer"})
            self.assertEqual(result["operationId"], "op-shared")
            record = journal.recent(1)[0]
            self.assertEqual(record["category"], "remote-lifecycle")
            self.assertEqual(record["operation_id"], "op-shared")
            self.assertEqual(record["target"], "peer")


class EventJournalRetentionTest(unittest.TestCase):
    """P8 retention slice: bounded options plus age and byte-budget pruning."""

    @staticmethod
    def _logical_events(connection: sqlite3.Connection) -> int:
        return int(
            connection.execute(
                "SELECT COALESCE(SUM(length(metadata_json) + length(category) + 96), 0) "
                "FROM events"
            ).fetchone()[0]
        )

    @staticmethod
    def _logical_records(connection: sqlite3.Connection) -> int:
        return int(
            connection.execute(
                "SELECT COALESCE(SUM(payload_bytes + 96), 0) FROM trace_records"
            ).fetchone()[0]
        )

    def test_constructor_retention_options_are_safe_and_bounded(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            journal = EventJournal(Path(temp) / "events.sqlite3")
            self.assertEqual(journal.max_events, 10000)
            self.assertEqual(journal.max_age_seconds, 7 * 24 * 3600)
            self.assertEqual(journal.max_logical_bytes, 64 * 1024 * 1024)
            tiny = EventJournal(
                Path(temp) / "tiny.sqlite3",
                max_events=1,
                max_age_seconds=1,
                max_logical_bytes=1,
            )
            self.assertEqual(tiny.max_events, 100)
            self.assertEqual(tiny.max_age_seconds, 60)
            self.assertEqual(tiny.max_logical_bytes, 64 * 1024)
            huge = EventJournal(
                Path(temp) / "huge.sqlite3",
                max_events=10 ** 9,
                max_age_seconds=10 ** 9,
                max_logical_bytes=10 ** 12,
            )
            self.assertEqual(huge.max_events, 100000)
            self.assertEqual(huge.max_age_seconds, 365 * 24 * 3600)
            self.assertEqual(huge.max_logical_bytes, 1024 * 1024 * 1024)

    def test_byte_budget_evicts_oldest_trace_records_and_keeps_accounting(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "events.sqlite3"
            journal = EventJournal(database, max_logical_bytes=64 * 1024)
            session = journal.start_trace(
                "bulk", "complete", 3600, 4 * 1024 * 1024, confirm_sensitive=True
            )
            payload = (
                b'{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"pad":"'
                + b"x" * 3960
                + b'"}}\n'
            )
            payload_bytes = len(payload)
            for _ in range(40):
                self.assertEqual(
                    journal.capture(target="bulk", direction="outbound", data=payload), 1
                )
            with sqlite3.connect(database) as connection:
                newest_before = int(
                    connection.execute(
                        "SELECT COALESCE(MAX(seq), 0) FROM trace_records"
                    ).fetchone()[0]
                )
            journal.prune()
            with sqlite3.connect(database) as connection:
                count, newest_after = connection.execute(
                    "SELECT COUNT(*), COALESCE(MAX(seq), 0) FROM trace_records"
                ).fetchone()
                used = int(
                    connection.execute(
                        "SELECT bytes_used FROM traces WHERE trace_id = ?",
                        (session["traceId"],),
                    ).fetchone()[0]
                )
                stored = int(
                    connection.execute(
                        "SELECT COALESCE(SUM(payload_bytes), 0) FROM trace_records "
                        "WHERE trace_id = ?",
                        (session["traceId"],),
                    ).fetchone()[0]
                )
                logical = self._logical_records(connection)
            self.assertGreater(count, 0)
            self.assertLess(count, 40)
            # Eviction is oldest-first: the newest record always survives.
            self.assertEqual(newest_after, newest_before)
            # Evicting records returns their payload to the live session's
            # byte budget, so capture accounting stays exact and the session
            # can keep recording new evidence.
            self.assertEqual(used, stored)
            self.assertLessEqual(logical, journal.max_logical_bytes)
            self.assertIn("bulk", journal.active_targets())
            self.assertEqual(
                journal.capture(target="bulk", direction="outbound", data=payload), 1
            )
            self.assertEqual(stored, payload_bytes * count)

    def test_byte_budget_prefers_older_trace_records_over_newer_events(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "events.sqlite3"
            journal = EventJournal(database, max_logical_bytes=64 * 1024)
            session = journal.start_trace(
                "traced", "complete", 3600, 1024 * 1024, confirm_sensitive=True
            )
            payload = (
                b'{"jsonrpc":"2.0","id":2,"method":"m/2","params":{"pad":"'
                + b"y" * 3960
                + b'"}}\n'
            )
            for _ in range(30):
                self.assertEqual(
                    journal.capture(target="traced", direction="outbound", data=payload), 1
                )
            # This event is recorded after all trace records, so it is the
            # newest evidence; byte-budget eviction must trim the older trace
            # records instead of evicting the event.
            journal.record(side="wsl", category="fresh", metadata={"kept": True})
            with sqlite3.connect(database) as connection:
                events = int(
                    connection.execute("SELECT COUNT(*) FROM events").fetchone()[0]
                )
                records = self._logical_records(connection)
                total = self._logical_events(connection) + records
            self.assertEqual(events, 1)
            self.assertGreater(records, 0)
            self.assertLess(records, 30 * (len(payload) + 96))
            self.assertLessEqual(total, journal.max_logical_bytes)

    def test_age_window_prunes_old_events_and_expired_trace_sessions(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "events.sqlite3"
            journal = EventJournal(database, max_age_seconds=3600)
            journal.record(side="wsl", category="old", metadata={"n": 1})
            now = time.time_ns()
            with sqlite3.connect(database) as connection:
                connection.execute(
                    "UPDATE events SET occurred_at_ns = ?", (now - 7200 * 10 ** 9,)
                )
            journal.record(side="wsl", category="current", metadata={"n": 2})
            # Backdating two seconds of an otherwise-live session past expiry
            # and past the record age window removes it with its records.
            session = journal.start_trace("gone", "complete", 3600, 65536, confirm_sensitive=True)
            journal.capture(target="gone", direction="outbound", data=b"x" * 512)
            with sqlite3.connect(database) as connection:
                connection.execute(
                    "UPDATE traces SET expires_at_ns = ? WHERE trace_id = ?",
                    (now - 1, session["traceId"]),
                )
                connection.execute(
                    "UPDATE trace_records SET occurred_at_ns = ?",
                    (now - 7200 * 10 ** 9,),
                )
            journal.prune()
            with sqlite3.connect(database) as connection:
                expired = int(
                    connection.execute(
                        "SELECT COUNT(*) FROM traces WHERE trace_id = ?",
                        (session["traceId"],),
                    ).fetchone()[0]
                )
                orphans = int(
                    connection.execute(
                        "SELECT COUNT(*) FROM trace_records WHERE trace_id = ?",
                        (session["traceId"],),
                    ).fetchone()[0]
                )
                event_categories = [
                    str(row[0])
                    for row in connection.execute(
                        "SELECT category FROM events ORDER BY seq ASC"
                    ).fetchall()
                ]
            self.assertEqual(expired, 0)
            self.assertEqual(orphans, 0)
            self.assertEqual(event_categories, ["current"])

    def test_age_window_prunes_old_records_of_live_session_with_exact_accounting(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "events.sqlite3"
            journal = EventJournal(database, max_age_seconds=60)
            session = journal.start_trace(
                "live", "complete", 3600, 65536, confirm_sensitive=True
            )
            self.assertEqual(
                journal.capture(target="live", direction="outbound", data=b"a" * 512), 1
            )
            now = time.time_ns()
            with sqlite3.connect(database) as connection:
                # One old record and one recent record; the session stays live.
                connection.execute(
                    "UPDATE trace_records SET occurred_at_ns = ? WHERE seq = "
                    "(SELECT MIN(seq) FROM trace_records)",
                    (now - 120 * 10 ** 9,),
                )
            self.assertEqual(
                journal.capture(target="live", direction="outbound", data=b"b" * 512), 1
            )
            journal.prune()
            with sqlite3.connect(database) as connection:
                records = int(
                    connection.execute(
                        "SELECT COUNT(*) FROM trace_records WHERE trace_id = ?",
                        (session["traceId"],),
                    ).fetchone()[0]
                )
                used = int(
                    connection.execute(
                        "SELECT bytes_used FROM traces WHERE trace_id = ?",
                        (session["traceId"],),
                    ).fetchone()[0]
                )
                stored = int(
                    connection.execute(
                        "SELECT COALESCE(SUM(payload_bytes), 0) FROM trace_records "
                        "WHERE trace_id = ?",
                        (session["traceId"],),
                    ).fetchone()[0]
                )
            self.assertEqual(records, 1)
            # The session is unexpired, so its age-pruned record returned its
            # payload to bytes_used: the live session keeps exact accounting.
            self.assertEqual(used, stored)
            self.assertEqual(used, 512)
            self.assertIn("live", journal.active_targets())
            self.assertEqual(
                journal.capture(target="live", direction="outbound", data=b"c" * 512), 1
            )


class EventJournalDurabilityTest(unittest.TestCase):
    """P8 crash-recovery and concurrent-writer durability evidence."""

    def test_concurrent_writers_lose_no_committed_rows(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "events.sqlite3"
            journal = EventJournal(database)
            workers = 4
            per_worker = 40
            expected = workers * per_worker

            def writer(worker_index: int) -> None:
                for number in range(per_worker):
                    journal.record(
                        side="wsl",
                        category="writer",
                        metadata={"worker": worker_index, "n": number},
                    )

            with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
                list(executor.map(writer, range(workers)))
            with sqlite3.connect(database) as connection:
                total = int(
                    connection.execute(
                        "SELECT COUNT(*) FROM events WHERE category = 'writer'"
                    ).fetchone()[0]
                )
                distinct = int(
                    connection.execute(
                        "SELECT COUNT(DISTINCT metadata_json) FROM events "
                        "WHERE category = 'writer'"
                    ).fetchone()[0]
                )
                integrity = str(
                    connection.execute("PRAGMA integrity_check").fetchone()[0]
                )
            self.assertEqual(total, expected)
            # No interleaved commit lost or duplicated a row.
            self.assertEqual(distinct, expected)
            self.assertEqual(integrity, "ok")

    def test_reopen_recovers_committed_wal_rows_after_unclean_exit(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "events.sqlite3"
            journal = EventJournal(database, max_events=1000)
            for number in range(5):
                journal.record(side="win", category="boot", metadata={"n": number})
            # A second writer (like a crashed process) committed a row and
            # closed without any explicit checkpoint; WAL frames hold it.
            with sqlite3.connect(database, timeout=5) as connection:
                connection.execute("PRAGMA journal_mode = WAL")
                connection.execute(
                    "INSERT INTO events (occurred_at_ns, monotonic_ns, side, category, "
                    "metadata_json) VALUES (?, ?, ?, ?, ?)",
                    (
                        time.time_ns(),
                        time.monotonic_ns(),
                        "win",
                        "crash-persist",
                        json.dumps({"from": "raw-writer"}),
                    ),
                )
            # A fresh journal instance over the same file replays committed
            # WAL frames and stays fully usable (crash recovery).
            reopened = EventJournal(database, max_events=1000)
            categories = [item["category"] for item in reopened.recent(100)]
            self.assertIn("crash-persist", categories)
            self.assertEqual(categories.count("boot"), 5)
            reopened.record(side="win", category="after-reopen", metadata={"ok": True})
            with sqlite3.connect(database) as connection:
                total = int(
                    connection.execute("SELECT COUNT(*) FROM events").fetchone()[0]
                )
                integrity = str(
                    connection.execute("PRAGMA integrity_check").fetchone()[0]
                )
            self.assertEqual(total, 7)
            self.assertEqual(integrity, "ok")


class TraceCliGateTest(unittest.TestCase):
    """The trace CLI's sensitive-capture and payload-readback gates."""

    def _run_trace(self, root: Path, *arguments: str) -> dict[str, Any]:
        environment = os.environ.copy()
        environment["PYTHONDONTWRITEBYTECODE"] = "1"
        process = subprocess.run(
            [sys.executable, str(WSL), "trace", "--journal", str(root / "events.sqlite3"), *arguments],
            cwd=ROOT,
            env=environment,
            capture_output=True,
            text=True,
            timeout=60,
        )
        self.assertEqual(process.returncode, 0, process.stderr)
        return json.loads(process.stdout)

    def test_start_gate_blocks_sensitive_levels_without_confirmation(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            complete = self._run_trace(root, "start", "--target", "demo", "--level", "complete")
            self.assertFalse(complete["applied"])
            self.assertTrue(complete["requiresConfirmation"])
            snippet = self._run_trace(root, "start", "--target", "demo", "--level", "snippet")
            self.assertFalse(snippet["applied"])
            # Envelope is metadata only and applies without confirmation.
            envelope = self._run_trace(root, "start", "--target", "demo", "--level", "envelope")
            self.assertTrue(envelope["applied"])
            self.assertTrue(envelope["traceId"].startswith("trace-"))
            # Filters are echoed on the applied session.
            applied = self._run_trace(
                root, "start", "--target", "demo", "--level", "complete",
                "--direction", "outbound", "--method", "initialize", "--confirm-sensitive",
            )
            self.assertTrue(applied["applied"])
            self.assertEqual(applied["direction"], "outbound")
            self.assertEqual(applied["methods"], ["initialize"])

    def test_records_payload_readback_is_gated_and_default_is_metadata_only(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            started = self._run_trace(
                root, "start", "--target", "demo", "--level", "complete",
                "--direction", "outbound", "--confirm-sensitive",
            )
            journal = EventJournal(root / "events.sqlite3")
            message = b'{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}\n'
            self.assertEqual(journal.capture(target="demo", direction="outbound", data=message), 1)
            # Default records readback: metadata only, no payload key.
            metadata = self._run_trace(root, "records", "--trace-id", started["traceId"])
            self.assertEqual(len(metadata["records"]), 1)
            self.assertNotIn("payload", metadata["records"][0])
            # Payload readback without confirmation is refused.
            denied = self._run_trace(
                root, "records", "--trace-id", started["traceId"], "--include-payload"
            )
            self.assertFalse(denied["applied"])
            self.assertTrue(denied["requiresConfirmation"])
            # Confirmed payload readback returns base64 content.
            allowed = self._run_trace(
                root, "records", "--trace-id", started["traceId"],
                "--include-payload", "--confirm-sensitive",
            )
            self.assertEqual(len(allowed["records"]), 1)
            self.assertEqual(
                base64.b64decode(allowed["records"][0]["payload"]), message
            )
            self.assertEqual(allowed["records"][0]["direction"], "outbound")
            self.assertEqual(allowed["records"][0]["method_class"], "initialize")


class BridgeNodeTraceRoutingTest(unittest.IsolatedAsyncioTestCase):
    """BridgeNode data hooks route only active trace targets to the worker."""

    async def test_node_hooks_route_only_active_targets(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            manifest = root / "manifest.json"
            manifest.write_text(json.dumps({"servers": []}), encoding="utf-8")
            database = root / "registry.sqlite3"
            Registry.initialize_database(database, manifest, replace=True)
            journal = EventJournal(root / "events.sqlite3")
            node = BridgeNode(
                side="wsl",
                registry=Registry(database),
                local_host="127.0.0.1",
                local_port=free_port(),
                link_mode="connect",
                link_host="127.0.0.1",
                link_port=free_port(),
                journal=journal,
            )
            node._trace_queue = asyncio.Queue(maxsize=16)
            stream = StreamState(stream_id="stream-a")
            stream.target = "sample-target"
            payload = b'{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}\n'
            # No active session: nothing is enqueued.
            await node._maybe_capture(stream, "outbound", payload)
            self.assertEqual(node._trace_queue.qsize(), 0)
            session = journal.start_trace(
                "sample-target", "complete", 60, 4096, confirm_sensitive=True
            )
            await node._refresh_trace_targets()
            self.assertIn("sample-target", node._trace_targets)
            # Another target is not routed.
            other = StreamState(stream_id="stream-b")
            other.target = "other-target"
            await node._maybe_capture(other, "outbound", payload)
            self.assertEqual(node._trace_queue.qsize(), 0)
            # Matching target is enqueued and the worker persists it.
            await node._maybe_capture(stream, "outbound", payload)
            self.assertEqual(node._trace_queue.qsize(), 1)
            item = await node._trace_queue.get()
            self.assertEqual(item, ("sample-target", "outbound", payload))
            await node._trace_process_item(*item)
            records = journal.trace_records(trace_id=session["traceId"])
            self.assertEqual(len(records), 1)
            self.assertEqual(records[0]["direction"], "outbound")
            self.assertEqual(records[0]["method_class"], "initialize")
            # Inbound routing carries the inbound label.
            await node._maybe_capture(stream, "inbound", payload)
            item = await node._trace_queue.get()
            self.assertEqual(item, ("sample-target", "inbound", payload))


class HttpControlContractRegistryTest(unittest.TestCase):
    """Registered bounded streamable-http lifecycle control contract schema.

    The contract is validated, normalized, and persisted privately; public and
    peer-visible registry views never disclose any contract detail.
    """

    @staticmethod
    def _write(database: Path, servers: list[dict[str, Any]]) -> None:
        manifest = database.with_name(database.name + ".contract.json")
        manifest.write_text(
            json.dumps({"servers": servers}, indent=2), encoding="utf-8"
        )
        Registry.initialize_database(database, manifest, replace=True)

    @staticmethod
    def _row(
        server_id: str = "http-ctl",
        *,
        endpoint: str = "http://127.0.0.1:39091/mcp",
        ownership: str = "external-controlled",
        contract: dict[str, Any] | None = None,
        transport_type: str = "streamable-http",
    ) -> dict[str, Any]:
        management: dict[str, Any] = {"ownership": ownership}
        if contract is not None:
            management["controlContract"] = contract
        return {
            "id": server_id,
            "name": "HTTP control fixture",
            "summary": "typed",
            "transport": {
                "type": transport_type,
                "endpoint": endpoint,
                "headers": {"Authorization": "owner-secret"},
            },
            "management": management,
            "capabilityGroups": ["test"],
        }

    def test_valid_contract_persists_private_and_public_stays_clean(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "registry.sqlite3"
            self._write(
                database,
                [
                    self._row(
                        contract={
                            "restart": {
                                "method": "POST",
                                "path": "/control/restart",
                                "timeoutSeconds": 7,
                                "successStatuses": [202, 200],
                                "readiness": {
                                    "method": "GET",
                                    "path": "/ready",
                                    "timeoutSeconds": 3,
                                    "successStatuses": [200],
                                },
                            },
                            "stop": {
                                "method": "POST",
                                "url": "http://127.0.0.1:39091/control/stop",
                                "timeoutSeconds": 5,
                                "successStatuses": [204],
                            },
                        }
                    )
                ],
            )
            private = Registry(database).launch("http-ctl")
            contract = private["management"]["controlContract"]
            self.assertEqual(sorted(contract), ["restart", "stop"])
            self.assertEqual(contract["restart"]["method"], "POST")
            self.assertEqual(contract["restart"]["path"], "/control/restart")
            self.assertEqual(contract["restart"]["timeoutSeconds"], 7.0)
            self.assertEqual(contract["restart"]["successStatuses"], [200, 202])
            self.assertEqual(contract["restart"]["readiness"]["method"], "GET")
            self.assertEqual(
                contract["restart"]["readiness"]["path"], "/ready"
            )
            self.assertEqual(
                contract["stop"]["url"], "http://127.0.0.1:39091/control/stop"
            )
            public = Registry(database).public("http-ctl")
            serialized = json.dumps(public)
            self.assertNotIn("controlContract", serialized)
            self.assertNotIn("/control/", serialized)
            self.assertNotIn("timeoutSeconds", serialized)
            self.assertNotIn("readiness", serialized)
            self.assertNotIn("owner-secret", serialized)
            self.assertNotIn("39091", serialized)

    def _rejected(self, contract: dict[str, Any]) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "registry.sqlite3"
            with self.assertRaises(BridgeError):
                self._write(database, [self._row(contract=contract)])

    def test_invalid_contracts_are_rejected(self) -> None:
        # Both path and url at once.
        self._rejected(
            {
                "restart": {
                    "method": "POST",
                    "path": "/control/restart",
                    "url": "http://127.0.0.1:39091/control/restart",
                    "timeoutSeconds": 5,
                    "successStatuses": [200],
                }
            }
        )
        # Neither path nor url.
        self._rejected(
            {
                "restart": {
                    "method": "POST",
                    "timeoutSeconds": 5,
                    "successStatuses": [200],
                }
            }
        )
        # Non-loopback url.
        self._rejected(
            {
                "restart": {
                    "method": "POST",
                    "url": "http://192.0.2.10/control/restart",
                    "timeoutSeconds": 5,
                    "successStatuses": [200],
                }
            }
        )
        # Relative url (must be absolute).
        self._rejected(
            {
                "restart": {
                    "method": "POST",
                    "url": "/control/restart",
                    "timeoutSeconds": 5,
                    "successStatuses": [200],
                }
            }
        )
        # Readiness must be the fixed GET method.
        self._rejected(
            {
                "restart": {
                    "method": "POST",
                    "path": "/control/restart",
                    "timeoutSeconds": 5,
                    "successStatuses": [200],
                    "readiness": {
                        "method": "POST",
                        "path": "/ready",
                        "timeoutSeconds": 5,
                        "successStatuses": [200],
                    },
                }
            }
        )
        # Unknown lifecycle action.
        self._rejected({"frobnicate": {"method": "POST", "path": "/x", "successStatuses": [200]}})
        # Empty contract.
        self._rejected({})
        # Unknown interface key.
        self._rejected(
            {
                "restart": {
                    "method": "POST",
                    "path": "/control/restart",
                    "timeoutSeconds": 5,
                    "successStatuses": [200],
                    "command": "reboot",
                }
            }
        )
        # Duplicate / out-of-range success status.
        self._rejected(
            {
                "restart": {
                    "method": "POST",
                    "path": "/control/restart",
                    "timeoutSeconds": 5,
                    "successStatuses": [200, 200],
                }
            }
        )
        self._rejected(
            {
                "restart": {
                    "method": "POST",
                    "path": "/control/restart",
                    "timeoutSeconds": 5,
                    "successStatuses": [199],
                }
            }
        )
        # Path without leading slash / bounded timeout / unsupported method.
        self._rejected(
            {
                "restart": {
                    "method": "POST",
                    "path": "control/restart",
                    "timeoutSeconds": 5,
                    "successStatuses": [200],
                }
            }
        )
        self._rejected(
            {
                "restart": {
                    "method": "POST",
                    "path": "/control/restart",
                    "timeoutSeconds": 500,
                    "successStatuses": [200],
                }
            }
        )
        self._rejected(
            {
                "restart": {
                    "method": "PATCH",
                    "path": "/control/restart",
                    "timeoutSeconds": 5,
                    "successStatuses": [200],
                }
            }
        )

    def test_peer_visible_describe_never_discloses_the_contract(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "registry.sqlite3"
            self._write(
                database,
                [
                    self._row(
                        contract={
                            "restart": {
                                "method": "POST",
                                "path": "/control/restart",
                                "timeoutSeconds": 7,
                                "successStatuses": [200],
                            }
                        }
                    )
                ],
            )
            node = BridgeNode(
                side="win",
                registry=Registry(database),
                local_host="127.0.0.1",
                local_port=free_port(),
                link_mode="listen",
                link_host="127.0.0.1",
                link_port=free_port(),
            )
            describe = node._with_lifecycle_fields(
                "describe",
                Registry(database).query("describe", {"id": "http-ctl"}),
            )
            self.assertEqual(describe["lifecycle"]["mode"], "http")
            self.assertEqual(describe["lifecycle"]["state"], "not-probed")
            serialized = json.dumps(describe)
            self.assertNotIn("controlContract", serialized)
            self.assertNotIn("controllableActions", serialized)
            self.assertNotIn("/control/", serialized)
            self.assertNotIn("timeoutSeconds", serialized)

    def test_contract_requires_streamable_http_and_external_controlled(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "registry.sqlite3"
            with self.assertRaises(BridgeError):
                # stdio rows may never carry a control contract.
                self._write(
                    database,
                    [
                        self._row(
                            transport_type="stdio",
                            endpoint="",
                            ownership="bridge-managed",
                            contract={
                                "restart": {
                                    "method": "POST",
                                    "path": "/control/restart",
                                    "timeoutSeconds": 5,
                                    "successStatuses": [200],
                                }
                            },
                        )
                    ],
                )
            with self.assertRaises(BridgeError):
                # external (not external-controlled) http rows may not either.
                self._write(
                    database,
                    [
                        self._row(
                            ownership="external",
                            contract={
                                "restart": {
                                    "method": "POST",
                                    "path": "/control/restart",
                                    "timeoutSeconds": 5,
                                    "successStatuses": [200],
                                }
                            },
                        )
                    ],
                )


class HttpControlContractExecutionTest(unittest.IsolatedAsyncioTestCase):
    """Owner-side execution of a registered bounded HTTP control contract."""

    def setUp(self) -> None:
        import http.server

        self.requests: list[tuple[str, str, str | None]] = []
        state: dict[str, Any] = {"ready": True}

        class _Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, *args: Any) -> None:  # silence
                pass

            def _record(self) -> None:
                auth = self.headers.get("Authorization")
                self.server.requests.append(  # type: ignore[attr-defined]
                    (self.command, self.path, auth)
                )

            def do_GET(self) -> None:  # noqa: N802
                self._record()
                if self.path == "/ready":
                    status = 200 if state["ready"] else 503
                    self.send_response(status)
                    self.end_headers()
                    return
                self.send_response(404)
                self.end_headers()

            def do_POST(self) -> None:  # noqa: N802
                self._record()
                if self.path == "/control/restart":
                    state["ready"] = True
                    self.send_response(202)
                elif self.path == "/control/stop":
                    state["ready"] = False
                    self.send_response(200)
                elif self.path == "/control/drain":
                    self.send_response(202)
                else:
                    self.send_response(404)
                self.end_headers()

        class _Server(http.server.ThreadingHTTPServer):
            requests: list[tuple[str, str, str | None]] = []

            daemon_threads = True

        self.server = _Server(("127.0.0.1", 0), _Handler)
        host, port = self.server.server_address[:2]
        self.server.requests = self.requests
        self.endpoint = f"http://127.0.0.1:{port}/mcp"
        self._thread = threading.Thread(
            target=self.server.serve_forever, daemon=True
        )
        self._thread.start()

    async def asyncTearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()

    def _registry(self, database: Path, *, contract: dict[str, Any] | None, ownership: str = "external-controlled") -> None:
        manifest = database.with_name(database.name + ".manifest.json")
        management: dict[str, Any] = {"ownership": ownership}
        if contract is not None:
            management["controlContract"] = contract
        manifest.write_text(
            json.dumps(
                {
                    "servers": [
                        {
                            "id": "http-ctl",
                            "name": "HTTP control",
                            "summary": "typed",
                            "transport": {
                                "type": "streamable-http",
                                "endpoint": self.endpoint,
                                "headers": {"Authorization": "Bearer owner-secret"},
                            },
                            "management": management,
                            "capabilityGroups": ["test"],
                        }
                    ]
                },
                indent=2,
            ),
            encoding="utf-8",
        )
        Registry.initialize_database(database, manifest, replace=True)

    def _node(self, database: Path) -> BridgeNode:
        return BridgeNode(
            side="win",
            registry=Registry(database),
            local_host="127.0.0.1",
            local_port=free_port(),
            link_mode="listen",
            link_host="127.0.0.1",
            link_port=free_port(),
        )

    @staticmethod
    def _contract() -> dict[str, Any]:
        return {
            "restart": {
                "method": "POST",
                "path": "/control/restart",
                "timeoutSeconds": 5,
                "successStatuses": [202, 200],
                "readiness": {
                    "method": "GET",
                    "path": "/ready",
                    "timeoutSeconds": 5,
                    "successStatuses": [200],
                },
            },
            "stop": {
                "method": "POST",
                "path": "/control/stop",
                "timeoutSeconds": 5,
                "successStatuses": [200],
            },
        }

    async def test_preview_does_not_touch_the_endpoint(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "registry.sqlite3"
            self._registry(database, contract=self._contract())
            node = self._node(database)
            preview = await node._lifecycle_control(
                {"action": "restart", "target": "http-ctl", "confirm": False}
            )
            self.assertFalse(preview["applied"])
            self.assertTrue(preview["confirmRequired"])
            self.assertEqual(preview["mode"], "http-external-controlled")
            self.assertEqual(self.requests, [])
            self.assertNotIn("/control/", json.dumps(preview))

    async def test_confirmed_restart_calls_contract_then_readiness(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "registry.sqlite3"
            self._registry(database, contract=self._contract())
            node = self._node(database)
            journal = EventJournal(Path(temp) / "events.sqlite3", max_events=100)
            node.journal = journal
            result = await node._lifecycle_control(
                {"action": "restart", "target": "http-ctl", "confirm": True}
            )
            self.assertTrue(result["applied"], result)
            inner = result["result"]
            self.assertEqual(inner["httpStatus"], 202)
            self.assertTrue(inner["success"])
            self.assertTrue(inner["readinessAttempted"])
            self.assertTrue(inner["readinessOk"])
            self.assertEqual(inner["readinessStatus"], 200)
            methods = [item[0] for item in self.requests]
            paths = [item[1] for item in self.requests]
            self.assertEqual(methods, ["POST", "GET"])
            self.assertEqual(paths, ["/control/restart", "/ready"])
            for _method, _path, auth in self.requests:
                self.assertEqual(auth, "Bearer owner-secret")
            self.assertTrue(result["operationId"].startswith("op-"))
            # Owner journal correlates the operation id across lifecycle and
            # per-phase rows, exactly like the stdio lifecycle paths.
            operation_id = result["operationId"]
            events = journal.recent(50)
            lifecycle_rows = [
                row
                for row in events
                if row["category"] == "lifecycle" and row["operation_id"] == operation_id
            ]
            self.assertEqual(len(lifecycle_rows), 1)
            self.assertEqual(lifecycle_rows[0]["outcome"], "applied")
            self.assertEqual(lifecycle_rows[0]["target"], "http-ctl")
            phase_rows = [
                row
                for row in events
                if row["category"] == "lifecycle-phase"
                and row["operation_id"] == operation_id
            ]
            phases = {row["metadata"].get("phase"): row["outcome"] for row in phase_rows}
            self.assertEqual(phases["resolve-contract"], "ok")
            self.assertEqual(phases["http-restart"], "success")
            self.assertEqual(phases["readiness"], "ok")

    async def test_confirmed_stop_without_readiness(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "registry.sqlite3"
            self._registry(database, contract=self._contract())
            node = self._node(database)
            result = await node._lifecycle_control(
                {"action": "stop", "target": "http-ctl", "confirm": True}
            )
            self.assertTrue(result["applied"], result)
            inner = result["result"]
            self.assertEqual(inner["httpStatus"], 200)
            self.assertTrue(inner["success"])
            self.assertFalse(inner["readinessAttempted"])
            self.assertEqual(self.requests[0][1], "/control/stop")

    async def test_mutation_requires_a_registered_contract(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "registry.sqlite3"
            # external ownership without any contract.
            self._registry(database, contract=None, ownership="external")
            node = self._node(database)
            with self.assertRaises(BridgeError) as error:
                await node._lifecycle_control(
                    {"action": "restart", "target": "http-ctl", "confirm": True}
                )
            self.assertIn("external-controlled", str(error.exception))
            # external-controlled but the contract lacks the requested action.
            database2 = Path(temp) / "registry2.sqlite3"
            self._registry(database2, contract={"stop": self._contract()["stop"]})
            node2 = self._node(database2)
            with self.assertRaises(BridgeError) as error:
                await node2._lifecycle_control(
                    {"action": "restart", "target": "http-ctl", "confirm": True}
                )
            self.assertIn("no registered bounded control contract", str(error.exception))
            self.assertEqual(self.requests, [])

    async def test_generation_guard_is_not_applicable_to_http_contract(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "registry.sqlite3"
            self._registry(database, contract=self._contract())
            node = self._node(database)
            with self.assertRaises(BridgeError) as error:
                await node._lifecycle_control(
                    {
                        "action": "restart",
                        "target": "http-ctl",
                        "generation": 3,
                        "confirm": False,
                    }
                )
            self.assertIn("no owned generation", str(error.exception))
            self.assertEqual(self.requests, [])

    async def test_agent_control_is_a_separate_opt_in_and_status_is_redacted(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "registry.sqlite3"
            manifest = database.with_name(database.name + ".manifest.json")
            manifest.write_text(
                json.dumps(
                    {
                        "servers": [
                            {
                                "id": "http-ctl",
                                "name": "HTTP control",
                                "summary": "typed",
                                "transport": {
                                    "type": "streamable-http",
                                    "endpoint": self.endpoint,
                                    "headers": {"Authorization": "Bearer owner-secret"},
                                },
                                "management": {
                                    "ownership": "external-controlled",
                                    "agentControl": {
                                        "enabled": True,
                                        "allowedActions": ["restart", "stop"],
                                    },
                                    "controlContract": self._contract(),
                                },
                                "capabilityGroups": ["test"],
                            }
                        ]
                    },
                    indent=2,
                ),
                encoding="utf-8",
            )
            Registry.initialize_database(database, manifest, replace=True)
            node = self._node(database)
            # Agent host-wide status lists the opted-in row but strips all
            # contract details and pids.
            status = await node._agent_control({"action": "status"})
            rows = {row["id"]: row for row in status["servers"]}
            self.assertIn("http-ctl", rows)
            lifecycle = rows["http-ctl"]["lifecycle"]
            self.assertNotIn("controlContract", lifecycle)
            self.assertNotIn("controllableActions", lifecycle)
            self.assertNotIn("pid", lifecycle)
            # A non-opted-in row never appears.
            database2 = Path(temp) / "registry-noopt.sqlite3"
            self._registry(database2, contract=self._contract(), ownership="external-controlled")
            node2 = self._node(database2)
            status2 = await node2._agent_control({"action": "status"})
            self.assertEqual(status2["servers"], [])
            # Agent double opt-in still routes through the contract for action.
            agent_result = await node._agent_control(
                {
                    "action": "restart",
                    "target": "http-ctl",
                    "confirm": True,
                    "reason": "agent test",
                }
            )
            self.assertTrue(agent_result["applied"], agent_result)
            self.assertEqual(self.requests[0][1], "/control/restart")


def _relay_exchange(
    relay_base: str,
    method: str,
    path: str,
    headers: dict[str, str] | None = None,
    body: bytes | None = None,
    timeout: float = 20.0,
) -> tuple[int, dict[str, str], bytes]:
    """One blocking HTTP exchange against the relay (runs in a worker thread)."""
    import http.client
    import urllib.parse

    parts = urllib.parse.urlsplit(relay_base)
    connection = http.client.HTTPConnection(
        parts.hostname, parts.port or 80, timeout=timeout
    )
    try:
        connection.request(method, path, body=body, headers=headers or {})
        response = connection.getresponse()
        payload = response.read()
        received = {str(key).lower(): str(value) for key, value in response.getheaders()}
        return response.status, received, payload
    finally:
        connection.close()


class StreamableHttpRelayTest(unittest.TestCase):
    """Native loopback Streamable HTTP relay over the peer link.

    Consumer node (the Agent-facing host) serves ``/mcp/<registered-id>``;
    request envelopes travel over the peer link; the owner node reaches its
    private loopback endpoint and injects owner-registry static headers. The
    endpoint URL and registry headers never appear on the wire to the Agent.
    """

    @staticmethod
    def _http_registry_manifest(fixture_url: str, *, bearer: str) -> dict[str, Any]:
        return {
            "servers": [
                {
                    "id": "http-fix",
                    "name": "HTTP Fixture",
                    "summary": "typed HTTP MCP for native relay tests",
                    "transport": {
                        "type": "streamable-http",
                        "endpoint": fixture_url,
                        "headers": {"Authorization": f"Bearer {bearer}"},
                    },
                    "management": {
                        "ownership": "external",
                        "agentControl": {"enabled": False},
                    },
                    "capabilityGroups": ["test", "relay"],
                }
            ]
        }

    async def _run_relay_pair(
        self,
        fixture_url: str,
        *,
        bearer: str,
        callback: Any,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            win_manifest = root / "win-manifest.json"
            win_manifest.write_text(
                json.dumps(
                    self._http_registry_manifest(fixture_url, bearer=bearer)
                ),
                encoding="utf-8",
            )
            empty_manifest = root / "empty.json"
            empty_manifest.write_text('{"servers": []}', encoding="utf-8")
            win_database = root / "win.sqlite3"
            wsl_database = root / "wsl.sqlite3"
            Registry.initialize_database(win_database, win_manifest, replace=True)
            Registry.initialize_database(wsl_database, empty_manifest, replace=True)
            link_port = free_port()
            win_local_port = free_port()
            wsl_local_port = free_port()
            relay_port = free_port()
            win = BridgeNode(
                side="win",
                registry=Registry(win_database),
                local_host="127.0.0.1",
                local_port=win_local_port,
                link_mode="listen",
                link_host="127.0.0.1",
                link_port=link_port,
                artifact_spool_root=root / "win-spool",
            )
            wsl = BridgeNode(
                side="wsl",
                registry=Registry(wsl_database),
                local_host="127.0.0.1",
                local_port=wsl_local_port,
                link_mode="connect",
                link_host="127.0.0.1",
                link_port=link_port,
                artifact_spool_root=root / "wsl-spool",
                http_relay_port=relay_port,
            )
            win_task = asyncio.create_task(win.run())
            await asyncio.sleep(0.05)
            wsl_task = asyncio.create_task(wsl.run())
            try:
                await asyncio.wait_for(win.link_ready.wait(), timeout=10)
                await asyncio.wait_for(wsl.link_ready.wait(), timeout=10)
                await callback(f"http://127.0.0.1:{relay_port}", fixture_url)
            finally:
                win_task.cancel()
                wsl_task.cancel()
                await asyncio.gather(win_task, wsl_task, return_exceptions=True)

    def test_native_relay_json_preserves_session_and_injects_owner_headers(self) -> None:
        from tests.test_streamable_http_stdio import FixtureConfig, FixtureServer

        config = FixtureConfig()
        config.required_bearer = "relay-owner-secret"
        fixture = FixtureServer(config)
        fixture_thread = threading.Thread(target=fixture.serve_forever, daemon=True)
        fixture_thread.start()

        async def scenario(relay_url: str, direct_url: str) -> None:
            self.assertNotEqual(relay_url, direct_url)
            base_headers = {
                "Accept": "application/json, text/event-stream",
                "Content-Type": "application/json",
            }

            async def exchange(*args: Any, **kwargs: Any) -> Any:
                return await asyncio.to_thread(_relay_exchange, *args, **kwargs)

            # Sanity: the fixture demands the bearer, which only the owner
            # registry may know. A direct unauthenticated initialize fails.
            direct_status, _direct_headers, _ = await exchange(
                direct_url, "POST", "/mcp", base_headers, _relay_initialize()
            )
            self.assertEqual(direct_status, 401)
            # Through the relay the owner injects Authorization from its local
            # registry: success is the end-to-end proof of header injection.
            status, headers, payload = await exchange(
                relay_url,
                "POST",
                "/mcp/http-fix",
                base_headers,
                _relay_initialize(),
            )
            self.assertEqual(status, 200, payload.decode("utf-8", "replace"))
            self.assertIn("mcp-session-id", headers)
            session_id = headers["mcp-session-id"]
            initialize_result = json.loads(payload.decode("utf-8"))
            self.assertEqual(
                initialize_result["result"]["protocolVersion"], "2025-06-18"
            )
            self.assertTrue(initialize_result["result"]["capabilities"]["tools"])
            # The session id round-trips so a follow-up tool call is served.
            status, headers, payload = await exchange(
                relay_url,
                "POST",
                "/mcp/http-fix",
                {
                    **base_headers,
                    "Mcp-Session-Id": session_id,
                    "MCP-Protocol-Version": "2025-06-18",
                },
                _relay_rpc("tools/list", 2),
            )
            self.assertEqual(status, 200, payload.decode("utf-8", "replace"))
            tools = json.loads(payload.decode("utf-8"))["result"]["tools"]
            self.assertIn("echo", {tool["name"] for tool in tools})
            # DELETE terminates the upstream session through the relay.
            status, _headers, _ = await exchange(
                relay_url,
                "DELETE",
                "/mcp/http-fix",
                {"Mcp-Session-Id": session_id},
            )
            self.assertEqual(status, 202)
            self.assertEqual(fixture.delete_count, 1)
            # GET against the deleted session reaches the owner (404).
            status, _headers, _ = await exchange(
                relay_url,
                "GET",
                "/mcp/http-fix",
                {"Accept": "text/event-stream", "Mcp-Session-Id": session_id},
            )
            self.assertEqual(status, 404)

        try:
            asyncio.run(
                self._run_relay_pair(
                    fixture.url, bearer=config.required_bearer, callback=scenario
                )
            )
        finally:
            fixture.stop()

    def test_native_relay_streams_sse_style_responses(self) -> None:
        from tests.test_streamable_http_stdio import FixtureConfig, FixtureServer

        config = FixtureConfig()
        config.response_mode = "sse"  # every POST answer streams as text/event-stream
        fixture = FixtureServer(config)
        fixture_thread = threading.Thread(target=fixture.serve_forever, daemon=True)
        fixture_thread.start()

        async def scenario(relay_url: str, direct_url: str) -> None:
            self.assertNotEqual(direct_url, relay_url)
            base_headers = {
                "Accept": "application/json, text/event-stream",
                "Content-Type": "application/json",
            }

            async def exchange(*args: Any, **kwargs: Any) -> Any:
                return await asyncio.to_thread(_relay_exchange, *args, **kwargs)

            status, headers, payload = await exchange(
                relay_url,
                "POST",
                "/mcp/http-fix",
                base_headers,
                _relay_initialize(),
            )
            self.assertEqual(status, 200, payload.decode("utf-8", "replace"))
            session_id = headers.get("mcp-session-id")
            self.assertTrue(session_id)
            negotiated = json.loads(payload.decode("utf-8"))["result"][
                "protocolVersion"
            ]
            # In SSE mode the follow-up tool call answers as a chunked
            # text/event-stream that must stream through the relay unchanged.
            status, _headers, payload = await exchange(
                relay_url,
                "POST",
                "/mcp/http-fix",
                {
                    **base_headers,
                    "Mcp-Session-Id": session_id,
                    "MCP-Protocol-Version": negotiated,
                },
                _relay_rpc("tools/list", 2),
            )
            self.assertEqual(status, 200, payload.decode("utf-8", "replace"))
            body_text = payload.decode("utf-8", "replace")
            self.assertIn("event: message", body_text)
            self.assertIn("echo", body_text)
            self.assertIn('"tools"', body_text)

        try:
            asyncio.run(
                self._run_relay_pair(
                    fixture.url, bearer="unused-bearer", callback=scenario
                )
            )
        finally:
            fixture.stop()

    def test_native_relay_unknown_path_and_owner_side_gates(self) -> None:
        from tests.test_streamable_http_stdio import FixtureConfig, FixtureServer

        config = FixtureConfig()
        fixture = FixtureServer(config)
        fixture_thread = threading.Thread(target=fixture.serve_forever, daemon=True)
        fixture_thread.start()

        async def scenario(relay_url: str, direct_url: str) -> None:
            self.assertTrue(relay_url)
            self.assertTrue(direct_url)

            async def exchange(*args: Any, **kwargs: Any) -> Any:
                return await asyncio.to_thread(_relay_exchange, *args, **kwargs)

            # Unregistered ids and malformed mounts are refused at the listener.
            status, _headers, payload = await exchange(
                relay_url,
                "POST",
                "/mcp/does-not-exist",
                {"Content-Type": "application/json"},
                _relay_initialize(),
            )
            self.assertEqual(status, 404)
            status, _headers, _ = await exchange(
                relay_url,
                "PUT",
                "/mcp/http-fix",
                {},
                b"",
            )
            self.assertEqual(status, 405)
            status, _headers, payload = await exchange(
                relay_url,
                "GET",
                "/not-mcp/http-fix",
                {},
            )
            self.assertEqual(status, 404)
            self.assertIn(b"/mcp/<registered-id>", payload)

        try:
            asyncio.run(
                self._run_relay_pair(
                    fixture.url, bearer="unused-bearer", callback=scenario
                )
            )
        finally:
            fixture.stop()

    def test_owner_refuses_non_streamable_and_non_loopback_targets(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            empty_manifest = root / "empty.json"
            empty_manifest.write_text('{"servers": []}', encoding="utf-8")
            database = root / "registry.sqlite3"
            Registry.initialize_database(database, empty_manifest, replace=True)
            sent: list[dict[str, Any]] = []

            class RegistryStub:
                def __init__(self, row: dict[str, Any]):
                    self.row = row

                def launch(self, target: str) -> dict[str, Any]:
                    return self.row

            async def exercise() -> None:
                node = BridgeNode(
                    side="wsl",
                    registry=Registry(database),
                    local_host="127.0.0.1",
                    local_port=free_port(),
                    link_mode="connect",
                    link_host="127.0.0.1",
                    link_port=free_port(),
                    artifact_spool_root=root / "spool",
                )

                async def capture(frame: dict[str, Any]) -> None:
                    sent.append(frame)

                node._send_frame = capture  # type: ignore[method-assign]
                # A stdio registration is not reachable through the native relay.
                node.registry = RegistryStub(
                    {"transport": {"type": "stdio"}}  # type: ignore[assignment]
                )
                await node._relay_execute(
                    "r-stdio", "stdio-srv", "POST", "", "", [], b"{}"
                )
                self.assertEqual(sent[-1]["type"], "relay_error")
                self.assertEqual(sent[-1]["status"], 400)
                # A registered non-loopback endpoint is refused at the owner.
                node.registry = RegistryStub(
                    {
                        "transport": {
                            "type": "streamable-http",
                            "endpoint": "http://192.0.2.55/mcp",
                            "headers": {},
                        }
                    }
                )  # type: ignore[assignment]
                await node._relay_execute(
                    "r-remote",
                    "remote-http",
                    "GET",
                    "",
                    "",
                    [],
                    b"",
                )
                self.assertEqual(sent[-1]["type"], "relay_error")
                self.assertEqual(sent[-1]["status"], 502)
                self.assertIn("loopback", str(sent[-1]["message"]))
                # A non-HTTP scheme is refused too.
                node.registry = RegistryStub(
                    {
                        "transport": {
                            "type": "streamable-http",
                            "endpoint": "ftp://127.0.0.1/mcp",
                            "headers": {},
                        }
                    }
                )  # type: ignore[assignment]
                await node._relay_execute(
                    "r-ftp",
                    "ftp-http",
                    "GET",
                    "",
                    "",
                    [],
                    b"",
                )
                self.assertEqual(sent[-1]["type"], "relay_error")
                self.assertEqual(sent[-1]["status"], 502)

            asyncio.run(exercise())


class HttpManagedSupervisionRegistryTest(unittest.TestCase):
    """Registered bridge-managed streamable-http supervision policy schema.

    A ``bridge-managed`` streamable-http row must carry a local launch command
    plus a bounded supervision policy (required GET readiness gate, bounded
    startup window, optional bounded graceful-shutdown interface).  The policy
    and the launch definition are private to the owner registry and never
    appear in public or peer-visible views.
    """

    @staticmethod
    def _fixture_args(port: int) -> list[str]:
        return [
            str(ROOT / "tests" / "fixtures" / "managed_http_fixture.py"),
            "--port",
            str(port),
        ]

    def _supervision(self, **overrides: Any) -> dict[str, Any]:
        policy: dict[str, Any] = {
            "startupTimeoutSeconds": 20,
            "ready": {
                "path": "/ready",
                "timeoutSeconds": 5,
                "pollIntervalSeconds": 0.5,
                "successStatuses": [200],
            },
            "shutdown": {
                "method": "POST",
                "path": "/shutdown",
                "timeoutSeconds": 5,
                "successStatuses": [200, 202, 204],
            },
        }
        policy.update(overrides)
        return policy

    def _manifest(
        self,
        port: int,
        *,
        ownership: str = "bridge-managed",
        supervision: dict[str, Any] | None = None,
        command: str | None = None,
        extra: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        row: dict[str, Any] = {
            "id": "http-man",
            "name": "Managed HTTP fixture",
            "summary": "supervised",
            "command": command if command is not None else sys.executable,
            "args": self._fixture_args(port),
            "cwd": str(ROOT),
            "transport": {
                "type": "streamable-http",
                "endpoint": f"http://127.0.0.1:{port}",
                "headers": {"Authorization": "Bearer owner-secret"},
            },
            "management": {"ownership": ownership},
            "capabilityGroups": ["test"],
        }
        if supervision is not None:
            row["management"]["supervision"] = supervision
        if extra:
            row.update(extra)
        return {"servers": [row]}

    def _init(self, temp: str, manifest: dict[str, Any]) -> Registry:
        database = Path(temp) / "registry.sqlite3"
        manifest_path = Path(temp) / "manifest.json"
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        Registry.initialize_database(database, manifest_path, replace=True)
        return Registry(database)

    def test_valid_row_persists_private_and_public_stays_clean(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            port = free_port()
            registry = self._init(
                temp, self._manifest(port, supervision=self._supervision())
            )
            entry = registry.launch("http-man")
            management = entry["management"]
            self.assertEqual(management["ownership"], "bridge-managed")
            self.assertEqual(entry["command"], sys.executable)
            self.assertIn("managed_http_fixture.py", " ".join(entry["args"]))
            supervision = management["supervision"]
            self.assertEqual(supervision["startupTimeoutSeconds"], 20.0)
            self.assertEqual(supervision["ready"]["path"], "/ready")
            self.assertEqual(supervision["ready"]["successStatuses"], [200])
            self.assertEqual(supervision["ready"]["pollIntervalSeconds"], 0.5)
            self.assertEqual(supervision["shutdown"]["method"], "POST")
            self.assertEqual(supervision["shutdown"]["path"], "/shutdown")
            serialized_public = json.dumps(registry.public("http-man"))
            self.assertNotIn("supervision", serialized_public)
            self.assertNotIn("/ready", serialized_public)
            self.assertNotIn("/shutdown", serialized_public)
            self.assertNotIn("owner-secret", serialized_public)
            self.assertNotIn("managed_http_fixture", serialized_public)
            self.assertNotIn("sys.executable", serialized_public)
            # Lifecycle observation may report the state machine but never the
            # launch definition or supervision policy.
            self.assertIsInstance(registry.public("http-man")["management"], dict)

    def test_supervision_defaults_are_bounded(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            port = free_port()
            registry = self._init(
                temp, self._manifest(port, supervision={"ready": {"path": "/ok"}})
            )
            supervision = registry.launch("http-man")["management"]["supervision"]
            self.assertEqual(supervision["startupTimeoutSeconds"], 60.0)
            self.assertEqual(supervision["ready"]["timeoutSeconds"], 5.0)
            self.assertEqual(supervision["ready"]["pollIntervalSeconds"], 1.0)
            self.assertEqual(supervision["ready"]["successStatuses"], [200])

    def _rejected(self, manifest: dict[str, Any]) -> None:
        with tempfile.TemporaryDirectory() as temp:
            with self.assertRaises(BridgeError):
                self._init(temp, manifest)

    def test_bridge_managed_http_requires_command(self) -> None:
        self._rejected(
            self._manifest(
                free_port(), command="", supervision=self._supervision()
            )
        )

    def test_bridge_managed_http_requires_supervision(self) -> None:
        self._rejected(self._manifest(free_port(), supervision=None))

    def test_supervision_requires_bridge_managed_streamable_http(self) -> None:
        # supervision on an external streamable-http row.
        self._rejected(
            self._manifest(
                free_port(),
                ownership="external",
                supervision=self._supervision(),
            )
        )
        # supervision on an external-controlled row with a control contract.
        self._rejected(
            self._manifest(
                free_port(),
                ownership="external-controlled",
                supervision=self._supervision(),
                extra={
                    "management": {
                        "ownership": "external-controlled",
                        "supervision": self._supervision(),
                        "controlContract": {
                            "restart": {
                                "method": "POST",
                                "path": "/control/restart",
                                "timeoutSeconds": 5,
                                "successStatuses": [200],
                            }
                        },
                    }
                },
            )
        )

    def test_bridge_managed_http_rejects_multiprocess_false(self) -> None:
        self._rejected(
            self._manifest(
                free_port(),
                supervision=self._supervision(),
                extra={"process": {"multiProcessAllowed": False}},
            )
        )

    def test_supervision_policy_is_bounded(self) -> None:
        # Unknown top-level key.
        self._rejected(
            self._manifest(
                free_port(),
                supervision={**self._supervision(), "command": "reboot"},
            )
        )
        # Readiness gate must use a relative path (no absolute url).
        self._rejected(
            self._manifest(
                free_port(),
                supervision={
                    "ready": {
                        "url": "http://127.0.0.1:1/ready",
                        "successStatuses": [200],
                    }
                },
            )
        )
        # Ready without a location.
        self._rejected(
            self._manifest(free_port(), supervision={"ready": {"timeoutSeconds": 5}})
        )
        # Startup window below the bounded minimum.
        self._rejected(
            self._manifest(
                free_port(),
                supervision={
                    "startupTimeoutSeconds": 1,
                    "ready": {"path": "/ready"},
                },
            )
        )
        # Shutdown interface must resolve only to loopback.
        self._rejected(
            self._manifest(
                free_port(),
                supervision={
                    "ready": {"path": "/ready"},
                    "shutdown": {
                        "method": "POST",
                        "url": "http://192.0.2.1/shutdown",
                        "successStatuses": [200],
                    },
                },
            )
        )


class HttpManagedSupervisionExecutionTest(unittest.IsolatedAsyncioTestCase):
    """Owner-node supervision of one bridge-managed streamable-http generation.

    The owner launches the registered process from the private launch
    definition, proves the bounded readiness gate before reporting ready,
    stops the exact owned generation (optionally via the graceful shutdown
    interface), and restarts with strictly newer generations and no overlap.
    """

    def _write_registry(
        self, root: Path, port: int, *, die_fast: bool = False, never_ready: bool = False
    ) -> Registry:
        database = root / "registry.sqlite3"
        manifest = root / "manifest.json"
        args = [
            str(ROOT / "tests" / "fixtures" / "managed_http_fixture.py"),
            "--port",
            str(port),
        ]
        if die_fast:
            args.append("--die-fast")
        if never_ready:
            args.append("--never-ready")
        row = {
            "id": "http-man",
            "name": "Managed HTTP fixture",
            "summary": "supervised",
            "command": sys.executable,
            "args": args,
            "cwd": str(ROOT),
            "transport": {
                "type": "streamable-http",
                "endpoint": f"http://127.0.0.1:{port}",
                "headers": {},
            },
            "management": {
                "ownership": "bridge-managed",
                "supervision": {
                    "startupTimeoutSeconds": 5,
                    "ready": {
                        "path": "/ready",
                        "timeoutSeconds": 5,
                        "pollIntervalSeconds": 0.2,
                        "successStatuses": [200],
                    },
                    "shutdown": {
                        "method": "POST",
                        "path": "/shutdown",
                        "timeoutSeconds": 5,
                        "successStatuses": [200, 202, 204],
                    },
                },
            },
            "capabilityGroups": ["test"],
        }
        manifest.write_text(json.dumps({"servers": [row]}), encoding="utf-8")
        Registry.initialize_database(database, manifest, replace=True)
        return Registry(database)

    def _node(self, database: Registry, root: Path) -> BridgeNode:
        node = BridgeNode(
            side="win",
            registry=database,
            local_host="127.0.0.1",
            local_port=free_port(),
            link_mode="listen",
            link_host="127.0.0.1",
            link_port=free_port(),
        )
        node.journal = EventJournal(root / "events.sqlite3", max_events=100)
        return node

    def _lifecycle(self, target: str) -> dict[str, Any]:
        return self.node._lifecycle_server_status(target)["lifecycle"]

    async def _stop_owned_generations(self) -> None:
        backend = self.node.http_backends.get("http-man")
        if backend is not None:
            await backend.stop("test teardown")

    async def test_restart_starts_passes_readiness_and_never_overlaps(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            port = free_port()
            self.node = self._node(self._write_registry(root, port), root)
            try:
                # Preview first: never touches the endpoint or spawns anything.
                preview = await self.node._lifecycle_control(
                    {"action": "restart", "target": "http-man"}
                )
                self.assertFalse(preview["applied"])
                self.assertTrue(preview["confirmRequired"])
                self.assertEqual(preview["observed"]["mode"], "http")
                self.assertEqual(preview["observed"]["state"], "exited")
                self.assertFalse(preview["observed"]["relayAvailable"])
                self.assertEqual(self.node.http_backends, {})

                applied = await self.node._lifecycle_control(
                    {"action": "restart", "target": "http-man", "confirm": True}
                )
                self.assertTrue(applied["applied"], applied)
                inner = applied["result"]
                self.assertEqual(inner["startedGeneration"], 1)
                self.assertEqual(inner["stoppedGeneration"], None)
                self.assertTrue(inner["ready"], inner)
                self.assertTrue(inner["relayAvailable"], inner)
                start_phases = {p["phase"]: p["outcome"] for p in inner["startPhases"]}
                self.assertEqual(start_phases.get("launch"), "ok")
                self.assertEqual(start_phases.get("readiness"), "ok")

                lifecycle = self._lifecycle("http-man")
                self.assertEqual(lifecycle["processState"], "ready")
                self.assertTrue(lifecycle["registered"])
                self.assertTrue(lifecycle["ready"])
                self.assertTrue(lifecycle["relayAvailable"])
                self.assertEqual(lifecycle["initializeHealth"], "not-probed")
                self.assertEqual(lifecycle["ownership"], "bridge-managed")
                self.assertEqual(lifecycle["ownedGeneration"], 1)
                self.assertFalse(lifecycle["drain"])
                backend = self.node.http_backends["http-man"]
                first_pid = backend.process.pid
                self.assertIsNotNone(first_pid)

                # Confirmed restart: old generation fully exits (graceful
                # shutdown interface accepted, exit proven) then a strictly
                # newer generation starts and passes readiness again.
                restarted = await self.node._lifecycle_control(
                    {
                        "action": "restart",
                        "target": "http-man",
                        "confirm": True,
                        "reason": "rotate generation",
                    }
                )
                self.assertTrue(restarted["applied"], restarted)
                rinner = restarted["result"]
                self.assertEqual(rinner["startedGeneration"], 2)
                self.assertEqual(rinner["stoppedGeneration"], 1)
                self.assertTrue(rinner["ready"], rinner)
                stop_phases = {p["phase"]: p["outcome"] for p in rinner["stopPhases"]}
                self.assertEqual(stop_phases.get("shutdown-request"), "accepted")
                self.assertEqual(stop_phases.get("wait"), "exited")
                second_pid = backend.process.pid
                self.assertNotEqual(first_pid, second_pid)
                lifecycle = self._lifecycle("http-man")
                self.assertEqual(lifecycle["processState"], "ready")
                self.assertEqual(lifecycle["ownedGeneration"], 2)

                # Journal correlates the operation id across the lifecycle row
                # and its per-phase rows, like the stdio lifecycle paths.
                operation_id = restarted["operationId"]
                events = self.node.journal.recent(200)
                lifecycle_rows = [
                    row
                    for row in events
                    if row["category"] == "lifecycle"
                    and row["operation_id"] == operation_id
                ]
                self.assertEqual(len(lifecycle_rows), 1)
                self.assertEqual(lifecycle_rows[0]["outcome"], "applied")
                self.assertEqual(lifecycle_rows[0]["target"], "http-man")
                phase_rows = [
                    row
                    for row in events
                    if row["category"] == "lifecycle-phase"
                    and row["operation_id"] == operation_id
                ]
                phases = {row["metadata"].get("phase"): row["outcome"] for row in phase_rows}
                self.assertEqual(phases.get("drain"), "entered")
                self.assertEqual(phases.get("shutdown-request"), "accepted")
                self.assertEqual(phases.get("readiness"), "ok")
            finally:
                await self._stop_owned_generations()

    async def test_stop_proves_exit_and_status_tracks_process_state(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            port = free_port()
            self.node = self._node(self._write_registry(root, port), root)
            try:
                await self.node._lifecycle_control(
                    {"action": "restart", "target": "http-man", "confirm": True}
                )
                self.assertEqual(self._lifecycle("http-man")["processState"], "ready")
                preview = await self.node._lifecycle_control(
                    {"action": "stop", "target": "http-man"}
                )
                self.assertFalse(preview["applied"])
                self.assertEqual(preview["observed"]["state"], "ready")
                stopped = await self.node._lifecycle_control(
                    {"action": "stop", "target": "http-man", "confirm": True}
                )
                self.assertTrue(stopped["applied"], stopped)
                self.assertEqual(stopped["result"]["stoppedGeneration"], 1)
                self.assertTrue(stopped["result"]["reconnectRequired"])
                lifecycle = self._lifecycle("http-man")
                self.assertEqual(lifecycle["processState"], "exited")
                self.assertEqual(lifecycle["ownedGeneration"], 1)
                self.assertFalse(lifecycle["ready"])
                self.assertFalse(lifecycle["relayAvailable"])
                backend = self.node.http_backends["http-man"]
                self.assertIsNone(backend.process)
                # A later relay demand starts a fresh generation with readiness.
                await self.node._http_backend_demand_ready(
                    "http-man", self.node.registry.launch("http-man")
                )
                self.assertEqual(self._lifecycle("http-man")["ownedGeneration"], 2)
                self.assertEqual(self._lifecycle("http-man")["processState"], "ready")
            finally:
                await self._stop_owned_generations()

    async def test_drain_refuses_new_demand_until_restart_clears_it(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            port = free_port()
            self.node = self._node(self._write_registry(root, port), root)
            try:
                await self.node._lifecycle_control(
                    {"action": "restart", "target": "http-man", "confirm": True}
                )
                preview = await self.node._lifecycle_control(
                    {"action": "drain", "target": "http-man"}
                )
                self.assertFalse(preview["applied"])
                drained = await self.node._lifecycle_control(
                    {"action": "drain", "target": "http-man", "confirm": True}
                )
                self.assertTrue(drained["applied"], drained)
                self.assertEqual(drained["result"]["stoppedGeneration"], 1)
                lifecycle = self._lifecycle("http-man")
                self.assertTrue(lifecycle["drain"])
                self.assertEqual(lifecycle["processState"], "exited")
                self.assertFalse(lifecycle["ready"])
                with self.assertRaises(BridgeError) as error:
                    await self.node._http_backend_demand_ready(
                        "http-man", self.node.registry.launch("http-man")
                    )
                self.assertIn("draining", str(error.exception))
                # Restart clears the armed drain and passes readiness again.
                cleared = await self.node._lifecycle_control(
                    {"action": "restart", "target": "http-man", "confirm": True}
                )
                self.assertTrue(cleared["applied"], cleared)
                lifecycle = self._lifecycle("http-man")
                self.assertFalse(lifecycle["drain"])
                self.assertEqual(lifecycle["processState"], "ready")
                self.assertEqual(lifecycle["ownedGeneration"], 2)
            finally:
                await self._stop_owned_generations()

    async def test_generation_guard_rejects_stale_expected_generation(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            port = free_port()
            self.node = self._node(self._write_registry(root, port), root)
            try:
                await self.node._lifecycle_control(
                    {"action": "restart", "target": "http-man", "confirm": True}
                )
                with self.assertRaises(BridgeError) as error:
                    await self.node._lifecycle_control(
                        {
                            "action": "stop",
                            "target": "http-man",
                            "generation": 7,
                            "confirm": True,
                        }
                    )
                self.assertIn("generation", str(error.exception))
            finally:
                await self._stop_owned_generations()

    async def test_process_exit_during_startup_is_never_reported_ready(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            port = free_port()
            self.node = self._node(self._write_registry(root, port, die_fast=True), root)
            try:
                applied = await self.node._lifecycle_control(
                    {"action": "restart", "target": "http-man", "confirm": True}
                )
                self.assertFalse(applied["applied"], applied)
                self.assertEqual(applied["result"]["state"], "failed")
                self.assertFalse(applied["result"]["ready"])
                self.assertFalse(applied["result"]["relayAvailable"])
                lifecycle = self._lifecycle("http-man")
                self.assertEqual(lifecycle["processState"], "failed")
                self.assertFalse(lifecycle["ready"])
                self.assertFalse(lifecycle["relayAvailable"])
                self.assertIn("exited", lifecycle["startError"])
            finally:
                await self._stop_owned_generations()

    async def test_live_process_that_fails_readiness_is_not_ready(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            port = free_port()
            self.node = self._node(self._write_registry(root, port, never_ready=True), root)
            try:
                started = time.monotonic()
                applied = await self.node._lifecycle_control(
                    {"action": "restart", "target": "http-man", "confirm": True}
                )
                elapsed = time.monotonic() - started
                self.assertFalse(applied["applied"], applied)
                self.assertEqual(applied["result"]["state"], "failed")
                self.assertFalse(applied["result"]["ready"])
                # The bounded startup window applied: it did not fail instantly
                # on Popen success and did not run forever.
                self.assertGreaterEqual(elapsed, 4.5)
                lifecycle = self._lifecycle("http-man")
                self.assertEqual(lifecycle["processState"], "failed")
                self.assertFalse(lifecycle["ready"])
                self.assertIn("readiness gate", lifecycle["startError"])
            finally:
                await self._stop_owned_generations()


def _relay_initialize() -> bytes:
    return json.dumps(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": {"name": "relay-test", "version": "0"},
            },
        }
    ).encode("utf-8")


def _relay_rpc(method: str, request_id: int) -> bytes:
    return json.dumps(
        {"jsonrpc": "2.0", "id": request_id, "method": method, "params": {}}
    ).encode("utf-8")


class FakeNodeServer:
    """Minimal stand-in for the local bridge node + one downstream MCP.

    Accepts the connector's ``connect`` handshake (replying ``coreVersion``) and
    then behaves like a fixture MCP over the same socket: initialize, ping,
    tools/list, and an echo tools/call.  Session loss is scriptable with
    ``exit_after_call`` (answer then close) and ``exit_before_response`` (close
    without answering a tools/call).
    """

    def __init__(
        self,
        *,
        core_version: str = "0.4.0",
        instructions: str = "Fake downstream instructions.",
        exit_after_call: bool = False,
        exit_before_response: bool = False,
    ) -> None:
        self.core_version = core_version
        self.instructions = instructions
        self.exit_after_call = exit_after_call
        self.exit_before_response = exit_before_response
        self.handshakes: list[dict] = []
        self.sessions: list[list[dict]] = []
        self._lock = threading.Lock()
        self._listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._listener.bind(("127.0.0.1", 0))
        self._listener.listen(8)
        self._listener.settimeout(0.5)
        self._stopped = False
        self._thread = threading.Thread(target=self._accept_loop, daemon=True)
        self._thread.start()

    @property
    def port(self) -> int:
        return int(self._listener.getsockname()[1])

    def snapshot(self) -> tuple[list[dict], list[list[dict]]]:
        with self._lock:
            return list(self.handshakes), [list(session) for session in self.sessions]

    def _accept_loop(self) -> None:
        while not self._stopped:
            try:
                conn, _address = self._listener.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            threading.Thread(target=self._serve, args=(conn,), daemon=True).start()

    @staticmethod
    def _recv_line(conn: socket.socket) -> bytes | None:
        data = bytearray()
        while len(data) <= 8 * 1024 * 1024:
            try:
                chunk = conn.recv(1)
            except (OSError, socket.timeout):
                return None
            if not chunk:
                return None
            data.extend(chunk)
            if chunk == b"\n":
                return bytes(data)
        return None

    def _send(self, conn: socket.socket, value: object) -> None:
        conn.sendall(
            json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
            + b"\n"
        )

    def _serve(self, conn: socket.socket) -> None:
        session: list[dict] = []
        try:
            conn.settimeout(15)
            raw = self._recv_line(conn)
            if raw is None:
                return
            try:
                op = json.loads(raw)
            except ValueError:
                op = {}
            with self._lock:
                self.handshakes.append(op)
            self._send(
                conn,
                {
                    "ok": True,
                    "stream": f"fake-{len(self.handshakes)}",
                    "coreVersion": self.core_version,
                },
            )
            while True:
                raw = self._recv_line(conn)
                if raw is None:
                    break
                try:
                    message = json.loads(raw)
                except ValueError:
                    continue
                session.append(message)
                method = message.get("method")
                request_id = message.get("id")
                if method == "initialize":
                    self._send(
                        conn,
                        {
                            "jsonrpc": "2.0",
                            "id": request_id,
                            "result": {
                                "protocolVersion": "2025-06-18",
                                "capabilities": {"tools": {"listChanged": False}},
                                "serverInfo": {"name": "fake-mcp", "version": "1.0.0"},
                                "instructions": self.instructions,
                            },
                        },
                    )
                    continue
                if method == "notifications/initialized":
                    continue
                if method == "tools/list":
                    self._send(
                        conn,
                        {
                            "jsonrpc": "2.0",
                            "id": request_id,
                            "result": {
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
                                        "name": "shout",
                                        "description": "Return the supplied value uppercased.",
                                        "inputSchema": {
                                            "type": "object",
                                            "properties": {"value": {}},
                                            "required": ["value"],
                                            "additionalProperties": False,
                                        },
                                    },
                                ]
                            },
                        },
                    )
                    continue
                if method == "ping":
                    self._send(conn, {"jsonrpc": "2.0", "id": request_id, "result": {}})
                    continue
                if method == "tools/call":
                    if self.exit_before_response:
                        break  # close without answering: pending call is lost
                    params = message.get("params") or {}
                    arguments = params.get("arguments") or {}
                    if params.get("name") == "echo":
                        value = arguments.get("value")
                        self._send(
                            conn,
                            {
                                "jsonrpc": "2.0",
                                "id": request_id,
                                "result": {
                                    "content": [
                                        {
                                            "type": "text",
                                            "text": json.dumps(value, ensure_ascii=False),
                                        }
                                    ],
                                    "structuredContent": {
                                        "value": value,
                                        "servedBy": "fake-mcp",
                                    },
                                },
                            },
                        )
                        if self.exit_after_call:
                            break
                        continue
                    if params.get("name") == "shout":
                        value = arguments.get("value")
                        self._send(
                            conn,
                            {
                                "jsonrpc": "2.0",
                                "id": request_id,
                                "result": {
                                    "content": [
                                        {
                                            "type": "text",
                                            "text": json.dumps(
                                                str(value).upper(), ensure_ascii=False
                                            ),
                                        }
                                    ],
                                    "structuredContent": {
                                        "value": str(value).upper(),
                                        "servedBy": "fake-mcp",
                                    },
                                },
                            },
                        )
                        if self.exit_after_call:
                            break
                        continue
                    self._send(
                        conn,
                        {
                            "jsonrpc": "2.0",
                            "id": request_id,
                            "error": {"code": -32602, "message": "unknown tool"},
                        },
                    )
                    continue
                self._send(
                    conn,
                    {
                        "jsonrpc": "2.0",
                        "id": request_id,
                        "error": {"code": -32601, "message": f"method not found: {method}"},
                    },
                )
        except (OSError, socket.timeout, ValueError):
            pass
        finally:
            with self._lock:
                self.sessions.append(session)
            try:
                conn.close()
            except OSError:
                pass

    def stop(self) -> None:
        self._stopped = True
        try:
            self._listener.close()
        except OSError:
            pass


def _write_test_engine(
    path: Path,
    *,
    name: str = "test-engine",
    version: str = "9.9.9",
    core: str = "9.9.9",
    note: bool = False,
) -> None:
    """Write a reloadable engine override that reuses the builtin recovery.

    The override only changes policy constants; it subclasses the builtin
    ``Recovery`` so the JSON-RPC-aware logic is inherited unchanged.
    """
    path.write_text(
        "\n".join(
            [
                "import connector_engine as _base",
                f"ENGINE_NAME = {name!r}",
                f"ENGINE_VERSION = {version!r}",
                "class Recovery(_base.Recovery):",
                f"    CORE_VERSION = {core!r}",
                f"    NOTE_RESULTS_WHEN_STALE = {note!r}",
                "def make_recovery(core_version=None):",
                "    return Recovery(core_version)",
            ]
        )
        + "\n",
        encoding="utf-8",
    )


def _init_request(request_id: int) -> bytes:
    return (
        json.dumps(
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "method": "initialize",
                "params": {"protocolVersion": "2025-06-18", "capabilities": {}},
            },
            separators=(",", ":"),
        )
        + "\n"
    ).encode("utf-8")


def _init_result(request_id: int, instructions: str = "instructions") -> bytes:
    return (
        json.dumps(
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "protocolVersion": "2025-06-18",
                    "capabilities": {"tools": {"listChanged": False}},
                    "serverInfo": {"name": "unit-mcp", "version": "1.0.0"},
                    "instructions": instructions,
                },
            },
            separators=(",", ":"),
        )
        + "\n"
    ).encode("utf-8")


def _call_result(request_id: int) -> bytes:
    return (
        json.dumps(
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "content": [{"type": "text", "text": '"ok"'}],
                    "structuredContent": {"value": "ok"},
                },
            },
            separators=(",", ":"),
        )
        + "\n"
    ).encode("utf-8")


class ConnectorEngineUnitTest(unittest.TestCase):
    """The JSON-RPC-aware recovery lives in the dynamic engine (pure logic)."""

    def test_staleness_is_simple_direct_version_equality(self) -> None:
        self.assertFalse(make_recovery("0.4.0").stale())  # exact match
        self.assertTrue(make_recovery("0.3.9").stale())   # any difference is stale
        self.assertTrue(make_recovery("0.4.1").stale())   # newer also differs
        self.assertFalse(make_recovery(None).stale())
        self.assertFalse(make_recovery("").stale())
        recovery = make_recovery("0.3.9")
        self.assertIn("expects node core 0.4.0", recovery.warning_line())
        self.assertIn("node reports 0.3.9", recovery.warning_line())
        self.assertIn("core mismatch", recovery.failed_message("bridge: x"))
        self.assertEqual(make_recovery("0.4.0").failed_message("bridge: x"), "bridge: x")

    def test_agent_lines_are_classified_and_handshake_cached(self) -> None:
        recovery = make_recovery("0.4.0")
        initialize = _init_request(1)
        initialized = (
            b'{"jsonrpc":"2.0","method":"notifications/initialized"}\n'
        )
        call = (
            b'{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{}}\n'
        )
        result = b'{"jsonrpc":"2.0","id":3,"result":{}}\n'
        role = recovery.handle_agent(initialize)
        self.assertEqual(role.kind, "initialize")
        self.assertEqual(role.id, 1)
        self.assertEqual(recovery.handle_agent(initialized).kind, "initialized")
        request_role = recovery.handle_agent(call)
        self.assertEqual(request_role.kind, "request")
        self.assertEqual((request_role.id, request_role.method), (2, "tools/call"))
        self.assertEqual(recovery.handle_agent(result).kind, "response")
        self.assertTrue(recovery.replay_available())
        self.assertEqual(recovery.cached_initialize(), initialize)
        self.assertEqual(recovery.cached_initialized(), initialized)

    def test_replay_absorb_swallows_only_the_replayed_initialize_result(self) -> None:
        recovery = make_recovery("0.4.0")
        recovery.handle_agent(_init_request(1))
        recovered_init = recovery.cached_initialize()
        assert recovered_init is not None
        # Replayed initialize result is absorbed (never forwarded twice).
        decision = recovery.handle_node(_init_result(1), absorbing=True)
        self.assertEqual(decision.kind, "absorb_done")
        self.assertIsNone(decision.payload)
        # Ordinary lines during absorb are buffered by the core, not dropped.
        self.assertEqual(
            recovery.handle_node(_call_result(99), absorbing=True).kind, "absorb"
        )

    def test_init_annotation_and_optional_tool_note_only_when_stale(self) -> None:
        clean = make_recovery("0.4.0")
        clean.handle_agent(_init_request(1))
        forwarded = clean.handle_node(_init_result(1, "DOWNSTREAM"), absorbing=False)
        self.assertNotIn(b"expects node core", forwarded.payload)
        stale = make_recovery("0.3.9")
        stale.handle_agent(_init_request(1))
        forwarded = stale.handle_node(_init_result(1, "DOWNSTREAM"), absorbing=False)
        self.assertIn(b"DOWNSTREAM", forwarded.payload)
        self.assertIn(b"expects node core 0.4.0", forwarded.payload)
        # Default policy: no tool-result note even when stale.
        stale.remember_sent(2, "tools/call", current=True)
        result = stale.handle_node(_call_result(2), absorbing=False)
        content = json.loads(result.payload)["result"]["content"]
        self.assertEqual(len(content), 1)

    def test_tool_note_is_gated_by_engine_policy(self) -> None:
        class NotingRecovery(Recovery):
            CORE_VERSION = "9.9.9"
            NOTE_RESULTS_WHEN_STALE = True

        recovery = NotingRecovery("9.9.8")
        self.assertTrue(recovery.stale())
        recovery.remember_sent(2, "tools/call", current=True)
        result = recovery.handle_node(_call_result(2), absorbing=False)
        content = json.loads(result.payload)["result"]["content"]
        self.assertEqual(len(content), 2)
        self.assertIn("expects node core 9.9.9", content[-1]["text"])
        # Exactly one note: the next tool result is left untouched.
        recovery.remember_sent(3, "tools/call", current=True)
        again = recovery.handle_node(_call_result(3), absorbing=False)
        self.assertEqual(len(json.loads(again.payload)["result"]["content"]), 1)

    def test_pending_calls_fail_exactly_once_on_stream_loss(self) -> None:
        recovery = make_recovery("0.3.9")  # stale, so errors carry the suffix
        recovery.remember_sent(1, "tools/call", current=True)
        recovery.remember_sent(2, "tools/call", current=False)  # uncertain send
        lost = recovery.lost_requests()
        self.assertEqual({item[0] for item in lost}, {1, 2})
        payloads = recovery.fail_once(lost, recovery.LOST_REASON)
        self.assertEqual(len(payloads), 2)
        errors = [json.loads(payload) for payload in payloads]
        for error in errors:
            self.assertEqual(error["error"]["code"], -32000)
            self.assertTrue(error["error"]["data"]["bridge"])
            self.assertTrue(error["error"]["message"].startswith("bridge:"))
            self.assertIn("core mismatch", error["error"]["message"])
        # Exactly once: repeating the same ids yields nothing.
        self.assertEqual(recovery.fail_once(lost, recovery.LOST_REASON), [])
        # Responses for remembered ids consume bookkeeping and never error.
        clean = make_recovery("0.4.0")
        clean.remember_sent(1, "tools/call", current=True)
        payload = clean.handle_node(_call_result(1), absorbing=False)
        self.assertIsNotNone(payload.payload)
        self.assertEqual(clean.lost_requests(), [])

    def test_error_payload_is_a_concise_bridge_json_rpc_error(self) -> None:
        payload = error_payload(7, "bridge: reason")
        message = json.loads(payload)
        self.assertEqual(message["id"], 7)
        self.assertEqual(message["error"]["code"], -32000)
        self.assertEqual(
            message["error"]["data"],
            {"bridge": True, "outcomeUnknown": True},
        )
        self.assertEqual(message["error"]["message"], "bridge: reason")

    def test_builtin_engine_loads_with_expected_policy(self) -> None:
        engine = load_engine()
        self.assertEqual(engine.ENGINE_NAME, ENGINE_NAME)
        self.assertEqual(engine.ENGINE_VERSION, ENGINE_VERSION)
        self.assertEqual(engine.Recovery.CORE_VERSION, CORE_VERSION)
        self.assertFalse(engine.Recovery.NOTE_RESULTS_WHEN_STALE)
        self.assertFalse(engine.make_recovery("0.4.0").stale())
        self.assertTrue(engine.make_recovery("0.3.9").stale())

    def test_engine_override_is_dynamically_loaded_by_version(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            first = Path(temp) / "engine_a.py"
            second = Path(temp) / "engine_b.py"
            _write_test_engine(first, name="engine-a", version="2.0.0", core="1.5.0")
            _write_test_engine(second, name="engine-b", version="3.1.0", core="2.0.0")
            engine_a = load_engine(first)
            engine_b = load_engine(second)
            self.assertEqual(engine_a.ENGINE_VERSION, "2.0.0")
            self.assertEqual(engine_a.ENGINE_NAME, "engine-a")
            self.assertEqual(engine_b.ENGINE_VERSION, "3.1.0")
            self.assertEqual(engine_b.ENGINE_NAME, "engine-b")
            self.assertTrue(engine_a.make_recovery("1.4.9").stale())
            self.assertFalse(engine_a.make_recovery("1.5.0").stale())

    def test_engine_loader_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            missing = Path(temp) / "missing.py"
            with self.assertRaisesRegex(ConnectorError, "not found"):
                load_engine(missing)
            bad_version = Path(temp) / "bad_version.py"
            bad_version.write_text(
                "ENGINE_NAME = 'test-engine'\nENGINE_VERSION = 'not-a-version'\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ConnectorError, "invalid ENGINE_VERSION"):
                load_engine(bad_version)
            bad_core = Path(temp) / "bad_core.py"
            _write_test_engine(bad_core, version="1.0.0", core="not-a-version")
            with self.assertRaisesRegex(ConnectorError, "invalid CORE_VERSION"):
                load_engine(bad_core)
            no_factory = Path(temp) / "no_factory.py"
            no_factory.write_text(
                "\n".join(
                    [
                        "import connector_engine as _base",
                        "ENGINE_NAME = 'test-engine'",
                        "ENGINE_VERSION = '1.0.0'",
                        "class Recovery(_base.Recovery):",
                        "    CORE_VERSION = '0.4.0'",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ConnectorError, "make_recovery"):
                load_engine(no_factory)


class PersistentConnectorStdioTest(unittest.TestCase):
    """Focused stdio tests for the persistent connector (fake node)."""

    def _start(self, node: FakeNodeServer, *, extra_env: dict[str, str] | None = None) -> tuple:
        process = _spawn_connect(node.port, extra_env=extra_env)
        reader = FdLineReader(process.stdout)  # type: ignore[arg-type]
        return process, reader

    @staticmethod
    def _send(process: subprocess.Popen, messages: list[dict]) -> None:
        assert process.stdin is not None
        process.stdin.write(
            "".join(
                json.dumps(item, separators=(",", ":")) + "\n" for item in messages
            ).encode("utf-8")
        )
        process.stdin.flush()

    def _collect(
        self, reader: FdLineReader, wanted: set[object], timeout: float = 10.0
    ) -> list[dict]:
        responses: list[dict] = []
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            raw = reader.read_line(timeout=0.3)
            if raw is None:
                break
            try:
                message = json.loads(raw)
            except ValueError:
                continue
            if (
                isinstance(message, dict)
                and message.get("id") in wanted
                and ("result" in message or "error" in message)
            ):
                responses.append(message)
                if {item.get("id") for item in responses} >= wanted:
                    break
        return responses

    @staticmethod
    def _stop_process(process: subprocess.Popen) -> int:
        if process.stdin is not None:
            try:
                process.stdin.close()
            except OSError:
                pass
        process.wait(timeout=15)
        for stream in (process.stdout, process.stderr):
            if stream is not None:
                try:
                    stream.close()
                except OSError:
                    pass
        return process.returncode

    def test_native_downstream_tools_are_discovered_and_proxied(self) -> None:
        node = FakeNodeServer()
        process: subprocess.Popen | None = None
        try:
            process, reader = self._start(node)
            self._send(
                process,
                [
                    {
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "initialize",
                        "params": {"protocolVersion": "2025-06-18", "capabilities": {}},
                    },
                    {"jsonrpc": "2.0", "method": "notifications/initialized"},
                    {"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}},
                    {
                        "jsonrpc": "2.0",
                        "id": 3,
                        "method": "tools/call",
                        "params": {"name": "echo", "arguments": {"value": "hello"}},
                    },
                    {
                        "jsonrpc": "2.0",
                        "id": 4,
                        "method": "tools/call",
                        "params": {"name": "shout", "arguments": {"value": "hello"}},
                    },
                ],
            )
            responses = self._collect(reader, {1, 2, 3, 4})
            by_id = {item.get("id"): item for item in responses}
            self.assertEqual({item.get("id") for item in responses}, {1, 2, 3, 4})
            # tools/list is proxied from the downstream session verbatim: the
            # Agent sees the native catalog, never a bridge-fixed tool surface.
            tools = by_id[2]["result"]["tools"]
            self.assertEqual(
                {tool["name"] for tool in tools}, {"echo", "shout"}
            )
            self.assertEqual(
                by_id[3]["result"]["structuredContent"]["value"], "hello"
            )
            self.assertEqual(
                by_id[4]["result"]["structuredContent"]["value"], "HELLO"
            )
            self.assertEqual(self._stop_process(process), 0)
        finally:
            node.stop()
            if process is not None and process.poll() is None:
                process.kill()

    def test_handshake_carries_connector_version_and_core_version(self) -> None:
        node = FakeNodeServer(core_version="0.4.0")
        process: subprocess.Popen | None = None
        try:
            process, reader = self._start(node)
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                handshakes, _sessions = node.snapshot()
                if handshakes:
                    break
                time.sleep(0.05)
            handshakes, _ = node.snapshot()
            self.assertEqual(len(handshakes), 1)
            self.assertEqual(handshakes[0]["op"], "connect")
            self.assertEqual(handshakes[0]["target"], "fake-target")
            self.assertEqual(handshakes[0]["connectorVersion"], ENGINE_VERSION)
            self._send(
                process,
                [
                    {
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "initialize",
                        "params": {"protocolVersion": "2025-06-18", "capabilities": {}},
                    }
                ],
            )
            responses = self._collect(reader, {1})
            self.assertEqual(len(responses), 1)
            result = responses[0]["result"]
            self.assertEqual(result["serverInfo"]["name"], "fake-mcp")
            # Core current -> initialize instructions stay raw (no stale note).
            self.assertEqual(result["instructions"], node.instructions)
            self.assertEqual(self._stop_process(process), 0)
        finally:
            node.stop()
            if process is not None and process.poll() is None:
                process.kill()

    def test_reconnects_replays_handshake_and_never_exits_while_stdin_open(self) -> None:
        node = FakeNodeServer(exit_after_call=True)
        process: subprocess.Popen | None = None
        try:
            process, reader = self._start(node)
            self._send(
                process,
                [
                    {
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "initialize",
                        "params": {"protocolVersion": "2025-06-18", "capabilities": {}},
                    },
                    {"jsonrpc": "2.0", "method": "notifications/initialized"},
                    {"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}},
                    {
                        "jsonrpc": "2.0",
                        "id": 3,
                        "method": "tools/call",
                        "params": {"name": "echo", "arguments": {"value": "first"}},
                    },
                ],
            )
            first = self._collect(reader, {1, 2, 3})
            self.assertEqual({item.get("id") for item in first}, {1, 2, 3})
            self.assertEqual(
                [item for item in first if item.get("id") == 3][0]["result"][
                    "structuredContent"
                ]["value"],
                "first",
            )
            # The fake MCP exited after answering; the connector must not exit
            # while Agent stdin is open.
            time.sleep(1.0)
            self.assertIsNone(process.poll())
            self._send(
                process,
                [
                    {"jsonrpc": "2.0", "id": 4, "method": "tools/list", "params": {}},
                    {
                        "jsonrpc": "2.0",
                        "id": 5,
                        "method": "tools/call",
                        "params": {"name": "echo", "arguments": {"value": "second"}},
                    },
                ],
            )
            second = self._collect(reader, {4, 5})
            self.assertEqual({item.get("id") for item in second}, {4, 5})
            self.assertEqual(
                [item for item in second if item.get("id") == 5][0]["result"][
                    "structuredContent"
                ]["value"],
                "second",
            )
            # No second initialize result may be forwarded to the Agent: the
            # replayed handshake response is swallowed by the connector.
            self.assertEqual(reader.drain(timeout=0.6), b"")
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                handshakes, sessions = node.snapshot()
                if len(sessions) >= 2:
                    break
                time.sleep(0.05)
            handshakes, sessions = node.snapshot()
            self.assertGreaterEqual(len(sessions), 2)
            self.assertEqual(sessions[1][0].get("method"), "initialize")
            self.assertEqual(self._stop_process(process), 0)
        finally:
            node.stop()
            if process is not None and process.poll() is None:
                process.kill()

    def test_pending_business_call_fails_exactly_once_when_stream_lost(self) -> None:
        node = FakeNodeServer(exit_before_response=True)
        process: subprocess.Popen | None = None
        try:
            process, reader = self._start(node)
            self._send(
                process,
                [
                    {
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "initialize",
                        "params": {"protocolVersion": "2025-06-18", "capabilities": {}},
                    },
                    {"jsonrpc": "2.0", "method": "notifications/initialized"},
                    {"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}},
                    {
                        "jsonrpc": "2.0",
                        "id": 3,
                        "method": "tools/call",
                        "params": {"name": "echo", "arguments": {"value": "lost"}},
                    },
                ],
            )
            responses = self._collect(reader, {1, 2, 3})
            self.assertEqual({item.get("id") for item in responses}, {1, 2, 3})
            error = [item for item in responses if item.get("id") == 3][0]["error"]
            self.assertEqual(error["code"], -32000)
            self.assertTrue(error["message"].startswith("bridge: node stream lost"))
            self.assertIn("will not be replayed", error["message"])
            self.assertIsNone(process.poll())
            # Exactly once: nothing further for id 3.
            self.assertEqual(reader.drain(timeout=0.6), b"")
            self.assertEqual(self._stop_process(process), 0)
        finally:
            node.stop()
            if process is not None and process.poll() is None:
                process.kill()

    def test_stale_core_warning_only_in_initialize_instructions_and_errors(self) -> None:
        node = FakeNodeServer(
            core_version="0.3.9",
            instructions="DOWNSTREAM INSTRUCTION.",
            exit_before_response=True,
        )
        process: subprocess.Popen | None = None
        try:
            process, reader = self._start(node)
            self._send(
                process,
                [
                    {
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "initialize",
                        "params": {"protocolVersion": "2025-06-18", "capabilities": {}},
                    },
                    {"jsonrpc": "2.0", "method": "notifications/initialized"},
                    {"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}},
                    {
                        "jsonrpc": "2.0",
                        "id": 3,
                        "method": "tools/call",
                        "params": {"name": "echo", "arguments": {"value": "stale"}},
                    },
                ],
            )
            responses = self._collect(reader, {1, 2, 3})
            instructions = [item for item in responses if item.get("id") == 1][0][
                "result"
            ]["instructions"]
            self.assertTrue(instructions.startswith("DOWNSTREAM INSTRUCTION."))
            self.assertIn("expects node core 0.4.0", instructions)
            self.assertIn("node reports 0.3.9", instructions)
            error = [item for item in responses if item.get("id") == 3][0]["error"]
            self.assertIn("core mismatch", error["message"])
            # Warnings appear on no other message class.
            for item in responses:
                self.assertLessEqual(
                    str(item).count("expects node core"), 1
                )
            self.assertEqual(self._stop_process(process), 0)
        finally:
            node.stop()
            if process is not None and process.poll() is None:
                process.kill()

    def test_no_stale_annotation_when_core_is_current(self) -> None:
        node = FakeNodeServer(core_version="0.4.0", instructions="CLEAN INST.")
        process: subprocess.Popen | None = None
        try:
            process, reader = self._start(node)
            self._send(
                process,
                [
                    {
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "initialize",
                        "params": {"protocolVersion": "2025-06-18", "capabilities": {}},
                    },
                    {"jsonrpc": "2.0", "method": "notifications/initialized"},
                    {
                        "jsonrpc": "2.0",
                        "id": 2,
                        "method": "tools/call",
                        "params": {"name": "echo", "arguments": {"value": "ok"}},
                    },
                ],
            )
            responses = self._collect(reader, {1, 2})
            init = [item for item in responses if item.get("id") == 1][0]["result"]
            self.assertEqual(init["instructions"], "CLEAN INST.")
            self.assertEqual(self._stop_process(process), 0)
        finally:
            node.stop()
            if process is not None and process.poll() is None:
                process.kill()

    def test_optional_result_note_only_with_engine_policy_and_stale_core(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            engine_path = Path(temp) / "engine.py"
            _write_test_engine(engine_path, note=True)
            node = FakeNodeServer(core_version="9.9.8", instructions="CLEAN INST.")
            process: subprocess.Popen | None = None
            try:
                process, reader = self._start(
                    node,
                    extra_env={"WIN_WSL_MCP_BRIDGE_CONNECTOR_ENGINE": str(engine_path)},
                )
                self._send(
                    process,
                    [
                        {
                            "jsonrpc": "2.0",
                            "id": 1,
                            "method": "initialize",
                            "params": {
                                "protocolVersion": "2025-06-18",
                                "capabilities": {},
                            },
                        },
                        {"jsonrpc": "2.0", "method": "notifications/initialized"},
                        {
                            "jsonrpc": "2.0",
                            "id": 2,
                            "method": "tools/call",
                            "params": {"name": "echo", "arguments": {"value": "note"}},
                        },
                    ],
                )
                responses = self._collect(reader, {1, 2})
                result = [item for item in responses if item.get("id") == 2][0]["result"]
                content = result["content"]
                self.assertGreaterEqual(len(content), 1)
                self.assertIn("expects node core", content[-1]["text"])
                self.assertEqual(self._stop_process(process), 0)
            finally:
                node.stop()
                if process is not None and process.poll() is None:
                    process.kill()

    def test_bad_engine_fails_startup_without_mcp_output(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            missing = Path(temp) / "engine.py"
            environment = os.environ.copy()
            environment["PYTHONDONTWRITEBYTECODE"] = "1"
            environment["WIN_WSL_MCP_BRIDGE_CONNECTOR_ENGINE"] = str(missing)
            process = subprocess.run(
                [
                    sys.executable,
                    str(WSL),
                    "connect",
                    "fake-target",
                    "--local-port",
                    "1",
                ],
                cwd=ROOT,
                env=environment,
                input="",
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=20,
                check=False,
            )
            self.assertEqual(process.returncode, 1)
            self.assertEqual(process.stdout, "")
            self.assertIn("connector engine", process.stderr)


if __name__ == "__main__":
    unittest.main(verbosity=2)
