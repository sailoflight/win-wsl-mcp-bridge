#!/usr/bin/env python3
"""Shared stdlib runtime for a bidirectional WIN-WSL MCP bridge."""

from __future__ import annotations

import argparse
import asyncio
import base64
import fnmatch
import hashlib
import ipaddress
import json
import os
import re
import secrets
import shutil
import signal
import socket
import sqlite3
import stat
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

BRIDGE_PROTOCOL = "win-wsl-mcp-bridge/0.2"
SERVER_VERSION = "0.4.0"
MAX_FRAME_BYTES = 1024 * 1024
BUFFER_SIZE = 65536
STREAM_DATA_ACK_TIMEOUT_SECONDS = 30
STREAM_EOF_GRACE_SECONDS = 5
ARTIFACT_CHUNK_BYTES = 65536
DEFAULT_MAX_ARTIFACT_BYTES = 512 * 1024 * 1024
MAX_DECLARED_ARTIFACT_BYTES = 4 * 1024 * 1024 * 1024
MAX_CONCURRENT_ARTIFACTS = 8
MAX_RESERVED_ARTIFACT_BYTES = 1024 * 1024 * 1024
MAX_SHARED_JSONRPC_BYTES = 16 * 1024 * 1024
SHARED_BACKEND_STOP_TIMEOUT_SECONDS = 10
ARTIFACT_META_KEY = "io.win-wsl-mcp-bridge/artifact"
ARTIFACT_ENV_PREFIX = "WIN_WSL_MCP_BRIDGE_ARTIFACT_"
ARTIFACT_ENV_KEYS = {
    f"{ARTIFACT_ENV_PREFIX}STAGE",
    f"{ARTIFACT_ENV_PREFIX}TOKEN",
    f"{ARTIFACT_ENV_PREFIX}LOCAL_HOST",
    f"{ARTIFACT_ENV_PREFIX}LOCAL_PORT",
    f"{ARTIFACT_ENV_PREFIX}PROTOCOL",
    f"{ARTIFACT_ENV_PREFIX}PYTHON",
    f"{ARTIFACT_ENV_PREFIX}PUBLISHER",
}
ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")


def _json_bytes(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def _is_loopback(host: str) -> bool:
    try:
        addresses = {
            ipaddress.ip_address(sockaddr[0].split("%", 1)[0])
            for _family, _type, _proto, _canonname, sockaddr in socket.getaddrinfo(
                host,
                None,
                type=socket.SOCK_STREAM,
            )
        }
    except (OSError, ValueError):
        return False
    return bool(addresses) and all(address.is_loopback for address in addresses)


class BridgeError(RuntimeError):
    pass


class JsonRpcError(BridgeError):
    def __init__(self, code: int, message: str):
        super().__init__(message)
        self.code = code


class _WindowsProcessJob:
    """Kill-on-close Job Object scoped to one shared backend generation."""

    def __init__(self, pid: int):
        if os.name != "nt":
            raise BridgeError("Windows process jobs are available only on Windows")
        import ctypes
        from ctypes import wintypes

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)

        class IO_COUNTERS(ctypes.Structure):
            _fields_ = [
                ("ReadOperationCount", ctypes.c_ulonglong),
                ("WriteOperationCount", ctypes.c_ulonglong),
                ("OtherOperationCount", ctypes.c_ulonglong),
                ("ReadTransferCount", ctypes.c_ulonglong),
                ("WriteTransferCount", ctypes.c_ulonglong),
                ("OtherTransferCount", ctypes.c_ulonglong),
            ]

        class BASIC_LIMIT_INFORMATION(ctypes.Structure):
            _fields_ = [
                ("PerProcessUserTimeLimit", ctypes.c_longlong),
                ("PerJobUserTimeLimit", ctypes.c_longlong),
                ("LimitFlags", wintypes.DWORD),
                ("MinimumWorkingSetSize", ctypes.c_size_t),
                ("MaximumWorkingSetSize", ctypes.c_size_t),
                ("ActiveProcessLimit", wintypes.DWORD),
                ("Affinity", ctypes.c_size_t),
                ("PriorityClass", wintypes.DWORD),
                ("SchedulingClass", wintypes.DWORD),
            ]

        class EXTENDED_LIMIT_INFORMATION(ctypes.Structure):
            _fields_ = [
                ("BasicLimitInformation", BASIC_LIMIT_INFORMATION),
                ("IoInfo", IO_COUNTERS),
                ("ProcessMemoryLimit", ctypes.c_size_t),
                ("JobMemoryLimit", ctypes.c_size_t),
                ("PeakProcessMemoryUsed", ctypes.c_size_t),
                ("PeakJobMemoryUsed", ctypes.c_size_t),
            ]

        kernel32.CreateJobObjectW.argtypes = [wintypes.LPVOID, wintypes.LPCWSTR]
        kernel32.CreateJobObjectW.restype = wintypes.HANDLE
        kernel32.SetInformationJobObject.argtypes = [
            wintypes.HANDLE,
            ctypes.c_int,
            wintypes.LPVOID,
            wintypes.DWORD,
        ]
        kernel32.SetInformationJobObject.restype = wintypes.BOOL
        kernel32.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
        kernel32.OpenProcess.restype = wintypes.HANDLE
        kernel32.AssignProcessToJobObject.argtypes = [wintypes.HANDLE, wintypes.HANDLE]
        kernel32.AssignProcessToJobObject.restype = wintypes.BOOL
        kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
        kernel32.CloseHandle.restype = wintypes.BOOL

        handle = kernel32.CreateJobObjectW(None, None)
        if not handle:
            raise BridgeError(f"CreateJobObjectW failed: {ctypes.get_last_error()}")
        self._kernel32 = kernel32
        self._handle = handle
        process_handle = None
        try:
            information = EXTENDED_LIMIT_INFORMATION()
            information.BasicLimitInformation.LimitFlags = 0x00002000
            if not kernel32.SetInformationJobObject(
                handle,
                9,
                ctypes.byref(information),
                ctypes.sizeof(information),
            ):
                raise BridgeError(
                    f"SetInformationJobObject failed: {ctypes.get_last_error()}"
                )
            access = 0x0001 | 0x0100 | 0x1000
            process_handle = kernel32.OpenProcess(access, False, pid)
            if not process_handle:
                raise BridgeError(f"OpenProcess failed: {ctypes.get_last_error()}")
            if not kernel32.AssignProcessToJobObject(handle, process_handle):
                raise BridgeError(
                    f"AssignProcessToJobObject failed: {ctypes.get_last_error()}"
                )
        except Exception:
            self.close()
            raise
        finally:
            if process_handle:
                kernel32.CloseHandle(process_handle)

    def close(self) -> None:
        handle = getattr(self, "_handle", None)
        if handle:
            self._handle = None
            self._kernel32.CloseHandle(handle)


class Registry:
    """SQLite-backed local allowlist with redacted public metadata."""

    SCHEMA_VERSION = 2
    SCHEMA = """
    CREATE TABLE IF NOT EXISTS servers (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        summary TEXT NOT NULL,
        command TEXT NOT NULL,
        args_json TEXT NOT NULL,
        cwd TEXT,
        env_json TEXT NOT NULL,
        process_json TEXT NOT NULL,
        capability_groups_json TEXT NOT NULL,
        server_info_json TEXT,
        artifact_delivery_json TEXT NOT NULL DEFAULT '{"enabled":false}',
        enabled INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0, 1)),
        updated_at_ns INTEGER NOT NULL
    )
    """

    def __init__(self, path: Path):
        self.path = path
        if not path.is_file():
            raise BridgeError(
                f"registry database does not exist: {path}; initialize it with registry-init"
            )
        if os.name != "nt":
            try:
                path.chmod(0o600)
            except OSError as exc:
                raise BridgeError(f"registry database permissions could not be restricted: {path}") from exc
        with self._connect() as connection:
            version = int(connection.execute("PRAGMA user_version").fetchone()[0])
            if version != self.SCHEMA_VERSION:
                raise BridgeError(
                    f"unsupported registry schema version {version}; expected {self.SCHEMA_VERSION}"
                )
            connection.execute("SELECT id FROM servers LIMIT 1").fetchall()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=5)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 5000")
        connection.execute("PRAGMA query_only = ON")
        return connection

    @classmethod
    def initialize_database(
        cls,
        database: Path,
        manifest: Path,
        *,
        replace: bool = False,
    ) -> None:
        raw = json.loads(manifest.read_text(encoding="utf-8"))
        rows = raw.get("servers") if isinstance(raw, dict) else raw
        if not isinstance(rows, list):
            raise BridgeError("manifest must be an array or an object with a servers array")
        validated = [cls._validate_manifest_row(row) for row in rows]
        ids = [row["id"] for row in validated]
        if len(ids) != len(set(ids)):
            raise BridgeError("manifest contains duplicate registry ids")
        previous_umask = os.umask(0o077) if os.name != "nt" else None
        connection: sqlite3.Connection | None = None
        try:
            database.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
            connection = sqlite3.connect(database, timeout=5)
            if os.name != "nt":
                database.chmod(0o600)
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("PRAGMA busy_timeout = 5000")
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute("PRAGMA synchronous = NORMAL")
            version = int(connection.execute("PRAGMA user_version").fetchone()[0])
            if version not in {0, 1, cls.SCHEMA_VERSION}:
                raise BridgeError(f"cannot migrate registry schema version {version}")
            connection.execute(cls.SCHEMA)
            if version == 1:
                connection.execute(
                    "ALTER TABLE servers ADD COLUMN artifact_delivery_json "
                    "TEXT NOT NULL DEFAULT '{\"enabled\":false}'"
                )
            connection.execute(f"PRAGMA user_version = {cls.SCHEMA_VERSION}")
            with connection:
                if replace:
                    connection.execute("DELETE FROM servers")
                for row in validated:
                    connection.execute(
                        """
                        INSERT INTO servers (
                            id, name, summary, command, args_json, cwd, env_json,
                            process_json, capability_groups_json, server_info_json,
                            artifact_delivery_json, enabled, updated_at_ns
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(id) DO UPDATE SET
                            name = excluded.name,
                            summary = excluded.summary,
                            command = excluded.command,
                            args_json = excluded.args_json,
                            cwd = excluded.cwd,
                            env_json = excluded.env_json,
                            process_json = excluded.process_json,
                            capability_groups_json = excluded.capability_groups_json,
                            server_info_json = excluded.server_info_json,
                            artifact_delivery_json = excluded.artifact_delivery_json,
                            enabled = excluded.enabled,
                            updated_at_ns = excluded.updated_at_ns
                        """,
                        (
                            row["id"],
                            row["name"],
                            row["summary"],
                            row["command"],
                            json.dumps(row["args"], separators=(",", ":")),
                            row["cwd"],
                            json.dumps(row["env"], separators=(",", ":")),
                            json.dumps(row["process"], separators=(",", ":")),
                            json.dumps(row["capabilityGroups"], separators=(",", ":")),
                            json.dumps(row["serverInfo"], separators=(",", ":"))
                            if row["serverInfo"] is not None
                            else None,
                            json.dumps(row["artifactDelivery"], separators=(",", ":")),
                            1 if row["enabled"] else 0,
                            time.time_ns(),
                        ),
                    )
        finally:
            if connection is not None:
                connection.close()
            if previous_umask is not None:
                os.umask(previous_umask)

    @staticmethod
    def _validate_manifest_row(row: Any) -> dict[str, Any]:
        if not isinstance(row, dict):
            raise BridgeError("every manifest entry must be an object")
        server_id = row.get("id")
        if not isinstance(server_id, str) or not ID_PATTERN.fullmatch(server_id):
            raise BridgeError(f"invalid registry id: {server_id!r}")
        command = row.get("command")
        if not isinstance(command, str) or not command:
            raise BridgeError(f"registry entry {server_id!r} requires command")
        args = row.get("args", [])
        if not isinstance(args, list) or not all(isinstance(arg, str) for arg in args):
            raise BridgeError(f"registry entry {server_id!r} args must be strings")
        env = row.get("env", {})
        if not isinstance(env, dict) or not all(
            isinstance(key, str) and isinstance(value, str) for key, value in env.items()
        ):
            raise BridgeError(f"registry entry {server_id!r} env must map strings to strings")
        if any(key.casefold().startswith(ARTIFACT_ENV_PREFIX.casefold()) for key in env):
            raise BridgeError(
                f"registry entry {server_id!r} env must not set bridge artifact variables"
            )
        process = row.get(
            "process",
            {"multiProcessAllowed": None, "enforcement": "unverified"},
        )
        if not isinstance(process, dict):
            raise BridgeError(f"registry entry {server_id!r} process must be an object")
        process = dict(process)
        allowed = process.get("multiProcessAllowed")
        if allowed is not None and not isinstance(allowed, bool):
            raise BridgeError(
                f"registry entry {server_id!r} multiProcessAllowed must be true, false, or null"
            )
        if allowed is False:
            process["enforcement"] = "bridge-shared-backend"
        client_lease = process.get("clientLease")
        if client_lease is not None:
            if allowed is not False:
                raise BridgeError(
                    f"registry entry {server_id!r} clientLease requires multiProcessAllowed=false"
                )
            if not isinstance(client_lease, dict):
                raise BridgeError(f"registry entry {server_id!r} clientLease must be an object")
            tool_patterns = client_lease.get("toolPatterns")
            release_tool = client_lease.get("releaseTool")
            release_arguments = client_lease.get("releaseArguments", {})
            released_path = client_lease.get(
                "releasedResultPath", ["structuredContent", "profileReleased"]
            )
            cleanup_timeout = client_lease.get("cleanupTimeoutSeconds", 15)
            if (
                not isinstance(tool_patterns, list)
                or not tool_patterns
                or not all(isinstance(pattern, str) and pattern for pattern in tool_patterns)
                or not isinstance(release_tool, str)
                or not release_tool
                or not isinstance(release_arguments, dict)
                or not all(isinstance(key, str) for key in release_arguments)
                or not isinstance(released_path, list)
                or not released_path
                or not all(isinstance(part, str) and part for part in released_path)
                or not isinstance(cleanup_timeout, (int, float))
                or isinstance(cleanup_timeout, bool)
                or cleanup_timeout <= 0
                or cleanup_timeout > 120
            ):
                raise BridgeError(f"registry entry {server_id!r} clientLease is invalid")
            process["clientLease"] = {
                "toolPatterns": list(tool_patterns),
                "releaseTool": release_tool,
                "releaseArguments": dict(release_arguments),
                "releasedResultPath": list(released_path),
                "releasedResultValue": client_lease.get("releasedResultValue", True),
                "cleanupTimeoutSeconds": float(cleanup_timeout),
                "busyPolicy": "error",
            }
        shared_state = process.get("sharedState")
        if shared_state is not None:
            if allowed is not False or not isinstance(shared_state, dict):
                raise BridgeError(
                    f"registry entry {server_id!r} sharedState requires a shared backend"
                )
            rejected_tools = shared_state.get("rejectTools")
            if (
                shared_state.get("mode") != "fixed"
                or not isinstance(rejected_tools, list)
                or not all(isinstance(tool, str) and tool for tool in rejected_tools)
            ):
                raise BridgeError(f"registry entry {server_id!r} sharedState is invalid")
            process["sharedState"] = {
                "mode": "fixed",
                "rejectTools": list(rejected_tools),
            }
        groups = row.get("capabilityGroups", [])
        if not isinstance(groups, list) or not all(isinstance(group, str) for group in groups):
            raise BridgeError(f"registry entry {server_id!r} capabilityGroups must be strings")
        server_info = row.get("serverInfo")
        if server_info is not None and not isinstance(server_info, dict):
            raise BridgeError(f"registry entry {server_id!r} serverInfo must be an object")
        cwd = row.get("cwd")
        if cwd is not None and not isinstance(cwd, str):
            raise BridgeError(f"registry entry {server_id!r} cwd must be a string or null")
        artifact_delivery = row.get("artifactDelivery", {"enabled": False})
        if not isinstance(artifact_delivery, dict):
            raise BridgeError(
                f"registry entry {server_id!r} artifactDelivery must be an object"
            )
        artifact_enabled = artifact_delivery.get("enabled", False)
        artifact_max_bytes = artifact_delivery.get(
            "maxBytes", DEFAULT_MAX_ARTIFACT_BYTES
        )
        if not isinstance(artifact_enabled, bool):
            raise BridgeError(
                f"registry entry {server_id!r} artifactDelivery.enabled must be boolean"
            )
        if (
            not isinstance(artifact_max_bytes, int)
            or isinstance(artifact_max_bytes, bool)
            or artifact_max_bytes <= 0
            or artifact_max_bytes > MAX_DECLARED_ARTIFACT_BYTES
        ):
            raise BridgeError(
                f"registry entry {server_id!r} artifactDelivery.maxBytes must be "
                f"between 1 and {MAX_DECLARED_ARTIFACT_BYTES}"
            )
        artifact_delivery = {
            "enabled": artifact_enabled,
            "maxBytes": artifact_max_bytes,
            "mode": "workspace-push-v1",
        }
        enabled = row.get("enabled", True)
        if not isinstance(enabled, bool):
            raise BridgeError(f"registry entry {server_id!r} enabled must be boolean")
        return {
            "id": server_id,
            "name": row.get("name") if isinstance(row.get("name"), str) else server_id,
            "summary": row.get("summary") if isinstance(row.get("summary"), str) else "",
            "command": command,
            "args": args,
            "cwd": cwd,
            "env": env,
            "process": process,
            "capabilityGroups": groups,
            "serverInfo": server_info,
            "artifactDelivery": artifact_delivery,
            "enabled": enabled,
        }

    def _get_row(self, server_id: str) -> sqlite3.Row:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM servers WHERE id = ? AND enabled = 1",
                (server_id,),
            ).fetchone()
        if row is None:
            raise BridgeError(f"unknown registry id: {server_id}")
        return row

    def _ids(self) -> list[str]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT id FROM servers WHERE enabled = 1 ORDER BY id"
            ).fetchall()
        return [str(row["id"]) for row in rows]

    def launch(self, server_id: str) -> dict[str, Any]:
        row = self._get_row(server_id)
        return {
            "id": row["id"],
            "command": row["command"],
            "args": json.loads(row["args_json"]),
            "cwd": row["cwd"],
            "env": json.loads(row["env_json"]),
            "process": json.loads(row["process_json"]),
            "artifactDelivery": json.loads(row["artifact_delivery_json"]),
        }

    def public(self, server_id: str) -> dict[str, Any]:
        row = self._get_row(server_id)
        private_process = json.loads(row["process_json"])
        process = {
            "multiProcessAllowed": private_process.get("multiProcessAllowed"),
            "enforcement": private_process.get("enforcement", "unverified"),
        }
        if isinstance(private_process.get("clientLease"), dict):
            process["clientLease"] = {
                "enabled": True,
                "busyPolicy": "error",
                "releaseOnDisconnect": True,
            }
        if isinstance(private_process.get("sharedState"), dict):
            process["sharedState"] = {"mode": "fixed"}
        public = {
            "id": row["id"],
            "name": row["name"],
            "summary": row["summary"],
            "process": process,
            "capabilityGroups": json.loads(row["capability_groups_json"]),
        }
        if row["server_info_json"] is not None:
            public["serverInfo"] = json.loads(row["server_info_json"])
        artifact_delivery = json.loads(row["artifact_delivery_json"])
        if artifact_delivery.get("enabled"):
            public["artifactDelivery"] = {
                "mode": "workspace-push-v1",
                "declaredMaxBytes": artifact_delivery["maxBytes"],
                "agentFetchRequired": False,
            }
        return public

    def query(self, action: str, arguments: dict[str, Any]) -> Any:
        if action == "list":
            return [self.public(server_id) for server_id in self._ids()]
        if action == "describe":
            server_id = arguments.get("id")
            if not isinstance(server_id, str):
                raise BridgeError("describe requires string id")
            return self.public(server_id)
        if action == "search":
            query = arguments.get("query", "")
            if not isinstance(query, str):
                raise BridgeError("search query must be a string")
            needle = query.casefold()
            return [
                row
                for row in (self.public(server_id) for server_id in self._ids())
                if needle in json.dumps(row, ensure_ascii=False).casefold()
            ]
        if action == "status":
            server_id = arguments.get("id")
            if server_id is None:
                return [
                    {"id": item, "registered": True, "availability": "not-probed"}
                    for item in self._ids()
                ]
            if not isinstance(server_id, str):
                raise BridgeError("status id must be a string")
            self._get_row(server_id)
            return {"id": server_id, "registered": True, "availability": "not-probed"}
        raise BridgeError(f"unknown registry action: {action}")


@dataclass
class StreamState:
    stream_id: str
    socket_writer: asyncio.StreamWriter | None = None
    process: asyncio.subprocess.Process | None = None
    opened: asyncio.Future[None] | None = None
    closed: asyncio.Event = field(default_factory=asyncio.Event)
    tasks: set[asyncio.Task[Any]] = field(default_factory=set)
    inbound: asyncio.Queue[tuple[int, bytes] | None] = field(
        default_factory=lambda: asyncio.Queue(maxsize=32)
    )
    inbound_sequence: int = 0
    outbound_sequence: int = 0
    outbound_ack: asyncio.Future[int] | None = None
    target: str | None = None
    artifact_inbox: Path | None = None
    artifact_stage: Path | None = None
    artifact_token: str | None = None
    artifact_max_bytes: int = DEFAULT_MAX_ARTIFACT_BYTES
    send_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    shared_backend: "SharedBackend | None" = None


@dataclass
class RoutedRequest:
    backend_id: str
    client_id: str | None
    original_id: Any
    message: dict[str, Any]
    done: asyncio.Future[dict[str, Any]]
    method: str
    is_initialize: bool = False
    is_release: bool = False
    cancelled: bool = False
    progress_token: tuple[str, Any] | None = None


@dataclass
class SharedBackendClient:
    stream: StreamState
    input_buffer: bytearray = field(default_factory=bytearray)
    input_eof: bool = False


@dataclass
class ArtifactTransferWaiter:
    stream_id: str
    ready: asyncio.Future[dict[str, Any]]
    done: asyncio.Future[dict[str, Any]]
    expected_size: int
    expected_sha256: str
    phase: str = "begin_sent"
    chunk_ack: asyncio.Future[int] | None = None


@dataclass
class ArtifactReceiveState:
    stream_id: str
    artifact_id: str
    name: str
    media_type: str | None
    expected_size: int
    temp_path: Path
    final_path: Path
    handle: Any
    queue: asyncio.Queue[bytes | None] = field(
        default_factory=lambda: asyncio.Queue(maxsize=32)
    )
    received: int = 0
    digest: Any = field(default_factory=hashlib.sha256)
    end_size: int | None = None
    end_sha256: str | None = None
    next_sequence: int = 0
    phase: str = "receiving"
    task: asyncio.Task[Any] | None = None
    end_task: asyncio.Task[Any] | None = None
    abort_task: asyncio.Task[Any] | None = None
    commit_task: asyncio.Task[Any] | None = None


class SharedBackend:
    """One generation-safe JSON-RPC backend shared by logical MCP clients."""

    def __init__(self, node: "BridgeNode", target: str, entry: dict[str, Any]):
        self.node = node
        self.target = target
        self.entry = entry
        self.process_config = dict(entry.get("process") or {})
        self.state = "exited"
        self.generation = 0
        self.state_history: list[tuple[str, int]] = [(self.state, self.generation)]
        self.process: asyncio.subprocess.Process | None = None
        self.windows_job: _WindowsProcessJob | None = None
        self.clients: dict[str, SharedBackendClient] = {}
        self.lifecycle_lock = asyncio.Lock()
        self.stdin_lock = asyncio.Lock()
        self.start_future: asyncio.Task[None] | None = None
        self.stop_future: asyncio.Task[None] | None = None
        self.request_queue: asyncio.Queue[RoutedRequest | None] = asyncio.Queue()
        self.pending_by_backend_id: dict[str, RoutedRequest] = {}
        self.pending_by_client_id: dict[tuple[str, str], RoutedRequest] = {}
        self.progress_tokens: dict[str, tuple[str, Any]] = {}
        self.server_requests: dict[tuple[str, str], Any] = {}
        self.current_request: RoutedRequest | None = None
        self.initialize_template: dict[str, Any] | None = None
        self.initialize_signature: tuple[Any, Any] | None = None
        self.initialize_pending = False
        self.initialize_event = asyncio.Event()
        self.initialized_forwarded = False
        self.lease_owner: str | None = None
        self.worker_task: asyncio.Task[Any] | None = None
        self.stdout_task: asyncio.Task[Any] | None = None
        self.stderr_task: asyncio.Task[Any] | None = None
        self.wait_task: asyncio.Task[Any] | None = None
        self.artifact_stage: Path | None = None
        self.artifact_token: str | None = None
        self.artifact_max_bytes = DEFAULT_MAX_ARTIFACT_BYTES

    def refresh_entry_if_exited(self, entry: dict[str, Any]) -> None:
        if self.state != "exited":
            return
        self.entry = entry
        self.process_config = dict(entry.get("process") or {})

    def _transition(self, state: str) -> None:
        self.state = state
        self.state_history.append((state, self.generation))
        if len(self.state_history) > 128:
            del self.state_history[:64]

    async def attach(self, stream: StreamState) -> None:
        while True:
            wait_for: asyncio.Task[None] | None = None
            async with self.lifecycle_lock:
                if self.state == "running":
                    self.clients[stream.stream_id] = SharedBackendClient(stream=stream)
                    stream.shared_backend = self
                    return
                if self.state == "starting":
                    wait_for = self.start_future
                elif self.state == "stopping":
                    wait_for = self.stop_future
                elif self.state == "exited":
                    self.generation += 1
                    self._transition("starting")
                    self.start_future = asyncio.create_task(
                        self._start_generation(self.generation)
                    )
                    wait_for = self.start_future
                else:
                    raise BridgeError(f"invalid shared backend state: {self.state}")
            if wait_for is None:
                raise BridgeError("shared backend lifecycle future is unavailable")
            try:
                await asyncio.shield(wait_for)
            except asyncio.CancelledError:
                self.node._background(self._stop_after_cancelled_attach(wait_for))
                raise

    async def _stop_after_cancelled_attach(self, start: asyncio.Task[None]) -> None:
        await asyncio.gather(start, return_exceptions=True)
        await self._stop_if_unused("all clients disconnected during shared backend startup")

    async def _start_generation(self, generation: int) -> None:
        environment = os.environ.copy()
        for key in ARTIFACT_ENV_KEYS:
            environment.pop(key, None)
        environment.update(self.entry.get("env", {}))
        artifact_config = self.entry.get("artifactDelivery", {"enabled": False})
        if bool(artifact_config.get("enabled")) and self.node.peer_artifacts:
            link_generation = self.node.link_generation or "disconnected"
            self.artifact_stage = (
                self.node.artifact_spool_root
                / link_generation
                / f"shared-{self.target}-{generation}"
            )
            self.artifact_stage.mkdir(parents=True, mode=0o700, exist_ok=False)
            try:
                self.artifact_stage.chmod(0o700)
            except OSError:
                pass
            self.artifact_token = secrets.token_urlsafe(32)
            self.artifact_max_bytes = min(
                int(artifact_config.get("maxBytes", DEFAULT_MAX_ARTIFACT_BYTES)),
                self.node.max_artifact_bytes,
            )
            environment.update(
                {
                    "WIN_WSL_MCP_BRIDGE_ARTIFACT_STAGE": str(self.artifact_stage),
                    "WIN_WSL_MCP_BRIDGE_ARTIFACT_TOKEN": self.artifact_token,
                    "WIN_WSL_MCP_BRIDGE_ARTIFACT_LOCAL_HOST": self.node.local_host,
                    "WIN_WSL_MCP_BRIDGE_ARTIFACT_LOCAL_PORT": str(self.node.local_port),
                    "WIN_WSL_MCP_BRIDGE_ARTIFACT_PROTOCOL": "artifacts/1",
                    "WIN_WSL_MCP_BRIDGE_ARTIFACT_PYTHON": sys.executable,
                    "WIN_WSL_MCP_BRIDGE_ARTIFACT_PUBLISHER": str(
                        Path(__file__).with_name("bridge_publisher.py").resolve()
                    ),
                }
            )
        kwargs: dict[str, Any] = {
            "cwd": self.entry.get("cwd") or None,
            "env": environment,
            "stdin": asyncio.subprocess.PIPE,
            "stdout": asyncio.subprocess.PIPE,
            "stderr": asyncio.subprocess.PIPE,
            "limit": MAX_SHARED_JSONRPC_BYTES,
        }
        if os.name == "nt":
            kwargs["creationflags"] = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
        else:
            kwargs["start_new_session"] = True
        process: asyncio.subprocess.Process | None = None
        windows_job: _WindowsProcessJob | None = None
        try:
            process = await asyncio.create_subprocess_exec(
                self.entry["command"],
                *self.entry.get("args", []),
                **kwargs,
            )
            if os.name == "nt":
                windows_job = _WindowsProcessJob(process.pid)
        except Exception:
            if windows_job is not None:
                windows_job.close()
            if process is not None:
                await self._terminate_process_tree(process, force=True)
            if self.artifact_stage is not None:
                shutil.rmtree(self.artifact_stage, ignore_errors=True)
            self.artifact_stage = None
            self.artifact_token = None
            async with self.lifecycle_lock:
                if self.generation == generation and self.state == "starting":
                    self._transition("stopping")
                    self._transition("exited")
            raise
        assert process is not None
        async with self.lifecycle_lock:
            if self.generation != generation or self.state != "starting":
                if windows_job is not None:
                    windows_job.close()
                await self._terminate_process_tree(process, force=True)
                raise BridgeError("shared backend generation changed during startup")
            self.process = process
            self.windows_job = windows_job
            if self.artifact_token is not None:
                self.node.shared_artifact_publishers[self.artifact_token] = self
            self.request_queue = asyncio.Queue()
            self.pending_by_backend_id.clear()
            self.pending_by_client_id.clear()
            self.progress_tokens.clear()
            self.server_requests.clear()
            self.current_request = None
            self.initialize_template = None
            self.initialize_signature = None
            self.initialize_pending = False
            self.initialize_event = asyncio.Event()
            self.initialized_forwarded = False
            self.lease_owner = None
            self.worker_task = asyncio.create_task(self._request_worker(generation))
            self.stdout_task = asyncio.create_task(self._read_stdout(generation))
            self.stderr_task = asyncio.create_task(self._read_stderr(generation))
            for task in (self.worker_task, self.stdout_task, self.stderr_task):
                task.add_done_callback(
                    lambda done, current_generation=generation: self._generation_task_done(
                        done, current_generation
                    )
                )
            self.wait_task = asyncio.create_task(self._wait_process(generation))
            self._transition("running")

    def _generation_task_done(
        self, task: asyncio.Task[Any], generation: int
    ) -> None:
        if task.cancelled() or generation != self.generation or self.state != "running":
            return
        error = task.exception()
        if error is None:
            return
        self.node.log(
            f"shared backend {self.target} generation {generation} task failed: "
            f"{type(error).__name__}: {error}"
        )
        self.node._background(self.stop("shared backend protocol task failed"))

    async def detach(self, stream_id: str) -> None:
        async with self.lifecycle_lock:
            self.clients.pop(stream_id, None)
        requests_settled = await self._cancel_client_requests(stream_id)
        if not requests_settled:
            await self.stop("disconnected client request did not cancel")
            return
        if self.lease_owner == stream_id:
            cleaned = await self._cleanup_lease(stream_id)
            if not cleaned:
                await self.stop("lease owner disconnected before cleanup completed")
                return
        await self._stop_if_unused("last client disconnected")

    async def _stop_if_unused(self, reason: str) -> None:
        async with self.lifecycle_lock:
            if self.clients or self.state != "running":
                return
            self._transition("stopping")
            self.stop_future = asyncio.create_task(
                self._stop_generation(self.generation, reason)
            )
            future = self.stop_future
        await future

    async def _cancel_client_requests(self, client_id: str) -> bool:
        pending_items = [
            pending
            for (owner, _request_id), pending in list(self.pending_by_client_id.items())
            if owner == client_id
        ]
        active: RoutedRequest | None = None
        for pending in pending_items:
            key = (client_id, self._typed_id(pending.original_id))
            if pending.is_initialize:
                self.pending_by_client_id.pop(key, None)
                pending.client_id = None
                if self.current_request is pending:
                    active = pending
                continue
            if self.current_request is pending:
                active = pending
                try:
                    await self._write_message(
                        {
                            "jsonrpc": "2.0",
                            "method": "notifications/cancelled",
                            "params": {"requestId": pending.backend_id},
                        }
                    )
                except BridgeError:
                    return False
                continue
            pending.cancelled = True
            self.pending_by_backend_id.pop(pending.backend_id, None)
            self.pending_by_client_id.pop(key, None)
            if pending.progress_token is not None:
                self.progress_tokens.pop(pending.progress_token[0], None)
            if not pending.done.done():
                pending.done.set_result({"cancelled": True})
        if active is None:
            return True
        try:
            await asyncio.wait_for(asyncio.shield(active.done), timeout=5)
        except TimeoutError:
            return False
        return True

    async def stop(self, reason: str) -> None:
        while True:
            async with self.lifecycle_lock:
                if self.state == "exited":
                    return
                if self.state == "stopping":
                    future = self.stop_future
                elif self.state == "starting":
                    future = self.start_future
                else:
                    self._transition("stopping")
                    self.stop_future = asyncio.create_task(
                        self._stop_generation(self.generation, reason)
                    )
                    future = self.stop_future
            if future is None:
                return
            await asyncio.gather(future, return_exceptions=False)
            if self.state == "exited":
                return

    async def _stop_generation(self, generation: int, reason: str) -> None:
        process = self.process
        if process is not None and process.stdin is not None and not process.stdin.is_closing():
            process.stdin.close()
        if process is not None:
            try:
                await asyncio.wait_for(
                    process.wait(), timeout=SHARED_BACKEND_STOP_TIMEOUT_SECONDS
                )
            except TimeoutError:
                await self._terminate_process_tree(process, force=False)
        await self._terminate_process_tree(process, force=True)
        current = asyncio.current_task()
        tasks = [self.worker_task, self.stdout_task, self.stderr_task, self.wait_task]
        for task in tasks:
            if task is not None and task is not current and not task.done():
                task.cancel()
        await asyncio.gather(
            *(task for task in tasks if task is not None and task is not current),
            return_exceptions=True,
        )
        self._fail_all_pending(BridgeError(reason))
        if self.artifact_token is not None:
            self.node.shared_artifact_publishers.pop(self.artifact_token, None)
        if self.artifact_stage is not None:
            shutil.rmtree(self.artifact_stage, ignore_errors=True)
        self.artifact_stage = None
        self.artifact_token = None
        self.process = None
        self.lease_owner = None
        async with self.lifecycle_lock:
            if self.generation == generation:
                self._transition("exited")

    async def _terminate_process_tree(
        self,
        process: asyncio.subprocess.Process | None,
        *,
        force: bool,
    ) -> None:
        if process is None:
            return
        if os.name == "nt":
            if force and self.windows_job is not None:
                self.windows_job.close()
                self.windows_job = None
            elif force:
                await asyncio.to_thread(
                    subprocess.run,
                    ["taskkill", "/PID", str(process.pid), "/T", "/F"],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    check=False,
                    timeout=10,
                )
            if process.returncode is None:
                try:
                    process.kill() if force else process.terminate()
                except ProcessLookupError:
                    pass
        else:
            try:
                os.killpg(process.pid, signal.SIGKILL if force else signal.SIGTERM)
            except ProcessLookupError:
                pass
        if process.returncode is None:
            try:
                await asyncio.wait_for(process.wait(), timeout=5)
            except TimeoutError:
                if not force:
                    await self._terminate_process_tree(process, force=True)

    async def _wait_process(self, generation: int) -> None:
        assert self.process is not None
        process = self.process
        code = await process.wait()
        if self.state == "stopping" or generation != self.generation:
            return
        self.node.log(
            f"shared backend {self.target} generation {generation} exited with code {code}"
        )
        async with self.lifecycle_lock:
            if self.generation != generation or self.state != "running":
                return
            self.stop_future = asyncio.current_task()
            self._transition("stopping")
        await self._terminate_process_tree(process, force=True)
        io_tasks = [
            task
            for task in (self.stdout_task, self.stderr_task)
            if task is not None and not task.done()
        ]
        if io_tasks:
            _done, still_running = await asyncio.wait(io_tasks, timeout=5)
            for task in still_running:
                task.cancel()
            if still_running:
                await asyncio.gather(*still_running, return_exceptions=True)
        self._fail_all_pending(BridgeError("shared backend exited"))
        worker = self.worker_task
        if worker is not None and not worker.done():
            worker.cancel()
            await asyncio.gather(worker, return_exceptions=True)
        if self.artifact_token is not None:
            self.node.shared_artifact_publishers.pop(self.artifact_token, None)
        if self.artifact_stage is not None:
            shutil.rmtree(self.artifact_stage, ignore_errors=True)
        self.artifact_stage = None
        self.artifact_token = None
        self.process = None
        streams = [client.stream for client in self.clients.values()]
        self.clients.clear()
        self.lease_owner = None
        async with self.lifecycle_lock:
            if self.generation == generation and self.state == "stopping":
                self._transition("exited")
        for stream in streams:
            stream.shared_backend = None
            await self.node._close_stream(stream.stream_id, remote=False)

    async def _read_stderr(self, generation: int) -> None:
        assert self.process is not None and self.process.stderr is not None
        while generation == self.generation:
            data = await self.process.stderr.read(BUFFER_SIZE)
            if not data:
                return
            text = data.decode("utf-8", errors="replace").rstrip()
            self.node.log(f"{self.target} shared stderr: {text}")

    async def _read_stdout(self, generation: int) -> None:
        assert self.process is not None and self.process.stdout is not None
        while generation == self.generation:
            line = await self.process.stdout.readline()
            if not line:
                return
            if len(line) > MAX_SHARED_JSONRPC_BYTES:
                raise BridgeError("shared backend JSON-RPC message exceeds limit")
            try:
                message = json.loads(line)
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise BridgeError("shared backend emitted invalid JSON-RPC") from exc
            if not isinstance(message, dict) or message.get("jsonrpc") != "2.0":
                raise BridgeError("shared backend emitted an invalid JSON-RPC object")
            await self._route_backend_message(message)

    async def consume_client_input(self, stream: StreamState) -> None:
        client = self.clients[stream.stream_id]
        try:
            while True:
                item = await stream.inbound.get()
                if item is None:
                    client.input_eof = True
                    self._finish_input_eof_if_idle(stream.stream_id)
                    return
                sequence, data = item
                client.input_buffer.extend(data)
                if len(client.input_buffer) > MAX_SHARED_JSONRPC_BYTES:
                    raise BridgeError("shared client JSON-RPC message exceeds limit")
                while b"\n" in client.input_buffer:
                    raw, _, remainder = client.input_buffer.partition(b"\n")
                    client.input_buffer = bytearray(remainder)
                    if not raw.strip():
                        continue
                    try:
                        message = json.loads(raw)
                    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                        raise BridgeError("shared client sent invalid JSON-RPC") from exc
                    if not isinstance(message, dict) or message.get("jsonrpc") != "2.0":
                        raise BridgeError("shared client sent an invalid JSON-RPC object")
                    await self._route_client_message(stream.stream_id, message)
                await self.node._send_frame(
                    {
                        "type": "data_ok",
                        "stream": stream.stream_id,
                        "sequence": sequence,
                    }
                )
        except (BridgeError, BrokenPipeError, ConnectionError, OSError) as exc:
            self.node.log(f"shared stream {stream.stream_id} input closed: {exc}")
            self.node._background(self.node._close_stream(stream.stream_id, remote=False))

    def _finish_input_eof_if_idle(self, client_id: str) -> None:
        client = self.clients.get(client_id)
        if client is None or not client.input_eof:
            return
        if any(owner == client_id for owner, _request_id in self.pending_by_client_id):
            return
        self.node._background(self.node._close_stream(client_id, remote=False))

    @staticmethod
    def _valid_rpc_id(value: Any) -> bool:
        return value is None or (
            isinstance(value, (str, int, float)) and not isinstance(value, bool)
        )

    @staticmethod
    def _typed_id(value: Any) -> str:
        return f"{type(value).__name__}:{json.dumps(value, ensure_ascii=False, sort_keys=True)}"

    def _next_backend_id(self, client_id: str | None) -> str:
        owner = client_id or "bridge"
        return f"bridge:{self.generation}:{owner}:{uuid.uuid4().hex}"

    async def _route_client_message(
        self, client_id: str, message: dict[str, Any]
    ) -> None:
        method = message.get("method")
        has_id = "id" in message
        if has_id and not self._valid_rpc_id(message.get("id")):
            raise BridgeError("shared client JSON-RPC id must be a scalar")
        if not isinstance(method, str):
            if has_id and ("result" in message or "error" in message):
                await self._route_client_response(client_id, message)
                return
            raise BridgeError("shared client JSON-RPC object has no method")
        if not has_id:
            if method == "notifications/initialized":
                if self.initialize_pending:
                    await self.initialize_event.wait()
                if (
                    not self.initialized_forwarded
                    and self.initialize_template is not None
                    and "result" in self.initialize_template
                ):
                    self.initialized_forwarded = True
                    await self._write_message(message)
                return
            if method == "notifications/cancelled":
                await self._route_cancellation(client_id, message)
                return
            await self._write_message(message)
            return
        if method == "initialize":
            await self._handle_initialize(client_id, message)
            return
        if method == "tools/call":
            if await self._reject_fixed_state_mutation(client_id, message):
                return
            if await self._apply_lease_policy(client_id, message):
                return
        await self._enqueue_request(client_id, message)

    async def _handle_initialize(
        self, client_id: str, message: dict[str, Any]
    ) -> None:
        params = message.get("params")
        if not isinstance(params, dict):
            raise BridgeError("initialize params must be an object")
        signature = (
            params.get("protocolVersion"),
            json.dumps(
                params.get("capabilities", {}),
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ),
        )
        if self.initialize_signature is not None and signature != self.initialize_signature:
            await self._send_client_message(
                client_id,
                {
                    "jsonrpc": "2.0",
                    "id": message.get("id"),
                    "error": {
                        "code": -32602,
                        "message": "shared backend initialize capabilities are incompatible",
                    },
                },
            )
            return
        if self.initialize_signature is None:
            self.initialize_signature = signature
        if self.initialize_template is not None:
            response = dict(self.initialize_template)
            response["id"] = message.get("id")
            await self._send_client_message(client_id, response)
            return
        if self.initialize_pending:
            await self.initialize_event.wait()
            if self.initialize_template is None:
                raise BridgeError("shared backend initialization failed")
            response = dict(self.initialize_template)
            response["id"] = message.get("id")
            await self._send_client_message(client_id, response)
            return
        self.initialize_pending = True
        await self._enqueue_request(client_id, message, is_initialize=True)

    async def _enqueue_request(
        self,
        client_id: str | None,
        message: dict[str, Any],
        *,
        is_initialize: bool = False,
        is_release: bool = False,
    ) -> RoutedRequest:
        original_id = message.get("id")
        if client_id is not None:
            client_key = (client_id, self._typed_id(original_id))
            if client_key in self.pending_by_client_id:
                raise BridgeError("client reused an active JSON-RPC request id")
        backend_id = self._next_backend_id(client_id)
        forwarded = dict(message)
        forwarded["id"] = backend_id
        progress_token: tuple[str, Any] | None = None
        params = forwarded.get("params")
        if client_id is not None and isinstance(params, dict):
            metadata = params.get("_meta")
            if isinstance(metadata, dict) and "progressToken" in metadata:
                original_progress = metadata["progressToken"]
                backend_progress = (
                    f"bridge-progress:{self.generation}:{client_id}:{uuid.uuid4().hex}"
                )
                forwarded_params = dict(params)
                forwarded_metadata = dict(metadata)
                forwarded_metadata["progressToken"] = backend_progress
                forwarded_params["_meta"] = forwarded_metadata
                forwarded["params"] = forwarded_params
                self.progress_tokens[backend_progress] = (client_id, original_progress)
                progress_token = (backend_progress, original_progress)
        pending = RoutedRequest(
            backend_id=backend_id,
            client_id=client_id,
            original_id=original_id,
            message=forwarded,
            done=asyncio.get_running_loop().create_future(),
            method=str(message.get("method")),
            is_initialize=is_initialize,
            is_release=is_release,
            progress_token=progress_token,
        )
        self.pending_by_backend_id[backend_id] = pending
        if client_id is not None:
            self.pending_by_client_id[(client_id, self._typed_id(original_id))] = pending
        await self.request_queue.put(pending)
        return pending

    async def _request_worker(self, generation: int) -> None:
        while generation == self.generation:
            pending = await self.request_queue.get()
            if pending is None:
                return
            if pending.cancelled:
                continue
            self.current_request = pending
            try:
                await self._write_message(pending.message)
                await pending.done
            finally:
                if self.current_request is pending:
                    self.current_request = None

    async def _write_message(self, message: dict[str, Any]) -> None:
        process = self.process
        if self.state != "running" or process is None or process.stdin is None:
            raise BridgeError("shared backend is not running")
        data = _json_bytes(message) + b"\n"
        if len(data) > MAX_SHARED_JSONRPC_BYTES:
            raise BridgeError("shared backend JSON-RPC input exceeds limit")
        async with self.stdin_lock:
            process.stdin.write(data)
            await process.stdin.drain()

    async def _route_backend_message(self, message: dict[str, Any]) -> None:
        method = message.get("method")
        if "id" in message and not self._valid_rpc_id(message.get("id")):
            raise BridgeError("shared backend JSON-RPC id must be a scalar")
        if isinstance(method, str):
            if "id" in message:
                await self._route_backend_request(message)
            elif method == "notifications/progress":
                params = message.get("params")
                token = params.get("progressToken") if isinstance(params, dict) else None
                route = self.progress_tokens.get(str(token))
                if route is not None:
                    client_id, original_token = route
                    forwarded = dict(message)
                    forwarded_params = dict(params)
                    forwarded_params["progressToken"] = original_token
                    forwarded["params"] = forwarded_params
                    await self._send_client_message(client_id, forwarded)
            elif method in {
                "notifications/tools/list_changed",
                "notifications/resources/list_changed",
                "notifications/prompts/list_changed",
            }:
                await asyncio.gather(
                    *(
                        self._send_client_message(client_id, message)
                        for client_id in list(self.clients)
                    ),
                    return_exceptions=True,
                )
            else:
                client_id = (
                    self.current_request.client_id
                    if self.current_request is not None
                    and self.current_request.client_id in self.clients
                    else None
                )
                if client_id is None and self.lease_owner in self.clients:
                    client_id = self.lease_owner
                if client_id is not None:
                    await self._send_client_message(client_id, message)
            return
        backend_id = message.get("id")
        pending = self.pending_by_backend_id.pop(str(backend_id), None)
        if pending is None:
            raise BridgeError("shared backend returned an unknown response id")
        if pending.client_id is not None:
            self.pending_by_client_id.pop(
                (pending.client_id, self._typed_id(pending.original_id)), None
            )
        if pending.progress_token is not None:
            self.progress_tokens.pop(pending.progress_token[0], None)
        response = dict(message)
        response["id"] = pending.original_id
        if pending.is_initialize:
            self.initialize_template = dict(response)
            self.initialize_template.pop("id", None)
            self.initialize_pending = False
            self.initialize_event.set()
        if pending.is_release and self._release_succeeded(message):
            self.lease_owner = None
        if pending.client_id is not None and pending.client_id in self.clients:
            await self._send_client_message(pending.client_id, response)
        if not pending.done.done():
            pending.done.set_result(message)
        if pending.client_id is not None:
            self._finish_input_eof_if_idle(pending.client_id)

    async def _route_backend_request(self, message: dict[str, Any]) -> None:
        client_id = (
            self.current_request.client_id
            if self.current_request is not None
            and self.current_request.client_id in self.clients
            else None
        )
        if client_id is None and self.lease_owner in self.clients:
            client_id = self.lease_owner
        if client_id is None and len(self.clients) == 1:
            client_id = next(iter(self.clients))
        if client_id is None:
            await self._write_message(
                {
                    "jsonrpc": "2.0",
                    "id": message.get("id"),
                    "error": {"code": -32000, "message": "no shared backend client available"},
                }
            )
            return
        client_request_id = f"bridge-server:{self.generation}:{uuid.uuid4().hex}"
        self.server_requests[
            (client_id, self._typed_id(client_request_id))
        ] = message.get("id")
        forwarded = dict(message)
        forwarded["id"] = client_request_id
        await self._send_client_message(client_id, forwarded)

    async def _route_client_response(
        self, client_id: str, message: dict[str, Any]
    ) -> None:
        key = (client_id, self._typed_id(message.get("id")))
        backend_id = self.server_requests.pop(key, None)
        if backend_id is None:
            raise BridgeError("client returned an unknown server-request id")
        forwarded = dict(message)
        forwarded["id"] = backend_id
        await self._write_message(forwarded)

    async def _route_cancellation(
        self, client_id: str, message: dict[str, Any]
    ) -> None:
        params = message.get("params")
        request_id = params.get("requestId") if isinstance(params, dict) else None
        pending = self.pending_by_client_id.get(
            (client_id, self._typed_id(request_id))
        )
        if pending is None:
            return
        if self.current_request is pending:
            forwarded = dict(message)
            forwarded_params = dict(params)
            forwarded_params["requestId"] = pending.backend_id
            forwarded["params"] = forwarded_params
            await self._write_message(forwarded)
            return
        pending.cancelled = True
        self.pending_by_backend_id.pop(pending.backend_id, None)
        self.pending_by_client_id.pop(
            (client_id, self._typed_id(pending.original_id)), None
        )
        if pending.progress_token is not None:
            self.progress_tokens.pop(pending.progress_token[0], None)
        await self._send_client_message(
            client_id,
            {
                "jsonrpc": "2.0",
                "id": pending.original_id,
                "error": {"code": -32800, "message": "Request cancelled"},
            },
        )
        if not pending.done.done():
            pending.done.set_result({"cancelled": True})

    def _tool_call(self, message: dict[str, Any]) -> tuple[str | None, dict[str, Any]]:
        params = message.get("params")
        if not isinstance(params, dict):
            return None, {}
        arguments = params.get("arguments")
        return (
            params.get("name") if isinstance(params.get("name"), str) else None,
            arguments if isinstance(arguments, dict) else {},
        )

    async def _reject_fixed_state_mutation(
        self, client_id: str, message: dict[str, Any]
    ) -> bool:
        shared_state = self.process_config.get("sharedState")
        if not isinstance(shared_state, dict):
            return False
        tool_name, _arguments = self._tool_call(message)
        if tool_name not in shared_state.get("rejectTools", []):
            return False
        await self._send_tool_policy_error(
            client_id,
            message.get("id"),
            "shared_view_fixed",
            "This shared backend uses a fixed tool view; per-client view changes are disabled.",
        )
        return True

    def _is_release_call(self, tool_name: str | None, arguments: dict[str, Any]) -> bool:
        lease = self.process_config.get("clientLease")
        if not isinstance(lease, dict) or tool_name != lease.get("releaseTool"):
            return False
        expected = lease.get("releaseArguments", {})
        return all(arguments.get(key) == value for key, value in expected.items())

    async def _apply_lease_policy(
        self, client_id: str, message: dict[str, Any]
    ) -> bool:
        lease = self.process_config.get("clientLease")
        if not isinstance(lease, dict):
            return False
        tool_name, arguments = self._tool_call(message)
        if tool_name is None:
            return False
        is_release = self._is_release_call(tool_name, arguments)
        acquires = any(
            fnmatch.fnmatchcase(tool_name, pattern)
            for pattern in lease.get("toolPatterns", [])
        )
        if not acquires and not is_release:
            return False
        if self.lease_owner is None and not is_release:
            self.lease_owner = client_id
        elif self.lease_owner != client_id:
            await self._send_tool_policy_error(
                client_id,
                message.get("id"),
                "client_lease_busy",
                "The shared backend resource is owned by another client.",
            )
            return True
        await self._enqueue_request(
            client_id,
            message,
            is_release=is_release,
        )
        return True

    async def _cleanup_lease(self, owner: str) -> bool:
        lease = self.process_config.get("clientLease")
        if not isinstance(lease, dict) or self.lease_owner != owner:
            self.lease_owner = None
            return True
        cleanup = {
            "jsonrpc": "2.0",
            "id": f"bridge-cleanup:{uuid.uuid4().hex}",
            "method": "tools/call",
            "params": {
                "name": lease["releaseTool"],
                "arguments": dict(lease.get("releaseArguments", {})),
            },
        }
        try:
            pending = await self._enqueue_request(None, cleanup, is_release=True)
            await asyncio.wait_for(
                pending.done,
                timeout=float(lease.get("cleanupTimeoutSeconds", 15)),
            )
        except (BridgeError, TimeoutError):
            return False
        return self.lease_owner is None

    def _release_succeeded(self, response: dict[str, Any]) -> bool:
        lease = self.process_config.get("clientLease")
        if not isinstance(lease, dict) or "result" not in response:
            return False
        value: Any = response["result"]
        for part in lease.get("releasedResultPath", []):
            if not isinstance(value, dict) or part not in value:
                return False
            value = value[part]
        return value == lease.get("releasedResultValue", True)

    async def _send_tool_policy_error(
        self,
        client_id: str,
        request_id: Any,
        code: str,
        message: str,
    ) -> None:
        detail = {"code": code, "retryable": code == "client_lease_busy"}
        await self._send_client_message(
            client_id,
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "content": [
                        {
                            "type": "text",
                            "text": json.dumps(
                                {"error": detail, "message": message},
                                ensure_ascii=False,
                                separators=(",", ":"),
                            ),
                        }
                    ],
                    "structuredContent": {"error": detail, "message": message},
                    "isError": True,
                    "_meta": {"io.win-wsl-mcp-bridge/runtime": detail},
                },
            },
        )

    async def _send_client_message(
        self, client_id: str, message: dict[str, Any]
    ) -> None:
        client = self.clients.get(client_id)
        if client is None:
            return
        try:
            await self.node._send_jsonrpc_to_stream(client.stream, message)
        except BridgeError:
            self.node._background(
                self.node._close_stream(client.stream.stream_id, remote=False)
            )

    def artifact_stream(self) -> StreamState | None:
        pending = self.current_request
        if pending is None or pending.client_id is None:
            return None
        client = self.clients.get(pending.client_id)
        return client.stream if client is not None else None

    def _fail_all_pending(self, error: BaseException) -> None:
        for pending in list(self.pending_by_backend_id.values()):
            if not pending.done.done():
                pending.done.set_result({"bridgeError": str(error)})
        self.pending_by_backend_id.clear()
        self.pending_by_client_id.clear()
        self.progress_tokens.clear()
        self.server_requests.clear()
        if self.initialize_pending and self.initialize_template is None:
            self.initialize_template = {
                "jsonrpc": "2.0",
                "error": {"code": -32000, "message": "shared backend initialization failed"},
            }
            self.initialize_pending = False
            self.initialize_event.set()


class BridgeNode:
    """One symmetric bridge node with a local control socket and one peer link."""

    def __init__(
        self,
        *,
        side: str,
        registry: Registry,
        local_host: str,
        local_port: int,
        link_mode: str,
        link_host: str,
        link_port: int,
        reconnect_delay: float = 0.5,
        allowed_artifact_roots: list[Path] | None = None,
        artifact_spool_root: Path | None = None,
        max_artifact_bytes: int = DEFAULT_MAX_ARTIFACT_BYTES,
    ):
        if side not in {"win", "wsl"}:
            raise BridgeError("side must be win or wsl")
        if not _is_loopback(local_host):
            raise BridgeError("local control listener must use a loopback address")
        if not _is_loopback(link_host):
            if link_mode == "listen":
                raise BridgeError("bridge link listener must use a loopback address")
            raise BridgeError("bridge link connector must use a loopback address")
        self.side = side
        self.registry = registry
        self.local_host = local_host
        self.local_port = local_port
        self.link_mode = link_mode
        self.link_host = link_host
        self.link_port = link_port
        self.reconnect_delay = reconnect_delay
        self.link_reader: asyncio.StreamReader | None = None
        self.link_writer: asyncio.StreamWriter | None = None
        self.link_ready = asyncio.Event()
        self.link_write_lock = asyncio.Lock()
        self.link_installing = False
        self.streams: dict[str, StreamState] = {}
        self.pending_registry: dict[str, asyncio.Future[Any]] = {}
        self.pending_artifacts: dict[str, ArtifactTransferWaiter] = {}
        self.receiving_artifacts: dict[str, ArtifactReceiveState] = {}
        self.artifact_publishers: dict[str, str] = {}
        self.shared_artifact_publishers: dict[str, SharedBackend] = {}
        self.shared_backends: dict[str, SharedBackend] = {}
        self.allowed_artifact_roots = [
            root.expanduser().resolve() for root in (allowed_artifact_roots or [])
        ]
        for root in self.allowed_artifact_roots:
            if not root.is_dir():
                raise BridgeError(f"allowed artifact root is not a directory: {root}")
        artifact_spool_base = (
            artifact_spool_root.expanduser().resolve()
            if artifact_spool_root is not None
            else default_registry_path(side).parent / "spool"
        )
        self.artifact_spool_root = artifact_spool_base / f"{side}-artifacts-v1"
        self.peer_artifacts = False
        self.peer_artifact_chunk_bytes = ARTIFACT_CHUNK_BYTES
        self.link_generation: str | None = None
        if max_artifact_bytes <= 0:
            raise BridgeError("max_artifact_bytes must be positive")
        self.max_artifact_bytes = max_artifact_bytes
        self.background_tasks: set[asyncio.Task[Any]] = set()
        self.local_server: asyncio.AbstractServer | None = None
        self.link_server: asyncio.AbstractServer | None = None

    def log(self, message: str) -> None:
        print(f"[{self.side}-bridge] {message}", file=sys.stderr, flush=True)

    def _background(self, coroutine: Any) -> asyncio.Task[Any]:
        task = asyncio.create_task(coroutine)
        self.background_tasks.add(task)

        def finished(done: asyncio.Task[Any]) -> None:
            self.background_tasks.discard(done)
            if done.cancelled():
                return
            error = done.exception()
            if error is not None:
                self.log(f"background task failed: {type(error).__name__}: {error}")

        task.add_done_callback(finished)
        return task

    def _prepare_artifact_spool(self) -> None:
        shutil.rmtree(self.artifact_spool_root, ignore_errors=True)
        self.artifact_spool_root.mkdir(parents=True, mode=0o700, exist_ok=True)
        try:
            self.artifact_spool_root.chmod(0o700)
        except OSError:
            pass
        removed_partials = self._cleanup_workspace_partials()
        if removed_partials:
            self.log(f"removed {removed_partials} stale workspace artifact partial(s)")

    def _cleanup_workspace_partials(self) -> int:
        removed = 0
        for root in self.allowed_artifact_roots:
            artifact_root = root / ".mcp-artifacts"
            try:
                root_metadata = artifact_root.lstat()
            except FileNotFoundError:
                continue
            if not stat.S_ISDIR(root_metadata.st_mode) or stat.S_ISLNK(root_metadata.st_mode):
                continue
            try:
                candidates = list(artifact_root.iterdir())
            except OSError:
                continue
            for candidate in candidates:
                try:
                    candidate_metadata = candidate.lstat()
                except FileNotFoundError:
                    continue
                if (
                    not stat.S_ISDIR(candidate_metadata.st_mode)
                    or stat.S_ISLNK(candidate_metadata.st_mode)
                ):
                    continue
                partial = candidate / ".partial"
                try:
                    partial_metadata = partial.lstat()
                except FileNotFoundError:
                    continue
                if not stat.S_ISREG(partial_metadata.st_mode):
                    continue
                try:
                    partial.unlink()
                except OSError:
                    continue
                removed += 1
                try:
                    candidate.rmdir()
                except OSError:
                    pass
        return removed

    async def run(self) -> None:
        connector: asyncio.Task[Any] | None = None
        try:
            self._prepare_artifact_spool()
            self.local_server = await asyncio.start_server(
                self._handle_local,
                self.local_host,
                self.local_port,
                limit=MAX_FRAME_BYTES,
            )
            self.log(f"local control listening on {self.local_host}:{self.local_port}")
            if self.link_mode == "listen":
                self.link_server = await asyncio.start_server(
                    self._accept_link,
                    self.link_host,
                    self.link_port,
                    limit=MAX_FRAME_BYTES,
                )
                self.log(f"peer link listening on {self.link_host}:{self.link_port}")
                async with self.local_server, self.link_server:
                    await asyncio.gather(
                        self.local_server.serve_forever(),
                        self.link_server.serve_forever(),
                    )
            elif self.link_mode == "connect":
                connector = asyncio.create_task(self._connect_loop())
                async with self.local_server:
                    await self.local_server.serve_forever()
            else:
                raise BridgeError("link mode must be listen or connect")
        finally:
            if connector is not None:
                connector.cancel()
                await asyncio.gather(connector, return_exceptions=True)
            for server in (self.local_server, self.link_server):
                if server is not None:
                    server.close()
            for server in (self.local_server, self.link_server):
                if server is not None:
                    await server.wait_closed()
            await self._fail_link_state("bridge node shutting down")
            if self.link_writer is not None:
                self.link_writer.close()
                try:
                    await self.link_writer.wait_closed()
                except OSError:
                    pass
            current = asyncio.current_task()
            pending = [
                task
                for task in self.background_tasks
                if task is not current and not task.done()
            ]
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)

    async def _connect_loop(self) -> None:
        while True:
            try:
                reader, writer = await asyncio.open_connection(
                    self.link_host,
                    self.link_port,
                    limit=MAX_FRAME_BYTES,
                )
                await self._install_link(reader, writer, initiator=True)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.log(f"peer link unavailable: {exc}")
            await asyncio.sleep(self.reconnect_delay)

    async def _accept_link(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        if self.link_ready.is_set() or self.link_installing:
            writer.write(_json_bytes({"type": "hello_error", "message": "peer already connected"}) + b"\n")
            await writer.drain()
            writer.close()
            await writer.wait_closed()
            return
        self.link_installing = True
        try:
            await self._install_link(reader, writer, initiator=False)
        except asyncio.CancelledError:
            raise
        except (EOFError, BridgeError, ConnectionError, OSError) as exc:
            self.log(f"peer link ended: {exc}")
        finally:
            self.link_installing = False

    async def _install_link(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        *,
        initiator: bool,
    ) -> None:
        try:
            if initiator:
                writer.write(
                    _json_bytes(
                        {
                            "type": "hello",
                            "protocol": BRIDGE_PROTOCOL,
                            "side": self.side,
                            "extensions": {
                                "artifacts": {"version": 1, "maxChunk": ARTIFACT_CHUNK_BYTES}
                            },
                        }
                    )
                    + b"\n"
                )
                await writer.drain()
                reply = await self._read_frame(reader)
                if reply.get("type") != "hello_ok" or reply.get("protocol") != BRIDGE_PROTOCOL:
                    raise BridgeError(f"peer rejected bridge handshake: {reply}")
                artifact_extension = (reply.get("extensions") or {}).get("artifacts")
                self.peer_artifacts = bool(
                    isinstance(artifact_extension, dict)
                    and artifact_extension.get("version") == 1
                    and isinstance(artifact_extension.get("maxChunk"), int)
                    and not isinstance(artifact_extension.get("maxChunk"), bool)
                    and 0 < artifact_extension["maxChunk"] <= ARTIFACT_CHUNK_BYTES
                )
                self.peer_artifact_chunk_bytes = (
                    min(ARTIFACT_CHUNK_BYTES, artifact_extension["maxChunk"])
                    if self.peer_artifacts
                    else ARTIFACT_CHUNK_BYTES
                )
            else:
                hello = await self._read_frame(reader)
                if hello.get("type") != "hello" or hello.get("protocol") != BRIDGE_PROTOCOL:
                    raise BridgeError("invalid bridge handshake")
                artifact_extension = (hello.get("extensions") or {}).get("artifacts")
                self.peer_artifacts = bool(
                    isinstance(artifact_extension, dict)
                    and artifact_extension.get("version") == 1
                    and isinstance(artifact_extension.get("maxChunk"), int)
                    and not isinstance(artifact_extension.get("maxChunk"), bool)
                    and 0 < artifact_extension["maxChunk"] <= ARTIFACT_CHUNK_BYTES
                )
                self.peer_artifact_chunk_bytes = (
                    min(ARTIFACT_CHUNK_BYTES, artifact_extension["maxChunk"])
                    if self.peer_artifacts
                    else ARTIFACT_CHUNK_BYTES
                )
                extensions = (
                    {"artifacts": {"version": 1, "maxChunk": ARTIFACT_CHUNK_BYTES}}
                    if self.peer_artifacts
                    else {}
                )
                writer.write(
                    _json_bytes(
                        {
                            "type": "hello_ok",
                            "protocol": BRIDGE_PROTOCOL,
                            "side": self.side,
                            "extensions": extensions,
                        }
                    )
                    + b"\n"
                )
                await writer.drain()
            self.link_generation = uuid.uuid4().hex
            self.link_reader = reader
            self.link_writer = writer
            self.link_ready.set()
            self.log("peer link established")
            await self._link_read_loop(reader)
        finally:
            if self.link_writer is writer:
                self.link_ready.clear()
                self.link_reader = None
                self.link_writer = None
                self.peer_artifacts = False
                self.peer_artifact_chunk_bytes = ARTIFACT_CHUNK_BYTES
                self.link_generation = None
                await self._fail_link_state("peer link disconnected")
            writer.close()
            try:
                await writer.wait_closed()
            except OSError:
                pass
            self.log("peer link closed")

    async def _read_frame(self, reader: asyncio.StreamReader) -> dict[str, Any]:
        line = await reader.readline()
        if not line:
            raise EOFError("peer closed")
        if len(line) > MAX_FRAME_BYTES:
            raise BridgeError("bridge frame exceeds limit")
        value = json.loads(line)
        if not isinstance(value, dict):
            raise BridgeError("bridge frame must be an object")
        return value

    async def _send_frame(self, frame: dict[str, Any]) -> None:
        if not self.link_writer or not self.link_ready.is_set():
            raise BridgeError("peer link is not connected")
        data = _json_bytes(frame) + b"\n"
        if len(data) > MAX_FRAME_BYTES:
            raise BridgeError("bridge frame exceeds limit")
        async with self.link_write_lock:
            assert self.link_writer is not None
            self.link_writer.write(data)
            await self.link_writer.drain()

    async def _link_read_loop(self, reader: asyncio.StreamReader) -> None:
        while True:
            frame = await self._read_frame(reader)
            await self._handle_frame(frame)

    async def _handle_frame(self, frame: dict[str, Any]) -> None:
        kind = frame.get("type")
        if kind == "open":
            self._background(self._handle_remote_open(frame))
            return
        if kind in {"open_ok", "open_error"}:
            stream = self.streams.get(str(frame.get("stream")))
            if stream and stream.opened and not stream.opened.done():
                if kind == "open_ok":
                    stream.opened.set_result(None)
                else:
                    stream.opened.set_exception(BridgeError(str(frame.get("message", "open failed"))))
            return
        if kind == "data":
            await self._handle_stream_data(frame)
            return
        if kind == "data_ok":
            self._handle_stream_data_ok(frame)
            return
        if kind == "eof":
            await self._handle_stream_eof(str(frame.get("stream")))
            return
        if kind == "close":
            self._background(self._close_stream(str(frame.get("stream")), remote=True))
            return
        if kind == "registry_request":
            self._background(self._handle_registry_request(frame))
            return
        if kind == "registry_response":
            request_id = str(frame.get("request"))
            future = self.pending_registry.pop(request_id, None)
            if future and not future.done():
                if frame.get("ok"):
                    future.set_result(frame.get("result"))
                else:
                    future.set_exception(BridgeError(str(frame.get("message", "registry request failed"))))
            return
        if isinstance(kind, str) and kind.startswith("artifact_"):
            if not self.peer_artifacts:
                self.log("ignored artifact frame without negotiated artifacts/1 extension")
                return
            if kind == "artifact_begin":
                self._background(self._handle_artifact_begin(frame))
            elif kind == "artifact_chunk":
                self._handle_artifact_chunk(frame)
            elif kind == "artifact_end":
                self._handle_artifact_end(frame)
            elif kind == "artifact_cancel":
                artifact_id = frame.get("artifact")
                state = (
                    self.receiving_artifacts.get(artifact_id)
                    if isinstance(artifact_id, str)
                    else None
                )
                if state and frame.get("stream") == state.stream_id:
                    self._schedule_artifact_abort(state, "sender cancelled transfer")
            elif kind in {
                "artifact_ready",
                "artifact_chunk_ok",
                "artifact_ok",
                "artifact_error",
            }:
                self._handle_artifact_reply(frame)
            else:
                self.log(f"ignored unknown artifact frame type: {kind}")
            return
        raise BridgeError(f"unknown bridge frame type: {kind!r}")

    async def _handle_local(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            request = await asyncio.wait_for(self._read_frame(reader), timeout=10)
            op = request.get("op")
            if op == "connect":
                target = request.get("target")
                if not isinstance(target, str) or not ID_PATTERN.fullmatch(target):
                    raise BridgeError("connect requires a valid registered target id")
                artifact_inbox = self._validate_artifact_inbox(request.get("artifactInbox"))
                await self._serve_local_stream(
                    reader, writer, target, artifact_inbox=artifact_inbox
                )
                return
            if op == "publish":
                result = await self._publish_local_artifact(request)
                writer.write(_json_bytes({"ok": True, "result": result}) + b"\n")
                await writer.drain()
                return
            if op == "registry":
                result = await self._registry_query(
                    str(request.get("scope", "remote")),
                    str(request.get("action", "list")),
                    request.get("arguments") if isinstance(request.get("arguments"), dict) else {},
                )
                writer.write(_json_bytes({"ok": True, "result": result}) + b"\n")
                await writer.drain()
                return
            raise BridgeError(f"unknown local operation: {op!r}")
        except Exception as exc:
            try:
                writer.write(_json_bytes({"ok": False, "message": str(exc)}) + b"\n")
                await writer.drain()
            except OSError:
                pass
        finally:
            if not writer.is_closing():
                writer.close()
                try:
                    await writer.wait_closed()
                except OSError:
                    pass

    def _validate_artifact_inbox(self, value: Any) -> Path | None:
        if value is None or value == "":
            return None
        if not isinstance(value, str):
            raise BridgeError("artifactInbox must be a local absolute directory path")
        inbox = Path(value).expanduser()
        if not inbox.is_absolute() or not inbox.is_dir():
            raise BridgeError("artifactInbox must be an existing local absolute directory")
        resolved = inbox.resolve()
        if not any(resolved == root or resolved.is_relative_to(root) for root in self.allowed_artifact_roots):
            raise BridgeError("artifactInbox is outside Operator-authorized workspace roots")
        return resolved

    @staticmethod
    def _safe_artifact_name(value: Any) -> str:
        if (
            not isinstance(value, str)
            or not value
            or len(value.encode("utf-8")) > 240
            or len(value.encode("utf-16-le")) // 2 > 240
        ):
            raise BridgeError(
                "artifact name must be non-empty and fit cross-platform component limits"
            )
        if value in {".", ".."} or any(
            ord(character) < 32 or character in '/\\\x00:<>"|?*'
            for character in value
        ):
            raise BridgeError("artifact name must be one cross-platform safe filename component")
        lowered = value.casefold()
        if any(encoded in lowered for encoded in ("%2f", "%5c", "%2e")):
            raise BridgeError("artifact name must not contain encoded path syntax")
        if value != value.rstrip(" ."):
            raise BridgeError("artifact name must not end with a dot or space")
        windows_stem = value.split(".", 1)[0].upper()
        reserved = {
            "CON",
            "PRN",
            "AUX",
            "NUL",
            "CONIN$",
            "CONOUT$",
            *{f"COM{i}" for i in range(1, 10)},
            *{f"LPT{i}" for i in range(1, 10)},
        }
        if windows_stem in reserved:
            raise BridgeError("artifact name is reserved on Windows")
        return value

    async def _publish_local_artifact(self, request: dict[str, Any]) -> dict[str, Any]:
        if not self.peer_artifacts:
            raise BridgeError("peer does not support artifacts/1")
        token = request.get("token")
        if not isinstance(token, str):
            raise BridgeError("artifact publish requires a session token")
        stream_id = self.artifact_publishers.get(token)
        stream = self.streams.get(stream_id) if stream_id else None
        shared_backend = self.shared_artifact_publishers.get(token)
        if stream and not stream.closed.is_set() and stream.artifact_stage is not None:
            artifact_stage = stream.artifact_stage
            artifact_max_bytes = stream.artifact_max_bytes
        elif shared_backend is not None and shared_backend.artifact_stage is not None:
            stream = shared_backend.artifact_stream()
            if stream is None or stream.closed.is_set():
                raise BridgeError("shared artifact publish has no active client request")
            artifact_stage = shared_backend.artifact_stage
            artifact_max_bytes = shared_backend.artifact_max_bytes
        else:
            raise BridgeError("artifact publish session is unavailable")
        relative_name = self._safe_artifact_name(request.get("relativePath"))
        display_name = self._safe_artifact_name(request.get("name", relative_name))
        media_type = request.get("mediaType")
        if media_type is not None and (
            not isinstance(media_type, str)
            or not media_type
            or len(media_type) > 255
            or any(character in media_type for character in ("\r", "\n", "\x00"))
        ):
            raise BridgeError("artifact mediaType is invalid")
        source = artifact_stage / relative_name
        snapshot, size, sha256, source_identity = await asyncio.to_thread(
            self._snapshot_artifact,
            source,
            artifact_max_bytes,
        )
        try:
            delivered = await self._send_artifact_snapshot(
                stream,
                snapshot,
                display_name,
                media_type,
                size,
                sha256,
            )
        finally:
            snapshot.close()
        await asyncio.to_thread(
            self._unlink_source_if_same,
            source,
            source_identity,
        )
        artifact = {
            "type": "resource_link",
            "uri": delivered["uri"],
            "name": display_name,
            "size": size,
            "_meta": {
                ARTIFACT_META_KEY: {
                    "delivered": True,
                    "protocol": "artifacts/1",
                    "sha256": sha256,
                    "localPath": delivered["path"],
                }
            },
        }
        if media_type is not None:
            artifact["mimeType"] = media_type
        return {"artifact": artifact}

    def _snapshot_artifact(
        self,
        source: Path,
        max_bytes: int,
    ) -> tuple[Any, int, str, tuple[int, int, int]]:
        if source.parent.resolve() != source.parent or source.parent.name == "":
            raise BridgeError("artifact staging directory is invalid")
        if source.is_symlink():
            raise BridgeError("published artifact must not be a symbolic link")
        flags = os.O_RDONLY | getattr(os, "O_BINARY", 0)
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            descriptor = os.open(source, flags)
        except OSError as exc:
            raise BridgeError("published artifact is unavailable") from exc
        source_handle = os.fdopen(descriptor, "rb", closefd=True)
        snapshot: Any | None = None
        try:
            metadata = os.fstat(source_handle.fileno())
            if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink > 1:
                raise BridgeError("published artifact must be one regular unlinked file")
            self.artifact_spool_root.mkdir(parents=True, mode=0o700, exist_ok=True)
            snapshot = tempfile.TemporaryFile(mode="w+b", dir=self.artifact_spool_root)
            digest = hashlib.sha256()
            total = 0
            while True:
                chunk = source_handle.read(ARTIFACT_CHUNK_BYTES)
                if not chunk:
                    break
                total += len(chunk)
                if total > max_bytes or total > self.max_artifact_bytes:
                    raise BridgeError("published artifact exceeds configured size limit")
                snapshot.write(chunk)
                digest.update(chunk)
            snapshot.flush()
            snapshot.seek(0)
            identity = (
                metadata.st_dev,
                metadata.st_ino,
                metadata.st_ctime_ns,
            )
            return snapshot, total, digest.hexdigest(), identity
        except Exception:
            if snapshot is not None:
                snapshot.close()
            raise
        finally:
            source_handle.close()

    @staticmethod
    def _unlink_source_if_same(
        source: Path,
        expected: tuple[int, int, int],
    ) -> None:
        try:
            metadata = source.lstat()
        except OSError:
            return
        actual = (metadata.st_dev, metadata.st_ino, metadata.st_ctime_ns)
        if actual == expected and stat.S_ISREG(metadata.st_mode):
            try:
                source.unlink()
            except OSError:
                pass

    async def _send_artifact_snapshot(
        self,
        stream: StreamState,
        snapshot: Any,
        name: str,
        media_type: str | None,
        size: int,
        sha256: str,
    ) -> dict[str, Any]:
        artifact_id = f"artifact-{uuid.uuid4().hex}"
        loop = asyncio.get_running_loop()
        waiter = ArtifactTransferWaiter(
            stream_id=stream.stream_id,
            ready=loop.create_future(),
            done=loop.create_future(),
            expected_size=size,
            expected_sha256=sha256,
        )
        self.pending_artifacts[artifact_id] = waiter
        completed = False
        try:
            await self._send_frame(
                {
                    "type": "artifact_begin",
                    "stream": stream.stream_id,
                    "artifact": artifact_id,
                    "name": name,
                    "mediaType": media_type,
                    "size": size,
                    "sha256": sha256,
                }
            )
            await asyncio.wait_for(waiter.ready, timeout=10)
            if waiter.phase != "ready":
                raise BridgeError("artifact receiver did not enter ready phase")
            waiter.phase = "sending"
            sequence = 0
            while True:
                chunk = await asyncio.to_thread(
                    snapshot.read,
                    self.peer_artifact_chunk_bytes,
                )
                if not chunk:
                    break
                waiter.chunk_ack = loop.create_future()
                await self._send_frame(
                    {
                        "type": "artifact_chunk",
                        "stream": stream.stream_id,
                        "artifact": artifact_id,
                        "sequence": sequence,
                        "data": base64.b64encode(chunk).decode("ascii"),
                    }
                )
                acknowledged = await asyncio.wait_for(waiter.chunk_ack, timeout=30)
                if acknowledged != sequence:
                    raise BridgeError("artifact receiver acknowledged the wrong chunk")
                waiter.chunk_ack = None
                sequence += 1
            waiter.phase = "end_sent"
            await self._send_frame(
                {
                    "type": "artifact_end",
                    "stream": stream.stream_id,
                    "artifact": artifact_id,
                    "chunks": sequence,
                    "size": size,
                    "sha256": sha256,
                }
            )
            result = await asyncio.wait_for(waiter.done, timeout=300)
            completed = True
            return result
        finally:
            self.pending_artifacts.pop(artifact_id, None)
            if not completed:
                try:
                    await self._send_frame(
                        {
                            "type": "artifact_cancel",
                            "stream": stream.stream_id,
                            "artifact": artifact_id,
                        }
                    )
                except BridgeError:
                    pass

    async def _serve_local_stream(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        target: str,
        *,
        artifact_inbox: Path | None,
    ) -> None:
        await asyncio.wait_for(self.link_ready.wait(), timeout=10)
        stream_id = f"{self.side}-{uuid.uuid4().hex}"
        loop = asyncio.get_running_loop()
        stream = StreamState(
            stream_id=stream_id,
            socket_writer=writer,
            opened=loop.create_future(),
            target=target,
            artifact_inbox=artifact_inbox,
        )
        self.streams[stream_id] = stream
        consumer = asyncio.create_task(self._consume_stream_input(stream))
        stream.tasks.add(consumer)
        await self._send_frame({"type": "open", "stream": stream_id, "target": target})
        handshake_sent = False
        try:
            await asyncio.wait_for(stream.opened, timeout=10)
            writer.write(_json_bytes({"ok": True, "stream": stream_id}) + b"\n")
            await writer.drain()
            handshake_sent = True
            pump = asyncio.create_task(self._pump_local_input(stream, reader))
            stream.tasks.add(pump)
            closed_wait = asyncio.create_task(stream.closed.wait())
            done, pending = await asyncio.wait({pump, closed_wait}, return_when=asyncio.FIRST_COMPLETED)
            if pump in done and not stream.closed.is_set():
                error = pump.exception()
                if error is not None:
                    raise error
                await self._send_frame({"type": "eof", "stream": stream_id})
                try:
                    await asyncio.wait_for(
                        stream.closed.wait(),
                        timeout=STREAM_EOF_GRACE_SECONDS,
                    )
                except TimeoutError:
                    await self._close_stream(stream_id, remote=False)
            for task in pending:
                task.cancel()
        except Exception:
            if handshake_sent:
                await self._close_stream(stream_id, remote=False)
                return
            removed = self.streams.pop(stream_id, None)
            if removed:
                removed.closed.set()
                for task in tuple(removed.tasks):
                    if not task.done():
                        task.cancel()
            try:
                await self._send_frame({"type": "close", "stream": stream_id})
            except BridgeError:
                pass
            raise

    async def _send_jsonrpc_to_stream(
        self, stream: StreamState, message: dict[str, Any]
    ) -> None:
        data = _json_bytes(message) + b"\n"
        async with stream.send_lock:
            await self._send_stream_data(stream, data)

    async def _send_stream_data(self, stream: StreamState, data: bytes) -> None:
        if stream.outbound_ack is not None:
            raise BridgeError("stream already has unacknowledged data")
        sequence = stream.outbound_sequence
        acknowledgement = asyncio.get_running_loop().create_future()
        stream.outbound_ack = acknowledgement
        try:
            await self._send_frame(
                {
                    "type": "data",
                    "stream": stream.stream_id,
                    "sequence": sequence,
                    "data": base64.b64encode(data).decode("ascii"),
                }
            )
            received = await asyncio.wait_for(
                acknowledgement,
                timeout=STREAM_DATA_ACK_TIMEOUT_SECONDS,
            )
            if received != sequence:
                raise BridgeError("peer acknowledged the wrong stream data sequence")
            stream.outbound_sequence += 1
        except Exception:
            self._background(self._close_stream(stream.stream_id, remote=False))
            raise
        finally:
            if stream.outbound_ack is acknowledgement:
                stream.outbound_ack = None

    async def _pump_local_input(self, stream: StreamState, reader: asyncio.StreamReader) -> None:
        while True:
            data = await reader.read(BUFFER_SIZE)
            if not data:
                return
            await self._send_stream_data(stream, data)

    async def _handle_remote_open(self, frame: dict[str, Any]) -> None:
        stream_id = frame.get("stream")
        target = frame.get("target")
        if (
            not isinstance(stream_id, str)
            or not ID_PATTERN.fullmatch(stream_id)
            or not isinstance(target, str)
            or not ID_PATTERN.fullmatch(target)
        ):
            await self._send_frame(
                {
                    "type": "open_error",
                    "stream": stream_id if isinstance(stream_id, str) else "",
                    "message": "invalid open frame",
                }
            )
            return
        if stream_id in self.streams:
            await self._send_frame(
                {
                    "type": "open_error",
                    "stream": stream_id,
                    "message": "duplicate stream id",
                }
            )
            return
        try:
            entry = self.registry.launch(target)
        except BridgeError as exc:
            await self._send_frame(
                {"type": "open_error", "stream": stream_id, "message": str(exc)}
            )
            return
        process_config = entry.get("process") or {}
        if process_config.get("multiProcessAllowed") is False:
            stream = StreamState(stream_id=stream_id, target=target)
            self.streams[stream_id] = stream
            backend = self.shared_backends.get(target)
            if backend is None:
                backend = SharedBackend(self, target, entry)
                self.shared_backends[target] = backend
            else:
                backend.refresh_entry_if_exited(entry)
            try:
                await backend.attach(stream)
            except asyncio.CancelledError:
                self.streams.pop(stream_id, None)
                raise
            except Exception as exc:
                self.streams.pop(stream_id, None)
                self.log(
                    f"failed to start shared registered MCP {target}: "
                    f"{type(exc).__name__}: {exc}"
                )
                await self._send_frame(
                    {
                        "type": "open_error",
                        "stream": stream_id,
                        "message": "registered MCP failed to start; inspect local bridge diagnostics",
                    }
                )
                return
            await self._send_frame({"type": "open_ok", "stream": stream_id})
            consumer = asyncio.create_task(backend.consume_client_input(stream))
            stream.tasks.add(consumer)
            return
        artifact_config = entry.get("artifactDelivery", {"enabled": False})
        artifact_enabled = bool(artifact_config.get("enabled")) and self.peer_artifacts
        artifact_token: str | None = None
        artifact_stage: Path | None = None
        environment = os.environ.copy()
        for key in ARTIFACT_ENV_KEYS:
            environment.pop(key, None)
        environment.update(entry.get("env", {}))
        if artifact_enabled:
            try:
                generation = self.link_generation or "disconnected"
                artifact_stage = self.artifact_spool_root / generation / stream_id
                artifact_stage.mkdir(parents=True, mode=0o700, exist_ok=False)
                try:
                    artifact_stage.chmod(0o700)
                except OSError:
                    pass
                artifact_token = secrets.token_urlsafe(32)
                environment.update(
                    {
                        "WIN_WSL_MCP_BRIDGE_ARTIFACT_STAGE": str(artifact_stage),
                        "WIN_WSL_MCP_BRIDGE_ARTIFACT_TOKEN": artifact_token,
                        "WIN_WSL_MCP_BRIDGE_ARTIFACT_LOCAL_HOST": self.local_host,
                        "WIN_WSL_MCP_BRIDGE_ARTIFACT_LOCAL_PORT": str(self.local_port),
                        "WIN_WSL_MCP_BRIDGE_ARTIFACT_PROTOCOL": "artifacts/1",
                        "WIN_WSL_MCP_BRIDGE_ARTIFACT_PYTHON": sys.executable,
                        "WIN_WSL_MCP_BRIDGE_ARTIFACT_PUBLISHER": str(
                            Path(__file__).with_name("bridge_publisher.py").resolve()
                        ),
                    }
                )
            except OSError as exc:
                self.log(
                    f"failed to prepare artifact staging for {target}: "
                    f"{type(exc).__name__}: {exc}"
                )
                await self._send_frame(
                    {
                        "type": "open_error",
                        "stream": stream_id,
                        "message": "registered MCP artifact staging failed; inspect local bridge diagnostics",
                    }
                )
                return
        stream = StreamState(
            stream_id=stream_id,
            target=target,
            artifact_stage=artifact_stage,
            artifact_token=artifact_token,
            artifact_max_bytes=min(
                int(artifact_config.get("maxBytes", DEFAULT_MAX_ARTIFACT_BYTES)),
                self.max_artifact_bytes,
            ),
        )
        self.streams[stream_id] = stream
        if artifact_token is not None:
            self.artifact_publishers[artifact_token] = stream_id
        try:
            process = await asyncio.create_subprocess_exec(
                entry["command"],
                *entry.get("args", []),
                cwd=entry.get("cwd") or None,
                env=environment,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stream.process = process
        except Exception as exc:
            self.streams.pop(stream_id, None)
            if artifact_token is not None:
                self.artifact_publishers.pop(artifact_token, None)
            if artifact_stage is not None:
                shutil.rmtree(artifact_stage, ignore_errors=True)
            self.log(f"failed to start registered MCP {target}: {type(exc).__name__}: {exc}")
            await self._send_frame(
                {
                    "type": "open_error",
                    "stream": stream_id,
                    "message": "registered MCP failed to start; inspect local bridge diagnostics",
                }
            )
            return
        await self._send_frame({"type": "open_ok", "stream": stream_id})
        consumer = asyncio.create_task(self._consume_stream_input(stream))
        stdout_task = asyncio.create_task(self._pump_process_output(stream))
        stderr_task = asyncio.create_task(self._pump_process_stderr(stream, target))
        wait_task = asyncio.create_task(
            self._wait_process(stream, stdout_task, stderr_task)
        )
        stream.tasks.update({consumer, stdout_task, stderr_task, wait_task})

    async def _pump_process_output(self, stream: StreamState) -> None:
        assert stream.process and stream.process.stdout
        while True:
            data = await stream.process.stdout.read(BUFFER_SIZE)
            if not data:
                return
            await self._send_stream_data(stream, data)

    async def _pump_process_stderr(self, stream: StreamState, target: str) -> None:
        assert stream.process and stream.process.stderr
        while True:
            data = await stream.process.stderr.read(BUFFER_SIZE)
            if not data:
                return
            text = data.decode("utf-8", errors="replace").rstrip()
            self.log(f"{target} stderr: {text}")

    async def _wait_process(
        self,
        stream: StreamState,
        stdout_task: asyncio.Task[Any],
        stderr_task: asyncio.Task[Any],
    ) -> None:
        assert stream.process
        code = await stream.process.wait()
        await asyncio.gather(stdout_task, stderr_task, return_exceptions=True)
        try:
            await self._send_frame(
                {"type": "close", "stream": stream.stream_id, "exitCode": code}
            )
        except BridgeError:
            pass
        await self._close_stream(stream.stream_id, remote=True)

    async def _consume_stream_input(self, stream: StreamState) -> None:
        try:
            while True:
                item = await stream.inbound.get()
                if item is None:
                    if stream.process and stream.process.stdin:
                        stream.process.stdin.close()
                    elif stream.socket_writer:
                        try:
                            stream.socket_writer.write_eof()
                        except (AttributeError, OSError):
                            pass
                    return
                sequence, data = item
                if stream.socket_writer:
                    stream.socket_writer.write(data)
                    await stream.socket_writer.drain()
                elif stream.process and stream.process.stdin:
                    stream.process.stdin.write(data)
                    await stream.process.stdin.drain()
                else:
                    raise BridgeError("stream has no downstream input")
                await self._send_frame(
                    {
                        "type": "data_ok",
                        "stream": stream.stream_id,
                        "sequence": sequence,
                    }
                )
        except (BridgeError, BrokenPipeError, ConnectionError, OSError) as exc:
            self.log(f"stream {stream.stream_id} input closed: {exc}")
            self._background(self._close_stream(stream.stream_id, remote=False))

    async def _handle_artifact_begin(self, frame: dict[str, Any]) -> None:
        artifact_id = frame.get("artifact")
        stream_id = frame.get("stream")
        artifact_dir: Path | None = None
        try:
            if (
                not isinstance(artifact_id, str)
                or not ID_PATTERN.fullmatch(artifact_id)
                or artifact_id in self.receiving_artifacts
                or not isinstance(stream_id, str)
            ):
                raise BridgeError("invalid artifact identity")
            stream = self.streams.get(stream_id)
            if not stream or stream.artifact_inbox is None:
                raise BridgeError("artifact workspace delivery is not configured")
            active_for_stream = sum(
                1
                for item in self.receiving_artifacts.values()
                if item.stream_id == stream_id
            )
            reserved_bytes = sum(
                item.expected_size for item in self.receiving_artifacts.values()
            )
            if (
                len(self.receiving_artifacts) >= MAX_CONCURRENT_ARTIFACTS
                or active_for_stream >= MAX_CONCURRENT_ARTIFACTS // 2
            ):
                raise BridgeError("artifact receive concurrency limit reached")
            name = self._safe_artifact_name(frame.get("name"))
            media_type = frame.get("mediaType")
            if media_type is not None and (
                not isinstance(media_type, str)
                or not media_type
                or len(media_type) > 255
                or any(character in media_type for character in ("\r", "\n", "\x00"))
            ):
                raise BridgeError("invalid artifact media type")
            size = frame.get("size")
            sha256 = frame.get("sha256")
            if (
                not isinstance(size, int)
                or isinstance(size, bool)
                or size < 0
                or size > self.max_artifact_bytes
                or reserved_bytes + size > MAX_RESERVED_ARTIFACT_BYTES
                or not isinstance(sha256, str)
                or re.fullmatch(r"[0-9a-f]{64}", sha256) is None
            ):
                raise BridgeError("invalid artifact size or digest")
            inbox = self._validate_artifact_inbox(str(stream.artifact_inbox))
            assert inbox is not None
            artifact_root = inbox / ".mcp-artifacts"
            if artifact_root.exists() and (
                artifact_root.is_symlink() or not artifact_root.is_dir()
            ):
                raise BridgeError("artifact workspace root is unsafe")
            artifact_root.mkdir(mode=0o700, exist_ok=True)
            if artifact_root.resolve().parent != inbox:
                raise BridgeError("artifact workspace root escaped its inbox")
            artifact_dir = artifact_root / artifact_id
            artifact_dir.mkdir(mode=0o700, exist_ok=False)
            final_path = artifact_dir / name
            temp_path = artifact_dir / ".partial"
            partial_flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_BINARY", 0)
            handle = os.fdopen(os.open(temp_path, partial_flags, 0o600), "wb")
            state = ArtifactReceiveState(
                stream_id=stream_id,
                artifact_id=artifact_id,
                name=name,
                media_type=media_type,
                expected_size=size,
                temp_path=temp_path,
                final_path=final_path,
                handle=handle,
                end_sha256=sha256,
            )
            self.receiving_artifacts[artifact_id] = state
            state.task = self._background(self._receive_artifact(state))
            await self._send_frame(
                {
                    "type": "artifact_ready",
                    "stream": stream_id,
                    "artifact": artifact_id,
                }
            )
        except Exception as exc:
            state = (
                self.receiving_artifacts.get(artifact_id)
                if isinstance(artifact_id, str)
                else None
            )
            if state is not None:
                await self._abort_received_artifact(
                    state,
                    "artifact begin response failed",
                )
            elif artifact_dir is not None:
                shutil.rmtree(artifact_dir, ignore_errors=True)
            try:
                await self._send_frame(
                    {
                        "type": "artifact_error",
                        "stream": stream_id if isinstance(stream_id, str) else "",
                        "artifact": artifact_id if isinstance(artifact_id, str) else "",
                        "message": "artifact delivery was rejected",
                    }
                )
            except BridgeError:
                pass
            self.log(f"artifact begin rejected: {type(exc).__name__}: {exc}")

    def _handle_artifact_chunk(self, frame: dict[str, Any]) -> None:
        artifact_id = frame.get("artifact")
        state = self.receiving_artifacts.get(artifact_id) if isinstance(artifact_id, str) else None
        if not state:
            return
        try:
            if state.phase != "receiving":
                raise BridgeError("artifact chunk arrived after terminal metadata")
            sequence = frame.get("sequence")
            if sequence != state.next_sequence or frame.get("stream") != state.stream_id:
                raise BridgeError("artifact chunk sequence is invalid")
            data = base64.b64decode(frame.get("data", ""), validate=True)
            if len(data) > ARTIFACT_CHUNK_BYTES:
                raise BridgeError("artifact chunk exceeds negotiated limit")
            state.queue.put_nowait((sequence, data))
            state.next_sequence += 1
        except Exception as exc:
            self._schedule_artifact_abort(state, str(exc))

    def _handle_artifact_end(self, frame: dict[str, Any]) -> None:
        artifact_id = frame.get("artifact")
        state = self.receiving_artifacts.get(artifact_id) if isinstance(artifact_id, str) else None
        if not state:
            return
        try:
            if state.phase != "receiving":
                raise BridgeError("duplicate or out-of-order artifact end")
            if (
                frame.get("stream") != state.stream_id
                or frame.get("chunks") != state.next_sequence
                or frame.get("size") != state.expected_size
                or frame.get("sha256") != state.end_sha256
            ):
                raise BridgeError("artifact end metadata does not match")
            state.phase = "ending"
            state.queue.put_nowait(None)
        except asyncio.QueueFull:
            state.end_task = self._background(state.queue.put(None))
        except Exception as exc:
            self._schedule_artifact_abort(state, str(exc))

    def _handle_artifact_reply(self, frame: dict[str, Any]) -> None:
        artifact_id = frame.get("artifact")
        waiter = self.pending_artifacts.get(artifact_id) if isinstance(artifact_id, str) else None
        if not waiter or frame.get("stream") != waiter.stream_id:
            return
        kind = frame.get("type")
        if kind == "artifact_ready":
            if waiter.phase != "begin_sent" or waiter.ready.done():
                self._fail_artifact_waiter(waiter, "out-of-order artifact_ready")
                return
            waiter.phase = "ready"
            waiter.ready.set_result(frame)
            return
        if kind == "artifact_chunk_ok":
            if (
                waiter.phase != "sending"
                or waiter.chunk_ack is None
                or waiter.chunk_ack.done()
                or not isinstance(frame.get("sequence"), int)
            ):
                self._fail_artifact_waiter(waiter, "out-of-order artifact_chunk_ok")
                return
            waiter.chunk_ack.set_result(frame["sequence"])
            return
        if kind == "artifact_ok":
            if waiter.phase != "end_sent" or waiter.done.done():
                self._fail_artifact_waiter(waiter, "out-of-order artifact_ok")
                return
            uri = frame.get("uri")
            path = frame.get("path")
            if (
                not isinstance(uri, str)
                or not uri.startswith("file:")
                or not isinstance(path, str)
                or frame.get("size") != waiter.expected_size
                or frame.get("sha256") != waiter.expected_sha256
            ):
                self._fail_artifact_waiter(waiter, "peer returned invalid artifact receipt")
                return
            waiter.phase = "completed"
            waiter.done.set_result(
                {
                    "uri": uri,
                    "path": path,
                    "size": frame.get("size"),
                    "sha256": frame.get("sha256"),
                }
            )
            return
        if kind == "artifact_error":
            self._fail_artifact_waiter(
                waiter,
                str(frame.get("message", "artifact delivery failed")),
            )

    @staticmethod
    def _fail_artifact_waiter(
        waiter: ArtifactTransferWaiter,
        message: str,
    ) -> None:
        if waiter.phase == "completed":
            return
        waiter.phase = "failed"
        error = BridgeError(message)
        if not waiter.ready.done():
            waiter.ready.set_exception(error)
        elif waiter.chunk_ack is not None and not waiter.chunk_ack.done():
            waiter.chunk_ack.set_exception(error)
        elif not waiter.done.done():
            waiter.done.set_exception(error)

    async def _receive_artifact(self, state: ArtifactReceiveState) -> None:
        try:
            while True:
                item = await state.queue.get()
                if item is None:
                    break
                sequence, data = item
                state.received += len(data)
                if state.received > state.expected_size:
                    raise BridgeError("artifact byte count exceeds announcement")
                state.digest.update(data)
                await asyncio.to_thread(state.handle.write, data)
                await self._send_frame(
                    {
                        "type": "artifact_chunk_ok",
                        "stream": state.stream_id,
                        "artifact": state.artifact_id,
                        "sequence": sequence,
                    }
                )
            if (
                state.received != state.expected_size
                or state.digest.hexdigest() != state.end_sha256
            ):
                raise BridgeError("artifact integrity verification failed")
            await asyncio.to_thread(state.handle.flush)
            await asyncio.to_thread(os.fsync, state.handle.fileno())
            state.handle.close()
            if state.phase != "ending":
                raise BridgeError("artifact reached commit before terminal metadata")
            state.phase = "committing"
            state.commit_task = asyncio.create_task(
                asyncio.to_thread(
                    self._commit_artifact_no_overwrite,
                    state.temp_path,
                    state.final_path,
                )
            )
            await asyncio.shield(state.commit_task)
            if state.phase == "aborting":
                try:
                    state.final_path.unlink()
                except OSError:
                    pass
                raise BridgeError("artifact commit was cancelled")
            state.phase = "completed"
            try:
                state.final_path.chmod(0o600)
            except OSError:
                pass
            self.receiving_artifacts.pop(state.artifact_id, None)
            await self._send_frame(
                {
                    "type": "artifact_ok",
                    "stream": state.stream_id,
                    "artifact": state.artifact_id,
                    "uri": state.final_path.as_uri(),
                    "path": str(state.final_path),
                    "size": state.received,
                    "sha256": state.digest.hexdigest(),
                }
            )
        except asyncio.CancelledError:
            if state.commit_task is not None:
                try:
                    await asyncio.shield(state.commit_task)
                except Exception:
                    pass
            if state.phase != "completed":
                try:
                    state.final_path.unlink()
                except OSError:
                    pass
            self._cleanup_received_artifact(state)
            raise
        except Exception as exc:
            self.receiving_artifacts.pop(state.artifact_id, None)
            if state.phase != "completed":
                try:
                    state.final_path.unlink()
                except OSError:
                    pass
            self._cleanup_received_artifact(state)
            try:
                await self._send_frame(
                    {
                        "type": "artifact_error",
                        "stream": state.stream_id,
                        "artifact": state.artifact_id,
                        "message": "artifact delivery failed integrity or confinement checks",
                    }
                )
            except BridgeError:
                pass
            self.log(f"artifact receive failed: {type(exc).__name__}: {exc}")

    def _schedule_artifact_abort(
        self,
        state: ArtifactReceiveState,
        reason: str,
    ) -> None:
        if state.phase in {"aborting", "completed"}:
            return
        state.phase = "aborting"
        state.abort_task = self._background(
            self._abort_received_artifact(state, reason)
        )

    async def _abort_received_artifact(
        self,
        state: ArtifactReceiveState,
        reason: str,
    ) -> None:
        if state.phase == "completed":
            return
        state.phase = "aborting"
        current = asyncio.current_task()
        if state.task and state.task is not current and not state.task.done():
            state.task.cancel()
            await asyncio.gather(state.task, return_exceptions=True)
        if state.commit_task is not None and not state.commit_task.done():
            try:
                await asyncio.shield(state.commit_task)
            except Exception:
                pass
        try:
            state.final_path.unlink()
        except OSError:
            pass
        self.receiving_artifacts.pop(state.artifact_id, None)
        self._cleanup_received_artifact(state)
        try:
            await self._send_frame(
                {
                    "type": "artifact_error",
                    "stream": state.stream_id,
                    "artifact": state.artifact_id,
                    "message": "artifact delivery failed integrity or confinement checks",
                }
            )
        except BridgeError:
            pass
        self.log(f"artifact receive aborted: {reason}")

    @staticmethod
    def _commit_artifact_no_overwrite(temp_path: Path, final_path: Path) -> None:
        if temp_path.parent.resolve() != final_path.parent.resolve():
            raise BridgeError("artifact commit directory changed")
        metadata = os.lstat(temp_path)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
            raise BridgeError("artifact partial changed before commit")
        if os.name == "nt":
            os.rename(temp_path, final_path)
        else:
            os.link(temp_path, final_path)
            temp_path.unlink()

    @staticmethod
    def _cleanup_received_artifact(state: ArtifactReceiveState) -> None:
        if state.end_task and not state.end_task.done():
            state.end_task.cancel()
        try:
            state.handle.close()
        except OSError:
            pass
        try:
            state.temp_path.unlink()
        except OSError:
            pass
        try:
            state.temp_path.parent.rmdir()
        except OSError:
            pass

    async def _handle_stream_data(self, frame: dict[str, Any]) -> None:
        stream_id = frame.get("stream")
        if not isinstance(stream_id, str):
            return
        stream = self.streams.get(stream_id)
        if not stream:
            return
        sequence = frame.get("sequence")
        if (
            not isinstance(sequence, int)
            or isinstance(sequence, bool)
            or sequence != stream.inbound_sequence
        ):
            self.log(f"stream {stream_id} received an invalid data sequence; closing it")
            self._background(self._close_stream(stream_id, remote=False))
            return
        try:
            data = base64.b64decode(frame.get("data", ""), validate=True)
            stream.inbound.put_nowait((sequence, data))
        except (ValueError, TypeError, asyncio.QueueFull):
            self.log(f"stream {stream_id} received invalid or excessive data; closing it")
            self._background(self._close_stream(stream_id, remote=False))
            return
        stream.inbound_sequence += 1

    def _handle_stream_data_ok(self, frame: dict[str, Any]) -> None:
        stream_id = frame.get("stream")
        stream = self.streams.get(stream_id) if isinstance(stream_id, str) else None
        if not stream:
            return
        acknowledgement = stream.outbound_ack
        sequence = frame.get("sequence")
        if (
            acknowledgement is None
            or acknowledgement.done()
            or not isinstance(sequence, int)
            or isinstance(sequence, bool)
            or sequence != stream.outbound_sequence
        ):
            self.log(f"stream {stream.stream_id} received an invalid data acknowledgement")
            self._background(self._close_stream(stream.stream_id, remote=False))
            return
        acknowledgement.set_result(sequence)

    async def _handle_stream_eof(self, stream_id: str) -> None:
        stream = self.streams.get(stream_id)
        if not stream:
            return
        try:
            stream.inbound.put_nowait(None)
        except asyncio.QueueFull:
            task = self._background(stream.inbound.put(None))
            stream.tasks.add(task)
            task.add_done_callback(stream.tasks.discard)

    async def _close_stream(self, stream_id: str, *, remote: bool) -> None:
        stream = self.streams.pop(stream_id, None)
        if not stream:
            return
        shared_backend = stream.shared_backend
        stream.shared_backend = None
        stream.closed.set()
        if stream.outbound_ack is not None and not stream.outbound_ack.done():
            stream.outbound_ack.set_exception(
                BridgeError("stream closed before data acknowledgement")
            )
        if stream.artifact_token is not None:
            self.artifact_publishers.pop(stream.artifact_token, None)
        for artifact_id, waiter in list(self.pending_artifacts.items()):
            if waiter.stream_id == stream_id:
                self._fail_artifact_waiter(
                    waiter,
                    "artifact stream closed before delivery completed",
                )
                self.pending_artifacts.pop(artifact_id, None)
        for state in list(self.receiving_artifacts.values()):
            if state.stream_id == stream_id:
                await self._abort_received_artifact(state, "logical stream closed")
        current = asyncio.current_task()
        for task in tuple(stream.tasks):
            if task is not current and not task.done():
                task.cancel()
        if stream.socket_writer and not stream.socket_writer.is_closing():
            stream.socket_writer.close()
            try:
                await stream.socket_writer.wait_closed()
            except OSError:
                pass
        if stream.process and stream.process.returncode is None:
            try:
                stream.process.terminate()
            except ProcessLookupError:
                pass
            try:
                await asyncio.wait_for(stream.process.wait(), timeout=5)
            except TimeoutError:
                stream.process.kill()
                await stream.process.wait()
        if shared_backend is not None:
            await shared_backend.detach(stream_id)
        if stream.artifact_stage is not None:
            shutil.rmtree(stream.artifact_stage, ignore_errors=True)
        if not remote:
            try:
                await self._send_frame({"type": "close", "stream": stream_id})
            except BridgeError:
                pass

    async def _fail_link_state(self, message: str) -> None:
        for future in self.pending_registry.values():
            if not future.done():
                future.set_exception(BridgeError(message))
        self.pending_registry.clear()
        for stream_id in list(self.streams):
            await self._close_stream(stream_id, remote=True)
        for backend in list(self.shared_backends.values()):
            await backend.stop(message)

    async def _handle_registry_request(self, frame: dict[str, Any]) -> None:
        request_id = str(frame.get("request"))
        try:
            action = str(frame.get("action"))
            arguments = frame.get("arguments") if isinstance(frame.get("arguments"), dict) else {}
            result = self.registry.query(action, arguments)
            response = {"type": "registry_response", "request": request_id, "ok": True, "result": result}
        except Exception as exc:
            response = {"type": "registry_response", "request": request_id, "ok": False, "message": str(exc)}
        if len(_json_bytes(response)) + 1 > MAX_FRAME_BYTES:
            response = {
                "type": "registry_response",
                "request": request_id,
                "ok": False,
                "message": "registry response exceeds the bridge frame limit; narrow the query",
            }
        await self._send_frame(response)

    async def _remote_registry_query(self, action: str, arguments: dict[str, Any]) -> Any:
        await asyncio.wait_for(self.link_ready.wait(), timeout=10)
        request_id = uuid.uuid4().hex
        future = asyncio.get_running_loop().create_future()
        self.pending_registry[request_id] = future
        await self._send_frame(
            {
                "type": "registry_request",
                "request": request_id,
                "action": action,
                "arguments": arguments,
            }
        )
        try:
            return await asyncio.wait_for(future, timeout=10)
        finally:
            self.pending_registry.pop(request_id, None)

    async def _registry_query(self, scope: str, action: str, arguments: dict[str, Any]) -> Any:
        if scope == "local":
            return self.registry.query(action, arguments)
        if scope == "remote":
            return await self._remote_registry_query(action, arguments)
        raise BridgeError("registry scope must be local or remote")


def _recv_line(sock: socket.socket, limit: int = MAX_FRAME_BYTES) -> bytes:
    data = bytearray()
    while len(data) <= limit:
        chunk = sock.recv(1)
        if not chunk:
            raise BridgeError("local bridge closed before handshake response")
        data.extend(chunk)
        if chunk == b"\n":
            return bytes(data)
    raise BridgeError("local bridge handshake exceeds limit")


def proxy_stdio(
    local_host: str,
    local_port: int,
    target: str,
    artifact_inbox: str | None = None,
) -> int:
    if not ID_PATTERN.fullmatch(target):
        print("bridge proxy: invalid target id", file=sys.stderr)
        return 2
    if not _is_loopback(local_host):
        print("bridge proxy: local host must resolve only to loopback", file=sys.stderr)
        return 2
    try:
        sock = socket.create_connection((local_host, local_port), timeout=10)
        request: dict[str, Any] = {"op": "connect", "target": target}
        if artifact_inbox:
            request["artifactInbox"] = artifact_inbox
        sock.sendall(_json_bytes(request) + b"\n")
        reply = json.loads(_recv_line(sock))
        if not reply.get("ok"):
            raise BridgeError(str(reply.get("message", "bridge open failed")))
        sock.settimeout(None)
    except Exception as exc:
        print(f"bridge proxy: {exc}", file=sys.stderr)
        return 1

    errors: list[BaseException] = []
    error_lock = threading.Lock()
    socket_done = threading.Event()
    input_done = threading.Event()

    def record_error(exc: BaseException) -> None:
        with error_lock:
            errors.append(exc)

    def socket_to_stdout() -> None:
        try:
            while True:
                data = sock.recv(BUFFER_SIZE)
                if not data:
                    break
                sys.stdout.buffer.write(data)
                sys.stdout.buffer.flush()
        except BaseException as exc:  # preserve a relay diagnostic, not MCP stdout
            record_error(exc)
        finally:
            try:
                sys.stdout.buffer.flush()
            except OSError as exc:
                record_error(exc)
            socket_done.set()

    def stdin_to_socket() -> None:
        try:
            while True:
                data = os.read(sys.stdin.fileno(), BUFFER_SIZE)
                if not data:
                    break
                sock.sendall(data)
        except OSError as exc:
            if not socket_done.is_set():
                record_error(exc)
        finally:
            try:
                sock.shutdown(socket.SHUT_WR)
            except OSError:
                pass
            input_done.set()

    reader = threading.Thread(target=socket_to_stdout, daemon=True)
    writer = threading.Thread(target=stdin_to_socket, daemon=True)
    reader.start()
    writer.start()
    try:
        while not socket_done.wait(0.05):
            if input_done.is_set():
                reader.join(timeout=30)
                if reader.is_alive():
                    record_error(
                        BridgeError("local bridge did not close the MCP stream within 30 seconds")
                    )
                break
    finally:
        sock.close()
    with error_lock:
        error = errors[0] if errors else None
    if error is not None:
        print(f"bridge proxy: {error}", file=sys.stderr)
        return 1
    return 0


def publish_artifact(
    local_host: str,
    local_port: int,
    token: str,
    relative_path: str,
    name: str | None,
    media_type: str | None,
) -> dict[str, Any]:
    if not _is_loopback(local_host):
        raise BridgeError("artifact publisher host must resolve only to loopback")
    request: dict[str, Any] = {
        "op": "publish",
        "token": token,
        "relativePath": relative_path,
    }
    if name:
        request["name"] = name
    if media_type:
        request["mediaType"] = media_type
    with socket.create_connection((local_host, local_port), timeout=10) as sock:
        sock.settimeout(320)
        sock.sendall(_json_bytes(request) + b"\n")
        reply = json.loads(_recv_line(sock, limit=MAX_FRAME_BYTES))
    if not reply.get("ok"):
        raise BridgeError(str(reply.get("message", "artifact publish failed")))
    result = reply.get("result")
    if not isinstance(result, dict):
        raise BridgeError("artifact publisher returned an invalid result")
    return result


def local_registry_query(
    local_host: str,
    local_port: int,
    scope: str,
    action: str,
    arguments: dict[str, Any],
) -> Any:
    if not _is_loopback(local_host):
        raise BridgeError("registry host must resolve only to loopback")
    with socket.create_connection((local_host, local_port), timeout=10) as sock:
        sock.sendall(
            _json_bytes(
                {
                    "op": "registry",
                    "scope": scope,
                    "action": action,
                    "arguments": arguments,
                }
            )
            + b"\n"
        )
        reply = json.loads(_recv_line(sock))
    if not reply.get("ok"):
        raise BridgeError(str(reply.get("message", "registry query failed")))
    return reply.get("result")


REGISTRY_INSTRUCTIONS = (
    "This read-only bridge registry describes only MCPs registered on the peer host. "
    "Local MCPs remain registered directly with the local agent and are intentionally omitted "
    "to prevent duplicate capability exposure. Production / User may list, search, describe, "
    "and inspect peer registration status. Production / Operator owns registration, command "
    "changes, availability recovery, and rollback outside this MCP. Descriptions are metadata, "
    "not authority to execute commands or grant credentials, data, cost, mutation, restart, "
    "or destructive permissions."
)


def _registry_tools() -> list[dict[str, Any]]:
    return [
        {
            "name": "bridge_registry_list",
            "description": "List redacted capability summaries for MCPs registered on the peer host.",
            "inputSchema": {
                "type": "object",
                "properties": {},
                "additionalProperties": False,
            },
        },
        {
            "name": "bridge_registry_search",
            "description": "Search peer MCP capability summaries without exposing launch configuration.",
            "inputSchema": {
                "type": "object",
                "properties": {"query": {"type": "string"}},
                "required": ["query"],
                "additionalProperties": False,
            },
        },
        {
            "name": "bridge_registry_describe",
            "description": "Describe one peer MCP using redacted authored capability metadata.",
            "inputSchema": {
                "type": "object",
                "properties": {"id": {"type": "string"}},
                "required": ["id"],
                "additionalProperties": False,
            },
        },
        {
            "name": "bridge_registry_status",
            "description": "Report peer registration state separately from transport and MCP initialization health.",
            "inputSchema": {
                "type": "object",
                "properties": {"id": {"type": "string"}},
                "additionalProperties": False,
            },
        },
    ]


def _registry_mcp_dispatch(
    message: dict[str, Any],
    tools: list[dict[str, Any]],
    actions: dict[str, str],
    local_host: str,
    local_port: int,
) -> dict[str, Any]:
    method = message["method"]
    params = message.get("params", {})
    if params is None:
        params = {}
    if not isinstance(params, dict):
        raise JsonRpcError(-32602, "params must be an object")
    if method == "initialize":
        requested_protocol = params.get("protocolVersion")
        if not isinstance(requested_protocol, str):
            raise JsonRpcError(-32602, "initialize requires protocolVersion")
        return {
            "protocolVersion": "2025-06-18",
            "capabilities": {"tools": {"listChanged": False}},
            "serverInfo": {"name": "win-wsl-mcp-registry", "version": SERVER_VERSION},
            "instructions": REGISTRY_INSTRUCTIONS,
        }
    if method == "ping":
        return {}
    if method == "tools/list":
        return {"tools": tools}
    if method == "tools/call":
        name = params.get("name")
        if not isinstance(name, str) or name not in actions:
            raise JsonRpcError(-32602, f"unknown registry tool: {name}")
        arguments = params.get("arguments", {})
        if not isinstance(arguments, dict):
            raise JsonRpcError(-32602, "tool arguments must be an object")
        try:
            value = local_registry_query(
                local_host,
                local_port,
                "remote",
                actions[name],
                arguments,
            )
        except BridgeError as exc:
            return {
                "content": [{"type": "text", "text": str(exc)}],
                "isError": True,
            }
        return {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps(value, ensure_ascii=False, indent=2),
                }
            ],
            "structuredContent": {"result": value},
        }
    raise JsonRpcError(-32601, f"method not found: {method}")


def _write_registry_mcp_response(response: dict[str, Any]) -> None:
    encoded = json.dumps(response, ensure_ascii=False, separators=(",", ":"))
    if len(encoded.encode("utf-8")) + 1 > MAX_FRAME_BYTES:
        encoded = json.dumps(
            {
                "jsonrpc": "2.0",
                "id": response.get("id"),
                "error": {
                    "code": -32603,
                    "message": "registry MCP response exceeds the configured limit",
                },
            },
            separators=(",", ":"),
        )
    sys.stdout.write(encoded + "\n")
    sys.stdout.flush()


def registry_mcp(local_host: str, local_port: int) -> int:
    tools = _registry_tools()
    actions = {
        "bridge_registry_list": "list",
        "bridge_registry_search": "search",
        "bridge_registry_describe": "describe",
        "bridge_registry_status": "status",
    }
    while True:
        raw = sys.stdin.buffer.readline(MAX_FRAME_BYTES + 1)
        if not raw:
            return 0
        if len(raw) > MAX_FRAME_BYTES:
            while raw and not raw.endswith(b"\n"):
                raw = sys.stdin.buffer.readline(MAX_FRAME_BYTES + 1)
            _write_registry_mcp_response(
                {
                    "jsonrpc": "2.0",
                    "id": None,
                    "error": {"code": -32700, "message": "MCP message exceeds the limit"},
                }
            )
            continue
        raw = raw.strip()
        if not raw:
            continue
        try:
            parsed = json.loads(raw)
        except (json.JSONDecodeError, UnicodeDecodeError):
            _write_registry_mcp_response(
                {
                    "jsonrpc": "2.0",
                    "id": None,
                    "error": {"code": -32700, "message": "parse error"},
                }
            )
            continue
        if (
            not isinstance(parsed, dict)
            or parsed.get("jsonrpc") != "2.0"
            or not isinstance(parsed.get("method"), str)
        ):
            _write_registry_mcp_response(
                {
                    "jsonrpc": "2.0",
                    "id": parsed.get("id") if isinstance(parsed, dict) else None,
                    "error": {"code": -32600, "message": "invalid request"},
                }
            )
            continue
        if "id" not in parsed:
            continue
        request_id = parsed.get("id")
        if isinstance(request_id, bool) or not isinstance(request_id, (str, int)):
            _write_registry_mcp_response(
                {
                    "jsonrpc": "2.0",
                    "id": None,
                    "error": {"code": -32600, "message": "invalid request id"},
                }
            )
            continue
        try:
            result = _registry_mcp_dispatch(
                parsed,
                tools,
                actions,
                local_host,
                local_port,
            )
            response = {"jsonrpc": "2.0", "id": request_id, "result": result}
        except JsonRpcError as exc:
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": exc.code, "message": str(exc)},
            }
        except Exception as exc:
            print(
                f"registry MCP internal error: {type(exc).__name__}: {exc}",
                file=sys.stderr,
                flush=True,
            )
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -32603, "message": "internal registry error"},
            }
        _write_registry_mcp_response(response)


def default_registry_path(side: str) -> Path:
    if side == "win":
        base = Path(os.environ.get("LOCALAPPDATA", Path.home() / "AppData" / "Local"))
        return base / "WinWslMcpBridge" / "registry.sqlite3"
    base = Path(os.environ.get("XDG_STATE_HOME", Path.home() / ".local" / "state"))
    return base / "win-wsl-mcp-bridge" / "registry.sqlite3"


def deployment_diagnostics(
    *,
    side: str,
    registry_path: Path,
    local_host: str,
    local_port: int,
    link_host: str,
    link_port: int,
    artifact_roots: list[Path],
) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []

    def record(name: str, ok: bool, detail: str) -> None:
        checks.append({"name": name, "ok": ok, "detail": detail})

    record(
        "python",
        sys.version_info >= (3, 11),
        f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}",
    )
    record("localHost", _is_loopback(local_host), local_host)
    record("linkHost", _is_loopback(link_host), link_host)
    record(
        "localPort",
        isinstance(local_port, int) and 1 <= local_port <= 65535,
        str(local_port),
    )
    record(
        "linkPort",
        isinstance(link_port, int) and 1 <= link_port <= 65535,
        str(link_port),
    )
    try:
        Registry(registry_path)
    except Exception as exc:
        record("registry", False, str(exc))
    else:
        record("registry", True, str(registry_path))
    for index, root in enumerate(artifact_roots):
        expanded = root.expanduser()
        ok = expanded.is_absolute() and expanded.is_dir()
        record(f"artifactRoot[{index}]", ok, str(expanded))
    return {
        "ok": all(check["ok"] for check in checks),
        "version": SERVER_VERSION,
        "bridgeProtocol": BRIDGE_PROTOCOL,
        "side": side,
        "checks": checks,
    }


def build_parser(default_side: str, default_local_port: int, default_link_mode: str) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Bidirectional WIN-WSL MCP bridge component")
    parser.add_argument("--version", action="version", version=f"%(prog)s {SERVER_VERSION}")
    subparsers = parser.add_subparsers(dest="command", required=True)

    serve = subparsers.add_parser("serve", help="run the local node and peer link")
    serve.add_argument("--registry")
    serve.add_argument("--local-host", default="127.0.0.1")
    serve.add_argument("--local-port", type=int, default=default_local_port)
    serve.add_argument("--link-mode", choices=["listen", "connect"], default=default_link_mode)
    serve.add_argument("--link-host", default="127.0.0.1")
    serve.add_argument("--link-port", type=int, default=8767)
    serve.add_argument("--side", choices=["win", "wsl"], default=default_side)
    serve.add_argument("--allow-artifact-root", action="append", default=[])
    serve.add_argument("--artifact-spool-root")
    serve.add_argument("--max-artifact-bytes", type=int, default=DEFAULT_MAX_ARTIFACT_BYTES)

    connect = subparsers.add_parser("connect", help="expose one remote registered MCP over local stdio")
    connect.add_argument("target")
    connect.add_argument("--local-host", default="127.0.0.1")
    connect.add_argument("--local-port", type=int, default=default_local_port)
    connect.add_argument(
        "--artifact-inbox",
        default=os.environ.get("WIN_WSL_MCP_BRIDGE_ARTIFACT_INBOX"),
    )

    publish = subparsers.add_parser(
        "publish",
        help="publish one staged business-MCP artifact to the peer workspace",
    )
    publish.add_argument("relative_path")
    publish.add_argument("--name")
    publish.add_argument("--media-type")
    publish.add_argument(
        "--local-host",
        default=os.environ.get("WIN_WSL_MCP_BRIDGE_ARTIFACT_LOCAL_HOST", "127.0.0.1"),
    )
    publish.add_argument(
        "--local-port",
        type=int,
        default=int(
            os.environ.get(
                "WIN_WSL_MCP_BRIDGE_ARTIFACT_LOCAL_PORT",
                str(default_local_port),
            )
        ),
    )
    publish.add_argument(
        "--token",
        default=os.environ.get("WIN_WSL_MCP_BRIDGE_ARTIFACT_TOKEN"),
    )

    registry = subparsers.add_parser("registry-mcp", help="serve the read-only bridge registry over stdio MCP")
    registry.add_argument("--local-host", default="127.0.0.1")
    registry.add_argument("--local-port", type=int, default=default_local_port)

    doctor = subparsers.add_parser(
        "doctor",
        help="validate local deployment configuration without starting listeners",
    )
    doctor.add_argument("--registry")
    doctor.add_argument("--side", choices=["win", "wsl"], default=default_side)
    doctor.add_argument("--local-host", default="127.0.0.1")
    doctor.add_argument("--local-port", type=int, default=default_local_port)
    doctor.add_argument("--link-host", default="127.0.0.1")
    doctor.add_argument("--link-port", type=int, default=8767)
    doctor.add_argument("--allow-artifact-root", action="append", default=[])

    registry_init = subparsers.add_parser(
        "registry-init",
        help="initialize or update the local SQLite registry from a manifest",
    )
    registry_init.add_argument("--registry")
    registry_init.add_argument("--manifest", required=True)
    registry_init.add_argument("--replace", action="store_true")
    return parser


def component_main(default_side: str, default_local_port: int, default_link_mode: str) -> int:
    if os.name != "nt":
        os.umask(0o077)
    parser = build_parser(default_side, default_local_port, default_link_mode)
    args = parser.parse_args()
    if args.command == "connect":
        return proxy_stdio(
            args.local_host,
            args.local_port,
            args.target,
            artifact_inbox=args.artifact_inbox,
        )
    if args.command == "publish":
        if not args.token:
            print("artifact publisher: missing session token", file=sys.stderr)
            return 2
        try:
            result = publish_artifact(
                args.local_host,
                args.local_port,
                args.token,
                args.relative_path,
                args.name,
                args.media_type,
            )
        except Exception as exc:
            print(f"artifact publisher: {exc}", file=sys.stderr)
            return 1
        print(json.dumps(result, ensure_ascii=False, separators=(",", ":")))
        return 0
    if args.command == "registry-mcp":
        return registry_mcp(args.local_host, args.local_port)
    registry_side = getattr(args, "side", default_side)
    registry_path = (
        Path(args.registry).expanduser().resolve()
        if args.registry
        else default_registry_path(registry_side)
    )
    if args.command == "doctor":
        report = deployment_diagnostics(
            side=args.side,
            registry_path=registry_path,
            local_host=args.local_host,
            local_port=args.local_port,
            link_host=args.link_host,
            link_port=args.link_port,
            artifact_roots=[Path(value) for value in args.allow_artifact_root],
        )
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 0 if report["ok"] else 1
    if args.command == "registry-init":
        Registry.initialize_database(
            registry_path,
            Path(args.manifest).expanduser().resolve(),
            replace=args.replace,
        )
        print(f"initialized registry: {registry_path}")
        return 0
    registry = Registry(registry_path)
    node = BridgeNode(
        side=args.side,
        registry=registry,
        local_host=args.local_host,
        local_port=args.local_port,
        link_mode=args.link_mode,
        link_host=args.link_host,
        link_port=args.link_port,
        allowed_artifact_roots=[Path(value) for value in args.allow_artifact_root],
        artifact_spool_root=Path(args.artifact_spool_root)
        if args.artifact_spool_root
        else None,
        max_artifact_bytes=args.max_artifact_bytes,
    )
    previous_sigterm: Any = None
    if os.name != "nt":
        def stop_on_sigterm(_signum: int, _frame: Any) -> None:
            raise KeyboardInterrupt

        previous_sigterm = signal.signal(signal.SIGTERM, stop_on_sigterm)
    try:
        asyncio.run(node.run())
    except KeyboardInterrupt:
        return 0
    finally:
        if previous_sigterm is not None:
            signal.signal(signal.SIGTERM, previous_sigterm)
    return 0


def _run_component(default_side: str, default_local_port: int, default_link_mode: str) -> int:
    try:
        return component_main(default_side, default_local_port, default_link_mode)
    except (BridgeError, OSError, sqlite3.Error, json.JSONDecodeError) as exc:
        print(f"{default_side} bridge: {exc}", file=sys.stderr)
        return 1


def win_main() -> int:
    return _run_component("win", 8768, "listen")


def wsl_main() -> int:
    return _run_component("wsl", 8769, "connect")


if __name__ == "__main__":
    raise SystemExit(
        "run the win-wsl-mcp-win or win-wsl-mcp-wsl entry point, or a component bridge.py"
    )
