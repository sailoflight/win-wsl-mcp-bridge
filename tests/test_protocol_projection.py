#!/usr/bin/env python3
"""Tests for the P10 dual-era tools-only modern projection.

Covers ``bridge_protocol`` (pure modern 2026-07-28 envelope/error/validation
helpers) and the ``SharedBackend`` Agent-facing surface it wires: era detection,
per-request metadata gating, ``server/discover``, -32022 / -32602 / -32601
answers, modern tools/list + tools/call forwarded over ONE physical legacy
2025-06-18 session, result shaping only on the modern side, progress/cancel
mapping, era isolation, and "no business replay".

The integration harness runs an in-process ``SharedBackend`` against the real
``protocol_fixture_mcp.py`` subprocess, with a stub node that records outbound
JSON-RPC per stream.  Legacy-era expectations in this file must stay in sync
with ``test_bridge.py``'s shared-backend contract (untouched there).

Run with PYTHONDONTWRITEBYTECODE=1 (CI does): python -m unittest -v
test_protocol_projection
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import bridge_protocol as bp
from bridge_protocol import (
    CACHE_SCOPE_PRIVATE,
    CACHE_TTL_MS_ZERO,
    JSONRPC_INTERNAL_ERROR,
    JSONRPC_INVALID_PARAMS,
    JSONRPC_METHOD_NOT_FOUND,
    MCP_UNSUPPORTED_PROTOCOL_VERSION,
    META_CLIENT_CAPABILITIES_KEY,
    META_PROTOCOL_VERSION_KEY,
    MODERN_MCP_PROTOCOL_VERSION,
    RESULT_TYPE_COMPLETE,
)
from bridge_runtime import SERVER_VERSION, SharedBackend, StreamState

PROTO_FIXTURE = ROOT / "tests" / "fixtures" / "protocol_fixture_mcp.py"
LEGACY_VERSION = "2025-06-18"

MODERN_META = {
    META_PROTOCOL_VERSION_KEY: MODERN_MCP_PROTOCOL_VERSION,
    META_CLIENT_CAPABILITIES_KEY: {"roots": {"listChanged": False}},
    "io.modelcontextprotocol/clientInfo": {"name": "p10-test-agent", "version": "1"},
}


def modern_request(method, request_id, params=None, meta=None):
    params = dict(params or {})
    if meta is not None:
        params["_meta"] = meta
    return {"jsonrpc": "2.0", "id": request_id, "method": method, "params": params}


def legacy_request(method, request_id, params=None):
    message = {"jsonrpc": "2.0", "id": request_id, "method": method}
    if params is not None:
        message["params"] = params
    return message


# --------------------------------------------------------------------------- #
# Pure module unit tests                                                      #
# --------------------------------------------------------------------------- #


class BridgeProtocolUnitTest(unittest.TestCase):
    def test_modern_version_constants(self) -> None:
        self.assertEqual(MODERN_MCP_PROTOCOL_VERSION, "2026-07-28")
        self.assertEqual(
            bp.SUPPORTED_MODERN_PROTOCOL_VERSIONS, ("2026-07-28",)
        )
        # The modern per-request surface must not claim legacy revisions; the
        # runtime serves those through the separate legacy initialize path.
        self.assertTrue(bp.is_supported_modern_version("2026-07-28"))
        self.assertFalse(bp.is_supported_modern_version("2025-11-25"))
        self.assertFalse(bp.is_supported_modern_version("2027-01-01"))

    def test_protocol_version_entry_detection(self) -> None:
        present, version = bp.protocol_version_entry(
            modern_request("tools/list", "r1", meta=MODERN_META)
        )
        self.assertTrue(present)
        self.assertEqual(version, MODERN_MCP_PROTOCOL_VERSION)
        present, version = bp.protocol_version_entry(
            legacy_request("tools/list", "r1", {"cursor": "x"})
        )
        self.assertFalse(present)
        self.assertIsNone(version)
        # Key present but not a string => modern-flagged with no usable version.
        present, version = bp.protocol_version_entry(
            modern_request("tools/list", "r1", meta={META_PROTOCOL_VERSION_KEY: 123})
        )
        self.assertTrue(present)
        self.assertIsNone(version)

    def test_validate_modern_request_meta_required_fields(self) -> None:
        self.assertEqual(
            bp.validate_modern_request_meta(modern_request("tools/list", "r1")),
            ["_meta is required on every modern request"],
        )
        self.assertEqual(
            bp.validate_modern_request_meta(
                modern_request(
                    "tools/list", "r1", meta={META_PROTOCOL_VERSION_KEY: "2026-07-28"}
                )
            ),
            ["io.modelcontextprotocol/clientCapabilities must be an object"],
        )
        bad_meta = dict(MODERN_META)
        bad_meta[META_CLIENT_CAPABILITIES_KEY] = ["not", "an", "object"]
        problems = bp.validate_modern_request_meta(
            modern_request("tools/list", "r1", meta=bad_meta)
        )
        self.assertIn(
            "io.modelcontextprotocol/clientCapabilities must be an object", problems
        )
        self.assertEqual(
            bp.validate_modern_request_meta(
                modern_request("tools/list", "r1", meta=MODERN_META)
            ),
            [],
        )
        # Optional clientInfo must at least be an object when present.
        bad_info = dict(MODERN_META)
        bad_info["io.modelcontextprotocol/clientInfo"] = "not-an-object"
        self.assertEqual(
            bp.validate_modern_request_meta(
                modern_request("tools/list", "r1", meta=bad_info)
            ),
            ["io.modelcontextprotocol/clientInfo must be an object when present"],
        )

    def test_unsupported_version_error_shape(self) -> None:
        error = bp.unsupported_version_error("2030-01-01")
        self.assertEqual(error["code"], MCP_UNSUPPORTED_PROTOCOL_VERSION)
        self.assertEqual(error["code"], -32022)
        self.assertEqual(error["message"], "Unsupported protocol version")
        self.assertEqual(error["data"]["supported"], ["2026-07-28"])
        self.assertEqual(error["data"]["requested"], "2030-01-01")

    def test_invalid_params_and_method_not_found_builders(self) -> None:
        self.assertEqual(bp.invalid_params_error("oops")["code"], JSONRPC_INVALID_PARAMS)
        self.assertEqual(bp.invalid_params_error("oops")["code"], -32602)
        not_found = bp.method_not_found_error("resources/list", "not advertised")
        self.assertEqual(not_found["code"], JSONRPC_METHOD_NOT_FOUND)
        self.assertEqual(not_found["code"], -32601)
        self.assertIn("resources/list", not_found["message"])

    def test_sanitize_params_for_legacy_strips_modern_meta(self) -> None:
        params = {
            "_meta": dict(MODERN_META),
            "cursor": "abc",
            "name": "echo",
            "arguments": {"value": 7},
        }
        out = bp.sanitize_params_for_legacy(params)
        self.assertNotIn("_meta", out)
        self.assertEqual(out["cursor"], "abc")
        self.assertEqual(out["arguments"], {"value": 7})
        # progressToken is the single allowed _meta carry-over.
        params["_meta"]["progressToken"] = "pt-original"
        out = bp.sanitize_params_for_legacy(params)
        self.assertEqual(out["_meta"], {"progressToken": "pt-original"})
        self.assertNotIn(META_PROTOCOL_VERSION_KEY, out["_meta"])
        self.assertNotIn(META_CLIENT_CAPABILITIES_KEY, out["_meta"])
        # Boolean/None tokens are not valid progress tokens; drop _meta entirely.
        params["_meta"] = {"progressToken": True}
        self.assertNotIn("_meta", bp.sanitize_params_for_legacy(params))
        # Non-object params => None (caller answers InvalidParams).
        self.assertIsNone(bp.sanitize_params_for_legacy("not-a-dict"))

    def test_mrtr_retry_field_detection(self) -> None:
        self.assertFalse(
            bp.has_mrtr_retry_fields(modern_request("tools/call", "r1", meta=MODERN_META))
        )
        with_retry = modern_request("tools/call", "r1", meta=MODERN_META)
        with_retry["params"]["inputResponses"] = [{"toolResult": {}}]
        self.assertTrue(bp.has_mrtr_retry_fields(with_retry))

    def test_discover_result_declares_only_supported_contract(self) -> None:
        server_info = {"name": "win-wsl-mcp-bridge", "version": SERVER_VERSION}
        # Observed backend offered tools: advertise the tools family.
        result = bp.discover_result(server_info, backend_tools=True)
        self.assertEqual(result["resultType"], RESULT_TYPE_COMPLETE)
        self.assertEqual(result["supportedVersions"], ["2026-07-28"])
        # Tools-only projection: no optional families invented.
        self.assertEqual(result["capabilities"], {"tools": {}})
        self.assertNotIn("resources", result["capabilities"])
        self.assertNotIn("prompts", result["capabilities"])
        self.assertNotIn("completions", result["capabilities"])
        self.assertNotIn("logging", result["capabilities"])
        self.assertNotIn("tasks", result["capabilities"])
        self.assertEqual(result["ttlMs"], CACHE_TTL_MS_ZERO)
        self.assertEqual(result["cacheScope"], CACHE_SCOPE_PRIVATE)
        self.assertEqual(
            result["_meta"]["io.modelcontextprotocol/serverInfo"]["name"],
            "win-wsl-mcp-bridge",
        )
        # No downstream instructions observed: never invent boilerplate text.
        self.assertNotIn("instructions", result)
        # Observed backend did NOT offer tools: no tools capability fabricated.
        no_tools = bp.discover_result(server_info, backend_tools=False)
        self.assertEqual(no_tools["capabilities"], {})
        self.assertNotIn("tools", no_tools["capabilities"])
        self.assertIsNone(bp.discover_result(server_info, backend_tools=None)["capabilities"].get("tools"))
        # Downstream instructions are preserved verbatim, never replaced.
        canonical = "Downstream canonical prompt: verify before use."
        with_instructions = bp.discover_result(
            server_info, backend_tools=True, backend_instructions=canonical
        )
        self.assertEqual(with_instructions["instructions"], canonical)

    def test_normalize_backend_error_retires_retired_mcp_codes(self) -> None:
        # -32042 (and -32002) are retired: a modern projection must not emit them.
        normalized = bp.normalize_backend_error(
            {"code": -32042, "message": "legacy transport closed", "data": {"k": 1}}
        )
        self.assertEqual(normalized["code"], JSONRPC_INTERNAL_ERROR)
        self.assertEqual(normalized["code"], -32603)
        self.assertEqual(normalized["message"], "legacy transport closed")
        self.assertEqual(normalized["data"]["retiredCode"], -32042)
        self.assertEqual(normalized["data"]["originalData"], {"k": 1})
        self.assertEqual(normalized["data"]["code"], "retired_mcp_error_code_normalized")
        normalized_32002 = bp.normalize_backend_error(
            {"code": -32002, "message": "connection reset"}
        )
        self.assertEqual(normalized_32002["code"], -32603)
        self.assertEqual(normalized_32002["data"]["retiredCode"], -32002)
        # Current-era and implementation-defined codes pass through untouched.
        passthrough = bp.normalize_backend_error(
            {"code": -32603, "message": "boom"}
        )
        self.assertEqual(passthrough["code"], -32603)
        self.assertEqual(passthrough["message"], "boom")
        passthrough = bp.normalize_backend_error(
            {"code": -32001, "message": "impl", "data": {"code": "x"}}
        )
        self.assertEqual(passthrough["code"], -32001)
        # Non-object error bodies pass through (fail closed is the caller's job).
        self.assertEqual(bp.normalize_backend_error("oops"), "oops")

    def test_shape_modern_result_adds_era_envelope(self) -> None:
        server_info = {"name": "n", "version": "v"}
        # tools/list is a CacheableResult family: cache fields + resultType.
        shaped = bp.shape_modern_result(
            "tools/list", {"tools": [{"name": "echo"}]}, server_info
        )
        self.assertEqual(shaped["resultType"], RESULT_TYPE_COMPLETE)
        self.assertEqual(shaped["ttlMs"], 0)
        self.assertEqual(shaped["cacheScope"], CACHE_SCOPE_PRIVATE)
        self.assertEqual(shaped["tools"], [{"name": "echo"}])
        self.assertEqual(shaped["_meta"]["io.modelcontextprotocol/serverInfo"], server_info)
        # Cache envelope is Bridge-owned: a legacy backend's cache hints are
        # never trusted onto the modern envelope.
        shaped = bp.shape_modern_result(
            "tools/list",
            {"tools": [], "ttlMs": 12345, "cacheScope": "public"},
            server_info,
        )
        self.assertEqual(shaped["ttlMs"], 0)
        self.assertEqual(shaped["cacheScope"], CACHE_SCOPE_PRIVATE)
        # Legacy task-augmented / unknown top-level fields are withheld on the
        # tools-only surface (deterministic projection, nothing unadvertised).
        shaped = bp.shape_modern_result(
            "tools/list",
            {
                "tools": [{"name": "echo"}],
                "nextCursor": "n1",
                "task": {"description": "augmented", "readMore": []},
                "unadvertisedLegacyField": {"x": 1},
                "_meta": {"io.win-wsl-mcp-bridge/runtime": {"r": 1}},
            },
            server_info,
        )
        self.assertEqual(shaped["tools"], [{"name": "echo"}])
        self.assertEqual(shaped["nextCursor"], "n1")
        self.assertNotIn("task", shaped)
        self.assertNotIn("unadvertisedLegacyField", shaped)
        self.assertEqual(
            shaped["_meta"]["io.win-wsl-mcp-bridge/runtime"], {"r": 1}
        )
        # tools/call: resultType + serverInfo, no cache fields; only the exact
        # modern CallToolResult payload keys survive.
        call = bp.shape_modern_result(
            "tools/call",
            {
                "content": [{"type": "text", "text": "hi"}],
                "structuredContent": {"ok": True},
                "task": {"keepOut": True},
                "unadvertised": 1,
            },
            server_info,
        )
        self.assertEqual(call["resultType"], RESULT_TYPE_COMPLETE)
        self.assertNotIn("ttlMs", call)
        self.assertNotIn("cacheScope", call)
        self.assertNotIn("task", call)
        self.assertNotIn("unadvertised", call)
        # Business bytes stay untouched.
        self.assertEqual(call["structuredContent"], {"ok": True})
        # Fail closed: non-object or unprojectable results raise instead of
        # leaking a raw invalid modern result.
        with self.assertRaises(ValueError):
            bp.shape_modern_result("tools/call", None, server_info)
        with self.assertRaises(ValueError):
            bp.shape_modern_result("tools/list", ["not", "an", "object"], server_info)
        with self.assertRaises(ValueError):
            bp.shape_modern_result("prompts/list", {"prompts": []}, server_info)

    def test_shape_modern_tool_result_preserves_policy_payload(self) -> None:
        legacy = {
            "content": [{"type": "text", "text": "{}"}],
            "structuredContent": {"error": {"code": "x"}, "message": "m"},
            "isError": True,
            "_meta": {"io.win-wsl-mcp-bridge/runtime": {"code": "x"}},
        }
        shaped = bp.shape_modern_tool_result(legacy, {"name": "n", "version": "v"})
        self.assertEqual(shaped["resultType"], RESULT_TYPE_COMPLETE)
        self.assertTrue(shaped["isError"])
        self.assertEqual(shaped["structuredContent"], legacy["structuredContent"])
        self.assertEqual(
            shaped["_meta"]["io.win-wsl-mcp-bridge/runtime"], {"code": "x"}
        )
        self.assertEqual(
            shaped["_meta"]["io.modelcontextprotocol/serverInfo"], {"name": "n", "version": "v"}
        )


# --------------------------------------------------------------------------- #
# In-process SharedBackend integration harness                                #
# --------------------------------------------------------------------------- #


class _NodeStub:
    """Minimal BridgeNode stand-in: records per-stream outbound JSON-RPC."""

    def __init__(self, stream_ids):
        self.streams: dict[str, StreamState] = {}
        self.outbound: dict[str, asyncio.Queue] = {}
        for sid in stream_ids:
            stream = StreamState(stream_id=sid)
            self.streams[sid] = stream
            self.outbound[sid] = asyncio.Queue()
        self.frames = []
        self.closed: list[tuple[str, bool]] = []
        self.log_lines: list[str] = []
        self.shared_artifact_publishers = {}
        self.peer_artifacts = False
        self.max_artifact_bytes = 0
        self.local_host = "127.0.0.1"
        self.local_port = 0
        self.link_generation = "tests"
        self.artifact_spool_root = None

    def log(self, line: str) -> None:
        self.log_lines.append(line)

    def _lifecycle_drain_armed(self, target: str) -> bool:
        return False

    def _background(self, coro):
        return asyncio.get_running_loop().create_task(coro)

    def _send_frame(self, frame: dict) -> None:
        self.frames.append(frame)

    async def _send_jsonrpc_to_stream(self, stream: StreamState, message: dict) -> None:
        await self.outbound[stream.stream_id].put(message)

    async def _close_stream(self, stream_id: str, remote: bool = False) -> None:
        self.closed.append((stream_id, remote))


class SharedBackendDualEraProjectionTest(unittest.IsolatedAsyncioTestCase):
    """Modern projection over one real legacy fixture-MCP process."""

    maxDiff = None

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(prefix="p10-projection-")
        self.tmp_dir = Path(self._tmp.name)
        test_id = self.id().replace("/", "_").replace(".", "_")
        self.log_path = self.tmp_dir / f"{test_id}.log"
        self.backend: SharedBackend | None = None
        self.node: _NodeStub | None = None

    def tearDown(self) -> None:
        if self.backend is not None:
            # Best-effort: the async teardown already stopped the process.
            pass
        self._tmp.cleanup()

    async def asyncTearDown(self) -> None:
        if self.backend is not None:
            try:
                await self.backend.stop("test teardown")
            except Exception as exc:  # noqa: BLE001
                self.node.log(f"teardown stop failed: {exc!r}")

    # -- harness helpers ---------------------------------------------------- #

    def _entry(self, env: dict | None = None, **process_extra) -> dict:
        process = dict(process_extra)
        return {
            "command": sys.executable,
            "args": [str(PROTO_FIXTURE)],
            "cwd": str(ROOT),
            "env": {
                **os.environ,
                "PYTHONDONTWRITEBYTECODE": "1",
                "PROTO_FIXTURE_LOG": str(self.log_path),
                **(env or {}),
            },
            "process": process,
            "artifactDelivery": {"enabled": False},
        }

    async def _make_backend(
        self, process: dict | None = None, env: dict | None = None
    ) -> SharedBackend:
        node = _NodeStub(stream_ids=["s1", "s2"])
        backend = SharedBackend(
            node=node,
            target="protocol-fixture",
            entry=self._entry(env=env, **dict(process or {})),
        )
        self.node = node
        self.backend = backend
        return backend

    async def _attach(self, backend: SharedBackend, sid: str) -> None:
        stream = self.node.streams[sid]
        await backend.attach(stream)

    async def _recv(self, sid: str = "s1", timeout: float = 10.0) -> dict:
        message = await asyncio.wait_for(self.node.outbound[sid].get(), timeout=timeout)
        return message

    async def _recv_for(
        self, sid: str, request_id, timeout: float = 10.0
    ) -> tuple[dict, list[dict]]:
        """Return (response for request_id, all messages consumed first)."""
        seen: list[dict] = []
        deadline = time.monotonic() + timeout
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise AssertionError(
                    f"no response for id {request_id!r}; saw {seen!r}"
                )
            try:
                message = await asyncio.wait_for(
                    self.node.outbound[sid].get(), timeout=min(remaining, 0.5)
                )
            except asyncio.TimeoutError:
                # A bounded chunk expired with no message: keep waiting up to
                # the overall deadline instead of failing early.
                continue
            seen.append(message)
            if message.get("id") == request_id and (
                "result" in message or "error" in message
            ):
                return message, seen
            # Notifications carry no id and are consumed into ``seen``.

    async def _events(self) -> list[dict]:
        if not self.log_path.exists():
            return []
        lines = self.log_path.read_text(encoding="utf-8").splitlines()
        return [json.loads(line) for line in lines if line.strip()]

    async def _wait_event_count(self, event: str, minimum: int, timeout: float = 8.0) -> list[dict]:
        deadline = time.monotonic() + timeout
        while True:
            events = await self._events()
            if sum(1 for item in events if item.get("event") == event) >= minimum:
                return events
            if time.monotonic() >= deadline:
                raise AssertionError(
                    f"fixture never logged {minimum}x {event!r}; saw {events!r}"
                )
            await asyncio.sleep(0.02)

    async def _send(self, backend: SharedBackend, sid: str, message: dict) -> None:
        await backend._route_client_message(sid, message)

    # -- integration tests --------------------------------------------------- #

    async def test_modern_clients_do_not_receive_legacy_subscription_notifications(self) -> None:
        backend = await self._make_backend()
        await self._attach(backend, "s1")
        await self._attach(backend, "s2")
        await self._send(backend, "s1", modern_request("server/discover", 1, meta=MODERN_META))
        await self._recv_for("s1", 1)
        await self._send(backend, "s2", legacy_request("initialize", 2, {
            "protocolVersion": LEGACY_VERSION, "capabilities": {},
            "clientInfo": {"name": "legacy", "version": "1"},
        }))
        await self._recv_for("s2", 2)
        notification = {"jsonrpc": "2.0", "method": "notifications/tools/list_changed"}
        await backend._route_backend_message(notification)
        self.assertEqual(await self._recv("s2"), notification)
        self.assertTrue(self.node.outbound["s1"].empty())

    async def test_modern_request_never_receives_legacy_logging_or_server_requests(self) -> None:
        backend = await self._make_backend()
        await self._attach(backend, "s1")
        await self._send(backend, "s1", modern_request("server/discover", 1, meta=MODERN_META))
        await self._recv_for("s1", 1)
        with patch.object(backend, "current_request", SimpleNamespace(client_id="s1", modern=True)):
            await backend._route_backend_message({
                "jsonrpc": "2.0", "method": "notifications/message",
                "params": {"level": "info", "data": "not requested"},
            })
            with patch.object(backend, "_write_message", new_callable=AsyncMock) as write:
                await backend._route_backend_message({
                    "jsonrpc": "2.0", "id": "backend-request", "method": "roots/list",
                })
                self.assertEqual(write.call_args.args[0]["error"]["code"], -32601)
        self.assertTrue(self.node.outbound["s1"].empty())

    async def test_discover_bootstraps_and_reports_observed_backend_contract(self) -> None:
        # A supported modern discover observes the physical backend before it
        # advertises anything: one Bridge-owned bootstrap initialize, tools
        # advertised only because the fixture actually offered the tools
        # capability, and the downstream instructions preserved verbatim.
        backend = await self._make_backend()
        await self._attach(backend, "s1")
        await self._send(
            backend,
            "s1",
            modern_request("server/discover", "d1", params={}, meta=MODERN_META),
        )
        response, _seen = await self._recv_for("s1", "d1")
        result = response["result"]
        self.assertEqual(result["resultType"], "complete")
        self.assertEqual(result["supportedVersions"], ["2026-07-28"])
        # Capability intersection with the observed backend: tools advertised,
        # no optional family invented.
        self.assertEqual(result["capabilities"], {"tools": {}})
        self.assertEqual(result["ttlMs"], 0)
        self.assertEqual(result["cacheScope"], "private")
        self.assertEqual(
            result["instructions"],
            "Protocol dual-era fixture for protocol-fixture.",
        )
        server_info = result["_meta"]["io.modelcontextprotocol/serverInfo"]
        self.assertEqual(server_info["name"], "win-wsl-mcp-bridge")
        self.assertEqual(server_info["version"], SERVER_VERSION)
        # Discover triggered exactly one physical initialize (the bootstrap) and
        # its single initialized handshake notification.
        events = await self._wait_event_count("initialized-notification", 1)
        self.assertEqual(
            [item["event"] for item in events],
            ["initialize", "initialized-notification"],
        )
        self.assertIsNotNone(backend.initialize_template)
        self.assertEqual(backend.generation, 1)

    async def test_discover_without_tools_backend_advertises_nothing_and_keeps_prompt(
        self,
    ) -> None:
        # Capability advertising is observation-backed, never fabricated: a
        # backend that offers no tools capability must yield an empty modern
        # capability set and a verbatim downstream canonical prompt.
        canonical = "Canonical downstream prompt: this fixture exposes no tools."
        backend = await self._make_backend(
            env={
                "PROTO_FIXTURE_NO_TOOLS": "1",
                "PROTO_FIXTURE_INSTRUCTIONS": canonical,
            }
        )
        await self._attach(backend, "s1")
        await self._send(
            backend,
            "s1",
            modern_request("server/discover", "nt1", params={}, meta=MODERN_META),
        )
        response, _seen = await self._recv_for("s1", "nt1")
        result = response["result"]
        self.assertEqual(result["capabilities"], {})
        self.assertNotIn("tools", result["capabilities"])
        self.assertEqual(result["instructions"], canonical)
        # tools/* is not part of the projected modern surface on this backend:
        # answer -32601 and never reach the backend.
        await self._send(
            backend,
            "s1",
            modern_request("tools/list", "nt2", params={}, meta=MODERN_META),
        )
        response, _seen = await self._recv_for("s1", "nt2")
        self.assertEqual(response["error"]["code"], -32601)
        self.assertIn("tools", response["error"]["message"])
        events = await self._wait_event_count("initialized-notification", 1)
        self.assertEqual(
            sum(1 for item in events if item["event"] == "initialize"), 1
        )
        self.assertEqual(
            [item for item in events if item["event"] == "tools-list"], []
        )

    async def test_unsupported_modern_version_answers_32022_with_data(self) -> None:
        backend = await self._make_backend()
        await self._attach(backend, "s1")
        meta = dict(MODERN_META)
        meta[META_PROTOCOL_VERSION_KEY] = "2030-01-01"
        await self._send(
            backend,
            "s1",
            modern_request("server/discover", "u1", params={}, meta=meta),
        )
        response, _seen = await self._recv_for("s1", "u1")
        error = response["error"]
        self.assertEqual(error["code"], -32022)
        self.assertEqual(error["message"], "Unsupported protocol version")
        self.assertEqual(error["data"]["supported"], ["2026-07-28"])
        self.assertEqual(error["data"]["requested"], "2030-01-01")
        self.assertEqual(await self._events(), [])
        # The same stream stays usable for a supported-version request.
        await self._send(
            backend, "s1", modern_request("server/discover", "u2", params={}, meta=MODERN_META)
        )
        response, _seen = await self._recv_for("s1", "u2")
        self.assertIn("result", response)

    async def test_missing_required_client_capabilities_answers_32602(self) -> None:
        backend = await self._make_backend()
        await self._attach(backend, "s1")
        meta = {META_PROTOCOL_VERSION_KEY: "2026-07-28"}
        await self._send(
            backend,
            "s1",
            modern_request("tools/list", "m1", params={}, meta=meta),
        )
        response, _seen = await self._recv_for("s1", "m1")
        self.assertEqual(response["error"]["code"], -32602)
        self.assertIn("clientCapabilities", response["error"]["message"])
        self.assertEqual(await self._events(), [])

    async def test_modern_tools_list_is_forwarded_and_shaped(self) -> None:
        backend = await self._make_backend()
        await self._attach(backend, "s1")
        await self._send(
            backend,
            "s1",
            modern_request("tools/list", "l1", params={"cursor": "page1"}, meta=MODERN_META),
        )
        events = await self._wait_event_count("tools-list", 1)
        self.assertEqual(
            [item["event"] for item in events],
            [
                "initialize",
                "initialized-notification",  # Bridge-owned bootstrap handshake
                "tools-list",
            ],
        )
        response, _seen = await self._recv_for("s1", "l1")
        result = response["result"]
        self.assertEqual(result["resultType"], "complete")
        self.assertEqual(result["ttlMs"], 0)
        self.assertEqual(result["cacheScope"], "private")
        names = {tool["name"] for tool in result["tools"]}
        self.assertEqual(
            names, {"echo", "probe", "slow", "tool_error", "retired_error"}
        )
        self.assertNotIn("nextCursor", result)
        self.assertEqual(
            result["_meta"]["io.modelcontextprotocol/serverInfo"]["name"],
            "win-wsl-mcp-bridge",
        )

    async def test_modern_tools_call_strips_modern_meta_from_backend(self) -> None:
        backend = await self._make_backend()
        await self._attach(backend, "s1")
        await self._send(
            backend,
            "s1",
            modern_request(
                "tools/call",
                "p1",
                params={"name": "probe", "arguments": {}},
                meta=MODERN_META,
            ),
        )
        await self._wait_event_count("tools-call", 1)
        response, _seen = await self._recv_for("s1", "p1")
        result = response["result"]
        self.assertEqual(result["resultType"], "complete")
        backend_params = result["structuredContent"]["params"]
        # No io.modelcontextprotocol/* claims may reach the physical backend,
        # and no empty _meta object is invented for it.
        meta = backend_params.get("_meta")
        self.assertTrue(meta is None or not any(
            key.startswith("io.modelcontextprotocol/")
            for key in meta
        ))
        self.assertEqual(backend_params["name"], "probe")
        self.assertEqual(
            result["_meta"]["io.modelcontextprotocol/serverInfo"]["version"],
            SERVER_VERSION,
        )

    async def test_legacy_era_responses_keep_exact_legacy_bytes(self) -> None:
        backend = await self._make_backend()
        await self._attach(backend, "s1")
        # Legacy initialize handshake (physical backend is initialized by it).
        await self._send(
            backend,
            "s1",
            legacy_request(
                "initialize",
                "li1",
                {"protocolVersion": LEGACY_VERSION, "capabilities": {"tools": {}}},
            ),
        )
        response, _seen = await self._recv_for("s1", "li1")
        self.assertEqual(response["result"]["protocolVersion"], LEGACY_VERSION)
        await self._send(
            backend,
            "s1",
            {"jsonrpc": "2.0", "method": "notifications/initialized", "params": {}},
        )
        await self._wait_event_count("initialized-notification", 1)
        # Legacy tools/list: byte-today legacy shape (no modern envelope).
        await self._send(backend, "s1", legacy_request("tools/list", "ll1"))
        response, _seen = await self._recv_for("s1", "ll1")
        result = response["result"]
        self.assertNotIn("resultType", result)
        self.assertNotIn("ttlMs", result)
        self.assertNotIn("cacheScope", result)
        meta = result.get("_meta")
        if meta is not None:
            self.assertNotIn("io.modelcontextprotocol/serverInfo", meta)
        # Legacy tools/call: no resultType.
        await self._send(
            backend,
            "s1",
            legacy_request(
                "tools/call", "lc1", {"name": "echo", "arguments": {"value": 5}}
            ),
        )
        response, _seen = await self._recv_for("s1", "lc1")
        self.assertNotIn("resultType", response["result"])
        self.assertEqual(
            response["result"]["structuredContent"]["value"], 5
        )

    async def test_one_physical_generation_across_modern_then_legacy(self) -> None:
        backend = await self._make_backend()
        await self._attach(backend, "s1")
        # Modern bootstrap: discover initializes the physical backend once; the
        # tools/list and tools/call reuse that same single generation.
        await self._send(
            backend, "s1", modern_request("server/discover", "d1", params={}, meta=MODERN_META)
        )
        await self._recv_for("s1", "d1")
        await self._send(backend, "s1", modern_request("tools/list", "l1", params={}, meta=MODERN_META))
        await self._recv_for("s1", "l1")
        await self._send(
            backend,
            "s1",
            modern_request(
                "tools/call", "c1", params={"name": "echo", "arguments": {"value": 1}}, meta=MODERN_META
            ),
        )
        await self._recv_for("s1", "c1")
        # A later legacy client must NOT trigger a second physical initialize.
        await self._attach(backend, "s2")
        await self._send(
            backend,
            "s2",
            legacy_request(
                "initialize",
                "li2",
                {"protocolVersion": LEGACY_VERSION, "capabilities": {"tools": {}}},
            ),
        )
        response, _seen = await self._recv_for("s2", "li2")
        self.assertEqual(response["result"]["protocolVersion"], LEGACY_VERSION)
        await self._send(
            backend,
            "s2",
            {"jsonrpc": "2.0", "method": "notifications/initialized", "params": {}},
        )
        await self._send(backend, "s2", legacy_request("tools/list", "ll2"))
        response, _seen = await self._recv_for("s2", "ll2")
        self.assertNotIn("resultType", response["result"])  # legacy side unshaped
        events = await self._events()
        self.assertEqual(
            sum(1 for item in events if item["event"] == "initialize"), 1,
            f"exactly one physical initialize expected; saw {events!r}",
        )
        self.assertEqual(
            sum(1 for item in events if item["event"] == "initialized-notification"), 1
        )
        self.assertEqual(backend.generation, 1)

    async def test_discover_without_modern_meta_falls_back_locally(self) -> None:
        # server/discover is a modern-era method that always carries per-request
        # metadata.  A message without it must not reach the legacy backend:
        # answer -32601 locally so dual-era clients fall back to initialize.
        backend = await self._make_backend()
        await self._attach(backend, "s1")
        await self._send(
            backend,
            "s1",
            legacy_request("server/discover", "dd1", {"cursor": "x"}),
        )
        response, _seen = await self._recv_for("s1", "dd1")
        self.assertEqual(response["error"]["code"], -32601)
        self.assertIn("server/discover", response["error"]["message"])
        self.assertIsNone(backend.initialize_template)
        self.assertEqual(await self._events(), [])
        # The stream still works on the legacy handshake afterwards.
        await self._send(
            backend,
            "s1",
            legacy_request(
                "initialize",
                "dd2",
                {"protocolVersion": LEGACY_VERSION, "capabilities": {"tools": {}}},
            ),
        )
        response, _seen = await self._recv_for("s1", "dd2")
        self.assertEqual(response["result"]["protocolVersion"], LEGACY_VERSION)

    async def test_modern_unadvertised_methods_answer_32601(self) -> None:
        backend = await self._make_backend()
        await self._attach(backend, "s1")
        for method, rid in (
            ("ping", "x1"),
            ("resources/list", "x2"),
            ("prompts/list", "x3"),
            ("logging/setLevel", "x4"),
            ("completions/complete", "x5"),
            ("tasks/get", "x6"),
        ):
            await self._send(
                backend, "s1", modern_request(method, rid, params={}, meta=MODERN_META)
            )
            response, _seen = await self._recv_for("s1", rid)
            self.assertEqual(response["error"]["code"], -32601, f"for {method}")
        # None of the unadvertised methods initialized the physical backend.
        self.assertIsNone(backend.initialize_template)
        self.assertEqual(await self._events(), [])
        # The stream remains usable for an advertised method.
        await self._send(backend, "s1", modern_request("tools/list", "l1", params={}, meta=MODERN_META))
        response, _seen = await self._recv_for("s1", "l1")
        self.assertIn("result", response)

    async def test_modern_mrtr_retry_is_rejected_not_stripped(self) -> None:
        backend = await self._make_backend()
        await self._attach(backend, "s1")
        message = modern_request(
            "tools/call", "r1", params={"name": "echo", "arguments": {"value": 1}}, meta=MODERN_META
        )
        message["params"]["inputResponses"] = [{"toolResult": {"content": []}}]
        await self._send(backend, "s1", message)
        response, _seen = await self._recv_for("s1", "r1")
        self.assertEqual(response["error"]["code"], -32602)
        self.assertIn("inputResponses", response["error"]["message"])
        self.assertEqual(await self._events(), [])

    async def test_modern_progress_token_is_mapped_back_to_client(self) -> None:
        backend = await self._make_backend()
        await self._attach(backend, "s1")
        meta = dict(MODERN_META)
        meta["progressToken"] = "client-token-1"
        await self._send(
            backend,
            "s1",
            modern_request(
                "tools/call",
                "sp1",
                params={"name": "slow", "arguments": {"delay": 0.4}},
                meta=meta,
            ),
        )
        response, seen = await self._recv_for("s1", "sp1", timeout=10.0)
        progress = [
            message
            for message in seen
            if message.get("method") == "notifications/progress"
        ]
        self.assertTrue(progress, f"expected progress notifications; saw {seen!r}")
        for notification in progress:
            self.assertEqual(
                notification["params"]["progressToken"], "client-token-1"
            )
        self.assertGreaterEqual(
            progress[-1]["params"]["progress"], progress[0]["params"]["progress"]
        )
        self.assertEqual(response["result"]["resultType"], "complete")
        self.assertEqual(response["result"]["structuredContent"]["done"], True)

    async def test_modern_inflight_cancel_forwards_backend_request_id(self) -> None:
        backend = await self._make_backend()
        await self._attach(backend, "s1")
        await self._send(
            backend,
            "s1",
            modern_request(
                "tools/call",
                "cv1",
                params={"name": "slow", "arguments": {"delay": 0.6}},
                meta=MODERN_META,
            ),
        )
        # Wait until the slow call is the in-flight (current) request.
        deadline = time.monotonic() + 8.0
        while True:
            current = backend.current_request
            if current is not None and current.original_id == "cv1":
                break
            if time.monotonic() >= deadline:
                raise AssertionError("slow call never became current")
            await asyncio.sleep(0.01)
        backend_request_id = current.backend_id
        await self._send(
            backend,
            "s1",
            {
                "jsonrpc": "2.0",
                "method": "notifications/cancelled",
                "params": {"requestId": "cv1"},
            },
        )
        events = await self._wait_event_count("cancelled-notification", 1)
        cancel = next(
            item for item in events if item["event"] == "cancelled-notification"
        )
        # The physical backend receives the rewritten bridge request id.
        self.assertEqual(cancel["params"]["requestId"], backend_request_id)
        self.assertNotEqual(cancel["params"]["requestId"], "cv1")
        # The slow call still completes and its result is shaped modern.
        response, _seen = await self._recv_for("s1", "cv1", timeout=10.0)
        self.assertEqual(response["result"]["resultType"], "complete")

    async def test_modern_queued_cancel_answers_32800_without_backend_call(self) -> None:
        backend = await self._make_backend()
        await self._attach(backend, "s1")
        await self._send(
            backend,
            "s1",
            modern_request(
                "tools/call",
                "qt1",
                params={"name": "slow", "arguments": {"delay": 0.7}},
                meta=MODERN_META,
            ),
        )
        deadline = time.monotonic() + 8.0
        while True:
            current = backend.current_request
            if current is not None and current.original_id == "qt1":
                break
            if time.monotonic() >= deadline:
                raise AssertionError("slow call never became current")
            await asyncio.sleep(0.01)
        # Queue a second modern request behind the slow one, then cancel it.
        await self._send(
            backend,
            "s1",
            modern_request(
                "tools/call", "qt2", params={"name": "echo", "arguments": {"value": 2}}, meta=MODERN_META
            ),
        )
        await self._send(
            backend,
            "s1",
            {
                "jsonrpc": "2.0",
                "method": "notifications/cancelled",
                "params": {"requestId": "qt2"},
            },
        )
        response, _seen = await self._recv_for("s1", "qt2")
        self.assertEqual(response["error"]["code"], -32800)
        # The cancelled queued request never reached the physical backend.
        events = await self._wait_event_count("tools-call", 1)
        calls = [item for item in events if item["event"] == "tools-call"]
        self.assertEqual([call["tool"] for call in calls], ["slow"])
        # And the first request still completes.
        response, _seen = await self._recv_for("s1", "qt1", timeout=10.0)
        self.assertEqual(response["result"]["resultType"], "complete")

    async def test_modern_policy_error_is_shaped_for_modern_client(self) -> None:
        backend = await self._make_backend(
            process={"sharedState": {"rejectTools": ["tool_error"]}}
        )
        await self._attach(backend, "s1")
        await self._send(
            backend,
            "s1",
            modern_request(
                "tools/call",
                "pe1",
                params={"name": "tool_error", "arguments": {"message": "nope"}},
                meta=MODERN_META,
            ),
        )
        response, _seen = await self._recv_for("s1", "pe1")
        result = response["result"]
        self.assertEqual(result["resultType"], "complete")
        self.assertTrue(result["isError"])
        self.assertEqual(
            result["structuredContent"]["error"]["code"], "shared_view_fixed"
        )
        self.assertIn(
            "io.modelcontextprotocol/serverInfo",
            result["_meta"],
        )
        # Policy rejection is local: no backend business call happened.
        self.assertEqual(await self._events(), [])

    async def test_modern_tools_list_error_passthrough_keeps_error_shape(self) -> None:
        # The physical backend is legacy and never sees server/discover; verify
        # a normal error path is not mis-shaped into a result.
        backend = await self._make_backend()
        await self._attach(backend, "s1")
        # A backend unknown-method would only happen for non-modern flows; here
        # verify an error response from the modern forward path keeps its shape
        # by calling tools/call with a malformed payload handled by the backend.
        await self._send(
            backend,
            "s1",
            modern_request(
                "tools/call",
                "e1",
                params={"name": "no_such_tool", "arguments": {}},
                meta=MODERN_META,
            ),
        )
        await self._wait_event_count("tools-call", 1)
        response, _seen = await self._recv_for("s1", "e1")
        self.assertIn("error", response)
        self.assertEqual(response["error"]["code"], -32603)

    async def test_modern_retired_backend_error_is_normalized_for_modern_client(
        self,
    ) -> None:
        # The legacy fixture can still answer with the retired -32042 code; the
        # modern-facing projection must normalize it (never emit a retired
        # code) while preserving the message and keeping the original in data.
        backend = await self._make_backend()
        await self._attach(backend, "s1")
        await self._send(
            backend,
            "s1",
            modern_request(
                "tools/call",
                "re1",
                params={
                    "name": "retired_error",
                    "arguments": {"message": "legacy transport closed"},
                },
                meta=MODERN_META,
            ),
        )
        await self._wait_event_count("tools-call", 1)
        response, _seen = await self._recv_for("s1", "re1")
        error = response["error"]
        self.assertEqual(error["code"], JSONRPC_INTERNAL_ERROR)
        self.assertEqual(error["code"], -32603)
        self.assertEqual(error["message"], "legacy transport closed")
        self.assertEqual(error["data"]["retiredCode"], -32042)
        self.assertEqual(error["data"]["originalData"], {"fixture": True})
        self.assertEqual(
            error["data"]["code"], "retired_mcp_error_code_normalized"
        )

    async def test_modern_task_augmented_tools_list_fields_are_withheld(self) -> None:
        # A legacy backend may emit task-augmented / unknown top-level fields on
        # tools/list.  The deterministic tools-only subset must not leak them to
        # a modern client that was never advertised them.
        backend = await self._make_backend(
            env={"PROTO_FIXTURE_TASK_AUGMENTED": "1"}
        )
        await self._attach(backend, "s1")
        await self._send(
            backend,
            "s1",
            modern_request("tools/list", "ta1", params={}, meta=MODERN_META),
        )
        await self._wait_event_count("tools-list", 1)
        response, _seen = await self._recv_for("s1", "ta1")
        result = response["result"]
        self.assertEqual(result["resultType"], "complete")
        self.assertEqual(result["ttlMs"], 0)
        self.assertEqual(result["cacheScope"], "private")
        self.assertNotIn("task", result)
        self.assertNotIn("unadvertisedLegacyField", result)
        names = {tool["name"] for tool in result["tools"]}
        self.assertEqual(
            names, {"echo", "probe", "slow", "tool_error", "retired_error"}
        )

    async def test_modern_business_request_without_metadata_cannot_bypass_gate(
        self,
    ) -> None:
        # Era discipline: a logical client that adopted the modern era (a
        # supported discover) never runs the legacy initialize, so a business
        # request that drops the required per-request metadata must error and
        # never reach the physical backend.  An explicit legacy initialize on
        # the SAME logical client must remain a valid fallback that restores
        # metadata-less legacy forwarding.
        backend = await self._make_backend()
        await self._attach(backend, "s1")
        # Modern client proves modern-era capability via a supported discover.
        await self._send(
            backend,
            "s1",
            modern_request("server/discover", "g1", params={}, meta=MODERN_META),
        )
        response, _seen = await self._recv_for("s1", "g1")
        self.assertIn("result", response)
        await self._wait_event_count("initialized-notification", 1)
        # Metadata-less tools/call: error, and no backend call happens.
        await self._send(
            backend,
            "s1",
            legacy_request(
                "tools/call", "g2", {"name": "echo", "arguments": {"value": 1}}
            ),
        )
        response, _seen = await self._recv_for("s1", "g2")
        self.assertEqual(response["error"]["code"], JSONRPC_INVALID_PARAMS)
        self.assertIn("protocol metadata", response["error"]["message"])
        # Metadata-less tools/list likewise.
        await self._send(backend, "s1", legacy_request("tools/list", "g3"))
        response, _seen = await self._recv_for("s1", "g3")
        self.assertEqual(response["error"]["code"], JSONRPC_INVALID_PARAMS)
        events = await self._events()
        calls = [item for item in events if item["event"] == "tools-call"]
        lists = [item for item in events if item["event"] == "tools-list"]
        self.assertEqual(calls, [])
        self.assertEqual(lists, [])
        # Explicit legacy initialize fallback on the same logical client.
        await self._send(
            backend,
            "s1",
            legacy_request(
                "initialize",
                "g4",
                {"protocolVersion": LEGACY_VERSION, "capabilities": {"tools": {}}},
            ),
        )
        response, _seen = await self._recv_for("s1", "g4")
        self.assertEqual(response["result"]["protocolVersion"], LEGACY_VERSION)
        await self._send(
            backend,
            "s1",
            {"jsonrpc": "2.0", "method": "notifications/initialized", "params": {}},
        )
        # The same metadata-less tools/call now forwards as a legitimate legacy
        # request (one physical generation is reused; no re-initialize).
        await self._send(
            backend,
            "s1",
            legacy_request(
                "tools/call", "g5", {"name": "echo", "arguments": {"value": 9}}
            ),
        )
        events = await self._wait_event_count("tools-call", 1)
        response, _seen = await self._recv_for("s1", "g5")
        self.assertNotIn("resultType", response["result"])  # legacy bytes unshaped
        self.assertEqual(response["result"]["structuredContent"]["value"], 9)
        calls = [item for item in events if item["event"] == "tools-call"]
        self.assertEqual([call["tool"] for call in calls], ["echo"])
        self.assertEqual(
            sum(1 for item in events if item["event"] == "initialize"), 1
        )


if __name__ == "__main__":
    unittest.main()
