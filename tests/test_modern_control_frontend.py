#!/usr/bin/env python3
"""Offline tests for the modern (2026-07-28) direct frontend of the
Bridge-owned registry_mcp and control_mcp endpoints (P10).

The modern frontend is opt-in per process (``--protocol-era modern``) and is
local and fixed: no remote business backend is ever consulted.  These tests
assert the semantic contract of the pure dispatch path and of the two stdio
loops:

* ``server/discover`` answers the fixed schema with the endpoint's real
  instructions and identity, advertising exactly the tools it offers and no
  optional modern family (tasks, MRTR, resources/prompts/logging, sampling/
  elicitation/roots, subscriptions);
* ``tools/list`` returns the fixed tool definitions and the modern list
  envelope (resultType, CacheableResult ttlMs/cacheScope);
* ``tools/call`` strips the reserved modern ``_meta`` envelope before invoking
  the exact existing handlers, so tool scope and the bridge_control
  confirm/expectedGeneration gates are byte-identical to legacy and a preview
  never becomes an apply;
* malformed metadata, an unsupported requested revision, MRTR retry fields,
  and any out-of-surface method are answered with errors and never reach the
  handlers (no side effect), and modern client metadata/identity claims never
  leak into results;
* the control loop drains an oversized frame so its remainder can never be
  parsed or executed;
* the default legacy envelope is untouched.

Runs offline with the standard library only.
"""

from __future__ import annotations

import io
import json
import sys
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import bridge_protocol as bp
import bridge_runtime as br

MCP_VERSION = bp.MODERN_MCP_PROTOCOL_VERSION  # "2026-07-28"

REGISTRY_ACTIONS = {
    "bridge_registry_list": "list",
    "bridge_registry_search": "search",
    "bridge_registry_describe": "describe",
    "bridge_registry_status": "status",
}


def _meta(
    version: str = MCP_VERSION,
    capabilities: dict | None = None,
    client_info: dict | None = None,
    extra: dict | None = None,
) -> dict:
    meta: dict = {
        bp.META_PROTOCOL_VERSION_KEY: version,
        bp.META_CLIENT_CAPABILITIES_KEY: capabilities if capabilities is not None else {},
    }
    if client_info is not None:
        meta[bp.META_CLIENT_INFO_KEY] = client_info
    if extra:
        meta.update(extra)
    return meta


def _request(
    method: str,
    request_id: int,
    *,
    meta: dict | None = None,
    extra_params: dict | None = None,
) -> dict:
    params: dict = {}
    if meta is not None:
        params["_meta"] = meta
    if extra_params:
        params.update(extra_params)
    message: dict = {"jsonrpc": "2.0", "id": request_id, "method": method, "params": params}
    return message


class _FakeStdin:
    """Exposes a BytesIO payload behind the ``.buffer`` attribute the loops use."""

    def __init__(self, payload: bytes) -> None:
        self.buffer = io.BytesIO(payload)


def _run_stdio(target: object, payload: bytes) -> tuple[int, list[dict]]:
    """Run one stdio loop with an in-memory stdin/stdout pair."""
    real_stdin, real_stdout = sys.stdin, sys.stdout
    sys.stdin = _FakeStdin(payload)  # type: ignore[assignment]
    out = io.StringIO()
    sys.stdout = out
    try:
        return_code = target()  # type: ignore[operator]
    finally:
        sys.stdin, sys.stdout = real_stdin, real_stdout
    lines = [line for line in out.getvalue().split("\n") if line]
    return int(return_code or 0), [json.loads(line) for line in lines]


class ModernRegistryFrontendTest(unittest.TestCase):
    """Pure modern dispatch semantics for the read-only registry endpoint."""

    def setUp(self) -> None:
        self.tools = br._registry_tools()
        self.tool_names = {tool["name"] for tool in self.tools}
        self.actions = dict(REGISTRY_ACTIONS)
        self.query = mock.patch("bridge_runtime.local_registry_query")
        self.mock_query = self.query.start()
        self.addCleanup(self.query.stop)

    def respond(self, message: dict) -> dict:
        return br._registry_mcp_modern_response(
            message, self.tools, self.actions, "127.0.0.1", 1
        )

    def call_tool(self, name: str, arguments: dict | None = None, **kwargs) -> dict:
        params: dict = {"name": name}
        if arguments is not None:
            params["arguments"] = arguments
        meta = kwargs.pop("meta", None)
        if meta is not None:
            params["_meta"] = meta
        params.update(kwargs.get("extra_params", {}))
        return self.respond(
            {"jsonrpc": "2.0", "id": 7, "method": "tools/call", "params": params}
        )

    def test_discover_answers_fixed_schema_and_real_instructions(self) -> None:
        message = _request("server/discover", 1, meta=_meta())
        response = self.respond(message)
        self.assertEqual(response["id"], 1)
        result = response["result"]
        self.assertEqual(result["resultType"], bp.RESULT_TYPE_COMPLETE)
        self.assertEqual(result["supportedVersions"], [MCP_VERSION])
        self.assertEqual(result["capabilities"], {"tools": {}})
        self.assertEqual(result["ttlMs"], 0)
        self.assertEqual(result["cacheScope"], "private")
        self.assertEqual(result["instructions"], br.REGISTRY_INSTRUCTIONS)
        server_info = result["_meta"][bp.META_SERVER_INFO_KEY]
        self.assertEqual(server_info["name"], "win-wsl-mcp-registry")
        self.assertEqual(server_info["version"], br.SERVER_VERSION)
        # No optional modern family is advertised or echoed.
        serialized = json.dumps(result)
        for family in ("tasks", "inputRequests", "subscriptions", "logging",
                       "resources", "prompts", "sampling", "elicitation"):
            self.assertNotIn(f'"{family}"', serialized)
        self.mock_query.assert_not_called()

    def test_tools_list_returns_fixed_tools_with_modern_envelope(self) -> None:
        message = _request("tools/list", 2, meta=_meta())
        response = self.respond(message)
        result = response["result"]
        self.assertEqual(result["resultType"], bp.RESULT_TYPE_COMPLETE)
        self.assertEqual(result["ttlMs"], 0)
        self.assertEqual(result["cacheScope"], "private")
        names = {tool["name"] for tool in result["tools"]}
        self.assertEqual(names, self.tool_names)
        schemas = {tool["name"]: tool["inputSchema"] for tool in result["tools"]}
        self.assertEqual(schemas["bridge_registry_list"], {
            "type": "object", "properties": {}, "additionalProperties": False,
        })
        for tool in result["tools"]:
            self.assertEqual(tool["inputSchema"]["additionalProperties"], False)
        self.assertEqual(
            result["_meta"][bp.META_SERVER_INFO_KEY]["name"], "win-wsl-mcp-registry"
        )
        self.mock_query.assert_not_called()

    def test_tools_call_strips_meta_and_never_leaks_client_claims(self) -> None:
        self.mock_query.return_value = {
            "count": 1,
            "servers": [{"id": "wsl-bridge", "name": "WSL bridge", "status": "ready"}],
        }
        secret = "s3cr3t-client-identity-xyz"
        client_info = {"name": "leaky-client", "secret": secret}
        capabilities = {"roots": {"listChanged": True}}
        message = _request(
            "tools/call", 3,
            meta=_meta(client_info=client_info, capabilities=capabilities),
            extra_params={"name": "bridge_registry_search", "arguments": {"query": "bridge"}},
        )
        response = self.respond(message)
        self.assertNotIn("error", response)
        result = response["result"]
        self.assertEqual(result["resultType"], bp.RESULT_TYPE_COMPLETE)
        self.assertNotIn("isError", result)
        # Exactly one read-only registry query with the sanitized arguments.
        self.mock_query.assert_called_once_with(
            "127.0.0.1", 1, "remote", "search", {"query": "bridge"}
        )
        # Modern client identity/capability claims never reach outputs.
        serialized = json.dumps(response)
        self.assertNotIn(secret, serialized)
        self.assertNotIn(bp.META_CLIENT_CAPABILITIES_KEY, serialized)
        self.assertNotIn(bp.META_CLIENT_INFO_KEY, serialized)
        self.assertEqual(
            result["structuredContent"]["result"]["servers"][0]["id"], "wsl-bridge"
        )
        self.assertEqual(
            result["_meta"][bp.META_SERVER_INFO_KEY]["name"], "win-wsl-mcp-registry"
        )

    def test_registry_query_failure_is_a_tool_iserror_not_rpc_error(self) -> None:
        self.mock_query.side_effect = br.BridgeError("registry unreachable boom")
        response = self.call_tool(
            "bridge_registry_search",
            {"query": "bridge"},
            meta=_meta(),
        )
        self.assertNotIn("error", response)
        result = response["result"]
        self.assertTrue(result["isError"])
        self.assertEqual(result["resultType"], bp.RESULT_TYPE_COMPLETE)
        self.assertIn("registry unreachable boom", result["content"][0]["text"])
        self.mock_query.assert_called_once()

    def test_unknown_registry_tool_is_invalid_params_without_side_effect(self) -> None:
        response = self.call_tool("not_a_registry_tool", {}, meta=_meta())
        self.assertEqual(response["error"]["code"], -32602)
        self.assertIn("unknown registry tool", response["error"]["message"])
        self.mock_query.assert_not_called()

    def test_non_object_tool_arguments_are_rejected_without_side_effect(self) -> None:
        message = _request(
            "tools/call", 4,
            meta=_meta(),
            extra_params={"name": "bridge_registry_list", "arguments": ["x"]},
        )
        response = self.respond(message)
        self.assertEqual(response["error"]["code"], -32602)
        self.assertIn("arguments must be an object", response["error"]["message"])
        self.mock_query.assert_not_called()

    def test_missing_meta_is_an_era_hint_without_side_effect(self) -> None:
        message = {
            "jsonrpc": "2.0", "id": 5, "method": "initialize",
            "params": {"protocolVersion": "2025-06-18"},
        }
        response = self.respond(message)
        self.assertEqual(response["error"]["code"], -32601)
        self.assertIn("modern 2026-07-28 era", response["error"]["message"])
        self.assertIn("initialize", response["error"]["message"])
        self.mock_query.assert_not_called()

    def test_malformed_metadata_is_invalid_params_without_side_effect(self) -> None:
        cases = [
            _meta(version=42),
            _meta(version=""),
            _meta(capabilities=["not", "an", "object"]),
            _meta(client_info="not-an-object"),
        ]
        for index, meta in enumerate(cases):
            with self.subTest(meta=meta):
                response = self.respond(_request("tools/list", 100 + index, meta=meta))
                self.assertEqual(response["error"]["code"], -32602)
                self.mock_query.assert_not_called()

    def test_unsupported_version_is_spec_error_without_side_effect(self) -> None:
        message = _request("server/discover", 6, meta=_meta(version="2027-01-01"))
        response = self.respond(message)
        error = response["error"]
        self.assertEqual(error["code"], bp.MCP_UNSUPPORTED_PROTOCOL_VERSION)
        self.assertEqual(error["message"], bp.UNSUPPORTED_PROTOCOL_VERSION_MESSAGE)
        self.assertEqual(error["data"]["supported"], [MCP_VERSION])
        self.assertEqual(error["data"]["requested"], "2027-01-01")
        self.mock_query.assert_not_called()

    def test_mrtr_retry_fields_are_rejected_without_side_effect(self) -> None:
        message = _request(
            "tools/call", 8,
            meta=_meta(),
            extra_params={
                "name": "bridge_registry_list",
                "inputResponses": [{"requestId": "req-1", "type": "result"}],
            },
        )
        response = self.respond(message)
        self.assertEqual(response["error"]["code"], -32602)
        self.assertIn("inputResponses", response["error"]["message"])
        self.mock_query.assert_not_called()

    def test_unsupported_modern_families_are_method_not_found(self) -> None:
        for method in (
            "resources/list", "resources/read", "prompts/list", "prompts/get",
            "completions/complete", "logging/setLevel", "sampling/createMessage",
            "tasks/list", "elicitation/createMessage", "subscriptions/listen",
            "notifications/roots/list_changed", "roots/list",
        ):
            with self.subTest(method=method):
                response = self.respond(_request(method, 9, meta=_meta()))
                self.assertEqual(response["error"]["code"], -32601)
                self.assertIn("tools-only", response["error"]["message"])
                self.mock_query.assert_not_called()

    def test_ping_returns_minimal_modern_complete_result(self) -> None:
        response = self.respond(_request("ping", 10, meta=_meta()))
        self.assertNotIn("error", response)
        self.assertEqual(response["result"], {"resultType": bp.RESULT_TYPE_COMPLETE})
        self.mock_query.assert_not_called()


class ModernControlFrontendTest(unittest.TestCase):
    """Pure modern dispatch semantics for the control endpoint: the exact
    preview/confirm and expectedGeneration gates of bridge_control survive."""

    def setUp(self) -> None:
        self.tools = br._control_tools()
        self.by_name = {tool["name"]: tool for tool in self.tools}
        self.query = mock.patch("bridge_runtime.local_control_query")
        self.mock_query = self.query.start()
        self.addCleanup(self.query.stop)

    def respond(self, message: dict) -> dict:
        return br._control_mcp_modern_response(message, "127.0.0.1", 1)

    def call_tool(self, name: str, arguments: dict | None = None, meta=None) -> dict:
        params: dict = {"name": name}
        if arguments is not None:
            params["arguments"] = arguments
        if meta is None:
            meta = _meta()
        params["_meta"] = meta
        return self.respond(
            {"jsonrpc": "2.0", "id": 21, "method": "tools/call", "params": params}
        )

    def test_discover_control_identity_and_instructions(self) -> None:
        response = self.respond(_request("server/discover", 1, meta=_meta()))
        result = response["result"]
        self.assertEqual(result["resultType"], bp.RESULT_TYPE_COMPLETE)
        self.assertEqual(result["capabilities"], {"tools": {}})
        self.assertEqual(result["instructions"], br.CONTROL_INSTRUCTIONS)
        server_info = result["_meta"][bp.META_SERVER_INFO_KEY]
        self.assertEqual(server_info["name"], "win-wsl-mcp-control")
        self.assertEqual(server_info["version"], br.SERVER_VERSION)
        serialized = json.dumps(result)
        for family in ("tasks", "inputRequests", "subscriptions", "logging",
                       "resources", "prompts", "sampling"):
            self.assertNotIn(f'"{family}"', serialized)

    def test_tools_list_advertises_exact_two_tools_with_gates(self) -> None:
        response = self.respond(_request("tools/list", 2, meta=_meta()))
        result = response["result"]
        self.assertEqual(
            {tool["name"] for tool in result["tools"]},
            {"bridge_control", "bridge_diagnostics"},
        )
        control_schema = self.by_name["bridge_control"]["inputSchema"]
        action_property = control_schema["properties"]["action"]
        self.assertEqual(
            action_property["enum"],
            ["status", "drain", "refresh", "restart", "stop"],
        )
        self.assertEqual(control_schema["required"], ["action"])
        self.assertEqual(control_schema["properties"]["confirm"]["default"], False)
        self.assertEqual(control_schema["properties"]["expectedGeneration"]["minimum"], 0)
        self.assertEqual(control_schema["properties"]["reason"]["maxLength"], 512)
        self.assertFalse(control_schema["additionalProperties"])
        diagnostics_schema = self.by_name["bridge_diagnostics"]["inputSchema"]
        self.assertEqual(diagnostics_schema["properties"]["limit"]["maximum"], 100)

    def test_control_preview_is_a_single_read_only_preview(self) -> None:
        preview = {"ok": True, "preview": True, "summary": "would restart x"}
        self.mock_query.return_value = preview
        response = self.call_tool(
            "bridge_control",
            {"action": "restart", "id": "x", "expectedGeneration": 3},
        )
        self.assertNotIn("error", response)
        result = response["result"]
        self.assertNotIn("isError", result)
        self.assertEqual(result["resultType"], bp.RESULT_TYPE_COMPLETE)
        self.assertEqual(result["structuredContent"], preview)
        # One preview only: confirm stays False and nothing is applied twice.
        self.mock_query.assert_called_once_with(
            "127.0.0.1", 1, {
                "op": "control", "action": "restart", "target": "x",
                "generation": 3, "confirm": False,
                "impactOverride": False, "reason": "",
            },
        )

    def test_control_confirm_is_one_explicit_apply(self) -> None:
        applied = {"ok": True, "applied": True, "summary": "restarted x"}
        self.mock_query.return_value = applied
        response = self.call_tool(
            "bridge_control",
            {
                "action": "restart", "id": "x", "expectedGeneration": 3,
                "confirm": True, "impactOverride": True, "reason": "approved",
            },
        )
        self.assertNotIn("error", response)
        self.assertEqual(response["result"]["structuredContent"], applied)
        self.assertNotIn("isError", response["result"])
        self.mock_query.assert_called_once_with(
            "127.0.0.1", 1, {
                "op": "control", "action": "restart", "target": "x",
                "generation": 3, "confirm": True,
                "impactOverride": True, "reason": "approved",
            },
        )

    def test_control_refusal_is_a_single_iserror_without_followup_apply(self) -> None:
        refusal = {
            "ok": False,
            "code": "agent_control_disabled",
            "summary": "registration did not opt in to agent control",
        }
        self.mock_query.return_value = refusal
        response = self.call_tool(
            "bridge_control", {"action": "restart", "id": "x"}
        )
        self.assertNotIn("error", response)
        result = response["result"]
        self.assertTrue(result["isError"])
        self.assertEqual(result["structuredContent"], refusal)
        self.assertEqual(result["resultType"], bp.RESULT_TYPE_COMPLETE)
        self.assertEqual(self.mock_query.call_count, 1)

    def test_control_dryrun_argument_defaults_are_preserved(self) -> None:
        self.mock_query.return_value = {"ok": True, "preview": True}
        response = self.call_tool(
            "bridge_control", {"action": "drain", "id": "y"}
        )
        self.assertNotIn("error", response)
        request = self.mock_query.call_args.args[2]
        self.assertEqual(request["confirm"], False)
        self.assertEqual(request["generation"], None)
        self.assertEqual(request["reason"], "")

    def test_diagnostics_maps_limit_default_and_passthrough(self) -> None:
        self.mock_query.return_value = {"ok": True, "summary": "fine", "recentErrors": []}
        response = self.call_tool("bridge_diagnostics", {})
        self.assertEqual(
            self.mock_query.call_args.args[2],
            {"op": "diagnostics", "limit": 20},
        )
        self.assertNotIn("error", response)
        response = self.call_tool(
            "bridge_diagnostics", {"limit": 5}, meta=_meta()
        )
        self.assertEqual(
            self.mock_query.call_args.args[2],
            {"op": "diagnostics", "limit": 5},
        )

    def test_unknown_control_tool_is_invalid_params_without_side_effect(self) -> None:
        response = self.call_tool("bridge_restart_everything", {})
        self.assertEqual(response["error"]["code"], -32602)
        self.assertIn("unknown control tool", response["error"]["message"])
        self.mock_query.assert_not_called()

    def test_control_malformed_or_unsupported_requests_have_no_side_effect(self) -> None:
        # Unsupported version.
        response = self.respond(_request("tools/list", 3, meta=_meta(version="2029-01-01")))
        self.assertEqual(response["error"]["code"], -32022)
        self.mock_query.assert_not_called()
        # Missing _meta (legacy-style initialize on a modern endpoint).
        response = self.respond(
            {"jsonrpc": "2.0", "id": 4, "method": "initialize",
             "params": {"protocolVersion": "2025-06-18"}}
        )
        self.assertEqual(response["error"]["code"], -32601)
        self.mock_query.assert_not_called()
        # Malformed metadata values.
        for bad in (_meta(version=7), _meta(capabilities=[])):
            response = self.respond(_request("tools/call", 5, meta=bad))
            self.assertEqual(response["error"]["code"], -32602)
            self.mock_query.assert_not_called()
        # Unsupported modern family method.
        response = self.respond(_request("resources/list", 6, meta=_meta()))
        self.assertEqual(response["error"]["code"], -32601)
        self.mock_query.assert_not_called()


class ModernLoopFramingAndEraTest(unittest.TestCase):
    """Loop-level framing and era wiring for control_mcp / registry_mcp."""

    def setUp(self) -> None:
        self.query = mock.patch("bridge_runtime.local_control_query")
        self.mock_query = self.query.start()
        self.addCleanup(self.query.stop)

    def test_control_loop_drains_oversized_frame_and_never_executes_remainder(self) -> None:
        self.mock_query.return_value = {"ok": True, "summary": "ok", "recentErrors": []}
        tail = json.dumps(
            {
                "jsonrpc": "2.0", "id": 99, "method": "tools/call",
                "params": {"name": "bridge_diagnostics", "arguments": {}},
            },
            separators=(",", ":"),
        ).encode("utf-8")
        filler = b"x" * (br.MAX_FRAME_BYTES + 1)
        payload = (
            filler
            + tail
            + b"\n"
            + b'{"jsonrpc":"2.0","id":1,"method":"ping"}\n'
        )
        return_code, responses = _run_stdio(
            lambda: br.control_mcp("127.0.0.1", 1), payload
        )
        self.assertEqual(return_code, 0)
        # Exactly the oversize error and the trailing valid request: the
        # drained remainder (a would-be tools/call) is never parsed or run.
        self.assertEqual(len(responses), 2)
        self.assertEqual(responses[0]["id"], None)
        self.assertEqual(responses[0]["error"]["code"], -32700)
        self.assertIn("exceeds the limit", responses[0]["error"]["message"])
        self.assertEqual(responses[1]["id"], 1)
        self.assertEqual(responses[1]["result"], {})
        self.mock_query.assert_not_called()

    def test_control_loop_modern_era_serves_discover_and_gates_legacy(self) -> None:
        payload = (
            json.dumps(
                _request("server/discover", 1, meta=_meta()),
                separators=(",", ":"),
            ).encode("utf-8")
            + b"\n"
            + b'{"jsonrpc":"2.0","id":2,"method":"initialize",'
            + b'"params":{"protocolVersion":"2025-06-18"}}\n'
        )
        return_code, responses = _run_stdio(
            lambda: br.control_mcp("127.0.0.1", 1, protocol_era="modern"), payload
        )
        self.assertEqual(return_code, 0)
        self.assertEqual(len(responses), 2)
        discover = responses[0]["result"]
        self.assertEqual(discover["capabilities"], {"tools": {}})
        self.assertEqual(discover["instructions"], br.CONTROL_INSTRUCTIONS)
        self.assertEqual(discover["_meta"][bp.META_SERVER_INFO_KEY]["name"],
                         "win-wsl-mcp-control")
        self.assertEqual(responses[1]["error"]["code"], -32601)
        self.assertIn("legacy initialize", responses[1]["error"]["message"])
        self.mock_query.assert_not_called()

    def test_control_loop_default_legacy_era_ignores_modern_meta(self) -> None:
        # Without --protocol-era modern the endpoint keeps its exact legacy
        # envelope: a modern-flavoured tools/list is served legacy-shaped.
        payload = (
            json.dumps(
                _request("tools/list", 1, meta=_meta()),
                separators=(",", ":"),
            ).encode("utf-8")
            + b"\n"
        )
        return_code, responses = _run_stdio(
            lambda: br.control_mcp("127.0.0.1", 1), payload
        )
        self.assertEqual(return_code, 0)
        self.assertEqual(len(responses), 1)
        result = responses[0]["result"]
        self.assertEqual(
            {tool["name"] for tool in result["tools"]},
            {"bridge_control", "bridge_diagnostics"},
        )
        self.assertNotIn("resultType", result)

    def test_registry_loop_modern_era_serves_discover(self) -> None:
        payload = (
            json.dumps(
                _request("server/discover", 1, meta=_meta()),
                separators=(",", ":"),
            ).encode("utf-8")
            + b"\n"
        )
        return_code, responses = _run_stdio(
            lambda: br.registry_mcp("127.0.0.1", 1, protocol_era="modern"), payload
        )
        self.assertEqual(return_code, 0)
        self.assertEqual(len(responses), 1)
        discover = responses[0]["result"]
        self.assertEqual(discover["instructions"], br.REGISTRY_INSTRUCTIONS)
        self.assertEqual(discover["_meta"][bp.META_SERVER_INFO_KEY]["name"],
                         "win-wsl-mcp-registry")


class LegacyDefaultRegressionTest(unittest.TestCase):
    """The default (no flag) legacy dispatch surface is byte-compatible."""

    def test_registry_legacy_initialize_and_list_are_unchanged(self) -> None:
        tools = br._registry_tools()
        init = br._registry_mcp_dispatch(
            {
                "jsonrpc": "2.0", "id": 1, "method": "initialize",
                "params": {"protocolVersion": "2025-06-18"},
            },
            tools,
            REGISTRY_ACTIONS,
            "127.0.0.1",
            1,
        )
        self.assertEqual(init["protocolVersion"], "2025-06-18")
        self.assertEqual(init["capabilities"], {"tools": {"listChanged": False}})
        self.assertEqual(init["serverInfo"]["name"], "win-wsl-mcp-registry")
        self.assertEqual(init["instructions"], br.REGISTRY_INSTRUCTIONS)
        listed = br._registry_mcp_dispatch(
            {"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}},
            tools,
            REGISTRY_ACTIONS,
            "127.0.0.1",
            1,
        )
        self.assertEqual({tool["name"] for tool in listed["tools"]},
                         {tool["name"] for tool in tools})
        self.assertNotIn("resultType", listed)

    def test_control_legacy_initialize_is_unchanged(self) -> None:
        init = br._control_mcp_dispatch(
            {
                "jsonrpc": "2.0", "id": 1, "method": "initialize",
                "params": {"protocolVersion": "2025-06-18"},
            },
            "127.0.0.1",
            1,
        )
        self.assertEqual(init["protocolVersion"], br.SHARED_MCP_PROTOCOL_VERSION)
        self.assertEqual(init["serverInfo"]["name"], "win-wsl-mcp-control")
        self.assertEqual(init["instructions"], br.CONTROL_INSTRUCTIONS)

    def test_legacy_dispatch_accepts_meta_laden_params_without_modern_shaping(self) -> None:
        # A modern-meta tools/call sent to the DEFAULT (legacy) dispatcher is
        # executed by the legacy path exactly as before: meta is inert there,
        # the request is a plain tools/call.
        with mock.patch(
            "bridge_runtime.local_control_query",
            return_value={"ok": True, "preview": True, "summary": "s"},
        ) as query:
            result = br._control_mcp_dispatch(
                {
                    "jsonrpc": "2.0", "id": 1, "method": "tools/call",
                    "params": {
                        "name": "bridge_control",
                        "arguments": {"action": "status", "id": "x"},
                        "_meta": _meta(),
                    },
                },
                "127.0.0.1",
                1,
            )
        self.assertIn("structuredContent", result)
        self.assertNotIn("resultType", result)
        query.assert_called_once_with(
            "127.0.0.1", 1, {
                "op": "control", "action": "status", "target": "x",
                "generation": None, "confirm": False,
                "impactOverride": False, "reason": "",
            },
        )


class ModernCliFlagTest(unittest.TestCase):
    def test_registry_and_control_parse_protocol_era_flag(self) -> None:
        parser = br.build_parser("wsl", 8766, "connect")
        default_registry = parser.parse_args(["registry-mcp"])
        self.assertEqual(default_registry.protocol_era, "legacy")
        modern_registry = parser.parse_args(
            ["registry-mcp", "--protocol-era", "modern"]
        )
        self.assertEqual(modern_registry.protocol_era, "modern")
        modern_control = parser.parse_args(
            ["control-mcp", "--protocol-era", "modern"]
        )
        self.assertEqual(modern_control.protocol_era, "modern")
        with self.assertRaises(SystemExit):
            parser.parse_args(["control-mcp", "--protocol-era", "ancient"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
