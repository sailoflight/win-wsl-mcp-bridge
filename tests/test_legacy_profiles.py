#!/usr/bin/env python3
"""Canonical shared legacy baseline: physical ``2025-11-25`` request,
server-choose actual-backend negotiation, and the four frozen tools profiles.

Three layers:

* Pure goldens (``LegacyProfileProjectionTest``): the era-gate matrix in
  ``bridge_protocol`` for every (logical client profile x backend actual)
  combination -- tool-catalog keys and ``structuredContent`` presence per
  frozen revision, exactly the fields the versioned fixture emits.
* SharedBackend observation units (``SharedBackendBaselineUnitTest``): the
  physical session is *requested* at the canonical ``2025-11-25``; any of the
  four verified legacy answers is accepted, recorded, and journaled as the
  generation's actual backend revision; a modern/missing/unusable answer fails
  closed (controlled error template, no fabricated revision, journaled
  rejection); journals report the pre-observation baseline before any session
  is observed and the actual revision afterwards.
* Black-box acceptance (``LegacySharedAcceptanceTest``): two real serve nodes
  sharing a legacy-versioned fixture whose actual revision is ``2025-11-25``.
  Proves the wire request version, one generation for four logical client
  profiles, per-profile projection of catalog and call results, crash-recovery
  restart that re-negotiates the same actual revision (no replay), and the
  journaled ``shared-backend-session`` observation rows.
"""

from __future__ import annotations

import json
import os
import socket
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from bridge_protocol import (
    is_verified_legacy_protocol_version,
    legacy_content_kinds,
    legacy_server_capability_families,
    legacy_tools_profile,
    project_legacy_content_item,
    project_legacy_server_capabilities,
    project_legacy_tool,
    project_legacy_tools_call_result,
    project_legacy_tools_list,
)
from bridge_runtime import (
    BridgeError,
    EventJournal,
    Registry,
    SharedBackend,
    SHARED_BACKEND_REQUEST_PROTOCOL_VERSION,
    SHARED_COMPATIBLE_PROTOCOL_VERSIONS,
    SHARED_MCP_PROTOCOL_VERSION,
    _event_journal_path,
    local_registry_query,
)

FIXTURE = ROOT / "tests" / "fixtures" / "legacy_versioned_fixture_mcp.py"
WIN = ROOT / "win-bridge-mcp" / "bridge.py"
WSL = ROOT / "wsl-bridge-mcp" / "bridge.py"

LEGACY_PROFILES = ("2024-11-05", "2025-03-26", "2025-06-18", "2025-11-25")
STRUCTURED_CONTENT_ANCHOR = "2025-06-18"

_ALLOCATED_TEST_PORTS: set[int] = set()
_ALLOCATED_TEST_PORTS_LOCK = threading.Lock()


def free_port() -> int:
    while True:
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = int(sock.getsockname()[1])
        with _ALLOCATED_TEST_PORTS_LOCK:
            if port not in _ALLOCATED_TEST_PORTS:
                _ALLOCATED_TEST_PORTS.add(port)
                return port


# --------------------------------------------------------------------------
# Pure era-gate goldens
# --------------------------------------------------------------------------

def _full_echo_tool() -> dict:
    return {
        "name": "echo",
        "description": "echo a value back",
        "inputSchema": {"type": "object", "properties": {"value": {"type": "string"}}},
        "annotations": {"title": "Echo tool", "readOnlyHint": True},
        "title": "Echo tool",
        "outputSchema": {"type": "object"},
        "icons": [{"src": "https://example.invalid/echo.png", "mimeType": "image/png"}],
        "futureUnknown": {"x": 1},
    }


def _expected_tool_keys(profile: str) -> set[str]:
    keys = {"name", "description", "inputSchema"}
    gates = legacy_tools_profile(profile)
    if gates["annotations"]:
        keys.add("annotations")
    if gates["title"]:
        keys.add("title")
    if gates["outputSchema"]:
        keys.add("outputSchema")
    if gates["icons"]:
        keys.add("icons")
    return keys


class LegacyProfileProjectionTest(unittest.TestCase):
    """Era-gate golden matrix: four logical profiles over a full-era tool."""

    def test_tool_keys_are_the_profile_intersection_of_a_full_era_catalog(self) -> None:
        for profile in LEGACY_PROFILES:
            with self.subTest(profile=profile):
                projected = project_legacy_tool(profile, _full_echo_tool())
                self.assertIsNotNone(projected)
                self.assertEqual(set(projected.keys()), _expected_tool_keys(profile))
                if profile < "2025-11-25":
                    self.assertNotIn("icons", projected)
                if profile < "2025-06-18":
                    self.assertNotIn("title", projected)
                    self.assertNotIn("outputSchema", projected)
                if profile < "2025-03-26":
                    self.assertNotIn("annotations", projected)
                # Extension/unknown keys never leak on any profile.
                self.assertNotIn("futureUnknown", projected)

    def test_icons_are_a_plural_list_of_objects_with_src(self) -> None:
        # Tool.icons (schema >= 2025-11-25) is a list of Icon objects, each
        # carrying a string src; it is never a singular scalar field.
        tool = {"name": "t", "icons": [{"src": "https://x/i.png", "mimeType": "image/png"}]}
        self.assertEqual(project_legacy_tool("2025-11-25", tool)["icons"], tool["icons"])
        self.assertNotIn("icons", project_legacy_tool("2025-06-18", tool))
        for bad in ({"name": "t", "icons": "https://x/i.png"},):
            self.assertNotIn("icons", project_legacy_tool("2025-11-25", bad))

    def test_tools_list_result_projects_each_entry_and_keeps_next_cursor(self) -> None:
        result = {"tools": [_full_echo_tool(), {"name": "plain"}], "nextCursor": "p2"}
        for profile in LEGACY_PROFILES:
            with self.subTest(profile=profile):
                out = project_legacy_tools_list(profile, result)
                self.assertEqual(out["nextCursor"], "p2")
                self.assertEqual(len(out["tools"]), 2)
                self.assertEqual(
                    set(out["tools"][0].keys()), _expected_tool_keys(profile)
                )
                self.assertEqual(out["tools"][1], {"name": "plain"})

    def test_call_result_structured_content_gate(self) -> None:
        result = {"content": [{"type": "text", "text": "ok"}], "structuredContent": {"v": 1}}
        for profile in LEGACY_PROFILES:
            with self.subTest(profile=profile):
                out = project_legacy_tools_call_result(profile, result)
                has = profile >= STRUCTURED_CONTENT_ANCHOR
                self.assertEqual("structuredContent" in out, has)
                self.assertEqual(out["content"], result["content"])
                if has:
                    self.assertEqual(out["structuredContent"], {"v": 1})

    def test_verified_profile_predicates_and_fail_closed_inputs(self) -> None:
        for profile in LEGACY_PROFILES:
            self.assertTrue(is_verified_legacy_protocol_version(profile))
        for bad in ("2026-07-28", "1999-01-01", "", None, 5, [], "banana"):
            self.assertFalse(is_verified_legacy_protocol_version(bad))
        with self.assertRaises(ValueError):
            legacy_tools_profile("2026-07-28")
        with self.assertRaises(ValueError):
            project_legacy_tools_list("2026-07-28", {"tools": []})
        with self.assertRaises(ValueError):
            project_legacy_tools_call_result("2024-11-05", "not-an-object")
        # A malformed tool entry is withheld, never leaked.
        self.assertIsNone(project_legacy_tool("2025-11-25", {"icons": [{"src": "x"}]}))

    # ------------------------------------------------------------------
    # Content variants: CallToolResult.content kinds per frozen profile.
    # ------------------------------------------------------------------

    def _content_vectors(self) -> dict[str, dict]:
        return {
            "text": {"type": "text", "text": "plain"},
            "image": {"type": "image", "data": "YWJj", "mimeType": "image/png"},
            "resource": {
                "type": "resource",
                "resource": {"uri": "file:///a.txt", "mimeType": "text/plain", "text": "body"},
            },
            "audio": {"type": "audio", "data": "QUJD", "mimeType": "audio/wav"},
            "resource_link": {"type": "resource_link", "uri": "file:///ref.txt"},
        }

    def test_content_kind_sets_match_schema_introductions(self) -> None:
        # Verified from the tagged schemas: 2024-11-05 has text/image/resource;
        # AudioContent arrives in 2025-03-26; ResourceLink arrives in 2025-06-18
        # and the set is unchanged at 2025-11-25.
        self.assertEqual(
            legacy_content_kinds("2024-11-05"), frozenset({"text", "image", "resource"})
        )
        self.assertEqual(
            legacy_content_kinds("2025-03-26"),
            frozenset({"text", "image", "audio", "resource"}),
        )
        self.assertEqual(
            legacy_content_kinds("2025-06-18"),
            frozenset({"text", "image", "audio", "resource", "resource_link"}),
        )
        self.assertEqual(
            legacy_content_kinds("2025-11-25"),
            frozenset({"text", "image", "audio", "resource", "resource_link"}),
        )

    def test_representable_common_kinds_pass_untouched_everywhere(self) -> None:
        vectors = self._content_vectors()
        for profile in LEGACY_PROFILES:
            with self.subTest(profile=profile):
                for kind in ("text", "image", "resource"):
                    item = vectors[kind]
                    projected = project_legacy_tools_call_result(
                        profile, {"content": [item], "isError": False}
                    )
                    # Verbatim value bytes: no reshaping of representable content.
                    self.assertEqual(projected["content"], [item])

    def test_mixed_content_golden_per_profile(self) -> None:
        vectors = self._content_vectors()
        vectors["resource"] = {
            "type": "resource",
            "resource": {
                "uri": "file:///blob.bin",
                "mimeType": "application/octet-stream",
                "blob": "AAEC",
                "_meta": {"resource-meta": 1},
            },
            "_meta": {"item-meta": 1},
        }
        vectors["text"] = {"type": "text", "text": "plain", "_meta": {"m": 1}}
        full_mixed = list(vectors.values())  # text,image,resource,audio,resource_link
        # A backend running actual 2025-11-25 legitimately emits all five
        # kinds.  Profiles that can represent every present kind pass the whole
        # array through untouched; a profile that cannot represent even one
        # kind fails the whole call explicitly -- it never silently drops the
        # audio/resource_link items and reports success.
        for profile in ("2025-06-18", "2025-11-25"):
            with self.subTest(profile=profile):
                result = project_legacy_tools_call_result(
                    profile, {"content": full_mixed, "isError": False}
                )
                self.assertEqual(
                    [c["type"] for c in result["content"]],
                    ["text", "image", "resource", "audio", "resource_link"],
                )
                # Item-level and ResourceContents-level _meta exist from
                # 2025-06-18 and are preserved verbatim.
                self.assertEqual(result["content"][0]["_meta"], {"m": 1})
                res = result["content"][2]
                self.assertEqual(res["_meta"], {"item-meta": 1})
                self.assertEqual(res["resource"]["_meta"], {"resource-meta": 1})
                self.assertNotIn("structuredContent", result)
                self.assertFalse(result["isError"])
        for profile in ("2024-11-05", "2025-03-26"):
            with self.subTest(profile=profile, expect="whole-call error"):
                with self.assertRaises(ValueError) as caught:
                    project_legacy_tools_call_result(
                        profile, {"content": full_mixed, "isError": False}
                    )
                self.assertIn(profile, str(caught.exception))

    def test_unrepresentable_kinds_fail_explicitly_never_silently_drop(self) -> None:
        vectors = self._content_vectors()
        cases = [
            # (profile, content, kind-that-must-fail, reason)
            ("2024-11-05", vectors["audio"], "audio", "audio predates 2025-03-26"),
            ("2024-11-05", vectors["resource_link"], "resource_link",
             "resource_link predates 2025-06-18"),
            ("2025-03-26", vectors["resource_link"], "resource_link",
             "resource_link predates 2025-06-18"),
        ]
        for profile, item, kind, reason in cases:
            with self.subTest(profile=profile, kind=kind, reason=reason):
                with self.assertRaises(ValueError) as caught:
                    # A representable item before the unrepresentable one must
                    # not turn into a partial result: the whole call fails.
                    project_legacy_tools_call_result(
                        profile, {"content": [vectors["text"], item]}
                    )
                self.assertIn(kind, str(caught.exception))
                self.assertIn(profile, str(caught.exception))
        # A kind no frozen profile can represent is never invented as usable.
        for profile in LEGACY_PROFILES:
            with self.subTest(profile=profile, kind="future_video"):
                with self.assertRaises(ValueError):
                    project_legacy_tools_call_result(
                        profile, {"content": [{"type": "future_video", "url": "x"}]}
                    )
        # Malformed items (no type, non-object) fail identically.
        for malformed in (["nope"], [{"text": "no type"}], [None]):
            for profile in ("2024-11-05", "2025-11-25"):
                with self.subTest(profile=profile, malformed=malformed):
                    with self.assertRaises(ValueError):
                        project_legacy_tools_call_result(
                            profile, {"content": [{"type": "text"}] + malformed}
                        )

    def test_content_meta_gating_leaves_representable_payload_intact(self) -> None:
        # Text is representable on every profile; its _meta (2025-06-18+) is
        # stripped only on the older profiles, payload and annotations never
        # touched.
        text = {"type": "text", "text": "plain", "_meta": {"m": 1},
                "annotations": {"audience": ["user"]}}
        for profile in LEGACY_PROFILES:
            with self.subTest(profile=profile, kind="text"):
                projected = project_legacy_content_item(profile, text)
                self.assertEqual(projected["text"], "plain")
                self.assertEqual(projected["annotations"], {"audience": ["user"]})
                if profile < "2025-06-18":
                    self.assertNotIn("_meta", projected)
                else:
                    self.assertEqual(projected["_meta"], {"m": 1})
        # Audio is representable from 2025-03-26; at 2024-11-05 it is not.
        audio = {"type": "audio", "data": "QUJD", "mimeType": "audio/wav",
                 "_meta": {"m": 1}, "annotations": {"audience": ["user"]}}
        for profile in ("2025-03-26", "2025-06-18", "2025-11-25"):
            with self.subTest(profile=profile, kind="audio"):
                projected = project_legacy_content_item(profile, audio)
                if profile < "2025-06-18":
                    self.assertNotIn("_meta", projected)
                else:
                    self.assertEqual(projected["_meta"], {"m": 1})
                self.assertEqual(projected["data"], "QUJD")
                self.assertEqual(projected["mimeType"], "audio/wav")
                self.assertEqual(projected["annotations"], {"audience": ["user"]})
        with self.assertRaises(ValueError):
            project_legacy_content_item("2024-11-05", audio)

    def test_non_array_or_null_content_is_handled_explicitly(self) -> None:
        with self.assertRaises(ValueError):
            project_legacy_tools_call_result(
                "2025-11-25", {"content": {"type": "text", "text": "not-an-array"}}
            )
        # No content key or explicit null content: no items to project.
        self.assertEqual(
            project_legacy_tools_call_result("2025-03-26", {"isError": False}),
            {"isError": False},
        )
        self.assertNotIn(
            "content",
            project_legacy_tools_call_result(
                "2025-03-26", {"content": None, "isError": True}
            ),
        )

    # ------------------------------------------------------------------
    # ServerCapabilities intersection (initialize replay).
    # ------------------------------------------------------------------

    def test_server_capability_families_follow_schema_introductions(self) -> None:
        # Verified from the tagged schemas: experimental/logging/prompts/
        # resources/tools exist from 2024-11-05; completions arrives in
        # 2025-03-26; tasks arrives only in 2025-11-25.
        self.assertEqual(
            legacy_server_capability_families("2024-11-05"),
            frozenset({"experimental", "logging", "prompts", "resources", "tools"}),
        )
        for profile in ("2025-03-26", "2025-06-18"):
            self.assertNotIn("tasks", legacy_server_capability_families(profile))
            self.assertIn("completions", legacy_server_capability_families(profile))
        self.assertIn("tasks", legacy_server_capability_families("2025-11-25"))

    def test_capability_replay_is_implemented_tools_only_intersection(self) -> None:
        # A 2025-11-25 actual may legitimately advertise tasks, prompts and an
        # ad-hoc family; the virtualizer implements only tools.
        advertised = {
            "tools": {"listChanged": True},
            "tasks": {"list": {}, "cancel": {}, "requests": {"tools": {"call": {}}}},
            "prompts": {"listChanged": True},
            "completions": {},
            "superstream": {"enabled": True},
        }
        for profile in LEGACY_PROFILES:
            with self.subTest(profile=profile):
                projected = project_legacy_server_capabilities(profile, advertised)
                # tasks is withheld even at 2025-11-25 (unimplemented family);
                # prompts/completions/unknown families likewise.
                self.assertEqual(projected, {"tools": {"listChanged": True}})

    def test_capability_never_fabricates_from_absent_or_malformed_caps(self) -> None:
        # No tools advertisement -> no tools family invented.
        self.assertEqual(
            project_legacy_server_capabilities("2025-11-25", {}), {}
        )
        self.assertEqual(
            project_legacy_server_capabilities("2025-11-25", None), {}
        )
        self.assertEqual(
            project_legacy_server_capabilities("2025-11-25", "not-a-dict"), {}
        )
        self.assertEqual(
            project_legacy_server_capabilities(
                "2025-11-25", {"prompts": {"listChanged": True}}
            ),
            {},
        )
        # tools with only unknown leaves -> tools with no leaves.
        self.assertEqual(
            project_legacy_server_capabilities(
                "2025-11-25", {"tools": {"execution": {}, "futureLeaf": 1}}
            ),
            {"tools": {}},
        )
        # listChanged false/absent is not turned into a fabricated true.
        self.assertEqual(
            project_legacy_server_capabilities("2025-11-25", {"tools": {}}),
            {"tools": {}},
        )
        self.assertEqual(
            project_legacy_server_capabilities(
                "2025-11-25", {"tools": {"listChanged": False}}
            ),
            {"tools": {}},
        )
        # An unverified logical revision fails closed.
        with self.assertRaises(ValueError):
            project_legacy_server_capabilities("2026-07-28", {"tools": {}})

    def test_capability_projection_needs_no_backend_version_data(self) -> None:
        # The intersection derives entirely from the observed capabilities and
        # the logical profile; it must not depend on (or fabricate) the
        # physical backend's negotiated revision, which may be unobserved.
        advertised = {"tools": {"listChanged": True}, "tasks": {"list": {}}}
        for profile in LEGACY_PROFILES:
            self.assertEqual(
                project_legacy_server_capabilities(profile, advertised),
                {"tools": {"listChanged": True}},
            )


# --------------------------------------------------------------------------
# SharedBackend physical-baseline observation units (no subprocesses)
# --------------------------------------------------------------------------

class _JournalNode:
    def __init__(self, side: str, store: object) -> None:
        self.side = side
        self.journal = store


class SharedBackendBaselineUnitTest(unittest.TestCase):
    def _backend(self, root: Path) -> tuple[SharedBackend, EventJournal]:
        journal = EventJournal(root / "events.sqlite3", max_events=100)
        backend = SharedBackend(
            node=_JournalNode("win", journal),
            target="legacy-shared",
            entry={"process": {}},
        )
        return backend, journal

    def _rows(self, journal: EventJournal, category: str) -> list[dict]:
        return [
            row
            for row in journal.recent(50)
            if row.get("category") == category
        ]

    def test_request_constant_is_the_canonical_legacy_revision(self) -> None:
        self.assertEqual(SHARED_BACKEND_REQUEST_PROTOCOL_VERSION, "2025-11-25")
        self.assertIn(SHARED_BACKEND_REQUEST_PROTOCOL_VERSION,
                      SHARED_COMPATIBLE_PROTOCOL_VERSIONS)
        # The pre-observation baseline reported by journals/errors stays the
        # pre-upgrade physical wire revision until a session is observed.
        self.assertIn(SHARED_MCP_PROTOCOL_VERSION, SHARED_COMPATIBLE_PROTOCOL_VERSIONS)

    def test_each_verified_legacy_answer_is_accepted_and_journaled(self) -> None:
        for actual in LEGACY_PROFILES:
            with tempfile.TemporaryDirectory() as temp:
                backend, journal = self._backend(Path(temp))
                self.assertIsNone(backend.backend_negotiated_version)
                backend.initialize_template = {
                    "result": {
                        "protocolVersion": actual,
                        "capabilities": {"tools": {"listChanged": True}},
                        "serverInfo": {"name": "x", "version": "1"},
                    }
                }
                backend._observe_backend_initialize()
                # Accepted and recorded exactly as answered (server-choose).
                self.assertEqual(backend.backend_negotiated_version, actual)
                self.assertEqual(backend.backend_server_info, {"name": "x", "version": "1"})
                # The success template is preserved; no fabricated error.
                self.assertIn("result", backend.initialize_template)
                rows = self._rows(journal, "shared-backend-session")
                self.assertEqual(len(rows), 1, rows)
                self.assertEqual(rows[0]["outcome"], "observed")
                metadata = rows[0]["metadata"]
                self.assertEqual(metadata["requestedVersion"],
                                 SHARED_BACKEND_REQUEST_PROTOCOL_VERSION)
                self.assertEqual(metadata["negotiatedVersion"], actual)
                self.assertEqual(metadata["backendVersion"], actual)
                self.assertEqual(rows[0]["target"], "legacy-shared")
                self.assertEqual(rows[0]["side"], "win")

    def test_modern_or_unusable_answers_fail_closed_without_fabrication(self) -> None:
        vectors = [
            # (result, expected observed fragment, reason)
            ({"protocolVersion": "2026-07-28"}, "2026-07-28",
             "modern revision is never accepted as a legacy backend"),
            ({"capabilities": {"tools": {}}}, None,
             "missing protocolVersion fails closed"),
            ({"protocolVersion": "banana"}, "banana",
             "malformed protocolVersion fails closed"),
            ({"protocolVersion": "1999-01-01"}, "1999-01-01",
             "pre-legacy revision fails closed"),
        ]
        for result, observed, reason in vectors:
            with tempfile.TemporaryDirectory() as temp, self.subTest(reason=reason):
                backend, journal = self._backend(Path(temp))
                backend.initialize_template = {"result": result}
                backend._observe_backend_initialize()
                self.assertIsNone(backend.backend_negotiated_version)
                self.assertIsNone(backend.backend_server_info)
                # The template was replaced with a controlled error: waiters
                # (modern bootstrap and legacy initialize replay) fail closed.
                self.assertNotIn("result", backend.initialize_template)
                self.assertIn("error", backend.initialize_template)
                data = backend.initialize_template["error"]["data"]
                self.assertEqual(data["code"], "backend_protocol_version_unusable")
                self.assertEqual(data["requested"],
                                 SHARED_BACKEND_REQUEST_PROTOCOL_VERSION)
                self.assertEqual(data["observed"], observed)
                self.assertEqual(
                    set(data["supportedProtocolVersions"]),
                    SHARED_COMPATIBLE_PROTOCOL_VERSIONS,
                )
                rows = self._rows(journal, "shared-backend-session")
                self.assertEqual(len(rows), 1, rows)
                self.assertEqual(rows[0]["outcome"], "rejected")
                self.assertEqual(rows[0]["metadata"]["backendVersion"], None)

    def test_backend_init_error_is_kept_and_journaled_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            backend, journal = self._backend(Path(temp))
            backend.initialize_template = {
                "jsonrpc": "2.0",
                "error": {"code": -32601, "message": "Method not found: initialize"},
            }
            backend._observe_backend_initialize()
            self.assertIsNone(backend.backend_negotiated_version)
            # The backend's own error is preserved (already fail-closed).
            self.assertIn("error", backend.initialize_template)
            rows = self._rows(journal, "shared-backend-session")
            self.assertEqual(len(rows), 1, rows)
            self.assertEqual(rows[0]["outcome"], "rejected")
            self.assertEqual(rows[0]["metadata"]["detail"], "backend_error")

    def test_journal_backend_version_is_baseline_before_and_actual_after(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            backend, journal = self._backend(Path(temp))
            backend._journal_initialize_negotiation(
                "accepted", "2025-11-25", "2025-11-25"
            )
            rows = self._rows(journal, "shared-initialize")
            self.assertEqual(len(rows), 1, rows)
            # Pre-observation: the normalized baseline, never a lie about the
            # session (which has not been observed yet).
            self.assertEqual(rows[0]["metadata"]["backendVersion"],
                             SHARED_MCP_PROTOCOL_VERSION)
            backend.initialize_template = {
                "result": {"protocolVersion": "2025-06-18",
                           "capabilities": {"tools": {}}}
            }
            backend._observe_backend_initialize()
            self.assertEqual(backend.backend_negotiated_version, "2025-06-18")
            backend._journal_initialize_negotiation(
                "accepted", "2025-03-26", "2025-03-26"
            )
            rows = self._rows(journal, "shared-initialize")
            self.assertEqual(len(rows), 2, rows)
            # Post-observation rows carry the actual negotiated revision.
            self.assertEqual(rows[1]["metadata"]["backendVersion"], "2025-06-18")


# --------------------------------------------------------------------------
# Black-box acceptance: two real serve nodes + a versioned fixture
# --------------------------------------------------------------------------

def _write_registries(win: Path, wsl: Path, state: Path, config: Path | None) -> None:
    manifest = win.with_name(win.name + ".manifest.json")
    entry: dict = {
        "id": "legacy-shared",
        "name": "Legacy versioned shared fixture",
        "summary": "Canonical legacy shared-baseline fixture.",
        "command": sys.executable,
        "args": [str(FIXTURE), "--state", str(state / "fixture.jsonl")],
        "cwd": str(ROOT),
        "env": {},
        "process": {
            "multiProcessAllowed": False,
        },
        "capabilityGroups": ["test", "shared-backend"],
        "artifactDelivery": {"enabled": False},
    }
    if config is not None:
        entry["args"].extend(["--config", str(config)])
    manifest.write_text(
        json.dumps({"servers": [entry]}, indent=2), encoding="utf-8"
    )
    Registry.initialize_database(win, manifest, replace=True)
    empty = wsl.with_name(wsl.name + ".manifest.json")
    empty.write_text(json.dumps({"servers": []}), encoding="utf-8")
    Registry.initialize_database(wsl, empty, replace=True)


class LegacyClient:
    """Raw JSON-line client to a serve node's local listener (like the shared
    acceptance tests use), speaking MCP JSON-RPC frames directly."""

    def __init__(self, port: int, target: str):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=10)
        self.sock.settimeout(10)
        self.buffer = bytearray()
        self.sock.sendall(
            json.dumps({"op": "connect", "target": target}, separators=(",", ":")).encode()
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
            message = self._recv_json()
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
        client_name: str = "legacy-probe",
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
            request_id, "tools/call", {"name": name, "arguments": arguments or {}}
        )

    def _recv_json(self) -> dict:
        while b"\n" not in self.buffer:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise EOFError("legacy client stream closed")
            self.buffer.extend(chunk)
        raw, _, remainder = self.buffer.partition(b"\n")
        self.buffer = bytearray(remainder)
        return json.loads(raw)

    def close(self) -> None:
        try:
            self.sock.close()
        except OSError:
            pass


class LegacySharedAcceptanceTest(unittest.TestCase):
    """End-to-end canonical baseline with a fixture whose actual revision is
    ``2025-11-25`` (its full-era catalog exercises every projection gate).

    The fixture is configured to advertise extra families beyond tools
    (``tasks`` -- legal at actual 2025-11-25 -- plus prompts/completions and
    an ad-hoc unknown family), so every legacy initialize is a live canary
    that the bridge replays only its implemented tools family and never leaks
    a family or capability field the logical profile cannot represent or the
    virtualizer does not implement.
    """

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp = tempfile.TemporaryDirectory()
        root = Path(cls.temp.name)
        cls.root = root
        cls.state = root / "state"
        cls.state.mkdir()
        (cls.state / "fixture.jsonl").touch()
        cls.win_registry = root / "win.sqlite3"
        cls.wsl_registry = root / "wsl.sqlite3"
        cls.fixture_config = root / "fixture.config.json"
        cls.fixture_config.write_text(
            json.dumps(
                {
                    "extra_capabilities": {
                        "tasks": {
                            "list": {},
                            "cancel": {},
                            "requests": {"tools": {"call": {}}},
                        },
                        "prompts": {"listChanged": True},
                        "completions": {},
                        "superstream": {"enabled": True},
                    }
                }
            ),
            encoding="utf-8",
        )
        _write_registries(
            cls.win_registry, cls.wsl_registry, cls.state, cls.fixture_config
        )
        cls.link_port = free_port()
        cls.win_local_port = free_port()
        cls.wsl_local_port = free_port()
        cls.environment = os.environ.copy()
        cls.environment["PYTHONDONTWRITEBYTECODE"] = "1"
        cls._start_node(WIN, cls.win_registry, cls.win_local_port, "win")
        cls._start_node(WSL, cls.wsl_registry, cls.wsl_local_port, "wsl")
        cls._wait_for_link()

    @staticmethod
    def _check_tools_only(caps: dict) -> None:
        if caps is None:
            raise AssertionError("legacy initialize result carried no capabilities")
        for family in ("tasks", "prompts", "resources", "logging",
                       "completions", "experimental", "superstream"):
            assert family not in caps, f"family {family!r} leaked to a legacy client"
        tools = caps.get("tools")
        assert isinstance(tools, dict), caps
        for leaf in tools:
            assert leaf == "listChanged", f"unrepresentable tools leaf {leaf!r} leaked"
        assert tools.get("listChanged") is True, tools

    @classmethod
    def _start_node(cls, script: Path, registry: Path, port: int, name: str) -> None:
        process = subprocess.Popen(
            [
                sys.executable,
                str(script),
                "serve",
                "--registry",
                str(registry),
                "--local-port",
                str(port),
                "--link-port",
                str(cls.link_port),
                "--artifact-spool-root",
                str(cls.root / f"{name}-spool"),
            ],
            cwd=ROOT,
            env=cls.environment,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        setattr(cls, f"_{name}_process", process)

    @classmethod
    def _wait_for_link(cls) -> None:
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            try:
                result = local_registry_query(
                    "127.0.0.1",
                    cls.wsl_local_port,
                    "remote",
                    "describe",
                    {"id": "legacy-shared"},
                )
                if result.get("id") == "legacy-shared":
                    return
            except (OSError, BridgeError):
                time.sleep(0.1)
        raise RuntimeError("legacy-shared acceptance nodes did not connect")

    @classmethod
    def tearDownClass(cls) -> None:
        for name in ("_wsl_process", "_win_process"):
            process = getattr(cls, name, None)
            if process and process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
        cls.temp.cleanup()

    # -- fixture state / journal helpers -----------------------------------

    def _fixture_lines(self) -> list[dict]:
        path = self.state / "fixture.jsonl"
        lines: list[dict] = []
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                lines.append(json.loads(line))
        return lines

    def _initialize_requests(self) -> list[dict]:
        return [
            line
            for line in self._fixture_lines()
            if line.get("event") == "frame"
            and line.get("method") == "initialize"
            and isinstance(line.get("params"), dict)
        ]

    def _spawn_starts(self) -> int:
        return sum(1 for line in self._fixture_lines() if line.get("event") == "start")

    def _win_journal_rows(self, category: str) -> list[dict]:
        rows: list[dict] = []
        path = _event_journal_path(self.win_registry)
        if not path.exists():
            return rows
        try:
            with sqlite3.connect(path) as connection:
                connection.row_factory = sqlite3.Row
                for row in connection.execute(
                    "SELECT category, target, outcome, metadata_json "
                    "FROM events WHERE category = ? ORDER BY seq ASC",
                    (category,),
                ):
                    rows.append(
                        {
                            "category": row["category"],
                            "target": row["target"],
                            "outcome": row["outcome"],
                            "metadata": json.loads(row["metadata_json"]),
                        }
                    )
        except sqlite3.Error:
            pass
        return rows

    # -- tests ---------------------------------------------------------------

    def test_01_physical_request_is_canonical_and_actual_is_journaled(self) -> None:
        spawn_delta_start = self._spawn_starts()
        init_delta_start = len(self._initialize_requests())
        client = LegacyClient(self.wsl_local_port, "legacy-shared")
        try:
            response = client.initialize(
                request_id="baseline-init", protocol_version="2025-11-25"
            )
        finally:
            client.close()
        self.assertIn("result", response, response)
        result = response["result"]
        self.assertEqual(result["protocolVersion"], "2025-11-25")
        self.assertEqual(result["capabilities"], {"tools": {"listChanged": True}})
        # Canary: the fixture actual advertises tasks/prompts/completions and an
        # ad-hoc family, but the virtualized initialize replays only the
        # implemented tools family -- even to this 2025-11-25 client.
        self._check_tools_only(result["capabilities"])
        self.assertEqual(result["instructions"],
                         "Synthetic versioned shared-backend instructions.")
        # The physical initialize request carried the canonical revision with
        # the empty bridge-owned client capability set.
        requests = self._initialize_requests()[init_delta_start:]
        self.assertEqual(len(requests), 1, requests)
        self.assertEqual(requests[0]["params"]["protocolVersion"], "2025-11-25")
        self.assertEqual(requests[0]["params"]["capabilities"], {})
        self.assertEqual(self._spawn_starts(), spawn_delta_start + 1)
        # The observed actual backend revision was journaled per generation.
        rows = self._win_journal_rows("shared-backend-session")
        self.assertEqual(len(rows), 1, rows)
        self.assertEqual(rows[0]["target"], "legacy-shared")
        self.assertEqual(rows[0]["outcome"], "observed")
        self.assertEqual(rows[0]["metadata"]["backendVersion"], "2025-11-25")
        self.assertEqual(rows[0]["metadata"]["requestedVersion"], "2025-11-25")

    def test_02_four_logical_profiles_share_one_generation_and_project(self) -> None:
        # A shared generation is torn down when its last logical client leaves,
        # so pin one long-lived keeper client first, then snapshot: the four
        # profile clients must reuse that same open generation (no new physical
        # spawn, no replayed initialize handshake).
        keeper = LegacyClient(self.wsl_local_port, "legacy-shared")
        try:
            keeper_init = keeper.initialize(request_id="keeper-init",
                                            protocol_version="2025-11-25")
            self.assertIn("result", keeper_init, keeper_init)
            spawn_delta_start = self._spawn_starts()
            init_delta_start = len(self._initialize_requests())
            clients = [LegacyClient(self.wsl_local_port, "legacy-shared")
                       for _ in LEGACY_PROFILES]
            try:
                for index, (client, profile) in enumerate(zip(clients, LEGACY_PROFILES)):
                    init = client.initialize(
                        request_id=f"init-{profile}", protocol_version=profile
                    )
                    self.assertIn("result", init, init)
                    self.assertEqual(init["result"]["protocolVersion"], profile)
                    # Canary: every logical profile -- including 2025-11-25 --
                    # receives only the implemented tools family in
                    # capabilities, never tasks or other advertised families.
                    self._check_tools_only(init["result"]["capabilities"])
                    # The backend (actual 2025-11-25) catalog is projected to
                    # each logical profile: era-newer fields are stripped, base
                    # fields and instructions remain.
                    listed = client.request(f"list-{profile}", "tools/list", {})
                    self.assertIn("result", listed, listed)
                    tools = listed["result"]["tools"]
                    self.assertEqual(len(tools), 2)
                    self.assertEqual({tool["name"] for tool in tools},
                                     {"echo", "variants"})
                    for tool in tools:
                        self.assertEqual(
                            set(tool.keys()), _expected_tool_keys(profile)
                        )
                    self.assertEqual(tools[0]["name"], "echo")
                    # Call results: content for every profile;
                    # structuredContent only for profiles >= 2025-06-18.
                    echoed = client.call(f"echo-{profile}", "echo", {"value": profile})
                    self.assertIn("result", echoed, echoed)
                    text = echoed["result"]["content"][0]["text"]
                    self.assertEqual(json.loads(text), profile)
                    has_structured = profile >= STRUCTURED_CONTENT_ANCHOR
                    self.assertEqual(
                        "structuredContent" in echoed["result"], has_structured
                    )
                    if has_structured:
                        self.assertEqual(
                            echoed["result"]["structuredContent"]["value"], profile
                        )
                # All four logical clients reused the open generation.
                self.assertEqual(self._spawn_starts(), spawn_delta_start)
                self.assertEqual(len(self._initialize_requests()), init_delta_start)
            finally:
                for client in clients:
                    client.close()
        finally:
            keeper.close()

    def test_03_crash_recovery_renegotiates_the_same_actual_without_replay(self) -> None:
        spawn_delta_start = self._spawn_starts()
        init_delta_start = len(self._initialize_requests())
        observed_delta_start = len(
            [r for r in self._win_journal_rows("shared-backend-session")
             if r["outcome"] == "observed"]
        )
        client = LegacyClient(self.wsl_local_port, "legacy-shared")
        try:
            init = client.initialize(request_id="crash-init", protocol_version="2025-11-25")
            self.assertIn("result", init, init)
            first = client.call("pre-crash", "echo", {"value": "before-crash"})
            self.assertIn("result", first, first)
            crashed = client.call("do-crash", "crash", {})
            # Crash is a business call that reached the backend: outcome is
            # genuinely unknown (or the backend answered before exiting).
            self.assertIn("error", crashed, crashed)
            # Automatic recovery restarts the generation; a fresh business call
            # then succeeds on the re-negotiated session (never replayed as a
            # duplicate of pre-crash: exactly one 'before-crash' echo).
            deadline = time.monotonic() + 25
            after = None
            while time.monotonic() < deadline:
                try:
                    after = client.call("post-recovery", "echo", {"value": "after"})
                    if "result" in after:
                        break
                except (OSError, EOFError, BridgeError):
                    time.sleep(0.2)
            self.assertIsNotNone(after, "recovery did not complete")
            self.assertIn("result", after, after)
            self.assertEqual(json.loads(after["result"]["content"][0]["text"]), "after")
            # The replacement generation re-negotiated: a second physical
            # initialize with the same canonical request and a second observed
            # session row for the new generation.
            requests = self._initialize_requests()[init_delta_start:]
            self.assertGreaterEqual(len(requests), 2, requests)
            for request in requests:
                self.assertEqual(
                    request["params"]["protocolVersion"], "2025-11-25"
                )
            self.assertGreater(self._spawn_starts(), spawn_delta_start)
            rows = self._win_journal_rows("shared-backend-session")
            observed = [row for row in rows if row["outcome"] == "observed"]
            self.assertGreaterEqual(
                len(observed) - observed_delta_start, 1, rows
            )
            for row in observed:
                self.assertEqual(row["metadata"]["backendVersion"], "2025-11-25")
            before_calls = sum(
                1
                for line in self._fixture_lines()
                if line.get("event") == "frame"
                and line.get("method") == "tools/call"
                and line.get("params", {}).get("name") == "echo"
                and line.get("params", {}).get("arguments", {}).get("value")
                == "before-crash"
            )
            self.assertEqual(before_calls, 1)
        finally:
            client.close()

    def test_04_unsupported_logical_requests_stay_clean(self) -> None:
        client = LegacyClient(self.wsl_local_port, "legacy-shared")
        try:
            rejected = client.initialize(
                request_id="old-init", protocol_version="1999-01-01"
            )
            self.assertIn("error", rejected, rejected)
            self.assertEqual(rejected["error"]["code"], -32602)
            self.assertIn("2025-11-25",
                          rejected["error"]["data"]["supportedProtocolVersions"])
        finally:
            client.close()

    def test_05_mixed_content_variants_project_or_fail_without_replay(self) -> None:
        # The actual-2025-11-25 backend's "variants" tool returns all five
        # content kinds (text/image/EmbeddedResource/audio/resource_link, plus
        # item-level _meta).  Per logical profile the bridge either passes the
        # full mixed result through untouched or fails the whole call
        # explicitly -- it must never drop audio/resource_link and report a
        # fabricated success, and an explicit failure must not poison the
        # generation or replay the business request.
        keeper = LegacyClient(self.wsl_local_port, "legacy-shared")
        frame_delta_start: int | None = None
        try:
            keeper_init = keeper.initialize(request_id="variants-keeper",
                                            protocol_version="2025-11-25")
            self.assertIn("result", keeper_init, keeper_init)
            frame_delta_start = len(self._fixture_lines())
            ok_profiles = ("2025-06-18", "2025-11-25")
            error_cases = [
                # (logical profile, first unrepresentable kind, why)
                ("2024-11-05", "audio", "audio arrives only in 2025-03-26"),
                ("2025-03-26", "resource_link",
                 "resource_link arrives only in 2025-06-18"),
            ]
            for profile in ok_profiles:
                with self.subTest(profile=profile, expect="full result"):
                    client = LegacyClient(self.wsl_local_port, "legacy-shared")
                    try:
                        init = client.initialize(
                            request_id=f"variants-init-{profile}",
                            protocol_version=profile,
                        )
                        self.assertIn("result", init, init)
                        variants = client.call(f"variants-{profile}", "variants", {})
                        self.assertIn("result", variants, variants)
                        kinds = [c["type"] for c in variants["result"]["content"]]
                        self.assertEqual(
                            kinds,
                            ["text", "image", "resource", "audio",
                             "resource_link"],
                        )
                        self.assertEqual(
                            variants["result"]["content"][0]["_meta"],
                            {"fixture": True},
                        )
                        self.assertEqual(
                            variants["result"]["content"][3]["data"], "QUJD"
                        )
                    finally:
                        client.close()
            for profile, kind, reason in error_cases:
                with self.subTest(profile=profile, expect="explicit error",
                                  reason=reason):
                    client = LegacyClient(self.wsl_local_port, "legacy-shared")
                    try:
                        init = client.initialize(
                            request_id=f"variants-init-{profile}",
                            protocol_version=profile,
                        )
                        self.assertIn("result", init, init)
                        variants = client.call(f"variants-{profile}", "variants", {})
                        self.assertIn("error", variants, variants)
                        self.assertEqual(variants["error"]["code"], -32603)
                        message = variants["error"]["message"]
                        self.assertIn(kind, message)
                        self.assertIn(profile, message)
                        # The explicit failure neither poisons the shared
                        # generation nor fabricates success: the next business
                        # call on the same client succeeds.
                        echoed = client.call(
                            f"variants-after-{profile}", "echo", {"value": "alive"}
                        )
                        self.assertIn("result", echoed, echoed)
                        self.assertEqual(
                            json.loads(echoed["result"]["content"][0]["text"]),
                            "alive",
                        )
                    finally:
                        client.close()
            # Business requests are never replayed: each profile issued
            # exactly one variants call; the two explicit failures produced no
            # duplicate backend execution.
            variants_calls = [
                line
                for line in self._fixture_lines()[frame_delta_start:]
                if line.get("event") == "frame"
                and line.get("method") == "tools/call"
                and line.get("params", {}).get("name") == "variants"
            ]
            self.assertEqual(
                len(variants_calls), len(ok_profiles) + len(error_cases)
            )
        finally:
            keeper.close()


if __name__ == "__main__":
    unittest.main()
