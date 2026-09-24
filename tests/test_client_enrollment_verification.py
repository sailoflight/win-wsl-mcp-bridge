"""Client enrollment evidence and conservative exposure policy regressions."""
from __future__ import annotations

import hashlib
import json
import sqlite3
import time
import unittest
from unittest import mock

from installer import projection as bridge
import harness_verification as hv
from tests import test_bridge as fixtures


class ClientEnrollmentVerificationTest(fixtures.ProjectionHarness):
    def setUp(self):
        super().setUp()
        self.overlay = self.dsh_home / "profiles" / "main" / "cordis-bridge-overlay.json"
        self.overlay.write_text("[]\n", encoding="utf-8")
        self.environment = self.enroll(self.candidate("dsh"))["environment"]
        self.environment_id = self.environment["environmentId"]
        self.database = bridge.ProjectionDatabase(self.projection())

    def row(self):
        with self.database._connect() as connection:
            return dict(self.database.environment_row(connection, self.environment_id))

    def identity(self, **overrides):
        identity = {
            "clientKind": "dsh",
            "observedClient": {"name": "fixture-client", "version": "1.0"},
            "protocolVersion": "2025-06-18",
            "configFingerprint": hashlib.sha256(self.overlay.read_bytes()).hexdigest(),
            "declaredClientVersion": None,
        }
        identity.update(overrides)
        return identity

    def evidence(self, *, native="unsupported", refresh="supported", model="supported"):
        """Recorded shape: version identity plus timestamps, never a validity window."""
        identity = self.identity()
        return {
            "environmentId": self.environment_id,
            "clientKind": "dsh", "probeId": "fixture-probe",
            "configFingerprint": identity["configFingerprint"],
            "observedAt": time.time() - 5, "recordedAt": time.time() - 5,
            "versionIdentity": identity,
            "versionFingerprint": hv.version_fingerprint(identity),
            "protocolVersion": "2025-06-18",
            "capabilities": {"toolsListChanged": refresh, "modelExposure": model,
                             "nativeToolSearch": native},
        }

    def status_entry(self):
        return bridge.projection_probe_status(
            projection=self.projection(), environment_id=self.environment_id,
        )["environments"][0]

    def save_evidence(self, value, mode="auto"):
        with self.database._connect(write=True) as connection:
            connection.execute(
                "UPDATE agent_environments SET tool_exposure=?, harness_verification_json=? "
                "WHERE environment_id=?", (mode, json.dumps(value), self.environment_id),
            )

    def row_of(self, environment_id):
        with self.database._connect() as connection:
            return dict(self.database.environment_row(connection, environment_id))

    def add_profile(self, name, *, client_kind="dsh", mode="native", evidence=None):
        """Insert a second enrolled profile row for the same host.

        A real Harness install has several enrolled profiles (web / dsh-tui /
        headless) that share one MCP client package; this creates the second one
        without a full second scan/enroll cycle.
        """
        row = self.row()
        environment_id = ("env-" + name.ljust(24, "0"))[:28]
        overlay = self.dsh_home / "profiles" / name / "cordis-bridge-overlay.json"
        overlay.parent.mkdir(parents=True, exist_ok=True)
        overlay.write_text("[]\n", encoding="utf-8")
        now = time.time_ns()
        with self.database._connect(write=True) as connection:
            connection.execute(
                "INSERT INTO agent_environments (environment_id, client_kind, host_side, "
                "scope, config_path, discovery_source, enabled, launcher_command, "
                "launcher_args_json, apply_adapter, file_sha256, file_size, file_mtime_ns, "
                "may_contain_secrets, transport_capabilities_json, compatibility_route, "
                "relay_base_url, stdio_http_endpoints_json, confirmed_at_ns, created_at_ns, "
                "updated_at_ns, tool_exposure, harness_verification_json) "
                "VALUES (?, ?, ?, 'user', ?, 'scan', 1, ?, ?, 'bridge-file', '', 0, 0, 0, "
                "?, 'native', '', '', ?, ?, ?, ?, ?)",
                (
                    environment_id, client_kind, row["host_side"], str(overlay),
                    row["launcher_command"], row["launcher_args_json"],
                    row["transport_capabilities_json"], now, now, now, mode,
                    json.dumps(evidence) if evidence is not None else "{}",
                ),
            )
        return environment_id

    def family_evidence(self, environment_id, **overrides):
        """The recorded shape of a family-scoped DSH observation."""
        evidence = self.evidence()
        evidence["scope"] = bridge.FAMILY_EVIDENCE_SCOPE
        evidence["recordedEnvironmentId"] = environment_id
        evidence.update(overrides)
        return evidence

    def test_dsh_observation_is_family_scoped_and_covers_a_sibling_profile(self):
        sibling = self.add_profile("dsh-tui")
        path = self.root / "probe.json"
        bridge.projection_probe_client(
            projection=self.projection(), environment_id=self.environment_id,
            probe_file=path, confirm=True,
        )
        receipt = self.root / "receipt.json"
        receipt.write_text("{}")
        with mock.patch(
            "harness_verification.validate_probe_receipt", return_value=self.evidence()
        ):
            result = bridge.projection_record_client_verification(
                projection=self.projection(), environment_id=self.environment_id,
                receipt_file=receipt, tool_exposure="auto", confirm=True,
            )
        self.assertEqual(
            [item["environmentId"] for item in result["familyAdoptedEnvironments"]],
            [sibling],
        )
        # The recording keeps its own observation and its own configuration guard.
        recorded = self.row()
        self.assertEqual(recorded["tool_exposure"], "auto")
        self.assertEqual(bridge._effective_tool_exposure(recorded), "deferred")
        stored = json.loads(recorded["harness_verification_json"])
        self.assertEqual(stored["scope"], bridge.FAMILY_EVIDENCE_SCOPE)
        self.assertEqual(stored["recordedEnvironmentId"], self.environment_id)
        # The sibling adopts the same observation without a second probe.
        adopted = self.row_of(sibling)
        self.assertEqual(adopted["tool_exposure"], "auto")
        state = bridge._harness_evidence_state(adopted)
        self.assertTrue(state["familyAdopted"])
        self.assertEqual(state["adoptedFromEnvironmentId"], self.environment_id)
        self.assertEqual(state["reasons"], [])
        self.assertEqual(bridge._effective_tool_exposure(adopted), "deferred")
        # The read-only report must present an adopted catalog as adopted.
        entry = bridge.projection_probe_status(
            projection=self.projection(), environment_id=sibling,
        )["environments"][0]
        self.assertTrue(entry["evidenceFamilyAdopted"])
        self.assertEqual(entry["evidenceAdoptedFromEnvironmentId"], self.environment_id)
        self.assertEqual(entry["effectiveToolExposure"], "deferred")

    def test_family_scope_requires_the_explicit_marker_and_a_dsh_row(self):
        sibling = self.add_profile("dsh-tui")
        unmarked = self.evidence()
        unmarked["environmentId"] = self.environment_id
        with self.database._connect(write=True) as connection:
            connection.execute(
                "UPDATE agent_environments SET tool_exposure='auto', "
                "harness_verification_json=? WHERE environment_id=?",
                (json.dumps(unmarked), sibling),
            )
        state = bridge._harness_evidence_state(self.row_of(sibling))
        self.assertFalse(state["familyAdopted"])
        self.assertTrue(any(
            "different environment" in reason for reason in state["reasons"]
        ))
        self.assertEqual(bridge._effective_tool_exposure(self.row_of(sibling)), "native")

        # The same marked observation never covers another client family.
        claude = self.add_profile(
            "claude-main", client_kind="claude", mode="auto",
            evidence=self.family_evidence(self.environment_id),
        )
        claude_state = bridge._harness_evidence_state(self.row_of(claude))
        self.assertFalse(claude_state["familyAdopted"])
        self.assertEqual(bridge._effective_tool_exposure(self.row_of(claude)), "native")

    def test_a_profile_with_its_own_observation_is_never_overwritten(self):
        own = self.evidence()
        sibling = self.add_profile("dsh-tui", mode="auto", evidence=own)
        before = self.row_of(sibling)["harness_verification_json"]
        with self.database._connect(write=True) as connection:
            adopted = bridge._adopt_family_evidence(
                connection, self.database, source_row=self.row(),
                evidence_json=json.dumps(self.family_evidence(self.environment_id)),
            )
        self.assertEqual(adopted, [])
        self.assertEqual(self.row_of(sibling)["harness_verification_json"], before)

    def test_a_deferred_profile_is_not_downgraded_by_family_adoption(self):
        sibling = self.add_profile("dsh-tui", mode="deferred")
        with self.database._connect(write=True) as connection:
            bridge._adopt_family_evidence(
                connection, self.database, source_row=self.row(),
                evidence_json=json.dumps(self.family_evidence(self.environment_id)),
            )
        self.assertEqual(self.row_of(sibling)["tool_exposure"], "deferred")

    def test_default_has_no_assumed_harness_support(self):
        row = self.row()
        self.assertEqual(bridge._effective_tool_exposure(row), "native")
        self.assertEqual(bridge._environment_summary(row)["harnessVerification"], {})
        row["tool_exposure"] = "auto"
        self.assertEqual(bridge._effective_tool_exposure(row), "native")
        row["tool_exposure"] = "deferred"
        with self.assertRaisesRegex(bridge.BridgeError, "requires fresh"):
            bridge._effective_tool_exposure(row)

    def test_auto_prefers_native_search_and_requires_model_evidence(self):
        for native, refresh, model, expected in [
            ("supported", "supported", "supported", "native"),
            ("unknown", "supported", "supported", "native"),
            ("unsupported", "supported", "unknown", "native"),
            ("unsupported", "unknown", "supported", "native"),
            ("unsupported", "supported", "supported", "deferred"),
        ]:
            with self.subTest(native=native, refresh=refresh, model=model):
                self.save_evidence(self.evidence(native=native, refresh=refresh, model=model))
                self.assertEqual(bridge._effective_tool_exposure(self.row()), expected)

    def test_identity_mismatch_foreign_modern_and_changed_config_cannot_enable_deferred(self):
        for change in [
            {"versionFingerprint": "0" * 64},
            {"versionFingerprint": None}, {"versionIdentity": None},
            {"versionIdentity": self.identity(declaredClientVersion="other 9.9")},
            {"environmentId": "other"},
            {"clientKind": "codex"}, {"protocolVersion": "2026-07-28"},
            {"configFingerprint": "0" * 64},
        ]:
            with self.subTest(change=change):
                evidence = self.evidence()
                evidence.update(change)
                self.save_evidence(evidence)
                self.assertEqual(bridge._effective_tool_exposure(self.row()), "native")
        self.save_evidence(self.evidence())
        self.overlay.write_text('[{"changed":true}]')
        self.assertEqual(bridge._effective_tool_exposure(self.row()), "native")

    def test_legacy_evidence_without_a_version_fingerprint_never_applies(self):
        """An old blob keeps its validity window; it is reported, never trusted."""
        legacy = {**self.evidence(), "validUntil": time.time() + 3600,
                  "verifiedAt": time.time()}
        for key in ("versionIdentity", "versionFingerprint"):
            legacy.pop(key)
        self.save_evidence(legacy)
        entry = self.status_entry()
        self.assertFalse(entry["evidenceFresh"])
        self.assertIsNone(entry["versionFingerprint"])
        self.assertTrue(any(
            "version identity" in reason for reason in entry["evidenceReasons"]
        ))
        self.assertEqual(entry["effectiveToolExposure"], "native")
        # Its observation time is still readable, which is the point of keeping
        # the timestamp: an Agent can tell how old the observation is.
        self.assertAlmostEqual(entry["observedAt"], legacy["observedAt"], places=3)

    def test_evidence_never_expires_and_a_new_peer_mcp_requests_a_recheck(self):
        old = time.time() - 400 * 24 * 3600
        aged = {**self.evidence(), "observedAt": old, "recordedAt": old}
        self.save_evidence({**aged, "peerServers": ["alpha", "gamma"]})
        entry = self.status_entry()
        # Age is never a reason on its own, however old the observation is.
        self.assertTrue(entry["evidenceFresh"])
        self.assertEqual(entry["evidenceReasons"], [])
        self.assertEqual(entry["effectiveToolExposure"], "deferred")
        # A mirror that was never refreshed is not comparable, so no false prompt.
        self.assertFalse(entry["recheckRequired"])
        self.assertEqual(entry["peerServersObserved"], ["alpha", "gamma"])
        self.sync_mirror(self.make_peer([self.server("alpha"), self.server("beta")]))
        entry = self.status_entry()
        self.assertTrue(entry["recheckRequired"])
        self.assertEqual(entry["newPeerServers"], ["beta"])
        self.assertEqual(entry["retiredPeerServers"], ["gamma"])
        self.assertTrue(any(
            "new peer MCP entered" in reason for reason in entry["recheckReasons"]
        ))
        self.assertTrue(any(
            "re-observe this environment" in action for action in entry["nextActions"]
        ))
        # A new peer MCP asks for re-observation; it never narrows the catalog.
        self.assertTrue(entry["evidenceFresh"])
        self.assertEqual(entry["effectiveToolExposure"], "deferred")
        # Re-observing the current peer set clears the request: the trigger is the
        # change, and the record stays valid until the identity itself changes.
        self.save_evidence({**aged, "peerServers": ["alpha", "beta"]})
        entry = self.status_entry()
        self.assertFalse(entry["recheckRequired"])
        self.assertEqual(entry["recheckReasons"], [])

    def test_descriptors_use_one_deferred_registration_per_peer(self):
        self.save_evidence(self.evidence())
        result = bridge._desired_entry_descriptors(self.row(), [
            ("alpha", "Alpha", "stdio"), ("beta", "Beta", "stdio"),
        ])
        self.assertEqual([item["args"][-2:] for item in result], [
            ["deferred-mcp", "alpha"], ["deferred-mcp", "beta"],
        ])
        self.assertTrue(all(item["projectionMode"] == "converted" for item in result))
        row = self.row()
        row["compatibility_route"] = "constant-two-tool"
        result = bridge._desired_entry_descriptors(row, [("alpha", "Alpha", "stdio")])
        self.assertEqual(result[0]["args"][-2:], ["compatibility-mcp", "alpha"])
        row["tool_exposure"] = "deferred"
        with self.assertRaisesRegex(bridge.BridgeError, "only native stdio"):
            bridge._desired_entry_descriptors(row, [("alpha", "Alpha", "stdio")])

    def test_projection_preserves_verification_across_own_rewrite(self):
        peer = self.make_peer([self.server("alpha")])
        self.sync_mirror(peer)
        bridge.projection_reconcile(projection=self.projection(), side="wsl")
        evidence = self.evidence()
        self.save_evidence(evidence)
        result = bridge.projection_reconcile(projection=self.projection(), side="wsl")
        self.assertTrue(result["ok"], result)
        row = self.row()
        updated = json.loads(row["harness_verification_json"])
        self.assertEqual(updated["configFingerprint"], evidence["configFingerprint"])
        self.assertNotEqual(updated["projectionConfigFingerprint"], evidence["configFingerprint"])
        self.assertEqual(bridge._effective_tool_exposure(row), "deferred")
        result = bridge.projection_reconcile(projection=self.projection(), side="wsl")
        self.assertTrue(result["ok"], result)
        self.assertEqual(bridge._effective_tool_exposure(self.row()), "deferred")

    def test_v4_migration_preserves_existing_enrollment_and_projection(self):
        old = self.root / "v4.sqlite3"
        schema = bridge.PROJECTION_SCHEMA.replace(
            "    tool_exposure TEXT NOT NULL DEFAULT 'native',\n", ""
        ).replace("    harness_verification_json TEXT NOT NULL DEFAULT '{}',\n", "")
        source = self.row()
        source.pop("tool_exposure")
        source.pop("harness_verification_json")
        with sqlite3.connect(old) as connection:
            connection.executescript(schema)
            connection.execute("PRAGMA user_version=4")
            keys = list(source)
            connection.execute(
                "INSERT INTO agent_environments (" + ",".join(keys) + ") VALUES ("
                + ",".join("?" for _ in keys) + ")", [source[key] for key in keys],
            )
            connection.execute("INSERT INTO projection_meta VALUES ('fixture', 'retained')")
        bridge.ProjectionDatabase.ensure(old)
        bridge.ProjectionDatabase.ensure(old)
        migrated = bridge.ProjectionDatabase(old)
        with migrated._connect() as connection:
            row = dict(migrated.environment_row(connection, self.environment_id))
            self.assertEqual(connection.execute("PRAGMA user_version").fetchone()[0], 5)
            self.assertEqual(migrated.meta_value(connection, "fixture"), "retained")
        self.assertEqual({key: row[key] for key in source}, source)
        self.assertEqual(row["tool_exposure"], "native")
        self.assertEqual(row["harness_verification_json"], "{}")

    def test_prepare_dry_run_confirmation_and_no_clobber(self):
        path = self.root / "probe.json"
        result = bridge.projection_probe_client(
            projection=self.projection(), environment_id=self.environment_id,
            probe_file=path, dry_run=True,
        )
        self.assertFalse(path.exists())
        self.assertTrue(result["dryRun"])
        with self.assertRaisesRegex(bridge.BridgeError, "requires --confirm"):
            bridge.projection_probe_client(
                projection=self.projection(), environment_id=self.environment_id, probe_file=path,
            )
        result = bridge.projection_probe_client(
            projection=self.projection(), environment_id=self.environment_id,
            probe_file=path, confirm=True,
        )
        self.assertTrue(path.is_file())
        before = path.read_bytes()
        with self.assertRaisesRegex(bridge.BridgeError, "already exists"):
            bridge.projection_probe_client(
                projection=self.projection(), environment_id=self.environment_id,
                probe_file=path, confirm=True,
            )
        self.assertEqual(path.read_bytes(), before)
        self.assertNotIn("launcherCommand", json.loads(before))
        self.assertEqual(bridge._effective_tool_exposure(self.row()), "native")

    def test_validated_receipt_is_bound_and_consumed_without_applying_config(self):
        path = self.root / "probe.json"
        bridge.projection_probe_client(
            projection=self.projection(), environment_id=self.environment_id,
            probe_file=path, confirm=True,
        )
        receipt = self.root / "receipt.json"
        receipt.write_text("{}")
        evidence = self.evidence()
        before = self.overlay.read_bytes()
        # Protocol/attestation validation has independent fixture tests; this
        # test covers the SQLite transaction and consumer boundary only.
        with mock.patch("harness_verification.validate_probe_receipt", return_value=evidence):
            result = bridge.projection_record_client_verification(
                projection=self.projection(), environment_id=self.environment_id,
                receipt_file=receipt, dry_run=True,
            )
            self.assertEqual(result["effectiveToolExposure"], "deferred")
            self.assertEqual(self.row()["harness_verification_json"], "{}")
            result = bridge.projection_record_client_verification(
                projection=self.projection(), environment_id=self.environment_id,
                receipt_file=receipt, confirm=True,
            )
            self.assertFalse(result["configurationApplied"])
            self.assertEqual(self.overlay.read_bytes(), before)
            with self.assertRaisesRegex(bridge.BridgeError, "unconsumed"):
                bridge.projection_record_client_verification(
                    projection=self.projection(), environment_id=self.environment_id,
                    receipt_file=receipt, confirm=True,
                )


    def test_probe_client_selects_one_aspect_and_prints_the_agent_path(self):
        path = self.root / "aspect-probe.json"
        result = bridge.projection_probe_client(
            projection=self.projection(), environment_id=self.environment_id,
            probe_file=path, aspect="model-exposure", dry_run=True,
        )
        self.assertFalse(path.exists())
        self.assertEqual(result["aspect"], "model-exposure")
        self.assertEqual(result["capability"], "modelExposure")
        self.assertEqual(result["observation"], "challenge-bound-attestation")
        self.assertEqual(result["registryField"], "tool_exposure")
        self.assertTrue(result["agentSteps"])
        command = result["recordCommand"]
        self.assertEqual(command[:3], ["projection", "record-client-verification", self.environment_id])
        self.assertEqual(command[command.index("--aspect") + 1], "model-exposure")
        self.assertIn(str(result["receiptFile"]), command)
        self.assertEqual(command[-1], "--confirm")

    def test_unknown_or_unobservable_aspects_never_prepare_a_challenge(self):
        path = self.root / "bad-aspect.json"
        with self.assertRaisesRegex(bridge.BridgeError, "unknown client capability aspect"):
            bridge.projection_probe_client(
                projection=self.projection(), environment_id=self.environment_id,
                probe_file=path, aspect="tool-token-cost", dry_run=True,
            )
        with self.assertRaisesRegex(bridge.BridgeError, "not observable"):
            bridge.projection_probe_client(
                projection=self.projection(), environment_id=self.environment_id,
                probe_file=path, aspect="modern-protocol", dry_run=True,
            )
        self.assertFalse(path.exists())

    def test_probe_aspects_are_independent_challenges(self):
        first = self.root / "refresh-probe.json"
        second = self.root / "model-probe.json"
        bridge.projection_probe_client(
            projection=self.projection(), environment_id=self.environment_id,
            probe_file=first, confirm=True,
        )
        bridge.projection_probe_client(
            projection=self.projection(), environment_id=self.environment_id,
            probe_file=second, aspect="model-exposure", confirm=True,
        )
        report = bridge.projection_probe_status(projection=self.projection())
        self.assertEqual(report["environmentCount"], 1)
        entry = report["environments"][0]
        items = {item["aspect"]: item for item in entry["capabilities"]}
        self.assertEqual(items["refresh"]["state"], "not-probed")
        self.assertEqual(items["refresh"]["outstandingChallenge"]["probeFile"], str(first.resolve()))
        self.assertFalse(items["refresh"]["outstandingChallenge"]["expired"])
        self.assertEqual(
            items["model-exposure"]["outstandingChallenge"]["probeFile"], str(second.resolve()),
        )
        self.assertEqual(
            [item["aspect"] for item in entry["outstandingChallenges"]],
            ["model-exposure", "refresh"],
        )
        # Not observable is a property of this probe version, not of the target.
        self.assertEqual(items["modern-protocol"]["state"], "not-observable")
        self.assertIsNone(items["modern-protocol"]["nextAction"])
        self.assertEqual(entry["effectiveToolExposure"], "native")
        self.assertEqual(entry["evidenceReasons"], ["no recorded client verification for this environment"])
        self.assertTrue(entry["toolExposureBlockers"])
        self.assertEqual(len(entry["nextActions"]), 4)
        self.assertEqual(
            {item["aspect"] for item in report["aspects"]}, set(bridge.CLIENT_CAPABILITY_ASPECTS),
        )
        self.assertFalse(
            [item for item in report["aspects"] if item["aspect"] == "modern-protocol"][0]["runnable"],
        )
        self.assertIn("never measures", report["scope"])

    def test_record_consumes_only_the_selected_aspect_challenge(self):
        for aspect in ("refresh", "native-search"):
            bridge.projection_probe_client(
                projection=self.projection(), environment_id=self.environment_id,
                probe_file=self.root / (aspect + "-probe.json"), aspect=aspect, confirm=True,
            )
        receipt = self.root / "aspect-receipt.json"
        receipt.write_text("{}")
        evidence = self.evidence()
        with self.assertRaisesRegex(bridge.BridgeError, "several unconsumed"):
            bridge.projection_record_client_verification(
                projection=self.projection(), environment_id=self.environment_id,
                receipt_file=receipt, confirm=True,
            )
        with mock.patch("harness_verification.validate_probe_receipt", return_value=evidence):
            result = bridge.projection_record_client_verification(
                projection=self.projection(), environment_id=self.environment_id,
                receipt_file=receipt, aspect="native-search", confirm=True,
            )
        self.assertEqual(result["aspect"], "native-search")
        self.assertEqual(result["capability"], "nativeToolSearch")
        self.assertEqual(result["capabilityValue"], "unsupported")
        self.assertEqual([item["aspect"] for item in result["outstandingChallenges"]], ["refresh"])
        with self.database._connect() as connection:
            self.assertIsNone(self.database.meta_value(
                connection, "client_probe:" + self.environment_id + ":native-search",
            ))
            self.assertIsNotNone(self.database.meta_value(
                connection, "client_probe:" + self.environment_id + ":refresh",
            ))
        # One challenge left: the aspect may be omitted and is then reported.
        with mock.patch("harness_verification.validate_probe_receipt", return_value=evidence):
            result = bridge.projection_record_client_verification(
                projection=self.projection(), environment_id=self.environment_id,
                receipt_file=receipt, confirm=True,
            )
        self.assertEqual(result["aspect"], "refresh")
        self.assertEqual(result["capabilityValue"], "supported")
        self.assertEqual(result["outstandingChallenges"], [])
        with self.assertRaisesRegex(bridge.BridgeError, "no unconsumed client probe"):
            bridge.projection_record_client_verification(
                projection=self.projection(), environment_id=self.environment_id,
                receipt_file=receipt, aspect="refresh", confirm=True,
            )
        with self.assertRaisesRegex(bridge.BridgeError, "unknown client capability aspect"):
            bridge.projection_record_client_verification(
                projection=self.projection(), environment_id=self.environment_id,
                receipt_file=receipt, aspect="tool-token-cost", confirm=True,
            )

    def test_legacy_challenge_key_stays_readable_and_recordable(self):
        # A challenge prepared before per-aspect keys has unknown intent, so it is
        # reported as legacy rather than mislabelled as a specific aspect.
        with self.database._connect(write=True) as connection:
            self.database.set_meta(
                connection, "client_probe:" + self.environment_id,
                json.dumps({"probeId": "legacy-probe"}),
            )
        entry = bridge.projection_probe_status(
            projection=self.projection(), environment_id=self.environment_id,
        )["environments"][0]
        self.assertEqual([item["aspect"] for item in entry["outstandingChallenges"]], ["legacy"])
        self.assertEqual(entry["capabilities"][0]["state"], "not-probed")
        receipt = self.root / "legacy-receipt.json"
        receipt.write_text("{}")
        with mock.patch("harness_verification.validate_probe_receipt", return_value=self.evidence()):
            result = bridge.projection_record_client_verification(
                projection=self.projection(), environment_id=self.environment_id,
                receipt_file=receipt, confirm=True,
            )
        self.assertEqual(result["aspect"], "legacy")
        self.assertIsNone(result["capability"])
        with self.database._connect() as connection:
            self.assertIsNone(self.database.meta_value(
                connection, "client_probe:" + self.environment_id,
            ))

    def test_probe_status_reports_recorded_states_and_stale_reasons(self):
        self.save_evidence(self.evidence())
        report = bridge.projection_probe_status(
            projection=self.projection(), environment_id=self.environment_id,
        )
        entry = report["environments"][0]
        self.assertEqual(
            {item["aspect"]: item["state"] for item in entry["capabilities"]},
            {
                "refresh": "supported",
                "harness-protocol": "supported",
                "model-exposure": "supported",
                "native-search": "unsupported",
                "modern-protocol": "not-observable",
            },
        )
        self.assertEqual(entry["effectiveToolExposure"], "deferred")
        self.assertEqual(entry["toolExposureBlockers"], [])
        self.assertTrue(entry["evidenceFresh"])
        # A configuration change after the run invalidates every observation.
        self.overlay.write_text('[{"changed": true}]')
        entry = bridge.projection_probe_status(
            projection=self.projection(), environment_id=self.environment_id,
        )["environments"][0]
        states = {item["aspect"]: item["state"] for item in entry["capabilities"]}
        self.assertEqual(states["refresh"], "stale")
        self.assertEqual(states["model-exposure"], "stale")
        self.assertEqual(states["modern-protocol"], "not-observable")
        self.assertFalse(entry["evidenceFresh"])
        self.assertTrue(any(
            "configuration changed" in reason for reason in entry["evidenceReasons"]
        ))
        self.assertEqual(entry["effectiveToolExposure"], "native")
        self.assertTrue(entry["toolExposureBlockers"])
        # A version-identity mismatch is reported as its own reason, not as an
        # unknown capability, and nothing expires on a clock.
        self.save_evidence({**self.evidence(), "versionFingerprint": "0" * 64})
        entry = bridge.projection_probe_status(
            projection=self.projection(), environment_id=self.environment_id,
        )["environments"][0]
        self.assertTrue(any(
            "version fingerprint does not match" in reason
            for reason in entry["evidenceReasons"]
        ))
        # A recorded deferred policy without usable evidence is reported as an
        # error state with its reasons, never as a silent downgrade.
        self.save_evidence({}, mode="deferred")
        entry = bridge.projection_probe_status(
            projection=self.projection(), environment_id=self.environment_id,
        )["environments"][0]
        self.assertEqual(entry["effectiveToolExposure"], "invalid")
        self.assertIn("deferred exposure requires", entry["toolExposureError"])
        self.assertEqual(len(entry["capabilities"]), 5)
        self.assertTrue(entry["nextActions"])
        with self.assertRaisesRegex(bridge.BridgeError, "unknown environment"):
            bridge.projection_probe_status(
                projection=self.projection(), environment_id="env-absent",
            )

    def test_projection_status_carries_a_compact_capability_summary(self):
        self.save_evidence(self.evidence())
        status = bridge.projection_status(projection=self.projection())
        entry = status["environments"][0]
        self.assertEqual(entry["effectiveToolExposure"], "deferred")
        self.assertEqual(entry["capabilityStates"]["refresh"], "supported")
        self.assertEqual(entry["capabilityStates"]["modern-protocol"], "not-observable")
        self.assertEqual(entry["outstandingChallenges"], [])
        self.assertEqual(entry["toolExposure"], "auto")
        self.assertEqual(entry["versionFingerprint"], self.evidence()["versionFingerprint"])
        self.assertTrue(isinstance(entry["observedAt"], (int, float)))
        self.assertFalse(entry["recheckRequired"])

    def test_real_negative_receipt_is_persisted_as_an_observed_negative(self):
        """No mock: a real fixture receipt flows through validation into policy."""
        path = self.root / "negative-probe.json"
        bridge.projection_probe_client(
            projection=self.projection(), environment_id=self.environment_id,
            probe_file=path, confirm=True,
        )
        challenge = json.loads(path.read_text(encoding="utf-8"))
        saved: list = []
        fixture = hv._Fixture(challenge, lambda _message: time.time(), saved.append)
        # A live session that never re-lists after the notification: the window
        # ends by the fixture's own deadline, which is the bounded negative.
        for message in (
            {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {
                "protocolVersion": "2025-06-18", "capabilities": {},
                "clientInfo": {"name": "dsh", "version": "fixture"}}},
            {"jsonrpc": "2.0", "method": "notifications/initialized"},
            {"jsonrpc": "2.0", "id": 2, "method": "tools/list"},
            {"jsonrpc": "2.0", "id": 3, "method": "tools/call", "params": {
                "name": challenge["tools"]["bootstrap"], "arguments": {}}},
        ):
            fixture.handle(message)
        fixture.finish("timeout")
        receipt = path.with_name(path.name + ".receipt.json")
        receipt.write_text(json.dumps(saved[-1]), encoding="utf-8")
        result = bridge.projection_record_client_verification(
            projection=self.projection(), environment_id=self.environment_id,
            receipt_file=receipt, aspect="refresh", confirm=True,
        )
        self.assertEqual(result["capabilityValue"], "unsupported")
        # The record pins identity and time; it carries no validity window.
        verification = result["verification"]
        self.assertNotIn("validUntil", verification)
        self.assertNotIn("verifiedAt", verification)
        self.assertEqual(
            verification["versionFingerprint"],
            hv.version_fingerprint(verification["versionIdentity"]),
        )
        self.assertEqual(result["versionFingerprint"], verification["versionFingerprint"])
        self.assertEqual(
            verification["versionIdentity"]["observedClient"],
            {"name": "dsh", "version": "fixture"},
        )
        self.assertEqual(verification["versionIdentity"]["protocolVersion"], "2025-06-18")
        self.assertIsNone(verification["versionIdentity"]["declaredClientVersion"])
        self.assertIsNone(result["declaredClientVersion"])
        self.assertGreaterEqual(verification["recordedAt"], verification["observedAt"])
        self.assertIsNone(verification["peerServers"])
        self.assertEqual(
            result["verification"]["evidence"]["protocolRefresh"]["refreshObservation"],
            "notification-without-refresh",
        )
        self.assertEqual(
            result["verification"]["evidence"]["protocolRefresh"]["stopReason"], "timeout",
        )
        entry = bridge.projection_probe_status(
            projection=self.projection(), environment_id=self.environment_id,
        )["environments"][0]
        items = {item["aspect"]: item for item in entry["capabilities"]}
        self.assertEqual(items["refresh"]["state"], "unsupported")
        self.assertEqual(items["refresh"]["capabilityValue"], "unsupported")
        self.assertEqual(items["harness-protocol"]["state"], "supported")
        self.assertEqual(items["model-exposure"]["state"], "unknown")
        self.assertTrue(entry["evidenceFresh"])
        # An observed negative never silently enables deferral.
        self.assertEqual(entry["effectiveToolExposure"], "native")
        self.assertIn(
            "model-exposure is not proven (modelExposure=unknown)", entry["toolExposureBlockers"],
        )

    def test_declared_client_version_is_bounded_and_never_capability_evidence(self):
        self.assertIsNone(bridge._declared_client_version(None))
        self.assertIsNone(bridge._declared_client_version(""))
        self.assertEqual(
            bridge._declared_client_version("dsh 0.1.1-rc.2"), "dsh 0.1.1-rc.2",
        )
        with self.assertRaisesRegex(bridge.BridgeError, "1-64 printable"):
            bridge._declared_client_version("x" * 65)
        with self.assertRaisesRegex(bridge.BridgeError, "printable ASCII"):
            bridge._declared_client_version("dsh\t1")
        # It is pinned inside the fingerprint, so a different declaration is a
        # different identity, and it never changes a capability verdict.
        first = hv.version_fingerprint(self.identity())
        second = hv.version_fingerprint(self.identity(declaredClientVersion="dsh 9.9"))
        self.assertNotEqual(first, second)
        self.assertEqual(
            set(self.identity()), {
                "clientKind", "observedClient", "protocolVersion",
                "configFingerprint", "declaredClientVersion",
            },
        )
        with self.assertRaises(hv.ProbeValidationError):
            hv.version_fingerprint({})


if __name__ == "__main__":
    unittest.main()
