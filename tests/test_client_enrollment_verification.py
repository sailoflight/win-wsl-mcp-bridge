"""Client enrollment evidence and conservative exposure policy regressions."""
from __future__ import annotations

import hashlib
import json
import sqlite3
import time
import unittest
from unittest import mock

import bridge_runtime as bridge
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

    def evidence(self, *, native="unsupported", refresh="supported", model="supported"):
        return {
            "environmentId": self.environment_id,
            "clientKind": "dsh", "probeId": "fixture-probe",
            "configFingerprint": hashlib.sha256(self.overlay.read_bytes()).hexdigest(),
            "validUntil": time.time() + 3600,
            "protocolVersion": "2025-06-18",
            "capabilities": {"toolsListChanged": refresh, "modelExposure": model,
                             "nativeToolSearch": native},
        }

    def save_evidence(self, value, mode="auto"):
        with self.database._connect(write=True) as connection:
            connection.execute(
                "UPDATE agent_environments SET tool_exposure=?, harness_verification_json=? "
                "WHERE environment_id=?", (mode, json.dumps(value), self.environment_id),
            )

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

    def test_expired_foreign_modern_and_changed_config_cannot_enable_deferred(self):
        for change in [
            {"validUntil": 1}, {"environmentId": "other"},
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


if __name__ == "__main__":
    unittest.main()
