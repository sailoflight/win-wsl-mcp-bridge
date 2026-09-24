"""Atomic projection upgrades and non-mutating previews using temporary fixtures."""
from __future__ import annotations

import os
from pathlib import Path
import sqlite3
import tempfile
import threading
import unittest
from unittest import mock

from installer import projection as bridge
from tests.test_bridge import ProjectionHarness


def make_v4(path: Path) -> None:
    schema = bridge.PROJECTION_SCHEMA.replace(
        "    tool_exposure TEXT NOT NULL DEFAULT 'native',\n", ""
    ).replace("    harness_verification_json TEXT NOT NULL DEFAULT '{}',\n", "")
    with sqlite3.connect(path) as connection:
        connection.executescript(schema)
        connection.execute("PRAGMA user_version=4")
        connection.execute("INSERT INTO projection_meta VALUES ('fixture', 'retained')")


class ProjectionDatabasePreviewTest(unittest.TestCase):
    def test_v4_failed_second_alter_rolls_back_then_retries(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "projection.sqlite3"
            make_v4(path)
            connect = sqlite3.connect

            class FailingConnection(sqlite3.Connection):
                def execute(self, sql, *args, **kwargs):
                    if sql.startswith("ALTER TABLE") and "harness_verification_json" in sql:
                        raise sqlite3.OperationalError("injected second ALTER failure")
                    return super().execute(sql, *args, **kwargs)

            with mock.patch.object(bridge.sqlite3, "connect", side_effect=lambda *a, **k:
                                   connect(*a, **k, factory=FailingConnection)):
                with self.assertRaisesRegex(sqlite3.OperationalError, "injected"):
                    bridge.ProjectionDatabase.ensure(path)
            with connect(path) as connection:
                self.assertEqual(connection.execute("PRAGMA user_version").fetchone()[0], 4)
                columns = {row[1] for row in connection.execute("PRAGMA table_info(agent_environments)")}
                self.assertNotIn("tool_exposure", columns)
                self.assertNotIn("harness_verification_json", columns)
            bridge.ProjectionDatabase.ensure(path)
            bridge.ProjectionDatabase.ensure(path)
            database = bridge.ProjectionDatabase(path)
            with database._connect() as connection:
                self.assertEqual(database.meta_value(connection, "fixture"), "retained")

    def test_concurrent_upgraders_read_version_after_acquiring_lock(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "projection.sqlite3"
            make_v4(path)
            # WAL is established before the contenders to isolate schema-lock
            # ordering from journal-mode transitions.
            with sqlite3.connect(path) as connection:
                connection.execute("PRAGMA journal_mode=WAL")
            barrier = threading.Barrier(2)
            connect = sqlite3.connect
            errors = []

            class ConcurrentConnection(sqlite3.Connection):
                def execute(self, sql, *args, **kwargs):
                    if sql == "PRAGMA synchronous = NORMAL":
                        result = super().execute(sql, *args, **kwargs)
                        barrier.wait(timeout=5)
                        return result
                    result = super().execute(sql, *args, **kwargs)
                    if sql == "PRAGMA user_version" and not self.in_transaction:
                        # Old code let both contenders observe version 4 before
                        # either acquired a writer lock. Reproduce that ordering.
                        barrier.wait(timeout=5)
                    return result

            def upgrade():
                try:
                    bridge.ProjectionDatabase.ensure(path)
                except Exception as exc:
                    errors.append(exc)

            with mock.patch.object(bridge.sqlite3, "connect", side_effect=lambda *a, **k:
                                   connect(*a, **k, factory=ConcurrentConnection)):
                workers = [threading.Thread(target=upgrade) for _ in range(2)]
                for worker in workers:
                    worker.start()
                for worker in workers:
                    worker.join(timeout=10)
                self.assertFalse(any(worker.is_alive() for worker in workers))
            self.assertEqual(errors, [])
            bridge.ProjectionDatabase(path)

    def test_genuine_older_schemas_upgrade_to_current(self):
        for version in (1, 2, 3):
            with self.subTest(version=version), tempfile.TemporaryDirectory() as temporary:
                path = Path(temporary) / "projection.sqlite3"
                make_v4(path)
                with sqlite3.connect(path) as connection:
                    connection.execute("ALTER TABLE agent_environments DROP COLUMN stdio_http_endpoints_json")
                    if version <= 2:
                        connection.execute("ALTER TABLE agent_environments DROP COLUMN relay_base_url")
                    if version == 1:
                        connection.execute("ALTER TABLE agent_environments DROP COLUMN compatibility_route")
                        connection.execute("ALTER TABLE agent_environments DROP COLUMN transport_capabilities_json")
                        connection.execute("ALTER TABLE peer_projection_state DROP COLUMN transport")
                    connection.execute(f"PRAGMA user_version={version}")
                bridge.ProjectionDatabase.ensure(path)
                database = bridge.ProjectionDatabase(path)
                with database._connect() as connection:
                    self.assertEqual(database.meta_value(connection, "fixture"), "retained")
                    columns = {row[1] for row in connection.execute("PRAGMA table_info(agent_environments)")}
                    self.assertTrue({"tool_exposure", "harness_verification_json", "relay_base_url",
                                     "stdio_http_endpoints_json"} <= columns)

    def test_observation_only_keeps_mode_bytes_and_rejects_writes(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "projection.sqlite3"
            bridge.ProjectionDatabase.ensure(path)
            if os.name != "nt":
                path.chmod(0o640)
            before = path.read_bytes(), path.stat().st_mode
            database = bridge.ProjectionDatabase(path, observe_only=True)
            with database._connect() as connection:
                with self.assertRaises(sqlite3.OperationalError):
                    connection.execute("INSERT INTO projection_meta VALUES ('bad', 'write')")
            with self.assertRaises(bridge.BridgeError):
                database._connect(write=True)
            self.assertEqual((path.read_bytes(), path.stat().st_mode), before)

    def test_preview_reads_committed_wal_without_changing_source_rows(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "wal # preview.sqlite3"
            bridge.ProjectionDatabase.ensure(path)
            writer = sqlite3.connect(path)
            try:
                writer.execute("PRAGMA wal_autocheckpoint=0")
                writer.execute("INSERT INTO peer_projection_state VALUES ('wal-peer', 'WAL peer', 'stdio', 1, 1)")
                writer.commit()
                before = path.read_bytes(), Path(str(path) + "-wal").read_bytes()
                result = bridge.projection_reconcile(projection=path, side="wsl", dry_run=True)
                self.assertEqual(result["mirrorServers"], ["wal-peer"])
                self.assertEqual((path.read_bytes(), Path(str(path) + "-wal").read_bytes()), before)
            finally:
                writer.close()

    def test_preview_upgrades_only_copy_and_missing_path_stays_missing(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            path = root / "projection.sqlite3"
            make_v4(path)
            before = path.read_bytes(), path.stat().st_mode
            result = bridge.projection_reconcile(projection=path, side="wsl", dry_run=True)
            self.assertTrue(result["ok"], result)
            self.assertEqual((path.read_bytes(), path.stat().st_mode), before)
            missing = root / "absent" / "projection.sqlite3"
            result = bridge.projection_reconcile(projection=missing, side="wsl", dry_run=True)
            self.assertTrue(result["ok"], result)
            self.assertFalse(missing.parent.exists())


class ProjectionRefreshPreviewTest(ProjectionHarness):
    def test_refresh_preview_changes_no_source_state_or_client_config(self):
        peer = self.make_peer([self.server("alpha")])
        self.enroll(self.candidate("dsh"))
        path = self.projection()
        if os.name != "nt":
            path.chmod(0o640)
        before = path.read_bytes(), path.stat().st_mode
        configs = {item: item.read_bytes() for item in self.dsh_home.rglob("*.json")}
        with sqlite3.connect(path) as connection:
            before_dump = list(connection.iterdump())
        result = bridge.projection_reconcile(
            projection=path, side="wsl", dry_run=True,
            refresh_source="registry-path", peer_registry=peer,
        )
        self.assertTrue(result["ok"], result)
        self.assertEqual(result["mirrorServers"], ["alpha"])
        self.assertTrue(result["environments"][0]["actions"], result)
        self.assertEqual((path.read_bytes(), path.stat().st_mode), before)
        with sqlite3.connect(path) as connection:
            self.assertEqual(list(connection.iterdump()), before_dump)
        self.assertEqual({item: item.read_bytes() for item in self.dsh_home.rglob("*.json")}, configs)


if __name__ == "__main__":
    unittest.main()
