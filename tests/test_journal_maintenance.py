"""Offline journal maintenance gates; never use a deployed journal."""

from contextlib import closing
from pathlib import Path
import json
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

from bridge_runtime import EventJournal
from journal_maintenance import JournalMaintenanceError, compact_journal


class JournalMaintenanceTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "journal.sqlite3"
        self.journal = EventJournal(self.path)

    def test_preview_is_read_only_and_apply_preserves_records(self):
        self.journal.record(side="wsl", category="retained", metadata={"canary": "not-public"})
        with closing(sqlite3.connect(self.path)) as connection:
            connection.execute("CREATE TABLE temporary_growth (payload BLOB)")
            connection.execute("INSERT INTO temporary_growth VALUES (zeroblob(2097152))")
            connection.commit()
            connection.execute("DROP TABLE temporary_growth")
            connection.commit()
            connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        before = self.path.read_bytes()
        preview = compact_journal(self.path)
        self.assertFalse(preview["applied"])
        self.assertTrue(preview["requiresConfirmation"])
        self.assertGreater(preview["reclaimablePageBytes"], 1024 * 1024)
        self.assertEqual(self.path.read_bytes(), before)
        expected = self.journal.recent()
        result = compact_journal(self.path, confirm=True)
        self.assertTrue(result["applied"])
        self.assertLess(result["after"]["pages"], result["before"]["pages"])
        self.assertEqual(self.journal.recent(), expected)
        self.assertNotIn("not-public", json.dumps(result))
        self.assertNotIn(str(self.path), json.dumps(result))
        with closing(sqlite3.connect(self.path)) as connection:
            self.assertEqual(connection.execute("PRAGMA integrity_check").fetchone(), ("ok",))

    def test_missing_or_unrelated_database_is_not_created_or_changed(self):
        missing = self.path.with_name("missing.sqlite3")
        with self.assertRaises(JournalMaintenanceError):
            compact_journal(missing, confirm=True)
        self.assertFalse(missing.exists())
        other = self.path.with_name("other.sqlite3")
        with closing(sqlite3.connect(other)) as connection:
            connection.execute("CREATE TABLE private_data (value TEXT)")
        before = other.read_bytes()
        with self.assertRaises(JournalMaintenanceError):
            compact_journal(other, confirm=True)
        self.assertEqual(other.read_bytes(), before)

    def test_busy_writer_fails_without_losing_records(self):
        with closing(sqlite3.connect(self.path)) as writer:
            writer.execute("BEGIN IMMEDIATE")
            writer.execute("INSERT INTO events (occurred_at_ns, monotonic_ns, side, category, metadata_json) "
                           "VALUES (1, 1, 'wsl', 'pending', '{}')")
            with self.assertRaises(JournalMaintenanceError):
                compact_journal(self.path, confirm=True, timeout_seconds=1)
            writer.commit()
        self.assertEqual(self.journal.recent()[0]["category"], "pending")

    def test_deadline_interrupt_keeps_database_integrity(self):
        with closing(sqlite3.connect(self.path)) as connection:
            connection.execute("CREATE TABLE retained_rows (payload TEXT)")
            connection.executemany("INSERT INTO retained_rows VALUES (?)",
                                   [("retained-record",)] * 10000)
            connection.commit()
        ticks = iter([0.0])
        with mock.patch("journal_maintenance.time.monotonic",
                        side_effect=lambda: next(ticks, 2.0)):
            with self.assertRaises(JournalMaintenanceError):
                compact_journal(self.path, confirm=True, timeout_seconds=1)
        with closing(sqlite3.connect(self.path)) as connection:
            self.assertEqual(connection.execute("PRAGMA integrity_check").fetchone(), ("ok",))
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM retained_rows").fetchone(),
                             (10000,))

    def test_invalid_limits_fail_closed(self):
        for value in (True, 0, 61, "10", 1.5):
            with self.subTest(value=value), self.assertRaises(JournalMaintenanceError):
                compact_journal(self.path, confirm=True, timeout_seconds=value)

    def test_cli_preview_and_missing_journal(self):
        root = Path(__file__).resolve().parents[1]
        for side in ("win", "wsl"):
            command = [sys.executable, str(root / (side + "-bridge-mcp") / "bridge.py"),
                       "trace", "compact", "--journal", str(self.path)]
            result = subprocess.run(command, capture_output=True, text=True, timeout=10)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertFalse(json.loads(result.stdout)["applied"])
            command[-1] = str(self.path.with_name("missing.sqlite3"))
            result = subprocess.run(command, capture_output=True, text=True, timeout=10)
            self.assertEqual(result.returncode, 1)
            self.assertNotIn("Traceback", result.stderr)
            self.assertFalse(Path(command[-1]).exists())


if __name__ == "__main__":
    unittest.main()
