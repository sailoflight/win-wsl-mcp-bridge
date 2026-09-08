"""Explicit offline maintenance for a host-local Bridge event journal.

Kept outside the transport runtime so a VACUUM can never enter a request or
capture hot path. SQLite owns rollback of an interrupted VACUUM; the operator
should quiesce journal writers before confirming this maintenance operation.
"""

from __future__ import annotations

from contextlib import closing
from pathlib import Path
import sqlite3
import time
from typing import Any


class JournalMaintenanceError(RuntimeError):
    """A bounded, payload-free maintenance failure."""


def _statistics(connection: sqlite3.Connection) -> dict[str, int]:
    return {
        "pageBytes": int(connection.execute("PRAGMA page_size").fetchone()[0]),
        "pages": int(connection.execute("PRAGMA page_count").fetchone()[0]),
        "freePages": int(connection.execute("PRAGMA freelist_count").fetchone()[0]),
    }


def compact_journal(
    path: Path, *, confirm: bool = False, timeout_seconds: int = 10
) -> dict[str, Any]:
    """Preview or compact one existing journal without deleting logical records.

    The preview opens SQLite read-only and never creates/migrates/prunes a file.
    Apply uses SQLite's transactional VACUUM, a bounded lock wait, and a progress
    deadline. OS filesystem stalls are outside the SQLite execution deadline.
    Neither journal payloads nor a database path appear in the result.
    """
    if (not isinstance(timeout_seconds, int) or isinstance(timeout_seconds, bool)
            or not 1 <= timeout_seconds <= 60):
        raise JournalMaintenanceError("maintenance timeout must be an integer from 1 to 60 seconds")
    if not isinstance(confirm, bool):
        raise JournalMaintenanceError("maintenance confirmation must be boolean")
    path = path.expanduser().absolute()
    if path.is_symlink() or not path.is_file():
        raise JournalMaintenanceError("maintenance requires an existing regular journal file")
    mode = "rw" if confirm else "ro"
    deadline = time.monotonic() + timeout_seconds
    try:
        with closing(sqlite3.connect(path.as_uri() + "?mode=" + mode, uri=True,
                                     timeout=0.25, isolation_level=None)) as connection:
            connection.set_progress_handler(lambda: int(time.monotonic() >= deadline), 1000)
            tables = {row[0] for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )}
            if not {"events", "traces", "trace_records"}.issubset(tables):
                raise JournalMaintenanceError("selected database is not a Bridge event journal")
            before = _statistics(connection)
            result: dict[str, Any] = {
                "applied": False,
                "requiresConfirmation": not confirm,
                "operation": "journal_compaction",
                "before": before,
                "reclaimablePageBytes": before["freePages"] * before["pageBytes"],
                "payloadsIncluded": False,
                "note": "Quiesce journal writers; interrupted VACUUM is rolled back by SQLite. "
                        "This is space reclamation, not secure erasure or retention pruning.",
            }
            if not confirm:
                return result
            checkpoint = connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
            if checkpoint[0]:
                raise JournalMaintenanceError("journal is busy; quiesce readers and writers before maintenance")
            connection.execute("VACUUM")
            connection.set_progress_handler(None, 0)
            # A writer may start after VACUUM commits. Report a busy checkpoint
            # honestly rather than claiming that committed compaction rolled back.
            checkpoint = connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
            result.update({
                "applied": True,
                "after": _statistics(connection),
                "checkpointBusy": bool(checkpoint[0]),
            })
            return result
    except sqlite3.Error as exc:
        raise JournalMaintenanceError(
            "journal maintenance failed or timed out; no logical records were intentionally removed"
        ) from exc
