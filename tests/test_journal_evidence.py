"""Offline gates for bounded, explicitly exported cross-host evidence."""

from pathlib import Path
import hashlib
import json
import tempfile
import unittest
import zipfile

from bridge_runtime import EventJournal
from journal_evidence import EvidenceError, correlate_bundles


class JournalEvidenceTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def bundle(self, name, events, *, payloads=False, wrong_digest=False):
        path = self.root / (name + ".zip")
        data = json.dumps(events).encode()
        manifest = {"schemaVersion": 1, "eventCount": len(events),
                    "payloadsIncluded": payloads,
                    "members": {"events.json": {"bytes": len(data),
                                "sha256": "bad" if wrong_digest else hashlib.sha256(data).hexdigest()}}}
        with zipfile.ZipFile(path, "w") as archive:
            archive.writestr("events.json", data)
            archive.writestr("manifest.json", json.dumps(manifest))
        return path

    def test_real_exports_correlate_without_payloads_or_paths(self):
        paths = []
        for side in ("win", "wsl"):
            journal = EventJournal(self.root / (side + ".sqlite3"))
            journal.record(side=side, category="lifecycle", operation_id="op-fixture",
                           target="fixture", outcome="applied", metadata={"secret": "canary-private"})
            path = self.root / (side + ".zip")
            journal.export_bundle(path)
            paths.append(path)
        result = correlate_bundles(paths)
        self.assertEqual(result["operationCount"], 1)
        self.assertTrue(result["operations"][0]["crossHostObserved"])
        self.assertEqual(result["operations"][0]["sides"], ["win", "wsl"])
        self.assertNotIn("canary-private", json.dumps(result))
        self.assertNotIn(str(self.root), json.dumps(result))

    def test_call_aliases_correlate_separately_and_invalid_ids_are_omitted(self):
        alias = "ev1:" + "a" * 32
        paths = []
        for side in ("win", "wsl"):
            journal = EventJournal(self.root / (side + ".sqlite3"))
            journal.record(side=side, category="call-evidence", correlation_id=alias,
                           outcome="request", target="fixture",
                           metadata={"rawRpcId": "private-canary"})
            journal.record(side=side, category="call-evidence", correlation_id="private-canary")
            path = self.root / (side + ".zip")
            journal.export_bundle(path)
            paths.append(path)
        result = correlate_bundles(paths)
        self.assertEqual(result["operations"], [])
        self.assertEqual(result["callCount"], 1)
        self.assertEqual(result["calls"][0]["correlationId"], alias)
        self.assertTrue(result["calls"][0]["crossHostObserved"])
        self.assertEqual(result["uncorrelatedEvents"], 2)
        self.assertNotIn("private-canary", json.dumps(result))

    def test_digest_payload_and_extra_member_rejection(self):
        for options in ({"wrong_digest": True}, {"payloads": True}):
            with self.subTest(options=options), self.assertRaises(EvidenceError):
                correlate_bundles([self.bundle("invalid", [], **options)])
        path = self.bundle("extra", [])
        with zipfile.ZipFile(path, "a") as archive:
            archive.writestr("trace_payload.json", "private")
        with self.assertRaises(EvidenceError):
            correlate_bundles([path])

    def test_limits_and_partial_evidence_are_explicit(self):
        events = [{"operation_id": "op-one", "side": "win"}] * 40
        events += [{"operation_id": "op-two", "side": "win"}, {"category": "uncorrelated"}]
        result = correlate_bundles([self.bundle("bounded", events)], limit=1)
        self.assertEqual(result["operationCount"], 2)
        self.assertTrue(result["truncated"])
        self.assertEqual(result["uncorrelatedEvents"], 1)
        operation = result["operations"][0]
        self.assertFalse(operation["crossHostObserved"])
        self.assertEqual(operation["eventCount"], 40)
        self.assertTrue(operation["eventsTruncated"])
        self.assertEqual(len(operation["events"]), 20)
        for limit in (0, True, 101, "1"):
            with self.subTest(limit=limit), self.assertRaises(EvidenceError):
                correlate_bundles([self.root / "bounded.zip"], limit=limit)
        with self.assertRaises(EvidenceError):
            correlate_bundles([])

    def test_invalid_event_shapes_fail_without_traceback(self):
        path = self.root / "invalid.zip"
        with zipfile.ZipFile(path, "w") as archive:
            archive.writestr("events.json", "[]")
            archive.writestr("manifest.json", '{"schemaVersion":1,"payloadsIncluded":false,"members":[]}')
        with self.assertRaises(EvidenceError):
            correlate_bundles([path])


if __name__ == "__main__":
    unittest.main()
