"""P8 tests: bounded metadata-only cross-host per-business-call correlation.

Covers the pure helpers in ``stream_evidence.py``, the BridgeNode data-observer
wiring (``_observe_correlation``/evidence queue/worker), the EventJournal
``recent()`` correlation_id readback, and a real two-node pair proving that
both hosts derive the same opaque correlation alias for one business call
(including fragmented request/reply transport), that raw bytes stay
transparent, that payload canaries never reach the journals, and that observer
overload/oversize never blocks the data plane.

Run from the repository root:
    PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s tests -t .
"""

from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
WIN = ROOT / "win-bridge-mcp" / "bridge.py"
WSL = ROOT / "wsl-bridge-mcp" / "bridge.py"
FIXTURE = ROOT / "tests" / "fixtures" / "fixture_mcp.py"

sys.path.insert(0, str(ROOT))

import bridge_runtime  # noqa: E402
import stream_evidence  # noqa: E402

from bridge_runtime import EventJournal, Registry  # noqa: E402


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


def write_registry(
    path: Path,
    server_id: str,
    fixture_name: str,
    *,
    multi_process_allowed: bool = False,
) -> None:
    manifest = path.with_name(path.name + ".manifest.json")
    manifest.write_text(
        json.dumps(
            {
                "servers": [
                    {
                        "id": server_id,
                        "name": fixture_name,
                        "summary": f"Integration fixture hosted on {fixture_name}.",
                        "command": sys.executable,
                        "args": [str(FIXTURE)],
                        "cwd": str(ROOT),
                        "env": {
                            "FIXTURE_MCP_NAME": fixture_name,
                            "FIXTURE_EXIT_AFTER_CALL": "1",
                        },
                        "token": "must-not-leak",
                        "process": {
                            "multiProcessAllowed": multi_process_allowed,
                            "enforcement": "business-mcp" if multi_process_allowed else "bridge-shared-backend",
                        },
                        "capabilityGroups": ["test", "echo", "artifact-delivery"],
                        "artifactDelivery": {
                            "enabled": True,
                            "maxBytes": 1048576,
                        },
                        "inputDelivery": {
                            "enabled": multi_process_allowed,
                            "maxBytes": 1048576,
                        },
                    }
                ]
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    Registry.initialize_database(path, manifest, replace=True)


def _line(value: object) -> bytes:
    return (json.dumps(value, separators=(",", ":")) + "\n").encode("utf-8")


def _asyncio(coroutine):  # noqa: ANN001
    return asyncio.run(coroutine)


class StreamEvidenceModuleTest(unittest.TestCase):
    """Pure helper behavior: framing, classification, alias derivation."""

    def test_fragmented_request_and_reply_share_one_alias(self) -> None:
        request = _line(
            {"jsonrpc": "2.0", "id": 7, "method": "tools/call",
             "params": {"name": "echo", "arguments": {"value": "x"}}}
        )
        response = _line({"jsonrpc": "2.0", "id": 7, "result": {"content": []}})
        correlator = stream_evidence.StreamCorrelator(side="wsl", max_message_bytes=4096)
        observed: list[stream_evidence.Observation] = []
        for index in range(0, len(request), 3):  # every byte split across frames
            observed += correlator.feed(stream_id="s1", flow="outbound",
                                        data=request[index:index + 3])
        for index in range(0, len(response), 5):
            observed += correlator.feed(stream_id="s1", flow="inbound",
                                        data=response[index:index + 5])
        self.assertEqual([item.kind for item in observed], ["request", "response"])
        self.assertIsNotNone(observed[0].correlation_id)
        self.assertEqual(observed[0].correlation_id, observed[1].correlation_id)
        self.assertEqual(observed[0].occurrence, 1)

    def test_alias_is_direction_consistent_across_nodes(self) -> None:
        request = _line({"jsonrpc": "2.0", "id": 5, "method": "ping", "params": {}})
        response = _line({"jsonrpc": "2.0", "id": 5, "result": {}})
        left = stream_evidence.StreamCorrelator(side="wsl", max_message_bytes=4096)
        right = stream_evidence.StreamCorrelator(side="win", max_message_bytes=4096)
        left_obs = left.feed(stream_id="shared", flow="outbound", data=request)
        left_obs += left.feed(stream_id="shared", flow="inbound", data=response)
        # The peer sees the same ordered bytes with mirrored labels.
        right_obs = right.feed(stream_id="shared", flow="inbound", data=request)
        right_obs += right.feed(stream_id="shared", flow="outbound", data=response)
        self.assertEqual(
            [item.correlation_id for item in left_obs],
            [item.correlation_id for item in right_obs],
        )
        self.assertEqual(left_obs[0].correlation_id, left_obs[1].correlation_id)
        # Aliases are opaque fixed-length digests, not the raw inputs.
        for item in left_obs:
            assert item.correlation_id is not None
            self.assertRegex(item.correlation_id, r"^ev1:[0-9a-f]{32}$")
            self.assertNotIn("shared", item.correlation_id)

    def test_request_id_reuse_gets_distinct_occurrence_aliases(self) -> None:
        correlator = stream_evidence.StreamCorrelator(side="wsl", max_message_bytes=4096)
        first = _line({"jsonrpc": "2.0", "id": 9, "method": "tools/call",
                       "params": {"name": "a"}})
        second = _line({"jsonrpc": "2.0", "id": 9, "method": "tools/call",
                        "params": {"name": "b"}})
        reply = _line({"jsonrpc": "2.0", "id": 9, "result": {}})
        observed = correlator.feed(stream_id="st", flow="outbound", data=first)
        observed += correlator.feed(stream_id="st", flow="inbound", data=reply)
        observed += correlator.feed(stream_id="st", flow="outbound", data=second)
        observed += correlator.feed(stream_id="st", flow="inbound", data=reply)
        self.assertEqual(
            [item.occurrence for item in observed], [1, 1, 2, 2]
        )
        self.assertEqual(observed[0].correlation_id, observed[1].correlation_id)
        self.assertEqual(observed[2].correlation_id, observed[3].correlation_id)
        self.assertNotEqual(observed[0].correlation_id, observed[2].correlation_id)

    def test_unpaired_response_and_unparseable_lines_are_flagged(self) -> None:
        correlator = stream_evidence.StreamCorrelator(side="wsl", max_message_bytes=4096)
        stray = correlator.feed(stream_id="st", flow="inbound",
                                data=_line({"jsonrpc": "2.0", "id": 99, "result": {}}))
        self.assertEqual(stray[0].kind, "response")
        self.assertIsNone(stray[0].correlation_id)
        self.assertEqual(stray[0].note, "response-without-observed-request")
        garbage = correlator.feed(stream_id="st", flow="outbound", data=b"not json at all\n")
        self.assertEqual(garbage[0].kind, "other")
        self.assertIsNone(garbage[0].correlation_id)
        self.assertEqual(garbage[0].note, "unparseable")

    def test_notifications_are_not_correlated_rows(self) -> None:
        correlator = stream_evidence.StreamCorrelator(side="wsl", max_message_bytes=4096)
        chunk = (
            _line({"jsonrpc": "2.0", "method": "notifications/progress",
                   "params": {"progressToken": "tok"}})
            + _line({"jsonrpc": "2.0", "id": 1, "method": "ping", "params": {}})
        )
        observed = correlator.feed(stream_id="st", flow="outbound", data=chunk)
        self.assertEqual(len(observed), 1)
        self.assertEqual(observed[0].kind, "request")

    def test_canonical_ids_distinguish_types(self) -> None:
        self.assertEqual(stream_evidence.canonical_request_id(True), "b:true")
        self.assertEqual(stream_evidence.canonical_request_id(1), "i:1")
        self.assertEqual(stream_evidence.canonical_request_id("1"), "s:1")
        self.assertNotEqual(
            stream_evidence.canonical_request_id(1),
            stream_evidence.canonical_request_id("1"),
        )
        self.assertIsNone(stream_evidence.canonical_request_id({"nested": True}))
        self.assertIsNone(stream_evidence.canonical_request_id("x" * 1000))

    def test_oversized_lines_are_counted_and_never_block_later_messages(self) -> None:
        big = b'{"jsonrpc":"2.0","id":1,"method":"x","params":{"v":"' + b"a" * 5000 + b'"}}\n'
        small = b'{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}\n'
        framer = stream_evidence.LineFramer(max_message_bytes=1024)
        completed = framer.feed(big[:2000])
        completed += framer.feed(big[2000:])
        completed += framer.feed(small[:4])
        completed += framer.feed(small[4:])
        self.assertEqual(framer.oversized_messages, 1)
        self.assertEqual(framer.oversized_bytes, len(big) - 1)
        self.assertEqual(completed[-1].kind, "request")
        self.assertEqual(completed[-1].request_id, 2)

    def test_oversize_overflow_across_chunk_boundary_is_counted_once(self) -> None:
        big = b'{"jsonrpc":"2.0","id":1,"method":"x","params":{"v":"' + b"b" * 4000 + b'"}}\n'
        framer = stream_evidence.LineFramer(max_message_bytes=1024)
        # First chunk has no terminator and exceeds the bound -> overflow state.
        first = framer.feed(big[:3000])
        self.assertEqual(first, [])
        self.assertEqual(framer.oversized_messages, 0)
        # Second chunk carries the terminator: the full oversized line closes
        # and is reported as a framing loss (in byte order), never buffered.
        second = framer.feed(big[3000:])
        self.assertEqual(len(second), 1)
        self.assertIsInstance(second[0], stream_evidence.FramingLoss)
        self.assertEqual(second[0].reason, stream_evidence.LOST_OVERSIZED_LINE)
        self.assertEqual(second[0].bytes, len(big) - 1)
        self.assertEqual(framer.oversized_messages, 1)
        self.assertEqual(framer.oversized_bytes, len(big) - 1)
        # A normal message after it is still classified.
        ok = framer.feed(b'{"jsonrpc":"2.0","id":3,"method":"ping","params":{}}\n')
        self.assertEqual(ok[0].kind, "request")

    def test_close_counts_partial_trailing_bytes_only(self) -> None:
        framer = stream_evidence.LineFramer(max_message_bytes=4096)
        framer.feed(b'{"jsonrpc":"2.0","id":1,"method":"ping","params":{}')
        self.assertEqual(framer.close(), [])
        self.assertEqual(framer.partial_bytes, len(b'{"jsonrpc":"2.0","id":1,"method":"ping","params":{}'))

    def test_same_id_in_opposite_directions_never_alias_and_pair_their_own(self) -> None:
        # A client request and a backend request with the SAME id (and the same
        # per-direction occurrence, 1) travel in opposite directions of one
        # stream.  The canonical origin in the alias must keep them apart, and
        # each response must pair with its own request only.
        correlator = stream_evidence.StreamCorrelator(side="win", max_message_bytes=4096)
        client_request = _line({"jsonrpc": "2.0", "id": 5, "method": "ping",
                                "params": {}})          # win -> peer (outbound)
        backend_request = _line({"jsonrpc": "2.0", "id": 5, "method": "ping",
                                 "params": {}})          # peer -> win (inbound)
        client_reply = _line({"jsonrpc": "2.0", "id": 5, "result": {"side": "win"}})
        backend_reply = _line({"jsonrpc": "2.0", "id": 5, "result": {"side": "peer"}})
        observed = correlator.feed(stream_id="st", flow="outbound", data=client_request)
        observed += correlator.feed(stream_id="st", flow="inbound", data=backend_request)
        observed += correlator.feed(stream_id="st", flow="inbound", data=client_reply)
        observed += correlator.feed(stream_id="st", flow="outbound", data=backend_reply)
        self.assertEqual(
            [item.kind for item in observed],
            ["request", "request", "response", "response"],
        )
        self.assertEqual(
            [item.occurrence for item in observed], [1, 1, 1, 1]
        )
        out_alias = observed[0].correlation_id
        in_alias = observed[1].correlation_id
        self.assertIsNotNone(out_alias)
        self.assertIsNotNone(in_alias)
        self.assertNotEqual(out_alias, in_alias)  # canonical origin separates them
        # client_reply (inbound) answers the outbound request; backend_reply
        # (outbound) answers the inbound request — never crossed.
        self.assertEqual(observed[2].correlation_id, out_alias)
        self.assertEqual(observed[3].correlation_id, in_alias)

    def test_duplicate_response_is_flagged_never_falsely_repaired(self) -> None:
        correlator = stream_evidence.StreamCorrelator(side="wsl", max_message_bytes=4096)
        request = _line({"jsonrpc": "2.0", "id": 3, "method": "tools/call",
                         "params": {"name": "echo"}})
        reply = _line({"jsonrpc": "2.0", "id": 3, "result": {}})
        observed = correlator.feed(stream_id="st", flow="outbound", data=request)
        observed += correlator.feed(stream_id="st", flow="inbound", data=reply)
        # A second copy of the same response: the request was already consumed,
        # so it must NOT pair to the request (that would hide a duplicate).
        observed += correlator.feed(stream_id="st", flow="inbound", data=reply)
        self.assertEqual(observed[0].correlation_id, observed[1].correlation_id)
        self.assertIsNotNone(observed[0].correlation_id)
        duplicate = observed[2]
        self.assertEqual(duplicate.kind, "response")
        self.assertIsNone(duplicate.correlation_id)
        self.assertEqual(duplicate.note, "response-without-observed-request")
        # A later, genuine reuse of the id still pairs normally (occ 2).
        observed += correlator.feed(stream_id="st", flow="outbound", data=request)
        observed += correlator.feed(stream_id="st", flow="inbound", data=reply)
        self.assertEqual(observed[-2].occurrence, 2)
        self.assertEqual(observed[-2].correlation_id, observed[-1].correlation_id)
        self.assertNotEqual(observed[-2].correlation_id, observed[0].correlation_id)

    def test_oversized_hidden_id_reuse_degrades_direction_never_false_pairs(self) -> None:
        # Scenario: request id 7 is outstanding; an id-7-reusing request is then
        # lost to the observer (oversized line).  A response for id 7 arrives.
        # Without fail-closed degradation it would pair to the stale occ-1
        # request; the observer must instead degrade and flag unknown.
        correlator = stream_evidence.StreamCorrelator(
            side="wsl", max_message_bytes=1024
        )
        request7 = _line({"jsonrpc": "2.0", "id": 7, "method": "tools/call",
                          "params": {"name": "echo"}})
        reply7 = _line({"jsonrpc": "2.0", "id": 7, "result": {}})
        observed = correlator.feed(stream_id="st", flow="outbound", data=request7)
        self.assertIsNotNone(observed[0].correlation_id)
        self.assertEqual(observed[0].occurrence, 1)
        # Hidden id-7 reuse: an oversized line in the same direction (would be
        # the second request with id 7) that the observer cannot parse.
        hidden = b'{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"echo","arguments":{"value":"' + b"h" * 5000 + b'"}}}\n'
        observed += correlator.feed(stream_id="st", flow="outbound", data=hidden)
        # The response for the hidden request must NOT pair to the stale occ-1
        # request: degrade the request direction and flag unknown.
        observed += correlator.feed(stream_id="st", flow="inbound", data=reply7)
        self.assertEqual(observed[-1].kind, "response")
        self.assertIsNone(observed[-1].correlation_id)
        self.assertTrue(
            observed[-1].note.startswith("direction-degraded:"),
            observed[-1].note,
        )
        # Any later id-7 request in the degraded direction is also unknown (no
        # confident occurrence numbering after an observable loss).
        observed += correlator.feed(stream_id="st", flow="outbound", data=request7)
        self.assertEqual(observed[-1].kind, "request")
        self.assertIsNone(observed[-1].correlation_id)
        self.assertTrue(
            observed[-1].note.startswith("direction-degraded:"),
            observed[-1].note,
        )

    def test_concurrent_same_direction_reuse_is_flagged_ambiguous_not_guessed(self) -> None:
        # Two requests with the same id become outstanding in the same
        # direction before any response (protocol anomaly).  Which of the two
        # the response answers is unknowable, so it must be flagged unknown and
        # pairing for that direction must stop — never a guess.
        correlator = stream_evidence.StreamCorrelator(side="wsl", max_message_bytes=4096)
        request = _line({"jsonrpc": "2.0", "id": 4, "method": "tools/call",
                         "params": {"name": "echo"}})
        reply = _line({"jsonrpc": "2.0", "id": 4, "result": {}})
        observed = correlator.feed(stream_id="st", flow="outbound", data=request)
        observed += correlator.feed(stream_id="st", flow="outbound", data=request)
        first_alias = observed[0].correlation_id
        second_alias = observed[1].correlation_id
        self.assertIsNotNone(first_alias)
        self.assertIsNone(second_alias)  # ambiguity is known at the second request
        for _ in range(10000):
            extra = correlator.feed(stream_id="st", flow="outbound", data=request)
            self.assertIsNone(extra[0].correlation_id)
        self.assertEqual(correlator._pending["outbound"], {})
        self.assertEqual(len(correlator._occurrences["outbound"]), 1)
        # The response cannot be attributed to either outstanding request.
        observed += correlator.feed(stream_id="st", flow="inbound", data=reply)
        ambiguous = observed[-1]
        self.assertEqual(ambiguous.kind, "response")
        self.assertIsNone(ambiguous.correlation_id)
        self.assertIn("concurrent-id-reuse-ambiguous", ambiguous.note)
        # Pairing for the request direction is stopped: another response for
        # the same id stays unknown instead of consuming a stale entry.
        observed += correlator.feed(stream_id="st", flow="inbound", data=reply)
        self.assertEqual(observed[-1].kind, "response")
        self.assertIsNone(observed[-1].correlation_id)
        self.assertTrue(observed[-1].note.startswith("direction-degraded:"))

    def test_unparseable_line_degrades_later_messages_in_that_direction(self) -> None:
        # A line the observer cannot parse could hide an id-reusing request, so
        # after it every later message in that direction must be unknown rather
        # than confidently correlated.
        correlator = stream_evidence.StreamCorrelator(side="wsl", max_message_bytes=4096)
        request = _line({"jsonrpc": "2.0", "id": 8, "method": "ping", "params": {}})
        garbage = correlator.feed(stream_id="st", flow="outbound",
                                  data=b"definitely not json\r\n")
        self.assertEqual(garbage[0].kind, "other")
        self.assertEqual(garbage[0].note, "unparseable")
        after = correlator.feed(stream_id="st", flow="outbound", data=request)
        self.assertEqual(after[0].kind, "request")
        self.assertIsNone(after[0].correlation_id)
        self.assertIsNone(after[0].occurrence)
        self.assertIn("unparseable", after[0].note)
        # The opposite direction (whose request numbering is untouched) can
        # still carry an independent inbound request with a confident alias.
        backend = _line({"jsonrpc": "2.0", "id": 8, "method": "ping", "params": {}})
        inbound = correlator.feed(stream_id="st", flow="inbound", data=backend)
        self.assertEqual(inbound[0].kind, "request")
        self.assertIsNotNone(inbound[0].correlation_id)
        # ... but an inbound response pairing the degraded outbound direction
        # is suppressed (the outstanding outbound request is now unverifiable).
        reply = _line({"jsonrpc": "2.0", "id": 8, "result": {}})
        suppressed = correlator.feed(stream_id="st", flow="inbound", data=reply)
        self.assertEqual(suppressed[0].kind, "response")
        self.assertIsNone(suppressed[0].correlation_id)
        self.assertTrue(suppressed[0].note.startswith("direction-degraded:"))

    def test_unreadable_response_id_degrades_opposite_request_direction(self) -> None:
        # A response whose id cannot be read (here: no id field) means an
        # outstanding request in the opposite direction may never be consumed,
        # so a later response could pair to a stale occurrence.  The opposite
        # direction must degrade instead of risking a false pairing.
        correlator = stream_evidence.StreamCorrelator(side="wsl", max_message_bytes=4096)
        response = _line({"jsonrpc": "2.0", "result": {"content": []}})
        unreadable = correlator.feed(stream_id="st", flow="inbound", data=response)
        self.assertEqual(unreadable[0].kind, "other")
        self.assertEqual(unreadable[0].note, "response-without-scalar-id")
        request = _line({"jsonrpc": "2.0", "id": 6, "method": "tools/call",
                         "params": {"name": "echo"}})
        later = correlator.feed(stream_id="st", flow="outbound", data=request)
        self.assertEqual(later[0].kind, "request")
        self.assertIsNone(later[0].correlation_id)
        self.assertIn("response-id-unreadable", later[0].note)


class EvidenceJournalReadbackTest(unittest.TestCase):
    """EventJournal.recent() exposes the opaque correlation_id."""

    def test_recent_includes_correlation_id_and_readback_is_safe(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            journal = EventJournal(Path(temp) / "events.sqlite3")
            journal.record(
                side="wsl", category="dataflow.correlation",
                correlation_id="ev1:" + "ab" * 16, target="win-echo",
                outcome="request",
                metadata={"kind": "request", "flow": "outbound",
                          "messageBytes": 41, "occurrence": 1, "note": ""},
            )
            row = journal.recent(1)[0]
            self.assertEqual(row["category"], "dataflow.correlation")
            self.assertEqual(row["correlation_id"], "ev1:" + "ab" * 16)
            self.assertEqual(row["metadata"]["flow"], "outbound")
            # Plain rows simply read back correlation_id as None.
            journal.record(side="wsl", category="runtime", metadata={"safe": True})
            plain = journal.recent(1)[0]
            self.assertIn("correlation_id", plain)
            self.assertIsNone(plain["correlation_id"])


class EvidenceObserverNodeTest(unittest.TestCase):
    """BridgeNode wiring: observations reach the journal; never the transport."""

    @staticmethod
    def _node(temp: Path, *, journal: EventJournal) -> bridge_runtime.BridgeNode:
        registry_path = temp / "registry.sqlite3"
        write_registry(registry_path, "win-echo", "windows-fixture-mcp",
                       multi_process_allowed=True)
        return bridge_runtime.BridgeNode(
            side="wsl",
            registry=Registry(registry_path),
            local_host="127.0.0.1",
            local_port=free_port(),
            link_mode="listen",
            link_host="127.0.0.1",
            link_port=free_port(),
            journal=journal,
        )

    @staticmethod
    def _stream(stream_id: str = "st-1", target: str = "win-echo"):
        return bridge_runtime.StreamState(stream_id=stream_id, target=target)

    def test_request_and_response_rows_reach_journal_with_metadata_only(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            journal = EventJournal(Path(temp) / "events.sqlite3")
            node = self._node(Path(temp), journal=journal)

            async def exercise() -> None:
                node._start_evidence_worker()
                stream = self._stream()
                request = _line({"jsonrpc": "2.0", "id": 1, "method": "initialize",
                                 "params": {"protocolVersion": "2025-06-18"}})
                response = _line({"jsonrpc": "2.0", "id": 1,
                                  "result": {"serverInfo": {"name": "x"}}})
                # Fragmented request + fragmented reply through the hook path.
                for index in range(0, len(request), 2):
                    node._observe_correlation(stream, "outbound",
                                              request[index:index + 2])
                for index in range(0, len(response), 3):
                    node._observe_correlation(stream, "inbound",
                                              response[index:index + 3])
                for _ in range(100):
                    if len(journal.recent(50)) >= 2:
                        break
                    await asyncio.sleep(0.02)

            _asyncio(exercise())
            rows = [item for item in journal.recent(50)
                    if item["category"] == "dataflow.correlation"]
            self.assertEqual(len(rows), 2)
            by_kind = {row["metadata"]["kind"]: row for row in rows}
            self.assertIn("request", by_kind)
            self.assertIn("response", by_kind)
            self.assertEqual(by_kind["request"]["correlation_id"],
                             by_kind["response"]["correlation_id"])
            self.assertEqual(by_kind["request"]["side"], "wsl")
            self.assertEqual(by_kind["request"]["outcome"], "request")
            for row in rows:
                # Correlation rows are metadata-only and bounded in shape.
                self.assertEqual(set(row["metadata"].keys()),
                                 {"kind", "flow", "messageBytes", "occurrence", "note"})
                self.assertIn(row["metadata"]["flow"], ("inbound", "outbound"))
                self.assertIn(row["metadata"]["occurrence"], (1, None))

    def test_queue_overload_drops_observations_but_keeps_transport_unaffected(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            journal = EventJournal(Path(temp) / "events.sqlite3")
            node = self._node(Path(temp), journal=journal)

            async def exercise() -> None:
                # A full, worker-less queue: every observation is dropped and
                # counted; nothing blocks and nothing is lost on the wire.
                queue: asyncio.Queue = asyncio.Queue(maxsize=1)
                await queue.put(("blocker", None))
                node._evidence_queue = queue  # type: ignore[assignment]
                stream = self._stream()
                for request_id in range(3):
                    node._observe_correlation(
                        stream, "outbound",
                        _line({"jsonrpc": "2.0", "id": request_id,
                               "method": "tools/list", "params": {}}),
                    )
                self.assertEqual(node._evidence_dropped, 3)
                # Tracking all three valid requests is independent of the
                # evidence queue; only persistence was dropped.
                state = node._evidence_states[stream.stream_id]
                self.assertEqual(state._occurrences["outbound"],
                                 {"i:0": 1, "i:1": 1, "i:2": 1})
                self.assertEqual(node._evidence_oversized_messages, 0)
                # Drain the blocker so the test ends cleanly.
                await queue.get()

            _asyncio(exercise())

    def test_oversize_never_blocks_and_direction_degrades_not_false_pairs(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            journal = EventJournal(Path(temp) / "events.sqlite3")
            node = self._node(Path(temp), journal=journal)
            saved_default = stream_evidence.DEFAULT_MAX_MESSAGE_BYTES
            stream_evidence.DEFAULT_MAX_MESSAGE_BYTES = 1024
            try:
                async def exercise() -> None:
                    queue: asyncio.Queue = asyncio.Queue(maxsize=100)
                    node._evidence_queue = queue  # type: ignore[assignment]
                    stream = self._stream()
                    big = b'{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"echo","arguments":{"value":"' + b"z" * 5000 + b'"}}}\n'
                    node._observe_correlation(stream, "outbound", big[:3000])
                    node._observe_correlation(stream, "outbound", big[3000:])
                    self.assertEqual(node._evidence_oversized_messages, 1)
                    self.assertGreater(node._evidence_oversized_bytes, 5000)
                    # A later request in the degraded direction still reaches
                    # the observer queue (transport unaffected) but is flagged
                    # unknown — never confidently correlated after a loss.
                    small = _line({"jsonrpc": "2.0", "id": 2,
                                   "method": "tools/list", "params": {}})
                    node._observe_correlation(stream, "outbound", small)
                    target, observation = await asyncio.wait_for(queue.get(), timeout=2)
                    self.assertEqual(target, "win-echo")
                    self.assertEqual(observation.kind, "request")
                    self.assertIsNone(observation.correlation_id)
                    self.assertIsNone(observation.occurrence)
                    self.assertTrue(
                        observation.note.startswith("direction-degraded:"),
                        observation.note,
                    )

                _asyncio(exercise())
            finally:
                stream_evidence.DEFAULT_MAX_MESSAGE_BYTES = saved_default

    def test_capacity_never_resets_live_stream_aliases_and_close_reclaims_state(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            journal = EventJournal(Path(temp) / "events.sqlite3")
            node = self._node(Path(temp), journal=journal)
            saved_cap = bridge_runtime.EVIDENCE_STATE_CAP
            bridge_runtime.EVIDENCE_STATE_CAP = 2
            try:
                async def exercise() -> None:
                    streams = {key: self._stream(key, "win-echo") for key in "abcd"}
                    node.streams.update(streams)
                    request = _line({"jsonrpc": "2.0", "id": 1, "method": "ping"})
                    response = _line({"jsonrpc": "2.0", "id": 1, "result": {}})
                    node._observe_correlation(streams["a"], "outbound", request)
                    node._observe_correlation(streams["a"], "inbound", response)
                    retained = node._evidence_states["a"]
                    node._observe_correlation(streams["b"], "outbound", b'{"partial":')
                    node._observe_correlation(streams["c"], "outbound", request)
                    self.assertTrue(streams["c"].evidence_disabled)
                    self.assertIs(node._evidence_states["a"], retained)
                    self.assertNotIn("c", node._evidence_states)
                    node._observe_correlation(streams["a"], "outbound", request)
                    self.assertEqual(retained._occurrences["outbound"]["i:1"], 2)
                    await node._close_stream("b", remote=True)
                    self.assertGreater(node._evidence_partial_bytes, 0)
                    # Capacity recovers, but a stream observed only mid-call
                    # must never be re-admitted with a fresh occurrence counter.
                    node._observe_correlation(streams["c"], "outbound", request)
                    self.assertNotIn("c", node._evidence_states)
                    node._observe_correlation(streams["d"], "outbound", request)
                    self.assertIn("d", node._evidence_states)
                    diagnostics = node._diagnostics_summary({})["correlationEvidence"]
                    self.assertEqual(diagnostics["capacityRejectedStreams"], 1)
                    self.assertEqual(diagnostics["skippedBytes"], len(request) * 2)
                    self.assertEqual(diagnostics["activeStates"], 2)

                _asyncio(exercise())
            finally:
                bridge_runtime.EVIDENCE_STATE_CAP = saved_cap

    def test_observer_never_runs_without_journal(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            node = self._node(Path(temp), journal=None)  # type: ignore[arg-type]
            self.assertIsNone(node._evidence_queue)
            node._observe_correlation(self._stream(), "outbound",
                                      _line({"jsonrpc": "2.0", "id": 1,
                                             "method": "ping", "params": {}}))
            self.assertEqual(node._evidence_dropped, 0)
            self.assertEqual(node._evidence_states, {})


def correlation_rpc_messages(value: str, call_id: int = 3) -> str:
    messages = [
        {"jsonrpc": "2.0", "id": 1, "method": "initialize",
         "params": {"protocolVersion": "2025-06-18", "capabilities": {},
                    "clientInfo": {"name": "evidence-test", "version": "1"}}},
        {"jsonrpc": "2.0", "method": "notifications/initialized"},
        {"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}},
        {"jsonrpc": "2.0", "id": call_id, "method": "tools/call",
         "params": {"name": "echo", "arguments": {"value": value}}},
    ]
    return "".join(json.dumps(item, separators=(",", ":")) + "\n" for item in messages)


def _invoke_proxy(local_port: int, target: str, messages: str) -> list[dict]:
    process = subprocess.run(
        [sys.executable, str(WSL), "connect", target, "--local-port", str(local_port)],
        input=messages, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        cwd=ROOT, timeout=60, check=True,
    )
    return [json.loads(line) for line in process.stdout.splitlines() if line]


def _wait_for_rows(journal_path: Path, *, minimum: int, timeout: float = 12.0) -> list[dict]:
    # Read-only polling: opens the journal with a strict read-only URI and never
    # runs EventJournal.__init__ (WAL pragma / prune writes) from this process.
    # Test-side writes against the same SQLite file the live node's observer
    # worker writes to raced stream teardown and could stall node recording.
    deadline = time.monotonic() + timeout
    rows: list[dict] = []
    while time.monotonic() < deadline:
        try:
            rows = _recent_correlation_rows(journal_path)
        except (OSError, sqlite3.Error):
            rows = []
        if len(rows) >= minimum:
            return rows
        time.sleep(0.1)
    return rows


def _recent_correlation_rows(journal_path: Path, limit: int = 200) -> list[dict]:
    import sqlite3 as _sqlite

    with _sqlite.connect(f"file:{journal_path}?mode=ro", uri=True, timeout=2) as connection:
        connection.row_factory = _sqlite.Row
        return [
            {key: row[key] for key in row.keys() if key != "metadata_json"}
            | {"metadata": json.loads(row["metadata_json"])}
            for row in connection.execute(
                "SELECT seq, occurred_at_ns, side, category, correlation_id, target, "
                "generation, operation_id, outcome, retryable, metadata_json "
                "FROM events WHERE category = 'dataflow.correlation' "
                "ORDER BY seq DESC LIMIT ?",
                (limit,),
            ).fetchall()
        ]


def _latest_correlation_seq(journal_path: Path) -> int:
    """Highest dataflow.correlation seq currently durable in one journal.

    Test-side read-only; used to extract a run's rows by seq delta so the
    assertions never depend on the observer worker's background write order.
    """
    rows = _recent_correlation_rows(journal_path, limit=1)
    return rows[0]["seq"] if rows else 0


class EvidencePairCorrelationTest(unittest.TestCase):
    """Real two-node pair: both journals derive identical opaque aliases."""

    def setUp(self) -> None:
        # A fresh node pair per test method: every method is the first caller on
        # its pair.  Reusing one pair across methods was unreliable — after the
        # first method's stream teardown the observer could silently record
        # nothing for later streams (no error, no rows, no recovery), while a
        # fresh pair's first stream always correlated in every reproduction.
        self.temp = tempfile.TemporaryDirectory()
        temp = Path(self.temp.name)
        self.win_registry = temp / "win.sqlite3"
        self.wsl_registry = temp / "wsl.sqlite3"
        self.win_workspace = temp / "win-workspace"
        self.wsl_workspace = temp / "wsl-workspace"
        self.win_spool = temp / "win-spool"
        self.wsl_spool = temp / "wsl-spool"
        self.win_workspace.mkdir()
        self.wsl_workspace.mkdir()
        write_registry(self.win_registry, "win-echo", "windows-fixture-mcp",
                       multi_process_allowed=True)
        write_registry(self.wsl_registry, "wsl-echo", "wsl-fixture-mcp",
                       multi_process_allowed=True)
        self.link_port = free_port()
        self.win_local_port = free_port()
        self.wsl_local_port = free_port()
        environment = os.environ.copy()
        environment["PYTHONDONTWRITEBYTECODE"] = "1"
        self.win_process = subprocess.Popen(
            [sys.executable, str(WIN), "serve", "--registry", str(self.win_registry),
             "--local-port", str(self.win_local_port), "--link-port", str(self.link_port),
             "--allow-artifact-root", str(self.win_workspace),
             "--artifact-spool-root", str(self.win_spool)],
            cwd=ROOT, env=environment,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        self.wsl_process = subprocess.Popen(
            [sys.executable, str(WSL), "serve", "--registry", str(self.wsl_registry),
             "--local-port", str(self.wsl_local_port), "--link-port", str(self.link_port),
             "--allow-artifact-root", str(self.wsl_workspace),
             "--artifact-spool-root", str(self.wsl_spool)],
            cwd=ROOT, env=environment,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            try:
                from bridge_runtime import local_registry_query
                result = local_registry_query(
                    "127.0.0.1", self.wsl_local_port, "remote", "describe",
                    {"id": "win-echo"},
                )
                if result.get("id") == "win-echo":
                    break
            except (OSError, RuntimeError):
                pass
            except Exception:
                pass
            time.sleep(0.1)
        else:
            self.tearDown()
            self.fail("bridge nodes did not establish their peer link")
        # A fresh pair starts empty; baselines stay zero.  Waits below still
        # count rows relatively for extra robustness (never test-side writes:
        # _recent_correlation_rows is strictly read-only).
        self._win_baseline = len(_recent_correlation_rows(self.win_journal))
        self._wsl_baseline = len(_recent_correlation_rows(self.wsl_journal))
        self._run_number = 0

    def tearDown(self) -> None:
        for name in ("wsl_process", "win_process"):
            process = getattr(self, name, None)
            if process and process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
        temp = getattr(self, "temp", None)
        if temp:
            temp.cleanup()

    @property
    def win_journal(self) -> Path:
        return Path(str(self.win_registry).replace(".sqlite3", "") + ".events.sqlite3")

    @property
    def wsl_journal(self) -> Path:
        return Path(str(self.wsl_registry).replace(".sqlite3", "") + ".events.sqlite3")

    def _correlation_sets(self, value: str, call_id: int = 3) -> tuple[list[dict], list[dict]]:
        # A run's rows are extracted by seq delta (rows written since this
        # invoke started), never by "newest N" slicing: the observer worker
        # writes rows to the journal asynchronously in background order, so
        # assertions must hold for any write schedule.  Each completed run
        # durably appends exactly 6 correlation rows per node (3 requests + 3
        # responses sharing 3 aliases, each alias exactly once per kind).
        before_win = _latest_correlation_seq(self.win_journal)
        before_wsl = _latest_correlation_seq(self.wsl_journal)
        responses = _invoke_proxy(self.wsl_local_port, "win-echo",
                                  correlation_rpc_messages(value, call_id))
        self.assertEqual(len(responses), 3, responses)
        self.assertEqual(
            responses[2]["result"]["structuredContent"]["value"], value,
            "raw dedicated bytes must stay byte-transparent end to end",
        )
        self._run_number += 1
        win_rows = _wait_for_rows(
            self.win_journal, minimum=self._win_baseline + 6 * self._run_number
        )
        wsl_rows = _wait_for_rows(
            self.wsl_journal, minimum=self._wsl_baseline + 6 * self._run_number
        )
        win_run = [row for row in win_rows if row["seq"] > before_win]
        wsl_run = [row for row in wsl_rows if row["seq"] > before_wsl]
        for rows in (win_run, wsl_run):
            # Schedule-independent invariants of one completed run.
            self.assertEqual(len(rows), 6, rows)
            kinds = [row["metadata"]["kind"] for row in rows]
            self.assertEqual(sorted(kinds), ["request", "request", "request",
                                             "response", "response", "response"])
            self.assertEqual({row["metadata"]["occurrence"] for row in rows}, {1})
            aliases = [row["correlation_id"] for row in rows]
            self.assertEqual(len(set(aliases)), 3, rows)
            for alias in set(aliases):
                self.assertEqual(aliases.count(alias), 2, rows)  # request + response
        return win_run, wsl_run

    def test_fragmented_call_correlates_identically_on_both_hosts(self) -> None:
        # The echo response embeds the value twice (content text +
        # structuredContent), so a 72k value makes the request ~72k and the
        # response ~144k: both exceed BUFFER_SIZE (multi-frame transport) yet
        # stay under the observer's passive-parse bound (256 KiB), so both must
        # still correlate.  (A response above the bound is intentionally
        # oversized-counted, never correlated — covered at unit level.)
        value = "evidence-" + "f" * 72_000
        win_rows, wsl_rows = self._correlation_sets(value)
        for rows in (win_rows, wsl_rows):
            requests = [row for row in rows if row["outcome"] == "request"]
            responses = [row for row in rows if row["outcome"] == "response"]
            self.assertGreaterEqual(len(requests), 3, rows)
            self.assertGreaterEqual(len(responses), 3, rows)
            # Fragmentation proof: a request and a response larger than one
            # transport frame were still correlated (byte-transparent).
            self.assertTrue(any(row["metadata"]["messageBytes"] > 65_536
                                for row in requests), rows)
            self.assertTrue(any(row["metadata"]["messageBytes"] > 65_536
                                for row in responses), rows)
            for row in rows:
                self.assertRegex(row["correlation_id"] or "",
                                 r"^ev1:[0-9a-f]{32}$")
            request_ids = {row["correlation_id"] for row in requests}
            response_ids = {row["correlation_id"] for row in responses}
            # Every request alias has exactly one matching response alias...
            self.assertEqual(request_ids, response_ids)
            # ...and no alias was emitted twice on one host.
            self.assertEqual(len(requests), len(request_ids))
            self.assertEqual(len(responses), len(response_ids))
        # The opaque alias multisets are identical on both hosts.
        win_alias = {row["correlation_id"]: row["metadata"]["kind"]
                     for row in win_rows}
        wsl_alias = {row["correlation_id"]: row["metadata"]["kind"]
                     for row in wsl_rows}
        self.assertEqual(win_alias, wsl_alias)
        # Request on the caller host is outbound; on the hosting host inbound.
        win_request_flows = {row["metadata"]["flow"] for row in win_rows
                             if row["metadata"]["kind"] == "request"}
        wsl_request_flows = {row["metadata"]["flow"] for row in wsl_rows
                             if row["metadata"]["kind"] == "request"}
        self.assertEqual(win_request_flows, {"inbound"})
        self.assertEqual(wsl_request_flows, {"outbound"})

    def test_canary_payload_never_reaches_either_journal(self) -> None:
        canary = "EVIDENCE-CANARY-9f3a1c-secret"
        value = f"payload:{canary}:{canary}"
        win_rows, wsl_rows = self._correlation_sets(value)
        for rows in (win_rows, wsl_rows):
            serialized = json.dumps(rows)
            self.assertNotIn(canary, serialized)
            # Correlation rows must not carry paths/args/results/ids.
            for row in rows:
                self.assertEqual(
                    set(row["metadata"].keys()),
                    {"kind", "flow", "messageBytes", "occurrence", "note"},
                )
        # The raw journal files themselves never contain the payload canary.
        for journal_path in (self.win_journal, self.wsl_journal):
            with open(journal_path, "rb") as handle:
                raw = handle.read()
            self.assertNotIn(canary.encode("utf-8"), raw)

    def test_two_calls_with_reused_id_correlate_as_distinct_occurrences(self) -> None:
        # Reuse the request id across two sequential streams: each stream gets
        # its own occurrence-1 alias; no false pairing across streams.
        first_rows, _ = self._correlation_sets("first", call_id=3)
        second_rows, _ = self._correlation_sets("second", call_id=3)
        for rows in (first_rows, second_rows):
            # 3 requests + 3 responses sharing the same 3 aliases per stream.
            self.assertEqual({row["metadata"]["occurrence"] for row in rows}, {1})
            self.assertEqual(len(rows), 6)
            self.assertEqual(len({row["correlation_id"] for row in rows}), 3)
        first_aliases = {row["correlation_id"] for row in first_rows}
        second_aliases = {row["correlation_id"] for row in second_rows}
        # Stream identity differs, so aliases never collide across streams.
        self.assertEqual(len(first_aliases), 3)
        self.assertEqual(len(second_aliases), 3)
        self.assertFalse(first_aliases & second_aliases)


if __name__ == "__main__":
    unittest.main()
