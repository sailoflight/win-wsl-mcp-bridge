"""Hermetic evidence-contract and real-pipe tests; no installed Harness is used."""
from __future__ import annotations

import copy
import hashlib
import json
import os
from pathlib import Path
import queue
import subprocess
import sys
import tempfile
import threading
import time
import unittest

import harness_verification as hv


BINDING = {
    "environmentId": "environment-test", "clientKind": "synthetic-harness",
    "configFingerprint": hashlib.sha256(b"synthetic effective config").hexdigest(),
}
CLIENT = {"name": "synthetic-harness", "version": "1.2.3"}


class Session:
    def __init__(self):
        self.now = 1000.0
        self.challenge = hv.create_probe(BINDING, now=self.now)
        self.messages = []
        self.saved = []
        self.fixture = hv._Fixture(
            self.challenge, self.emit, lambda value: self.saved.append(copy.deepcopy(value)),
            clock=lambda: self.now,
        )
        self.request_id = 0

    def emit(self, message):
        self.messages.append(copy.deepcopy(message))
        return self.now

    def send(self, method, params=None, *, notification=False, received_at=None):
        self.now += 1
        message = {"jsonrpc": "2.0", "method": method}
        if not notification:
            self.request_id += 1
            message["id"] = self.request_id
        if params is not None:
            message["params"] = params
        offset = len(self.messages)
        self.fixture.handle(message, received_at=received_at)
        return self.messages[offset:]

    def initialize(self):
        self.send("initialize", {
            "protocolVersion": "2025-11-25", "capabilities": {},
            "clientInfo": {**CLIENT, "title": "ignored private-looking title"},
        })
        self.send("notifications/initialized", notification=True)

    def bootstrap(self):
        self.initialize()
        self.send("tools/list")
        self.send("tools/call", {"name": self.challenge["tools"]["bootstrap"], "arguments": {}})

    def complete(self):
        self.bootstrap()
        result = self.send("tools/list")[-1]["result"]
        canary = result["tools"][0]
        proof = canary["inputSchema"]["properties"]["proof"]["const"]
        self.send("tools/call", {"name": canary["name"], "arguments": {"proof": proof}})
        return copy.deepcopy(self.fixture.receipt)

    def validate(self, receipt=None):
        return hv.validate_probe_receipt(
            self.challenge, self.fixture.receipt if receipt is None else receipt,
            now=self.now + 1,
        )


def attestation(session, observation):
    return {
        "schemaVersion": 1, "source": "attested", "actorKind": "agent",
        "probeId": session.challenge["probeId"], "nonce": session.challenge["nonce"],
        "binding": copy.deepcopy(session.challenge["binding"]), "clientInfo": dict(CLIENT),
        "observedAt": session.now + 0.5, "modelId": "synthetic-model-v1",
        "providerId": "synthetic-provider", "observation": observation,
        "evidenceRef": "session:isolated-synthetic-observation",
        "evidenceSha256": hashlib.sha256(b"bounded synthetic context excerpt").hexdigest(),
        "canaryCallSequence": 6,
        "protocolEvidenceSha256": hashlib.sha256(json.dumps({
            "challengeSha256": session.fixture.receipt["challengeSha256"],
            "startedAt": session.fixture.receipt["startedAt"],
            "observations": session.fixture.receipt["observations"],
        }, sort_keys=True, separators=(",", ":")).encode()).hexdigest(),
    }


class ProbeContractTests(unittest.TestCase):
    def test_challenge_is_random_bound_and_redacted(self):
        first = hv.create_probe({
            **BINDING, "configPath": "/private/config", "launcherArgs": ["secret"],
            "env": {"API_KEY": "private-value"},
        }, now=100)
        second = hv.create_probe(BINDING, now=100)
        self.assertEqual(first["binding"], BINDING)
        self.assertNotEqual(first["probeId"], second["probeId"])
        self.assertNotEqual(first["nonce"], second["nonce"])
        self.assertNotEqual(first["tools"]["canary"], second["tools"]["canary"])
        self.assertLessEqual(first["expiresAt"] - 100, hv.PROBE_TTL_SECONDS)
        for secret in ("/private/config", "API_KEY", "private-value", "launcherArgs"):
            self.assertNotIn(secret, json.dumps(first))

    def test_environment_summary_alias_and_bad_bindings(self):
        summary = {k: v for k, v in BINDING.items() if k != "configFingerprint"}
        summary["fileSha256"] = BINDING["configFingerprint"]
        self.assertEqual(hv.create_probe(summary, now=100)["binding"], BINDING)
        bad = [None, {}, {**BINDING, "configFingerprint": "not-a-digest"},
               {**BINDING, "environmentId": "../private"},
               {**BINDING, "fileSha256": "0" * 64}]
        for value in bad:
            with self.subTest(value=value), self.assertRaises(hv.ProbeValidationError):
                hv.create_probe(value, now=100)
        for now in (True, -1, float("nan"), float("inf"), "100"):
            with self.subTest(now=now), self.assertRaises(hv.ProbeValidationError):
                hv.create_probe(BINDING, now=now)

    def test_protocol_completion_does_not_claim_model_or_native_search(self):
        session = Session()
        receipt = session.complete()
        normalized = session.validate(receipt)
        self.assertEqual(normalized["capabilities"], {
            "toolsListChanged": "supported", "modelExposure": "unknown",
            "nativeToolSearch": "unknown",
        })
        self.assertEqual(normalized["observedClientInfo"], CLIENT)
        self.assertEqual(normalized["protocolVersion"], "2025-11-25")
        self.assertEqual(normalized["configFingerprint"], BINDING["configFingerprint"])
        self.assertEqual(normalized["evidence"]["protocolRefresh"]["modelSchemaVisibility"], "not-proven")
        self.assertNotIn("ignored private-looking title", json.dumps(receipt))
        self.assertNotIn(session.fixture.proof, json.dumps(receipt))
        self.assertEqual(receipt["observations"][4]["removedTools"], [session.challenge["tools"]["bootstrap"]])

    def test_every_incomplete_prefix_remains_unknown(self):
        session = Session()
        session.complete()
        for receipt in session.saved[:-1]:
            with self.subTest(count=len(receipt["observations"])):
                normalized = session.validate(receipt)
                self.assertEqual(normalized["capabilities"]["toolsListChanged"], "unknown")
                self.assertEqual(normalized["capabilities"]["modelExposure"], "unknown")

    def test_missing_reordered_and_fabricated_support_fields_are_rejected(self):
        session = Session()
        receipt = session.complete()
        bad_receipts = []
        claimed = copy.deepcopy(receipt)
        claimed["toolsListChanged"] = True
        bad_receipts.append(claimed)
        claimed = copy.deepcopy(receipt)
        claimed["capabilities"] = {"toolsListChanged": "supported"}
        bad_receipts.append(claimed)
        for index in range(6):
            missing = copy.deepcopy(receipt)
            del missing["observations"][index]
            bad_receipts.append(missing)
        reordered = copy.deepcopy(receipt)
        reordered["observations"][3:5] = list(reversed(reordered["observations"][3:5]))
        bad_receipts.append(reordered)
        altered = copy.deepcopy(receipt)
        altered["observations"][4]["removedTools"] = []
        bad_receipts.append(altered)
        altered = copy.deepcopy(receipt)
        altered["observations"][5]["proofSha256"] = "0" * 64
        bad_receipts.append(altered)
        altered = copy.deepcopy(receipt)
        altered["observations"][5]["catalogSequence"] = True
        bad_receipts.append(altered)
        for index, value in enumerate(bad_receipts):
            with self.subTest(case=index), self.assertRaises(hv.ProbeValidationError):
                session.validate(value)

    def test_receipt_rejects_wrong_environment_client_fingerprint_nonce_and_probe(self):
        session = Session()
        receipt = session.complete()
        for field, changed in (("environmentId", "other"), ("clientKind", "other"),
                               ("configFingerprint", "0" * 64)):
            altered = copy.deepcopy(receipt)
            altered["binding"][field] = changed
            with self.subTest(field=field), self.assertRaises(hv.ProbeValidationError):
                session.validate(altered)
        for field in ("nonce", "probeId", "challengeSha256"):
            altered = copy.deepcopy(receipt)
            altered[field] = "0" * len(altered[field])
            with self.subTest(field=field), self.assertRaises(hv.ProbeValidationError):
                session.validate(altered)
        other = hv.create_probe(BINDING, now=1000)
        with self.assertRaises(hv.ProbeValidationError):
            hv.validate_probe_receipt(other, receipt, now=session.now + 1)

    def test_expired_future_and_excessive_time_budgets_rejected(self):
        session = Session()
        receipt = session.complete()
        for now in (999, session.challenge["expiresAt"], float("nan")):
            with self.subTest(now=now), self.assertRaises(hv.ProbeValidationError):
                hv.validate_probe_receipt(session.challenge, receipt, now=now)
        for field, value in (("startedAt", 999), ("updatedAt", session.now + 100)):
            altered = copy.deepcopy(receipt)
            altered[field] = value
            with self.subTest(field=field), self.assertRaises(hv.ProbeValidationError):
                session.validate(altered)
        for field in ("expiresAt", "verificationValidUntil"):
            altered = copy.deepcopy(session.challenge)
            altered[field] += 1
            with self.subTest(field=field), self.assertRaises(hv.ProbeValidationError):
                hv.validate_probe_receipt(altered, receipt, now=session.now + 1)
        altered = copy.deepcopy(receipt)
        altered["observations"][4]["requestReceivedAt"] = altered["observations"][3]["at"] - 1
        with self.assertRaises(hv.ProbeValidationError):
            session.validate(altered)
        altered = copy.deepcopy(receipt)
        altered["observations"][5]["requestReceivedAt"] = altered["observations"][4]["at"] - 1
        with self.assertRaises(hv.ProbeValidationError):
            session.validate(altered)

    def test_later_request_model_exposure_is_valid_without_same_request_barrier(self):
        session = Session()
        session.bootstrap()
        canary = session.send("tools/list")[-1]["result"]["tools"][0]
        # A completed protocol refresh is not yet model exposure. The runner
        # may still have an old request snapshot; this is unknown, not failure.
        pending = session.validate()
        self.assertEqual(pending["capabilities"]["modelExposure"], "unknown")
        session.now += 30  # bounded later model step, not a new probe/replay
        proof = canary["inputSchema"]["properties"]["proof"]["const"]
        session.send("tools/call", {"name": canary["name"], "arguments": {"proof": proof}})
        receipt = copy.deepcopy(session.fixture.receipt)
        receipt["attestations"] = {"modelExposure": attestation(session, "canary-schema-visible-to-model")}
        accepted = session.validate(receipt)
        self.assertEqual(accepted["capabilities"]["toolsListChanged"], "supported")
        self.assertEqual(accepted["capabilities"]["modelExposure"], "supported")
        self.assertEqual(accepted["capabilities"]["nativeToolSearch"], "unknown")

    def test_model_attestation_is_separate_bounded_and_challenge_bound(self):
        session = Session()
        receipt = session.complete()
        receipt["attestations"] = {"modelExposure": attestation(session, "canary-schema-visible-to-model")}
        normalized = session.validate(receipt)
        self.assertEqual(normalized["capabilities"]["modelExposure"], "supported")
        self.assertEqual(normalized["capabilities"]["nativeToolSearch"], "unknown")
        self.assertEqual(normalized["evidence"]["modelExposure"]["source"], "attested")
        receipt["attestations"]["nativeToolSearch"] = attestation(session, "native-search-loaded-canary-schema")
        self.assertEqual(session.validate(receipt)["capabilities"]["nativeToolSearch"], "supported")
        receipt["attestations"]["nativeToolSearch"]["observation"] = "native-search-disabled-in-effective-session"
        self.assertEqual(session.validate(receipt)["capabilities"]["nativeToolSearch"], "unsupported")
        changes = {
            "source": "protocol-proven", "probeId": "0" * 32,
            "nonce": "0" * 64, "clientInfo": {"name": "other", "version": "9"},
            "binding": {**BINDING, "configFingerprint": "0" * 64},
            "observedAt": session.now + 100, "actorKind": [],
            "evidenceRef": "file:///private/config", "evidenceSha256": "not-a-hash",
            "canaryCallSequence": 5, "observation": "I assume it works",
        }
        for key, value in changes.items():
            altered = copy.deepcopy(receipt)
            altered["attestations"]["modelExposure"][key] = value
            with self.subTest(field=key), self.assertRaises(hv.ProbeValidationError):
                session.validate(altered)
        altered = copy.deepcopy(receipt)
        del altered["attestations"]["modelExposure"]["evidenceSha256"]
        with self.assertRaises(hv.ProbeValidationError):
            session.validate(altered)

    def test_attestation_without_successful_protocol_probe_is_rejected(self):
        session = Session()
        session.bootstrap()
        receipt = copy.deepcopy(session.fixture.receipt)
        receipt["attestations"] = {"modelExposure": attestation(session, "canary-schema-visible-to-model")}
        with self.assertRaises(hv.ProbeValidationError):
            session.validate(receipt)

    def test_shape_limits_versions_and_duplicate_json(self):
        session = Session()
        receipt = session.complete()
        for key, value in (("schemaVersion", True), ("observations", {}),
                           ("observations", receipt["observations"] * 2),
                           ("kind", "x" * hv.MAX_RECEIPT_BYTES), ("stopReason", {})):
            altered = copy.deepcopy(receipt)
            altered[key] = value
            with self.subTest(field=key), self.assertRaises(hv.ProbeValidationError):
                session.validate(altered)
        for raw in (b'{"id":1,"id":2}', b'{"value":NaN}', b'\xff', b'{broken'):
            with self.subTest(raw=raw), self.assertRaises(hv.ProbeValidationError):
                hv._parse_json(raw)


class FixtureStateTests(unittest.TestCase):
    def test_refetch_required_even_with_known_name_and_proof(self):
        session = Session()
        session.bootstrap()
        name = session.challenge["tools"]["canary"]
        result = session.send("tools/call", {"name": name, "arguments": {"proof": session.fixture.proof}})
        self.assertIn("error", result[-1])
        self.assertEqual(session.validate()["capabilities"]["toolsListChanged"], "unknown")
        session.send("tools/list")
        result = session.send("tools/call", {"name": name, "arguments": {"proof": "guessed"}})
        self.assertIn("error", result[-1])
        self.assertEqual(session.validate()["capabilities"]["toolsListChanged"], "unknown")
        session.send("tools/call", {"name": name, "arguments": {"proof": session.fixture.proof}})
        self.assertEqual(session.validate()["capabilities"]["toolsListChanged"], "supported")

    def test_pipelined_pre_notification_list_is_not_refresh_evidence(self):
        session = Session()
        session.bootstrap()
        result = session.send("tools/list", received_at=session.fixture.notification_at - 1)
        self.assertIn("result", result[-1])
        self.assertFalse(session.fixture.refetched)
        self.assertEqual(len(session.fixture.receipt["observations"]), 4)
        session.send("tools/list")
        result = session.send("tools/call", {
            "name": session.challenge["tools"]["canary"], "arguments": {"proof": session.fixture.proof},
        }, received_at=session.fixture.catalog_at - 1)
        self.assertIn("error", result[-1])
        self.assertEqual(session.validate()["capabilities"]["toolsListChanged"], "unknown")

    def test_bootstrap_is_removed_and_duplicate_requests_do_not_expand_receipt(self):
        session = Session()
        session.complete()
        previous = copy.deepcopy(session.fixture.receipt)
        for _ in range(5):
            listed = session.send("tools/list", {"_meta": {"progressToken": "ignored"}})[-1]["result"]["tools"]
            self.assertEqual([tool["name"] for tool in listed], [session.challenge["tools"]["canary"]])
            result = session.send("tools/call", {"name": session.challenge["tools"]["bootstrap"]})
            self.assertIn("error", result[-1])
            session.send("tools/call", {"name": listed[0]["name"], "arguments": {"proof": session.fixture.proof}})
        self.assertEqual(session.fixture.receipt, previous)

    def test_modern_unsupported_and_legacy_identity_is_observed_not_guessed(self):
        session = Session()
        for method in ("server/discover", "subscriptions/listen"):
            response = session.send(method)[-1]
            self.assertEqual(response["error"]["code"], -32601)
            self.assertNotIn("capabilities", response)
        for protocol in ("2026-07-28", "2030-01-01", "invalid", None):
            response = session.send("initialize", {
                "protocolVersion": protocol, "capabilities": {}, "clientInfo": CLIENT,
            })[-1]
            self.assertIn("error", response)
        self.assertEqual(session.fixture.receipt["observations"], [])
        for protocol in hv.LEGACY_PROTOCOL_VERSIONS:
            with self.subTest(protocol=protocol):
                probe = Session()
                response = probe.send("initialize", {
                    "protocolVersion": protocol, "capabilities": {"experimental": {"nativeSearch": True}},
                    "clientInfo": {"name": "Codex-or-Claude-name-is-not-proof", "version": "99"},
                })[-1]
                self.assertEqual(response["result"]["protocolVersion"], protocol)
                self.assertEqual(response["result"]["capabilities"], {"tools": {"listChanged": True}})
                self.assertEqual(probe.validate()["capabilities"]["nativeToolSearch"], "unknown")

    def test_unobserved_refresh_is_negative_only_when_the_window_kept_running(self):
        # An ended session (eof) leaves the post-notification window unknown, so a
        # catalog request the Harness pipelined before the notification stays
        # indistinguishable from no request at all: never a negative verdict.
        ended = Session()
        ended.bootstrap()
        self.assertEqual(len(ended.fixture.receipt["observations"]), 4)
        ended.fixture.finish("eof")
        normalized = ended.validate()
        self.assertEqual(normalized["capabilities"]["toolsListChanged"], "unknown")
        self.assertEqual(normalized["evidence"]["protocolRefresh"]["refreshObservation"], "no-verdict-yet")
        self.assertEqual(normalized["evidence"]["protocolRefresh"]["stopReason"], "eof")
        # A live window that kept running for the whole budget, still with no
        # post-notification catalog, is the bounded negative observation.
        for reason in ("timeout", "message-limit"):
            with self.subTest(stopReason=reason):
                idle = Session()
                idle.bootstrap()
                idle.fixture.finish(reason)
                normalized = idle.validate()
                self.assertEqual(normalized["capabilities"]["toolsListChanged"], "unsupported")
                self.assertEqual(normalized["evidence"]["protocolRefresh"]["refreshObservation"],
                                 "notification-without-refresh")
                self.assertEqual(normalized["evidence"]["protocolRefresh"]["receiptStatus"], "incomplete")
        # A refresh without the completing canary call is real evidence of
        # refresh, but not of a completed capability, so it stays unknown.
        partial = Session()
        partial.bootstrap()
        partial.send("tools/list")
        partial.fixture.finish("eof")
        normalized = partial.validate()
        self.assertEqual(normalized["capabilities"]["toolsListChanged"], "unknown")
        self.assertEqual(normalized["evidence"]["protocolRefresh"]["refreshObservation"],
                         "refreshed-without-canary-completion")

    def test_initialization_and_initial_catalog_are_required(self):
        session = Session()
        self.assertIn("error", session.send("tools/list")[-1])
        session.initialize()
        result = session.send("tools/call", {"name": session.challenge["tools"]["bootstrap"]})
        self.assertIn("error", result[-1])
        self.assertEqual(len(session.fixture.receipt["observations"]), 1)

    def test_notification_write_failure_cannot_generate_refresh_evidence(self):
        session = Session()
        session.initialize()
        session.send("tools/list")
        original = session.fixture.emit
        def fail_notification(message):
            if message.get("method") == "notifications/tools/list_changed":
                raise TimeoutError("synthetic blocked output")
            return original(message)
        session.fixture.emit = fail_notification
        with self.assertRaises(TimeoutError):
            session.send("tools/call", {"name": session.challenge["tools"]["bootstrap"]})
        self.assertEqual(len(session.fixture.receipt["observations"]), 3)
        self.assertEqual(session.validate()["capabilities"]["toolsListChanged"], "unknown")


class FixtureProcessTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.probe = self.root / "probe.json"
        self.receipt = self.root / "receipt.json"
        self.challenge = hv.create_probe(BINDING)
        self.probe.write_text(json.dumps(self.challenge), encoding="utf-8")

    def start(self, command=None):
        process = subprocess.Popen(
            command or [sys.executable, "-m", "harness_verification",
                        "--probe", str(self.probe), "--receipt", str(self.receipt)],
            cwd=Path(__file__).resolve().parents[1], stdin=subprocess.PIPE,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
        )
        def cleanup():
            if process.poll() is None:
                process.kill()
            process.communicate(timeout=5)
        self.addCleanup(cleanup)
        return process

    def test_stdio_end_to_end_metadata_receipt_and_atomic_replacement(self):
        self.receipt.write_text("old incomplete output", encoding="utf-8")
        process = self.start()
        received = queue.Queue()
        def read_lines():
            for line in process.stdout:
                received.put(json.loads(line))
        reader = threading.Thread(target=read_lines, daemon=True)
        reader.start()
        def send(method, request_id=None, params=None):
            payload = {"jsonrpc": "2.0", "method": method}
            if request_id is not None:
                payload["id"] = request_id
            if params is not None:
                payload["params"] = params
            process.stdin.write(json.dumps(payload).encode() + b"\n")
            process.stdin.flush()
        def get():
            return received.get(timeout=5)
        send("initialize", 1, {"protocolVersion": "2025-11-25", "clientInfo": CLIENT, "capabilities": {}})
        self.assertEqual(get()["result"]["capabilities"], {"tools": {"listChanged": True}})
        send("notifications/initialized")
        send("tools/list", 2)
        bootstrap = get()["result"]["tools"][0]["name"]
        send("tools/call", 3, {"name": bootstrap, "arguments": {}})
        self.assertIn("result", get())
        self.assertEqual(get()["method"], "notifications/tools/list_changed")
        send("tools/list", 4)
        canary = get()["result"]["tools"][0]
        self.assertNotEqual(canary["name"], bootstrap)
        proof = canary["inputSchema"]["properties"]["proof"]["const"]
        send("tools/call", 5, {"name": canary["name"], "arguments": {"proof": proof}})
        self.assertIn("result", get())
        process.stdin.close()
        process.stdin = None
        self.assertEqual(process.wait(timeout=5), 0)
        reader.join(timeout=5)
        self.assertEqual(process.stderr.read(), b"")
        receipt = json.loads(self.receipt.read_text(encoding="utf-8"))
        normalized = hv.validate_probe_receipt(self.challenge, receipt)
        self.assertEqual(normalized["capabilities"]["toolsListChanged"], "supported")
        self.assertEqual(normalized["capabilities"]["modelExposure"], "unknown")
        self.assertNotIn(proof, self.receipt.read_text())
        self.assertEqual(list(self.root.glob(".harness-receipt-*")), [])
        if os.name != "nt":
            self.assertEqual(self.receipt.stat().st_mode & 0o777, 0o600)

    def test_prequeued_catalog_request_is_not_post_notification_refetch(self):
        process = self.start()
        payloads = [
            {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {
                "protocolVersion": "2025-11-25", "clientInfo": CLIENT, "capabilities": {}}},
            {"jsonrpc": "2.0", "method": "notifications/initialized"},
            {"jsonrpc": "2.0", "id": 2, "method": "tools/list"},
            {"jsonrpc": "2.0", "id": 3, "method": "tools/call", "params": {
                "name": self.challenge["tools"]["bootstrap"]}},
            {"jsonrpc": "2.0", "id": 4, "method": "tools/list"},
        ]
        output, error = process.communicate(
            b"".join(json.dumps(value).encode() + b"\n" for value in payloads), timeout=5,
        )
        self.assertEqual(process.returncode, 0, error)
        messages = [json.loads(line) for line in output.splitlines()]
        self.assertTrue(any(value.get("method") == "notifications/tools/list_changed" for value in messages))
        receipt = json.loads(self.receipt.read_text())
        self.assertEqual(len(receipt["observations"]), 4)
        self.assertEqual(hv.validate_probe_receipt(self.challenge, receipt)["capabilities"]["toolsListChanged"], "unknown")

    def test_live_idle_window_records_a_bounded_negative_verdict(self):
        # The fixture's own run budget is shortened so this stays end-to-end and
        # fast while the challenge lifetime stays realistic: a timeout stop can
        # therefore still be validated afterwards, exactly as production would.
        bootstrap = (
            "import sys, harness_verification as hv\n"
            "hv.MAX_RUNTIME_SECONDS = 1.0\n"
            "raise SystemExit(hv.main(sys.argv[1:]))\n"
        )
        process = self.start([sys.executable, "-c", bootstrap,
                              "--probe", str(self.probe), "--receipt", str(self.receipt)])
        payloads = [
            {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {
                "protocolVersion": "2025-11-25", "clientInfo": CLIENT, "capabilities": {}}},
            {"jsonrpc": "2.0", "method": "notifications/initialized"},
            {"jsonrpc": "2.0", "id": 2, "method": "tools/list"},
            {"jsonrpc": "2.0", "id": 3, "method": "tools/call", "params": {
                "name": self.challenge["tools"]["bootstrap"]}},
        ]
        for payload in payloads:
            process.stdin.write(json.dumps(payload).encode() + b"\n")
        process.stdin.flush()
        # Stdin deliberately stays open: the Harness is still running and simply
        # never asks for the post-notification catalog within the whole budget.
        self.assertEqual(process.wait(timeout=15), 2)
        process.stdin.close()
        process.stdin = None
        self.assertEqual(process.stderr.read(), b"")
        receipt = json.loads(self.receipt.read_text(encoding="utf-8"))
        self.assertEqual(receipt["stopReason"], "timeout")
        self.assertEqual([event["type"] for event in receipt["observations"]], [
            "initialized", "initial_catalog", "bootstrap_call", "list_changed",
        ])
        normalized = hv.validate_probe_receipt(self.challenge, receipt)
        self.assertEqual(normalized["capabilities"]["toolsListChanged"], "unsupported")
        self.assertEqual(normalized["evidence"]["protocolRefresh"]["refreshObservation"],
                         "notification-without-refresh")

    def test_open_idle_stdin_has_a_deadline(self):
        self.challenge["expiresAt"] = time.time() + 0.5
        self.probe.write_text(json.dumps(self.challenge))
        process = self.start()
        self.assertEqual(process.wait(timeout=5), 2)
        self.assertEqual(process.stdout.read(), b"")
        receipt = json.loads(self.receipt.read_text())
        self.assertEqual(receipt["status"], "incomplete")
        self.assertEqual(receipt["stopReason"], "timeout")

    def test_oversized_and_malformed_input_fail_without_payload_receipts(self):
        for data in (b"x" * (hv.MAX_MESSAGE_BYTES + 2), b'{"id":1,"id":2}\n'):
            with self.subTest(size=len(data)):
                process = self.start()
                output, _ = process.communicate(data, timeout=5)
                self.assertEqual(process.returncode, 2)
                for line in output.splitlines():
                    self.assertIn("error", json.loads(line))
                receipt = json.loads(self.receipt.read_text())
                self.assertEqual(receipt["observations"], [])
                self.assertLess(self.receipt.stat().st_size, 2048)

    def test_same_input_output_and_symlink_or_hardlink_are_rejected(self):
        original = self.probe.read_bytes()
        result = hv.main(["--probe", str(self.probe), "--receipt", str(self.probe)])
        self.assertEqual(result, 2)
        self.assertEqual(self.probe.read_bytes(), original)
        other = self.root / "other.json"
        other.write_text("keep")
        try:
            self.receipt.symlink_to(other)
        except OSError:
            self.skipTest("symlinks unavailable")
        with self.assertRaises(hv.ProbeValidationError):
            hv._atomic_receipt(self.receipt, {})
        self.assertEqual(other.read_text(), "keep")
        with self.assertRaises(hv.ProbeValidationError):
            hv._read_challenge(self.receipt)
        self.receipt.unlink()
        os.link(other, self.receipt)
        with self.assertRaises(hv.ProbeValidationError):
            hv._atomic_receipt(self.receipt, {})
        self.assertEqual(other.read_text(), "keep")


if __name__ == "__main__":
    unittest.main()
