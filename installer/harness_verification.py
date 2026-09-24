"""Bounded, local evidence probe for legacy MCP clients (stdlib only).

Public integration: create_probe(binding, now=None) and
validate_probe_receipt(challenge, receipt, now=None).  The caller stores the
original challenge, compares its binding with the CURRENT enrolled configuration,
and consumes its probeId when accepting a receipt.  This module never reads a
client configuration, launches a client, or obtains credentials.

Run the fixture in an explicitly selected, isolated target Harness session::

    python -m installer.harness_verification --probe CHALLENGE.json --receipt RECEIPT.json

Only --probe is read.  --receipt is an Operator-selected local output, replaced
atomically.  Stdout contains only newline-delimited JSON-RPC.  The fixture speaks
explicitly supported legacy revisions, never modern discovery/subscriptions.

Receipts are bounded local observations, not signatures against a malicious local
principal.  Strict validation prevents accepting a claimed support boolean,
missing/reordered evidence, or another challenge's receipt; an actor who can
fabricate the entire local receipt is inside the project's trusted-OS-user
boundary.  Attestations are explicitly weaker than protocol observations.

Accepted evidence is normalized into a version identity (client kind, observed
MCP client identity, negotiated protocol revision, enrolled configuration
fingerprint) plus the times it was observed and recorded.  It carries no expiry:
evidence stops applying when that identity changes, and it is re-observed when a
client or its configuration changes or when a new peer MCP enters the bridge.

Scope boundary: this module probes the *client Harness* capability only - whether
a Harness observes tools/list_changed, re-fetches its catalog, and can reach a
tool discovered after startup.  It deliberately does NOT measure any business
MCP's tool count, tool-definition volume, or the model-context/token cost of
exposing that catalog, which are separate per-MCP exposure observations taken by
the host from an observed catalog.  A negative refresh verdict here therefore
says nothing about any business MCP's cost or catalog size, and a large business
catalog says nothing about Harness refresh capability.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import queue
import re
import secrets
import stat
import sys
import tempfile
import threading
import time
from typing import Any, Callable

PROBE_VERSION = 1
PROBE_KIND = "win-wsl-mcp-harness-probe"
RECEIPT_KIND = "win-wsl-mcp-harness-receipt"
LEGACY_PROTOCOL_VERSIONS = (
    "2024-11-05", "2025-03-26", "2025-06-18", "2025-11-25",
)
MAX_CHALLENGE_BYTES = 16 * 1024
MAX_RECEIPT_BYTES = 32 * 1024
MAX_MESSAGE_BYTES = 64 * 1024
MAX_TOTAL_INPUT_BYTES = 1024 * 1024
MAX_MESSAGES = 128
MAX_RUNTIME_SECONDS = 300
MAX_VERSION_IDENTITY_BYTES = 4 * 1024
PROBE_TTL_SECONDS = 15 * 60
#: Bounds how long a prepared challenge may be consumed.  It is a property of
#: the challenge, never of the recorded evidence: recorded evidence carries a
#: version fingerprint and timestamps instead of a validity window, so it stops
#: applying on an identity change rather than on a clock.
VERIFICATION_TTL_SECONDS = 24 * 60 * 60

_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:@+-]{0,127}\Z")
_HASH = re.compile(r"[0-9a-f]{64}\Z")
_PROBE_ID = re.compile(r"[0-9a-f]{32}\Z")
_EVIDENCE_REF = re.compile(
    r"(?:session|artifact|observation):[A-Za-z0-9][A-Za-z0-9._:-]{0,190}\Z"
)
_INSTRUCTIONS = (
    "Use this fixture only in an isolated session of the bound target Harness. "
    "Let the Harness initialize and discover the initial bootstrap tool. Invoke "
    "that tool once. Let the Harness process tools/list_changed and refresh its "
    "catalog; invoke the newly discovered canary using the proof required by its "
    "schema. Do not read the challenge or receipt into the model to supply a "
    "canary name/proof, manually refetch with a different client, or infer model "
    "visibility from protocol traffic. Record any model-context evidence "
    "separately as a challenge-bound Agent/human attestation. This fixture "
    "measures Harness capability only, never a business MCP's catalog size or "
    "token cost. Stop after the canary succeeds and remove this temporary "
    "fixture registration."
)
_EVENT_TYPES = (
    "initialized", "initial_catalog", "bootstrap_call", "list_changed",
    "refreshed_catalog", "canary_call_succeeded",
)
_STOP_REASONS = {
    "running", "eof", "timeout", "message-limit", "input-limit",
    "invalid-input", "output-error", "expired",
}
# Stop reasons that justify a negative catalog-refresh verdict: the fixture kept
# an observable window open after emitting the notification, and the Harness
# never asked for a post-notification catalog.  Excluded, so "not tested" can
# never be recorded as "does not support":
#   eof                 the Harness ended the session on its own schedule, so the
#                       window after the notification is unknown (and a catalog
#                       request the Harness pipelined before the notification is
#                       indistinguishable from no request at all)
#   expired             the challenge ran out of lifetime while handling input
#   input-limit, invalid-input, output-error
#                       fixture-side failures that say nothing about the Harness
_NEGATIVE_VERDICT_STOP_REASONS = frozenset({"timeout", "message-limit"})


class ProbeValidationError(ValueError):
    """The bounded probe contract or receipt evidence was not satisfied."""


def _refresh_verdict(receipt: dict, supported: bool) -> tuple[str, str]:
    """Derive the catalog-refresh observation label and capability value.

    Only an observed ``notifications/tools/list_changed`` followed by *no*
    post-notification catalog and a closed observation window that kept running
    (see ``_NEGATIVE_VERDICT_STOP_REASONS``) is a negative verdict.  Everything
    else stays ``unknown`` so recorded evidence never turns "not yet tested" into
    "does not support".  This observes Harness capability only: it never measures
    a business MCP's tool count, tool-definition volume, or model-context token
    cost, which are separate observations taken by the host from an observed
    catalog.
    """
    observations = receipt["observations"]
    notified = any(item.get("type") == "list_changed" for item in observations)
    refreshed = any(item.get("type") == "refreshed_catalog" for item in observations)
    if supported:
        return "refresh-observed", "supported"
    if notified and refreshed:
        return "refreshed-without-canary-completion", "unknown"
    if notified and receipt["stopReason"] in _NEGATIVE_VERDICT_STOP_REASONS:
        return "notification-without-refresh", "unsupported"
    return "no-verdict-yet", "unknown"


def _fail(message: str) -> None:
    raise ProbeValidationError(message)


def _json_bytes(value: Any, limit: int) -> bytes:
    try:
        encoded = json.dumps(
            value, sort_keys=True, separators=(",", ":"), ensure_ascii=True,
            allow_nan=False,
        ).encode("utf-8")
    except (ValueError, TypeError, RecursionError):
        _fail("probe data must be bounded finite JSON")
    if len(encoded) > limit:
        _fail("probe data exceeds byte limit")
    return encoded


def _digest(value: Any, limit: int = MAX_RECEIPT_BYTES) -> str:
    return hashlib.sha256(_json_bytes(value, limit)).hexdigest()


def version_fingerprint(identity: dict[str, Any]) -> str:
    """SHA-256 over one canonical version identity (public helper).

    The fingerprint pins *what* was observed - client kind, the MCP client
    identity seen on the wire, the negotiated protocol revision, the enrolled
    configuration content, and any declared client product version - and it
    replaces a validity window: recorded evidence applies while this identity
    still matches, and is re-observed when the client, the enrolled
    configuration, or the peer server set changes.  A caller that adds
    components must recompute the fingerprint with this same function.
    """
    if not isinstance(identity, dict) or not identity:
        _fail("version identity must be a non-empty object")
    return _digest(identity, MAX_VERSION_IDENTITY_BYTES)


def _exact(value: Any, required: set[str], optional: set[str] | None = None) -> dict:
    if not isinstance(value, dict) or not all(isinstance(k, str) for k in value):
        _fail("expected an object with explicit probe fields")
    if not required.issubset(value) or set(value) - required - (optional or set()):
        _fail("missing or unexpected probe fields")
    return value


def _number(value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        _fail("probe time must be a finite number")
    try:
        number = float(value)
    except (ValueError, OverflowError):
        _fail("probe time must be a finite number")
    if not math.isfinite(number) or number < 0:
        _fail("probe time must be a finite nonnegative number")
    return number


def _now(value: float | None) -> float:
    return _number(time.time() if value is None else value)


def _text(value: Any, pattern: re.Pattern = _ID) -> str:
    if not isinstance(value, str) or not pattern.fullmatch(value):
        _fail("invalid bounded probe identifier")
    return value


def _binding(value: Any) -> dict:
    value = _exact(value, {"environmentId", "clientKind", "configFingerprint"})
    return {
        "environmentId": _text(value["environmentId"]),
        "clientKind": _text(value["clientKind"]),
        "configFingerprint": _text(value["configFingerprint"], _HASH),
    }


def _tool_names(probe_id: str, nonce: str) -> dict:
    return {
        "bootstrap": "harness_probe_begin_" + probe_id,
        "canary": "harness_probe_canary_" + nonce[:32],
    }


def create_probe(binding: dict, now: float | None = None) -> dict:
    """Project a redacted environment binding into a fresh, expiring challenge.

    Accepts an environment summary: fileSha256 is an alias for
    configFingerprint.  No other summary fields are copied.  The fingerprint is
    a caller-computed SHA-256 of the exact effective enrolled configuration.
    """
    if not isinstance(binding, dict):
        _fail("environment binding must be an object")
    fingerprint = binding.get("configFingerprint", binding.get("fileSha256"))
    if ("configFingerprint" in binding and "fileSha256" in binding
            and binding["fileSha256"] != fingerprint):
        _fail("configuration fingerprint aliases disagree")
    normalized = _binding({
        "environmentId": binding.get("environmentId"),
        "clientKind": binding.get("clientKind"),
        "configFingerprint": fingerprint,
    })
    stamp = _now(now)
    probe_id, nonce = secrets.token_hex(16), secrets.token_hex(32)
    return {
        "schemaVersion": PROBE_VERSION,
        "kind": PROBE_KIND,
        "probeId": probe_id,
        "nonce": nonce,
        "binding": normalized,
        "issuedAt": stamp,
        "expiresAt": stamp + PROBE_TTL_SECONDS,
        "verificationValidUntil": stamp + VERIFICATION_TTL_SECONDS,
        "tools": _tool_names(probe_id, nonce),
        "instructions": _INSTRUCTIONS,
    }


def _validate_challenge(challenge: Any, now: float) -> dict:
    _json_bytes(challenge, MAX_CHALLENGE_BYTES)
    challenge = _exact(challenge, {
        "schemaVersion", "kind", "probeId", "nonce", "binding", "issuedAt",
        "expiresAt", "verificationValidUntil", "tools", "instructions",
    })
    if type(challenge["schemaVersion"]) is not int or challenge["schemaVersion"] != PROBE_VERSION:
        _fail("unsupported probe schema version")
    if challenge["kind"] != PROBE_KIND:
        _fail("unsupported probe kind")
    _binding(challenge["binding"])
    probe_id = _text(challenge["probeId"], _PROBE_ID)
    nonce = _text(challenge["nonce"], _HASH)
    if challenge["tools"] != _tool_names(probe_id, nonce):
        _fail("probe tool names do not match challenge identity")
    if challenge["instructions"] != _INSTRUCTIONS:
        _fail("probe instructions do not match this version")
    issued = _number(challenge["issuedAt"])
    expires = _number(challenge["expiresAt"])
    valid_until = _number(challenge["verificationValidUntil"])
    if not 0 < expires - issued <= PROBE_TTL_SECONDS:
        _fail("invalid probe completion lifetime")
    if not expires <= valid_until <= issued + VERIFICATION_TTL_SECONDS:
        _fail("invalid verification lifetime")
    if not issued <= now < expires:
        _fail("probe is not yet valid or has expired")
    return challenge


def _client_info(value: Any, *, project: bool = False) -> dict:
    if project:
        if not isinstance(value, dict):
            _fail("clientInfo must contain name and version")
        value = {"name": value.get("name"), "version": value.get("version")}
    value = _exact(value, {"name", "version"})
    for item in value.values():
        if (not isinstance(item, str) or not 1 <= len(item) <= 128
                or not all(32 <= ord(c) < 127 for c in item)):
            _fail("clientInfo name and version must be bounded printable metadata")
    return dict(value)


def _bootstrap_schema(challenge: dict) -> dict:
    return {
        "name": challenge["tools"]["bootstrap"],
        "description": "Begin the harmless catalog-refresh probe once; then use the newly discovered canary.",
        "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False},
    }


def _canary_schema(challenge: dict, proof: str) -> dict:
    return {
        "name": challenge["tools"]["canary"],
        "description": "Complete the harmless refresh probe using the proof from this newly discovered schema.",
        "inputSchema": {
            "type": "object", "properties": {
                "proof": {"type": "string", "const": proof,
                          "description": "Use exactly this schema's const value."},
            },
            "required": ["proof"], "additionalProperties": False,
        },
    }


def _proof_hash(proof: str) -> str:
    return hashlib.sha256(proof.encode("ascii")).hexdigest()


def _validate_events(challenge: dict, receipt: dict) -> tuple[dict | None, str | None]:
    events = receipt["observations"]
    if not isinstance(events, list) or len(events) > len(_EVENT_TYPES):
        _fail("invalid observation count")
    started = _number(receipt["startedAt"])
    updated = _number(receipt["updatedAt"])
    previous = started
    client_info, protocol = None, None
    names = challenge["tools"]
    for index, event in enumerate(events):
        fields = {"sequence", "at", "type"}
        extras = (
            {"clientInfo", "protocolVersion"},
            {"tools", "schemaSha256"},
            {"tool"},
            set(),
            {"tools", "removedTools", "addedTools", "schemaSha256", "proofSha256", "requestReceivedAt"},
            {"tool", "proofSha256", "catalogSequence", "requestReceivedAt"},
        )[index]
        _exact(event, fields | extras)
        if type(event["sequence"]) is not int or event["sequence"] != index + 1:
            _fail("probe observations must be sequential")
        if event["type"] != _EVENT_TYPES[index]:
            _fail("probe observation sequence is incomplete or reordered")
        stamp = _number(event["at"])
        if not previous <= stamp <= updated:
            _fail("probe observation timestamps are out of order")
        previous = stamp
        if index == 0:
            client_info = _client_info(event["clientInfo"])
            protocol = event["protocolVersion"]
            if protocol not in LEGACY_PROTOCOL_VERSIONS:
                _fail("receipt does not prove a supported legacy protocol")
        elif index == 1:
            if (event["tools"] != [names["bootstrap"]]
                    or event["schemaSha256"] != _digest(_bootstrap_schema(challenge))):
                _fail("initial catalog does not match challenge")
        elif index == 2:
            if event["tool"] != names["bootstrap"]:
                _fail("bootstrap observation does not match challenge")
        elif index == 4:
            if (event["tools"] != [names["canary"]]
                    or event["removedTools"] != [names["bootstrap"]]
                    or event["addedTools"] != [names["canary"]]):
                _fail("refreshed catalog must remove bootstrap and add canary")
            _text(event["schemaSha256"], _HASH)
            _text(event["proofSha256"], _HASH)
            if event["schemaSha256"] == events[1]["schemaSha256"]:
                _fail("refreshed schema must differ from initial schema")
            if not events[3]["at"] <= _number(event["requestReceivedAt"]) <= stamp:
                _fail("catalog refetch request predates the emitted notification")
        elif index == 5:
            if not events[4]["at"] <= _number(event["requestReceivedAt"]) <= stamp:
                _fail("canary request predates the emitted refreshed catalog")
            if (event["tool"] != names["canary"]
                    or type(event["catalogSequence"]) is not int
                    or event["catalogSequence"] != 5
                    or event["proofSha256"] != events[4]["proofSha256"]):
                _fail("canary call must use proof from the post-notification catalog")
    complete = len(events) == len(_EVENT_TYPES)
    if receipt["status"] != ("complete" if complete else "incomplete"):
        _fail("receipt status contradicts its observations")
    return client_info, protocol


def _protocol_evidence_digest(receipt: dict) -> str:
    """Bind attestation to this run; a later EOF does not change observations."""
    return _digest({
        "challengeSha256": receipt["challengeSha256"],
        "startedAt": receipt["startedAt"],
        "observations": receipt["observations"],
    })


def _validate_attestation(
    kind: str, value: Any, challenge: dict, receipt: dict,
    client_info: dict | None, now: float,
) -> tuple[str, dict]:
    value = _exact(value, {
        "schemaVersion", "source", "actorKind", "probeId", "nonce", "binding",
        "clientInfo", "observedAt", "modelId", "providerId", "observation",
        "evidenceRef", "evidenceSha256", "canaryCallSequence", "protocolEvidenceSha256",
    })
    if (type(value["schemaVersion"]) is not int or value["schemaVersion"] != PROBE_VERSION
            or value["source"] != "attested" or value["actorKind"] not in ("agent", "human")):
        _fail("invalid model-context attestation source")
    for key in ("probeId", "nonce", "binding"):
        if value[key] != challenge[key]:
            _fail("model-context attestation is bound to a different probe")
    if client_info is None or _client_info(value["clientInfo"]) != client_info:
        _fail("model-context attestation client differs from observed client")
    if (receipt["status"] != "complete" or type(value["canaryCallSequence"]) is not int
            or value["canaryCallSequence"] != 6):
        _fail("model-context attestation requires a completed canary probe")
    if value["protocolEvidenceSha256"] != _protocol_evidence_digest(receipt):
        _fail("model-context attestation belongs to a different fixture run")
    observed = _number(value["observedAt"])
    if not receipt["observations"][-1]["at"] <= observed <= now:
        _fail("model-context attestation is stale or future-dated")
    _text(value["modelId"])
    _text(value["providerId"])
    _text(value["evidenceRef"], _EVIDENCE_REF)
    _text(value["evidenceSha256"], _HASH)
    observations = {
        "modelExposure": {
            "canary-schema-visible-to-model": "supported",
            "canary-schema-not-visible-to-model": "unsupported",
        },
        "nativeToolSearch": {
            "native-search-loaded-canary-schema": "supported",
            "native-search-disabled-in-effective-session": "unsupported",
        },
    }
    observation = value["observation"]
    if not isinstance(observation, str) or observation not in observations[kind]:
        _fail("unsupported model-context evidence observation")
    return observations[kind][observation], dict(value)


def validate_probe_receipt(
    challenge: dict, receipt: dict, now: float | None = None,
) -> dict:
    """Derive capabilities from observations, never from caller-supplied flags.

    This validates one saved challenge; the parent must also check its binding
    against CURRENT enrollment/configuration and reject already-consumed probeIds.
    Optional attestations reference separately retained bounded model-context
    evidence.  References are opaque metadata, never paths that this module opens.
    """
    stamp = _now(now)
    challenge = _validate_challenge(challenge, stamp)
    _json_bytes(receipt, MAX_RECEIPT_BYTES)
    receipt = _exact(receipt, {
        "schemaVersion", "kind", "probeId", "nonce", "binding", "challengeSha256",
        "startedAt", "updatedAt", "status", "stopReason", "observations",
    }, {"attestations"})
    if (type(receipt["schemaVersion"]) is not int or receipt["schemaVersion"] != PROBE_VERSION
            or receipt["kind"] != RECEIPT_KIND):
        _fail("unsupported receipt version or kind")
    for key in ("probeId", "nonce", "binding"):
        if receipt[key] != challenge[key]:
            _fail("receipt is bound to a different environment or probe")
    if receipt["challengeSha256"] != _digest(challenge, MAX_CHALLENGE_BYTES):
        _fail("receipt challenge digest mismatch")
    started, updated = _number(receipt["startedAt"]), _number(receipt["updatedAt"])
    if not challenge["issuedAt"] <= started <= updated <= stamp:
        _fail("receipt timestamps are stale or future-dated")
    if updated - started > MAX_RUNTIME_SECONDS:
        _fail("receipt exceeds probe runtime budget")
    if not isinstance(receipt["stopReason"], str) or receipt["stopReason"] not in _STOP_REASONS:
        _fail("unknown probe stop reason")
    client_info, protocol = _validate_events(challenge, receipt)
    supported = receipt["status"] == "complete"
    refresh_observation, refresh_capability = _refresh_verdict(receipt, supported)
    capabilities = {
        "toolsListChanged": refresh_capability,
        "modelExposure": "unknown", "nativeToolSearch": "unknown",
    }
    evidence: dict[str, Any] = {
        "protocolRefresh": {
            "source": "protocol-observed",
            "observationCount": len(receipt["observations"]),
            "catalogRemoval": "server-catalog-observed" if supported else "unknown",
            "modelSchemaVisibility": "not-proven",
            "refreshObservation": refresh_observation,
            # Why this run ended, so a Bridge Agent can tell an observed negative
            # apart from an untested or still-running capability.
            "receiptStatus": receipt["status"],
            "stopReason": receipt["stopReason"],
        },
        "modelExposure": {"source": "unknown"},
        "nativeToolSearch": {"source": "unknown"},
    }
    attestations = _exact(receipt.get("attestations", {}), set(), {"modelExposure", "nativeToolSearch"})
    for kind, value in attestations.items():
        capabilities[kind], evidence[kind] = _validate_attestation(
            kind, value, challenge, receipt, client_info, stamp,
        )
    identity = {
        "clientKind": challenge["binding"]["clientKind"],
        # A bounded negative may end before any initialize was observed, so the
        # client identity is optional and never invented.
        "observedClient": dict(client_info) if isinstance(client_info, dict) else None,
        "protocolVersion": protocol,
        "configFingerprint": challenge["binding"]["configFingerprint"],
    }
    return {
        "schemaVersion": PROBE_VERSION, **challenge["binding"],
        "probeId": challenge["probeId"], "protocolVersion": protocol,
        "observedClientInfo": client_info,
        # Evidence identity, not a validity window: observedAt is when the
        # observation window ended, recordedAt when this host accepted it.
        "observedAt": updated, "recordedAt": stamp,
        "versionIdentity": identity,
        "versionFingerprint": version_fingerprint(identity),
        "receiptSha256": _digest(receipt),
        "protocolEvidenceSha256": _protocol_evidence_digest(receipt),
        "capabilities": capabilities, "evidence": evidence,
    }


class _Fixture:
    """Sequential state machine; observations follow successfully emitted bytes."""

    def __init__(
        self, challenge: dict, emit: Callable[[dict], float | None],
        persist: Callable[[dict], None], clock: Callable[[], float] = time.time,
    ):
        self.clock, self.emit, self.persist = clock, emit, persist
        stamp = _now(clock())
        self.challenge = _validate_challenge(challenge, stamp)
        self.receipt = {
            "schemaVersion": PROBE_VERSION, "kind": RECEIPT_KIND,
            "probeId": challenge["probeId"], "nonce": challenge["nonce"],
            "binding": dict(challenge["binding"]),
            "challengeSha256": _digest(challenge, MAX_CHALLENGE_BYTES),
            "startedAt": stamp, "updatedAt": stamp, "status": "incomplete",
            "stopReason": "running", "observations": [],
        }
        self.initialized = False
        self.ready = False
        self.initial_listed = False
        self.changed = False
        self.refetched = False
        self.proof: str | None = None
        self.last_sent_at = stamp
        self.notification_at: float | None = None
        self.catalog_at: float | None = None
        self.persist(self.receipt)

    def _send(self, message: dict) -> None:
        sent_at = self.emit(message)
        self.last_sent_at = _now(self.clock() if sent_at is None else sent_at)

    def _observe(self, event_type: str, **details: Any) -> None:
        event = {
            "sequence": len(self.receipt["observations"]) + 1,
            "at": self.last_sent_at, "type": event_type, **details,
        }
        self.receipt["observations"].append(event)
        self.receipt["updatedAt"] = event["at"]
        if event_type == "canary_call_succeeded":
            self.receipt["status"] = "complete"
        self.persist(self.receipt)

    def finish(self, reason: str) -> None:
        self.receipt["stopReason"] = reason
        # A later EOF/timeout is not another observation and must not extend the
        # successfully completed evidence's runtime or make it look newer.
        self.persist(self.receipt)

    def _reply(self, request_id: Any, result: dict) -> None:
        self._send({"jsonrpc": "2.0", "id": request_id, "result": result})

    def _error(self, request_id: Any, code: int, message: str) -> None:
        self._send({"jsonrpc": "2.0", "id": request_id,
                    "error": {"code": code, "message": message}})

    def handle(self, message: Any, received_at: float | None = None) -> None:
        stamp = _now(self.clock())
        _validate_challenge(self.challenge, stamp)
        received_at = stamp if received_at is None else _number(received_at)
        if not isinstance(message, dict):
            self._error(None, -32600, "Expected one JSON-RPC request")
            return
        request_id = message.get("id")
        if (message.get("jsonrpc") != "2.0" or not isinstance(message.get("method"), str)
                or ("id" in message and (isinstance(request_id, bool)
                    or not isinstance(request_id, (int, str))
                    or (isinstance(request_id, str) and len(request_id) > 128)))):
            self._error(None, -32600, "Invalid JSON-RPC request")
            return
        method = message["method"]
        if "id" not in message:
            if method == "notifications/initialized" and self.initialized:
                self.ready = True
            return
        if method in {"server/discover", "subscriptions/listen"}:
            self._error(request_id, -32601, "This probe supports legacy MCP only")
            return
        if method == "ping":
            self._reply(request_id, {})
            return
        params = message.get("params", {})
        if not isinstance(params, dict):
            self._error(request_id, -32602, "Expected object params")
            return
        if method == "initialize":
            if self.initialized:
                self._error(request_id, -32600, "Probe already initialized")
                return
            protocol = params.get("protocolVersion")
            if protocol not in LEGACY_PROTOCOL_VERSIONS:
                self._error(request_id, -32602, "Unsupported probe protocol version; use a supported legacy revision")
                return
            try:
                client_info = _client_info(params.get("clientInfo"), project=True)
            except ProbeValidationError:
                self._error(request_id, -32602, "Expected bounded clientInfo name and version")
                return
            if not isinstance(params.get("capabilities"), dict):
                self._error(request_id, -32602, "Expected client capabilities object")
                return
            self._reply(request_id, {
                "protocolVersion": protocol,
                "capabilities": {"tools": {"listChanged": True}},
                "serverInfo": {"name": "bridge-harness-verification", "version": str(PROBE_VERSION)},
                "instructions": _INSTRUCTIONS,
            })
            self.initialized = True
            self._observe("initialized", clientInfo=client_info, protocolVersion=protocol)
            return
        if not self.ready:
            self._error(request_id, -32000, "Complete legacy initialization before probing")
            return
        if method == "tools/list":
            if set(params) - {"_meta"}:
                self._error(request_id, -32602, "This probe has one catalog page and no list parameters")
                return
            if not self.changed:
                schema = _bootstrap_schema(self.challenge)
                self._reply(request_id, {"tools": [schema]})
                if not self.initial_listed:
                    self.initial_listed = True
                    self._observe("initial_catalog", tools=[schema["name"]], schemaSha256=_digest(schema))
            else:
                schema = _canary_schema(self.challenge, self.proof)
                self._reply(request_id, {"tools": [schema]})
                if not self.refetched and self.notification_at is not None and received_at >= self.notification_at:
                    self.refetched = True
                    self.catalog_at = self.last_sent_at
                    self._observe(
                        "refreshed_catalog", tools=[schema["name"]],
                        removedTools=[self.challenge["tools"]["bootstrap"]],
                        addedTools=[schema["name"]], schemaSha256=_digest(schema),
                        proofSha256=_proof_hash(self.proof), requestReceivedAt=received_at,
                    )
            return
        if method != "tools/call":
            self._error(request_id, -32601, "Method not supported by harmless probe")
            return
        tool, arguments = params.get("name"), params.get("arguments", {})
        if set(params) - {"name", "arguments", "_meta"} or not isinstance(arguments, dict):
            self._error(request_id, -32602, "Invalid probe tool arguments")
            return
        if not self.changed and tool == self.challenge["tools"]["bootstrap"]:
            if not self.initial_listed or arguments:
                self._error(request_id, -32602, "Discover the initial catalog and call bootstrap with empty arguments")
                return
            self.proof = secrets.token_hex(32)
            self.changed = True
            self._reply(request_id, {"content": [{"type": "text", "text":
                "Catalog changed. Let the Harness refresh it and invoke the newly discovered canary using its schema."}]})
            self._observe("bootstrap_call", tool=tool)
            self._send({"jsonrpc": "2.0", "method": "notifications/tools/list_changed"})
            self.notification_at = self.last_sent_at
            self._observe("list_changed")
            return
        if self.changed and tool == self.challenge["tools"]["canary"]:
            if (not self.refetched or arguments != {"proof": self.proof}
                    or self.catalog_at is None or received_at < self.catalog_at):
                self._error(request_id, -32602, "Canary requires proof from a post-notification catalog refetch")
                return
            self._reply(request_id, {"content": [{"type": "text", "text":
                "Canary succeeded. Protocol refresh observed; model visibility and native search still require separate attestation."}]})
            if self.receipt["status"] != "complete":
                self._observe("canary_call_succeeded", tool=tool,
                              proofSha256=_proof_hash(self.proof), catalogSequence=5,
                              requestReceivedAt=received_at)
            return
        self._error(request_id, -32602, "Tool is absent from the current probe catalog")


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict:
    value: dict = {}
    for key, item in pairs:
        if key in value:
            _fail("duplicate JSON keys are not permitted")
        value[key] = item
    return value


def _parse_json(raw: bytes) -> Any:
    try:
        return json.loads(raw.decode("utf-8"), object_pairs_hook=_reject_duplicate_keys,
                          parse_constant=lambda _: _fail("non-finite JSON is not permitted"))
    except (UnicodeError, ValueError, RecursionError):
        _fail("invalid bounded JSON input")


def _read_challenge(path: Path) -> dict:
    before = path.lstat()
    if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1:
        _fail("probe input must be a single-link regular file")
    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(path, flags)
    with os.fdopen(fd, "rb") as stream:
        after = os.fstat(stream.fileno())
        if (not stat.S_ISREG(after.st_mode) or after.st_nlink != 1
                or (before.st_dev, before.st_ino) != (after.st_dev, after.st_ino)):
            _fail("probe input changed while opening")
        raw = stream.read(MAX_CHALLENGE_BYTES + 1)
    if len(raw) > MAX_CHALLENGE_BYTES:
        _fail("probe input exceeds byte limit")
    return _parse_json(raw)


def _atomic_receipt(path: Path, receipt: dict) -> None:
    data = _json_bytes(receipt, MAX_RECEIPT_BYTES) + b"\n"
    if path.is_symlink():
        _fail("receipt destination cannot be a symlink")
    try:
        existing = path.lstat()
    except FileNotFoundError:
        existing = None
    if existing is not None and (not stat.S_ISREG(existing.st_mode) or existing.st_nlink != 1):
        _fail("receipt destination must be a single-link regular file")
    fd, temporary = tempfile.mkstemp(prefix=".harness-receipt-", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass


class _PipeIO:
    """Bound reads/writes even when a client holds a pipe open without progress.

    Daemon workers use raw descriptors, not buffered sys.stdin/stdout, so a stuck
    peer cannot hold Python's buffered-I/O locks during process shutdown.
    """

    def __init__(self, deadline: float):
        self.deadline = deadline
        self.inbox: queue.Queue = queue.Queue(maxsize=1)
        self.outbox: queue.Queue = queue.Queue(maxsize=1)
        threading.Thread(target=self._read, daemon=True).start()
        threading.Thread(target=self._write, daemon=True).start()

    def _read(self) -> None:
        pending = bytearray()
        try:
            while True:
                block = os.read(sys.stdin.fileno(), 4096)
                received_at = time.time()
                if not block:
                    if pending:
                        self.inbox.put((bytes(pending), received_at))
                    self.inbox.put(None)
                    return
                pending.extend(block)
                while b"\n" in pending:
                    line, _, rest = pending.partition(b"\n")
                    pending = bytearray(rest)
                    self.inbox.put((bytes(line) + b"\n", received_at))
                if len(pending) > MAX_MESSAGE_BYTES:
                    self.inbox.put(ProbeValidationError("message exceeds byte limit"))
                    return
        except OSError:
            self.inbox.put(ProbeValidationError("probe stdin read failed"))

    def _write(self) -> None:
        while True:
            data, acknowledgement = self.outbox.get()
            try:
                view = memoryview(data)
                while view:
                    count = os.write(sys.stdout.fileno(), view)
                    if count <= 0:
                        raise OSError("zero-byte pipe write")
                    view = view[count:]
                acknowledgement.put((None, time.time()))
            except OSError:
                acknowledgement.put((ProbeValidationError("probe stdout write failed"), None))

    def remaining(self) -> float:
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("probe runtime budget exhausted")
        return remaining

    def receive(self) -> tuple[bytes, float] | None:
        try:
            result = self.inbox.get(timeout=self.remaining())
        except queue.Empty:
            raise TimeoutError("probe input deadline reached") from None
        if isinstance(result, Exception):
            raise result
        return result

    def send(self, message: dict) -> float:
        encoded = _json_bytes(message, MAX_MESSAGE_BYTES) + b"\n"
        acknowledgement: queue.Queue = queue.Queue(maxsize=1)
        try:
            self.outbox.put((encoded, acknowledgement), timeout=self.remaining())
            error, sent_at = acknowledgement.get(timeout=self.remaining())
        except (queue.Empty, queue.Full):
            raise TimeoutError("probe output deadline reached") from None
        if error is not None:
            raise error
        return sent_at


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the bounded, harmless legacy MCP verification fixture")
    parser.add_argument("--probe", required=True, type=Path)
    parser.add_argument("--receipt", required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        if args.probe.resolve() == args.receipt.resolve():
            _fail("probe input and receipt output must be different files")
        challenge = _read_challenge(args.probe)
        stamp = _now(None)
        _validate_challenge(challenge, stamp)
        deadline = time.monotonic() + min(MAX_RUNTIME_SECONDS, challenge["expiresAt"] - stamp)
        channel = _PipeIO(deadline)
        fixture = _Fixture(challenge, channel.send, lambda value: _atomic_receipt(args.receipt, value))
        total = count = 0
        while True:
            try:
                incoming = channel.receive()
            except TimeoutError:
                fixture.finish("timeout")
                return 0 if fixture.receipt["status"] == "complete" else 2
            except ProbeValidationError:
                fixture.finish("input-limit")
                return 2
            if incoming is None:
                fixture.finish("eof")
                return 0
            raw, received_at = incoming
            total += len(raw)
            count += 1
            if len(raw) > MAX_MESSAGE_BYTES or total > MAX_TOTAL_INPUT_BYTES:
                fixture.finish("input-limit")
                return 2
            if count > MAX_MESSAGES:
                fixture.finish("message-limit")
                return 2
            try:
                message = _parse_json(raw)
            except ProbeValidationError:
                channel.send({"jsonrpc": "2.0", "id": None,
                              "error": {"code": -32700, "message": "Invalid JSON input"}})
                fixture.finish("invalid-input")
                return 2
            try:
                fixture.handle(message, received_at)
            except TimeoutError:
                fixture.finish("timeout")
                return 2
            except ProbeValidationError:
                fixture.finish("expired" if time.time() >= challenge["expiresAt"] else "output-error")
                return 2
    except ProbeValidationError as error:
        print("Harness probe rejected: " + str(error), file=sys.stderr)
        return 2
    except OSError:
        # Never print a private local path or raw I/O exception into a receipt.
        print("Harness probe local input/output failed", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
