"""P8 privacy-bounded cross-host per-business-call correlation helpers.

Pure, standard-library-only helpers for the BridgeNode *data observers*.  The
goal is that one business JSON-RPC request/response can be correlated across
the two bridge nodes with an **opaque correlation alias** — never a raw stream
id, RPC id, path, header, argument, or result.

Design contract
---------------
* Observer only: these helpers never mutate, replay, or block the MCP data
  plane.  They consume the exact bytes that already cross the peer link; raw
  dedicated bytes stay byte-transparent.
* Metadata only: nothing here persists payloads.  Journal rows derived from
  these observations carry only the opaque alias plus bounded metadata
  (message kind, flow label, message byte count, occurrence).  A parsed
  JSON-RPC message is discarded immediately after classification.
* Bounded passive parsing: newline-delimited JSON-RPC lines are recognized
  incrementally across arbitrary transport fragments.  Lines larger than the
  configured bound are dropped from correlation (counted, never buffered), and
  an oversized line can never stall the transport.  Parsing only *classifies*
  (request/response/notification/other) — it never infers or executes
  instructions.
* Shared identity and canonical origin: the alias is derived from the logical
  stream id (identical on both nodes), the *canonical sender side* (derived on
  each node from its own side plus the flow label, so both nodes agree), the
  typed request id, and an occurrence counter.  Requests travelling in the two
  opposite directions of one stream therefore never collide even with the same
  id and occurrence.
* Id reuse is paired through *outstanding* requests, not "last seen": a
  response consumes the oldest un-answered request with its id, so a duplicate
  or unobserved response is flagged and never falsely paired.  If two
  same-direction requests with the same id are outstanding at once (protocol
  anomaly) the response is flagged ambiguous rather than guessed.
* Fail closed on observer loss: any loss of framing or classification fidelity
  (oversized line, unparseable bytes) degrades the affected direction to
  unknown — the observer cannot know whether the lost bytes were an id-reusing
  request, so later aliases/pairings in that direction are suppressed rather
  than risking a false pairing.  Both nodes see the same bytes, so both degrade
  identically.
* Bound on memory: per stream only the incremental line buffers (bounded),
  per-direction occurrence counters (bounded distinct ids), and outstanding
  request queues (bounded by the same distinct-id bound) are retained.

Nothing in this module touches credentials, business processes, sockets, or
SQLite; it is all deterministic byte classification.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Optional, Union

#: Version tag mixed into every alias so a format change cannot silently alias
#: observations from two different derivations.
EVIDENCE_ALGORITHM = "win-wsl-mcp-evidence-v1"

#: Flow labels are the observer's own peer-link directions (the same labels the
#: trace capture pipeline already uses).
FLOW_INBOUND = "inbound"  # bytes received from the peer over the link
FLOW_OUTBOUND = "outbound"  # bytes sent to the peer over the link
ALL_FLOWS = (FLOW_INBOUND, FLOW_OUTBOUND)

#: Canonical bridge host sides.
SIDE_WIN = "win"
SIDE_WSL = "wsl"
ALL_SIDES = (SIDE_WIN, SIDE_WSL)

#: Largest JSON-RPC line the passive observer buffers for classification.
#: Bigger lines (and the bytes of an oversized line until its terminator) are
#: dropped from correlation and only counted; the data plane is untouched.
DEFAULT_MAX_MESSAGE_BYTES = 256 * 1024

#: Longest canonical request id token accepted (bounded; longer ids are flagged
#: unknown instead of being buffered/compared).
MAX_REQUEST_ID_CANONICAL_BYTES = 128

#: Distinct request ids tracked per direction before the flow degrades to
#: counting only (bounded memory on long-lived, high-volume streams).
DEFAULT_MAX_TRACKED_REQUEST_IDS = 4096

#: Stable prefix so an opaque alias is recognizable without any structure.
ALIAS_PREFIX = "ev1:"
#: Aliases carry 128 bits of digest (never the raw inputs).
ALIAS_DIGEST_HEX_LENGTH = 32

#: Event kinds emitted to the journal.
KIND_REQUEST = "request"
KIND_RESPONSE = "response"
KIND_OTHER = "other"
KIND_OVERSIZED = "oversized"

#: Loss reasons that put a direction into the unknown (fail-closed) state.
LOST_OVERSIZED_LINE = "oversized-line"
LOST_UNPARSEABLE = "unparseable"
LOST_NOT_AN_OBJECT = "not-an-object"
LOST_UNRECOGNIZED = "unrecognized-envelope"
LOST_RESPONSE_ID_UNREADABLE = "response-id-unreadable"
LOST_CONCURRENT_REUSE = "concurrent-id-reuse-ambiguous"


def canonical_request_id(value: Any) -> Optional[str]:
    """Deterministic, bounded canonical token for one JSON-RPC request id.

    Returns None for non-scalar ids or ids longer than the canonical bound
    (callers flag those as unknown instead of correlating on them).  The token
    is only ever used as an internal map key / alias input; it is never stored
    or exposed.
    """
    if isinstance(value, bool):
        return "b:true" if value else "b:false"
    if isinstance(value, int):
        return f"i:{value}"
    if isinstance(value, float):
        return f"f:{value!r}"
    if isinstance(value, str):
        if len(value.encode("utf-8")) > MAX_REQUEST_ID_CANONICAL_BYTES:
            return None
        return "s:" + value
    return None


def _peer_side(side: str) -> str:
    return SIDE_WSL if side == SIDE_WIN else SIDE_WIN


def origin_side_for(flow: str, side: str) -> str:
    """Canonical sender side for a message observed in ``flow`` on ``side``.

    Both nodes derive the same value for the same wire bytes: a message this
    node sends (outbound) originates here; a message this node receives
    (inbound) originates on the peer.  This origin is part of the alias, so a
    client request and a backend request that share an id and occurrence but
    travel in opposite directions never alias to each other.
    """
    if flow not in ALL_FLOWS:
        raise ValueError(f"unknown flow: {flow!r}")
    if side not in ALL_SIDES:
        raise ValueError(f"unknown side: {side!r}")
    return side if flow == FLOW_OUTBOUND else _peer_side(side)


def _alias_from_canonical(
    *, stream_id: str, origin: str, canonical_id: str, occurrence: int
) -> str:
    if origin not in ALL_SIDES:
        raise ValueError(f"unknown origin: {origin!r}")
    if not isinstance(occurrence, int) or isinstance(occurrence, bool) or occurrence < 1:
        raise ValueError("occurrence must be a positive integer")
    payload = json.dumps(
        {
            "v": EVIDENCE_ALGORITHM,
            "s": stream_id,
            "o": origin,
            "i": canonical_id,
            "n": occurrence,
        },
        ensure_ascii=True,
        separators=(",", ":"),
    )
    digest = hashlib.sha256(payload.encode("ascii")).hexdigest()
    return ALIAS_PREFIX + digest[:ALIAS_DIGEST_HEX_LENGTH]


def correlation_alias(
    *, stream_id: str, origin: str, request_id: Any, occurrence: int
) -> str:
    """Opaque correlation alias shared by both nodes for one request/response.

    Inputs are the shared logical stream id (the stream id both nodes use for
    the same link stream), the canonical sender side (``win``/``wsl``, derived
    identically on both nodes from local side + flow), the typed request id,
    and the occurrence of that request id in its direction on that stream
    (1 for the first request, 2 for a reused id, ...).  Because both nodes
    observe the same ordered bytes and derive the same origin, they compute the
    same digest without exchanging any correlation state.  The output is a
    fixed-length digest string that reveals none of its inputs.
    """
    canonical = canonical_request_id(request_id)
    if canonical is None:
        raise ValueError("cannot derive an alias from a non-scalar/oversized id")
    return _alias_from_canonical(
        stream_id=stream_id,
        origin=origin,
        canonical_id=canonical,
        occurrence=occurrence,
    )


@dataclass(frozen=True)
class CompletedMessage:
    """One complete newline-delimited JSON-RPC line, classified, not executed.

    ``request_id``/``canonical_id`` are the raw typed id (transient only) and
    its bounded canonical token.  ``message_bytes`` counts only the message
    line itself (no terminator); oversized lines never reach this dataclass.
    """

    kind: str
    request_id: Any = None
    canonical_id: Optional[str] = None
    message_bytes: int = 0
    note: str = ""


@dataclass(frozen=True)
class FramingLoss:
    """A message the observer could not classify (bounded, counted, dropped).

    Emitted by :class:`LineFramer` in byte order so the correlator can degrade
    the affected direction *before* processing any later message in the same
    chunk — fail closed instead of risking a false pairing.
    """

    reason: str
    bytes: int = 0


def _classify_line(line: bytes) -> CompletedMessage:
    """Passively classify one JSON-RPC line. Never executes anything."""
    size = len(line)
    try:
        parsed = json.loads(line.decode("utf-8", errors="strict"))
    except (ValueError, UnicodeDecodeError):
        return CompletedMessage(kind=KIND_OTHER, message_bytes=size, note="unparseable")
    if not isinstance(parsed, dict):
        return CompletedMessage(kind=KIND_OTHER, message_bytes=size, note="not-an-object")
    method = parsed.get("method")
    has_result = "result" in parsed
    has_error = "error" in parsed
    request_id: Any = None
    canonical: Optional[str] = None
    if "id" in parsed:
        request_id = parsed["id"]
        canonical = canonical_request_id(request_id)
    if isinstance(method, str) and method.startswith("notifications/"):
        return CompletedMessage(
            kind="notification", message_bytes=size, note="notification"
        )
    if has_result or has_error:
        if "id" not in parsed or canonical is None:
            return CompletedMessage(
                kind=KIND_OTHER,
                request_id=request_id,
                canonical_id=canonical,
                message_bytes=size,
                note="response-without-scalar-id",
            )
        return CompletedMessage(
            kind=KIND_RESPONSE,
            request_id=request_id,
            canonical_id=canonical,
            message_bytes=size,
            note=("error" if has_error else ""),
        )
    if isinstance(method, str) and canonical is not None:
        return CompletedMessage(
            kind=KIND_REQUEST,
            request_id=request_id,
            canonical_id=canonical,
            message_bytes=size,
            note=method,  # method class hint, discarded by callers before journal
        )
    return CompletedMessage(
        kind=KIND_OTHER,
        request_id=request_id,
        canonical_id=canonical,
        message_bytes=size,
        note="unrecognized-envelope",
    )


class LineFramer:
    """Bounded incremental framer for one direction of one stream.

    Splits the raw byte stream on newline boundaries (MCP protocol-clean
    stdout/stdin is newline-delimited JSON), accumulating a line across
    arbitrary transport fragments.  A line that exceeds the byte bound enters
    an overflow state: its bytes are dropped (counted) until the terminator,
    and nothing is buffered or parsed for it, so an oversized message can never
    block the transport or unboundedly grow memory.  Completed messages and
    framing losses are returned in byte order (losses as :class:`FramingLoss`).
    """

    def __init__(self, max_message_bytes: int = DEFAULT_MAX_MESSAGE_BYTES):
        self.max_message_bytes = max(1024, int(max_message_bytes))
        self._buffer = bytearray()
        self._overflow = False
        self._overflow_line_bytes = 0
        #: explicit counts surfaced to tests/diagnostics.
        self.oversized_messages = 0
        self.oversized_bytes = 0
        self.partial_bytes = 0

    def feed(self, data: bytes) -> list[Union[CompletedMessage, FramingLoss]]:
        """Feed raw bytes; return completed messages/losses in byte order."""
        if not data:
            return []
        emitted: list[Union[CompletedMessage, FramingLoss]] = []
        index = 0
        size = len(data)
        while True:
            if self._overflow:
                # Dropping the tail of an oversized line until its terminator.
                newline = data.find(b"\n", index)
                if newline < 0:
                    self._overflow_line_bytes += size - index
                    return emitted
                self._overflow_line_bytes += newline - index
                total = self._overflow_line_bytes
                self.oversized_bytes += total
                self.oversized_messages += 1
                self._overflow = False
                self._overflow_line_bytes = 0
                emitted.append(FramingLoss(reason=LOST_OVERSIZED_LINE, bytes=total))
                index = newline + 1
                continue
            newline = data.find(b"\n", index)
            if newline < 0:
                # No terminator in the remainder of this chunk.
                if len(self._buffer) + (size - index) > self.max_message_bytes:
                    # The unterminated line would exceed the bound: drop it and
                    # keep dropping until a future terminator (counted, never
                    # buffered), so an oversized message can never stall us.
                    self._overflow = True
                    self._overflow_line_bytes = len(self._buffer) + (size - index)
                    self._buffer.clear()
                    return emitted
                self._buffer.extend(data[index:])
                return emitted
            line_length = len(self._buffer) + (newline - index)
            if line_length > self.max_message_bytes:
                # A complete line still exceeds the bound: count and drop it.
                self.oversized_messages += 1
                self.oversized_bytes += line_length
                self._buffer.clear()
                emitted.append(
                    FramingLoss(reason=LOST_OVERSIZED_LINE, bytes=line_length)
                )
                index = newline + 1
                continue
            self._buffer.extend(data[index:newline])
            line = bytes(self._buffer).rstrip(b"\r")
            self._buffer.clear()
            if line:
                emitted.append(_classify_line(line))
            index = newline + 1

    def close(self) -> list[Union[CompletedMessage, FramingLoss]]:
        """Finalize a stream direction; partial trailing bytes are only counted."""
        if self._buffer:
            self.partial_bytes += len(self._buffer)
            self._buffer.clear()
        if self._overflow_line_bytes:
            self.oversized_bytes += self._overflow_line_bytes
            self.oversized_messages += 1
            self._overflow_line_bytes = 0
            self._overflow = False
        return []


@dataclass(frozen=True)
class Observation:
    """One metadata-only correlation observation ready for the journal worker.

    ``correlation_id`` is None exactly when the observation could not be
    attributed (unpaired/ambiguous/duplicate response, unknown or degraded
    direction, unparseable bytes) — those are explicitly flagged via ``note``
    and never silently dropped.
    """

    kind: str
    correlation_id: Optional[str]
    flow: str
    message_bytes: int
    occurrence: Optional[int]
    note: str = ""


def _opposite_flow(direction: str) -> str:
    return FLOW_INBOUND if direction == FLOW_OUTBOUND else FLOW_OUTBOUND


class StreamCorrelator:
    """Per-logical-stream correlation state (both link directions).

    Thread-unsafe by design: the runtime feeds it only from the stream data
    path of one node (single event loop, no awaits inside the feed).
    """

    def __init__(
        self,
        *,
        side: str,
        max_message_bytes: int = DEFAULT_MAX_MESSAGE_BYTES,
        max_tracked_request_ids: int = DEFAULT_MAX_TRACKED_REQUEST_IDS,
    ):
        if side not in ALL_SIDES:
            raise ValueError(f"unknown side: {side!r}")
        self.side = side
        self.max_tracked_request_ids = max(16, int(max_tracked_request_ids))
        self.framers = {
            FLOW_INBOUND: LineFramer(max_message_bytes=max_message_bytes),
            FLOW_OUTBOUND: LineFramer(max_message_bytes=max_message_bytes),
        }
        #: Requests observed per direction: canonical id -> occurrence count.
        self._occurrences: dict[str, dict[str, int]] = {
            FLOW_INBOUND: {},
            FLOW_OUTBOUND: {},
        }
        #: Outstanding (answered-not-yet-observed) requests per direction:
        #: canonical id -> ordered list of occurrences still waiting for a
        #: response.  A response consumes the oldest entry for its id, so a
        #: duplicate or unobserved response is flagged instead of paired.
        self._pending: dict[str, dict[str, list[int]]] = {
            FLOW_INBOUND: {},
            FLOW_OUTBOUND: {},
        }
        #: Distinct-id capacity reached per direction (count-only thereafter).
        self._degraded_capacity: dict[str, bool] = {
            FLOW_INBOUND: False,
            FLOW_OUTBOUND: False,
        }
        #: Fail-closed loss state per direction: reason string once the
        #: observer lost framing/classification fidelity in that direction.
        self._lost: dict[str, Optional[str]] = {
            FLOW_INBOUND: None,
            FLOW_OUTBOUND: None,
        }
        #: Last oversized counters handed to the runtime per flow, so node-level
        #: totals accumulate exact deltas (never double-counted, and in-flight
        #: overflow at eviction is still accounted by take_final_counts).
        self._counted_oversized: dict[str, tuple[int, int]] = {
            FLOW_INBOUND: (0, 0),
            FLOW_OUTBOUND: (0, 0),
        }

    def _origin_for(self, flow: str) -> str:
        return origin_side_for(flow, self.side)

    def take_oversized_delta(self, flow: str) -> tuple[int, int]:
        """Return oversized (messages, bytes) observed since the last call."""
        framer = self.framers[flow]
        current = (framer.oversized_messages, framer.oversized_bytes)
        base = self._counted_oversized[flow]
        self._counted_oversized[flow] = current
        return current[0] - base[0], current[1] - base[1]

    def take_final_counts(self, flow: str) -> tuple[int, int, int]:
        """Close one direction; return (remaining oversized messages, oversized
        bytes, partial trailing bytes) not yet handed to the runtime."""
        framer = self.framers[flow]
        framer.close()
        base = self._counted_oversized[flow]
        current = (framer.oversized_messages, framer.oversized_bytes)
        self._counted_oversized[flow] = current
        return (
            current[0] - base[0],
            current[1] - base[1],
            framer.partial_bytes,
        )

    def _degrade(self, flow: str, reason: str) -> None:
        if self._lost[flow] is None:
            self._lost[flow] = reason

    def feed(self, *, stream_id: str, flow: str, data: bytes) -> list[Observation]:
        """Classify one data-plane chunk for one direction and project rows.

        Messages and framing losses are processed strictly in byte order: a
        loss degrades the direction before any later message in the same chunk
        is projected, so nothing after a loss is ever confidently paired.
        """
        if flow not in ALL_FLOWS:
            raise ValueError(f"unknown flow: {flow!r}")
        observations: list[Observation] = []
        origin = self._origin_for(flow)
        opposite = _opposite_flow(flow)
        for item in self.framers[flow].feed(data):
            if isinstance(item, FramingLoss):
                if item.reason == LOST_OVERSIZED_LINE:
                    self._degrade(flow, LOST_OVERSIZED_LINE)
                continue
            message = item
            if message.kind == "notification":
                continue  # per-business-call only; notifications have no call pair
            if message.kind == KIND_OTHER:
                if message.note == "response-without-scalar-id":
                    # A response whose id cannot be read means an outstanding
                    # request in the opposite direction may never be consumed,
                    # so a later response could pair to a stale occurrence.
                    self._degrade(opposite, LOST_RESPONSE_ID_UNREADABLE)
                elif message.note in (
                    "unparseable",
                    "not-an-object",
                    "unrecognized-envelope",
                ):
                    self._degrade(flow, message.note)
                observations.append(
                    Observation(
                        kind=KIND_OTHER,
                        correlation_id=None,
                        flow=flow,
                        message_bytes=message.message_bytes,
                        occurrence=None,
                        note=message.note,
                    )
                )
                continue
            if message.kind == KIND_REQUEST:
                if message.canonical_id is None:
                    observations.append(
                        Observation(
                            kind=KIND_REQUEST,
                            correlation_id=None,
                            flow=flow,
                            message_bytes=message.message_bytes,
                            occurrence=None,
                            note="request-id-unbounded",
                        )
                    )
                    continue
                lost = self._lost[flow]
                if lost is not None:
                    # The observer lost framing in this direction and cannot
                    # know whether the loss was an id-reusing request: suppress
                    # confident aliases (fail closed, never false pair).
                    observations.append(
                        Observation(
                            kind=KIND_REQUEST,
                            correlation_id=None,
                            flow=flow,
                            message_bytes=message.message_bytes,
                            occurrence=None,
                            note=f"direction-degraded:{lost}",
                        )
                    )
                    continue
                occurrences = self._occurrences[flow]
                if self._degraded_capacity[flow]:
                    if message.canonical_id not in occurrences:
                        # Tracking capacity exhausted: count-only for new ids;
                        # their responses are flagged unpaired, never paired.
                        continue
                elif (
                    message.canonical_id not in occurrences
                    and len(occurrences) >= self.max_tracked_request_ids
                ):
                    self._degraded_capacity[flow] = True
                    continue
                if self._pending[flow].get(message.canonical_id):
                    # A second outstanding use is already ambiguous. Degrade
                    # now, before an attacker or noisy peer can grow a list of
                    # duplicate ids without ever supplying a response.
                    self._degrade(flow, LOST_CONCURRENT_REUSE)
                    self._pending[flow].clear()
                    observations.append(Observation(
                        kind=KIND_REQUEST, correlation_id=None, flow=flow,
                        message_bytes=message.message_bytes, occurrence=None,
                        note=f"direction-degraded:{LOST_CONCURRENT_REUSE}",
                    ))
                    continue
                occurrence = occurrences.get(message.canonical_id, 0) + 1
                occurrences[message.canonical_id] = occurrence
                self._pending[flow][message.canonical_id] = [occurrence]
                observations.append(
                    Observation(
                        kind=KIND_REQUEST,
                        correlation_id=_alias_from_canonical(
                            stream_id=stream_id,
                            origin=origin,
                            canonical_id=message.canonical_id,
                            occurrence=occurrence,
                        ),
                        flow=flow,
                        message_bytes=message.message_bytes,
                        occurrence=occurrence,
                        note="",
                    )
                )
                continue
            if message.kind == KIND_RESPONSE:
                if message.canonical_id is None:
                    self._degrade(opposite, LOST_RESPONSE_ID_UNREADABLE)
                    observations.append(
                        Observation(
                            kind=KIND_RESPONSE,
                            correlation_id=None,
                            flow=flow,
                            message_bytes=message.message_bytes,
                            occurrence=None,
                            note="response-id-unbounded",
                        )
                    )
                    continue
                lost = self._lost[opposite]
                if lost is not None:
                    # Responses pair to requests in the opposite direction;
                    # that direction degraded, so pairing is suppressed.
                    observations.append(
                        Observation(
                            kind=KIND_RESPONSE,
                            correlation_id=None,
                            flow=flow,
                            message_bytes=message.message_bytes,
                            occurrence=None,
                            note=f"direction-degraded:{lost}",
                        )
                    )
                    continue
                pending = self._pending[opposite].get(message.canonical_id)
                if not pending:
                    observations.append(
                        Observation(
                            kind=KIND_RESPONSE,
                            correlation_id=None,
                            flow=flow,
                            message_bytes=message.message_bytes,
                            occurrence=None,
                            note="response-without-observed-request",
                        )
                    )
                    continue
                if len(pending) > 1:
                    # Two same-direction requests with this id are outstanding
                    # at once (protocol anomaly): which one this answers is
                    # unknowable, so flag it and stop pairing that direction.
                    self._degrade(opposite, LOST_CONCURRENT_REUSE)
                    observations.append(
                        Observation(
                            kind=KIND_RESPONSE,
                            correlation_id=None,
                            flow=flow,
                            message_bytes=message.message_bytes,
                            occurrence=None,
                            note=f"direction-degraded:{LOST_CONCURRENT_REUSE}",
                        )
                    )
                    continue
                occurrence = pending.pop(0)
                observations.append(
                    Observation(
                        kind=KIND_RESPONSE,
                        correlation_id=_alias_from_canonical(
                            stream_id=stream_id,
                            origin=origin_side_for(opposite, self.side),
                            canonical_id=message.canonical_id,
                            occurrence=occurrence,
                        ),
                        flow=flow,
                        message_bytes=message.message_bytes,
                        occurrence=occurrence,
                        note="",
                    )
                )
                continue
        return observations

    @property
    def degraded_flows(self) -> list[str]:
        return [
            flow
            for flow, reason in self._lost.items()
            if reason is not None or self._degraded_capacity[flow]
        ]
