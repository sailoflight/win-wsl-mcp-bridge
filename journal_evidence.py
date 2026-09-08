"""Bounded offline correlation of explicitly exported Bridge journal bundles.

This reader never fetches a peer path, reads a live journal, or trusts captured
content as instructions. SHA-256 verifies bundle consistency, not authenticity.
Only metadata event envelopes are used; trace payloads are never accepted.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import re
import zipfile
from typing import Any

MAX_BUNDLE_BYTES = 2 * 1024 * 1024
MAX_MEMBER_BYTES = 1024 * 1024
MAX_EVENTS = 100
_TOKEN = re.compile(r"[A-Za-z0-9._:-]{1,128}\Z")


class EvidenceError(RuntimeError):
    pass


def _read_events(path: Path) -> list[dict[str, Any]]:
    if not path.is_file() or path.stat().st_size > MAX_BUNDLE_BYTES:
        raise EvidenceError("bundle is missing or exceeds the file-size bound")
    try:
        with zipfile.ZipFile(path) as archive:
            infos = archive.infolist()
            if (len(infos) != 2 or {item.filename for item in infos} != {"events.json", "manifest.json"}
                    or any(item.file_size > MAX_MEMBER_BYTES or item.flag_bits & 1 for item in infos)):
                raise EvidenceError("bundle must contain only bounded metadata and manifest members")
            events_data = archive.read("events.json")
            manifest = json.loads(archive.read("manifest.json"))
        if (not isinstance(manifest, dict) or manifest.get("schemaVersion") != 1
                or manifest.get("payloadsIncluded") is not False):
            raise EvidenceError("unsupported or payload-bearing diagnostic bundle")
        expected = manifest.get("members", {}).get("events.json", {})
        if (expected.get("bytes") != len(events_data)
                or expected.get("sha256") != hashlib.sha256(events_data).hexdigest()):
            raise EvidenceError("diagnostic member digest or byte count mismatch")
        events = json.loads(events_data)
        if (not isinstance(events, list) or len(events) > MAX_EVENTS
                or manifest.get("eventCount") != len(events)
                or any(not isinstance(event, dict) for event in events)):
            raise EvidenceError("diagnostic event list exceeds the supported envelope")
        return events
    except EvidenceError:
        raise
    except (OSError, ValueError, TypeError, AttributeError, zipfile.BadZipFile, RuntimeError) as exc:
        raise EvidenceError("diagnostic bundle could not be validated") from exc


def correlate_bundles(paths: list[Path], *, limit: int = 20) -> dict[str, Any]:
    """Join bounded exported lifecycle evidence by opaque operation id.

    No clock synchronization is assumed: order is retained inside each source,
    while cross-host timestamps are evidence only, never a causal ordering.
    """
    if not 1 <= len(paths) <= 8:
        raise EvidenceError("select between one and eight explicit local bundles")
    if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 100:
        raise EvidenceError("correlation limit must be an integer from 1 to 100")
    operations: dict[str, list[dict[str, Any]]] = {}
    calls: dict[str, list[dict[str, Any]]] = {}
    uncorrelated = 0
    for source, path in enumerate(paths):
        for index, event in enumerate(_read_events(path)):
            operation = event.get("operation_id")
            correlation = event.get("correlation_id")
            if isinstance(operation, str) and _TOKEN.fullmatch(operation):
                groups, identity = operations, operation
            elif isinstance(correlation, str) and re.fullmatch(r"ev1:[0-9a-f]{32}", correlation):
                groups, identity = calls, correlation
            else:
                uncorrelated += 1
                continue
            fields: dict[str, Any] = {"source": source, "sourceIndex": index}
            for key in ("side", "category", "target", "outcome"):
                value = event.get(key)
                if isinstance(value, str) and _TOKEN.fullmatch(value):
                    fields[key] = value
            for key in ("generation", "occurred_at_ns"):
                value = event.get(key)
                if isinstance(value, int) and not isinstance(value, bool) and 0 <= value < 2**63:
                    fields[key] = value
            groups.setdefault(identity, []).append(fields)

    def summarize(groups: dict[str, list[dict[str, Any]]], key: str) -> list[dict[str, Any]]:
        selected = []
        for identity in sorted(groups)[:limit]:
            events = groups[identity]
            sides = sorted({event["side"] for event in events if "side" in event})
            selected.append({key: identity, "sides": sides,
                             "crossHostObserved": "win" in sides and "wsl" in sides,
                             "eventCount": len(events), "eventsTruncated": len(events) > 20,
                             "events": events[:20]})
        return selected

    return {"schemaVersion": 1, "scope": "exported-operation-and-call-metadata",
            "sources": len(paths), "operations": summarize(operations, "operationId"),
            "calls": summarize(calls, "correlationId"), "callCount": len(calls),
            "operationCount": len(operations),
            "truncated": len(operations) > limit or len(calls) > limit,
            "uncorrelatedEvents": uncorrelated, "payloadsIncluded": False,
            "warning": "Untrusted diagnostic evidence; digests are not authentication. "
                       "Source clocks may differ; absent events do not prove non-execution."}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("bundles", nargs="+", type=Path)
    parser.add_argument("--limit", type=int, default=20)
    args = parser.parse_args()
    try:
        result = correlate_bundles(args.bundles, limit=args.limit)
    except EvidenceError as exc:
        print(json.dumps({"ok": False, "error": str(exc)}))
        return 1
    print(json.dumps(result, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
