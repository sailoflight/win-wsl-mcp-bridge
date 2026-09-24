"""Existing projection CLI, owned by the installer and loaded on demand."""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path
from typing import Any

from bridge_runtime import BridgeError
from installer.projection import (
    CLIENT_CAPABILITY_ASPECTS, _projection_side_local_port, default_projection_path, projection_enroll,
    projection_probe_client, projection_record_client_verification,
    projection_probe_status, projection_unenroll, projection_sync_peer,
    projection_reconcile, projection_watch, projection_status,
    projection_scan_candidates,
)


def add_projection_parser(subparsers, *, default_side: str) -> None:
    projection = subparsers.add_parser(
        "projection",
        help="enroll agent environments and project peer MCP registrations",
    )
    projection_sub = projection.add_subparsers(
        dest="projection_command", required=True
    )

    scan = projection_sub.add_parser(
        "scan", help="scan known client configuration locations (read-only)"
    )
    scan.add_argument("--side", choices=["win", "wsl"], default=default_side)
    scan.add_argument("--path", action="append", default=[])

    enroll = projection_sub.add_parser(
        "enroll", help="enroll one revalidated scanned candidate into an environment"
    )
    enroll.add_argument("candidate_id")
    enroll.add_argument("--side", choices=["win", "wsl"], default=default_side)
    enroll.add_argument("--path", action="append", default=[])
    enroll.add_argument("--projection")
    enroll.add_argument("--launcher")
    enroll.add_argument("--launcher-args", nargs="*", default=None)
    enroll.add_argument(
        "--native-http",
        action="store_true",
        help="record that this installed Agent client supports native Streamable HTTP",
    )
    enroll.add_argument(
        "--relay-url",
        default=None,
        metavar="URL",
        help=(
            "Operator-selected loopback base URL of this host's native Streamable "
            "HTTP relay (serve --http-relay-port), e.g. http://127.0.0.1:8878. "
            "Required with --native-http; never taken from the peer registry. "
            "The bridge appends /mcp/<registered-id>."
        ),
    )
    enroll.add_argument(
        "--stdio-http-endpoints",
        default=None,
        metavar="JSON|FILE",
        help=(
            "explicit per-target facade endpoint mapping for the stdio-to-http "
            "compatibility route: a JSON object of registered id -> loopback "
            "/mcp URL (e.g. '{\"alpha\": \"http://127.0.0.1:9011/mcp\"}'), or the "
            "path of a local file containing that JSON. Facade provisioning "
            "and readiness are the Operator's responsibility; the bridge only "
            "projects the configured URL and never launches or supervises a "
            "facade process."
        ),
    )
    enroll.add_argument(
        "--tool-exposure",
        choices=["native", "auto", "deferred"],
        default=None,
        help=(
            "exposure mode recorded for the new environment. Omit for the "
            "per-client-kind default: auto for DeepSeek Harness profiles "
            "(the bridge policy decides, and still yields the complete catalog "
            "until that environment has probe evidence), native for every "
            "other client. deferred cannot be selected here: it is reachable "
            "only by recording a client verification after enrollment."
        ),
    )
    enroll.add_argument(
        "--compatibility-route",
        choices=["native", "http-to-stdio", "stdio-to-http", "constant-two-tool"],
        default="native",
    )
    enroll.add_argument("--confirm", action="store_true")

    probe_client = projection_sub.add_parser(
        "probe-client",
        help="prepare an isolated client capability probe for a Bridge Agent "
             "(one aspect per prepared challenge; run --aspect to select)",
    )
    probe_client.add_argument("environment_id")
    probe_client.add_argument("--projection")
    probe_client.add_argument("--probe-file", required=True)
    probe_client.add_argument(
        "--aspect", choices=sorted(CLIENT_CAPABILITY_ASPECTS), default="refresh",
        help="registry decision this prepared challenge is meant to establish",
    )
    probe_client.add_argument("--dry-run", action="store_true")
    probe_client.add_argument("--confirm", action="store_true")
    record_client = projection_sub.add_parser(
        "record-client-verification", help="validate a bound probe receipt and record Harness evidence",
    )
    record_client.add_argument("environment_id")
    record_client.add_argument("--projection")
    record_client.add_argument("--receipt", required=True)
    record_client.add_argument("--tool-exposure", choices=["native", "auto", "deferred"], default="auto")
    record_client.add_argument(
        "--aspect", choices=sorted(CLIENT_CAPABILITY_ASPECTS), default=None,
        help="prepared challenge to consume; required only when several are outstanding",
    )
    record_client.add_argument(
        "--client-version", default=None,
        help="optional client product version this observation ran under; it is pinned "
             "inside the recorded version fingerprint and is never capability evidence",
    )
    record_client.add_argument("--dry-run", action="store_true")
    record_client.add_argument("--confirm", action="store_true")
    probe_status = projection_sub.add_parser(
        "probe-status",
        help="read-only: per-environment client capability evidence, outstanding "
             "challenges, and the next observation for each aspect",
    )
    probe_status.add_argument("environment_id", nargs="?")
    probe_status.add_argument("--projection")

    unenroll = projection_sub.add_parser(
        "unenroll",
        help="stop synchronizing an environment; optionally remove owned entries",
    )
    unenroll.add_argument("environment_id")
    unenroll.add_argument("--projection")
    plan = unenroll.add_mutually_exclusive_group()
    plan.add_argument("--keep-entries", action="store_true")
    plan.add_argument("--remove-entries", action="store_true")
    unenroll.add_argument("--dry-run", action="store_true")
    unenroll.add_argument("--confirm", action="store_true")

    sync = projection_sub.add_parser(
        "sync", help="refresh the peer-registry mirror and append outbox events"
    )
    sync.add_argument("--side", choices=["win", "wsl"], default=default_side)
    sync.add_argument("--projection")
    sync.add_argument(
        "--source",
        choices=["registry-path", "registry-remote"],
        default="registry-path",
    )
    sync.add_argument("--peer-registry")
    sync.add_argument("--local-host", default="127.0.0.1")
    sync.add_argument("--local-port", type=int)

    reconcile = projection_sub.add_parser(
        "reconcile",
        help="converge enrolled environments on the peer mirror "
        "(add --refresh-from to refresh first; --watch-seconds for polling)",
    )
    reconcile.add_argument("--side", choices=["win", "wsl"], default=default_side)
    reconcile.add_argument("--projection")
    reconcile.add_argument(
        "--refresh-from", choices=["registry-path", "registry-remote"]
    )
    reconcile.add_argument("--peer-registry")
    reconcile.add_argument("--local-host", default="127.0.0.1")
    reconcile.add_argument("--local-port", type=int)
    reconcile.add_argument("--dry-run", action="store_true")
    reconcile.add_argument("--watch-seconds", type=float, default=0.0)

    status = projection_sub.add_parser(
        "status", help="report environments, projections, mirror, and outbox state"
    )
    status.add_argument("--side", choices=["win", "wsl"], default=default_side)
    status.add_argument("--projection")


def _projection_db_path(args: argparse.Namespace, side: str) -> Path:
    explicit = getattr(args, "projection", None)
    if explicit:
        return Path(explicit).expanduser().resolve()
    return default_projection_path(side)


def _projection_extra_paths(args: argparse.Namespace) -> tuple[Path, ...]:
    return tuple(Path(item).expanduser().resolve() for item in getattr(args, "path", []))


def _print_json(value: Any) -> None:
    print(json.dumps(value, ensure_ascii=False, indent=2))


def _projection_cli_main(args: argparse.Namespace, *, default_side: str) -> int:
    """Operator CLI for enrollment and projection. Traceback-free on failure."""
    command = str(getattr(args, "projection_command", ""))
    try:
        side = str(getattr(args, "side", default_side) or default_side)
        if command == "scan":
            result = projection_scan_candidates(
                side, extra_paths=_projection_extra_paths(args)
            )
            _print_json({"ok": True, "candidates": result})
            return 0
        projection = _projection_db_path(args, side)
        if command == "enroll":
            launcher_args = (
                list(args.launcher_args)
                if getattr(args, "launcher_args", None) is not None
                else None
            )
            result = projection_enroll(
                projection=projection,
                side=side,
                candidate_id=str(args.candidate_id),
                extra_paths=_projection_extra_paths(args),
                launcher_command=getattr(args, "launcher", None),
                launcher_args=launcher_args,
                confirm=bool(getattr(args, "confirm", False)),
                transport_capabilities={
                    "stdio": True,
                    "streamable-http": bool(getattr(args, "native_http", False)),
                },
                compatibility_route=str(
                    getattr(args, "compatibility_route", "native")
                ),
                relay_base_url=getattr(args, "relay_url", None),
                stdio_http_endpoints=getattr(args, "stdio_http_endpoints", None),
                tool_exposure=getattr(args, "tool_exposure", None),
            )
            _print_json(result)
            return 0
        if command == "probe-client":
            _print_json(projection_probe_client(
                projection=projection, environment_id=args.environment_id,
                probe_file=Path(args.probe_file), aspect=args.aspect,
                confirm=args.confirm, dry_run=args.dry_run,
            ))
            return 0
        if command == "record-client-verification":
            _print_json(projection_record_client_verification(
                projection=projection, environment_id=args.environment_id,
                receipt_file=Path(args.receipt).expanduser().resolve(),
                tool_exposure=args.tool_exposure, aspect=args.aspect,
                client_version=args.client_version,
                confirm=args.confirm, dry_run=args.dry_run,
            ))
            return 0
        if command == "probe-status":
            _print_json(projection_probe_status(
                projection=projection, environment_id=getattr(args, "environment_id", None),
            ))
            return 0
        if command == "unenroll":
            result = projection_unenroll(
                projection=projection,
                environment_id=str(args.environment_id),
                remove_entries=bool(getattr(args, "remove_entries", False)),
                confirm=bool(getattr(args, "confirm", False)),
                dry_run=bool(getattr(args, "dry_run", False)),
            )
            _print_json(result)
            return 0
        if command == "sync":
            result = projection_sync_peer(
                projection=projection,
                source=str(getattr(args, "source", "registry-path")),
                side=side,
                peer_registry=(
                    Path(args.peer_registry).expanduser().resolve()
                    if getattr(args, "peer_registry", None)
                    else None
                ),
                local_host=str(getattr(args, "local_host", "127.0.0.1")),
                local_port=getattr(args, "local_port", None),
            )
            _print_json(result)
            return 0
        if command == "reconcile":
            local_port = getattr(args, "local_port", None)
            if local_port is None:
                local_port = _projection_side_local_port(side)
            if float(getattr(args, "watch_seconds", 0.0) or 0.0) > 0:
                return projection_watch(
                    projection=projection,
                    side=side,
                    refresh_source=str(getattr(args, "refresh_from", "registry-path")),
                    peer_registry=(
                        Path(args.peer_registry).expanduser().resolve()
                        if getattr(args, "peer_registry", None)
                        else None
                    ),
                    local_host=str(getattr(args, "local_host", "127.0.0.1")),
                    local_port=local_port,
                    interval_seconds=float(args.watch_seconds),
                )
            result = projection_reconcile(
                projection=projection,
                side=side,
                refresh_source=getattr(args, "refresh_from", None),
                peer_registry=(
                    Path(args.peer_registry).expanduser().resolve()
                    if getattr(args, "peer_registry", None)
                    else None
                ),
                local_host=str(getattr(args, "local_host", "127.0.0.1")),
                local_port=local_port,
                dry_run=bool(getattr(args, "dry_run", False)),
            )
            _print_json(result)
            return 0 if result.get("ok") else 1
        if command == "status":
            _print_json(projection_status(projection=projection))
            return 0
        raise BridgeError(f"unknown projection command: {command}")
    except (BridgeError, OSError, sqlite3.Error, ValueError) as exc:
        print(f"projection {command}: {exc}", file=sys.stderr)
        return 1


