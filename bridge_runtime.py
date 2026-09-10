#!/usr/bin/env python3
"""Shared stdlib runtime for a bidirectional WIN-WSL MCP bridge."""

from __future__ import annotations

import argparse
import asyncio
import base64
import fnmatch
import hashlib
import ipaddress
import json
import os
import re
import secrets
import shutil
import signal
import socket
import sqlite3
import ssl
import stat
import subprocess
import sys
import tempfile
import threading
import time
import uuid
import urllib.parse
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

import bridge_protocol
import stream_evidence

#: P8 always-on cross-host correlation observer bounds.
EVIDENCE_QUEUE_MAX = 2048
EVIDENCE_STATE_CAP = 64
EVIDENCE_CATEGORY = "dataflow.correlation"

BRIDGE_PROTOCOL = "win-wsl-mcp-bridge/0.2"
SERVER_VERSION = "0.4.0"
MAX_FRAME_BYTES = 1024 * 1024
BUFFER_SIZE = 65536
STREAM_DATA_ACK_TIMEOUT_SECONDS = 30
SHARED_BACKEND_RECOVERY_MAX_ATTEMPTS = 3
SHARED_BACKEND_RECOVERY_RESET_SECONDS = 30.0
SHARED_BACKEND_RECOVERY_DELAYS_SECONDS = (0.1, 0.5, 2.0)
STREAM_EOF_GRACE_SECONDS = 5
ARTIFACT_CHUNK_BYTES = 65536
DEFAULT_MAX_ARTIFACT_BYTES = 512 * 1024 * 1024
MAX_DECLARED_ARTIFACT_BYTES = 4 * 1024 * 1024 * 1024
MAX_CONCURRENT_ARTIFACTS = 8
MAX_RESERVED_ARTIFACT_BYTES = 1024 * 1024 * 1024
MAX_SHARED_JSONRPC_BYTES = 16 * 1024 * 1024
SHARED_BACKEND_STOP_TIMEOUT_SECONDS = 10
ARTIFACT_META_KEY = "io.win-wsl-mcp-bridge/artifact"
ARTIFACT_ENV_PREFIX = "WIN_WSL_MCP_BRIDGE_ARTIFACT_"
#: artifacts extension revisions. Version 1 has no resume; version 2 adds
#: content-addressed destinations, per-chunk hashes, durable receipts, and
#: link-loss resume bounded by an explicit retention window.
ARTIFACT_PROTOCOL_V1 = 1
ARTIFACT_PROTOCOL_V2 = 2
#: highest artifacts revision this runtime offers and accepts.
ARTIFACT_PROTOCOL_VERSION = ARTIFACT_PROTOCOL_V2
#: how long a parked partial / durable journal record stays resumable.
ARTIFACT_RESUME_RETENTION_SECONDS = 3600.0
#: durable per-inbox v2 delivery journal (owner-only, atomically replaced).
ARTIFACT_JOURNAL_NAME = ".v2-journal.json"
#: cap on sender-side remembered resume tokens per node session.
ARTIFACT_MAX_RESUME_TOKENS = 256
ARTIFACT_ENV_KEYS = {
    f"{ARTIFACT_ENV_PREFIX}STAGE",
    f"{ARTIFACT_ENV_PREFIX}TOKEN",
    f"{ARTIFACT_ENV_PREFIX}LOCAL_HOST",
    f"{ARTIFACT_ENV_PREFIX}LOCAL_PORT",
    f"{ARTIFACT_ENV_PREFIX}PROTOCOL",
    f"{ARTIFACT_ENV_PREFIX}PYTHON",
    f"{ARTIFACT_ENV_PREFIX}PUBLISHER",
}
INPUT_ENV_PREFIX = "WIN_WSL_MCP_BRIDGE_INPUT_"
INPUT_ENV_KEYS = {
    f"{INPUT_ENV_PREFIX}STAGE",
    f"{INPUT_ENV_PREFIX}PROTOCOL",
    f"{INPUT_ENV_PREFIX}ENABLED",
}
#: negotiated artifact-inputs/1 extension key used in hello/hello_ok.
ARTIFACT_INPUTS_EXTENSION = "artifactInputs"
#: tool-argument descriptor marker. Only literals minted by a staged input
#: transfer are ever rewritten; path-looking text is never inferred.
INPUT_DESCRIPTOR_MARKER = "bridge-input://"
ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
MAX_CONCURRENT_INPUTS = 8
MAX_STAGED_INPUT_BYTES = 512 * 1024 * 1024
MAX_DECLARED_INPUT_BYTES = 4 * 1024 * 1024 * 1024
INPUT_LINE_CAP_BYTES = MAX_SHARED_JSONRPC_BYTES
SHARED_MCP_PROTOCOL_VERSION = "2025-06-18"
#: MCP protocol revisions this runtime has verified for its logical shared
#: virtualization surface (also reported to rejected clients).  The physical
#: backend session is requested at ``SHARED_BACKEND_REQUEST_PROTOCOL_VERSION``
#: (the canonical legacy baseline) and actually runs at whatever verified
#: legacy revision the third-party backend answers (server-choose, at or below
#: the request); before any session is observed, ``SHARED_MCP_PROTOCOL_VERSION``
#: remains the pre-observation baseline reported by journals and errors.
#: Logical ``2025-11-25`` support is deliberately capability-subset support:
#: the Bridge does not advertise tasks or forward client-only
#: sampling/elicitation/task capabilities, while the pre-existing JSON-RPC/
#: tools subset is wire-compatible.
#: Newer unverified revision requests are negotiated down to the newest entry.
SHARED_COMPATIBLE_PROTOCOL_VERSIONS = {
    "2024-11-05",
    "2025-03-26",
    SHARED_MCP_PROTOCOL_VERSION,
    "2025-11-25",
}
#: Canonical legacy revision the Bridge requests when it opens its own physical
#: backend session.  A third-party backend answers with its own supported
#: verified legacy revision at or below the request (server-choose); the
#: observed answer is validated against ``SHARED_COMPATIBLE_PROTOCOL_VERSIONS``
#: and recorded as the generation's actual backend revision.  A backend that
#: answers with a modern revision, a missing/unusable protocolVersion, or an
#: error fails the shared session closed: it is never assigned a fabricated
#: revision and no logical client is handed a success initialize from it.
SHARED_BACKEND_REQUEST_PROTOCOL_VERSION = "2025-11-25"
SHARED_BACKEND_CLIENT_CAPABILITIES: dict[str, Any] = {}
#: shape of MCP protocol revision identifiers (``YYYY-MM-DD`` dates).
MCP_PROTOCOL_REVISION_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def negotiate_mcp_protocol_version(
    requested: Any,
) -> tuple[str, str | None]:
    """Negotiate one MCP ``initialize`` request generically (never client-specific).

    MCP version negotiation lets a server answer an unsupported requested
    revision with a revision it does support.  This function decides what the
    *logical client session* operates under only; the physical backend session
    is opened independently at ``SHARED_BACKEND_REQUEST_PROTOCOL_VERSION`` and
    observed (server-choose) for its actual revision.

    Returns ``(outcome, negotiated_version)`` where ``outcome`` is one of:

    * ``"accepted"`` -- the requested revision is a verified supported
      revision; the session operates under exactly that revision.
    * ``"downgraded"`` -- the requested revision is a well-formed protocol
      date this side does not support but that a newer client could step down
      from; the session operates under the newest verified revision that is
      not newer than the request.
    * ``"rejected"`` -- there is no usable revision to reply with (missing,
      non-string, malformed, or older than every verified revision); the
      caller answers with the structured diagnostic.
    """
    if not isinstance(requested, str) or not requested.strip():
        return ("rejected", None)
    if requested in SHARED_COMPATIBLE_PROTOCOL_VERSIONS:
        return ("accepted", requested)
    if MCP_PROTOCOL_REVISION_PATTERN.fullmatch(requested) is None:
        return ("rejected", None)
    # A well-formed date this side does not implement.  Reply with the newest
    # verified revision that does not exceed the requested one, so a client
    # that asked for a newer revision can continue on a revision it knows.
    # A date older than every verified revision (nothing usable to reply
    # with) is rejected with the structured diagnostic.
    candidates = sorted(
        (version for version in SHARED_COMPATIBLE_PROTOCOL_VERSIONS
         if version <= requested),
        reverse=True,
    )
    if not candidates:
        return ("rejected", None)
    return ("downgraded", candidates[0])

#: Native loopback Streamable HTTP relay. A ``serve`` node opts in with
#: ``--http-relay-port`` (default disabled). The consumer-side listener mounts
#: each registered HTTP MCP at ``/mcp/<registered-id>`` and proxies bounded
#: request envelopes over the peer link; the owner host connects to its private
#: loopback endpoint and injects owner-registry static headers. Endpoint URLs,
#: registry headers, and credentials never cross the peer link, and the peer
#: frame carries only method/target-suffix/query/headers/body plus the id.
HTTP_RELAY_MAX_REQUEST_BYTES = 512 * 1024
HTTP_RELAY_MAX_HEADER_BYTES = 65536
HTTP_RELAY_MAX_RESPONSE_HEADER_BYTES = 65536
HTTP_RELAY_MAX_HEADERS = 128
HTTP_RELAY_MAX_HEADER_VALUE_BYTES = 8192
HTTP_RELAY_MAX_INFLIGHT = 8
HTTP_RELAY_HEADER_TIMEOUT_SECONDS = 30
HTTP_RELAY_CONNECT_TIMEOUT_SECONDS = 10
HTTP_RELAY_IDLE_TIMEOUT_SECONDS = 120
HTTP_RELAY_BODY_CHUNK_BYTES = 49152
HTTP_RELAY_METHODS = {"GET", "POST", "DELETE"}
_HTTP_HEADER_NAME_RE = re.compile(rb"^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$")
_HTTP_HOP_BY_HOP = {
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailer", "transfer-encoding", "upgrade", "host", "content-length",
}
_HTTP_TOKEN_RE = re.compile(r"^[A-Za-z0-9!#$%&'*+\-.^_`|~]+$")
_HTTP_TARGET_SUFFIX_RE = re.compile(r"^[/A-Za-z0-9._~%!$&'()*+,;=:@-]*$")
_HTTP_QUERY_RE = re.compile(r"^[A-Za-z0-9._~%!$&'()*+,;=:@/?-]*$")


def _json_bytes(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def _is_loopback(host: str) -> bool:
    try:
        addresses = {
            ipaddress.ip_address(sockaddr[0].split("%", 1)[0])
            for _family, _type, _proto, _canonname, sockaddr in socket.getaddrinfo(
                host,
                None,
                type=socket.SOCK_STREAM,
            )
        }
    except (OSError, ValueError):
        return False
    return bool(addresses) and all(address.is_loopback for address in addresses)


def _http_reason(status: int) -> str:
    try:
        from http.client import responses

        return responses.get(status, "Relay")
    except Exception:  # pragma: no cover - import fallback
        return "Relay"


def _relay_safe_text(value: Any) -> str:
    """Render one header component with no CR/LF or control bytes."""
    text = value if isinstance(value, str) else str(value)
    return "".join(
        ch if (ord(ch) >= 0x20 and ord(ch) != 0x7F) or ch == "\t" else " "
        for ch in text
    )


# ---------------------------------------------------------------------------
# Bounded registered streamable-http lifecycle control contract.
#
# An Operator may register a *bounded fixed HTTP interface* for an
# ``external-controlled`` streamable-http registration.  The contract never
# contains a command, service, container, pid, or control definition: each
# allowed lifecycle action names only a fixed HTTP method, one loopback
# location (a relative ``path`` resolved against the registered private
# endpoint, or an absolute loopback ``url``), a bounded timeout, success
# statuses, and an optional bounded loopback GET readiness gate.  The owner
# node resolves everything against its private registry; the contract is
# never disclosed in public or peer-visible registry views.
# ---------------------------------------------------------------------------

#: Fixed methods permitted for a lifecycle control action.
HTTP_CONTROL_METHODS = {"GET", "POST", "PUT", "DELETE"}
#: Lifecycle actions a control contract may name.
HTTP_CONTROL_ACTIONS = ("drain", "restart", "stop")
#: Per-action interface keys.
HTTP_CONTROL_INTERFACE_KEYS = {
    "method", "path", "url", "timeoutSeconds", "successStatuses", "readiness",
}
#: Readiness gate keys (no nested readiness).
HTTP_CONTROL_READINESS_KEYS = {
    "method", "path", "url", "timeoutSeconds", "successStatuses",
}
HTTP_CONTROL_PATH_MAX = 512
HTTP_CONTROL_URL_MAX = 2048
HTTP_CONTROL_TIMEOUT_MIN = 1.0
HTTP_CONTROL_TIMEOUT_MAX = 120.0
HTTP_CONTROL_STATUS_MIN = 200
HTTP_CONTROL_STATUS_MAX = 599
_HTTP_CONTROL_LOCATION_RE = re.compile(r"^[/A-Za-z0-9._~%!$&'()*+,;=:@-]*$")


def _validate_http_control_location(
    server_id: str, action: str, interface: dict[str, Any], where: str,
    label: str = "control contract",
) -> dict[str, Any]:
    """Normalize one bounded loopback location (path or url) for a contract."""
    raw_path = interface.get("path")
    raw_url = interface.get("url")
    has_path = raw_path is not None
    has_url = raw_url is not None
    if has_path == has_url:
        raise BridgeError(
            f"registry entry {server_id!r} {label} {action}.{where} "
            "must set exactly one of path or url"
        )
    if has_path:
        if (
            not isinstance(raw_path, str)
            or not raw_path.startswith("/")
            or len(raw_path) > HTTP_CONTROL_PATH_MAX
            or "?" in raw_path
            or "#" in raw_path
            or any(ord(ch) < 0x21 or ord(ch) == 0x7F for ch in raw_path)
            or not _HTTP_CONTROL_LOCATION_RE.fullmatch(raw_path)
        ):
            raise BridgeError(
                f"registry entry {server_id!r} {label} {action}.{where} "
                "path must be a bounded relative path starting with '/'"
            )
        return {"path": raw_path}
    if (
        not isinstance(raw_url, str)
        or len(raw_url) > HTTP_CONTROL_URL_MAX
        or any(ord(ch) < 0x21 for ch in raw_url)
    ):
        raise BridgeError(
            f"registry entry {server_id!r} {label} {action}.{where} "
            "url must be a bounded string"
        )
    parsed = urllib.parse.urlsplit(raw_url)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise BridgeError(
            f"registry entry {server_id!r} {label} {action}.{where} "
            "url must be an absolute http(s) URL"
        )
    if not _is_loopback(parsed.hostname):
        raise BridgeError(
            f"registry entry {server_id!r} {label} {action}.{where} "
            "url must resolve only to loopback"
        )
    if (
        parsed.username
        or parsed.password
        or parsed.fragment
        or parsed.query
        or any(ch in parsed.path for ch in ("\r", "\n", " "))
    ):
        raise BridgeError(
            f"registry entry {server_id!r} {label} {action}.{where} "
            "url must not contain userinfo, fragment, query, or whitespace"
        )
    return {"url": raw_url}


def _validate_http_control_statuses(
    server_id: str, action: str, where: str, value: Any,
    label: str = "control contract",
) -> list[int]:
    if (
        not isinstance(value, list)
        or not value
        or len(value) > 8
        or not all(
            isinstance(code, int) and not isinstance(code, bool)
            and HTTP_CONTROL_STATUS_MIN <= code <= HTTP_CONTROL_STATUS_MAX
            for code in value
        )
        or len(value) != len(set(value))
    ):
        raise BridgeError(
            f"registry entry {server_id!r} {label} {action}.{where} "
            f"successStatuses must be unique statuses in "
            f"[{HTTP_CONTROL_STATUS_MIN}, {HTTP_CONTROL_STATUS_MAX}]"
        )
    return sorted(value)


def _validate_http_control_timeout(
    server_id: str, action: str, where: str, value: Any,
    label: str = "control contract",
) -> float:
    if (
        not isinstance(value, (int, float))
        or isinstance(value, bool)
        or value < HTTP_CONTROL_TIMEOUT_MIN
        or value > HTTP_CONTROL_TIMEOUT_MAX
    ):
        raise BridgeError(
            f"registry entry {server_id!r} {label} {action}.{where} "
            f"timeoutSeconds must be between {HTTP_CONTROL_TIMEOUT_MIN} and "
            f"{HTTP_CONTROL_TIMEOUT_MAX}"
        )
    return float(value)


def _validate_http_control_interface(
    server_id: str, action: str, interface: Any
) -> dict[str, Any]:
    """Validate one action interface of a registered HTTP control contract."""
    if not isinstance(interface, dict):
        raise BridgeError(
            f"registry entry {server_id!r} control contract {action} must be an object"
        )
    if set(interface) - HTTP_CONTROL_INTERFACE_KEYS:
        raise BridgeError(
            f"registry entry {server_id!r} control contract {action} contains "
            "unknown keys"
        )
    method = interface.get("method")
    if not isinstance(method, str) or method.upper() not in HTTP_CONTROL_METHODS:
        raise BridgeError(
            f"registry entry {server_id!r} control contract {action} method must "
            f"be one of {sorted(HTTP_CONTROL_METHODS)}"
        )
    location = _validate_http_control_location(server_id, action, interface, "location")
    timeout = _validate_http_control_timeout(
        server_id, action, interface, interface.get("timeoutSeconds", 10)
    )
    statuses = _validate_http_control_statuses(
        server_id, action, interface, interface.get("successStatuses", [200])
    )
    readiness = interface.get("readiness")
    normalized: dict[str, Any] = {
        "method": method.upper(),
        "timeoutSeconds": timeout,
        "successStatuses": statuses,
        **location,
    }
    if readiness is not None:
        if not isinstance(readiness, dict):
            raise BridgeError(
                f"registry entry {server_id!r} control contract {action} "
                "readiness must be an object"
            )
        if set(readiness) - HTTP_CONTROL_READINESS_KEYS:
            raise BridgeError(
                f"registry entry {server_id!r} control contract {action} "
                "readiness contains unknown keys"
            )
        ready_method = readiness.get("method", "GET")
        if not isinstance(ready_method, str) or ready_method.upper() != "GET":
            raise BridgeError(
                f"registry entry {server_id!r} control contract {action} "
                "readiness must use the fixed GET method"
            )
        ready_location = _validate_http_control_location(
            server_id, action, readiness, "readiness"
        )
        ready_timeout = _validate_http_control_timeout(
            server_id,
            action,
            "readiness",
            readiness.get("timeoutSeconds", 10),
        )
        ready_statuses = _validate_http_control_statuses(
            server_id,
            action,
            "readiness",
            readiness.get("successStatuses", [200]),
        )
        normalized["readiness"] = {
            "method": "GET",
            "timeoutSeconds": ready_timeout,
            "successStatuses": ready_statuses,
            **ready_location,
        }
    return normalized


# ---------------------------------------------------------------------------
# Bounded bridge-managed streamable-http supervision policy.
#
# A ``bridge-managed`` streamable-http registration declares a *local launch
# definition* (command/args/cwd/env in the private registry) together with a
# bounded supervision policy.  The owner node supervises exactly one process
# generation per registration: it launches the process, polls the required
# loopback GET readiness gate until it succeeds or the bounded startup window
# expires, and reports ``ready`` only when the gate passed.  The optional
# shutdown interface is one fixed bounded HTTP request used before signal
# escalation so the process can exit gracefully.  The policy is private to the
# owner registry and is never disclosed in public or peer-visible registry
# views, and it never accepts a command, service, container, or pid from the
# peer or the Agent.
# ---------------------------------------------------------------------------

#: Top-level supervision policy keys.
HTTP_SUPERVISION_KEYS = {"startupTimeoutSeconds", "ready", "shutdown"}
#: Readiness-gate keys (fixed GET, relative path resolved against the endpoint).
HTTP_SUPERVISION_READY_KEYS = {
    "path", "timeoutSeconds", "successStatuses", "pollIntervalSeconds",
}
#: Optional graceful shutdown interface keys (path/url exactly one).
HTTP_SUPERVISION_SHUTDOWN_KEYS = {
    "method", "path", "url", "timeoutSeconds", "successStatuses",
}
HTTP_SUPERVISION_STARTUP_TIMEOUT_MIN = 5.0
HTTP_SUPERVISION_STARTUP_TIMEOUT_MAX = 600.0
HTTP_SUPERVISION_REQUEST_TIMEOUT_MIN = 1.0
HTTP_SUPERVISION_REQUEST_TIMEOUT_MAX = 120.0
HTTP_SUPERVISION_POLL_INTERVAL_MIN = 0.2
HTTP_SUPERVISION_POLL_INTERVAL_MAX = 30.0
HTTP_SUPERVISION_STATUS_MIN = 200
HTTP_SUPERVISION_STATUS_MAX = 599
#: Bounded grace for a draining bridge-managed HTTP generation: in-flight
#: relay requests finish within this window before the generation is stopped.
HTTP_MANAGED_DRAIN_GRACE_SECONDS = 30.0


def _validate_supervision_bounded_number(
    server_id: str,
    value: Any,
    minimum: float,
    maximum: float,
    message: str,
) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not (minimum <= float(value) <= maximum)
    ):
        raise BridgeError(
            f"registry entry {server_id!r} {message} must be a number "
            f"between {minimum:g} and {maximum:g}"
        )
    return float(value)


def _validate_supervision_location(
    server_id: str, interface: dict[str, Any], where: str
) -> dict[str, Any]:
    """One bounded loopback location (path or url) inside a supervision policy."""
    raw_path = interface.get("path")
    raw_url = interface.get("url")
    has_path = raw_path is not None
    has_url = raw_url is not None
    if has_path == has_url:
        raise BridgeError(
            f"registry entry {server_id!r} supervision {where} must set exactly "
            "one of path or url"
        )
    if has_path:
        if (
            not isinstance(raw_path, str)
            or not raw_path.startswith("/")
            or len(raw_path) > HTTP_CONTROL_PATH_MAX
            or "?" in raw_path
            or "#" in raw_path
            or any(ord(ch) < 0x21 or ord(ch) == 0x7F for ch in raw_path)
            or not _HTTP_CONTROL_LOCATION_RE.fullmatch(raw_path)
        ):
            raise BridgeError(
                f"registry entry {server_id!r} supervision {where} path must be "
                "a bounded relative path starting with '/'"
            )
        return {"path": raw_path}
    if (
        not isinstance(raw_url, str)
        or len(raw_url) > HTTP_CONTROL_URL_MAX
        or any(ord(ch) < 0x21 for ch in raw_url)
    ):
        raise BridgeError(
            f"registry entry {server_id!r} supervision {where} url must be a "
            "bounded string"
        )
    parsed = urllib.parse.urlsplit(raw_url)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise BridgeError(
            f"registry entry {server_id!r} supervision {where} url must be an "
            "absolute http(s) URL"
        )
    if not _is_loopback(parsed.hostname):
        raise BridgeError(
            f"registry entry {server_id!r} supervision {where} url must resolve "
            "only to loopback"
        )
    if (
        parsed.username
        or parsed.password
        or parsed.fragment
        or parsed.query
        or any(ch in parsed.path for ch in ("\r", "\n", " "))
    ):
        raise BridgeError(
            f"registry entry {server_id!r} supervision {where} url must not "
            "contain userinfo, fragment, query, or whitespace"
        )
    return {"url": raw_url}


def _validate_supervision_statuses(
    server_id: str, where: str, value: Any
) -> list[int]:
    if (
        not isinstance(value, list)
        or not value
        or len(value) > 8
        or not all(
            isinstance(code, int) and not isinstance(code, bool)
            and HTTP_SUPERVISION_STATUS_MIN <= code <= HTTP_SUPERVISION_STATUS_MAX
            for code in value
        )
        or len(value) != len(set(value))
    ):
        raise BridgeError(
            f"registry entry {server_id!r} {where} successStatuses must be unique "
            f"statuses in [{HTTP_SUPERVISION_STATUS_MIN}, {HTTP_SUPERVISION_STATUS_MAX}]"
        )
    return sorted(value)


def _validate_http_supervision(server_id: str, value: Any) -> dict[str, Any]:
    """Validate and normalize a bridge-managed HTTP supervision policy."""
    if not isinstance(value, dict):
        raise BridgeError(
            f"registry entry {server_id!r} management.supervision must be an object"
        )
    if set(value) - HTTP_SUPERVISION_KEYS:
        raise BridgeError(
            f"registry entry {server_id!r} management.supervision contains "
            "unknown keys"
        )
    startup = _validate_supervision_bounded_number(
        server_id,
        value.get("startupTimeoutSeconds", 60),
        HTTP_SUPERVISION_STARTUP_TIMEOUT_MIN,
        HTTP_SUPERVISION_STARTUP_TIMEOUT_MAX,
        "management.supervision.startupTimeoutSeconds",
    )
    ready_raw = value.get("ready")
    if not isinstance(ready_raw, dict):
        raise BridgeError(
            f"registry entry {server_id!r} management.supervision.ready must be "
            "an object"
        )
    if set(ready_raw) - HTTP_SUPERVISION_READY_KEYS:
        raise BridgeError(
            f"registry entry {server_id!r} management.supervision.ready contains "
            "unknown keys"
        )
    ready_location = _validate_supervision_location(
        server_id, ready_raw, "ready"
    )
    if "url" in ready_location:
        raise BridgeError(
            f"registry entry {server_id!r} management.supervision.ready must "
            "resolve against the registered endpoint: use a relative path"
        )
    ready_timeout = _validate_supervision_bounded_number(
        server_id,
        ready_raw.get("timeoutSeconds", 5),
        HTTP_SUPERVISION_REQUEST_TIMEOUT_MIN,
        HTTP_SUPERVISION_REQUEST_TIMEOUT_MAX,
        "management.supervision.ready.timeoutSeconds",
    )
    ready_poll = _validate_supervision_bounded_number(
        server_id,
        ready_raw.get("pollIntervalSeconds", 1),
        HTTP_SUPERVISION_POLL_INTERVAL_MIN,
        HTTP_SUPERVISION_POLL_INTERVAL_MAX,
        "management.supervision.ready.pollIntervalSeconds",
    )
    ready_statuses = _validate_supervision_statuses(
        server_id, "management.supervision.ready", ready_raw.get("successStatuses", [200])
    )
    normalized: dict[str, Any] = {
        "startupTimeoutSeconds": startup,
        "ready": {
            "path": ready_location["path"],
            "timeoutSeconds": ready_timeout,
            "pollIntervalSeconds": ready_poll,
            "successStatuses": ready_statuses,
        },
    }
    shutdown_raw = value.get("shutdown")
    if shutdown_raw is not None:
        if not isinstance(shutdown_raw, dict):
            raise BridgeError(
                f"registry entry {server_id!r} management.supervision.shutdown "
                "must be an object"
            )
        if set(shutdown_raw) - HTTP_SUPERVISION_SHUTDOWN_KEYS:
            raise BridgeError(
                f"registry entry {server_id!r} management.supervision.shutdown "
                "contains unknown keys"
            )
        method = shutdown_raw.get("method", "POST")
        if not isinstance(method, str) or method.upper() not in HTTP_CONTROL_METHODS:
            raise BridgeError(
                f"registry entry {server_id!r} management.supervision.shutdown "
                f"method must be one of {sorted(HTTP_CONTROL_METHODS)}"
            )
        shutdown_location = _validate_supervision_location(
            server_id, shutdown_raw, "shutdown"
        )
        shutdown_timeout = _validate_supervision_bounded_number(
            server_id,
            shutdown_raw.get("timeoutSeconds", 10),
            HTTP_SUPERVISION_REQUEST_TIMEOUT_MIN,
            HTTP_SUPERVISION_REQUEST_TIMEOUT_MAX,
            "management.supervision.shutdown.timeoutSeconds",
        )
        shutdown_statuses = _validate_supervision_statuses(
            server_id,
            "management.supervision.shutdown",
            shutdown_raw.get("successStatuses", [200, 202, 204]),
        )
        normalized["shutdown"] = {
            "method": method.upper(),
            "timeoutSeconds": shutdown_timeout,
            "successStatuses": shutdown_statuses,
            **shutdown_location,
        }
    return normalized


def _split_relay_target(target: str) -> tuple[str, str, str] | None:
    """Split ``/mcp/<id><suffix>?query`` into (id, suffix, query).

    The id is the registered target; the suffix (path after the id) and query
    are forwarded verbatim so an owner can mount beneath its endpoint path.
    Anything that does not start with the exact relay mount prefix is rejected.
    """
    if not target.startswith("/mcp/"):
        return None
    remainder = target[len("/mcp/"):]
    query = ""
    if "?" in remainder:
        remainder, query = remainder.split("?", 1)
        if not _HTTP_QUERY_RE.fullmatch(query):
            return None
    segments = remainder.split("/", 1)
    server_id = segments[0]
    if not ID_PATTERN.fullmatch(server_id):
        return None
    suffix = ""
    if len(segments) == 2 and segments[1]:
        suffix = "/" + segments[1]
        if not _HTTP_TARGET_SUFFIX_RE.fullmatch(suffix):
            return None
    return server_id, suffix, query


class _RelayClientError(Exception):
    """Consumer-side HTTP request rejection carrying an HTTP status."""

    def __init__(self, status: int, message: str, *, close: bool = True):
        super().__init__(message)
        self.status = int(status)
        self.close = close


class _RelayUpstreamError(Exception):
    """Owner-side relay failure reported with its intended HTTP status."""

    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status = int(status)


def _parse_artifact_extension(value: Any) -> int | None:
    """Return the effective artifacts revision offered by a peer extension.

    Version 1 peers offer ``{"version": 1, "maxChunk": ...}``. This runtime
    offers ``{"version": 1, "highestVersion": 2, ...}`` so that an unmodified
    version-1 peer still negotiates ``artifacts/1`` while two current nodes
    agree on ``artifacts/2``. Anything else (absent or an unknown future
    revision) disables artifact delivery rather than guessing.
    """
    if not isinstance(value, dict):
        return None
    version = value.get("version")
    if not isinstance(version, int) or isinstance(version, bool):
        return None
    if version == ARTIFACT_PROTOCOL_V2:
        return ARTIFACT_PROTOCOL_V2
    if version == ARTIFACT_PROTOCOL_V1:
        if value.get("highestVersion") == ARTIFACT_PROTOCOL_V2:
            return ARTIFACT_PROTOCOL_V2
        return ARTIFACT_PROTOCOL_V1
    return None


def _artifact_max_chunk(value: Any) -> int | None:
    if not isinstance(value, dict):
        return None
    maximum = value.get("maxChunk")
    if (
        not isinstance(maximum, int)
        or isinstance(maximum, bool)
        or maximum <= 0
        or maximum > ARTIFACT_CHUNK_BYTES
    ):
        return None
    return maximum


def _artifact_key(sha256: str, name: str) -> str:
    return f"{sha256}:{name}"


def _sha256_file(path: Path) -> str:
    """Hash one regular file without following symlinks."""
    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0)
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(path, flags)
    try:
        digest = hashlib.sha256()
        with os.fdopen(descriptor, "rb", closefd=True) as handle:
            while True:
                chunk = handle.read(ARTIFACT_CHUNK_BYTES)
                if not chunk:
                    break
                digest.update(chunk)
    except BaseException:
        raise
    return digest.hexdigest()


def _read_artifact_journal(path: Path) -> dict[str, Any]:
    """Read one durable per-inbox artifacts/2 delivery journal.

    Missing journals start empty; corrupt or unknown journals fail closed so a
    resume can never trust fabricated offsets or receipts.
    """
    try:
        raw = path.read_bytes()
    except FileNotFoundError:
        return {"version": 2, "records": {}}
    try:
        value = json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeDecodeError) as exc:
        raise BridgeError("artifact delivery journal is corrupt") from exc
    if not isinstance(value, dict) or value.get("version") != 2:
        raise BridgeError("artifact delivery journal has an unsupported version")
    records = value.get("records")
    if not isinstance(records, dict):
        raise BridgeError("artifact delivery journal is malformed")
    return {"version": 2, "records": records}


def _write_artifact_journal(path: Path, journal: dict[str, Any]) -> None:
    data = json.dumps(journal, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}")
    descriptor = os.open(
        temporary,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_BINARY", 0),
        0o600,
    )
    try:
        with os.fdopen(descriptor, "wb", closefd=True) as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        try:
            directory = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        except OSError:
            pass
    except BaseException:
        try:
            temporary.unlink()
        except OSError:
            pass
        raise


class BridgeError(RuntimeError):
    pass


class JsonRpcError(BridgeError):
    def __init__(self, code: int, message: str):
        super().__init__(message)
        self.code = code


class DrainingError(BridgeError):
    """Raised when a shared registration refuses a new stream because the
    Operator has armed lifecycle drain for that registration."""


@dataclass
class LifecycleDesired:
    """Operator-armed desired lifecycle state for one bridge-owned registration.

    Desired state survives shared-backend generations on one node, so a drain
    armed while no generation is live still refuses the next demand attach.
    """

    drain: bool = False


class _WindowsProcessJob:
    """Kill-on-close Job Object scoped to one shared backend generation."""

    def __init__(self, pid: int):
        if os.name != "nt":
            raise BridgeError("Windows process jobs are available only on Windows")
        import ctypes
        from ctypes import wintypes

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)

        class IO_COUNTERS(ctypes.Structure):
            _fields_ = [
                ("ReadOperationCount", ctypes.c_ulonglong),
                ("WriteOperationCount", ctypes.c_ulonglong),
                ("OtherOperationCount", ctypes.c_ulonglong),
                ("ReadTransferCount", ctypes.c_ulonglong),
                ("WriteTransferCount", ctypes.c_ulonglong),
                ("OtherTransferCount", ctypes.c_ulonglong),
            ]

        class BASIC_LIMIT_INFORMATION(ctypes.Structure):
            _fields_ = [
                ("PerProcessUserTimeLimit", ctypes.c_longlong),
                ("PerJobUserTimeLimit", ctypes.c_longlong),
                ("LimitFlags", wintypes.DWORD),
                ("MinimumWorkingSetSize", ctypes.c_size_t),
                ("MaximumWorkingSetSize", ctypes.c_size_t),
                ("ActiveProcessLimit", wintypes.DWORD),
                ("Affinity", ctypes.c_size_t),
                ("PriorityClass", wintypes.DWORD),
                ("SchedulingClass", wintypes.DWORD),
            ]

        class EXTENDED_LIMIT_INFORMATION(ctypes.Structure):
            _fields_ = [
                ("BasicLimitInformation", BASIC_LIMIT_INFORMATION),
                ("IoInfo", IO_COUNTERS),
                ("ProcessMemoryLimit", ctypes.c_size_t),
                ("JobMemoryLimit", ctypes.c_size_t),
                ("PeakProcessMemoryUsed", ctypes.c_size_t),
                ("PeakJobMemoryUsed", ctypes.c_size_t),
            ]

        kernel32.CreateJobObjectW.argtypes = [wintypes.LPVOID, wintypes.LPCWSTR]
        kernel32.CreateJobObjectW.restype = wintypes.HANDLE
        kernel32.SetInformationJobObject.argtypes = [
            wintypes.HANDLE,
            ctypes.c_int,
            wintypes.LPVOID,
            wintypes.DWORD,
        ]
        kernel32.SetInformationJobObject.restype = wintypes.BOOL
        kernel32.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
        kernel32.OpenProcess.restype = wintypes.HANDLE
        kernel32.AssignProcessToJobObject.argtypes = [wintypes.HANDLE, wintypes.HANDLE]
        kernel32.AssignProcessToJobObject.restype = wintypes.BOOL
        kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
        kernel32.CloseHandle.restype = wintypes.BOOL

        handle = kernel32.CreateJobObjectW(None, None)
        if not handle:
            raise BridgeError(f"CreateJobObjectW failed: {ctypes.get_last_error()}")
        self._kernel32 = kernel32
        self._handle = handle
        process_handle = None
        try:
            information = EXTENDED_LIMIT_INFORMATION()
            information.BasicLimitInformation.LimitFlags = 0x00002000
            if not kernel32.SetInformationJobObject(
                handle,
                9,
                ctypes.byref(information),
                ctypes.sizeof(information),
            ):
                raise BridgeError(
                    f"SetInformationJobObject failed: {ctypes.get_last_error()}"
                )
            access = 0x0001 | 0x0100 | 0x1000
            process_handle = kernel32.OpenProcess(access, False, pid)
            if not process_handle:
                raise BridgeError(f"OpenProcess failed: {ctypes.get_last_error()}")
            if not kernel32.AssignProcessToJobObject(handle, process_handle):
                raise BridgeError(
                    f"AssignProcessToJobObject failed: {ctypes.get_last_error()}"
                )
        except Exception:
            self.close()
            raise
        finally:
            if process_handle:
                kernel32.CloseHandle(process_handle)

    def close(self) -> None:
        handle = getattr(self, "_handle", None)
        if handle:
            self._handle = None
            self._kernel32.CloseHandle(handle)


# =========================================================================
# P5 local slice: capability-warehouse primitives (static index, dedup, plan)
# =========================================================================
#
# The bridge is loopback-local, stdlib-only, and never opens a remote
# capability repository. What repository authority allows here is the *local*
# half of the P5 capability warehouse contract:
#
#   * a stable local capability inventory (Registry.identities) to dedup
#     against;
#   * a static, Operator-supplied capability index (a bounded JSON catalog
#     mirroring an external repository's *public* summaries) whose validation
#     fails closed if any item tries to carry launch/install configuration;
#   * bounded discovery over summaries only - exact schemas are a separate
#     selection step that needs the external contract;
#   * deterministic dedup so a directly registered local MCP is never offered
#     a second time through the warehouse;
#   * an install *plan* that only derives a registry-init manifest reference -
#     launch definitions must come from an Operator-authorized source and are
#     never invented from catalog content.
#
# The genuinely external pieces (fetching/trusting a remote repository,
# per-item schema exchange, and dynamic tools/add-replace-remove for DSH/Codex)
# have no contract inside this repository and are reported as blockers in the
# docs rather than guessed at here.

CAPABILITY_INDEX_VERSION = 1
WAREHOUSE_MAX_ITEMS = 4096
WAREHOUSE_MAX_IDENTITY = 128
WAREHOUSE_MAX_SUMMARY = 400
WAREHOUSE_MAX_GROUPS = 64
WAREHOUSE_MAX_GROUP = 64
WAREHOUSE_MAX_DESCRIPTION = 4000
WAREHOUSE_MAX_SEARCH_RESULTS = 50
#: Item keys that would grant execution/installation authority. A static
#: capability index may never contain them, so repository content can never
#: grant credentials, mutation, cost, restart, or installation through the
#: warehouse path.
WAREHOUSE_FORBIDDEN_ITEM_KEYS = frozenset(
    {
        "command",
        "args",
        "cwd",
        "env",
        "env_json",
        "install",
        "installer",
        "script",
        "launcher",
        "launch",
        "exec",
        "executable",
        "binary",
        "token",
        "secret",
        "credential",
    }
)


def _bounded_nonempty_text(value: Any, field: str, maximum: int) -> str:
    if not isinstance(value, str) or not value or len(value) > maximum:
        raise BridgeError(
            f"capability index item {field} must be non-empty text no longer than {maximum} characters"
        )
    if any(ord(character) < 32 and character not in "\t" for character in value):
        raise BridgeError(f"capability index item {field} contains control characters")
    return value


def _identity_key(publisher: str, name: str, version: str) -> str:
    return f"{publisher}/{name}@{version}"


def _parse_capability_index(value: Any) -> list[dict[str, Any]]:
    """Validate one static capability index and return its normalized items."""
    if not isinstance(value, dict):
        raise BridgeError("capability index must be a JSON object")
    if value.get("version") != CAPABILITY_INDEX_VERSION:
        raise BridgeError(
            f"unsupported capability index version {value.get('version')!r}; "
            f"expected {CAPABILITY_INDEX_VERSION}"
        )
    raw_items = value.get("items")
    if not isinstance(raw_items, list) or not raw_items:
        raise BridgeError("capability index must contain a non-empty items list")
    if len(raw_items) > WAREHOUSE_MAX_ITEMS:
        raise BridgeError(f"capability index exceeds {WAREHOUSE_MAX_ITEMS} items")
    items: list[dict[str, Any]] = []
    seen: set[str] = set()
    for index, raw in enumerate(raw_items):
        if not isinstance(raw, dict):
            raise BridgeError(f"capability index item {index} must be an object")
        forbidden = WAREHOUSE_FORBIDDEN_ITEM_KEYS.intersection(raw)
        if forbidden:
            raise BridgeError(
                "capability index item may not carry launch/install authority "
                f"(found: {sorted(forbidden)})"
            )
        identity = raw.get("identity")
        if not isinstance(identity, dict):
            raise BridgeError(f"capability index item {index} is missing its identity")
        publisher = _bounded_nonempty_text(
            identity.get("publisher"), "identity.publisher", WAREHOUSE_MAX_IDENTITY
        )
        name = _bounded_nonempty_text(
            identity.get("name"), "identity.name", WAREHOUSE_MAX_IDENTITY
        )
        version = _bounded_nonempty_text(
            identity.get("version"), "identity.version", WAREHOUSE_MAX_IDENTITY
        )
        summary = _bounded_nonempty_text(
            raw.get("summary"), "summary", WAREHOUSE_MAX_SUMMARY
        )
        key = _identity_key(publisher, name, version)
        if key in seen:
            raise BridgeError(f"capability index contains duplicate identity {key}")
        seen.add(key)
        description = raw.get("description", "")
        if description is not None and (
            not isinstance(description, str) or len(description) > WAREHOUSE_MAX_DESCRIPTION
        ):
            raise BridgeError(
                "capability index item description is invalid or exceeds "
                f"{WAREHOUSE_MAX_DESCRIPTION} characters"
            )
        groups = raw.get("capabilityGroups", [])
        if (
            not isinstance(groups, list)
            or not groups
            or len(groups) > WAREHOUSE_MAX_GROUPS
            or any(
                not isinstance(group, str)
                or not group
                or len(group) > WAREHOUSE_MAX_GROUP
                for group in groups
            )
        ):
            raise BridgeError("capability index item capabilityGroups are invalid")
        manifest_digest = raw.get("manifestDigest")
        if manifest_digest is not None and (
            not isinstance(manifest_digest, str)
            or re.fullmatch(r"[0-9a-f]{64}", manifest_digest) is None
        ):
            raise BridgeError("capability index item manifestDigest must be a sha256 hex digest")
        suggested_id = raw.get("id")
        if suggested_id is not None and (
            not isinstance(suggested_id, str)
            or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", suggested_id)
        ):
            raise BridgeError("capability index item id must be a safe registry-style id")
        item: dict[str, Any] = {
            "identity": {"publisher": publisher, "name": name, "version": version},
            "summary": summary,
            "capabilityGroups": list(groups),
            "key": key,
        }
        if description:
            item["description"] = description
        if manifest_digest is not None:
            item["manifestDigest"] = manifest_digest
        if suggested_id is not None:
            item["id"] = suggested_id
        items.append(item)
    return items


def capability_index_load(source: Path) -> dict[str, Any]:
    """Load and validate a static capability index from a local file."""
    if not isinstance(source, Path):
        raise BridgeError("capability index source must be a local file path")
    if source.is_symlink() or not source.is_file():
        raise BridgeError("capability index must be an existing regular local file")
    try:
        raw = json.loads(source.read_text(encoding="utf-8"))
    except (ValueError, OSError, UnicodeDecodeError) as exc:
        raise BridgeError("capability index could not be read as JSON") from exc
    items = _parse_capability_index(raw)
    return {"version": CAPABILITY_INDEX_VERSION, "items": items}


def capability_index_search(
    index: dict[str, Any], query: str, limit: int = 12
) -> list[dict[str, Any]]:
    """Bounded discovery over public summaries only (never exact schemas)."""
    if not isinstance(query, str):
        raise BridgeError("warehouse search query must be a string")
    if not isinstance(limit, int) or isinstance(limit, bool) or limit <= 0:
        raise BridgeError("warehouse search limit must be a positive integer")
    limit = min(limit, WAREHOUSE_MAX_SEARCH_RESULTS)
    items = _parse_capability_index(index)
    tokens = [token for token in query.casefold().split() if token]
    if not tokens:
        return [
            {
                "key": item["key"],
                "identity": item["identity"],
                "summary": item["summary"],
                "capabilityGroups": item["capabilityGroups"],
            }
            for item in items[:limit]
        ]
    matches: list[dict[str, Any]] = []
    for item in items:
        haystack = " ".join(
            [
                item["identity"]["publisher"],
                item["identity"]["name"],
                item["identity"]["version"],
                item["summary"],
                *item["capabilityGroups"],
                item.get("description", ""),
            ]
        ).casefold()
        if all(token in haystack for token in tokens):
            matches.append(
                {
                    "key": item["key"],
                    "identity": item["identity"],
                    "summary": item["summary"],
                    "capabilityGroups": item["capabilityGroups"],
                }
            )
        if len(matches) >= limit:
            break
    return matches


def capability_index_dedupe(
    index: dict[str, Any], local_identities: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """Return catalog items that are not already directly registered locally.

    A direct local registration never appears twice through the warehouse:
    identity ``name`` (and, when the catalog suggests it, the registry ``id``)
    is the dedup anchor, so importing the catalog can never duplicate an MCP
    the host already registered directly.
    """
    items = _parse_capability_index(index)
    local = {str(entry.get("id")): entry for entry in local_identities}
    local_names = {
        str(entry.get("name")).casefold(): entry for entry in local_identities
    }
    remaining: list[dict[str, Any]] = []
    for item in items:
        if item.get("id") in local:
            continue
        if item["identity"]["name"].casefold() in local_names:
            continue
        remaining.append(item)
    return remaining


def capability_install_plan(
    index: dict[str, Any],
    key: str,
    registry: Registry | None = None,
) -> dict[str, Any]:
    """Derive a read-only install/import plan for one catalog item.

    The plan never performs or stages an installation. Launch configuration
    cannot be invented from catalog content (the catalog cannot carry it), so
    the plan always points at an Operator-authorized ``registry-init`` manifest
    as the next step.
    """
    items = _parse_capability_index(index)
    if not isinstance(key, str) or not key:
        raise BridgeError("capability plan requires an item key")
    for item in items:
        if item["key"] == key:
            selected = item
            break
    else:
        raise BridgeError(f"capability index has no item with key {key}")
    local_match: dict[str, Any] | None = None
    if registry is not None:
        for entry in registry.identities():
            if str(entry.get("name")).casefold() == selected["identity"]["name"].casefold():
                local_match = {"id": entry["id"], "name": entry["name"]}
                break
    return {
        "key": selected["key"],
        "identity": selected["identity"],
        "summary": selected["summary"],
        "capabilityGroups": selected["capabilityGroups"],
        "locallyRegistered": local_match is not None,
        "local": local_match,
        "manifestDigest": selected.get("manifestDigest"),
        "action": "none-local" if local_match is not None else "import",
        "nextStep": (
            "already directly registered; no warehouse action"
            if local_match is not None
            else (
                "Operator imports a registry-init manifest whose server "
                f"identity is {selected['key']}; the launch definition must "
                "come from an Operator-authorized source, never from the "
                "capability catalog"
            )
        ),
    }


class EventJournal:
    """Host-local bounded metadata journal; payload capture is explicit and expiring.

    Trace sessions are created with ``start_trace``.  ``capture`` matches the
    caller-supplied (target, direction, bytes) against every confirmed,
    unexpired session that has remaining byte budget, enforces expiry and budget
    inside one SQLite transaction, and records evidence at the session's level:

    * ``envelope`` stores metadata only (message class/method, never content);
    * ``snippet`` stores a bounded prefix of the payload;
    * ``complete`` stores the full payload while it fits the remaining budget.

    Trace records are never returned with payloads by the ordinary diagnostics
    readers; payload readback is an explicit, confirmation-gated option.
    """

    SCHEMA_VERSION = 2
    #: per-message prefix kept for a ``snippet`` trace session.
    SNIPPET_PREFIX_BYTES = 1024
    #: reserved method-filter tokens matching any message of that class.
    TRACE_CLASS_TOKENS = ("request", "notification", "response", "error", "other")
    TRACE_DIRECTIONS = ("inbound", "outbound")
    #: how often the BridgeNode refreshes its cached active-trace target set.
    TRACE_TARGET_REFRESH_SECONDS = 1.0
    #: bounded eviction work per prune pass; an overflow that cannot be removed
    #: within this budget converges on the next prune call, so prune can never
    #: stall the journal hot path (no VACUUM, no unbounded row loop).
    MAX_PRUNE_EVICTIONS = 4096

    SCHEMA = """
    CREATE TABLE IF NOT EXISTS events (
        seq INTEGER PRIMARY KEY AUTOINCREMENT,
        occurred_at_ns INTEGER NOT NULL,
        monotonic_ns INTEGER NOT NULL,
        side TEXT NOT NULL,
        category TEXT NOT NULL,
        correlation_id TEXT,
        target TEXT,
        generation INTEGER,
        operation_id TEXT,
        outcome TEXT,
        retryable INTEGER,
        metadata_json TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS events_time ON events(occurred_at_ns);
    CREATE TABLE IF NOT EXISTS traces (
        trace_id TEXT PRIMARY KEY,
        target TEXT NOT NULL,
        level TEXT NOT NULL CHECK (level IN ('envelope', 'snippet', 'complete')),
        byte_budget INTEGER NOT NULL,
        bytes_used INTEGER NOT NULL DEFAULT 0,
        expires_at_ns INTEGER NOT NULL,
        confirmed INTEGER NOT NULL CHECK (confirmed IN (0, 1)),
        created_at_ns INTEGER NOT NULL,
        direction_filter TEXT CHECK (direction_filter IS NULL OR
            direction_filter IN ('inbound', 'outbound')),
        method_filter_json TEXT
    );
    """

    #: trace_records was recreated at v2 (no v1 producer ever existed).  Payload
    #: is nullable so envelope metadata-only records never hold content, and
    #: payload_bytes is always populated for bounded diagnostics readback.
    TRACE_RECORDS_SCHEMA = """
    CREATE TABLE IF NOT EXISTS trace_records (
        seq INTEGER PRIMARY KEY AUTOINCREMENT,
        trace_id TEXT NOT NULL REFERENCES traces(trace_id) ON DELETE CASCADE,
        occurred_at_ns INTEGER NOT NULL,
        direction TEXT NOT NULL CHECK (direction IN ('inbound', 'outbound')),
        kind TEXT NOT NULL,
        method_class TEXT NOT NULL,
        payload_bytes INTEGER NOT NULL,
        payload BLOB
    );
    CREATE INDEX IF NOT EXISTS trace_records_time ON trace_records(occurred_at_ns);
    CREATE INDEX IF NOT EXISTS trace_records_trace ON trace_records(trace_id);
    """

    def __init__(
        self,
        path: Path,
        *,
        max_events: int = 10000,
        max_age_seconds: int = 7 * 24 * 3600,
        max_logical_bytes: int = 64 * 1024 * 1024,
    ):
        self.path = path.expanduser().resolve()
        self.max_events = max(100, min(int(max_events), 100000))
        self.max_age_seconds = max(60, min(int(max_age_seconds), 365 * 24 * 3600))
        self.max_logical_bytes = max(64 * 1024, min(int(max_logical_bytes), 1024 * 1024 * 1024))
        previous_umask = os.umask(0o077) if os.name != "nt" else None
        try:
            self.path.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
            with sqlite3.connect(self.path, timeout=5) as connection:
                connection.execute("PRAGMA journal_mode = WAL")
                connection.execute("PRAGMA synchronous = NORMAL")
                connection.execute("PRAGMA foreign_keys = ON")
                connection.executescript(self.SCHEMA)
                connection.executescript(self.TRACE_RECORDS_SCHEMA)
                self._migrate(connection)
                connection.execute(f"PRAGMA user_version = {self.SCHEMA_VERSION}")
            if os.name != "nt":
                self.path.chmod(0o600)
        finally:
            if previous_umask is not None:
                os.umask(previous_umask)
        self.prune()

    @staticmethod
    def _ensure_column(
        connection: sqlite3.Connection, table: str, name: str, definition: str
    ) -> None:
        columns = {
            str(row[1])
            for row in connection.execute(f"PRAGMA table_info({table})").fetchall()
        }
        if name not in columns:
            connection.execute(f"ALTER TABLE {table} ADD COLUMN {name} {definition}")

    def _migrate(self, connection: sqlite3.Connection) -> None:
        version = int(connection.execute("PRAGMA user_version").fetchone()[0])
        if version < 2:
            # v1 schema had no producer for trace_records and no per-session
            # method/direction filters; recreate records and extend traces.
            connection.execute("DROP TABLE IF EXISTS trace_records")
            connection.executescript(self.TRACE_RECORDS_SCHEMA)
            self._ensure_column(connection, "traces", "direction_filter", "TEXT")
            self._ensure_column(
                connection, "traces", "method_filter_json", "TEXT"
            )
            connection.execute("PRAGMA user_version = 2")

    def record(self, *, side: str, category: str, correlation_id: str | None = None,
               target: str | None = None, generation: int | None = None,
               operation_id: str | None = None, outcome: str | None = None,
               retryable: bool | None = None, metadata: dict[str, Any] | None = None) -> None:
        safe = metadata or {}
        encoded = _canonical_json(safe)
        if len(encoded.encode("utf-8")) > 4096:
            encoded = _canonical_json({"truncated": True})
        with sqlite3.connect(self.path, timeout=5) as connection:
            connection.execute("PRAGMA busy_timeout = 5000")
            connection.execute(
                "INSERT INTO events (occurred_at_ns, monotonic_ns, side, category, "
                "correlation_id, target, generation, operation_id, outcome, retryable, metadata_json) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (time.time_ns(), time.monotonic_ns(), side, category[:64], correlation_id,
                 target, generation, operation_id, outcome, None if retryable is None else int(retryable), encoded),
            )
        self.prune()

    def recent(self, limit: int = 20) -> list[dict[str, Any]]:
        limit = max(1, min(int(limit), 100))
        with sqlite3.connect(self.path, timeout=5) as connection:
            connection.row_factory = sqlite3.Row
            rows = connection.execute(
                "SELECT occurred_at_ns, side, category, correlation_id, target, "
                "generation, operation_id, outcome, retryable, metadata_json "
                "FROM events ORDER BY seq DESC LIMIT ?", (limit,)
            ).fetchall()
        return [
            {**{key: row[key] for key in row.keys() if key != "metadata_json"},
             "metadata": json.loads(row["metadata_json"])} for row in rows
        ]

    def prune(self) -> None:
        now = time.time_ns()
        with sqlite3.connect(self.path, timeout=5) as connection:
            connection.execute("PRAGMA foreign_keys = ON")
            cutoff = now - self.max_age_seconds * 1_000_000_000
            connection.execute("DELETE FROM traces WHERE expires_at_ns <= ?", (now,))
            connection.execute("DELETE FROM events WHERE occurred_at_ns < ?", (cutoff,))
            # Age-pruned trace records return their payload to the owning
            # session's byte budget so an unexpired session keeps capturing
            # new evidence with exact accounting.
            connection.execute(
                "UPDATE traces SET bytes_used = MAX(bytes_used - ("
                "SELECT COALESCE(SUM(payload_bytes), 0) FROM trace_records "
                "WHERE trace_records.trace_id = traces.trace_id "
                "AND occurred_at_ns < ?), 0)",
                (cutoff,),
            )
            connection.execute("DELETE FROM trace_records WHERE occurred_at_ns < ?", (cutoff,))
            connection.execute(
                "DELETE FROM events WHERE seq <= COALESCE((SELECT MAX(seq) - ? FROM events), -1)",
                (self.max_events,),
            )
            logical = connection.execute(
                "SELECT COALESCE(SUM(length(metadata_json) + length(category) + 96), 0) FROM events"
            ).fetchone()[0] + connection.execute(
                "SELECT COALESCE(SUM(payload_bytes + 96), 0) FROM trace_records"
            ).fetchone()[0]
            evictions = 0
            while (
                logical > self.max_logical_bytes
                and evictions < self.MAX_PRUNE_EVICTIONS
            ):
                event = connection.execute(
                    "SELECT seq, occurred_at_ns, "
                    "length(metadata_json) + length(category) + 96 FROM events "
                    "ORDER BY occurred_at_ns, seq LIMIT 1"
                ).fetchone()
                trace = connection.execute(
                    "SELECT seq, occurred_at_ns, payload_bytes + 96, "
                    "trace_id, payload_bytes FROM trace_records "
                    "ORDER BY occurred_at_ns, seq LIMIT 1"
                ).fetchone()
                if event is None and trace is None:
                    break
                delete_event = trace is None or (
                    event is not None and (event[1], event[0]) <= (trace[1], trace[0])
                )
                if delete_event:
                    connection.execute("DELETE FROM events WHERE seq = ?", (event[0],))
                    logical -= int(event[2] or 0)
                else:
                    connection.execute(
                        "DELETE FROM trace_records WHERE seq = ?", (trace[0],)
                    )
                    # Evicting an oldest record returns its payload to the
                    # owning session's remaining byte budget so an unexpired
                    # trace session can keep capturing new evidence.
                    connection.execute(
                        "UPDATE traces SET bytes_used = MAX(bytes_used - ?, 0) "
                        "WHERE trace_id = ?",
                        (trace[4], trace[3]),
                    )
                    logical -= int(trace[2] or 0)
                evictions += 1

    def start_trace(
        self,
        target: str,
        level: str,
        seconds: int,
        byte_budget: int,
        *,
        confirm_sensitive: bool = False,
        direction: str | None = None,
        methods: list[str] | None = None,
    ) -> dict[str, Any]:
        if level not in {"envelope", "snippet", "complete"}:
            raise BridgeError("trace level must be envelope, snippet, or complete")
        if not 1 <= seconds <= 3600 or not 1 <= byte_budget <= 64 * 1024 * 1024:
            raise BridgeError("trace bounds exceed the allowed time or byte budget")
        if direction is not None and direction not in self.TRACE_DIRECTIONS:
            raise BridgeError("trace direction must be inbound, outbound, or omitted")
        method_filter: list[str] = []
        for item in methods or []:
            if not isinstance(item, str) or not item or len(item) > 200:
                raise BridgeError("trace method filters must be non-empty short strings")
            method_filter.append(item)
        if len(method_filter) > 64:
            raise BridgeError("trace method filter is too large")
        if level in {"snippet", "complete"} and not confirm_sensitive:
            return {"applied": False, "warning": "payload capture is sensitive and unverified", "requiresConfirmation": True}
        trace_id = "trace-" + uuid.uuid4().hex
        now = time.time_ns()
        method_json = (
            None
            if not method_filter
            else _canonical_json(method_filter)
        )
        with sqlite3.connect(self.path, timeout=5) as connection:
            connection.execute(
                "INSERT INTO traces (trace_id,target,level,byte_budget,expires_at_ns,"
                "confirmed,created_at_ns,direction_filter,method_filter_json) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (trace_id, target, level, byte_budget, now + seconds * 1_000_000_000,
                 int(confirm_sensitive or level == "envelope"), now,
                 direction, method_json),
            )
        response: dict[str, Any] = {
            "applied": True,
            "traceId": trace_id,
            "level": level,
            "expiresAtNs": now + seconds * 1_000_000_000,
        }
        if direction is not None:
            response["direction"] = direction
        if method_filter:
            response["methods"] = method_filter
        return response

    # ------------------------------------------------------------------
    # trace-record capture and readback
    # ------------------------------------------------------------------

    @staticmethod
    def _classify_message(data: bytes) -> tuple[str, str | None]:
        """Best-effort JSON-RPC classification over one data chunk.

        Chunks may split or merge messages, so classification is tolerant: a
        leading ``method`` field decides request/notification, and result/error
        fields decide responses.  Returns ``(kind, method)`` where method is
        None for responses and undecodable content.
        """
        text = data[:8192].decode("utf-8", errors="replace")
        method_match = re.search(r'"method"\s*:\s*"([^"\\]*(?:\\.[^"\\]*)*)"', text)
        method = method_match.group(1) if method_match else None
        if method is not None:
            if re.search(r'"id"\s*:', text):
                return ("request", method)
            return ("notification", method)
        if '"result"' in text or '"success"' in text:
            return ("response", None)
        if '"error"' in text:
            return ("error", None)
        return ("other", None)

    @staticmethod
    def _matches_filter(allowed: list[str], kind: str, method: str | None) -> bool:
        if method is not None and method in allowed:
            return True
        return kind in allowed

    @classmethod
    def _level_payload(
        cls, level: str, data: bytes, kind: str, method: str | None
    ) -> bytes | None:
        """Payload stored for one level: metadata-only, prefix, or full data."""
        if level == "envelope":
            envelope = {
                "kind": kind,
                "method": method,
                "bytes": len(data),
                "sha256": hashlib.sha256(data).hexdigest(),
            }
            return _canonical_json(envelope).encode("utf-8")
        if level == "snippet":
            return data[: cls.SNIPPET_PREFIX_BYTES]
        return data

    def capture(
        self, *, target: str, direction: str, data: bytes
    ) -> int:
        """Record trace evidence for matching active sessions.

        Selection, expiry, and byte-budget enforcement happen inside one SQLite
        transaction; the budget reservation is a conditional UPDATE so a stale
        candidate row cannot over-commit.  Returns the number of records written.
        """
        if not isinstance(target, str) or not target:
            raise BridgeError("trace capture requires a target")
        if direction not in self.TRACE_DIRECTIONS:
            raise BridgeError(f"trace direction must be one of {self.TRACE_DIRECTIONS}")
        if isinstance(data, bytearray):
            data = bytes(data)
        if not isinstance(data, bytes):
            raise BridgeError("trace capture data must be bytes")
        kind, method = self._classify_message(data)
        now = time.time_ns()
        written = 0
        with sqlite3.connect(self.path, timeout=5) as connection:
            connection.execute("PRAGMA busy_timeout = 5000")
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("BEGIN IMMEDIATE")
            try:
                rows = connection.execute(
                    "SELECT trace_id, level, direction_filter, method_filter_json "
                    "FROM traces WHERE target = ? AND confirmed = 1 "
                    "AND expires_at_ns > ? AND bytes_used < byte_budget",
                    (target, now),
                ).fetchall()
                for trace_id, level, direction_filter, method_json in rows:
                    if direction_filter is not None and direction_filter != direction:
                        continue
                    if method_json is not None:
                        allowed = json.loads(method_json)
                        if not self._matches_filter(allowed, kind, method):
                            continue
                    payload = self._level_payload(str(level), data, kind, method)
                    payload_bytes = len(payload) if payload is not None else 0
                    reserved = connection.execute(
                        "UPDATE traces SET bytes_used = bytes_used + ? "
                        "WHERE trace_id = ? AND expires_at_ns > ? "
                        "AND bytes_used + ? <= byte_budget",
                        (payload_bytes, trace_id, now, payload_bytes),
                    )
                    if reserved.rowcount != 1:
                        # expired or budget exhausted before this record.
                        continue
                    connection.execute(
                        "INSERT INTO trace_records (trace_id, occurred_at_ns, "
                        "direction, kind, method_class, payload_bytes, payload) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (trace_id, now, direction, kind, method or "",
                         payload_bytes, payload),
                    )
                    written += 1
                connection.execute("DELETE FROM traces WHERE expires_at_ns <= ?", (now,))
                connection.commit()
            except Exception:
                connection.rollback()
                raise
        return written

    def trace_records(
        self,
        *,
        trace_id: str | None = None,
        limit: int = 20,
        include_payload: bool = False,
    ) -> list[dict[str, Any]]:
        """Bounded trace-record readback.

        Ordinary diagnostics pass no ``include_payload`` and receive metadata
        only (direction, class, method, byte counts, timestamps) — never payload
        content.  ``include_payload`` returns payloads as base64 text so the
        result stays JSON-safe; callers gate that explicitly.
        """
        limit = max(1, min(int(limit), 200))
        with sqlite3.connect(self.path, timeout=5) as connection:
            connection.row_factory = sqlite3.Row
            clause = "WHERE trace_id = ?" if trace_id is not None else ""
            arguments: list[Any] = [trace_id] if trace_id is not None else []
            arguments.append(limit)
            rows = connection.execute(
                "SELECT seq, trace_id, occurred_at_ns, direction, kind, "
                "method_class, payload_bytes FROM trace_records "
                f"{clause} ORDER BY seq DESC LIMIT ?",
                arguments,
            ).fetchall()
            records = [dict(row) for row in rows]
            if include_payload and records:
                ids = [row["seq"] for row in rows]
                placeholders = ",".join("?" for _ in ids)
                payload_rows = connection.execute(
                    f"SELECT seq, payload FROM trace_records "
                    f"WHERE seq IN ({placeholders})",
                    ids,
                ).fetchall()
                payloads = {
                    int(row["seq"]): row["payload"] for row in payload_rows
                }
                for record in records:
                    blob = payloads.get(int(record["seq"]))
                    record["payload"] = (
                        base64.b64encode(blob).decode("ascii")
                        if blob is not None
                        else None
                    )
        return records

    def export_bundle(self, destination: Path, *, event_limit: int = 100) -> dict[str, Any]:
        """Write a deterministic metadata-only diagnostic ZIP and manifest.

        Trace payloads and the SQLite database are intentionally excluded. The
        caller selects a local destination; each member digest covers exact ZIP
        member bytes and the outer digest covers the completed archive.
        """
        destination = destination.expanduser().resolve()
        destination.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
        events = self.recent(event_limit)
        events_bytes = (_canonical_json(events) + "\n").encode("utf-8")
        summary = {
            "schemaVersion": 1,
            "createdAtNs": time.time_ns(),
            "eventCount": len(events),
            "payloadsIncluded": False,
            "members": {"events.json": {"sha256": hashlib.sha256(events_bytes).hexdigest(), "bytes": len(events_bytes)}},
        }
        manifest_bytes = (_canonical_json(summary) + "\n").encode("utf-8")
        temporary = destination.with_name(destination.name + ".tmp-" + uuid.uuid4().hex)
        try:
            with zipfile.ZipFile(temporary, "w", compression=zipfile.ZIP_DEFLATED) as archive:
                for name, data in (("events.json", events_bytes), ("manifest.json", manifest_bytes)):
                    info = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
                    info.compress_type = zipfile.ZIP_DEFLATED
                    info.external_attr = 0o600 << 16
                    archive.writestr(info, data)
            os.replace(temporary, destination)
            if os.name != "nt":
                destination.chmod(0o600)
        finally:
            temporary.unlink(missing_ok=True)
        return {"path": str(destination), "sha256": _sha256_file(destination), **summary}

    def active_targets(self) -> set[str]:
        """Distinct targets with confirmed, unexpired, non-exhausted sessions."""
        now = time.time_ns()
        with sqlite3.connect(self.path, timeout=5) as connection:
            rows = connection.execute(
                "SELECT DISTINCT target FROM traces WHERE confirmed = 1 "
                "AND expires_at_ns > ? AND bytes_used < byte_budget",
                (now,),
            ).fetchall()
        return {str(row[0]) for row in rows}


class Registry:
    """SQLite-backed local allowlist with redacted public metadata."""

    SCHEMA_VERSION = 4
    SCHEMA = """
    CREATE TABLE IF NOT EXISTS servers (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        summary TEXT NOT NULL,
        command TEXT NOT NULL,
        args_json TEXT NOT NULL,
        cwd TEXT,
        env_json TEXT NOT NULL,
        process_json TEXT NOT NULL,
        capability_groups_json TEXT NOT NULL,
        server_info_json TEXT,
        artifact_delivery_json TEXT NOT NULL DEFAULT '{"enabled":false}',
        input_delivery_json TEXT NOT NULL DEFAULT '{"enabled":false}',
        transport_json TEXT NOT NULL DEFAULT '{"type":"stdio"}',
        management_json TEXT NOT NULL DEFAULT '{"ownership":"external","agentControl":{"enabled":false,"allowedActions":[]}}',
        enabled INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0, 1)),
        updated_at_ns INTEGER NOT NULL
    )
    """

    def __init__(self, path: Path):
        self.path = path
        if not path.is_file():
            raise BridgeError(
                f"registry database does not exist: {path}; initialize it with registry-init"
            )
        if os.name != "nt":
            try:
                path.chmod(0o600)
            except OSError as exc:
                raise BridgeError(f"registry database permissions could not be restricted: {path}") from exc
        with self._connect() as connection:
            version = int(connection.execute("PRAGMA user_version").fetchone()[0])
            if version != self.SCHEMA_VERSION:
                raise BridgeError(
                    f"unsupported registry schema version {version}; expected {self.SCHEMA_VERSION}"
                )
            connection.execute("SELECT id FROM servers LIMIT 1").fetchall()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=5)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 5000")
        connection.execute("PRAGMA query_only = ON")
        return connection

    @classmethod
    def initialize_database(
        cls,
        database: Path,
        manifest: Path,
        *,
        replace: bool = False,
        projection_path: Path | None = None,
    ) -> None:
        raw = json.loads(manifest.read_text(encoding="utf-8"))
        rows = raw.get("servers") if isinstance(raw, dict) else raw
        if not isinstance(rows, list):
            raise BridgeError("manifest must be an array or an object with a servers array")
        validated = [cls._validate_manifest_row(row) for row in rows]
        ids = [row["id"] for row in validated]
        if len(ids) != len(set(ids)):
            raise BridgeError("manifest contains duplicate registry ids")
        previous_umask = os.umask(0o077) if os.name != "nt" else None
        connection: sqlite3.Connection | None = None
        try:
            database.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
            connection = sqlite3.connect(database, timeout=5)
            if os.name != "nt":
                database.chmod(0o600)
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("PRAGMA busy_timeout = 5000")
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute("PRAGMA synchronous = NORMAL")
            version = int(connection.execute("PRAGMA user_version").fetchone()[0])
            if version not in {0, 1, 2, 3, cls.SCHEMA_VERSION}:
                raise BridgeError(f"cannot migrate registry schema version {version}")
            connection.execute(cls.SCHEMA)
            if version == 1:
                connection.execute(
                    "ALTER TABLE servers ADD COLUMN artifact_delivery_json "
                    "TEXT NOT NULL DEFAULT '{\"enabled\":false}'"
                )
            if version in {1, 2}:
                connection.execute(
                    "ALTER TABLE servers ADD COLUMN input_delivery_json "
                    "TEXT NOT NULL DEFAULT '{\"enabled\":false}'"
                )
            if version in {1, 2, 3}:
                connection.execute(
                    "ALTER TABLE servers ADD COLUMN transport_json "
                    "TEXT NOT NULL DEFAULT '{\"type\":\"stdio\"}'"
                )
                connection.execute(
                    "ALTER TABLE servers ADD COLUMN management_json "
                    "TEXT NOT NULL DEFAULT '{\"ownership\":\"external\","
                    "\"agentControl\":{\"enabled\":false,\"allowedActions\":[]}}'"
                )
                connection.execute(
                    "UPDATE servers SET management_json = "
                    "'{\"ownership\":\"bridge-managed\",\"agentControl\":{"
                    "\"enabled\":false,\"allowedActions\":[]}}'"
                )
            connection.execute(f"PRAGMA user_version = {cls.SCHEMA_VERSION}")
            before_facts = _registry_projection_facts(connection)
            after_facts = sorted(
                (
                    row["id"],
                    row["name"],
                    1 if row["enabled"] else 0,
                    row["transport"]["type"],
                )
                for row in validated
            )
            projection_changed = (
                projection_path is not None and before_facts != after_facts
            )
            if projection_changed:
                resolved_projection = Path(projection_path).expanduser().resolve()
                if os.path.abspath(str(resolved_projection)) == os.path.abspath(str(database)):
                    raise BridgeError(
                        "projection database must differ from the registry database"
                    )
                ProjectionDatabase.ensure(resolved_projection)
                connection.execute(
                    "ATTACH DATABASE ? AS projection", (str(resolved_projection),)
                )
            with connection:
                if replace:
                    connection.execute("DELETE FROM servers")
                if projection_changed:
                    _append_registry_change_event(
                        connection, before_facts, after_facts
                    )
                for row in validated:
                    connection.execute(
                        """
                        INSERT INTO servers (
                            id, name, summary, command, args_json, cwd, env_json,
                            process_json, capability_groups_json, server_info_json,
                            artifact_delivery_json, input_delivery_json,
                            transport_json, management_json, enabled, updated_at_ns
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(id) DO UPDATE SET
                            name = excluded.name,
                            summary = excluded.summary,
                            command = excluded.command,
                            args_json = excluded.args_json,
                            cwd = excluded.cwd,
                            env_json = excluded.env_json,
                            process_json = excluded.process_json,
                            capability_groups_json = excluded.capability_groups_json,
                            server_info_json = excluded.server_info_json,
                            artifact_delivery_json = excluded.artifact_delivery_json,
                            input_delivery_json = excluded.input_delivery_json,
                            transport_json = excluded.transport_json,
                            management_json = excluded.management_json,
                            enabled = excluded.enabled,
                            updated_at_ns = excluded.updated_at_ns
                        """,
                        (
                            row["id"],
                            row["name"],
                            row["summary"],
                            row["command"],
                            json.dumps(row["args"], separators=(",", ":")),
                            row["cwd"],
                            json.dumps(row["env"], separators=(",", ":")),
                            json.dumps(row["process"], separators=(",", ":")),
                            json.dumps(row["capabilityGroups"], separators=(",", ":")),
                            json.dumps(row["serverInfo"], separators=(",", ":"))
                            if row["serverInfo"] is not None
                            else None,
                            json.dumps(row["artifactDelivery"], separators=(",", ":")),
                            json.dumps(row["inputDelivery"], separators=(",", ":")),
                            json.dumps(row["transport"], separators=(",", ":")),
                            json.dumps(row["management"], separators=(",", ":")),
                            1 if row["enabled"] else 0,
                            time.time_ns(),
                        ),
                    )
        finally:
            if connection is not None:
                connection.close()
            if previous_umask is not None:
                os.umask(previous_umask)

    @staticmethod
    def _validate_manifest_row(row: Any) -> dict[str, Any]:
        if not isinstance(row, dict):
            raise BridgeError("every manifest entry must be an object")
        server_id = row.get("id")
        if not isinstance(server_id, str) or not ID_PATTERN.fullmatch(server_id):
            raise BridgeError(f"invalid registry id: {server_id!r}")
        transport = row.get("transport", {"type": "stdio"})
        if not isinstance(transport, dict):
            raise BridgeError(f"registry entry {server_id!r} transport must be an object")
        transport_type = transport.get("type", "stdio")
        if transport_type not in {"stdio", "streamable-http"}:
            raise BridgeError(
                f"registry entry {server_id!r} transport.type must be stdio or streamable-http"
            )
        protocol_era = transport.get("protocolEra")
        if "protocolEra" in transport and (
            transport_type != "streamable-http" or protocol_era not in ("legacy", "modern")
        ):
            raise BridgeError(
                f"registry entry {server_id!r} transport.protocolEra is only legacy or modern for HTTP conversion"
            )
        transport = {"type": transport_type}
        if protocol_era is not None:
            transport["protocolEra"] = protocol_era
        if transport_type == "streamable-http":
            endpoint = row.get("transport", {}).get("endpoint")
            if not isinstance(endpoint, str) or not endpoint:
                raise BridgeError(
                    f"registry entry {server_id!r} streamable-http transport requires endpoint"
                )
            parsed_endpoint = urllib.parse.urlsplit(endpoint)
            if parsed_endpoint.scheme not in {"http", "https"} or not parsed_endpoint.hostname:
                raise BridgeError(f"registry entry {server_id!r} endpoint is invalid")
            if not _is_loopback(parsed_endpoint.hostname):
                raise BridgeError(
                    f"registry entry {server_id!r} streamable-http endpoint must use loopback"
                )
            if parsed_endpoint.username or parsed_endpoint.password or parsed_endpoint.fragment:
                raise BridgeError(
                    f"registry entry {server_id!r} endpoint must not contain userinfo or fragment"
                )
            transport["endpoint"] = endpoint
            headers = row.get("transport", {}).get("headers", {})
            if not isinstance(headers, dict) or not all(
                isinstance(key, str) and isinstance(value, str)
                for key, value in headers.items()
            ):
                raise BridgeError(
                    f"registry entry {server_id!r} transport.headers must map strings to strings"
                )
            transport["headers"] = dict(headers)
        command = row.get("command", "")
        if not isinstance(command, str):
            raise BridgeError(f"registry entry {server_id!r} command must be a string")
        if transport_type == "stdio" and not command:
            raise BridgeError(f"registry entry {server_id!r} requires command")
        args = row.get("args", [])
        if not isinstance(args, list) or not all(isinstance(arg, str) for arg in args):
            raise BridgeError(f"registry entry {server_id!r} args must be strings")
        env = row.get("env", {})
        if not isinstance(env, dict) or not all(
            isinstance(key, str) and isinstance(value, str) for key, value in env.items()
        ):
            raise BridgeError(f"registry entry {server_id!r} env must map strings to strings")
        if any(
            key.casefold().startswith(prefix.casefold())
            for key in env
            for prefix in (ARTIFACT_ENV_PREFIX, INPUT_ENV_PREFIX)
        ):
            raise BridgeError(
                f"registry entry {server_id!r} env must not set bridge artifact/input variables"
            )
        process = row.get(
            "process",
            {"multiProcessAllowed": None, "enforcement": "unverified"},
        )
        if not isinstance(process, dict):
            raise BridgeError(f"registry entry {server_id!r} process must be an object")
        process = dict(process)
        allowed = process.get("multiProcessAllowed")
        if allowed is not None and not isinstance(allowed, bool):
            raise BridgeError(
                f"registry entry {server_id!r} multiProcessAllowed must be true, false, or null"
            )
        if allowed is False:
            process["enforcement"] = "bridge-shared-backend"
        client_lease = process.get("clientLease")
        if client_lease is not None:
            if allowed is not False:
                raise BridgeError(
                    f"registry entry {server_id!r} clientLease requires multiProcessAllowed=false"
                )
            if not isinstance(client_lease, dict):
                raise BridgeError(f"registry entry {server_id!r} clientLease must be an object")
            tool_patterns = client_lease.get("toolPatterns")
            release_tool = client_lease.get("releaseTool")
            release_arguments = client_lease.get("releaseArguments", {})
            released_path = client_lease.get(
                "releasedResultPath", ["structuredContent", "profileReleased"]
            )
            cleanup_timeout = client_lease.get("cleanupTimeoutSeconds", 15)
            if (
                not isinstance(tool_patterns, list)
                or not tool_patterns
                or not all(isinstance(pattern, str) and pattern for pattern in tool_patterns)
                or not isinstance(release_tool, str)
                or not release_tool
                or not isinstance(release_arguments, dict)
                or not all(isinstance(key, str) for key in release_arguments)
                or not isinstance(released_path, list)
                or not released_path
                or not all(isinstance(part, str) and part for part in released_path)
                or not isinstance(cleanup_timeout, (int, float))
                or isinstance(cleanup_timeout, bool)
                or cleanup_timeout <= 0
                or cleanup_timeout > 120
            ):
                raise BridgeError(f"registry entry {server_id!r} clientLease is invalid")
            process["clientLease"] = {
                "toolPatterns": list(tool_patterns),
                "releaseTool": release_tool,
                "releaseArguments": dict(release_arguments),
                "releasedResultPath": list(released_path),
                "releasedResultValue": client_lease.get("releasedResultValue", True),
                "cleanupTimeoutSeconds": float(cleanup_timeout),
                "busyPolicy": "error",
            }
        shared_state = process.get("sharedState")
        if shared_state is not None:
            if allowed is not False or not isinstance(shared_state, dict):
                raise BridgeError(
                    f"registry entry {server_id!r} sharedState requires a shared backend"
                )
            rejected_tools = shared_state.get("rejectTools")
            if (
                shared_state.get("mode") != "fixed"
                or not isinstance(rejected_tools, list)
                or not all(isinstance(tool, str) and tool for tool in rejected_tools)
            ):
                raise BridgeError(f"registry entry {server_id!r} sharedState is invalid")
            process["sharedState"] = {
                "mode": "fixed",
                "rejectTools": list(rejected_tools),
            }
        groups = row.get("capabilityGroups", [])
        if not isinstance(groups, list) or not all(isinstance(group, str) for group in groups):
            raise BridgeError(f"registry entry {server_id!r} capabilityGroups must be strings")
        server_info = row.get("serverInfo")
        if server_info is not None and not isinstance(server_info, dict):
            raise BridgeError(f"registry entry {server_id!r} serverInfo must be an object")
        cwd = row.get("cwd")
        if cwd is not None and not isinstance(cwd, str):
            raise BridgeError(f"registry entry {server_id!r} cwd must be a string or null")
        artifact_delivery = row.get("artifactDelivery", {"enabled": False})
        if not isinstance(artifact_delivery, dict):
            raise BridgeError(
                f"registry entry {server_id!r} artifactDelivery must be an object"
            )
        artifact_enabled = artifact_delivery.get("enabled", False)
        artifact_max_bytes = artifact_delivery.get(
            "maxBytes", DEFAULT_MAX_ARTIFACT_BYTES
        )
        if not isinstance(artifact_enabled, bool):
            raise BridgeError(
                f"registry entry {server_id!r} artifactDelivery.enabled must be boolean"
            )
        if (
            not isinstance(artifact_max_bytes, int)
            or isinstance(artifact_max_bytes, bool)
            or artifact_max_bytes <= 0
            or artifact_max_bytes > MAX_DECLARED_ARTIFACT_BYTES
        ):
            raise BridgeError(
                f"registry entry {server_id!r} artifactDelivery.maxBytes must be "
                f"between 1 and {MAX_DECLARED_ARTIFACT_BYTES}"
            )
        artifact_delivery = {
            "enabled": artifact_enabled,
            "maxBytes": artifact_max_bytes,
            "mode": "workspace-push-v1",
        }
        input_delivery = row.get("inputDelivery", {"enabled": False})
        if not isinstance(input_delivery, dict):
            raise BridgeError(
                f"registry entry {server_id!r} inputDelivery must be an object"
            )
        input_enabled = input_delivery.get("enabled", False)
        input_max_bytes = input_delivery.get("maxBytes", MAX_STAGED_INPUT_BYTES)
        if not isinstance(input_enabled, bool):
            raise BridgeError(
                f"registry entry {server_id!r} inputDelivery.enabled must be boolean"
            )
        if (
            not isinstance(input_max_bytes, int)
            or isinstance(input_max_bytes, bool)
            or input_max_bytes <= 0
            or input_max_bytes > MAX_DECLARED_INPUT_BYTES
        ):
            raise BridgeError(
                f"registry entry {server_id!r} inputDelivery.maxBytes must be "
                f"between 1 and {MAX_DECLARED_INPUT_BYTES}"
            )
        input_delivery = {
            "enabled": input_enabled,
            "maxBytes": input_max_bytes,
            "mode": "agent-input-v1",
        }
        if input_enabled and process.get("multiProcessAllowed") is False:
            raise BridgeError(
                f"registry entry {server_id!r} inputDelivery requires a dedicated "
                "business process; shared-backend input staging is not implemented"
            )
        if transport_type == "streamable-http" and (artifact_enabled or input_enabled):
            raise BridgeError(
                f"registry entry {server_id!r} artifact/input delivery is not yet "
                "supported for streamable-http registrations"
            )
        management = row.get("management", {})
        if not isinstance(management, dict):
            raise BridgeError(f"registry entry {server_id!r} management must be an object")
        ownership = management.get(
            "ownership", "bridge-managed" if transport_type == "stdio" else "external"
        )
        allowed_ownership = (
            {"bridge-managed"}
            if transport_type == "stdio"
            else {"external", "external-controlled", "bridge-managed"}
        )
        if ownership not in allowed_ownership:
            raise BridgeError(
                f"registry entry {server_id!r} management.ownership is invalid for {transport_type}"
            )
        agent_control = management.get("agentControl", {"enabled": False})
        if not isinstance(agent_control, dict):
            raise BridgeError(
                f"registry entry {server_id!r} management.agentControl must be an object"
            )
        control_enabled = agent_control.get("enabled", False)
        allowed_actions = agent_control.get("allowedActions", [])
        if (
            not isinstance(control_enabled, bool)
            or not isinstance(allowed_actions, list)
            or not all(action in {"drain", "refresh", "restart", "stop"} for action in allowed_actions)
            or len(allowed_actions) != len(set(allowed_actions))
        ):
            raise BridgeError(
                f"registry entry {server_id!r} management.agentControl is invalid"
            )
        if not control_enabled and allowed_actions:
            raise BridgeError(
                f"registry entry {server_id!r} disabled agentControl cannot allow actions"
            )
        contract_raw = management.get("controlContract")
        control_contract: dict[str, Any] = {}
        if contract_raw is not None:
            if transport_type != "streamable-http":
                raise BridgeError(
                    f"registry entry {server_id!r} management.controlContract "
                    "requires a streamable-http registration"
                )
            if ownership != "external-controlled":
                raise BridgeError(
                    f"registry entry {server_id!r} management.controlContract "
                    "requires management.ownership 'external-controlled'"
                )
            if not isinstance(contract_raw, dict):
                raise BridgeError(
                    f"registry entry {server_id!r} management.controlContract "
                    "must be an object"
                )
            if set(contract_raw) - set(HTTP_CONTROL_ACTIONS):
                raise BridgeError(
                    f"registry entry {server_id!r} management.controlContract may "
                    f"only name lifecycle actions {list(HTTP_CONTROL_ACTIONS)}"
                )
            for action, interface in contract_raw.items():
                control_contract[action] = _validate_http_control_interface(
                    server_id, action, interface
                )
            if not control_contract:
                raise BridgeError(
                    f"registry entry {server_id!r} management.controlContract "
                    "must name at least one lifecycle action"
                )
        supervision_raw = management.get("supervision")
        supervision: dict[str, Any] | None = None
        if supervision_raw is not None:
            if transport_type != "streamable-http" or ownership != "bridge-managed":
                raise BridgeError(
                    f"registry entry {server_id!r} management.supervision requires "
                    "a streamable-http registration with management.ownership "
                    "'bridge-managed'"
                )
            supervision = _validate_http_supervision(server_id, supervision_raw)
        if transport_type == "streamable-http" and ownership == "bridge-managed":
            if not command.strip():
                raise BridgeError(
                    f"registry entry {server_id!r} bridge-managed streamable-http "
                    "requires a local command to supervise"
                )
            if allowed is False:
                raise BridgeError(
                    f"registry entry {server_id!r} bridge-managed streamable-http "
                    "cannot use multiProcessAllowed=false (the bridge-managed "
                    "shared JSON-RPC backend is stdio-only)"
                )
            if supervision is None:
                raise BridgeError(
                    f"registry entry {server_id!r} bridge-managed streamable-http "
                    "requires management.supervision with a bounded readiness gate"
                )
        management = {
            "ownership": ownership,
            "agentControl": {
                "enabled": control_enabled,
                "allowedActions": sorted(allowed_actions),
                "allowImpactOverride": bool(agent_control.get("allowImpactOverride", False)),
            },
        }
        if control_contract:
            management["controlContract"] = dict(
                sorted(control_contract.items())
            )
        if supervision is not None:
            management["supervision"] = supervision
        enabled = row.get("enabled", True)
        if not isinstance(enabled, bool):
            raise BridgeError(f"registry entry {server_id!r} enabled must be boolean")
        return {
            "id": server_id,
            "name": row.get("name") if isinstance(row.get("name"), str) else server_id,
            "summary": row.get("summary") if isinstance(row.get("summary"), str) else "",
            "transport": transport,
            "management": management,
            "command": command,
            "args": args,
            "cwd": cwd,
            "env": env,
            "process": process,
            "capabilityGroups": groups,
            "serverInfo": server_info,
            "artifactDelivery": artifact_delivery,
            "inputDelivery": input_delivery,
            "enabled": enabled,
        }

    def _get_row(self, server_id: str) -> sqlite3.Row:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM servers WHERE id = ? AND enabled = 1",
                (server_id,),
            ).fetchone()
        if row is None:
            raise BridgeError(f"unknown registry id: {server_id}")
        return row

    def _ids(self) -> list[str]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT id FROM servers WHERE enabled = 1 ORDER BY id"
            ).fetchall()
        return [str(row["id"]) for row in rows]

    def launch(self, server_id: str) -> dict[str, Any]:
        row = self._get_row(server_id)
        return {
            "id": row["id"],
            "command": row["command"],
            "args": json.loads(row["args_json"]),
            "cwd": row["cwd"],
            "env": json.loads(row["env_json"]),
            "process": json.loads(row["process_json"]),
            "artifactDelivery": json.loads(row["artifact_delivery_json"]),
            "inputDelivery": json.loads(row["input_delivery_json"]),
            "transport": json.loads(row["transport_json"]),
            "management": json.loads(row["management_json"]),
        }

    def public(self, server_id: str) -> dict[str, Any]:
        row = self._get_row(server_id)
        private_process = json.loads(row["process_json"])
        process = {
            "multiProcessAllowed": private_process.get("multiProcessAllowed"),
            "enforcement": private_process.get("enforcement", "unverified"),
        }
        if isinstance(private_process.get("clientLease"), dict):
            process["clientLease"] = {
                "enabled": True,
                "busyPolicy": "error",
                "releaseOnDisconnect": True,
            }
        if isinstance(private_process.get("sharedState"), dict):
            process["sharedState"] = {"mode": "fixed"}
        private_transport = json.loads(row["transport_json"])
        private_management = json.loads(row["management_json"])
        agent_control = private_management.get("agentControl", {})
        public = {
            "id": row["id"],
            "name": row["name"],
            "summary": row["summary"],
            "transport": {"type": private_transport.get("type", "stdio")},
            "management": {
                "ownership": private_management.get("ownership", "external"),
                "agentControl": {
                    "enabled": bool(agent_control.get("enabled", False)),
                    "allowedActions": list(agent_control.get("allowedActions", [])),
                },
            },
            "process": process,
            "capabilityGroups": json.loads(row["capability_groups_json"]),
        }
        if row["server_info_json"] is not None:
            public["serverInfo"] = json.loads(row["server_info_json"])
        artifact_delivery = json.loads(row["artifact_delivery_json"])
        if artifact_delivery.get("enabled"):
            public["artifactDelivery"] = {
                "mode": "workspace-push-v1",
                "declaredMaxBytes": artifact_delivery["maxBytes"],
                "agentFetchRequired": False,
            }
        input_delivery = json.loads(row["input_delivery_json"])
        if input_delivery.get("enabled"):
            public["inputDelivery"] = {
                "mode": "agent-input-v1",
                "declaredMaxBytes": input_delivery["maxBytes"],
                "agentSubmitMode": "push",
            }
        return public

    def identities(self) -> list[dict[str, Any]]:
        """Local capability inventory: stable public identity rows only.

        This is the dedup baseline for capability-warehouse integration. It
        exposes the same redacted metadata as ``public()`` minus per-server
        delivery shape, and never a command, argument, environment, token, or
        installation detail.
        """
        rows = []
        for server_id in self._ids():
            row = self._get_row(server_id)
            rows.append(
                {
                    "id": row["id"],
                    "name": row["name"],
                    "summary": row["summary"],
                    "capabilityGroups": json.loads(row["capability_groups_json"]),
                    "enabled": bool(row["enabled"]),
                }
            )
        return rows

    def query(self, action: str, arguments: dict[str, Any]) -> Any:
        if action == "list":
            return [self.public(server_id) for server_id in self._ids()]
        if action == "describe":
            server_id = arguments.get("id")
            if not isinstance(server_id, str):
                raise BridgeError("describe requires string id")
            return self.public(server_id)
        if action == "search":
            query = arguments.get("query", "")
            if not isinstance(query, str):
                raise BridgeError("search query must be a string")
            needle = query.casefold()
            return [
                row
                for row in (self.public(server_id) for server_id in self._ids())
                if needle in json.dumps(row, ensure_ascii=False).casefold()
            ]
        if action == "status":
            server_id = arguments.get("id")
            if server_id is None:
                return [
                    {"id": item, "registered": True, "availability": "not-probed"}
                    for item in self._ids()
                ]
            if not isinstance(server_id, str):
                raise BridgeError("status id must be a string")
            self._get_row(server_id)
            return {"id": server_id, "registered": True, "availability": "not-probed"}
        raise BridgeError(f"unknown registry action: {action}")


@dataclass
class StreamState:
    stream_id: str
    socket_writer: asyncio.StreamWriter | None = None
    process: asyncio.subprocess.Process | None = None
    opened: asyncio.Future[None] | None = None
    closed: asyncio.Event = field(default_factory=asyncio.Event)
    tasks: set[asyncio.Task[Any]] = field(default_factory=set)
    inbound: asyncio.Queue[tuple[int, bytes] | None] = field(
        default_factory=lambda: asyncio.Queue(maxsize=32)
    )
    inbound_sequence: int = 0
    outbound_sequence: int = 0
    outbound_ack: asyncio.Future[int] | None = None
    target: str | None = None
    artifact_inbox: Path | None = None
    artifact_stage: Path | None = None
    artifact_token: str | None = None
    artifact_max_bytes: int = DEFAULT_MAX_ARTIFACT_BYTES
    #: artifact-inputs/1 receiver state: private per-stream stage on the MCP host.
    input_stage: Path | None = None
    input_max_bytes: int = MAX_STAGED_INPUT_BYTES
    #: staged input id -> committed staged path (immutable for the stream lifetime).
    input_handles: dict[str, Path] = field(default_factory=dict)
    #: byte buffer for exact input-descriptor rewriting on dedicated streams.
    input_rewrite_buffer: bytearray = field(default_factory=bytearray)
    send_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    shared_backend: "SharedBackend | None" = None
    # Evidence admission is permanent for this stream lifetime: never restart
    # correlation counters after capacity pressure or lost state.
    evidence_disabled: bool = False


@dataclass
class RoutedRequest:
    backend_id: str
    client_id: str | None
    original_id: Any
    message: dict[str, Any]
    done: asyncio.Future[dict[str, Any]]
    method: str
    is_initialize: bool = False
    is_release: bool = False
    cancelled: bool = False
    progress_token: tuple[str, Any] | None = None
    #: True for a request issued under the modern (2026-07-28 per-request
    #: metadata) era: its backend response is shaped with the modern envelope
    #: (resultType/cache/serverInfo) and never leaks modern fields to legacy
    #: clients.  Legacy-era requests keep ``modern`` False and byte-today shape.
    modern: bool = False


@dataclass
class SharedBackendClient:
    stream: StreamState
    input_buffer: bytearray = field(default_factory=bytearray)
    input_eof: bool = False
    protocol_version: str | None = None
    capabilities: dict[str, Any] = field(default_factory=dict)


@dataclass
class ArtifactTransferWaiter:
    stream_id: str
    ready: asyncio.Future[dict[str, Any]]
    done: asyncio.Future[dict[str, Any]]
    expected_size: int
    expected_sha256: str
    phase: str = "begin_sent"
    chunk_ack: asyncio.Future[int] | None = None


@dataclass
class ArtifactReceiveState:
    stream_id: str
    artifact_id: str
    name: str
    media_type: str | None
    expected_size: int
    temp_path: Path
    final_path: Path
    handle: Any
    queue: asyncio.Queue[bytes | None] = field(
        default_factory=lambda: asyncio.Queue(maxsize=32)
    )
    received: int = 0
    digest: Any = field(default_factory=hashlib.sha256)
    end_size: int | None = None
    end_sha256: str | None = None
    next_sequence: int = 0
    phase: str = "receiving"
    task: asyncio.Task[Any] | None = None
    end_task: asyncio.Task[Any] | None = None
    abort_task: asyncio.Task[Any] | None = None
    commit_task: asyncio.Task[Any] | None = None


@dataclass
class InputTransferWaiter:
    """Sender-side state for one client file pushed into a peer input stage."""

    stream_id: str
    ready: asyncio.Future[dict[str, Any]]
    done: asyncio.Future[dict[str, Any]]
    expected_size: int
    expected_sha256: str
    phase: str = "begin_sent"
    chunk_ack: asyncio.Future[int] | None = None


@dataclass
class InputReceiveState:
    """Receiver-side state for one staged client file on the MCP host."""

    stream_id: str
    input_id: str
    name: str
    media_type: str | None
    expected_size: int
    temp_path: Path
    final_path: Path
    handle: Any
    queue: asyncio.Queue[bytes | None] = field(
        default_factory=lambda: asyncio.Queue(maxsize=32)
    )
    received: int = 0
    digest: Any = field(default_factory=hashlib.sha256)
    end_sha256: str | None = None
    next_sequence: int = 0
    phase: str = "receiving"
    task: asyncio.Task[Any] | None = None
    end_task: asyncio.Task[Any] | None = None
    abort_task: asyncio.Task[Any] | None = None
    commit_task: asyncio.Task[Any] | None = None


@dataclass
class ArtifactReceiveStateV2:
    """Receiver state for one artifacts/2 delivery (durable + resumable).

    The destination is content-addressed (``.mcp-artifacts/<sha256>/<name>``),
    partials are journaled with a random resume token, and a parked state keeps
    exactly the durably acked prefix so a resumed attempt never duplicates.
    """

    stream_id: str
    artifact_id: str
    name: str
    media_type: str | None
    expected_size: int
    sha256: str
    token: str
    inbox: Path
    content_dir: Path
    partial_path: Path
    handle: Any
    queue: asyncio.Queue[tuple[int, bytes] | None] = field(
        default_factory=lambda: asyncio.Queue(maxsize=32)
    )
    digest: Any = field(default_factory=hashlib.sha256)
    received: int = 0
    acked_bytes: int = 0
    next_sequence: int = 0
    phase: str = "receiving"
    task: asyncio.Task[Any] | None = None
    end_task: asyncio.Task[Any] | None = None
    abort_task: asyncio.Task[Any] | None = None
    commit_task: asyncio.Task[Any] | None = None
    #: True once file/journal cleanup ran, so abort/discard stay exactly-once
    #: even when several owners (abort path, worker error path) race.
    discarded: bool = False


class SharedBackend:
    """One generation-safe JSON-RPC backend shared by logical MCP clients."""

    def __init__(self, node: "BridgeNode", target: str, entry: dict[str, Any]):
        self.node = node
        self.target = target
        self.entry = entry
        self.process_config = dict(entry.get("process") or {})
        self.state = "exited"
        self.generation = 0
        self.state_history: list[tuple[str, int]] = [(self.state, self.generation)]
        #: Operator lifecycle drain armed for this backend (also mirrored in the
        #: node's per-registration desired state so it survives generations).
        self.drain_requested = False
        self.process: asyncio.subprocess.Process | None = None
        self.windows_job: _WindowsProcessJob | None = None
        self.clients: dict[str, SharedBackendClient] = {}
        self.lifecycle_lock = asyncio.Lock()
        self.stdin_lock = asyncio.Lock()
        self.start_future: asyncio.Task[None] | None = None
        self.stop_future: asyncio.Task[None] | None = None
        self.last_stop_phases: list[dict[str, Any]] = []
        self.request_queue: asyncio.Queue[RoutedRequest | None] = asyncio.Queue()
        self.pending_by_backend_id: dict[str, RoutedRequest] = {}
        self.pending_by_client_id: dict[tuple[str, str], RoutedRequest] = {}
        self.progress_tokens: dict[str, tuple[str, Any]] = {}
        self.server_requests: dict[tuple[str, str], Any] = {}
        self.current_request: RoutedRequest | None = None
        self.initialize_template: dict[str, Any] | None = None
        self.initialize_pending = False
        self.initialize_event = asyncio.Event()
        self.initialized_forwarded = False
        #: The physical backend's actual negotiated legacy revision once its
        #: initialize result has been observed and validated this generation
        #: (None until then).  Also the identity it reported, if any.
        self.backend_negotiated_version: str | None = None
        self.backend_server_info: dict[str, Any] | None = None
        # A lifecycle restart may replace only the physical backend generation
        # while every Agent-facing logical stream remains attached.  Input
        # consumers wait on this gate instead of observing a transient EOF.
        self.restart_in_progress = False
        self.restart_ready = asyncio.Event()
        self.restart_ready.set()
        self.recovery_failures = 0
        self.running_since: float | None = None
        # Exactly one wait task coordinates a crash-recovery episode.  Wait
        # tasks belonging to failed replacement generations reap only their
        # own generation and leave retry/stream policy to this owner.
        self.recovery_task: asyncio.Task[Any] | None = None
        self.lease_owner: str | None = None
        self.worker_task: asyncio.Task[Any] | None = None
        self.stdout_task: asyncio.Task[Any] | None = None
        self.stderr_task: asyncio.Task[Any] | None = None
        self.wait_task: asyncio.Task[Any] | None = None
        self.artifact_stage: Path | None = None
        self.artifact_token: str | None = None
        self.artifact_max_bytes = DEFAULT_MAX_ARTIFACT_BYTES

    def refresh_entry_if_exited(self, entry: dict[str, Any]) -> None:
        if self.state != "exited":
            return
        self.entry = entry
        self.process_config = dict(entry.get("process") or {})

    def _transition(self, state: str) -> None:
        self.state = state
        self.state_history.append((state, self.generation))
        if len(self.state_history) > 128:
            del self.state_history[:64]

    async def attach(self, stream: StreamState) -> None:
        while True:
            wait_for: asyncio.Task[None] | None = None
            wait_for_restart = False
            async with self.lifecycle_lock:
                if self.drain_requested or self.node._lifecycle_drain_armed(self.target):
                    raise DrainingError(
                        f"registered MCP {self.target} is draining and not "
                        "accepting new streams"
                    )
                if (
                    stream.stream_id not in self.node.streams
                    or stream.closed.is_set()
                ):
                    raise BridgeError(
                        "shared backend stream closed while waiting to attach"
                    )
                if self.restart_in_progress:
                    wait_for_restart = True
                elif self.state == "running":
                    self.clients[stream.stream_id] = SharedBackendClient(stream=stream)
                    stream.shared_backend = self
                    return
                elif self.state == "starting":
                    wait_for = self.start_future
                elif self.state == "stopping":
                    wait_for = self.stop_future
                elif self.state == "exited":
                    # A newly demand-started session gets a fresh recovery
                    # budget; failures from a prior abandoned session must not
                    # poison it.
                    self.recovery_failures = 0
                    self.generation += 1
                    self._transition("starting")
                    self.start_future = asyncio.create_task(
                        self._start_generation(self.generation)
                    )
                    wait_for = self.start_future
                else:
                    raise BridgeError(f"invalid shared backend state: {self.state}")
            if wait_for_restart:
                await self.restart_ready.wait()
                continue
            if wait_for is None:
                raise BridgeError("shared backend lifecycle future is unavailable")
            try:
                await asyncio.shield(wait_for)
            except asyncio.CancelledError:
                if wait_for is not None:
                    self.node._background(self._stop_after_cancelled_attach(wait_for))
                raise

    async def _stop_after_cancelled_attach(self, start: asyncio.Task[None]) -> None:
        await asyncio.gather(start, return_exceptions=True)
        await self._stop_if_unused("all clients disconnected during shared backend startup")

    async def _start_generation(
        self,
        generation: int,
        *,
        preserved_initialize: dict[str, Any] | None = None,
    ) -> None:
        environment = os.environ.copy()
        for key in ARTIFACT_ENV_KEYS:
            environment.pop(key, None)
        environment.update(self.entry.get("env", {}))
        artifact_config = self.entry.get("artifactDelivery", {"enabled": False})
        if bool(artifact_config.get("enabled")) and self.node.peer_artifacts:
            link_generation = self.node.link_generation or "disconnected"
            self.artifact_stage = (
                self.node.artifact_spool_root
                / link_generation
                / f"shared-{self.target}-{generation}"
            )
            self.artifact_stage.mkdir(parents=True, mode=0o700, exist_ok=False)
            try:
                self.artifact_stage.chmod(0o700)
            except OSError:
                pass
            self.artifact_token = secrets.token_urlsafe(32)
            self.artifact_max_bytes = min(
                int(artifact_config.get("maxBytes", DEFAULT_MAX_ARTIFACT_BYTES)),
                self.node.max_artifact_bytes,
            )
            environment.update(
                {
                    "WIN_WSL_MCP_BRIDGE_ARTIFACT_STAGE": str(self.artifact_stage),
                    "WIN_WSL_MCP_BRIDGE_ARTIFACT_TOKEN": self.artifact_token,
                    "WIN_WSL_MCP_BRIDGE_ARTIFACT_LOCAL_HOST": self.node.local_host,
                    "WIN_WSL_MCP_BRIDGE_ARTIFACT_LOCAL_PORT": str(self.node.local_port),
                    "WIN_WSL_MCP_BRIDGE_ARTIFACT_PROTOCOL": "artifacts/1",
                    "WIN_WSL_MCP_BRIDGE_ARTIFACT_PYTHON": sys.executable,
                    "WIN_WSL_MCP_BRIDGE_ARTIFACT_PUBLISHER": str(
                        Path(__file__).with_name("bridge_publisher.py").resolve()
                    ),
                }
            )
        kwargs: dict[str, Any] = {
            "cwd": self.entry.get("cwd") or None,
            "env": environment,
            "stdin": asyncio.subprocess.PIPE,
            "stdout": asyncio.subprocess.PIPE,
            "stderr": asyncio.subprocess.PIPE,
            "limit": MAX_SHARED_JSONRPC_BYTES,
        }
        if os.name == "nt":
            kwargs["creationflags"] = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
        else:
            kwargs["start_new_session"] = True
        process: asyncio.subprocess.Process | None = None
        windows_job: _WindowsProcessJob | None = None
        try:
            process = await asyncio.create_subprocess_exec(
                self.entry["command"],
                *self.entry.get("args", []),
                **kwargs,
            )
            if os.name == "nt":
                windows_job = _WindowsProcessJob(process.pid)
        except Exception:
            if windows_job is not None:
                windows_job.close()
            if process is not None:
                await self._terminate_process_tree(process, force=True)
            if self.artifact_stage is not None:
                shutil.rmtree(self.artifact_stage, ignore_errors=True)
            self.artifact_stage = None
            self.artifact_token = None
            async with self.lifecycle_lock:
                if self.generation == generation and self.state == "starting":
                    self._transition("stopping")
                    self._transition("exited")
            raise
        assert process is not None
        async with self.lifecycle_lock:
            if self.generation != generation or self.state != "starting":
                if windows_job is not None:
                    windows_job.close()
                await self._terminate_process_tree(process, force=True)
                raise BridgeError("shared backend generation changed during startup")
            self.process = process
            self.windows_job = windows_job
            if self.artifact_token is not None:
                self.node.shared_artifact_publishers[self.artifact_token] = self
            self.request_queue = asyncio.Queue()
            self.pending_by_backend_id.clear()
            self.pending_by_client_id.clear()
            self.progress_tokens.clear()
            self.server_requests.clear()
            self.current_request = None
            self.initialize_template = None
            self.initialize_pending = False
            self.initialize_event = asyncio.Event()
            self.initialized_forwarded = False
            self.backend_negotiated_version = None
            self.backend_server_info = None
            self.lease_owner = None
            self.worker_task = asyncio.create_task(self._request_worker(generation))
            self.stdout_task = asyncio.create_task(self._read_stdout(generation))
            self.stderr_task = asyncio.create_task(self._read_stderr(generation))
            for task in (self.worker_task, self.stdout_task, self.stderr_task):
                task.add_done_callback(
                    lambda done, current_generation=generation: self._generation_task_done(
                        done, current_generation
                    )
                )
            self.wait_task = asyncio.create_task(self._wait_process(generation))
            self.running_since = time.monotonic()
            self._transition("running")
        if preserved_initialize is not None:
            # The recovery/restart coordinator owns failure policy.  Do not call
            # generation-agnostic stop() or close preserved streams here: doing
            # so could race a newer generation and defeat bounded retry.
            await self._initialize_replacement_generation(preserved_initialize)

    async def _initialize_replacement_generation(
        self, initialize_message: dict[str, Any]
    ) -> None:
        """Initialize a replacement generation without involving MCP clients.

        Logical clients already completed their initialize exchanges.  A
        transparent restart therefore replays only the Bridge-owned canonical
        initialize request and the single initialized notification required by
        the new physical backend.
        """
        self.initialize_pending = True
        pending = await self._enqueue_request(
            None, initialize_message, is_initialize=True
        )
        await asyncio.wait_for(
            asyncio.shield(pending.done), timeout=SHARED_BACKEND_STOP_TIMEOUT_SECONDS
        )
        if self.initialize_template is None or "error" in self.initialize_template:
            raise BridgeError("replacement shared backend initialization failed")
        await self._write_message(
            {"jsonrpc": "2.0", "method": "notifications/initialized", "params": {}}
        )
        self.initialized_forwarded = True

    def _generation_task_done(
        self, task: asyncio.Task[Any], generation: int
    ) -> None:
        if task.cancelled() or generation != self.generation or self.state != "running":
            return
        error = task.exception()
        if error is None:
            return
        self.node.log(
            f"shared backend {self.target} generation {generation} task failed: "
            f"{type(error).__name__}: {error}"
        )
        self.node._background(
            self._stop_failed_generation(generation, "shared backend protocol task failed")
        )

    async def _stop_failed_generation(self, generation: int, reason: str) -> None:
        """Stop only the generation whose protocol task reported failure."""
        async with self.lifecycle_lock:
            if generation != self.generation or self.state != "running":
                return
            self._transition("stopping")
            self.stop_future = asyncio.create_task(
                self._stop_generation(generation, reason)
            )
            future = self.stop_future
        await future

    async def detach(self, stream_id: str) -> None:
        async with self.lifecycle_lock:
            self.clients.pop(stream_id, None)
        requests_settled = await self._cancel_client_requests(stream_id)
        if not requests_settled:
            await self.stop("disconnected client request did not cancel")
            return
        if self.lease_owner == stream_id:
            cleaned = await self._cleanup_lease(stream_id)
            if not cleaned:
                await self.stop("lease owner disconnected before cleanup completed")
                return
        await self._stop_if_unused("last client disconnected")

    async def _stop_if_unused(self, reason: str) -> None:
        async with self.lifecycle_lock:
            if self.clients or self.state != "running":
                return
            self._transition("stopping")
            self.stop_future = asyncio.create_task(
                self._stop_generation(self.generation, reason)
            )
            future = self.stop_future
        await future

    async def _cancel_client_requests(self, client_id: str) -> bool:
        # During crash reap/recovery there is no safe backend generation to
        # notify.  Settle ownership locally; never schedule a delayed generic
        # stop that could kill the replacement generation.
        if self.state != "running":
            for (owner, request_id), pending in list(self.pending_by_client_id.items()):
                if owner != client_id:
                    continue
                pending.cancelled = True
                pending.client_id = None
                self.pending_by_client_id.pop((owner, request_id), None)
                self.pending_by_backend_id.pop(pending.backend_id, None)
                if not pending.done.done():
                    pending.done.set_result({"cancelled": True})
            return True
        pending_items = [
            pending
            for (owner, _request_id), pending in list(self.pending_by_client_id.items())
            if owner == client_id
        ]
        active: RoutedRequest | None = None
        for pending in pending_items:
            key = (client_id, self._typed_id(pending.original_id))
            if pending.is_initialize:
                self.pending_by_client_id.pop(key, None)
                pending.client_id = None
                if self.current_request is pending:
                    active = pending
                continue
            if self.current_request is pending:
                active = pending
                try:
                    await self._write_message(
                        {
                            "jsonrpc": "2.0",
                            "method": "notifications/cancelled",
                            "params": {"requestId": pending.backend_id},
                        }
                    )
                except BridgeError:
                    return False
                continue
            pending.cancelled = True
            self.pending_by_backend_id.pop(pending.backend_id, None)
            self.pending_by_client_id.pop(key, None)
            if pending.progress_token is not None:
                self.progress_tokens.pop(pending.progress_token[0], None)
            if not pending.done.done():
                pending.done.set_result({"cancelled": True})
        if active is None:
            return True
        try:
            await asyncio.wait_for(asyncio.shield(active.done), timeout=5)
        except TimeoutError:
            return False
        return True

    async def stop(self, reason: str) -> None:
        while True:
            async with self.lifecycle_lock:
                if self.state == "exited":
                    return
                if self.state == "stopping":
                    future = self.stop_future
                elif self.state == "starting":
                    future = self.start_future
                else:
                    self._transition("stopping")
                    self.stop_future = asyncio.create_task(
                        self._stop_generation(self.generation, reason)
                    )
                    future = self.stop_future
            if future is None:
                return
            await asyncio.gather(future, return_exceptions=False)
            if self.state == "exited":
                return

    async def broadcast_tools_list_changed(self) -> int:
        """Notify every initialized logical client to refresh its native catalog."""
        notification = {
            "jsonrpc": "2.0",
            "method": "notifications/tools/list_changed",
            "params": {},
        }
        recipients = [
            client_id
            for client_id, client in list(self.clients.items())
            if client.protocol_version is not None
        ]
        outcomes = await asyncio.gather(
            *(self._send_client_message(client_id, notification) for client_id in recipients),
            return_exceptions=True,
        )
        return sum(not isinstance(outcome, Exception) for outcome in outcomes)

    async def restart_preserving_clients(self, reason: str) -> dict[str, Any]:
        """Replace the physical backend while retaining logical MCP streams."""
        async with self.lifecycle_lock:
            if self.restart_in_progress:
                raise BridgeError("restart_in_progress: shared backend is already restarting")
            if self.current_request is not None or self.pending_by_backend_id:
                raise BridgeError(
                    "active_requests: transparent restart requires an idle shared backend"
                )
            if self.initialize_template is None:
                raise BridgeError(
                    "backend_not_initialized: transparent restart requires an initialized backend"
                )
            self.restart_in_progress = True
            self.restart_ready.clear()
        if self.lease_owner is not None:
            try:
                cleaned = await self._cleanup_lease(self.lease_owner)
            except Exception:
                self.restart_in_progress = False
                self.restart_ready.set()
                raise
            if not cleaned:
                self.restart_in_progress = False
                self.restart_ready.set()
                raise BridgeError(
                    "lease_cleanup_failed: transparent restart could not release the backend resource"
                )
        initialize_message = {
            "jsonrpc": "2.0",
            "id": f"bridge-restart-init:{uuid.uuid4().hex}",
            "method": "initialize",
            "params": {
                "protocolVersion": SHARED_BACKEND_REQUEST_PROTOCOL_VERSION,
                "capabilities": dict(SHARED_BACKEND_CLIENT_CAPABILITIES),
                "clientInfo": {
                    "name": "win-wsl-mcp-bridge-shared-backend",
                    "version": SERVER_VERSION,
                },
            },
        }
        old_generation = self.generation
        preserved_clients = len(self.clients)
        try:
            await self.stop(reason)
            async with self.lifecycle_lock:
                if self.state != "exited" or self.generation != old_generation:
                    raise BridgeError("shared backend changed during transparent restart")
                if not self.clients:
                    return {
                        "stoppedGeneration": old_generation,
                        "startedGeneration": None,
                        "preservedClients": 0,
                    }
                self.refresh_entry_if_exited(self.node.registry.launch(self.target))
                self.generation += 1
                replacement_generation = self.generation
                self._transition("starting")
                self.start_future = asyncio.create_task(
                    self._start_generation(
                        replacement_generation,
                        preserved_initialize=initialize_message,
                    )
                )
                future = self.start_future
            await asyncio.shield(future)
            notified_clients = await self.broadcast_tools_list_changed()
            return {
                "stoppedGeneration": old_generation,
                "startedGeneration": replacement_generation,
                "preservedClients": preserved_clients,
                "toolsListChangedNotifiedClients": notified_clients,
            }
        finally:
            self.restart_in_progress = False
            self.restart_ready.set()

    async def _stop_generation(self, generation: int, reason: str) -> None:
        process = self.process
        phases: list[dict[str, Any]] = []
        self.last_stop_phases = phases
        phases.append({"phase": "drain", "outcome": "entered"})
        if process is not None and process.stdin is not None and not process.stdin.is_closing():
            process.stdin.close()
            phases.append({"phase": "protocol-close", "outcome": "stdin-closed"})
        else:
            phases.append({"phase": "protocol-close", "outcome": "not-applicable"})
        forced = False
        if process is not None:
            try:
                await asyncio.wait_for(
                    process.wait(), timeout=SHARED_BACKEND_STOP_TIMEOUT_SECONDS
                )
                phases.append({"phase": "wait", "outcome": "exited"})
            except TimeoutError:
                phases.append({"phase": "wait", "outcome": "timeout"})
                escalated = await self._terminate_process_tree(process, force=False)
                phases.append({"phase": "terminate", "outcome": "requested"})
                forced = escalated or process.returncode is None
        if process is not None and process.returncode is None:
            await self._terminate_process_tree(process, force=True)
            forced = True
        phases.append({"phase": "force-kill", "outcome": "used" if forced else "not-needed"})
        current = asyncio.current_task()
        tasks = [self.worker_task, self.stdout_task, self.stderr_task, self.wait_task]
        for task in tasks:
            if task is not None and task is not current and not task.done():
                task.cancel()
        await asyncio.gather(
            *(task for task in tasks if task is not None and task is not current),
            return_exceptions=True,
        )
        self._fail_all_pending(BridgeError(reason))
        if self.artifact_token is not None:
            self.node.shared_artifact_publishers.pop(self.artifact_token, None)
        if self.artifact_stage is not None:
            shutil.rmtree(self.artifact_stage, ignore_errors=True)
        self.artifact_stage = None
        self.artifact_token = None
        self.process = None
        self.lease_owner = None
        async with self.lifecycle_lock:
            if self.generation == generation:
                self._transition("exited")

    async def _terminate_process_tree(
        self,
        process: asyncio.subprocess.Process | None,
        *,
        force: bool,
    ) -> bool:
        """Terminate one shared-backend generation's process tree.

        ``force=False`` first asks the process group to exit gracefully and
        only escalates to a kill after a bounded wait.  Returns True when a
        forced kill was actually required because the graceful request did
        not stop the process, so lifecycle evidence can truthfully record
        ``force-kill: used`` even when the escalation happened inside this
        graceful path.
        """
        if process is None:
            return False
        if os.name == "nt":
            if force and self.windows_job is not None:
                self.windows_job.close()
                self.windows_job = None
            elif force:
                await asyncio.to_thread(
                    subprocess.run,
                    ["taskkill", "/PID", str(process.pid), "/T", "/F"],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    check=False,
                    timeout=10,
                )
            if process.returncode is None:
                try:
                    process.kill() if force else process.terminate()
                except ProcessLookupError:
                    pass
        else:
            try:
                os.killpg(process.pid, signal.SIGKILL if force else signal.SIGTERM)
            except ProcessLookupError:
                pass
        if process.returncode is not None:
            # The leader had already exited (e.g. crash cleanup of a defunct
            # generation).  The group kill above still reached any surviving
            # children, but no forced-kill escalation decision was required.
            return False
        try:
            await asyncio.wait_for(process.wait(), timeout=5)
        except TimeoutError:
            if not force:
                # The graceful request did not stop the group inside the
                # wait; escalate to a kill and report the force path.
                await self._terminate_process_tree(process, force=True)
            return True
        return force

    async def _wait_process(self, generation: int) -> None:
        assert self.process is not None
        process = self.process
        code = await process.wait()
        if self.state == "stopping" or generation != self.generation:
            return
        self.node.log(
            f"shared backend {self.target} generation {generation} exited with code {code}"
        )
        current_wait = asyncio.current_task()
        async with self.lifecycle_lock:
            if self.generation != generation or self.state not in {"starting", "running"}:
                return
            nested_replacement_failure = (
                self.recovery_task is not None
                and self.recovery_task is not current_wait
                and not self.recovery_task.done()
            )
            if self.recovery_task is None or self.recovery_task.done():
                self.recovery_task = current_wait
            self.stop_future = current_wait
            self.restart_in_progress = True
            self.restart_ready.clear()
            self._transition("stopping")
        await self._terminate_process_tree(process, force=True)
        io_tasks = [
            task
            for task in (self.stdout_task, self.stderr_task)
            if task is not None and not task.done()
        ]
        if io_tasks:
            _done, still_running = await asyncio.wait(io_tasks, timeout=5)
            for task in still_running:
                task.cancel()
            if still_running:
                await asyncio.gather(*still_running, return_exceptions=True)
        pending = list(self.pending_by_backend_id.values())
        incomplete_clients = {
            request.client_id
            for request in pending
            if request.is_initialize and request.client_id is not None
        }
        recoverable = bool(
            any(client_id not in incomplete_clients for client_id in self.clients)
            and self.initialize_template is not None
        )
        for request in pending:
            if request.client_id is not None and request.client_id in self.clients:
                await self._send_backend_lost_error(request, generation)
        # A logical MCP session whose initialize never completed cannot be
        # reconstructed transparently.  Close it rather than preserving a
        # stream whose caller would otherwise wait forever.
        for client_id in incomplete_clients:
            client = self.clients.pop(client_id, None)
            if client is not None:
                client.stream.shared_backend = None
                await self.node._close_stream(client.stream.stream_id, remote=False)
        self._fail_all_pending(BridgeError("shared backend exited"))
        worker = self.worker_task
        if worker is not None and not worker.done():
            worker.cancel()
            await asyncio.gather(worker, return_exceptions=True)
        if self.artifact_token is not None:
            self.node.shared_artifact_publishers.pop(self.artifact_token, None)
        if self.artifact_stage is not None:
            shutil.rmtree(self.artifact_stage, ignore_errors=True)
        self.artifact_stage = None
        self.artifact_token = None
        self.process = None
        self.lease_owner = None
        async with self.lifecycle_lock:
            if self.generation == generation and self.state == "stopping":
                self._transition("exited")
        if nested_replacement_failure:
            # The outer recovery coordinator awaits this generation's failed
            # start and owns the next bounded retry.  Do not close clients or
            # release the restart gate from this nested wait task.
            return
        if recoverable:
            recovered = await self._recover_after_exit(generation)
            if recovered:
                self.recovery_task = None
                return
        streams = [client.stream for client in self.clients.values()]
        self.clients.clear()
        for stream in streams:
            stream.shared_backend = None
            await self.node._close_stream(stream.stream_id, remote=False)
        self.restart_in_progress = False
        self.restart_ready.set()
        self.recovery_task = None

    async def _send_backend_lost_error(
        self, pending: RoutedRequest, generation: int
    ) -> None:
        if pending.client_id is None or pending.is_initialize:
            return
        await self._send_client_message(
            pending.client_id,
            {
                "jsonrpc": "2.0",
                "id": pending.original_id,
                "error": {
                    "code": -32001,
                    "message": "shared backend exited while the request was in flight",
                    "data": {
                        "code": "backend_generation_lost",
                        "retryable": True,
                        "outcomeUnknown": True,
                        "generation": generation,
                    },
                },
            },
        )

    async def _recover_after_exit(self, generation: int) -> bool:
        template = self.initialize_template
        if template is None:
            return False
        uptime = (
            time.monotonic() - self.running_since
            if self.running_since is not None
            else 0.0
        )
        if uptime >= SHARED_BACKEND_RECOVERY_RESET_SECONDS:
            self.recovery_failures = 0
        replacement = generation
        while self.recovery_failures < SHARED_BACKEND_RECOVERY_MAX_ATTEMPTS:
            if (
                not self.clients
                or self.drain_requested
                or self.node._lifecycle_drain_armed(self.target)
                or not self.restart_in_progress
            ):
                return False
            attempt = self.recovery_failures + 1
            delay = SHARED_BACKEND_RECOVERY_DELAYS_SECONDS[
                min(attempt - 1, len(SHARED_BACKEND_RECOVERY_DELAYS_SECONDS) - 1)
            ]
            if delay:
                await asyncio.sleep(delay)
            # Recheck after backoff: detach, drain, or explicit lifecycle stop
            # wins and must not be followed by a surprise respawn.
            if (
                not self.clients
                or self.drain_requested
                or self.node._lifecycle_drain_armed(self.target)
                or not self.restart_in_progress
            ):
                return False
            initialize_message = {
                "jsonrpc": "2.0",
                "id": f"bridge-recovery-init:{uuid.uuid4().hex}",
                "method": "initialize",
                "params": {
                    "protocolVersion": SHARED_BACKEND_REQUEST_PROTOCOL_VERSION,
                    "capabilities": dict(SHARED_BACKEND_CLIENT_CAPABILITIES),
                    "clientInfo": {
                        "name": "win-wsl-mcp-bridge-shared-backend",
                        "version": SERVER_VERSION,
                    },
                },
            }
            self.recovery_failures = attempt
            try:
                async with self.lifecycle_lock:
                    if self.state != "exited":
                        return self.state == "running"
                    self.refresh_entry_if_exited(self.node.registry.launch(self.target))
                    self.generation += 1
                    replacement = self.generation
                    self._transition("starting")
                    self.start_future = asyncio.create_task(
                        self._start_generation(
                            replacement, preserved_initialize=initialize_message
                        )
                    )
                    future = self.start_future
                await asyncio.shield(future)
                break
            except Exception as exc:
                self.node.log(
                    f"shared backend {self.target} automatic recovery attempt "
                    f"{attempt} failed: {type(exc).__name__}: {exc}"
                )
                await self._stop_failed_generation(
                    replacement, "replacement shared backend initialization failed"
                )
                if self.state == "stopping" and self.stop_future is not None:
                    await asyncio.gather(self.stop_future, return_exceptions=True)
                continue
        else:
            self.node.log(
                f"shared backend {self.target} automatic recovery exhausted after "
                f"generation {generation}"
            )
            return False
        self.node.log(
            f"shared backend {self.target} recovered generation {generation} as {replacement}"
        )
        await self.broadcast_tools_list_changed()
        # Publish the recovered generation before opening the input gate.  A
        # crash immediately after this point must begin a fresh episode rather
        # than be mistaken for a nested replacement failure.
        self.recovery_task = None
        self.restart_in_progress = False
        self.restart_ready.set()
        return True

    async def _read_stderr(self, generation: int) -> None:
        assert self.process is not None and self.process.stderr is not None
        while generation == self.generation:
            data = await self.process.stderr.read(BUFFER_SIZE)
            if not data:
                return
            text = data.decode("utf-8", errors="replace").rstrip()
            self.node.log(f"{self.target} shared stderr: {text}")

    async def _read_stdout(self, generation: int) -> None:
        assert self.process is not None and self.process.stdout is not None
        while generation == self.generation:
            line = await self.process.stdout.readline()
            if not line:
                return
            if len(line) > MAX_SHARED_JSONRPC_BYTES:
                raise BridgeError("shared backend JSON-RPC message exceeds limit")
            try:
                message = json.loads(line)
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise BridgeError("shared backend emitted invalid JSON-RPC") from exc
            if not isinstance(message, dict) or message.get("jsonrpc") != "2.0":
                raise BridgeError("shared backend emitted an invalid JSON-RPC object")
            await self._route_backend_message(message)

    async def consume_client_input(self, stream: StreamState) -> None:
        client = self.clients[stream.stream_id]
        try:
            while True:
                item = await stream.inbound.get()
                if item is None:
                    client.input_eof = True
                    self._finish_input_eof_if_idle(stream.stream_id)
                    return
                sequence, data = item
                client.input_buffer.extend(data)
                if len(client.input_buffer) > MAX_SHARED_JSONRPC_BYTES:
                    raise BridgeError("shared client JSON-RPC message exceeds inbound limit")
                # Acknowledge bounded transport receipt before an automatic
                # backend-recovery gate can wait. The bytes are already owned by
                # this node in the per-client buffer, so the peer need not time
                # out and tear down the otherwise preserved logical stream.
                await self.node._send_frame(
                    {
                        "type": "data_ok",
                        "stream": stream.stream_id,
                        "sequence": sequence,
                    }
                )
                while b"\n" in client.input_buffer:
                    raw, _, remainder = client.input_buffer.partition(b"\n")
                    client.input_buffer = bytearray(remainder)
                    if not raw.strip():
                        continue
                    try:
                        message = json.loads(raw)
                    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                        raise BridgeError("shared client sent invalid JSON-RPC") from exc
                    if not isinstance(message, dict) or message.get("jsonrpc") != "2.0":
                        raise BridgeError("shared client sent an invalid JSON-RPC object")
                    if self.restart_in_progress:
                        await self.restart_ready.wait()
                    await self._route_client_message(stream.stream_id, message)
        except (BridgeError, BrokenPipeError, ConnectionError, OSError) as exc:
            self.node.log(f"shared stream {stream.stream_id} input closed: {exc}")
            self.node._background(self.node._close_stream(stream.stream_id, remote=False))

    def _finish_input_eof_if_idle(self, client_id: str) -> None:
        client = self.clients.get(client_id)
        if client is None or not client.input_eof:
            return
        if any(owner == client_id for owner, _request_id in self.pending_by_client_id):
            return
        self.node._background(self.node._close_stream(client_id, remote=False))

    @staticmethod
    def _valid_rpc_id(value: Any) -> bool:
        return value is None or (
            isinstance(value, (str, int, float)) and not isinstance(value, bool)
        )

    @staticmethod
    def _typed_id(value: Any) -> str:
        return f"{type(value).__name__}:{json.dumps(value, ensure_ascii=False, sort_keys=True)}"

    def _next_backend_id(self, client_id: str | None) -> str:
        owner = client_id or "bridge"
        return f"bridge:{self.generation}:{owner}:{uuid.uuid4().hex}"

    async def _route_client_message(
        self, client_id: str, message: dict[str, Any]
    ) -> None:
        method = message.get("method")
        has_id = "id" in message
        if has_id and not self._valid_rpc_id(message.get("id")):
            raise BridgeError("shared client JSON-RPC id must be a scalar")
        if not isinstance(method, str):
            if has_id and ("result" in message or "error" in message):
                await self._route_client_response(client_id, message)
                return
            raise BridgeError("shared client JSON-RPC object has no method")
        if not has_id:
            if method == "notifications/initialized":
                if self.initialize_pending:
                    await self.initialize_event.wait()
                if (
                    not self.initialized_forwarded
                    and self.initialize_template is not None
                    and "result" in self.initialize_template
                ):
                    self.initialized_forwarded = True
                    await self._write_message(message)
                return
            if method == "notifications/cancelled":
                await self._route_cancellation(client_id, message)
                return
            await self._write_message(message)
            return
        if method == "initialize":
            await self._handle_initialize(client_id, message)
            return
        # P10 dual-era boundary: a request that declares its protocol version as
        # per-request ``_meta`` metadata belongs to the modern (2026-07-28)
        # era and is served by the tools-only modern router.  Requests without
        # that key (legacy clients, which carry no modern ``_meta``) continue
        # through the untouched initialize/session path below.
        version_present, _requested_version = bridge_protocol.protocol_version_entry(
            message
        )
        if version_present:
            await self._route_modern_client_message(client_id, message)
            return
        if method == "server/discover":
            # server/discover exists only in the modern (2026-07-28) era and
            # always carries per-request metadata.  A message without it is not
            # a modern-era request: answer locally so a dual-era client falls
            # back to the legacy initialize handshake instead of sending a
            # method the legacy physical backend cannot answer.
            await self._send_client_message(
                client_id,
                {
                    "jsonrpc": "2.0",
                    "id": message.get("id"),
                    "error": bridge_protocol.method_not_found_error(
                        method,
                        "server/discover is a modern-era method; use the legacy "
                        "initialize handshake for pre-2026-07-28 revisions",
                    ),
                },
            )
            return
        # P10 era discipline: a request that reaches this point carries no modern
        # per-request metadata and is not itself an initialize, so it can only be
        # legitimate as a legacy-era request from a logical client that completed
        # the legacy initialize handshake.  A logical client that has not
        # (``protocol_version`` still None) cannot be attributed to either era:
        # forwarding would let a modern client bypass the per-request metadata
        # gate simply by omitting the key, and a legacy client that skips the
        # mandatory initialize handshake must not reach the physical backend.
        # Fail closed with an error and never touch the backend; an explicit
        # legacy initialize on this logical client remains the supported way to
        # opt into legacy request handling.
        client = self.clients.get(client_id)
        if client is None:
            raise BridgeError(
                "shared backend client disconnected during request routing"
            )
        if client.protocol_version is None:
            await self._send_client_message(
                client_id,
                {
                    "jsonrpc": "2.0",
                    "id": message.get("id"),
                    "error": bridge_protocol.invalid_params_error(
                        "request carries no modern per-request protocol metadata "
                        "and the logical client has not completed the legacy "
                        "initialize handshake"
                    ),
                },
            )
            return
        if method == "tools/call":
            if await self._reject_fixed_state_mutation(client_id, message):
                return
            if await self._apply_lease_policy(client_id, message):
                return
        await self._enqueue_request(client_id, message)

    async def _handle_initialize(
        self, client_id: str, message: dict[str, Any]
    ) -> None:
        params = message.get("params")
        if not isinstance(params, dict):
            raise BridgeError("initialize params must be an object")
        protocol_version = params.get("protocolVersion")
        capabilities = params.get("capabilities", {})
        if not isinstance(protocol_version, str) or not isinstance(capabilities, dict):
            self._journal_initialize_negotiation("rejected", protocol_version, None)
            await self._send_initialize_error(
                client_id,
                message.get("id"),
                "protocolVersion must be a supported string and capabilities must be an object",
                protocol_version,
            )
            return
        outcome, negotiated_version = negotiate_mcp_protocol_version(protocol_version)
        if negotiated_version is None:
            self._journal_initialize_negotiation("rejected", protocol_version, None)
            await self._send_initialize_error(
                client_id,
                message.get("id"),
                "unsupported MCP protocol revision for shared backend virtualization",
                protocol_version,
            )
            return
        client = self.clients.get(client_id)
        if client is None:
            raise BridgeError("shared backend client disconnected during initialization")
        # The logical session operates under the negotiated revision.  An
        # accepted request (for example ``2025-11-25``) keeps the requested
        # revision: that acceptance is deliberately capability-subset because
        # the optional newer features (server tasks, tool
        # ``execution.taskSupport``, task-augmented sampling/elicitation, URL
        # elicitation) are only exercised when a party advertises them, and the
        # tools-only virtualizer never does.  A spec-compliant downgrade (for
        # example a future revision this runtime has not verified) steps the
        # client down to the newest verified revision instead of falsely
        # claiming the requested one.  The physical backend initialize always
        # requests SHARED_BACKEND_REQUEST_PROTOCOL_VERSION with the fixed
        # empty client capability profile, so a logical client's claims are
        # never forwarded and never solicit sampling, elicitation, or tasks
        # from the backend.
        client.protocol_version = negotiated_version
        client.capabilities = dict(capabilities)
        self._journal_initialize_negotiation(
            outcome, protocol_version, negotiated_version
        )
        if self.initialize_template is not None:
            await self._send_initialize_response(client_id, message.get("id"))
            return
        if self.initialize_pending:
            await self.initialize_event.wait()
            if self.initialize_template is None:
                raise BridgeError("shared backend initialization failed")
            await self._send_initialize_response(client_id, message.get("id"))
            return
        self.initialize_pending = True
        forwarded = dict(message)
        forwarded["params"] = {
            "protocolVersion": SHARED_BACKEND_REQUEST_PROTOCOL_VERSION,
            "capabilities": dict(SHARED_BACKEND_CLIENT_CAPABILITIES),
            "clientInfo": {
                "name": "win-wsl-mcp-bridge-shared-backend",
                "version": SERVER_VERSION,
            },
        }
        await self._enqueue_request(client_id, forwarded, is_initialize=True)

    def _journal_initialize_negotiation(
        self, outcome: str, requested: Any, negotiated: str | None
    ) -> None:
        """Record one metadata-only shared initialize negotiation outcome.

        The event row carries the registry ``target`` plus the requested and
        negotiated logical revisions and the physical backend revision; no
        request or response payload is ever stored.  ``backendVersion`` is the
        current generation's *observed* actual backend revision when the
        physical session has already been validated, otherwise the
        pre-observation baseline ``SHARED_MCP_PROTOCOL_VERSION``.  Outcomes are
        ``accepted``, ``downgraded``, and ``rejected`` and journaling failures
        never change the outcome the logical client observes.
        """
        node = self.node
        journal = getattr(node, "journal", None)
        if journal is None:
            return
        try:
            journal.record(
                side=node.side,
                category="shared-initialize",
                target=self.target,
                outcome=outcome,
                metadata={
                    "requestedVersion": (
                        requested if isinstance(requested, str) else None
                    ),
                    "negotiatedVersion": negotiated,
                    "backendVersion": (
                        self.backend_negotiated_version
                        if self.backend_negotiated_version is not None
                        else SHARED_MCP_PROTOCOL_VERSION
                    ),
                },
            )
        except (OSError, sqlite3.Error):
            pass

    async def _send_initialize_error(
        self,
        client_id: str,
        request_id: Any,
        message: str,
        requested_version: Any,
    ) -> None:
        await self._send_client_message(
            client_id,
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {
                    "code": -32602,
                    "message": message,
                    "data": {
                        "requestedProtocolVersion": requested_version,
                        "supportedProtocolVersions": sorted(
                            SHARED_COMPATIBLE_PROTOCOL_VERSIONS
                        ),
                        "backendProtocolVersion": (
                            self.backend_negotiated_version
                            if self.backend_negotiated_version is not None
                            else SHARED_MCP_PROTOCOL_VERSION
                        ),
                    },
                },
            },
        )

    async def _send_initialize_response(
        self, client_id: str, request_id: Any
    ) -> None:
        if self.initialize_template is None:
            raise BridgeError("shared backend initialization is unavailable")
        response = dict(self.initialize_template)
        response["id"] = request_id
        result = response.get("result")
        client = self.clients.get(client_id)
        if isinstance(result, dict) and client is not None and client.protocol_version:
            response_result = dict(result)
            response_result["protocolVersion"] = client.protocol_version
            # Capabilities are an intersection, never a verbatim replay: the
            # physical backend may run at a newer actual revision and advertise
            # families the tools-only virtualizer does not implement (tasks and
            # any other non-tools family are withheld from every logical
            # client, including 2025-11-25), and a family the negotiated
            # profile cannot represent must never leak.  The observed
            # capabilities remain the sole source; nothing is fabricated from
            # unobserved revision data.
            if "capabilities" in response_result:
                response_result["capabilities"] = (
                    bridge_protocol.project_legacy_server_capabilities(
                        client.protocol_version,
                        response_result.get("capabilities"),
                    )
                )
            response["result"] = response_result
        await self._send_client_message(client_id, response)

    async def _enqueue_request(
        self,
        client_id: str | None,
        message: dict[str, Any],
        *,
        is_initialize: bool = False,
        is_release: bool = False,
        modern: bool = False,
    ) -> RoutedRequest:
        original_id = message.get("id")
        if client_id is not None:
            client_key = (client_id, self._typed_id(original_id))
            if client_key in self.pending_by_client_id:
                raise BridgeError("client reused an active JSON-RPC request id")
        backend_id = self._next_backend_id(client_id)
        forwarded = dict(message)
        forwarded["id"] = backend_id
        progress_token: tuple[str, Any] | None = None
        params = forwarded.get("params")
        if client_id is not None and isinstance(params, dict):
            metadata = params.get("_meta")
            if isinstance(metadata, dict) and "progressToken" in metadata:
                original_progress = metadata["progressToken"]
                backend_progress = (
                    f"bridge-progress:{self.generation}:{client_id}:{uuid.uuid4().hex}"
                )
                forwarded_params = dict(params)
                forwarded_metadata = dict(metadata)
                forwarded_metadata["progressToken"] = backend_progress
                forwarded_params["_meta"] = forwarded_metadata
                forwarded["params"] = forwarded_params
                self.progress_tokens[backend_progress] = (client_id, original_progress)
                progress_token = (backend_progress, original_progress)
        pending = RoutedRequest(
            backend_id=backend_id,
            client_id=client_id,
            original_id=original_id,
            message=forwarded,
            done=asyncio.get_running_loop().create_future(),
            method=str(message.get("method")),
            is_initialize=is_initialize,
            is_release=is_release,
            progress_token=progress_token,
            modern=modern,
        )
        self.pending_by_backend_id[backend_id] = pending
        if client_id is not None:
            self.pending_by_client_id[(client_id, self._typed_id(original_id))] = pending
        await self.request_queue.put(pending)
        return pending

    async def _request_worker(self, generation: int) -> None:
        while generation == self.generation:
            pending = await self.request_queue.get()
            if pending is None:
                return
            if pending.cancelled:
                continue
            self.current_request = pending
            try:
                await self._write_message(pending.message)
                await pending.done
            finally:
                if self.current_request is pending:
                    self.current_request = None

    async def _write_message(self, message: dict[str, Any]) -> None:
        process = self.process
        if self.state != "running" or process is None or process.stdin is None:
            raise BridgeError("shared backend is not running")
        data = _json_bytes(message) + b"\n"
        if len(data) > MAX_SHARED_JSONRPC_BYTES:
            raise BridgeError("shared backend JSON-RPC input exceeds limit")
        async with self.stdin_lock:
            process.stdin.write(data)
            await process.stdin.drain()

    async def _route_backend_message(self, message: dict[str, Any]) -> None:
        method = message.get("method")
        if "id" in message and not self._valid_rpc_id(message.get("id")):
            raise BridgeError("shared backend JSON-RPC id must be a scalar")
        if isinstance(method, str):
            if "id" in message:
                await self._route_backend_request(message)
            elif method == "notifications/progress":
                params = message.get("params")
                token = params.get("progressToken") if isinstance(params, dict) else None
                route = self.progress_tokens.get(str(token))
                if route is not None:
                    client_id, original_token = route
                    forwarded = dict(message)
                    forwarded_params = dict(params)
                    forwarded_params["progressToken"] = original_token
                    forwarded["params"] = forwarded_params
                    await self._send_client_message(client_id, forwarded)
            elif method in {
                "notifications/tools/list_changed",
                "notifications/resources/list_changed",
                "notifications/prompts/list_changed",
            }:
                await asyncio.gather(
                    *(
                        self._send_client_message(client_id, message)
                        for client_id, client in list(self.clients.items())
                        if client.protocol_version is not None
                    ),
                    return_exceptions=True,
                )
            else:
                if self.current_request is not None and self.current_request.modern:
                    # Modern logging/subscriptions are not advertised. Only
                    # request-scoped progress has an implemented route above.
                    return
                client_id = (
                    self.current_request.client_id
                    if self.current_request is not None
                    and self.current_request.client_id in self.clients
                    else None
                )
                if client_id is None and self.lease_owner in self.clients:
                    client_id = self.lease_owner
                if client_id is not None:
                    await self._send_client_message(client_id, message)
            return
        backend_id = message.get("id")
        pending = self.pending_by_backend_id.pop(str(backend_id), None)
        if pending is None:
            raise BridgeError("shared backend returned an unknown response id")
        if pending.client_id is not None:
            self.pending_by_client_id.pop(
                (pending.client_id, self._typed_id(pending.original_id)), None
            )
        if pending.progress_token is not None:
            self.progress_tokens.pop(pending.progress_token[0], None)
        response = dict(message)
        response["id"] = pending.original_id
        if pending.is_initialize:
            self.initialize_template = dict(response)
            self.initialize_template.pop("id", None)
            # Observe and validate the physical session's actual negotiated
            # legacy revision (server-choose) before any waiter is released.
            # A valid result records the actual version; a modern/missing/
            # unusable answer replaces the template with a controlled error so
            # the session fails closed and is never assigned a fabricated
            # revision.
            self._observe_backend_initialize()
            self.initialize_pending = False
            self.initialize_event.set()
        if pending.is_release and self._release_succeeded(message):
            self.lease_owner = None
        if pending.client_id is not None and pending.client_id in self.clients:
            if pending.is_initialize:
                await self._send_initialize_response(
                    pending.client_id, pending.original_id
                )
            else:
                if not pending.modern:
                    # Four frozen legacy tools profiles: a physical backend
                    # that negotiated a higher legacy revision may answer with
                    # fields the logical client's own revision cannot carry
                    # (annotations/title/outputSchema/icons, structuredContent).
                    # Project tools responses to the client's negotiated
                    # profile at emission; other methods keep exact bytes.
                    client = self.clients[pending.client_id]
                    version = (
                        client.protocol_version if client is not None else None
                    )
                    if version is not None and isinstance(
                        response.get("result"), dict
                    ):
                        try:
                            if pending.method == "tools/list":
                                response["result"] = (
                                    bridge_protocol.project_legacy_tools_list(
                                        version, response["result"]
                                    )
                                )
                            elif pending.method == "tools/call":
                                response["result"] = (
                                    bridge_protocol.project_legacy_tools_call_result(
                                        version, response["result"]
                                    )
                                )
                        except ValueError as exc:
                            # Fail closed: never hand a malformed or
                            # unprojectable backend result to a legacy client
                            # as a success.
                            response = {
                                "jsonrpc": "2.0",
                                "id": pending.original_id,
                                "error": {
                                    "code": -32603,
                                    "message": str(exc),
                                },
                            }
                if pending.modern:
                    # Era isolation: only a modern-era request is projected.
                    # Legacy clients keep the exact legacy response bytes.
                    if "error" in response:
                        # The modern envelope forbids retired MCP codes
                        # (-32002/-32042) that a legacy backend may still emit;
                        # normalize them without altering legacy passthrough.
                        response["error"] = bridge_protocol.normalize_backend_error(
                            response["error"]
                        )
                    elif isinstance(response.get("result"), dict):
                        try:
                            response["result"] = bridge_protocol.shape_modern_result(
                                pending.method,
                                response["result"],
                                self._modern_server_info(),
                            )
                        except ValueError as exc:
                            # Fail closed: never hand a malformed or unprojectable
                            # result to a modern client as a success.
                            response = {
                                "jsonrpc": "2.0",
                                "id": pending.original_id,
                                "error": {
                                    "code": -32603,
                                    "message": str(exc),
                                },
                            }
                    else:
                        response = {
                            "jsonrpc": "2.0",
                            "id": pending.original_id,
                            "error": {
                                "code": -32603,
                                "message": (
                                    "shared backend returned a malformed result "
                                    "for a modern request"
                                ),
                            },
                        }
                await self._send_client_message(pending.client_id, response)
        if not pending.done.done():
            pending.done.set_result(message)
        if pending.client_id is not None:
            self._finish_input_eof_if_idle(pending.client_id)

    @staticmethod
    def _required_client_capability(method: str) -> str | None:
        if method.startswith("sampling/"):
            return "sampling"
        if method.startswith("roots/"):
            return "roots"
        if method.startswith("elicitation/"):
            return "elicitation"
        return None

    def _client_supports_backend_method(self, client_id: str, method: str) -> bool:
        client = self.clients.get(client_id)
        if client is None:
            return False
        required = self._required_client_capability(method)
        return required is None or required in client.capabilities

    async def _route_backend_request(self, message: dict[str, Any]) -> None:
        method = str(message.get("method"))
        if self.current_request is not None and self.current_request.modern:
            await self._write_message({
                "jsonrpc": "2.0", "id": message.get("id"),
                "error": {"code": -32601,
                          "message": "backend requests are unsupported on the modern tools-only projection"},
            })
            return
        client_id = (
            self.current_request.client_id
            if self.current_request is not None
            and self.current_request.client_id in self.clients
            else None
        )
        if client_id is not None and not self._client_supports_backend_method(
            client_id, method
        ):
            client_id = None
        if (
            client_id is None
            and self.current_request is None
            and self.lease_owner in self.clients
            and self._client_supports_backend_method(self.lease_owner, method)
        ):
            client_id = self.lease_owner
        if client_id is None and self.current_request is None:
            capable = [
                owner
                for owner in self.clients
                if self._client_supports_backend_method(owner, method)
            ]
            if len(capable) == 1:
                client_id = capable[0]
        if client_id is None:
            required = self._required_client_capability(method)
            await self._write_message(
                {
                    "jsonrpc": "2.0",
                    "id": message.get("id"),
                    "error": {
                        "code": -32000,
                        "message": "no compatible shared backend client available",
                        "data": {"requiredClientCapability": required},
                    },
                }
            )
            return
        client_request_id = f"bridge-server:{self.generation}:{uuid.uuid4().hex}"
        self.server_requests[
            (client_id, self._typed_id(client_request_id))
        ] = message.get("id")
        forwarded = dict(message)
        forwarded["id"] = client_request_id
        await self._send_client_message(client_id, forwarded)

    async def _route_client_response(
        self, client_id: str, message: dict[str, Any]
    ) -> None:
        key = (client_id, self._typed_id(message.get("id")))
        backend_id = self.server_requests.pop(key, None)
        if backend_id is None:
            request_id = message.get("id")
            if isinstance(request_id, str) and request_id.startswith("bridge-server:"):
                parts = request_id.split(":", 3)
                if len(parts) >= 3 and parts[1].isdigit() and (
                    self.restart_in_progress or int(parts[1]) < self.generation
                ):
                    return
            raise BridgeError("client returned an unknown server-request id")
        forwarded = dict(message)
        forwarded["id"] = backend_id
        await self._write_message(forwarded)

    async def _route_cancellation(
        self, client_id: str, message: dict[str, Any]
    ) -> None:
        params = message.get("params")
        request_id = params.get("requestId") if isinstance(params, dict) else None
        pending = self.pending_by_client_id.get(
            (client_id, self._typed_id(request_id))
        )
        if pending is None:
            return
        if self.current_request is pending:
            forwarded = dict(message)
            forwarded_params = dict(params)
            forwarded_params["requestId"] = pending.backend_id
            forwarded["params"] = forwarded_params
            await self._write_message(forwarded)
            return
        pending.cancelled = True
        self.pending_by_backend_id.pop(pending.backend_id, None)
        self.pending_by_client_id.pop(
            (client_id, self._typed_id(pending.original_id)), None
        )
        if pending.progress_token is not None:
            self.progress_tokens.pop(pending.progress_token[0], None)
        await self._send_client_message(
            client_id,
            {
                "jsonrpc": "2.0",
                "id": pending.original_id,
                "error": {"code": -32800, "message": "Request cancelled"},
            },
        )
        if not pending.done.done():
            pending.done.set_result({"cancelled": True})

    def _tool_call(self, message: dict[str, Any]) -> tuple[str | None, dict[str, Any]]:
        params = message.get("params")
        if not isinstance(params, dict):
            return None, {}
        arguments = params.get("arguments")
        return (
            params.get("name") if isinstance(params.get("name"), str) else None,
            arguments if isinstance(arguments, dict) else {},
        )

    async def _reject_fixed_state_mutation(
        self,
        client_id: str,
        message: dict[str, Any],
        *,
        modern: bool = False,
    ) -> bool:
        shared_state = self.process_config.get("sharedState")
        if not isinstance(shared_state, dict):
            return False
        tool_name, _arguments = self._tool_call(message)
        if tool_name not in shared_state.get("rejectTools", []):
            return False
        await self._send_tool_policy_error(
            client_id,
            message.get("id"),
            "shared_view_fixed",
            "This shared backend uses a fixed tool view; per-client view changes are disabled.",
            modern=modern,
        )
        return True

    def _is_release_call(self, tool_name: str | None, arguments: dict[str, Any]) -> bool:
        lease = self.process_config.get("clientLease")
        if not isinstance(lease, dict) or tool_name != lease.get("releaseTool"):
            return False
        expected = lease.get("releaseArguments", {})
        return all(arguments.get(key) == value for key, value in expected.items())

    async def _apply_lease_policy(
        self,
        client_id: str,
        message: dict[str, Any],
        *,
        modern: bool = False,
    ) -> bool:
        lease = self.process_config.get("clientLease")
        if not isinstance(lease, dict):
            return False
        tool_name, arguments = self._tool_call(message)
        if tool_name is None:
            return False
        is_release = self._is_release_call(tool_name, arguments)
        acquires = any(
            fnmatch.fnmatchcase(tool_name, pattern)
            for pattern in lease.get("toolPatterns", [])
        )
        if not acquires and not is_release:
            return False
        if self.lease_owner is None and not is_release:
            self.lease_owner = client_id
        elif self.lease_owner != client_id:
            await self._send_tool_policy_error(
                client_id,
                message.get("id"),
                "client_lease_busy",
                "The shared backend resource is owned by another client.",
                modern=modern,
            )
            return True
        await self._enqueue_request(
            client_id,
            message,
            is_release=is_release,
            modern=modern,
        )
        return True

    async def _cleanup_lease(self, owner: str) -> bool:
        lease = self.process_config.get("clientLease")
        if not isinstance(lease, dict) or self.lease_owner != owner:
            self.lease_owner = None
            return True
        cleanup = {
            "jsonrpc": "2.0",
            "id": f"bridge-cleanup:{uuid.uuid4().hex}",
            "method": "tools/call",
            "params": {
                "name": lease["releaseTool"],
                "arguments": dict(lease.get("releaseArguments", {})),
            },
        }
        try:
            pending = await self._enqueue_request(None, cleanup, is_release=True)
            await asyncio.wait_for(
                pending.done,
                timeout=float(lease.get("cleanupTimeoutSeconds", 15)),
            )
        except (BridgeError, TimeoutError):
            return False
        return self.lease_owner is None

    def _release_succeeded(self, response: dict[str, Any]) -> bool:
        lease = self.process_config.get("clientLease")
        if not isinstance(lease, dict) or "result" not in response:
            return False
        value: Any = response["result"]
        for part in lease.get("releasedResultPath", []):
            if not isinstance(value, dict) or part not in value:
                return False
            value = value[part]
        return value == lease.get("releasedResultValue", True)

    async def _send_tool_policy_error(
        self,
        client_id: str,
        request_id: Any,
        code: str,
        message: str,
        *,
        modern: bool = False,
    ) -> None:
        detail = {"code": code, "retryable": code == "client_lease_busy"}
        result: dict[str, Any] = {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps(
                        {"error": detail, "message": message},
                        ensure_ascii=False,
                        separators=(",", ":"),
                    ),
                }
            ],
            "structuredContent": {"error": detail, "message": message},
            "isError": True,
            "_meta": {"io.win-wsl-mcp-bridge/runtime": detail},
        }
        if modern:
            # Policy rejections are tool results: give them the modern result
            # envelope on the modern-facing side only.
            result = bridge_protocol.shape_modern_tool_result(
                result, self._modern_server_info()
            )
        await self._send_client_message(
            client_id,
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": result,
            },
        )

    async def _send_client_message(
        self, client_id: str, message: dict[str, Any]
    ) -> None:
        client = self.clients.get(client_id)
        if client is None:
            return
        try:
            await self.node._send_jsonrpc_to_stream(client.stream, message)
        except BridgeError:
            self.node._background(
                self.node._close_stream(client.stream.stream_id, remote=False)
            )

    def artifact_stream(self) -> StreamState | None:
        pending = self.current_request
        if pending is None or pending.client_id is None:
            return None
        client = self.clients.get(pending.client_id)
        return client.stream if client is not None else None

    def _modern_server_info(self) -> dict[str, Any]:
        """Agent-facing server identity carried on modern result ``_meta``."""
        return {
            "name": "win-wsl-mcp-bridge",
            "version": SERVER_VERSION,
            "description": (
                "Bridge-owned shared MCP projection; modern surface is tools-only."
            ),
        }

    def _bridge_session_initialize(self) -> dict[str, Any]:
        """One Bridge-owned physical legacy initialize request.

        A modern client never runs the legacy ``initialize`` handshake, but the
        physical backend is a legacy MCP that expects its client (the bridge)
        to initialize first.  This request uses exactly the same profile as
        logical-client-triggered initialization: the canonical legacy request
        revision (SHARED_BACKEND_REQUEST_PROTOCOL_VERSION) plus an empty client
        capability set.  The backend answers with its own supported verified
        legacy revision at or below the request (server-choose).
        """
        return {
            "jsonrpc": "2.0",
            "id": f"bridge-session-init:{uuid.uuid4().hex}",
            "method": "initialize",
            "params": {
                "protocolVersion": SHARED_BACKEND_REQUEST_PROTOCOL_VERSION,
                "capabilities": dict(SHARED_BACKEND_CLIENT_CAPABILITIES),
                "clientInfo": {
                    "name": "win-wsl-mcp-bridge-shared-backend",
                    "version": SERVER_VERSION,
                },
            },
        }

    async def _ensure_backend_session(self) -> None:
        """Bring up the single physical legacy session exactly once per generation.

        Uses the same single-flight discipline as logical-client initialization
        (``initialize_pending`` set before the enqueue with no await in between,
        then ``initialize_event`` released from the response path), so concurrent
        modern requests and legacy initializations can never double-initialize.
        Afterwards a single ``notifications/initialized`` is forwarded.  Only the
        initialize handshake may be replayed across a recovery restart; business
        requests are never replayed by the bridge.
        """
        if self.initialize_template is not None:
            return
        if not self.initialize_pending:
            self.initialize_pending = True
            try:
                pending = await self._enqueue_request(
                    None, self._bridge_session_initialize(), is_initialize=True
                )
                try:
                    await asyncio.wait_for(
                        asyncio.shield(pending.done),
                        timeout=SHARED_BACKEND_STOP_TIMEOUT_SECONDS,
                    )
                except asyncio.TimeoutError as exc:
                    raise BridgeError(
                        "shared backend session bootstrap timed out"
                    ) from exc
            except BridgeError:
                self.initialize_pending = False
                raise
            except Exception as exc:
                self.initialize_pending = False
                raise BridgeError(
                    "shared backend session bootstrap failed"
                ) from exc
            if self.initialize_template is None or "error" in self.initialize_template:
                self.initialize_pending = False
                raise BridgeError("shared backend initialization failed")
            if not self.initialized_forwarded:
                await self._write_message(
                    {
                        "jsonrpc": "2.0",
                        "method": "notifications/initialized",
                        "params": {},
                    }
                )
                self.initialized_forwarded = True
            return
        # Another initialization is already in flight (modern bootstrap racing
        # a logical legacy initialize, or two first modern requests): wait for
        # its outcome instead of enqueueing a second initialize.
        await self.initialize_event.wait()
        if self.initialize_template is None or "error" in self.initialize_template:
            raise BridgeError("shared backend initialization failed")

    def _observe_backend_initialize(self) -> None:
        """Validate the just-observed physical backend initialize result.

        Called exactly when ``initialize_template`` is (re)captured for a
        generation.  A result whose ``protocolVersion`` is a verified frozen
        legacy revision records the generation's actual negotiated backend
        revision (and optional server identity) and journals one
        ``shared-backend-session`` ``observed`` row.  A result that carries a
        modern revision, a missing/unusable protocolVersion, or a backend error
        never fabricates a revision: the template is replaced (for a usable-
        shaped but invalid result) with a controlled error so every waiter --
        the modern bootstrap and any logical legacy client -- observes a clean
        failure, and a ``shared-backend-session`` ``rejected`` row is journaled.
        Journaling failures never change the outcome clients observe.
        """
        requested = SHARED_BACKEND_REQUEST_PROTOCOL_VERSION
        template = self.initialize_template
        if not isinstance(template, dict):
            return
        if "error" in template:
            # The backend itself refused the initialize (for example a
            # modern-only backend answering method-not-found): the error is
            # already fail-closed downstream and needs no revision record.
            self._journal_backend_session(
                "rejected", requested, None, "backend_error"
            )
            return
        result = template.get("result")
        if not isinstance(result, dict):
            self._journal_backend_session(
                "rejected", requested, None, "missing_result"
            )
            self.initialize_template = self._backend_session_error_template(
                requested, None, "missing initialize result"
            )
            return
        version = result.get("protocolVersion")
        if not isinstance(version, str) or version not in SHARED_COMPATIBLE_PROTOCOL_VERSIONS:
            observed = version if isinstance(version, str) else None
            self._journal_backend_session(
                "rejected", requested, observed, "unusable_protocol_version"
            )
            self.initialize_template = self._backend_session_error_template(
                requested, observed, "unusable backend protocol revision"
            )
            return
        self.backend_negotiated_version = version
        server_info = result.get("serverInfo")
        self.backend_server_info = (
            dict(server_info) if isinstance(server_info, dict) else None
        )
        self._journal_backend_session("observed", requested, version, None)

    def _backend_session_error_template(
        self, requested: str, observed: Any, reason: str
    ) -> dict[str, Any]:
        """A controlled error template for an unusable backend session.

        Every logical waiter treats an error-carrying initialize template as a
        clean session failure (legacy replay sends the error; the modern
        bootstrap raises); a fabricated success is never handed out.
        """
        return {
            "jsonrpc": "2.0",
            "error": {
                "code": -32603,
                "message": f"shared backend session unavailable: {reason}",
                "data": {
                    "code": "backend_protocol_version_unusable",
                    "requested": requested,
                    "observed": observed,
                    "supportedProtocolVersions": sorted(
                        SHARED_COMPATIBLE_PROTOCOL_VERSIONS
                    ),
                },
            },
        }

    def _journal_backend_session(
        self,
        outcome: str,
        requested: Any,
        observed: str | None,
        detail: str | None,
    ) -> None:
        """Record one metadata-only physical backend session observation.

        ``observed`` is the actual negotiated revision the backend answered.
        A rejected session never negotiates, so its rows carry None (no
        fabricated revision is ever stored); ``detail`` preserves the reason.
        No request or response payload is ever stored.
        """
        node = self.node
        journal = getattr(node, "journal", None)
        if journal is None:
            return
        try:
            if outcome == "rejected":
                observed = None
            metadata: dict[str, Any] = {
                "requestedVersion": (
                    requested if isinstance(requested, str) else None
                ),
                "negotiatedVersion": observed,
                "backendVersion": observed,
            }
            if detail is not None:
                metadata["detail"] = detail
            journal.record(
                side=node.side,
                category="shared-backend-session",
                target=self.target,
                generation=self.generation,
                outcome=outcome,
                metadata=metadata,
            )
        except (OSError, sqlite3.Error):
            pass

    def _backend_initialize_result(self) -> dict[str, Any] | None:
        """The physical backend's initialize ``result`` once observed, else None."""
        template = self.initialize_template
        if template is None:
            return None
        result = template.get("result")
        return result if isinstance(result, dict) else None

    def _backend_offers_tools(self) -> bool | None:
        """Whether the observed physical backend advertised the tools capability.

        Returns None before any backend initialize has been observed.  Presence
        of a ``tools`` object inside the backend's own capabilities is the only
        basis for advertising tools to modern clients: capability claims are an
        intersection with the observed backend, never invented.
        """
        result = self._backend_initialize_result()
        if result is None:
            return None
        capabilities = result.get("capabilities")
        if not isinstance(capabilities, dict):
            return False
        return isinstance(capabilities.get("tools"), dict)

    def _backend_instructions(self) -> str | None:
        """The physical backend's initialize ``instructions`` verbatim, if any.

        Downstream instructions are preserved (never replaced by projection
        boilerplate); None is returned when the backend supplied none.
        """
        result = self._backend_initialize_result()
        if result is None:
            return None
        text = result.get("instructions")
        return text if isinstance(text, str) and text else None

    async def _send_modern_backend_unavailable(
        self, client_id: str, request_id: Any, exc: BridgeError
    ) -> None:
        """Answer one modern request with the spec-shaped session-unavailable error.

        The request was dropped before any backend side effect, so
        ``outcomeUnknown`` is honestly False.
        """
        await self._send_client_message(
            client_id,
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {
                    "code": -32001,
                    "message": "shared backend session unavailable",
                    "data": {
                        "code": "backend_session_unavailable",
                        "retryable": True,
                        "outcomeUnknown": False,
                        "detail": str(exc),
                    },
                },
            },
        )

    async def _route_modern_client_message(
        self, client_id: str, message: dict[str, Any]
    ) -> None:
        """Handle one modern-era (2026-07-28) request on the tools-only surface.

        Modern-era dispatch is per request: there is no session state to
        negotiate, so every request carries its protocol version and client
        capabilities in ``_meta`` and is validated independently.  Discover
        bootstraps the single physical legacy generation once (a Bridge-owned
        initialize) and answers from what that backend actually offered:
        capabilities and instructions are observed downstream, never invented.
        tools/list and tools/call are forwarded only when the observed backend
        advertised the tools capability; every other method answers ``-32601``
        (method not part of the advertised tools-only projection).  Unsupported
        or malformed probes never touch the backend.  Nothing here ever forwards
        modern client capabilities or identity claims to the legacy backend.
        """
        request_id = message.get("id")
        problems = bridge_protocol.validate_modern_request_meta(message)
        if problems:
            await self._send_client_message(
                client_id,
                {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "error": bridge_protocol.invalid_params_error(
                        "; ".join(problems)
                    ),
                },
            )
            return
        _present, version = bridge_protocol.protocol_version_entry(message)
        if not isinstance(version, str) or not bridge_protocol.is_supported_modern_version(
            version
        ):
            # Well-formed but unsupported requested revision: answer exactly the
            # spec-defined -32022 with the supported list; never touch backend.
            await self._send_client_message(
                client_id,
                {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "error": bridge_protocol.unsupported_version_error(version),
                },
            )
            return
        method = message.get("method")
        if method == "server/discover":
            # Discover is observation-backed: a supported modern discover
            # bootstraps the backend once and reports the intersection of what
            # this projection can serve and what the physical backend actually
            # offered (tools capability, downstream instructions verbatim).
            try:
                await self._ensure_backend_session()
            except BridgeError as exc:
                await self._send_modern_backend_unavailable(
                    client_id, request_id, exc
                )
                return
            await self._send_client_message(
                client_id,
                {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "result": bridge_protocol.discover_result(
                        self._modern_server_info(),
                        backend_tools=self._backend_offers_tools(),
                        backend_instructions=self._backend_instructions(),
                    ),
                },
            )
            return
        if method not in bridge_protocol.FORWARDED_TOOLS_ONLY_METHODS:
            await self._send_client_message(
                client_id,
                {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "error": bridge_protocol.method_not_found_error(
                        method,
                        "method is not part of the advertised tools-only modern "
                        "projection",
                    ),
                },
            )
            return
        if bridge_protocol.has_mrtr_retry_fields(message):
            await self._send_client_message(
                client_id,
                {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "error": bridge_protocol.invalid_params_error(
                        "unexpected inputResponses/requestState retry fields on a "
                        "request this server never returned as input_required"
                    ),
                },
            )
            return
        params = message.get("params")
        sanitized_params = bridge_protocol.sanitize_params_for_legacy(params)
        if sanitized_params is None:
            await self._send_client_message(
                client_id,
                {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "error": bridge_protocol.invalid_params_error(
                        "params must be an object"
                    ),
                },
            )
            return
        sanitized = dict(message)
        sanitized["params"] = sanitized_params
        if method == "tools/call":
            # A fixed-state rejection needs no backend session: refuse locally
            # before the Bridge-owned initialize bootstrap, so a policy veto can
            # never be charged against the physical generation.
            if await self._reject_fixed_state_mutation(
                client_id, sanitized, modern=True
            ):
                return
        try:
            await self._ensure_backend_session()
        except BridgeError as exc:
            # No business call executed: the request was dropped before any
            # backend side effect, so outcomeUnknown is honestly False.
            await self._send_modern_backend_unavailable(client_id, request_id, exc)
            return
        if not self._backend_offers_tools():
            # Capability intersection: the observed physical backend did not
            # advertise the tools family, so tools/* is not part of the projected
            # modern surface and must never reach the backend.
            await self._send_client_message(
                client_id,
                {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "error": bridge_protocol.method_not_found_error(
                        method,
                        "the physical backend did not advertise the tools "
                        "capability; tools are not part of the projected modern "
                        "surface",
                    ),
                },
            )
            return
        if method == "tools/call":
            if await self._apply_lease_policy(client_id, sanitized, modern=True):
                return
        await self._enqueue_request(client_id, sanitized, modern=True)

    def _fail_all_pending(self, error: BaseException) -> None:
        for pending in list(self.pending_by_backend_id.values()):
            if not pending.done.done():
                pending.done.set_result({"bridgeError": str(error)})
        self.pending_by_backend_id.clear()
        self.pending_by_client_id.clear()
        self.progress_tokens.clear()
        self.server_requests.clear()
        if self.initialize_pending and self.initialize_template is None:
            self.initialize_template = {
                "jsonrpc": "2.0",
                "error": {"code": -32000, "message": "shared backend initialization failed"},
            }
            self.initialize_pending = False
            self.initialize_event.set()


class ManagedHttpBackend:
    """One supervised bridge-managed streamable-http process generation.

    A ``bridge-managed`` streamable-http registration carries a local launch
    definition plus a bounded supervision policy in the *owner* registry.  This
    backend owns exactly one live process generation per registration:

    * ``start`` spawns a strictly newer generation only after any previous
      generation has fully exited (never two live generations at once), then
      polls the registered loopback GET readiness gate until it passes or the
      bounded startup window expires.  ``ready`` is only ever true after the
      gate passed - Popen success alone is never reported as ready.
    * ``stop`` proves the exact owned generation exits, optionally after one
      bounded graceful shutdown HTTP request, escalating to SIGTERM/SIGKILL on
      the process tree (job-object/taskkill on Windows).
    * ``drain`` refuses new relay demand and stops the generation once its
      in-flight relay requests finish, within a bounded grace window.

    The Agent-facing relay mount (``/mcp/<registered-id>``) stays stable across
    generations because the process always binds the registered loopback
    endpoint.  Peer frames never carry the launch definition or supervision
    policy; both live only in the private owner registry.
    """

    def __init__(self, node: "BridgeNode", target: str, entry: dict[str, Any]):
        self.node = node
        self.target = target
        self.entry = entry
        self.state = "exited"
        self.generation = 0
        self.state_history: list[tuple[str, int]] = [(self.state, self.generation)]
        #: Operator lifecycle drain armed (mirrored in the node's desired state).
        self.drain_requested = False
        self.process: asyncio.subprocess.Process | None = None
        self.windows_job: _WindowsProcessJob | None = None
        self.lifecycle_lock = asyncio.Lock()
        self.start_future: asyncio.Task[None] | None = None
        self.stop_future: asyncio.Task[None] | None = None
        self.wait_task: asyncio.Task[None] | None = None
        self.stderr_task: asyncio.Task[Any] | None = None
        self.last_stop_phases: list[dict[str, Any]] = []
        self.last_start_phases: list[dict[str, Any]] = []
        #: True only while the readiness gate of the current generation passed.
        self.ready = False
        self.last_probe_status: int | None = None
        self.last_probe_detail: str = ""
        self.start_error: str | None = None
        #: Owner-side relay requests currently being served by this generation.
        self.inflight = 0
        self.stderr_tail: list[str] = []

    def refresh_entry_if_exited(self, entry: dict[str, Any]) -> None:
        if self.state not in {"exited", "failed"}:
            return
        self.entry = entry

    def supervision(self) -> dict[str, Any]:
        return dict(self.entry.get("management", {}).get("supervision") or {})

    def _transition(self, state: str) -> None:
        self.state = state
        self.state_history.append((state, self.generation))
        if len(self.state_history) > 128:
            del self.state_history[:64]

    def _pid(self) -> int | None:
        process = self.process
        if process is not None and process.returncode is None:
            return process.pid
        return None

    async def start(self, reason: str) -> None:
        """Start (or wait for) the supervised generation and pass readiness.

        Raises ``DrainingError`` when a drain is armed, and ``BridgeError`` on
        spawn failure, readiness-gate timeout, or an unexpected exit during
        startup.  A later demand may retry from the failed state.
        """
        while True:
            wait_for: asyncio.Task[None] | None = None
            async with self.lifecycle_lock:
                if self.drain_requested or self.node._lifecycle_drain_armed(self.target):
                    raise DrainingError(
                        f"bridge-managed streamable-http {self.target} is draining "
                        "and not accepting new relay demand"
                    )
                if self.state == "ready":
                    return
                if self.state == "starting":
                    wait_for = self.start_future
                elif self.state == "stopping":
                    wait_for = self.stop_future
                elif self.state in {"exited", "failed"}:
                    self.generation += 1
                    self._transition("starting")
                    self.ready = False
                    self.start_error = None
                    self.last_probe_status = None
                    self.last_probe_detail = ""
                    self.last_start_phases = []
                    self.start_future = asyncio.create_task(
                        self._start_generation(self.generation, reason)
                    )
                    wait_for = self.start_future
                else:
                    raise BridgeError(f"invalid managed backend state: {self.state}")
            if wait_for is None:
                raise BridgeError("managed backend lifecycle future is unavailable")
            try:
                await asyncio.shield(wait_for)
            except asyncio.CancelledError:
                raise
            if self.state == "ready":
                return
            # A completed startup attempt must surface its bounded terminal
            # failure to the caller.  Looping here would silently respawn an
            # unbounded sequence of generations when readiness can never pass.
            if self.state in {"failed", "exited"} and self.start_error:
                raise BridgeError(self.start_error)

    async def _start_generation(self, generation: int, reason: str) -> None:
        phases = self.last_start_phases
        supervision = self.supervision()
        ready_gate = supervision.get("ready") or {}
        startup_timeout = float(supervision.get("startupTimeoutSeconds", 60))
        environment = os.environ.copy()
        for key in ARTIFACT_ENV_KEYS:
            environment.pop(key, None)
        environment.update(self.entry.get("env", {}))
        kwargs: dict[str, Any] = {
            "cwd": self.entry.get("cwd") or None,
            "env": environment,
            "stdin": asyncio.subprocess.DEVNULL,
            "stdout": asyncio.subprocess.DEVNULL,
            "stderr": asyncio.subprocess.PIPE,
        }
        if os.name == "nt":
            kwargs["creationflags"] = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
        else:
            kwargs["start_new_session"] = True
        process: asyncio.subprocess.Process | None = None
        windows_job: _WindowsProcessJob | None = None
        try:
            process = await asyncio.create_subprocess_exec(
                self.entry["command"],
                *self.entry.get("args", []),
                **kwargs,
            )
            if os.name == "nt":
                windows_job = _WindowsProcessJob(process.pid)
        except Exception as exc:
            if windows_job is not None:
                windows_job.close()
            if process is not None:
                await self._terminate_tree(process, force=True)
            phases.append({"phase": "launch", "outcome": "failure", "detail": str(exc)[:200]})
            await self._settle_failed(generation, f"process launch failed: {exc}")
            return
        assert process is not None
        async with self.lifecycle_lock:
            if self.generation != generation or self.state != "starting":
                if windows_job is not None:
                    windows_job.close()
                await self._terminate_tree(process, force=True)
                raise BridgeError("managed backend generation changed during startup")
            self.process = process
            self.windows_job = windows_job
            self.stderr_tail = []
            self.stderr_task = asyncio.create_task(self._drain_stderr(process))
            self.wait_task = asyncio.create_task(self._wait_process(generation))
        phases.append({"phase": "launch", "outcome": "ok", "pid": process.pid})
        gate_path = str(ready_gate.get("path", ""))
        gate_timeout = float(ready_gate.get("timeoutSeconds", 5))
        gate_statuses = list(ready_gate.get("successStatuses", [200]))
        poll_interval = float(ready_gate.get("pollIntervalSeconds", 1))
        deadline = time.monotonic() + startup_timeout
        probe_interface = {
            "method": "GET",
            "path": gate_path,
            "timeoutSeconds": gate_timeout,
            "successStatuses": gate_statuses,
        }
        phases.append(
            {
                "phase": "readiness-start",
                "outcome": "probing",
                "startupTimeoutSeconds": startup_timeout,
            }
        )
        last_detail = "gate not yet attempted"
        while True:
            if process.returncode is not None:
                phases.append(
                    {
                        "phase": "readiness",
                        "outcome": "failure",
                        "detail": f"backend exited (code {process.returncode}) during startup",
                    }
                )
                await self._terminate_tree(process, force=False)
                await self._settle_failed(
                    generation,
                    f"backend exited with code {process.returncode} before readiness",
                )
                return
            try:
                status, _reason_text = await self.node._http_contract_call(
                    self.entry, probe_interface
                )
                self.last_probe_status = int(status)
                if int(status) in gate_statuses:
                    self.ready = True
                    phases.append(
                        {
                            "phase": "readiness",
                            "outcome": "ok",
                            "status": int(status),
                        }
                    )
                    async with self.lifecycle_lock:
                        if self.generation == generation and self.state == "starting":
                            self._transition("ready")
                    return
                last_detail = f"readiness gate returned status {int(status)}"
            except (BridgeError, OSError, asyncio.TimeoutError) as exc:
                last_detail = f"readiness probe failed: {str(exc)[:200]}"
            self.last_probe_detail = last_detail[:400]
            if time.monotonic() >= deadline:
                phases.append(
                    {
                        "phase": "readiness",
                        "outcome": "timeout",
                        "detail": last_detail[:200],
                    }
                )
                await self._terminate_tree(process, force=True)
                await self._settle_failed(
                    generation,
                    f"did not pass the readiness gate within "
                    f"{startup_timeout:g}s; {last_detail}",
                )
                return
            await asyncio.sleep(poll_interval)

    async def _settle_failed(self, generation: int, message: str) -> None:
        """Clean up after a failed start: no live process, failed state."""
        async with self.lifecycle_lock:
            if self.generation == generation:
                self.process = None
                self.windows_job = None
                self.ready = False
                self.start_error = message[:400]
                if self.state == "starting":
                    self._transition("failed")
                elif self.state != "failed":
                    self._transition("exited")

    async def _drain_stderr(self, process: asyncio.subprocess.Process) -> None:
        assert process.stderr is not None
        try:
            while True:
                line = await process.stderr.readline()
                if not line:
                    break
                text = line.decode("utf-8", "replace").rstrip("\r\n")
                self.stderr_tail.append(text[-800:])
                if len(self.stderr_tail) > 40:
                    del self.stderr_tail[: len(self.stderr_tail) - 40]
        except (asyncio.CancelledError, ValueError):
            raise
        except Exception:
            pass

    async def _wait_process(self, generation: int) -> None:
        assert self.process is not None
        process = self.process
        code = await process.wait()
        if self.state == "stopping" or generation != self.generation:
            return
        self.node.log(
            f"bridge-managed http backend {self.target} generation {generation} "
            f"exited with code {code}"
        )
        async with self.lifecycle_lock:
            if self.generation == generation and self.state in {"ready", "starting"}:
                self.ready = False
                self.start_error = f"backend exited with code {code}"
                if self.state == "ready":
                    self._transition("exited")
                # ``starting`` is settled by the readiness loop, which observes
                # the exit and fails the start.

    async def stop(self, reason: str) -> None:
        """Stop the current generation (if live) and prove exit."""
        while True:
            async with self.lifecycle_lock:
                if self.state in {"exited", "failed"}:
                    return
                if self.state == "stopping":
                    future = self.stop_future
                elif self.state == "starting":
                    future = self.start_future
                else:
                    self._transition("stopping")
                    self.stop_future = asyncio.create_task(
                        self._stop_generation(self.generation, reason)
                    )
                    future = self.stop_future
            if future is None:
                return
            await asyncio.shield(future)
            if self.state in {"exited", "failed"}:
                return

    async def stop_when_drained(self, reason: str) -> None:
        """Wait (bounded) for in-flight relay requests, then stop the generation."""
        deadline = time.monotonic() + HTTP_MANAGED_DRAIN_GRACE_SECONDS
        while self.inflight > 0 and time.monotonic() < deadline:
            await asyncio.sleep(0.05)
        if self.inflight > 0:
            self.node.log(
                f"bridge-managed http backend {self.target} drain grace expired "
                f"with {self.inflight} in-flight relay request(s)"
            )
        await self.stop(reason)

    async def _stop_generation(self, generation: int, reason: str) -> None:
        process = self.process
        phases: list[dict[str, Any]] = []
        self.last_stop_phases = phases
        phases.append({"phase": "drain", "outcome": "entered"})
        supervision = self.supervision()
        shutdown = supervision.get("shutdown")
        forced = False
        if process is not None and shutdown is not None:
            try:
                status, _reason_text = await self.node._http_contract_call(
                    self.entry,
                    {
                        "method": str(shutdown.get("method", "POST")),
                        "timeoutSeconds": float(shutdown.get("timeoutSeconds", 10)),
                        "successStatuses": list(
                            shutdown.get("successStatuses", [200, 202, 204])
                        ),
                        **(
                            {"path": shutdown["path"]}
                            if "path" in shutdown
                            else {"url": shutdown["url"]}
                        ),
                    },
                )
                phases.append(
                    {
                        "phase": "shutdown-request",
                        "outcome": (
                            "accepted"
                            if int(status)
                            in shutdown.get("successStatuses", [200, 202, 204])
                            else "unexpected-status"
                        ),
                        "status": int(status),
                    }
                )
            except (BridgeError, OSError, asyncio.TimeoutError) as exc:
                phases.append(
                    {
                        "phase": "shutdown-request",
                        "outcome": "failure",
                        "detail": str(exc)[:200],
                    }
                )
        if process is not None:
            try:
                await asyncio.wait_for(
                    process.wait(), timeout=SHARED_BACKEND_STOP_TIMEOUT_SECONDS
                )
                phases.append({"phase": "wait", "outcome": "exited"})
            except TimeoutError:
                phases.append({"phase": "wait", "outcome": "timeout"})
                await self._terminate_tree(process, force=False)
                phases.append({"phase": "terminate", "outcome": "requested"})
                forced = process.returncode is None
        if process is not None and process.returncode is None:
            await self._terminate_tree(process, force=True)
            forced = True
        phases.append(
            {"phase": "force-kill", "outcome": "used" if forced else "not-needed"}
        )
        current = asyncio.current_task()
        tasks = [self.wait_task, self.stderr_task]
        for task in tasks:
            if task is not None and task is not current and not task.done():
                task.cancel()
        await asyncio.gather(
            *(task for task in tasks if task is not None and task is not current),
            return_exceptions=True,
        )
        self.process = None
        self.windows_job = None
        self.ready = False
        async with self.lifecycle_lock:
            if self.generation == generation:
                self._transition("exited")

    async def _terminate_tree(
        self,
        process: asyncio.subprocess.Process | None,
        *,
        force: bool,
    ) -> None:
        if process is None:
            return
        if os.name == "nt":
            if force and self.windows_job is not None:
                self.windows_job.close()
                self.windows_job = None
            elif force:
                await asyncio.to_thread(
                    subprocess.run,
                    ["taskkill", "/PID", str(process.pid), "/T", "/F"],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    check=False,
                    timeout=10,
                )
            if process.returncode is None:
                try:
                    process.kill() if force else process.terminate()
                except ProcessLookupError:
                    pass
        else:
            try:
                os.killpg(process.pid, signal.SIGKILL if force else signal.SIGTERM)
            except ProcessLookupError:
                pass
        if process.returncode is None:
            try:
                await asyncio.wait_for(process.wait(), timeout=5)
            except TimeoutError:
                if not force:
                    await self._terminate_tree(process, force=True)


class BridgeNode:
    """One symmetric bridge node with a local control socket and one peer link."""

    def __init__(
        self,
        *,
        side: str,
        registry: Registry,
        local_host: str,
        local_port: int,
        link_mode: str,
        link_host: str,
        link_port: int,
        reconnect_delay: float = 0.5,
        allowed_artifact_roots: list[Path] | None = None,
        artifact_spool_root: Path | None = None,
        artifact_resume_retention_seconds: float = ARTIFACT_RESUME_RETENTION_SECONDS,
        max_artifact_bytes: int = DEFAULT_MAX_ARTIFACT_BYTES,
        journal: EventJournal | None = None,
        http_relay_port: int = 0,
    ):
        if side not in {"win", "wsl"}:
            raise BridgeError("side must be win or wsl")
        if not _is_loopback(local_host):
            raise BridgeError("local control listener must use a loopback address")
        if not _is_loopback(link_host):
            if link_mode == "listen":
                raise BridgeError("bridge link listener must use a loopback address")
            raise BridgeError("bridge link connector must use a loopback address")
        if not isinstance(http_relay_port, int) or isinstance(http_relay_port, bool) or http_relay_port < 0:
            raise BridgeError("http_relay_port must be a non-negative integer (0 disables the relay)")
        self.http_relay_port = http_relay_port
        self.side = side
        self.registry = registry
        self.journal = journal
        self.local_host = local_host
        self.local_port = local_port
        self.link_mode = link_mode
        self.link_host = link_host
        self.link_port = link_port
        self.reconnect_delay = reconnect_delay
        self.link_reader: asyncio.StreamReader | None = None
        self.link_writer: asyncio.StreamWriter | None = None
        self.link_ready = asyncio.Event()
        self.link_write_lock = asyncio.Lock()
        self.link_installing = False
        self.streams: dict[str, StreamState] = {}
        self.pending_registry: dict[str, asyncio.Future[Any]] = {}
        self.pending_control: dict[str, asyncio.Future[Any]] = {}
        self.pending_artifacts: dict[str, ArtifactTransferWaiter] = {}
        self.receiving_artifacts: dict[str, ArtifactReceiveState] = {}
        self.pending_inputs: dict[str, InputTransferWaiter] = {}
        self.receiving_inputs: dict[str, InputReceiveState] = {}
        self.artifact_publishers: dict[str, str] = {}
        self.shared_artifact_publishers: dict[str, SharedBackend] = {}
        self.shared_backends: dict[str, SharedBackend] = {}
        #: Supervised bridge-managed streamable-http process generations.
        self.http_backends: dict[str, ManagedHttpBackend] = {}
        #: Operator-armed lifecycle desired state keyed by local registry id.
        self.lifecycle_desired: dict[str, LifecycleDesired] = {}
        #: Lifecycle operations serialize per registration; unrelated targets
        #: remain independently controllable.
        self.lifecycle_op_lock = asyncio.Lock()  # compatibility alias; no global use
        # Runtime-only Operator intent. False prevents Agent auto-reconnect from
        # silently resurrecting a deliberately stopped registration. It is not
        # persisted and never changes the Agent-visible MCP tool catalog.
        self.lifecycle_armed: dict[str, bool] = {}
        self.lifecycle_locks: dict[str, asyncio.Lock] = {}
        self.lifecycle_last: dict[str, dict[str, Any]] = {}
        self.allowed_artifact_roots = [
            root.expanduser().resolve() for root in (allowed_artifact_roots or [])
        ]
        for root in self.allowed_artifact_roots:
            if not root.is_dir():
                raise BridgeError(f"allowed artifact root is not a directory: {root}")
        artifact_spool_base = (
            artifact_spool_root.expanduser().resolve()
            if artifact_spool_root is not None
            else default_registry_path(side).parent / "spool"
        )
        self.artifact_spool_root = artifact_spool_base / f"{side}-artifacts-v1"
        self.peer_artifacts = False
        self.peer_artifact_inputs = False
        self.peer_artifact_protocol: int | None = None
        self.peer_artifact_chunk_bytes = ARTIFACT_CHUNK_BYTES
        self.link_generation: str | None = None
        if artifact_resume_retention_seconds <= 0:
            raise BridgeError("artifact_resume_retention_seconds must be positive")
        self.artifact_resume_retention_seconds = float(artifact_resume_retention_seconds)
        #: per-inbox locks serializing durable v2 journal read-modify-write.
        self.artifact_journal_locks: dict[str, asyncio.Lock] = {}
        #: sender-side remembered resume tokens keyed by "<sha256>:<name>".
        self.publish_resume_tokens: dict[str, dict[str, Any]] = {}
        #: true only while the peer link is being torn down (drives parking).
        self._link_failing = False
        if max_artifact_bytes <= 0:
            raise BridgeError("max_artifact_bytes must be positive")
        self.max_artifact_bytes = max_artifact_bytes
        self.background_tasks: set[asyncio.Task[Any]] = set()
        self.local_server: asyncio.AbstractServer | None = None
        self.link_server: asyncio.AbstractServer | None = None
        self.relay_server: asyncio.AbstractServer | None = None
        #: Native HTTP relay state. ``relay_start`` resolves once the owner has
        #: produced response status/headers; ``relay_bodies`` streams the body.
        self.relay_start: dict[str, asyncio.Future[tuple[Any, ...]]] = {}
        self.relay_bodies: dict[str, asyncio.Queue[tuple[Any, ...]]] = {}
        self.relay_tasks: dict[str, asyncio.Task[Any]] = {}
        #: bounded trace evidence pipeline.  Data hooks enqueue only when the
        #: cached active-trace target set matches; the worker performs the
        #: journal writes in a worker thread so the event loop never blocks.
        self._trace_queue: asyncio.Queue[tuple[str, str, bytes]] | None = None
        self._trace_dropped = 0
        self._trace_targets: set[str] = set()
        self._trace_refresh_at = 0.0
        #: P8 always-on metadata-only cross-host correlation observer.  Separate
        #: from trace capture: it needs no explicit trace session, parses only
        #: bounded newline-delimited JSON (never executes anything), keeps no
        #: payload, and enqueues only metadata-only observations for the worker.
        #: Overload and oversized input never block or alter the data plane.
        self._evidence_queue: asyncio.Queue[tuple[str, stream_evidence.Observation]] | None = None
        self._evidence_worker_started = False
        self._evidence_dropped = 0
        self._evidence_capacity_streams = 0
        self._evidence_skipped_bytes = 0
        self._evidence_oversized_messages = 0
        self._evidence_oversized_bytes = 0
        self._evidence_partial_bytes = 0
        self._evidence_states: dict[str, stream_evidence.StreamCorrelator] = {}

    def log(self, message: str, *, category: str = "runtime", target: str | None = None) -> None:
        print(f"[{self.side}-bridge] {message}", file=sys.stderr, flush=True)
        if self.journal is not None:
            try:
                self.journal.record(
                    side=self.side,
                    category=category,
                    target=target,
                    outcome="observed",
                    metadata={"messageClass": category, "messageBytes": len(message.encode("utf-8"))},
                )
            except (OSError, sqlite3.Error):
                pass

    def _background(self, coroutine: Any) -> asyncio.Task[Any]:
        task = asyncio.create_task(coroutine)
        self.background_tasks.add(task)

        def finished(done: asyncio.Task[Any]) -> None:
            self.background_tasks.discard(done)
            if done.cancelled():
                return
            error = done.exception()
            if error is not None:
                self.log(f"background task failed: {type(error).__name__}: {error}")

        task.add_done_callback(finished)
        return task

    # ---- bounded trace evidence capture (never blocks the event loop) ----

    async def _refresh_trace_targets(self) -> None:
        if self.journal is None:
            return
        try:
            self._trace_targets = await asyncio.to_thread(self.journal.active_targets)
        except (OSError, sqlite3.Error):
            self._trace_targets = set()

    async def _maybe_capture(
        self, stream: StreamState, direction: str, data: bytes
    ) -> None:
        # P8 cross-host correlation observer runs before the trace gates; it is
        # always-on, bounded, and never raises (see _observe_correlation).
        self._observe_correlation(stream, direction, data)
        journal = self.journal
        if journal is None or not data or not stream.target:
            return
        if self._trace_queue is None:
            return
        now = time.monotonic()
        if now >= self._trace_refresh_at:
            self._trace_refresh_at = now + journal.TRACE_TARGET_REFRESH_SECONDS
            await self._refresh_trace_targets()
        if stream.target not in self._trace_targets:
            return
        try:
            self._trace_queue.put_nowait((stream.target, direction, data))
        except asyncio.QueueFull:
            # Trace evidence is best-effort and bounded; dropping a saturated
            # chunk never affects MCP data flow. Expose only the aggregate.
            self._trace_dropped += 1

    async def _trace_process_item(
        self, target: str, direction: str, data: bytes
    ) -> None:
        if self.journal is None:
            return
        try:
            await asyncio.to_thread(
                self.journal.capture, target=target, direction=direction, data=data
            )
        except (BridgeError, OSError, sqlite3.Error):
            pass

    async def _trace_worker(self) -> None:
        queue = self._trace_queue
        if queue is None:
            return
        while True:
            item = await queue.get()
            try:
                if item is None:
                    return
                target, direction, data = item
                await self._trace_process_item(target, direction, data)
            finally:
                queue.task_done()

    # ---- P8 bounded metadata-only cross-host correlation observer ----
    # Observer only: it never mutates, replays, or blocks the data plane, never
    # persists payloads, and classification never executes anything.  Both
    # nodes derive the same opaque correlation alias from the shared logical
    # stream id + typed request id + occurrence, so no correlation state is
    # ever exchanged and no raw stream/RPC identity is ever recorded.

    def _start_evidence_worker(self) -> None:
        if self.journal is None or self._evidence_worker_started:
            return
        self._evidence_worker_started = True
        self._evidence_queue = asyncio.Queue(maxsize=EVIDENCE_QUEUE_MAX)
        self._background(self._evidence_worker())

    async def _evidence_worker(self) -> None:
        queue = self._evidence_queue
        if queue is None:
            return
        while True:
            item = await queue.get()
            try:
                if item is None:
                    return
                target, observation = item
                await self._evidence_process_item(target, observation)
            finally:
                queue.task_done()

    async def _evidence_process_item(
        self, target: str, observation: stream_evidence.Observation
    ) -> None:
        journal = self.journal
        if journal is None:
            return
        try:
            await asyncio.to_thread(
                journal.record,
                side=self.side,
                category=EVIDENCE_CATEGORY,
                correlation_id=observation.correlation_id,
                target=target,
                outcome=observation.kind,
                metadata={
                    "kind": observation.kind,
                    "flow": observation.flow,
                    "messageBytes": observation.message_bytes,
                    "occurrence": observation.occurrence,
                    "note": observation.note,
                },
            )
        except (OSError, sqlite3.Error):
            pass

    def _retire_evidence_state(self, state: stream_evidence.StreamCorrelator) -> None:
        for flow in stream_evidence.ALL_FLOWS:
            messages, evicted_bytes, partial = state.take_final_counts(flow)
            self._evidence_oversized_messages += messages
            self._evidence_oversized_bytes += evicted_bytes
            self._evidence_partial_bytes += partial

    def _observe_correlation(
        self, stream: StreamState, direction: str, data: bytes
    ) -> None:
        if self.journal is None or not data or not stream.target:
            return
        if direction not in stream_evidence.ALL_FLOWS:
            return
        if stream.evidence_disabled or stream.closed.is_set():
            self._evidence_skipped_bytes += len(data)
            return
        try:
            state = self._evidence_states.get(stream.stream_id)
            if state is None:
                if len(self._evidence_states) >= EVIDENCE_STATE_CAP:
                    stream.evidence_disabled = True
                    self._evidence_capacity_streams += 1
                    self._evidence_skipped_bytes += len(data)
                    return
                state = stream_evidence.StreamCorrelator(
                    side=self.side,
                    max_message_bytes=stream_evidence.DEFAULT_MAX_MESSAGE_BYTES,
                )
                self._evidence_states[stream.stream_id] = state
            observations = state.feed(
                stream_id=stream.stream_id, flow=direction, data=data
            )
            messages, dropped_bytes = state.take_oversized_delta(direction)
            self._evidence_oversized_messages += messages
            self._evidence_oversized_bytes += dropped_bytes
        except Exception:
            # No later alias may rely on partially updated observation state.
            stream.evidence_disabled = True
            self._evidence_skipped_bytes += len(data)
            return
        queue = self._evidence_queue
        if queue is None:
            return
        for observation in observations:
            try:
                queue.put_nowait((stream.target, observation))
            except asyncio.QueueFull:
                # Observation overload is dropped and counted; the transport
                # and the occurrence counters (updated inline by the framer)
                # are unaffected.
                self._evidence_dropped += 1

    def _prepare_artifact_spool(self) -> None:
        shutil.rmtree(self.artifact_spool_root, ignore_errors=True)
        self.artifact_spool_root.mkdir(parents=True, mode=0o700, exist_ok=True)
        try:
            self.artifact_spool_root.chmod(0o700)
        except OSError:
            pass
        removed_partials = self._cleanup_workspace_partials()
        if removed_partials:
            self.log(f"removed {removed_partials} stale workspace artifact partial(s)")

    def _cleanup_workspace_partials(self) -> int:
        removed = 0
        for root in self.allowed_artifact_roots:
            artifact_root = root / ".mcp-artifacts"
            try:
                root_metadata = artifact_root.lstat()
            except FileNotFoundError:
                continue
            if not stat.S_ISDIR(root_metadata.st_mode) or stat.S_ISLNK(root_metadata.st_mode):
                continue
            # artifacts/2 durable journal: referenced partials survive a node
            # restart so a resumed delivery can continue; orphans do not.
            referenced: set[str] = set()
            try:
                journal = _read_artifact_journal(artifact_root / ARTIFACT_JOURNAL_NAME)
            except BridgeError:
                journal = None
            if isinstance(journal, dict):
                for record in journal["records"].values():
                    if not isinstance(record, dict):
                        continue
                    partial = BridgeNode._journal_partial(artifact_root, record)
                    if partial is not None:
                        referenced.add(partial.name)
            try:
                candidates = list(artifact_root.iterdir())
            except OSError:
                continue
            for candidate in candidates:
                try:
                    candidate_metadata = candidate.lstat()
                except FileNotFoundError:
                    continue
                if (
                    not stat.S_ISDIR(candidate_metadata.st_mode)
                    or stat.S_ISLNK(candidate_metadata.st_mode)
                ):
                    continue
                try:
                    entries = list(candidate.iterdir())
                except OSError:
                    continue
                removed_entry = False
                for entry in entries:
                    name = entry.name
                    if name == ".partial":
                        removed_entry = True
                    elif name.startswith(".partial-") and name not in referenced:
                        removed_entry = True
                    else:
                        continue
                    try:
                        entry.unlink()
                    except OSError:
                        continue
                    removed += 1
                if removed_entry:
                    try:
                        candidate.rmdir()
                    except OSError:
                        pass
        return removed

    async def run(self) -> None:
        connector: asyncio.Task[Any] | None = None
        try:
            self._prepare_artifact_spool()
            self.local_server = await asyncio.start_server(
                self._handle_local,
                self.local_host,
                self.local_port,
                limit=MAX_FRAME_BYTES,
            )
            self.log(f"local control listening on {self.local_host}:{self.local_port}")
            servers = [self.local_server]
            if self.http_relay_port > 0:
                self.relay_server = await asyncio.start_server(
                    self._handle_http_relay_connection,
                    self.local_host,
                    self.http_relay_port,
                    limit=BUFFER_SIZE,
                )
                self.log(
                    f"streamable HTTP relay listening on "
                    f"{self.local_host}:{self.http_relay_port}"
                )
                servers.append(self.relay_server)
            if self.journal is not None:
                self._trace_queue = asyncio.Queue(maxsize=4096)
                worker = asyncio.create_task(self._trace_worker())
                self.background_tasks.add(worker)
                self._start_evidence_worker()
            if self.link_mode == "listen":
                self.link_server = await asyncio.start_server(
                    self._accept_link,
                    self.link_host,
                    self.link_port,
                    limit=MAX_FRAME_BYTES,
                )
                self.log(f"peer link listening on {self.link_host}:{self.link_port}")
                servers.append(self.link_server)
                await asyncio.gather(*(server.serve_forever() for server in servers))
            elif self.link_mode == "connect":
                connector = asyncio.create_task(self._connect_loop())
                await asyncio.gather(*(server.serve_forever() for server in servers))
            else:
                raise BridgeError("link mode must be listen or connect")
        finally:
            if connector is not None:
                connector.cancel()
                await asyncio.gather(connector, return_exceptions=True)
            for server in (self.local_server, self.link_server, self.relay_server):
                if server is not None:
                    server.close()
            for server in (self.local_server, self.link_server, self.relay_server):
                if server is not None:
                    await server.wait_closed()
            await self._fail_link_state("bridge node shutting down")
            if self.link_writer is not None:
                self.link_writer.close()
                try:
                    await self.link_writer.wait_closed()
                except OSError:
                    pass
            current = asyncio.current_task()
            pending = [
                task
                for task in self.background_tasks
                if task is not current and not task.done()
            ]
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)

    async def _connect_loop(self) -> None:
        while True:
            try:
                reader, writer = await asyncio.open_connection(
                    self.link_host,
                    self.link_port,
                    limit=MAX_FRAME_BYTES,
                )
                await self._install_link(reader, writer, initiator=True)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.log(f"peer link unavailable: {exc}")
            await asyncio.sleep(self.reconnect_delay)

    async def _accept_link(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        if self.link_ready.is_set() or self.link_installing:
            writer.write(_json_bytes({"type": "hello_error", "message": "peer already connected"}) + b"\n")
            await writer.drain()
            writer.close()
            await writer.wait_closed()
            return
        self.link_installing = True
        try:
            await self._install_link(reader, writer, initiator=False)
        except asyncio.CancelledError:
            raise
        except (EOFError, BridgeError, ConnectionError, OSError) as exc:
            self.log(f"peer link ended: {exc}")
        finally:
            self.link_installing = False

    async def _install_link(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        *,
        initiator: bool,
    ) -> None:
        offered_extensions = {
            "artifacts": {
                "version": ARTIFACT_PROTOCOL_V1,
                "highestVersion": ARTIFACT_PROTOCOL_VERSION,
                "maxChunk": ARTIFACT_CHUNK_BYTES,
            },
            ARTIFACT_INPUTS_EXTENSION: {"version": 1},
        }

        def inputs_valid(value: Any) -> bool:
            return isinstance(value, dict) and value.get("version") == 1

        def accept_artifacts(value: Any) -> None:
            """Record the effective artifacts revision negotiated with a peer.

            A version-1-only peer still negotiates artifacts/1; a peer that
            answers the ``highestVersion`` offer (or speaks version 2 directly)
            negotiates artifacts/2. Unknown or absent offers disable artifact
            delivery rather than guessing.
            """
            protocol = _parse_artifact_extension(value)
            maximum = _artifact_max_chunk(value) if protocol is not None else None
            self.peer_artifact_protocol = protocol
            self.peer_artifacts = protocol is not None
            self.peer_artifact_chunk_bytes = (
                min(ARTIFACT_CHUNK_BYTES, maximum)
                if protocol is not None and maximum is not None
                else ARTIFACT_CHUNK_BYTES
            )

        def peer_extensions(frame: Any) -> tuple[Any, Any]:
            if not isinstance(frame, dict):
                return None, None
            extensions = frame.get("extensions") if isinstance(frame, dict) else None
            if not isinstance(extensions, dict):
                return None, None
            return extensions.get("artifacts"), extensions.get(ARTIFACT_INPUTS_EXTENSION)

        try:
            if initiator:
                writer.write(
                    _json_bytes(
                        {
                            "type": "hello",
                            "protocol": BRIDGE_PROTOCOL,
                            "side": self.side,
                            "extensions": offered_extensions,
                        }
                    )
                    + b"\n"
                )
                await writer.drain()
                reply = await self._read_frame(reader)
                if reply.get("type") != "hello_ok" or reply.get("protocol") != BRIDGE_PROTOCOL:
                    raise BridgeError(f"peer rejected bridge handshake: {reply}")
                artifact_extension, input_extension = peer_extensions(reply)
                accept_artifacts(artifact_extension)
                self.peer_artifact_inputs = inputs_valid(input_extension)
            else:
                hello = await self._read_frame(reader)
                if hello.get("type") != "hello" or hello.get("protocol") != BRIDGE_PROTOCOL:
                    raise BridgeError("invalid bridge handshake")
                artifact_extension, input_extension = peer_extensions(hello)
                accept_artifacts(artifact_extension)
                self.peer_artifact_inputs = inputs_valid(input_extension)
                extensions: dict[str, Any] = {}
                if self.peer_artifacts:
                    extensions["artifacts"] = {
                        "version": self.peer_artifact_protocol or ARTIFACT_PROTOCOL_V1,
                        "maxChunk": ARTIFACT_CHUNK_BYTES,
                    }
                if self.peer_artifact_inputs:
                    extensions[ARTIFACT_INPUTS_EXTENSION] = {"version": 1}
                writer.write(
                    _json_bytes(
                        {
                            "type": "hello_ok",
                            "protocol": BRIDGE_PROTOCOL,
                            "side": self.side,
                            "extensions": extensions,
                        }
                    )
                    + b"\n"
                )
                await writer.drain()
            self.link_generation = uuid.uuid4().hex
            self.link_reader = reader
            self.link_writer = writer
            self.link_ready.set()
            self.log("peer link established")
            await self._link_read_loop(reader)
        finally:
            if self.link_writer is writer:
                self.link_ready.clear()
                self.link_reader = None
                self.link_writer = None
                self.peer_artifacts = False
                self.peer_artifact_inputs = False
                self.peer_artifact_protocol = None
                self.peer_artifact_chunk_bytes = ARTIFACT_CHUNK_BYTES
                self.link_generation = None
                await self._fail_link_state("peer link disconnected")
            writer.close()
            try:
                await writer.wait_closed()
            except OSError:
                pass
            self.log("peer link closed")

    async def _read_frame(self, reader: asyncio.StreamReader) -> dict[str, Any]:
        line = await reader.readline()
        if not line:
            raise EOFError("peer closed")
        if len(line) > MAX_FRAME_BYTES:
            raise BridgeError("bridge frame exceeds limit")
        value = json.loads(line)
        if not isinstance(value, dict):
            raise BridgeError("bridge frame must be an object")
        return value

    async def _send_frame(self, frame: dict[str, Any]) -> None:
        if not self.link_writer or not self.link_ready.is_set():
            raise BridgeError("peer link is not connected")
        data = _json_bytes(frame) + b"\n"
        if len(data) > MAX_FRAME_BYTES:
            raise BridgeError("bridge frame exceeds limit")
        async with self.link_write_lock:
            assert self.link_writer is not None
            self.link_writer.write(data)
            await self.link_writer.drain()

    async def _link_read_loop(self, reader: asyncio.StreamReader) -> None:
        while True:
            frame = await self._read_frame(reader)
            await self._handle_frame(frame)

    async def _handle_frame(self, frame: dict[str, Any]) -> None:
        kind = frame.get("type")
        if kind == "open":
            self._background(self._handle_remote_open(frame))
            return
        if kind in {"open_ok", "open_error"}:
            stream = self.streams.get(str(frame.get("stream")))
            if stream and stream.opened and not stream.opened.done():
                if kind == "open_ok":
                    stream.opened.set_result(None)
                else:
                    stream.opened.set_exception(BridgeError(str(frame.get("message", "open failed"))))
            return
        if kind == "data":
            await self._handle_stream_data(frame)
            return
        if kind == "data_ok":
            self._handle_stream_data_ok(frame)
            return
        if kind == "eof":
            await self._handle_stream_eof(str(frame.get("stream")))
            return
        if kind == "close":
            self._background(self._close_stream(str(frame.get("stream")), remote=True))
            return
        if kind == "registry_request":
            self._background(self._handle_registry_request(frame))
            return
        if kind == "registry_response":
            request_id = str(frame.get("request"))
            future = self.pending_registry.pop(request_id, None)
            if future and not future.done():
                if frame.get("ok"):
                    future.set_result(frame.get("result"))
                else:
                    future.set_exception(BridgeError(str(frame.get("message", "registry request failed"))))
            return
        if kind == "control_request":
            self._background(self._handle_control_request(frame))
            return
        if kind == "control_response":
            request_id = str(frame.get("request"))
            future = self.pending_control.pop(request_id, None)
            if future and not future.done():
                if frame.get("ok"):
                    future.set_result(frame.get("result"))
                else:
                    future.set_exception(BridgeError(str(frame.get("message", "control request failed"))))
            return
        if kind == "relay_request":
            self._background(self._handle_relay_request(frame))
            return
        if kind == "relay_cancel":
            self._background(self._handle_relay_cancel(frame))
            return
        if kind == "relay_start":
            self._handle_relay_start_frame(frame)
            return
        if kind == "relay_body":
            self._handle_relay_body_frame(frame)
            return
        if kind == "relay_end":
            self._handle_relay_end_frame(frame)
            return
        if kind == "relay_error":
            self._handle_relay_error_frame(frame)
            return
        if isinstance(kind, str) and kind.startswith("artifact_"):
            if not self.peer_artifacts:
                self.log("ignored artifact frame without negotiated artifacts extension")
                return
            if kind == "artifact_begin":
                if self.peer_artifact_protocol == ARTIFACT_PROTOCOL_V2:
                    self._background(self._handle_artifact_begin_v2(frame))
                else:
                    self._background(self._handle_artifact_begin(frame))
            elif kind == "artifact_chunk":
                artifact_id = frame.get("artifact")
                state = (
                    self.receiving_artifacts.get(artifact_id)
                    if isinstance(artifact_id, str)
                    else None
                )
                if state is None:
                    return
                if isinstance(state, ArtifactReceiveStateV2):
                    self._handle_artifact_chunk_v2(state, frame)
                else:
                    self._handle_artifact_chunk(frame)
            elif kind == "artifact_end":
                artifact_id = frame.get("artifact")
                state = (
                    self.receiving_artifacts.get(artifact_id)
                    if isinstance(artifact_id, str)
                    else None
                )
                if state is None:
                    return
                if isinstance(state, ArtifactReceiveStateV2):
                    self._handle_artifact_end_v2(state, frame)
                else:
                    self._handle_artifact_end(frame)
            elif kind == "artifact_cancel":
                artifact_id = frame.get("artifact")
                state = (
                    self.receiving_artifacts.get(artifact_id)
                    if isinstance(artifact_id, str)
                    else None
                )
                if state and frame.get("stream") == state.stream_id:
                    if isinstance(state, ArtifactReceiveStateV2):
                        self._schedule_artifact_abort_v2(
                            state, "sender cancelled transfer"
                        )
                    else:
                        self._schedule_artifact_abort(state, "sender cancelled transfer")
            elif kind in {
                "artifact_ready",
                "artifact_chunk_ok",
                "artifact_ok",
                "artifact_error",
                "artifact_committed",
            }:
                self._handle_artifact_reply(frame)
            else:
                self.log(f"ignored unknown artifact frame type: {kind}")
            return
        if isinstance(kind, str) and kind.startswith("input_"):
            if not self.peer_artifact_inputs:
                self.log(
                    "ignored input frame without negotiated artifact-inputs/1 extension"
                )
                return
            if kind == "input_begin":
                self._background(self._handle_input_begin(frame))
            elif kind == "input_chunk":
                self._handle_input_chunk(frame)
            elif kind == "input_end":
                self._handle_input_end(frame)
            elif kind == "input_cancel":
                input_id = frame.get("input")
                state = (
                    self.receiving_inputs.get(input_id)
                    if isinstance(input_id, str)
                    else None
                )
                if state and frame.get("stream") == state.stream_id:
                    self._schedule_input_abort(state, "sender cancelled transfer")
            elif kind in {
                "input_ready",
                "input_chunk_ok",
                "input_ok",
                "input_error",
            }:
                self._handle_input_reply(frame)
            else:
                self.log(f"ignored unknown input frame type: {kind}")
            return
        raise BridgeError(f"unknown bridge frame type: {kind!r}")

    async def _handle_local(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            request = await asyncio.wait_for(self._read_frame(reader), timeout=10)
            op = request.get("op")
            if op in {"connect", "connect-http"}:
                target = request.get("target")
                if not isinstance(target, str) or not ID_PATTERN.fullmatch(target):
                    raise BridgeError("connect requires a valid registered target id")
                artifact_inbox = self._validate_artifact_inbox(request.get("artifactInbox"))
                await self._serve_local_stream(
                    reader,
                    writer,
                    target,
                    artifact_inbox=artifact_inbox,
                    compatibility_http=(op == "connect-http"),
                )
                return
            if op == "publish":
                result = await self._publish_local_artifact(request)
                writer.write(_json_bytes({"ok": True, "result": result}) + b"\n")
                await writer.drain()
                return
            if op == "stage_input":
                result = await self._stage_local_input(request)
                writer.write(_json_bytes({"ok": True, "result": result}) + b"\n")
                await writer.drain()
                return
            if op == "lifecycle":
                result = await self._lifecycle_control(request)
                writer.write(_json_bytes({"ok": True, "result": result}) + b"\n")
                await writer.drain()
                return
            if op == "control":
                scope = request.get("scope", "remote")
                if scope == "local":
                    result = await self._agent_control(request)
                elif scope == "remote":
                    payload = {key: value for key, value in request.items() if key not in {"op", "scope"}}
                    result = await self._remote_control(payload)
                else:
                    raise BridgeError("control scope must be local or remote")
                writer.write(_json_bytes({"ok": True, "result": result}) + b"\n")
                await writer.drain()
                return
            if op == "diagnostics":
                result = self._diagnostics_summary(request)
                writer.write(_json_bytes({"ok": True, "result": result}) + b"\n")
                await writer.drain()
                return
            if op == "registry":
                result = await self._registry_query(
                    str(request.get("scope", "remote")),
                    str(request.get("action", "list")),
                    request.get("arguments") if isinstance(request.get("arguments"), dict) else {},
                )
                writer.write(_json_bytes({"ok": True, "result": result}) + b"\n")
                await writer.drain()
                return
            raise BridgeError(f"unknown local operation: {op!r}")
        except Exception as exc:
            try:
                writer.write(_json_bytes({"ok": False, "message": str(exc)}) + b"\n")
                await writer.drain()
            except OSError:
                pass
        finally:
            if not writer.is_closing():
                writer.close()
                try:
                    await writer.wait_closed()
                except OSError:
                    pass

    def _validate_artifact_inbox(self, value: Any) -> Path | None:
        if value is None or value == "":
            return None
        if not isinstance(value, str):
            raise BridgeError("artifactInbox must be a local absolute directory path")
        inbox = Path(value).expanduser()
        if not inbox.is_absolute() or not inbox.is_dir():
            raise BridgeError("artifactInbox must be an existing local absolute directory")
        resolved = inbox.resolve()
        if not any(resolved == root or resolved.is_relative_to(root) for root in self.allowed_artifact_roots):
            raise BridgeError("artifactInbox is outside Operator-authorized workspace roots")
        return resolved

    @staticmethod
    def _safe_artifact_name(value: Any) -> str:
        if (
            not isinstance(value, str)
            or not value
            or len(value.encode("utf-8")) > 240
            or len(value.encode("utf-16-le")) // 2 > 240
        ):
            raise BridgeError(
                "artifact name must be non-empty and fit cross-platform component limits"
            )
        if value in {".", ".."} or any(
            ord(character) < 32 or character in '/\\\x00:<>"|?*'
            for character in value
        ):
            raise BridgeError("artifact name must be one cross-platform safe filename component")
        lowered = value.casefold()
        if any(encoded in lowered for encoded in ("%2f", "%5c", "%2e")):
            raise BridgeError("artifact name must not contain encoded path syntax")
        if value != value.rstrip(" ."):
            raise BridgeError("artifact name must not end with a dot or space")
        windows_stem = value.split(".", 1)[0].upper()
        reserved = {
            "CON",
            "PRN",
            "AUX",
            "NUL",
            "CONIN$",
            "CONOUT$",
            *{f"COM{i}" for i in range(1, 10)},
            *{f"LPT{i}" for i in range(1, 10)},
        }
        if windows_stem in reserved:
            raise BridgeError("artifact name is reserved on Windows")
        return value

    async def _publish_local_artifact(self, request: dict[str, Any]) -> dict[str, Any]:
        if not self.peer_artifacts:
            raise BridgeError("peer does not support the negotiated artifacts extension")
        token = request.get("token")
        if not isinstance(token, str):
            raise BridgeError("artifact publish requires a session token")
        stream_id = self.artifact_publishers.get(token)
        stream = self.streams.get(stream_id) if stream_id else None
        shared_backend = self.shared_artifact_publishers.get(token)
        if stream and not stream.closed.is_set() and stream.artifact_stage is not None:
            artifact_stage = stream.artifact_stage
            artifact_max_bytes = stream.artifact_max_bytes
        elif shared_backend is not None and shared_backend.artifact_stage is not None:
            stream = shared_backend.artifact_stream()
            if stream is None or stream.closed.is_set():
                raise BridgeError("shared artifact publish has no active client request")
            artifact_stage = shared_backend.artifact_stage
            artifact_max_bytes = shared_backend.artifact_max_bytes
        else:
            raise BridgeError("artifact publish session is unavailable")
        relative_name = self._safe_artifact_name(request.get("relativePath"))
        display_name = self._safe_artifact_name(request.get("name", relative_name))
        media_type = request.get("mediaType")
        if media_type is not None and (
            not isinstance(media_type, str)
            or not media_type
            or len(media_type) > 255
            or any(character in media_type for character in ("\r", "\n", "\x00"))
        ):
            raise BridgeError("artifact mediaType is invalid")
        source = artifact_stage / relative_name
        snapshot, size, sha256, source_identity = await asyncio.to_thread(
            self._snapshot_artifact,
            source,
            artifact_max_bytes,
        )
        try:
            if self.peer_artifact_protocol == ARTIFACT_PROTOCOL_V2:
                delivered = await self._send_artifact_snapshot_v2(
                    stream,
                    snapshot,
                    display_name,
                    media_type,
                    size,
                    sha256,
                    allow_auto_resume=(shared_backend is None),
                )
            else:
                delivered = await self._send_artifact_snapshot(
                    stream,
                    snapshot,
                    display_name,
                    media_type,
                    size,
                    sha256,
                )
        finally:
            snapshot.close()
        await asyncio.to_thread(
            self._unlink_source_if_same,
            source,
            source_identity,
        )
        artifact = {
            "type": "resource_link",
            "uri": delivered["uri"],
            "name": display_name,
            "size": size,
            "_meta": {
                ARTIFACT_META_KEY: {
                    "delivered": True,
                    "protocol": f"artifacts/{self.peer_artifact_protocol or 1}",
                    "sha256": sha256,
                    "localPath": delivered["path"],
                }
            },
        }
        if media_type is not None:
            artifact["mimeType"] = media_type
        return {"artifact": artifact}

    def _remember_resume_token(self, sha256: str, name: str, token: str) -> None:
        if not token:
            return
        self.publish_resume_tokens[_artifact_key(sha256, name)] = {
            "token": token,
            "createdAtNs": time.monotonic_ns(),
        }
        if len(self.publish_resume_tokens) > ARTIFACT_MAX_RESUME_TOKENS:
            cutoff = time.monotonic_ns() - int(
                self.artifact_resume_retention_seconds * 1_000_000_000
            )
            for entry_key, entry in list(self.publish_resume_tokens.items()):
                if entry.get("createdAtNs", 0) < cutoff:
                    self.publish_resume_tokens.pop(entry_key, None)

    @staticmethod
    def _is_link_loss_error(exc: BaseException) -> bool:
        if isinstance(exc, BridgeError):
            message = str(exc)
            return "peer link" in message or "stream closed" in message
        return False

    async def _attempt_artifact_send_v2(
        self,
        stream: StreamState,
        snapshot: Any,
        name: str,
        media_type: str | None,
        size: int,
        sha256: str,
        resume_token: str | None,
    ) -> dict[str, Any]:
        artifact_id = f"artifact-{uuid.uuid4().hex}"
        loop = asyncio.get_running_loop()
        waiter = ArtifactTransferWaiter(
            stream_id=stream.stream_id,
            ready=loop.create_future(),
            done=loop.create_future(),
            expected_size=size,
            expected_sha256=sha256,
        )
        self.pending_artifacts[artifact_id] = waiter
        completed = False
        try:
            begin: dict[str, Any] = {
                "type": "artifact_begin",
                "stream": stream.stream_id,
                "artifact": artifact_id,
                "name": name,
                "mediaType": media_type,
                "size": size,
                "sha256": sha256,
            }
            if resume_token:
                begin["resumeToken"] = resume_token
            await self._send_frame(begin)
            ready = await asyncio.wait_for(waiter.ready, timeout=10)
            if waiter.phase not in {"ready", "completed"}:
                raise BridgeError("artifact receiver did not enter ready phase")
            if ready.get("status") == "committed" or waiter.phase == "completed":
                result = await asyncio.wait_for(waiter.done, timeout=10)
                completed = True
                return result
            offset = ready.get("resumeOffset")
            ready_token = ready.get("resumeToken")
            if (
                not isinstance(offset, int)
                or isinstance(offset, bool)
                or not 0 <= offset <= size
                or not isinstance(ready_token, str)
                or not ready_token
            ):
                raise BridgeError("artifact receiver returned an invalid resume state")
            self._remember_resume_token(sha256, name, ready_token)
            resume_token = ready_token
            waiter.phase = "sending"
            chunk_bytes = self.peer_artifact_chunk_bytes
            total_chunks = (size + chunk_bytes - 1) // chunk_bytes if size else 0
            if offset < size:
                if offset % chunk_bytes != 0:
                    raise BridgeError("artifact resume offset is not on a chunk boundary")
                await asyncio.to_thread(snapshot.seek, offset)
                sequence = offset // chunk_bytes
                while True:
                    chunk = await asyncio.to_thread(snapshot.read, chunk_bytes)
                    if not chunk:
                        break
                    waiter.chunk_ack = loop.create_future()
                    await self._send_frame(
                        {
                            "type": "artifact_chunk",
                            "stream": stream.stream_id,
                            "artifact": artifact_id,
                            "sequence": sequence,
                            "sha256": hashlib.sha256(chunk).hexdigest(),
                            "data": base64.b64encode(chunk).decode("ascii"),
                        }
                    )
                    acknowledged = await asyncio.wait_for(
                        waiter.chunk_ack, timeout=30
                    )
                    if acknowledged != sequence:
                        raise BridgeError(
                            "artifact receiver acknowledged the wrong chunk"
                        )
                    waiter.chunk_ack = None
                    sequence += 1
                if sequence != total_chunks:
                    raise BridgeError("artifact snapshot ended before its declared size")
            waiter.phase = "end_sent"
            await self._send_frame(
                {
                    "type": "artifact_end",
                    "stream": stream.stream_id,
                    "artifact": artifact_id,
                    "chunks": total_chunks,
                    "size": size,
                    "sha256": sha256,
                }
            )
            result = await asyncio.wait_for(waiter.done, timeout=300)
            completed = True
            return result
        finally:
            self.pending_artifacts.pop(artifact_id, None)
            if not completed:
                try:
                    await self._send_frame(
                        {
                            "type": "artifact_cancel",
                            "stream": stream.stream_id,
                            "artifact": artifact_id,
                        }
                    )
                except BridgeError:
                    pass

    async def _send_artifact_snapshot_v2(
        self,
        stream: StreamState,
        snapshot: Any,
        name: str,
        media_type: str | None,
        size: int,
        sha256: str,
        *,
        allow_auto_resume: bool,
    ) -> dict[str, Any]:
        """Send one artifact with bounded auto-resume on link loss.

        Resume only repeats within the authenticated publisher identity of this
        link and only for the same declared content digest; a committed but
        unacknowledged delivery is reconciled exactly-once by the receiver
        (``artifact_committed`` fast path or end-time idempotence), never by
        overwriting an existing final file.
        """
        token_entry = self.publish_resume_tokens.get(_artifact_key(sha256, name))
        resume_token = token_entry.get("token") if token_entry else None
        deadline = time.monotonic() + self.artifact_resume_retention_seconds
        while True:
            try:
                return await self._attempt_artifact_send_v2(
                    stream,
                    snapshot,
                    name,
                    media_type,
                    size,
                    sha256,
                    resume_token,
                )
            except (BridgeError, TimeoutError, asyncio.CancelledError) as exc:
                if isinstance(exc, asyncio.CancelledError):
                    raise
                if not self._is_link_loss_error(exc):
                    raise
                token_entry = self.publish_resume_tokens.get(
                    _artifact_key(sha256, name)
                )
                if token_entry is not None:
                    resume_token = token_entry.get("token")
                if not allow_auto_resume:
                    raise BridgeError(
                        "artifact transfer interrupted by link loss; "
                        "redeliver with the same digest and name to resume"
                    ) from exc
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise BridgeError(
                        "artifact transfer did not resume before its retention window expired"
                    ) from exc
                self.log(
                    f"artifact send interrupted; waiting up to {remaining:.1f}s "
                    f"for the peer link before resuming at the last acked byte"
                )
                try:
                    await asyncio.wait_for(self.link_ready.wait(), timeout=remaining)
                except TimeoutError:
                    raise BridgeError(
                        "artifact transfer did not resume before its retention window expired"
                    ) from exc
                await asyncio.sleep(0.05)

    def _snapshot_artifact(
        self,
        source: Path,
        max_bytes: int,
    ) -> tuple[Any, int, str, tuple[int, int, int]]:
        if source.parent.resolve() != source.parent or source.parent.name == "":
            raise BridgeError("artifact staging directory is invalid")
        if source.is_symlink():
            raise BridgeError("published artifact must not be a symbolic link")
        flags = os.O_RDONLY | getattr(os, "O_BINARY", 0)
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            descriptor = os.open(source, flags)
        except OSError as exc:
            raise BridgeError("published artifact is unavailable") from exc
        source_handle = os.fdopen(descriptor, "rb", closefd=True)
        snapshot: Any | None = None
        try:
            metadata = os.fstat(source_handle.fileno())
            if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink > 1:
                raise BridgeError("published artifact must be one regular unlinked file")
            self.artifact_spool_root.mkdir(parents=True, mode=0o700, exist_ok=True)
            snapshot = tempfile.TemporaryFile(mode="w+b", dir=self.artifact_spool_root)
            digest = hashlib.sha256()
            total = 0
            while True:
                chunk = source_handle.read(ARTIFACT_CHUNK_BYTES)
                if not chunk:
                    break
                total += len(chunk)
                if total > max_bytes or total > self.max_artifact_bytes:
                    raise BridgeError("published artifact exceeds configured size limit")
                snapshot.write(chunk)
                digest.update(chunk)
            snapshot.flush()
            snapshot.seek(0)
            identity = (
                metadata.st_dev,
                metadata.st_ino,
                metadata.st_ctime_ns,
            )
            return snapshot, total, digest.hexdigest(), identity
        except Exception:
            if snapshot is not None:
                snapshot.close()
            raise
        finally:
            source_handle.close()

    @staticmethod
    def _unlink_source_if_same(
        source: Path,
        expected: tuple[int, int, int],
    ) -> None:
        try:
            metadata = source.lstat()
        except OSError:
            return
        actual = (metadata.st_dev, metadata.st_ino, metadata.st_ctime_ns)
        if actual == expected and stat.S_ISREG(metadata.st_mode):
            try:
                source.unlink()
            except OSError:
                pass

    async def _send_artifact_snapshot(
        self,
        stream: StreamState,
        snapshot: Any,
        name: str,
        media_type: str | None,
        size: int,
        sha256: str,
    ) -> dict[str, Any]:
        artifact_id = f"artifact-{uuid.uuid4().hex}"
        loop = asyncio.get_running_loop()
        waiter = ArtifactTransferWaiter(
            stream_id=stream.stream_id,
            ready=loop.create_future(),
            done=loop.create_future(),
            expected_size=size,
            expected_sha256=sha256,
        )
        self.pending_artifacts[artifact_id] = waiter
        completed = False
        try:
            await self._send_frame(
                {
                    "type": "artifact_begin",
                    "stream": stream.stream_id,
                    "artifact": artifact_id,
                    "name": name,
                    "mediaType": media_type,
                    "size": size,
                    "sha256": sha256,
                }
            )
            await asyncio.wait_for(waiter.ready, timeout=10)
            if waiter.phase != "ready":
                raise BridgeError("artifact receiver did not enter ready phase")
            waiter.phase = "sending"
            sequence = 0
            while True:
                chunk = await asyncio.to_thread(
                    snapshot.read,
                    self.peer_artifact_chunk_bytes,
                )
                if not chunk:
                    break
                waiter.chunk_ack = loop.create_future()
                await self._send_frame(
                    {
                        "type": "artifact_chunk",
                        "stream": stream.stream_id,
                        "artifact": artifact_id,
                        "sequence": sequence,
                        "data": base64.b64encode(chunk).decode("ascii"),
                    }
                )
                acknowledged = await asyncio.wait_for(waiter.chunk_ack, timeout=30)
                if acknowledged != sequence:
                    raise BridgeError("artifact receiver acknowledged the wrong chunk")
                waiter.chunk_ack = None
                sequence += 1
            waiter.phase = "end_sent"
            await self._send_frame(
                {
                    "type": "artifact_end",
                    "stream": stream.stream_id,
                    "artifact": artifact_id,
                    "chunks": sequence,
                    "size": size,
                    "sha256": sha256,
                }
            )
            result = await asyncio.wait_for(waiter.done, timeout=300)
            completed = True
            return result
        finally:
            self.pending_artifacts.pop(artifact_id, None)
            if not completed:
                try:
                    await self._send_frame(
                        {
                            "type": "artifact_cancel",
                            "stream": stream.stream_id,
                            "artifact": artifact_id,
                        }
                    )
                except BridgeError:
                    pass

    def _validate_input_source(self, value: Any) -> Path:
        """Resolve one Agent-local file only beneath Operator-authorized roots."""
        if value is None or value == "":
            raise BridgeError("inputSource must be a local absolute file path")
        if not isinstance(value, str):
            raise BridgeError("inputSource must be a local absolute file path")
        source = Path(value).expanduser()
        if not source.is_absolute() or not source.is_file():
            raise BridgeError("inputSource must be an existing local absolute file")
        resolved = source.resolve()
        if not any(
            resolved == root or resolved.is_relative_to(root)
            for root in self.allowed_artifact_roots
        ):
            raise BridgeError("inputSource is outside Operator-authorized workspace roots")
        if source.is_symlink():
            raise BridgeError("inputSource must not be a symbolic link")
        return resolved

    async def _stage_local_input(self, request: dict[str, Any]) -> dict[str, Any]:
        """Push one client-workspace file into the peer's private input stage."""
        if not self.peer_artifact_inputs:
            raise BridgeError("peer does not support artifact-inputs/1")
        if not self.allowed_artifact_roots:
            raise BridgeError("input staging requires an Operator-authorized workspace root")
        stream_id = request.get("stream")
        if not isinstance(stream_id, str) or stream_id not in self.streams:
            raise BridgeError("input staging requires an open local stream id")
        stream = self.streams[stream_id]
        if (
            stream.socket_writer is None
            or stream.opened is None
            or not stream.opened.done()
            or stream.closed.is_set()
            or stream.socket_writer.is_closing()
        ):
            raise BridgeError("input staging stream is not an active local client session")
        source = self._validate_input_source(request.get("sourcePath"))
        relative_name = self._safe_artifact_name(request.get("name") or source.name)
        media_type = request.get("mediaType")
        if media_type is not None and (
            not isinstance(media_type, str)
            or not media_type
            or len(media_type) > 255
            or any(character in media_type for character in ("\r", "\n", "\x00"))
        ):
            raise BridgeError("input mediaType is invalid")
        snapshot, size, sha256, _identity = await asyncio.to_thread(
            self._snapshot_artifact,
            source,
            self.max_artifact_bytes,
        )
        try:
            receipt = await self._send_input_snapshot(
                stream,
                snapshot,
                relative_name,
                media_type,
                size,
                sha256,
            )
        finally:
            snapshot.close()
        return receipt

    async def _send_input_snapshot(
        self,
        stream: StreamState,
        snapshot: Any,
        name: str,
        media_type: str | None,
        size: int,
        sha256: str,
    ) -> dict[str, Any]:
        """Chunk one local file to the peer's private per-stream input stage."""
        input_id = f"input-{uuid.uuid4().hex}"
        descriptor = f"{INPUT_DESCRIPTOR_MARKER}{input_id}"
        loop = asyncio.get_running_loop()
        waiter = InputTransferWaiter(
            stream_id=stream.stream_id,
            ready=loop.create_future(),
            done=loop.create_future(),
            expected_size=size,
            expected_sha256=sha256,
        )
        self.pending_inputs[input_id] = waiter
        completed = False
        try:
            await self._send_frame(
                {
                    "type": "input_begin",
                    "stream": stream.stream_id,
                    "input": input_id,
                    "name": name,
                    "mediaType": media_type,
                    "size": size,
                    "sha256": sha256,
                }
            )
            await asyncio.wait_for(waiter.ready, timeout=10)
            if waiter.phase != "ready":
                raise BridgeError("input receiver did not enter ready phase")
            waiter.phase = "sending"
            sequence = 0
            while True:
                chunk = await asyncio.to_thread(
                    snapshot.read,
                    self.peer_artifact_chunk_bytes,
                )
                if not chunk:
                    break
                waiter.chunk_ack = loop.create_future()
                await self._send_frame(
                    {
                        "type": "input_chunk",
                        "stream": stream.stream_id,
                        "input": input_id,
                        "sequence": sequence,
                        "data": base64.b64encode(chunk).decode("ascii"),
                    }
                )
                acknowledged = await asyncio.wait_for(waiter.chunk_ack, timeout=30)
                if acknowledged != sequence:
                    raise BridgeError("input receiver acknowledged the wrong chunk")
                waiter.chunk_ack = None
                sequence += 1
            waiter.phase = "end_sent"
            await self._send_frame(
                {
                    "type": "input_end",
                    "stream": stream.stream_id,
                    "input": input_id,
                    "chunks": sequence,
                    "size": size,
                    "sha256": sha256,
                }
            )
            confirmed = await asyncio.wait_for(waiter.done, timeout=300)
            completed = True
        finally:
            self.pending_inputs.pop(input_id, None)
            if not completed:
                try:
                    await self._send_frame(
                        {
                            "type": "input_cancel",
                            "stream": stream.stream_id,
                            "input": input_id,
                        }
                    )
                except BridgeError:
                    pass
        # Only the opaque handle and metadata cross back; the staged path stays
        # on the MCP host and is never exposed to the sender workspace.
        return {
            "handle": descriptor,
            "name": confirmed["name"],
            "size": confirmed["size"],
            "sha256": confirmed["sha256"],
        }

    @staticmethod
    def _fail_input_waiter(waiter: InputTransferWaiter, message: str) -> None:
        if waiter.phase == "completed":
            return
        waiter.phase = "failed"
        error = BridgeError(message)
        if not waiter.ready.done():
            waiter.ready.set_exception(error)
        elif waiter.chunk_ack is not None and not waiter.chunk_ack.done():
            waiter.chunk_ack.set_exception(error)
        elif not waiter.done.done():
            waiter.done.set_exception(error)

    def _handle_input_reply(self, frame: dict[str, Any]) -> None:
        input_id = frame.get("input")
        waiter = self.pending_inputs.get(input_id) if isinstance(input_id, str) else None
        if not waiter or frame.get("stream") != waiter.stream_id:
            return
        kind = frame.get("type")
        if kind == "input_ready":
            if waiter.phase != "begin_sent" or waiter.ready.done():
                self._fail_input_waiter(waiter, "out-of-order input_ready")
                return
            waiter.phase = "ready"
            waiter.ready.set_result(frame)
            return
        if kind == "input_chunk_ok":
            if (
                waiter.phase != "sending"
                or waiter.chunk_ack is None
                or waiter.chunk_ack.done()
                or not isinstance(frame.get("sequence"), int)
            ):
                self._fail_input_waiter(waiter, "out-of-order input_chunk_ok")
                return
            waiter.chunk_ack.set_result(frame["sequence"])
            return
        if kind == "input_ok":
            if waiter.phase != "end_sent" or waiter.done.done():
                self._fail_input_waiter(waiter, "out-of-order input_ok")
                return
            if (
                not isinstance(frame.get("name"), str)
                or frame.get("size") != waiter.expected_size
                or frame.get("sha256") != waiter.expected_sha256
            ):
                self._fail_input_waiter(waiter, "peer returned an invalid input receipt")
                return
            waiter.phase = "completed"
            waiter.done.set_result(
                {
                    "name": frame.get("name"),
                    "size": frame.get("size"),
                    "sha256": frame.get("sha256"),
                }
            )
            return
        if kind == "input_error":
            self._fail_input_waiter(
                waiter,
                str(frame.get("message", "input staging failed")),
            )

    async def _handle_input_begin(self, frame: dict[str, Any]) -> None:
        input_id = frame.get("input")
        stream_id = frame.get("stream")
        temp_path: Path | None = None
        stage: Path | None = None
        try:
            if (
                not isinstance(input_id, str)
                or not ID_PATTERN.fullmatch(input_id)
                or input_id in self.receiving_inputs
                or not isinstance(stream_id, str)
            ):
                raise BridgeError("invalid input identity")
            stream = self.streams.get(stream_id)
            if (
                not stream
                or stream.process is None
                or stream.input_stage is None
                or stream.closed.is_set()
            ):
                raise BridgeError("input staging is not enabled for this stream")
            active_for_stream = sum(
                1
                for item in self.receiving_inputs.values()
                if item.stream_id == stream_id
            )
            reserved_bytes = sum(
                item.expected_size for item in self.receiving_inputs.values()
            )
            if (
                len(self.receiving_inputs) >= MAX_CONCURRENT_INPUTS
                or active_for_stream >= MAX_CONCURRENT_INPUTS // 2
            ):
                raise BridgeError("input receive concurrency limit reached")
            name = self._safe_artifact_name(frame.get("name"))
            media_type = frame.get("mediaType")
            if media_type is not None and (
                not isinstance(media_type, str)
                or not media_type
                or len(media_type) > 255
                or any(character in media_type for character in ("\r", "\n", "\x00"))
            ):
                raise BridgeError("invalid input media type")
            size = frame.get("size")
            sha256 = frame.get("sha256")
            if (
                not isinstance(size, int)
                or isinstance(size, bool)
                or size < 0
                or size > stream.input_max_bytes
                or reserved_bytes + size > MAX_RESERVED_ARTIFACT_BYTES
                or not isinstance(sha256, str)
                or re.fullmatch(r"[0-9a-f]{64}", sha256) is None
            ):
                raise BridgeError("invalid input size or digest")
            stage = stream.input_stage
            if stage.is_symlink() or not stage.is_dir():
                raise BridgeError("input stage is unsafe")
            final_path = stage / name
            if final_path.exists() or final_path.is_symlink():
                raise BridgeError("a staged input with this name already exists")
            temp_path = stage / f".partial-{input_id}"
            partial_flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(
                os, "O_BINARY", 0
            )
            handle = os.fdopen(os.open(temp_path, partial_flags, 0o600), "wb")
            state = InputReceiveState(
                stream_id=stream_id,
                input_id=input_id,
                name=name,
                media_type=media_type,
                expected_size=size,
                temp_path=temp_path,
                final_path=final_path,
                handle=handle,
                end_sha256=sha256,
            )
            self.receiving_inputs[input_id] = state
            state.task = self._background(self._receive_input(state))
            await self._send_frame(
                {
                    "type": "input_ready",
                    "stream": stream_id,
                    "input": input_id,
                }
            )
        except Exception as exc:
            state = (
                self.receiving_inputs.get(input_id)
                if isinstance(input_id, str)
                else None
            )
            if state is not None:
                await self._abort_received_input(state, "input begin response failed")
            elif temp_path is not None:
                try:
                    temp_path.unlink()
                except OSError:
                    pass
            try:
                await self._send_frame(
                    {
                        "type": "input_error",
                        "stream": stream_id if isinstance(stream_id, str) else "",
                        "input": input_id if isinstance(input_id, str) else "",
                        "message": "input delivery was rejected",
                    }
                )
            except BridgeError:
                pass
            self.log(f"input begin rejected: {type(exc).__name__}: {exc}")

    def _handle_input_chunk(self, frame: dict[str, Any]) -> None:
        input_id = frame.get("input")
        state = self.receiving_inputs.get(input_id) if isinstance(input_id, str) else None
        if not state:
            return
        try:
            if state.phase != "receiving":
                raise BridgeError("input chunk arrived after terminal metadata")
            sequence = frame.get("sequence")
            if sequence != state.next_sequence or frame.get("stream") != state.stream_id:
                raise BridgeError("input chunk sequence is invalid")
            data = base64.b64decode(frame.get("data", ""), validate=True)
            if len(data) > self.peer_artifact_chunk_bytes:
                raise BridgeError("input chunk exceeds negotiated limit")
            state.queue.put_nowait((sequence, data))
            state.next_sequence += 1
        except Exception as exc:
            self._schedule_input_abort(state, str(exc))

    def _handle_input_end(self, frame: dict[str, Any]) -> None:
        input_id = frame.get("input")
        state = self.receiving_inputs.get(input_id) if isinstance(input_id, str) else None
        if not state:
            return
        try:
            if state.phase != "receiving":
                raise BridgeError("duplicate or out-of-order input end")
            if (
                frame.get("stream") != state.stream_id
                or frame.get("chunks") != state.next_sequence
                or frame.get("size") != state.expected_size
                or frame.get("sha256") != state.end_sha256
            ):
                raise BridgeError("input end metadata does not match")
            state.phase = "ending"
            state.queue.put_nowait(None)
        except asyncio.QueueFull:
            state.end_task = self._background(state.queue.put(None))
        except Exception as exc:
            self._schedule_input_abort(state, str(exc))

    async def _receive_input(self, state: InputReceiveState) -> None:
        try:
            while True:
                item = await state.queue.get()
                if item is None:
                    break
                sequence, data = item
                state.received += len(data)
                if state.received > state.expected_size:
                    raise BridgeError("input byte count exceeds announcement")
                state.digest.update(data)
                await asyncio.to_thread(state.handle.write, data)
                await self._send_frame(
                    {
                        "type": "input_chunk_ok",
                        "stream": state.stream_id,
                        "input": state.input_id,
                        "sequence": sequence,
                    }
                )
            if (
                state.received != state.expected_size
                or state.digest.hexdigest() != state.end_sha256
            ):
                raise BridgeError("input integrity verification failed")
            await asyncio.to_thread(state.handle.flush)
            await asyncio.to_thread(os.fsync, state.handle.fileno())
            state.handle.close()
            if state.phase != "ending":
                raise BridgeError("input reached commit before terminal metadata")
            state.phase = "committing"
            state.commit_task = asyncio.create_task(
                asyncio.to_thread(
                    self._commit_artifact_no_overwrite,
                    state.temp_path,
                    state.final_path,
                )
            )
            await asyncio.shield(state.commit_task)
            if state.phase == "aborting":
                try:
                    state.final_path.unlink()
                except OSError:
                    pass
                raise BridgeError("input commit was cancelled")
            try:
                state.final_path.chmod(0o600)
            except OSError:
                pass
            state.phase = "completed"
            self.receiving_inputs.pop(state.input_id, None)
            stream = self.streams.get(state.stream_id)
            if stream is None or stream.input_handles is None:
                raise BridgeError("input stream disappeared before commit")
            stream.input_handles[state.input_id] = state.final_path
            await self._send_frame(
                {
                    "type": "input_ok",
                    "stream": state.stream_id,
                    "input": state.input_id,
                    "name": state.name,
                    "size": state.received,
                    "sha256": state.digest.hexdigest(),
                }
            )
        except asyncio.CancelledError:
            if state.commit_task is not None:
                try:
                    await asyncio.shield(state.commit_task)
                except Exception:
                    pass
            if state.phase != "completed":
                try:
                    state.final_path.unlink()
                except OSError:
                    pass
            self._cleanup_received_input(state)
            raise
        except Exception as exc:
            self.receiving_inputs.pop(state.input_id, None)
            if state.phase != "completed":
                try:
                    state.final_path.unlink()
                except OSError:
                    pass
            self._cleanup_received_input(state)
            try:
                await self._send_frame(
                    {
                        "type": "input_error",
                        "stream": state.stream_id,
                        "input": state.input_id,
                        "message": "input delivery failed integrity or confinement checks",
                    }
                )
            except BridgeError:
                pass
            self.log(f"input receive failed: {type(exc).__name__}: {exc}")

    def _schedule_input_abort(
        self,
        state: InputReceiveState,
        reason: str,
    ) -> None:
        if state.phase in {"aborting", "completed"}:
            return
        state.phase = "aborting"
        state.abort_task = self._background(
            self._abort_received_input(state, reason)
        )

    async def _abort_received_input(
        self,
        state: InputReceiveState,
        reason: str,
    ) -> None:
        if state.phase == "completed":
            return
        state.phase = "aborting"
        current = asyncio.current_task()
        if state.task and state.task is not current and not state.task.done():
            state.task.cancel()
            await asyncio.gather(state.task, return_exceptions=True)
        if state.commit_task is not None and not state.commit_task.done():
            try:
                await asyncio.shield(state.commit_task)
            except Exception:
                pass
        try:
            state.final_path.unlink()
        except OSError:
            pass
        self.receiving_inputs.pop(state.input_id, None)
        self._cleanup_received_input(state)
        try:
            await self._send_frame(
                {
                    "type": "input_error",
                    "stream": state.stream_id,
                    "input": state.input_id,
                    "message": "input delivery failed integrity or confinement checks",
                }
            )
        except BridgeError:
            pass
        self.log(f"input receive aborted: {reason}")

    @staticmethod
    def _cleanup_received_input(state: InputReceiveState) -> None:
        if state.end_task and not state.end_task.done():
            state.end_task.cancel()
        try:
            state.handle.close()
        except OSError:
            pass
        try:
            state.temp_path.unlink()
        except OSError:
            pass

    def _rewrite_input_descriptor_line(
        self,
        stream: StreamState,
        line: bytes,
    ) -> tuple[bytes | None, dict[str, Any] | None]:
        """Replace only exact staged-input descriptors in one tools/call line.

        Returns (rewritten bytes, None) on success, (None, error response) to
        fail closed on a foreign or expired handle, or the original line when no
        descriptor is present. Path-looking text is never inferred.
        """
        if INPUT_DESCRIPTOR_MARKER.encode("ascii") not in line:
            return line, None
        try:
            message = json.loads(line)
        except (json.JSONDecodeError, UnicodeDecodeError):
            return line, None
        if (
            not isinstance(message, dict)
            or message.get("method") != "tools/call"
            or not isinstance(message.get("params"), dict)
        ):
            return line, None
        arguments = message["params"].get("arguments")
        if not isinstance(arguments, dict):
            return line, None

        wanted: list[str] = []

        def visit(value: Any) -> None:
            if isinstance(value, str):
                if value.startswith(INPUT_DESCRIPTOR_MARKER):
                    wanted.append(value)
                return
            if isinstance(value, list):
                for item in value:
                    visit(item)
                return
            if isinstance(value, dict):
                for item in value.values():
                    visit(item)

        visit(arguments)
        if not wanted:
            return line, None
        replacements: list[tuple[bytes, bytes]] = []
        for descriptor in wanted:
            input_id = descriptor[len(INPUT_DESCRIPTOR_MARKER):]
            if input_id not in stream.input_handles:
                return None, {
                    "jsonrpc": "2.0",
                    "id": message.get("id"),
                    "error": {
                        "code": -32602,
                        "message": (
                            "tool argument references an unknown, foreign, or "
                            "expired staged input handle"
                        ),
                        "data": {
                            "protocol": "artifact-inputs/1",
                            "stage": "remote-input-stage",
                        },
                    },
                }
            original = json.dumps(descriptor).encode("utf-8")
            staged_path = json.dumps(str(stream.input_handles[input_id])).encode(
                "utf-8"
            )
            replacements.append((original, staged_path))
        occurrence_total = sum(line.count(original) for original, _ in replacements)
        if occurrence_total != len(wanted):
            # The descriptor literal is embedded inside other JSON text; fail
            # closed rather than rewriting bytes we cannot attribute exactly.
            return None, {
                "jsonrpc": "2.0",
                "id": message.get("id"),
                "error": {
                    "code": -32602,
                    "message": "ambiguous staged input descriptor in tool arguments",
                    "data": {"protocol": "artifact-inputs/1"},
                },
            }
        rewritten = line
        for original, staged_path in sorted(
            replacements, key=lambda item: line.find(item[0]), reverse=True
        ):
            rewritten = rewritten.replace(original, staged_path, 1)
        return rewritten, None

    async def _consume_dedicated_input(
        self,
        stream: StreamState,
        data: bytes,
    ) -> None:
        """Newline-buffer one dedicated MCP leg and rewrite exact input handles."""
        assert stream.process and stream.process.stdin
        buffer = stream.input_rewrite_buffer
        buffer.extend(data)
        while True:
            newline = buffer.find(b"\n")
            if newline < 0:
                if len(buffer) > INPUT_LINE_CAP_BYTES and stream.process.stdin:
                    # Degrade to raw forwarding for a single oversized line.
                    payload = bytes(buffer)
                    buffer.clear()
                    stream.process.stdin.write(payload)
                    await stream.process.stdin.drain()
                break
            line = bytes(buffer[: newline])
            del buffer[: newline + 1]
            if not line:
                if stream.process.stdin:
                    stream.process.stdin.write(b"\n")
                    await stream.process.stdin.drain()
                continue
            forwarded, error = self._rewrite_input_descriptor_line(stream, line)
            if error is not None:
                if not stream.closed.is_set():
                    try:
                        await self._send_jsonrpc_to_stream(stream, error)
                    except (BridgeError, ConnectionError, OSError):
                        pass
                continue
            if stream.process.stdin:
                stream.process.stdin.write(forwarded)
                stream.process.stdin.write(b"\n")
                await stream.process.stdin.drain()

    async def _serve_local_stream(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        target: str,
        *,
        artifact_inbox: Path | None,
        compatibility_http: bool = False,
    ) -> None:
        await asyncio.wait_for(self.link_ready.wait(), timeout=10)
        stream_id = f"{self.side}-{uuid.uuid4().hex}"
        loop = asyncio.get_running_loop()
        stream = StreamState(
            stream_id=stream_id,
            socket_writer=writer,
            opened=loop.create_future(),
            target=target,
            artifact_inbox=artifact_inbox,
        )
        self.streams[stream_id] = stream
        consumer = asyncio.create_task(self._consume_stream_input(stream))
        stream.tasks.add(consumer)
        await self._send_frame(
            {
                "type": "open",
                "stream": stream_id,
                "target": target,
                "compatibilityHttp": compatibility_http,
            }
        )
        handshake_sent = False
        try:
            await asyncio.wait_for(stream.opened, timeout=10)
            writer.write(
                _json_bytes(
                    {"ok": True, "stream": stream_id, "coreVersion": SERVER_VERSION}
                )
                + b"\n"
            )
            await writer.drain()
            handshake_sent = True
            pump = asyncio.create_task(self._pump_local_input(stream, reader))
            stream.tasks.add(pump)
            closed_wait = asyncio.create_task(stream.closed.wait())
            done, pending = await asyncio.wait({pump, closed_wait}, return_when=asyncio.FIRST_COMPLETED)
            if pump in done and not stream.closed.is_set():
                error = pump.exception()
                if error is not None:
                    raise error
                await self._send_frame({"type": "eof", "stream": stream_id})
                try:
                    await asyncio.wait_for(
                        stream.closed.wait(),
                        timeout=STREAM_EOF_GRACE_SECONDS,
                    )
                except TimeoutError:
                    await self._close_stream(stream_id, remote=False)
            for task in pending:
                task.cancel()
        except Exception:
            if handshake_sent:
                await self._close_stream(stream_id, remote=False)
                return
            removed = self.streams.pop(stream_id, None)
            if removed:
                removed.closed.set()
                for task in tuple(removed.tasks):
                    if not task.done():
                        task.cancel()
            try:
                await self._send_frame({"type": "close", "stream": stream_id})
            except BridgeError:
                pass
            raise

    async def _send_jsonrpc_to_stream(
        self, stream: StreamState, message: dict[str, Any]
    ) -> None:
        data = _json_bytes(message) + b"\n"
        async with stream.send_lock:
            await self._send_stream_data(stream, data)

    async def _send_stream_data(self, stream: StreamState, data: bytes) -> None:
        await self._maybe_capture(stream, "outbound", data)
        if stream.outbound_ack is not None:
            raise BridgeError("stream already has unacknowledged data")
        sequence = stream.outbound_sequence
        acknowledgement = asyncio.get_running_loop().create_future()
        stream.outbound_ack = acknowledgement
        try:
            await self._send_frame(
                {
                    "type": "data",
                    "stream": stream.stream_id,
                    "sequence": sequence,
                    "data": base64.b64encode(data).decode("ascii"),
                }
            )
            received = await asyncio.wait_for(
                acknowledgement,
                timeout=STREAM_DATA_ACK_TIMEOUT_SECONDS,
            )
            if received != sequence:
                raise BridgeError("peer acknowledged the wrong stream data sequence")
            stream.outbound_sequence += 1
        except Exception:
            self._background(self._close_stream(stream.stream_id, remote=False))
            raise
        finally:
            if stream.outbound_ack is acknowledgement:
                stream.outbound_ack = None

    async def _pump_local_input(self, stream: StreamState, reader: asyncio.StreamReader) -> None:
        while True:
            data = await reader.read(BUFFER_SIZE)
            if not data:
                return
            await self._send_stream_data(stream, data)

    async def _start_http_stdio_compatibility(
        self, stream_id: str, target: str, entry: dict[str, Any]
    ) -> None:
        transport = entry.get("transport", {})
        endpoint = transport.get("endpoint")
        if not isinstance(endpoint, str):
            await self._send_frame(
                {"type": "open_error", "stream": stream_id, "message": "HTTP endpoint is unavailable"}
            )
            return
        adapter = Path(__file__).resolve().with_name("streamable_http_stdio.py")
        if not adapter.is_file():
            await self._send_frame(
                {"type": "open_error", "stream": stream_id, "message": "HTTP compatibility adapter is not installed"}
            )
            return
        env = os.environ.copy()
        headers = transport.get("headers", {})
        if headers:
            env["WIN_WSL_MCP_BRIDGE_HTTP_HEADERS"] = _canonical_json(headers)
        stream = StreamState(stream_id=stream_id, target=target)
        self.streams[stream_id] = stream
        try:
            kwargs: dict[str, Any] = {
                "stdin": asyncio.subprocess.PIPE,
                "stdout": asyncio.subprocess.PIPE,
                "stderr": asyncio.subprocess.PIPE,
                "env": env,
            }
            if os.name != "nt":
                kwargs["start_new_session"] = True
            process = await asyncio.create_subprocess_exec(
                sys.executable, str(adapter), "--url", endpoint,
                "--protocol-era", transport.get("protocolEra", "legacy"), **kwargs
            )
            stream.process = process
            await self._send_frame({"type": "open_ok", "stream": stream_id})
            consumer = asyncio.create_task(self._consume_stream_input(stream))
            stdout_task = asyncio.create_task(self._pump_process_output(stream))
            stderr_task = asyncio.create_task(self._pump_process_stderr(stream, target))
            wait_task = asyncio.create_task(
                self._wait_process(stream, stdout_task, stderr_task)
            )
            stream.tasks.update({consumer, stdout_task, stderr_task, wait_task})
        except Exception as exc:
            self.streams.pop(stream_id, None)
            await self._send_frame(
                {
                    "type": "open_error", "stream": stream_id,
                    "code": "backend_unavailable", "retryable": True,
                    "message": f"HTTP compatibility adapter failed to start: {type(exc).__name__}",
                }
            )

    async def _handle_remote_open(self, frame: dict[str, Any]) -> None:
        stream_id = frame.get("stream")
        target = frame.get("target")
        if (
            not isinstance(stream_id, str)
            or not ID_PATTERN.fullmatch(stream_id)
            or not isinstance(target, str)
            or not ID_PATTERN.fullmatch(target)
        ):
            await self._send_frame(
                {
                    "type": "open_error",
                    "stream": stream_id if isinstance(stream_id, str) else "",
                    "message": "invalid open frame",
                }
            )
            return
        if stream_id in self.streams:
            await self._send_frame(
                {
                    "type": "open_error",
                    "stream": stream_id,
                    "message": "duplicate stream id",
                }
            )
            return
        try:
            entry = self.registry.launch(target)
        except BridgeError as exc:
            await self._send_frame(
                {"type": "open_error", "stream": stream_id, "message": str(exc)}
            )
            return
        transport_type = entry.get("transport", {}).get("type", "stdio")
        compatibility_http = frame.get("compatibilityHttp") is True
        if transport_type != "stdio":
            if compatibility_http and transport_type == "streamable-http":
                await self._start_http_stdio_compatibility(stream_id, target, entry)
                return
            await self._send_frame(
                {
                    "type": "open_error",
                    "stream": stream_id,
                    "code": "unsupported_transport",
                    "retryable": False,
                    "message": "streamable-http registration requires native HTTP projection or an explicit compatibility route",
                }
            )
            return
        if compatibility_http:
            await self._send_frame(
                {
                    "type": "open_error", "stream": stream_id,
                    "code": "transport_mismatch", "retryable": False,
                    "message": "HTTP-to-stdio compatibility route requires a streamable-http registration",
                }
            )
            return
        process_config = entry.get("process") or {}
        if process_config.get("multiProcessAllowed") is False:
            stream = StreamState(stream_id=stream_id, target=target)
            self.streams[stream_id] = stream
            backend = self.shared_backends.get(target)
            if backend is None:
                backend = SharedBackend(self, target, entry)
                self.shared_backends[target] = backend
            else:
                backend.refresh_entry_if_exited(entry)
            try:
                await backend.attach(stream)
            except asyncio.CancelledError:
                self.streams.pop(stream_id, None)
                raise
            except DrainingError as exc:
                self.streams.pop(stream_id, None)
                self.log(
                    f"refused shared registered MCP {target}: {exc}"
                )
                await self._send_frame(
                    {
                        "type": "open_error",
                        "stream": stream_id,
                        "message": "registered MCP is draining and not accepting new streams",
                    }
                )
                return
            except Exception as exc:
                self.streams.pop(stream_id, None)
                self.log(
                    f"failed to start shared registered MCP {target}: "
                    f"{type(exc).__name__}: {exc}"
                )
                await self._send_frame(
                    {
                        "type": "open_error",
                        "stream": stream_id,
                        "message": "registered MCP failed to start; inspect local bridge diagnostics",
                    }
                )
                return
            await self._send_frame({"type": "open_ok", "stream": stream_id})
            consumer = asyncio.create_task(backend.consume_client_input(stream))
            stream.tasks.add(consumer)
            return
        artifact_config = entry.get("artifactDelivery", {"enabled": False})
        artifact_enabled = bool(artifact_config.get("enabled")) and self.peer_artifacts
        artifact_token: str | None = None
        artifact_stage: Path | None = None
        input_config = entry.get("inputDelivery", {"enabled": False})
        input_enabled = bool(input_config.get("enabled")) and self.peer_artifact_inputs
        input_stage: Path | None = None
        environment = os.environ.copy()
        for key in ARTIFACT_ENV_KEYS | INPUT_ENV_KEYS:
            environment.pop(key, None)
        environment.update(entry.get("env", {}))
        if artifact_enabled:
            try:
                generation = self.link_generation or "disconnected"
                artifact_stage = self.artifact_spool_root / generation / stream_id
                artifact_stage.mkdir(parents=True, mode=0o700, exist_ok=False)
                try:
                    artifact_stage.chmod(0o700)
                except OSError:
                    pass
                artifact_token = secrets.token_urlsafe(32)
                environment.update(
                    {
                        "WIN_WSL_MCP_BRIDGE_ARTIFACT_STAGE": str(artifact_stage),
                        "WIN_WSL_MCP_BRIDGE_ARTIFACT_TOKEN": artifact_token,
                        "WIN_WSL_MCP_BRIDGE_ARTIFACT_LOCAL_HOST": self.local_host,
                        "WIN_WSL_MCP_BRIDGE_ARTIFACT_LOCAL_PORT": str(self.local_port),
                        "WIN_WSL_MCP_BRIDGE_ARTIFACT_PROTOCOL": "artifacts/1",
                        "WIN_WSL_MCP_BRIDGE_ARTIFACT_PYTHON": sys.executable,
                        "WIN_WSL_MCP_BRIDGE_ARTIFACT_PUBLISHER": str(
                            Path(__file__).with_name("bridge_publisher.py").resolve()
                        ),
                    }
                )
            except OSError as exc:
                self.log(
                    f"failed to prepare artifact staging for {target}: "
                    f"{type(exc).__name__}: {exc}"
                )
                await self._send_frame(
                    {
                        "type": "open_error",
                        "stream": stream_id,
                        "message": "registered MCP artifact staging failed; inspect local bridge diagnostics",
                    }
                )
                return
        if input_enabled:
            try:
                generation = self.link_generation or "disconnected"
                input_stage = self.artifact_spool_root / generation / f"{stream_id}-inputs"
                input_stage.mkdir(parents=True, mode=0o700, exist_ok=False)
                try:
                    input_stage.chmod(0o700)
                except OSError:
                    pass
                environment.update(
                    {
                        "WIN_WSL_MCP_BRIDGE_INPUT_STAGE": str(input_stage),
                        "WIN_WSL_MCP_BRIDGE_INPUT_PROTOCOL": "artifact-inputs/1",
                        "WIN_WSL_MCP_BRIDGE_INPUT_ENABLED": "1",
                    }
                )
            except OSError as exc:
                self.log(
                    f"failed to prepare input staging for {target}: "
                    f"{type(exc).__name__}: {exc}"
                )
                if artifact_stage is not None:
                    shutil.rmtree(artifact_stage, ignore_errors=True)
                if artifact_token is not None:
                    self.artifact_publishers.pop(artifact_token, None)
                await self._send_frame(
                    {
                        "type": "open_error",
                        "stream": stream_id,
                        "message": "registered MCP input staging failed; inspect local bridge diagnostics",
                    }
                )
                return
        stream = StreamState(
            stream_id=stream_id,
            target=target,
            artifact_stage=artifact_stage,
            artifact_token=artifact_token,
            artifact_max_bytes=min(
                int(artifact_config.get("maxBytes", DEFAULT_MAX_ARTIFACT_BYTES)),
                self.max_artifact_bytes,
            ),
            input_stage=input_stage,
            input_max_bytes=min(
                int(input_config.get("maxBytes", MAX_STAGED_INPUT_BYTES)),
                self.max_artifact_bytes,
            ),
        )
        self.streams[stream_id] = stream
        if artifact_token is not None:
            self.artifact_publishers[artifact_token] = stream_id
        try:
            process = await asyncio.create_subprocess_exec(
                entry["command"],
                *entry.get("args", []),
                cwd=entry.get("cwd") or None,
                env=environment,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stream.process = process
        except Exception as exc:
            self.streams.pop(stream_id, None)
            if artifact_token is not None:
                self.artifact_publishers.pop(artifact_token, None)
            if artifact_stage is not None:
                shutil.rmtree(artifact_stage, ignore_errors=True)
            if input_stage is not None:
                shutil.rmtree(input_stage, ignore_errors=True)
            self.log(f"failed to start registered MCP {target}: {type(exc).__name__}: {exc}")
            await self._send_frame(
                {
                    "type": "open_error",
                    "stream": stream_id,
                    "message": "registered MCP failed to start; inspect local bridge diagnostics",
                }
            )
            return
        await self._send_frame({"type": "open_ok", "stream": stream_id})
        consumer = asyncio.create_task(self._consume_stream_input(stream))
        stdout_task = asyncio.create_task(self._pump_process_output(stream))
        stderr_task = asyncio.create_task(self._pump_process_stderr(stream, target))
        wait_task = asyncio.create_task(
            self._wait_process(stream, stdout_task, stderr_task)
        )
        stream.tasks.update({consumer, stdout_task, stderr_task, wait_task})

    async def _pump_process_output(self, stream: StreamState) -> None:
        assert stream.process and stream.process.stdout
        while True:
            data = await stream.process.stdout.read(BUFFER_SIZE)
            if not data:
                return
            await self._send_stream_data(stream, data)

    async def _pump_process_stderr(self, stream: StreamState, target: str) -> None:
        assert stream.process and stream.process.stderr
        while True:
            data = await stream.process.stderr.read(BUFFER_SIZE)
            if not data:
                return
            text = data.decode("utf-8", errors="replace").rstrip()
            self.log(f"{target} stderr: {text}")

    async def _wait_process(
        self,
        stream: StreamState,
        stdout_task: asyncio.Task[Any],
        stderr_task: asyncio.Task[Any],
    ) -> None:
        assert stream.process
        code = await stream.process.wait()
        await asyncio.gather(stdout_task, stderr_task, return_exceptions=True)
        try:
            await self._send_frame(
                {"type": "close", "stream": stream.stream_id, "exitCode": code}
            )
        except BridgeError:
            pass
        await self._close_stream(stream.stream_id, remote=True)

    async def _consume_stream_input(self, stream: StreamState) -> None:
        try:
            while True:
                item = await stream.inbound.get()
                if item is None:
                    if stream.process and stream.process.stdin:
                        stream.process.stdin.close()
                    elif stream.socket_writer:
                        try:
                            stream.socket_writer.write_eof()
                        except (AttributeError, OSError):
                            pass
                    return
                sequence, data = item
                if stream.socket_writer:
                    stream.socket_writer.write(data)
                    await stream.socket_writer.drain()
                elif stream.process and stream.process.stdin:
                    if stream.input_stage is not None:
                        await self._consume_dedicated_input(stream, data)
                    else:
                        stream.process.stdin.write(data)
                        await stream.process.stdin.drain()
                else:
                    raise BridgeError("stream has no downstream input")
                await self._send_frame(
                    {
                        "type": "data_ok",
                        "stream": stream.stream_id,
                        "sequence": sequence,
                    }
                )
        except (BridgeError, BrokenPipeError, ConnectionError, OSError) as exc:
            self.log(f"stream {stream.stream_id} input closed: {exc}")
            self._background(self._close_stream(stream.stream_id, remote=False))

    # ---- artifacts/2: content-addressed durable receipts and resume ------

    @staticmethod
    def _v2_content_dir(inbox: Path, sha256: str) -> Path:
        return inbox / ".mcp-artifacts" / sha256

    def _artifact_journal_path(self, inbox: Path) -> Path:
        return inbox / ".mcp-artifacts" / ARTIFACT_JOURNAL_NAME

    def _journal_lock_for(self, inbox: Path) -> asyncio.Lock:
        key = str(inbox.resolve())
        lock = self.artifact_journal_locks.get(key)
        if lock is None:
            lock = asyncio.Lock()
            self.artifact_journal_locks[key] = lock
        return lock

    async def _mutate_artifact_journal(self, inbox: Path, mutator: Any) -> Any:
        path = self._artifact_journal_path(inbox)
        async with self._journal_lock_for(inbox):
            journal = await asyncio.to_thread(_read_artifact_journal, path)
            result = mutator(journal)
            await asyncio.to_thread(_write_artifact_journal, path, journal)
            return result

    @staticmethod
    def _journal_rel_path(artifact_root: Path, relative: Any) -> Path | None:
        if not isinstance(relative, str) or not relative:
            return None
        path = Path(relative)
        if path.is_absolute() or ".." in path.parts:
            return None
        resolved_root = artifact_root.resolve()
        candidate = (artifact_root / path).resolve()
        if candidate.parent != resolved_root and not candidate.is_relative_to(
            resolved_root
        ):
            return None
        return artifact_root / path

    @staticmethod
    def _journal_partial(
        artifact_root: Path, record: dict[str, Any]
    ) -> Path | None:
        path = BridgeNode._journal_rel_path(artifact_root, record.get("partialRel"))
        if path is None or not path.name.startswith(".partial-"):
            return None
        return path

    @staticmethod
    def _final_artifact_path(artifact_root: Path, record: dict[str, Any]) -> Path | None:
        path = BridgeNode._journal_rel_path(artifact_root, record.get("finalRel"))
        if path is None or path.name.startswith(".partial-"):
            return None
        return path

    async def _prune_artifact_receipts(self, inbox: Path) -> None:
        """Expire or drop durable receipt state beneath one inbox."""
        retention_ns = int(self.artifact_resume_retention_seconds * 1_000_000_000)
        now = time.monotonic_ns()
        artifact_root = inbox / ".mcp-artifacts"

        def prune(journal: dict[str, Any]) -> int:
            records = journal["records"]
            for token, record in list(records.items()):
                if not isinstance(record, dict):
                    records.pop(token, None)
                    continue
                age = now - int(record.get("updatedAtNs") or 0)
                if age <= retention_ns:
                    continue
                records.pop(token, None)
                partial = BridgeNode._journal_partial(artifact_root, record)
                if partial is not None:
                    try:
                        partial.unlink()
                    except OSError:
                        pass
            for token, record in list(records.items()):
                if not isinstance(record, dict) or record.get("state") != "partial":
                    continue
                partial = BridgeNode._journal_partial(artifact_root, record)
                if partial is None or not partial.is_file():
                    records.pop(token, None)
            return len(records)

        await self._mutate_artifact_journal(inbox, prune)

    async def _handle_artifact_begin_v2(self, frame: dict[str, Any]) -> None:
        artifact_id: Any = frame.get("artifact")
        stream_id: Any = frame.get("stream")
        inbox: Path | None = None
        try:
            if (
                not isinstance(artifact_id, str)
                or not ID_PATTERN.fullmatch(artifact_id)
                or artifact_id in self.receiving_artifacts
                or not isinstance(stream_id, str)
            ):
                raise BridgeError("invalid artifact identity")
            stream = self.streams.get(stream_id)
            if not stream or stream.artifact_inbox is None:
                raise BridgeError("artifact workspace delivery is not configured")
            active_for_stream = sum(
                1
                for item in self.receiving_artifacts.values()
                if item.stream_id == stream_id
            )
            reserved_bytes = sum(
                item.expected_size for item in self.receiving_artifacts.values()
            )
            if (
                len(self.receiving_artifacts) >= MAX_CONCURRENT_ARTIFACTS
                or active_for_stream >= MAX_CONCURRENT_ARTIFACTS // 2
            ):
                raise BridgeError("artifact receive concurrency limit reached")
            name = self._safe_artifact_name(frame.get("name"))
            media_type = frame.get("mediaType")
            if media_type is not None and (
                not isinstance(media_type, str)
                or not media_type
                or len(media_type) > 255
                or any(character in media_type for character in ("\r", "\n", "\x00"))
            ):
                raise BridgeError("invalid artifact media type")
            size = frame.get("size")
            sha256 = frame.get("sha256")
            if (
                not isinstance(size, int)
                or isinstance(size, bool)
                or size < 0
                or size > self.max_artifact_bytes
                or reserved_bytes + size > MAX_RESERVED_ARTIFACT_BYTES
                or not isinstance(sha256, str)
                or re.fullmatch(r"[0-9a-f]{64}", sha256) is None
            ):
                raise BridgeError("invalid artifact size or digest")
            resume_token = frame.get("resumeToken")
            if resume_token is not None and not (
                isinstance(resume_token, str)
                and 1 <= len(resume_token) <= 128
                and re.fullmatch(r"[0-9a-f]+", resume_token) is not None
            ):
                raise BridgeError("invalid artifact resume token")
            inbox = self._validate_artifact_inbox(str(stream.artifact_inbox))
            assert inbox is not None
            artifact_root = inbox / ".mcp-artifacts"
            if artifact_root.exists() and (
                artifact_root.is_symlink() or not artifact_root.is_dir()
            ):
                raise BridgeError("artifact workspace root is unsafe")
            artifact_root.mkdir(mode=0o700, exist_ok=True)
            if artifact_root.resolve().parent != inbox:
                raise BridgeError("artifact workspace root escaped its inbox")
            content_dir = artifact_root / sha256
            await self._prune_artifact_receipts(inbox)

            def prepare(journal: dict[str, Any]) -> dict[str, Any]:
                records = journal["records"]
                record = records.get(resume_token) if resume_token else None
                if record is not None and not isinstance(record, dict):
                    raise BridgeError("artifact resume token is invalid")
                if record is not None and (
                    record.get("sha256") != sha256
                    or record.get("name") != name
                    or record.get("size") != size
                ):
                    raise BridgeError(
                        "artifact resume token does not match the declared digest"
                    )
                if record is not None and record.get("state") == "committed":
                    final = BridgeNode._final_artifact_path(artifact_root, record)
                    if final is None or not final.is_file():
                        raise BridgeError("artifact committed record is missing its file")
                    return {"committed": final}
                if record is not None:
                    partial = BridgeNode._journal_partial(artifact_root, record)
                    if partial is None or not partial.is_file():
                        records.pop(resume_token, None)
                        record = None
                if record is None:
                    token = secrets.token_hex(24)
                    partial_name = f".partial-{secrets.token_hex(8)}"
                    content_dir.mkdir(mode=0o700, exist_ok=True)
                    partial = content_dir / partial_name
                    handle = os.fdopen(
                        os.open(
                            partial,
                            os.O_WRONLY
                            | os.O_CREAT
                            | os.O_EXCL
                            | getattr(os, "O_BINARY", 0),
                            0o600,
                        ),
                        "wb",
                    )
                    records[token] = {
                        "token": token,
                        "sha256": sha256,
                        "name": name,
                        "size": size,
                        "state": "partial",
                        "ackedBytes": 0,
                        "chunks": 0,
                        "partialRel": f"{sha256}/{partial_name}",
                        "updatedAtNs": time.monotonic_ns(),
                    }
                    return {
                        "token": token,
                        "handle": handle,
                        "partial": partial,
                        "ackedBytes": 0,
                        "chunks": 0,
                        "created": True,
                    }
                partial = BridgeNode._journal_partial(artifact_root, record)
                assert partial is not None
                acked_bytes = int(record.get("ackedBytes") or 0)
                chunks = int(record.get("chunks") or 0)
                content_dir.mkdir(mode=0o700, exist_ok=True)
                handle = os.fdopen(
                    os.open(partial, os.O_RDWR | getattr(os, "O_BINARY", 0)), "r+b"
                )
                try:
                    present = os.fstat(handle.fileno()).st_size
                    if present < acked_bytes or present > size:
                        raise BridgeError("artifact parked partial is inconsistent")
                    if present > acked_bytes:
                        handle.truncate(acked_bytes)
                        handle.flush()
                        os.fsync(handle.fileno())
                    handle.seek(acked_bytes)
                except BaseException:
                    handle.close()
                    raise
                return {
                    "token": resume_token,
                    "handle": handle,
                    "partial": partial,
                    "ackedBytes": acked_bytes,
                    "chunks": chunks,
                    "created": True,
                }

            outcome = await self._mutate_artifact_journal(inbox, prepare)
            if "committed" in outcome:
                final = outcome["committed"]
                await self._send_frame(
                    {
                        "type": "artifact_committed",
                        "stream": stream_id,
                        "artifact": artifact_id,
                        "uri": final.as_uri(),
                        "path": str(final),
                        "size": size,
                        "sha256": sha256,
                    }
                )
                return
            token = outcome["token"]
            handle = outcome["handle"]
            partial = outcome["partial"]
            acked_bytes = outcome["ackedBytes"]
            state = ArtifactReceiveStateV2(
                stream_id=stream_id,
                artifact_id=artifact_id,
                name=name,
                media_type=media_type,
                expected_size=size,
                sha256=sha256,
                token=token,
                inbox=inbox,
                content_dir=content_dir,
                partial_path=partial,
                handle=handle,
                received=acked_bytes,
                acked_bytes=acked_bytes,
                next_sequence=outcome["chunks"],
            )
            self.receiving_artifacts[artifact_id] = state
            state.task = self._background(self._receive_artifact_v2(state))
            await self._send_frame(
                {
                    "type": "artifact_ready",
                    "stream": stream_id,
                    "artifact": artifact_id,
                    "status": "receiving",
                    "resumeOffset": acked_bytes,
                    "resumeToken": token,
                }
            )
        except Exception as exc:
            state = (
                self.receiving_artifacts.get(artifact_id)
                if isinstance(artifact_id, str)
                else None
            )
            if state is not None:
                await self._abort_artifact_v2(state, "artifact begin response failed")
            elif inbox is not None:
                await self._prune_artifact_receipts(inbox)
            try:
                await self._send_frame(
                    {
                        "type": "artifact_error",
                        "stream": stream_id if isinstance(stream_id, str) else "",
                        "artifact": artifact_id if isinstance(artifact_id, str) else "",
                        "message": "artifact delivery was rejected",
                    }
                )
            except BridgeError:
                pass
            self.log(f"artifact begin rejected: {type(exc).__name__}: {exc}")

    def _handle_artifact_chunk_v2(
        self, state: ArtifactReceiveStateV2, frame: dict[str, Any]
    ) -> None:
        try:
            if state.phase != "receiving":
                raise BridgeError("artifact chunk arrived after terminal metadata")
            sequence = frame.get("sequence")
            if (
                not isinstance(sequence, int)
                or isinstance(sequence, bool)
                or sequence != state.next_sequence
                or frame.get("stream") != state.stream_id
            ):
                raise BridgeError("artifact chunk sequence is invalid")
            chunk_hash = frame.get("sha256")
            data = base64.b64decode(frame.get("data", ""), validate=True)
            if (
                not isinstance(chunk_hash, str)
                or re.fullmatch(r"[0-9a-f]{64}", chunk_hash) is None
                or len(data) > ARTIFACT_CHUNK_BYTES
            ):
                raise BridgeError("artifact chunk is missing a valid content hash")
            state.queue.put_nowait((sequence, data, chunk_hash))
            state.next_sequence += 1
        except Exception as exc:
            self._schedule_artifact_abort_v2(state, str(exc))

    def _handle_artifact_end_v2(
        self, state: ArtifactReceiveStateV2, frame: dict[str, Any]
    ) -> None:
        try:
            if state.phase != "receiving":
                raise BridgeError("duplicate or out-of-order artifact end")
            if (
                frame.get("stream") != state.stream_id
                or frame.get("chunks") != state.next_sequence
                or frame.get("size") != state.expected_size
                or frame.get("sha256") != state.sha256
            ):
                raise BridgeError("artifact end metadata does not match")
            state.phase = "ending"
            state.queue.put_nowait(None)
        except asyncio.QueueFull:
            state.end_task = self._background(state.queue.put(None))
        except Exception as exc:
            self._schedule_artifact_abort_v2(state, str(exc))

    def _schedule_artifact_abort_v2(
        self, state: ArtifactReceiveStateV2, reason: str
    ) -> None:
        if state.phase in {"aborting", "completed", "parked"}:
            return
        state.phase = "aborting"
        state.abort_task = self._background(self._abort_artifact_v2(state, reason))

    async def _park_artifact_v2(
        self, state: ArtifactReceiveStateV2, reason: str
    ) -> None:
        """Freeze a live v2 receive at its durably acked prefix."""
        if state.phase in {"parked", "completed", "aborting", "committing"}:
            return
        state.phase = "parked"
        try:
            await asyncio.to_thread(state.handle.flush)
            await asyncio.to_thread(os.fsync, state.handle.fileno())
            await asyncio.to_thread(state.handle.truncate, state.acked_bytes)
            await asyncio.to_thread(os.fsync, state.handle.fileno())
            await asyncio.to_thread(state.handle.close)
        except OSError:
            pass
        current = asyncio.current_task()
        for task in (state.task, state.end_task):
            if task is not None and task is not current and not task.done():
                task.cancel()
        for task in (state.task, state.end_task):
            if task is not None and task is not current and not task.done():
                try:
                    await task
                except (asyncio.CancelledError, Exception):
                    pass
        artifact_root = state.inbox / ".mcp-artifacts"

        def record_park(journal: dict[str, Any]) -> None:
            record = journal["records"].get(state.token)
            if record is not None and isinstance(record, dict):
                record["ackedBytes"] = state.acked_bytes
                record["chunks"] = state.next_sequence
                record["state"] = "partial"
                record["updatedAtNs"] = time.monotonic_ns()
                partial_rel = record.get("partialRel")
                if partial_rel != f"{state.sha256}/{state.partial_path.name}":
                    record["partialRel"] = f"{state.sha256}/{state.partial_path.name}"

        await self._mutate_artifact_journal(state.inbox, record_park)
        self.receiving_artifacts.pop(state.artifact_id, None)
        self.log(f"artifact receive parked at {state.acked_bytes} bytes: {reason}")

    async def _abort_artifact_v2(
        self, state: ArtifactReceiveStateV2, reason: str
    ) -> None:
        if state.phase == "completed":
            return
        state.phase = "aborting"
        if state.commit_task is not None and not state.commit_task.done():
            try:
                await asyncio.shield(state.commit_task)
            except Exception:
                pass
        current = asyncio.current_task()
        for task in (state.task, state.end_task):
            if task is not None and task is not current and not task.done():
                task.cancel()
        for task in (state.task, state.end_task):
            if task is not None and task is not current and not task.done():
                try:
                    await asyncio.gather(task, return_exceptions=True)
                except Exception:
                    pass
        # Cleanup is unconditional: a caller may already have removed the state
        # from receiving_artifacts (abort is also invoked on pre-popped states),
        # and discard is exactly-once via the state flag, so no open handle or
        # journal record can outlive an abort regardless of who owns the entry.
        self.receiving_artifacts.pop(state.artifact_id, None)
        await self._discard_artifact_v2(state)
        try:
            await self._send_frame(
                {
                    "type": "artifact_error",
                    "stream": state.stream_id,
                    "artifact": state.artifact_id,
                    "message": "artifact delivery failed integrity or confinement checks",
                }
            )
        except BridgeError:
            pass
        self.log(f"artifact receive aborted: {reason}")

    async def _discard_artifact_v2(self, state: ArtifactReceiveStateV2) -> None:
        """Remove a v2 attempt's partial bytes and its journal record.

        Exactly-once per state: abort, the worker error path, and the janitor
        may all try to clean the same attempt up, and any of them may run
        first.
        """
        if state.discarded:
            return
        state.discarded = True
        try:
            await asyncio.to_thread(state.handle.close)
        except OSError:
            pass
        try:
            await asyncio.to_thread(state.partial_path.unlink)
        except OSError:
            pass

        def drop(journal: dict[str, Any]) -> Any:
            record = journal["records"].pop(state.token, None)
            if record is not None and isinstance(record, dict):
                return BridgeNode._journal_partial(
                    state.inbox / ".mcp-artifacts", record
                )
            return None

        partial = await self._mutate_artifact_journal(state.inbox, drop)
        if partial is not None:
            try:
                await asyncio.to_thread(partial.unlink)
            except OSError:
                pass

    @staticmethod
    def _commit_artifact_v2_file(state: ArtifactReceiveStateV2) -> dict[str, Any]:
        """Atomically commit one v2 partial.

        Existing identical final content is an idempotent success (returns the
        existing path and removes the fresh partial); existing different content
        fails without overwriting; otherwise the partial is linked into place.
        """
        final = state.content_dir / state.name
        try:
            metadata = os.lstat(state.partial_path)
        except OSError as exc:
            raise BridgeError("artifact partial disappeared before commit") from exc
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
            raise BridgeError("artifact partial changed before commit")
        if final.exists():
            if final.is_symlink() or not final.is_file():
                raise BridgeError("artifact destination is unsafe")
            if _sha256_file(final) == state.sha256:
                try:
                    state.partial_path.unlink()
                except OSError:
                    pass
                return {"path": str(final), "uri": final.as_uri(), "created": False}
            raise BridgeError("artifact destination exists with different content")
        if os.name == "nt":
            os.rename(state.partial_path, final)
        else:
            os.link(state.partial_path, final)
            try:
                state.partial_path.unlink()
            except OSError:
                pass
        try:
            final.chmod(0o600)
        except OSError:
            pass
        return {"path": str(final), "uri": final.as_uri(), "created": True}

    async def _seed_artifact_digest_v2(self, state: ArtifactReceiveStateV2) -> None:
        """Rebuild the running digest over a resumed durable prefix."""
        flags = os.O_RDONLY | getattr(os, "O_BINARY", 0)
        descriptor = os.open(state.partial_path, flags)
        remaining = state.acked_bytes
        digest = hashlib.sha256()
        try:
            with os.fdopen(descriptor, "rb", closefd=True) as handle:
                while remaining > 0:
                    chunk = handle.read(min(ARTIFACT_CHUNK_BYTES, remaining))
                    if not chunk:
                        break
                    digest.update(chunk)
                    remaining -= len(chunk)
        except OSError:
            raise
        if remaining != 0:
            raise BridgeError("artifact parked partial is shorter than acknowledged")
        state.digest = digest

    async def _receive_artifact_v2(self, state: ArtifactReceiveStateV2) -> None:
        try:
            if state.acked_bytes > 0:
                await self._seed_artifact_digest_v2(state)
            while True:
                item = await state.queue.get()
                if item is None:
                    break
                sequence, data, chunk_hash = item
                if state.phase not in {"receiving", "ending"}:
                    raise BridgeError("artifact receive entered a terminal phase early")
                if hashlib.sha256(data).hexdigest() != chunk_hash:
                    raise BridgeError("artifact chunk failed its content hash")
                state.digest.update(data)
                state.received += len(data)
                if state.received > state.expected_size:
                    raise BridgeError("artifact byte count exceeds announcement")
                await asyncio.to_thread(state.handle.write, data)
                try:
                    await self._send_frame(
                        {
                            "type": "artifact_chunk_ok",
                            "stream": state.stream_id,
                            "artifact": state.artifact_id,
                            "sequence": sequence,
                        }
                    )
                except BridgeError:
                    if self._link_failing or not self.link_ready.is_set():
                        state.acked_bytes = state.received
                        await self._park_artifact_v2(state, "peer link lost")
                    raise
                state.acked_bytes = state.received
                state.next_sequence = sequence + 1
            if (
                state.received != state.expected_size
                or state.digest.hexdigest() != state.sha256
            ):
                raise BridgeError("artifact integrity verification failed")
            await asyncio.to_thread(state.handle.flush)
            await asyncio.to_thread(os.fsync, state.handle.fileno())
            await asyncio.to_thread(state.handle.close)
            if state.phase != "ending":
                raise BridgeError("artifact reached commit before terminal metadata")
            state.phase = "committing"
            state.commit_task = asyncio.create_task(
                asyncio.to_thread(self._commit_artifact_v2_file, state)
            )
            receipt = await asyncio.shield(state.commit_task)
            if state.phase == "aborting":
                if receipt["created"]:
                    try:
                        await asyncio.to_thread(Path(receipt["path"]).unlink)
                    except OSError:
                        pass
                raise BridgeError("artifact commit was cancelled")
            state.phase = "completed"
            if self.receiving_artifacts.pop(state.artifact_id, None) is None:
                return
            await self._journal_mark_committed(state)
            await self._send_frame(
                {
                    "type": "artifact_ok",
                    "stream": state.stream_id,
                    "artifact": state.artifact_id,
                    "uri": receipt["uri"],
                    "path": receipt["path"],
                    "size": state.received,
                    "sha256": state.digest.hexdigest(),
                }
            )
        except asyncio.CancelledError:
            if state.phase == "parked":
                raise
            if state.commit_task is not None and not state.commit_task.done():
                try:
                    await asyncio.shield(state.commit_task)
                except Exception:
                    pass
            if state.phase != "completed":
                if self.receiving_artifacts.pop(state.artifact_id, None) is not None:
                    await self._discard_artifact_v2(state)
            raise
        except Exception as exc:
            if state.phase == "parked":
                return
            try:
                await asyncio.to_thread(state.handle.close)
            except OSError:
                pass
            if self.receiving_artifacts.pop(state.artifact_id, None) is not None:
                await self._discard_artifact_v2(state)
            if state.phase == "aborting":
                return
            try:
                await self._send_frame(
                    {
                        "type": "artifact_error",
                        "stream": state.stream_id,
                        "artifact": state.artifact_id,
                        "message": "artifact delivery failed integrity or confinement checks",
                    }
                )
            except BridgeError:
                pass
            self.log(f"artifact receive failed: {type(exc).__name__}: {exc}")

    async def _journal_mark_committed(self, state: ArtifactReceiveStateV2) -> None:
        """Collapse this digest's partial records into one committed receipt."""
        key_state = "committed"
        final_rel = f"{state.sha256}/{state.name}"

        def commit(journal: dict[str, Any]) -> None:
            records = journal["records"]
            for token, record in list(records.items()):
                if not isinstance(record, dict):
                    continue
                if record.get("sha256") == state.sha256 and record.get("name") == state.name:
                    partial = BridgeNode._journal_partial(
                        state.inbox / ".mcp-artifacts", record
                    )
                    if partial is not None and partial != state.partial_path:
                        try:
                            partial.unlink()
                        except OSError:
                            pass
                    if record.get("token") != state.token:
                        records.pop(token, None)
            records[state.token] = {
                "token": state.token,
                "sha256": state.sha256,
                "name": state.name,
                "size": state.expected_size,
                "state": key_state,
                "ackedBytes": state.acked_bytes,
                "chunks": state.next_sequence,
                "finalRel": final_rel,
                "updatedAtNs": time.monotonic_ns(),
            }

        await self._mutate_artifact_journal(state.inbox, commit)

    async def _handle_artifact_begin(self, frame: dict[str, Any]) -> None:
        artifact_id = frame.get("artifact")
        stream_id = frame.get("stream")
        artifact_dir: Path | None = None
        try:
            if (
                not isinstance(artifact_id, str)
                or not ID_PATTERN.fullmatch(artifact_id)
                or artifact_id in self.receiving_artifacts
                or not isinstance(stream_id, str)
            ):
                raise BridgeError("invalid artifact identity")
            stream = self.streams.get(stream_id)
            if not stream or stream.artifact_inbox is None:
                raise BridgeError("artifact workspace delivery is not configured")
            active_for_stream = sum(
                1
                for item in self.receiving_artifacts.values()
                if item.stream_id == stream_id
            )
            reserved_bytes = sum(
                item.expected_size for item in self.receiving_artifacts.values()
            )
            if (
                len(self.receiving_artifacts) >= MAX_CONCURRENT_ARTIFACTS
                or active_for_stream >= MAX_CONCURRENT_ARTIFACTS // 2
            ):
                raise BridgeError("artifact receive concurrency limit reached")
            name = self._safe_artifact_name(frame.get("name"))
            media_type = frame.get("mediaType")
            if media_type is not None and (
                not isinstance(media_type, str)
                or not media_type
                or len(media_type) > 255
                or any(character in media_type for character in ("\r", "\n", "\x00"))
            ):
                raise BridgeError("invalid artifact media type")
            size = frame.get("size")
            sha256 = frame.get("sha256")
            if (
                not isinstance(size, int)
                or isinstance(size, bool)
                or size < 0
                or size > self.max_artifact_bytes
                or reserved_bytes + size > MAX_RESERVED_ARTIFACT_BYTES
                or not isinstance(sha256, str)
                or re.fullmatch(r"[0-9a-f]{64}", sha256) is None
            ):
                raise BridgeError("invalid artifact size or digest")
            inbox = self._validate_artifact_inbox(str(stream.artifact_inbox))
            assert inbox is not None
            artifact_root = inbox / ".mcp-artifacts"
            if artifact_root.exists() and (
                artifact_root.is_symlink() or not artifact_root.is_dir()
            ):
                raise BridgeError("artifact workspace root is unsafe")
            artifact_root.mkdir(mode=0o700, exist_ok=True)
            if artifact_root.resolve().parent != inbox:
                raise BridgeError("artifact workspace root escaped its inbox")
            artifact_dir = artifact_root / artifact_id
            artifact_dir.mkdir(mode=0o700, exist_ok=False)
            final_path = artifact_dir / name
            temp_path = artifact_dir / ".partial"
            partial_flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_BINARY", 0)
            handle = os.fdopen(os.open(temp_path, partial_flags, 0o600), "wb")
            state = ArtifactReceiveState(
                stream_id=stream_id,
                artifact_id=artifact_id,
                name=name,
                media_type=media_type,
                expected_size=size,
                temp_path=temp_path,
                final_path=final_path,
                handle=handle,
                end_sha256=sha256,
            )
            self.receiving_artifacts[artifact_id] = state
            state.task = self._background(self._receive_artifact(state))
            await self._send_frame(
                {
                    "type": "artifact_ready",
                    "stream": stream_id,
                    "artifact": artifact_id,
                }
            )
        except Exception as exc:
            state = (
                self.receiving_artifacts.get(artifact_id)
                if isinstance(artifact_id, str)
                else None
            )
            if state is not None:
                await self._abort_received_artifact(
                    state,
                    "artifact begin response failed",
                )
            elif artifact_dir is not None:
                shutil.rmtree(artifact_dir, ignore_errors=True)
            try:
                await self._send_frame(
                    {
                        "type": "artifact_error",
                        "stream": stream_id if isinstance(stream_id, str) else "",
                        "artifact": artifact_id if isinstance(artifact_id, str) else "",
                        "message": "artifact delivery was rejected",
                    }
                )
            except BridgeError:
                pass
            self.log(f"artifact begin rejected: {type(exc).__name__}: {exc}")

    def _handle_artifact_chunk(self, frame: dict[str, Any]) -> None:
        artifact_id = frame.get("artifact")
        state = self.receiving_artifacts.get(artifact_id) if isinstance(artifact_id, str) else None
        if not state:
            return
        try:
            if state.phase != "receiving":
                raise BridgeError("artifact chunk arrived after terminal metadata")
            sequence = frame.get("sequence")
            if sequence != state.next_sequence or frame.get("stream") != state.stream_id:
                raise BridgeError("artifact chunk sequence is invalid")
            data = base64.b64decode(frame.get("data", ""), validate=True)
            if len(data) > ARTIFACT_CHUNK_BYTES:
                raise BridgeError("artifact chunk exceeds negotiated limit")
            state.queue.put_nowait((sequence, data))
            state.next_sequence += 1
        except Exception as exc:
            self._schedule_artifact_abort(state, str(exc))

    def _handle_artifact_end(self, frame: dict[str, Any]) -> None:
        artifact_id = frame.get("artifact")
        state = self.receiving_artifacts.get(artifact_id) if isinstance(artifact_id, str) else None
        if not state:
            return
        try:
            if state.phase != "receiving":
                raise BridgeError("duplicate or out-of-order artifact end")
            if (
                frame.get("stream") != state.stream_id
                or frame.get("chunks") != state.next_sequence
                or frame.get("size") != state.expected_size
                or frame.get("sha256") != state.end_sha256
            ):
                raise BridgeError("artifact end metadata does not match")
            state.phase = "ending"
            state.queue.put_nowait(None)
        except asyncio.QueueFull:
            state.end_task = self._background(state.queue.put(None))
        except Exception as exc:
            self._schedule_artifact_abort(state, str(exc))

    def _handle_artifact_reply(self, frame: dict[str, Any]) -> None:
        artifact_id = frame.get("artifact")
        waiter = self.pending_artifacts.get(artifact_id) if isinstance(artifact_id, str) else None
        if not waiter or frame.get("stream") != waiter.stream_id:
            return
        kind = frame.get("type")
        if kind == "artifact_ready":
            if waiter.phase != "begin_sent" or waiter.ready.done():
                self._fail_artifact_waiter(waiter, "out-of-order artifact_ready")
                return
            waiter.phase = "ready"
            waiter.ready.set_result(frame)
            return
        if kind == "artifact_chunk_ok":
            if (
                waiter.phase != "sending"
                or waiter.chunk_ack is None
                or waiter.chunk_ack.done()
                or not isinstance(frame.get("sequence"), int)
            ):
                self._fail_artifact_waiter(waiter, "out-of-order artifact_chunk_ok")
                return
            waiter.chunk_ack.set_result(frame["sequence"])
            return
        if kind == "artifact_committed":
            if waiter.phase not in {"begin_sent", "ready"} or waiter.done.done():
                self._fail_artifact_waiter(waiter, "out-of-order artifact_committed")
                return
            uri = frame.get("uri")
            path = frame.get("path")
            if (
                not isinstance(uri, str)
                or not uri.startswith("file:")
                or not isinstance(path, str)
                or frame.get("size") != waiter.expected_size
                or frame.get("sha256") != waiter.expected_sha256
            ):
                self._fail_artifact_waiter(waiter, "peer returned invalid artifact receipt")
                return
            waiter.phase = "completed"
            waiter.done.set_result(
                {
                    "uri": uri,
                    "path": path,
                    "size": frame.get("size"),
                    "sha256": frame.get("sha256"),
                }
            )
            if not waiter.ready.done():
                waiter.ready.set_result({"status": "committed"})
            return
        if kind == "artifact_ok":
            if waiter.phase != "end_sent" or waiter.done.done():
                self._fail_artifact_waiter(waiter, "out-of-order artifact_ok")
                return
            uri = frame.get("uri")
            path = frame.get("path")
            if (
                not isinstance(uri, str)
                or not uri.startswith("file:")
                or not isinstance(path, str)
                or frame.get("size") != waiter.expected_size
                or frame.get("sha256") != waiter.expected_sha256
            ):
                self._fail_artifact_waiter(waiter, "peer returned invalid artifact receipt")
                return
            waiter.phase = "completed"
            waiter.done.set_result(
                {
                    "uri": uri,
                    "path": path,
                    "size": frame.get("size"),
                    "sha256": frame.get("sha256"),
                }
            )
            return
        if kind == "artifact_error":
            self._fail_artifact_waiter(
                waiter,
                str(frame.get("message", "artifact delivery failed")),
            )

    @staticmethod
    def _fail_artifact_waiter(
        waiter: ArtifactTransferWaiter,
        message: str,
    ) -> None:
        if waiter.phase == "completed":
            return
        waiter.phase = "failed"
        error = BridgeError(message)
        if not waiter.ready.done():
            waiter.ready.set_exception(error)
        elif waiter.chunk_ack is not None and not waiter.chunk_ack.done():
            waiter.chunk_ack.set_exception(error)
        elif not waiter.done.done():
            waiter.done.set_exception(error)

    async def _receive_artifact(self, state: ArtifactReceiveState) -> None:
        try:
            while True:
                item = await state.queue.get()
                if item is None:
                    break
                sequence, data = item
                state.received += len(data)
                if state.received > state.expected_size:
                    raise BridgeError("artifact byte count exceeds announcement")
                state.digest.update(data)
                await asyncio.to_thread(state.handle.write, data)
                await self._send_frame(
                    {
                        "type": "artifact_chunk_ok",
                        "stream": state.stream_id,
                        "artifact": state.artifact_id,
                        "sequence": sequence,
                    }
                )
            if (
                state.received != state.expected_size
                or state.digest.hexdigest() != state.end_sha256
            ):
                raise BridgeError("artifact integrity verification failed")
            await asyncio.to_thread(state.handle.flush)
            await asyncio.to_thread(os.fsync, state.handle.fileno())
            state.handle.close()
            if state.phase != "ending":
                raise BridgeError("artifact reached commit before terminal metadata")
            state.phase = "committing"
            state.commit_task = asyncio.create_task(
                asyncio.to_thread(
                    self._commit_artifact_no_overwrite,
                    state.temp_path,
                    state.final_path,
                )
            )
            await asyncio.shield(state.commit_task)
            if state.phase == "aborting":
                try:
                    state.final_path.unlink()
                except OSError:
                    pass
                raise BridgeError("artifact commit was cancelled")
            state.phase = "completed"
            try:
                state.final_path.chmod(0o600)
            except OSError:
                pass
            self.receiving_artifacts.pop(state.artifact_id, None)
            await self._send_frame(
                {
                    "type": "artifact_ok",
                    "stream": state.stream_id,
                    "artifact": state.artifact_id,
                    "uri": state.final_path.as_uri(),
                    "path": str(state.final_path),
                    "size": state.received,
                    "sha256": state.digest.hexdigest(),
                }
            )
        except asyncio.CancelledError:
            if state.commit_task is not None:
                try:
                    await asyncio.shield(state.commit_task)
                except Exception:
                    pass
            if state.phase != "completed":
                try:
                    state.final_path.unlink()
                except OSError:
                    pass
            self._cleanup_received_artifact(state)
            raise
        except Exception as exc:
            self.receiving_artifacts.pop(state.artifact_id, None)
            if state.phase != "completed":
                try:
                    state.final_path.unlink()
                except OSError:
                    pass
            self._cleanup_received_artifact(state)
            try:
                await self._send_frame(
                    {
                        "type": "artifact_error",
                        "stream": state.stream_id,
                        "artifact": state.artifact_id,
                        "message": "artifact delivery failed integrity or confinement checks",
                    }
                )
            except BridgeError:
                pass
            self.log(f"artifact receive failed: {type(exc).__name__}: {exc}")

    def _schedule_artifact_abort(
        self,
        state: ArtifactReceiveState,
        reason: str,
    ) -> None:
        if state.phase in {"aborting", "completed"}:
            return
        state.phase = "aborting"
        state.abort_task = self._background(
            self._abort_received_artifact(state, reason)
        )

    async def _abort_received_artifact(
        self,
        state: ArtifactReceiveState,
        reason: str,
    ) -> None:
        if state.phase == "completed":
            return
        state.phase = "aborting"
        current = asyncio.current_task()
        if state.task and state.task is not current and not state.task.done():
            state.task.cancel()
            await asyncio.gather(state.task, return_exceptions=True)
        if state.commit_task is not None and not state.commit_task.done():
            try:
                await asyncio.shield(state.commit_task)
            except Exception:
                pass
        try:
            state.final_path.unlink()
        except OSError:
            pass
        self.receiving_artifacts.pop(state.artifact_id, None)
        self._cleanup_received_artifact(state)
        try:
            await self._send_frame(
                {
                    "type": "artifact_error",
                    "stream": state.stream_id,
                    "artifact": state.artifact_id,
                    "message": "artifact delivery failed integrity or confinement checks",
                }
            )
        except BridgeError:
            pass
        self.log(f"artifact receive aborted: {reason}")

    @staticmethod
    def _commit_artifact_no_overwrite(temp_path: Path, final_path: Path) -> None:
        if temp_path.parent.resolve() != final_path.parent.resolve():
            raise BridgeError("artifact commit directory changed")
        metadata = os.lstat(temp_path)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
            raise BridgeError("artifact partial changed before commit")
        if os.name == "nt":
            os.rename(temp_path, final_path)
        else:
            os.link(temp_path, final_path)
            temp_path.unlink()

    @staticmethod
    def _cleanup_received_artifact(state: ArtifactReceiveState) -> None:
        if state.end_task and not state.end_task.done():
            state.end_task.cancel()
        try:
            state.handle.close()
        except OSError:
            pass
        try:
            state.temp_path.unlink()
        except OSError:
            pass
        try:
            state.temp_path.parent.rmdir()
        except OSError:
            pass

    async def _handle_stream_data(self, frame: dict[str, Any]) -> None:
        stream_id = frame.get("stream")
        if not isinstance(stream_id, str):
            return
        stream = self.streams.get(stream_id)
        if not stream:
            return
        sequence = frame.get("sequence")
        if (
            not isinstance(sequence, int)
            or isinstance(sequence, bool)
            or sequence != stream.inbound_sequence
        ):
            self.log(f"stream {stream_id} received an invalid data sequence; closing it")
            self._background(self._close_stream(stream_id, remote=False))
            return
        try:
            data = base64.b64decode(frame.get("data", ""), validate=True)
            await self._maybe_capture(stream, "inbound", data)
            stream.inbound.put_nowait((sequence, data))
        except (ValueError, TypeError, asyncio.QueueFull):
            self.log(f"stream {stream_id} received invalid or excessive data; closing it")
            self._background(self._close_stream(stream_id, remote=False))
            return
        stream.inbound_sequence += 1

    def _handle_stream_data_ok(self, frame: dict[str, Any]) -> None:
        stream_id = frame.get("stream")
        stream = self.streams.get(stream_id) if isinstance(stream_id, str) else None
        if not stream:
            return
        acknowledgement = stream.outbound_ack
        sequence = frame.get("sequence")
        if (
            acknowledgement is None
            or acknowledgement.done()
            or not isinstance(sequence, int)
            or isinstance(sequence, bool)
            or sequence != stream.outbound_sequence
        ):
            self.log(f"stream {stream.stream_id} received an invalid data acknowledgement")
            self._background(self._close_stream(stream.stream_id, remote=False))
            return
        acknowledgement.set_result(sequence)

    async def _handle_stream_eof(self, stream_id: str) -> None:
        stream = self.streams.get(stream_id)
        if not stream:
            return
        try:
            stream.inbound.put_nowait(None)
        except asyncio.QueueFull:
            task = self._background(stream.inbound.put(None))
            stream.tasks.add(task)
            task.add_done_callback(stream.tasks.discard)

    async def _close_stream(self, stream_id: str, *, remote: bool) -> None:
        stream = self.streams.pop(stream_id, None)
        if not stream:
            return
        shared_backend = stream.shared_backend
        stream.shared_backend = None
        stream.closed.set()
        evidence = self._evidence_states.pop(stream_id, None)
        if evidence is not None:
            self._retire_evidence_state(evidence)
        if stream.outbound_ack is not None and not stream.outbound_ack.done():
            stream.outbound_ack.set_exception(
                BridgeError("stream closed before data acknowledgement")
            )
        if stream.artifact_token is not None:
            self.artifact_publishers.pop(stream.artifact_token, None)
        for artifact_id, waiter in list(self.pending_artifacts.items()):
            if waiter.stream_id == stream_id:
                self._fail_artifact_waiter(
                    waiter,
                    "artifact stream closed before delivery completed",
                )
                self.pending_artifacts.pop(artifact_id, None)
        for input_id, waiter in list(self.pending_inputs.items()):
            if waiter.stream_id == stream_id:
                self._fail_input_waiter(
                    waiter,
                    "input stream closed before staging completed",
                )
                self.pending_inputs.pop(input_id, None)
        for state in list(self.receiving_artifacts.values()):
            if state.stream_id != stream_id:
                continue
            if isinstance(state, ArtifactReceiveStateV2):
                if self._link_failing:
                    await self._park_artifact_v2(state, "peer link lost")
                else:
                    await self._abort_artifact_v2(state, "logical stream closed")
            else:
                await self._abort_received_artifact(state, "logical stream closed")
        for state in list(self.receiving_inputs.values()):
            if state.stream_id == stream_id:
                await self._abort_received_input(state, "logical stream closed")
        current = asyncio.current_task()
        for task in tuple(stream.tasks):
            if task is not current and not task.done():
                task.cancel()
        if stream.socket_writer and not stream.socket_writer.is_closing():
            stream.socket_writer.close()
            try:
                await stream.socket_writer.wait_closed()
            except OSError:
                pass
        if stream.process and stream.process.returncode is None:
            try:
                stream.process.terminate()
            except ProcessLookupError:
                pass
            try:
                await asyncio.wait_for(stream.process.wait(), timeout=5)
            except TimeoutError:
                stream.process.kill()
                await stream.process.wait()
        if shared_backend is not None:
            await shared_backend.detach(stream_id)
        if stream.artifact_stage is not None:
            shutil.rmtree(stream.artifact_stage, ignore_errors=True)
        if stream.input_stage is not None:
            stream.input_handles.clear()
            shutil.rmtree(stream.input_stage, ignore_errors=True)
            stream.input_stage = None
        if not remote:
            try:
                await self._send_frame({"type": "close", "stream": stream_id})
            except BridgeError:
                pass

    async def _fail_link_state(self, message: str) -> None:
        self._link_failing = True
        try:
            for future in self.pending_registry.values():
                if not future.done():
                    future.set_exception(BridgeError(message))
            self.pending_registry.clear()
            for future in self.pending_control.values():
                if not future.done():
                    future.set_exception(BridgeError(message))
            self.pending_control.clear()
            self._fail_relay_state(message)
            for stream_id in list(self.streams):
                await self._close_stream(stream_id, remote=True)
            for backend in list(self.shared_backends.values()):
                await backend.stop(message)
            for backend in list(self.http_backends.values()):
                await backend.stop(message)
        finally:
            self._link_failing = False

    def _fail_relay_state(self, message: str) -> None:
        """Fail consumer-side relay futures/queues and cancel owner-side work."""
        for future in self.relay_start.values():
            if not future.done():
                future.set_exception(BridgeError(f"relay aborted: {message}"))
        self.relay_start.clear()
        for queue in self.relay_bodies.values():
            try:
                queue.put_nowait(("abort", message))
            except Exception:
                pass
        self.relay_bodies.clear()
        for task in list(self.relay_tasks.values()):
            task.cancel()
        self.relay_tasks.clear()

    async def _handle_registry_request(self, frame: dict[str, Any]) -> None:
        request_id = str(frame.get("request"))
        try:
            action = str(frame.get("action"))
            arguments = frame.get("arguments") if isinstance(frame.get("arguments"), dict) else {}
            result = self.registry.query(action, arguments)
            result = self._with_lifecycle_fields(action, result)
            response = {"type": "registry_response", "request": request_id, "ok": True, "result": result}
        except Exception as exc:
            response = {"type": "registry_response", "request": request_id, "ok": False, "message": str(exc)}
        if len(_json_bytes(response)) + 1 > MAX_FRAME_BYTES:
            response = {
                "type": "registry_response",
                "request": request_id,
                "ok": False,
                "message": "registry response exceeds the bridge frame limit; narrow the query",
            }
        await self._send_frame(response)

    async def _handle_control_request(self, frame: dict[str, Any]) -> None:
        request_id = str(frame.get("request"))
        try:
            payload = frame.get("payload")
            if not isinstance(payload, dict):
                raise BridgeError("invalid control payload")
            allowed = {
                "action", "target", "generation", "confirm", "impactOverride",
                "reason", "operationId",
            }
            if set(payload) - allowed:
                raise BridgeError("control payload contains forbidden fields")
            result = await self._agent_control(payload)
            response = {
                "type": "control_response", "request": request_id,
                "ok": True, "result": result,
            }
        except Exception as exc:
            response = {
                "type": "control_response", "request": request_id,
                "ok": False, "message": str(exc)[:400],
            }
        if len(_json_bytes(response)) + 1 > MAX_FRAME_BYTES:
            response = {
                "type": "control_response", "request": request_id,
                "ok": False, "message": "control response exceeds bridge frame limit",
            }
        await self._send_frame(response)

    async def _remote_control(self, payload: dict[str, Any]) -> dict[str, Any]:
        try:
            await asyncio.wait_for(self.link_ready.wait(), timeout=10)
        except TimeoutError as exc:
            raise BridgeError("peer_unavailable: peer bridge link is unavailable") from exc
        request_id = uuid.uuid4().hex
        future = asyncio.get_running_loop().create_future()
        self.pending_control[request_id] = future
        try:
            await self._send_frame(
                {"type": "control_request", "request": request_id, "payload": payload}
            )
            result = await asyncio.wait_for(future, timeout=30)
            if not isinstance(result, dict):
                raise BridgeError("peer returned an invalid control result")
            if self.journal is not None:
                operation_id = result.get("operationId")
                target = result.get("id", payload.get("target"))
                if isinstance(operation_id, str):
                    await asyncio.to_thread(
                        self.journal.record,
                        side=self.side,
                        category="remote-lifecycle",
                        operation_id=operation_id,
                        target=target if isinstance(target, str) else None,
                        outcome="ok" if result.get("ok") else "error",
                        metadata={"action": payload.get("action"), "requester": True},
                    )
            return result
        finally:
            self.pending_control.pop(request_id, None)

    async def _remote_registry_query(self, action: str, arguments: dict[str, Any]) -> Any:
        await asyncio.wait_for(self.link_ready.wait(), timeout=10)
        request_id = uuid.uuid4().hex
        future = asyncio.get_running_loop().create_future()
        self.pending_registry[request_id] = future
        await self._send_frame(
            {
                "type": "registry_request",
                "request": request_id,
                "action": action,
                "arguments": arguments,
            }
        )
        try:
            return await asyncio.wait_for(future, timeout=10)
        finally:
            self.pending_registry.pop(request_id, None)

    async def _registry_query(self, scope: str, action: str, arguments: dict[str, Any]) -> Any:
        if scope == "local":
            result = self.registry.query(action, arguments)
            return self._with_lifecycle_fields(action, result)
        if scope == "remote":
            return await self._remote_registry_query(action, arguments)
        raise BridgeError("registry scope must be local or remote")

    # ---- Native loopback Streamable HTTP relay ---------------------------

    async def _handle_http_relay_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        """Consumer-side HTTP/1.1 listener (``serve --http-relay-port``).

        Each inbound request is validated and forwarded to the peer owner as a
        bounded ``relay_request`` envelope; the response status/headers/body are
        streamed back and re-framed for the Agent (chunked unless the response
        cannot carry a body). Endpoint URLs and registry headers never appear
        here or on the peer link.
        """
        try:
            while True:
                try:
                    head = await self._relay_read_request_head(reader)
                    if head is None:
                        return
                    keep = await self._relay_dispatch_request(head, reader, writer)
                except _RelayClientError as exc:
                    keep = exc.close
                    try:
                        await self._relay_write_simple(writer, exc.status, str(exc))
                    except OSError:
                        return
                except (asyncio.CancelledError, ConnectionError, OSError):
                    raise
                except Exception as exc:
                    # Never let an internal relay error surface as a silent
                    # disconnect: answer 502 and close the Agent connection.
                    try:
                        await self._relay_write_simple(
                            writer, 502, f"relay internal error: {type(exc).__name__}"
                        )
                    except OSError:
                        return
                    return
                if not keep:
                    return
        except (ConnectionError, OSError, asyncio.CancelledError):
            return
        finally:
            if not writer.is_closing():
                writer.close()
                try:
                    await writer.wait_closed()
                except OSError:
                    pass

    async def _relay_read_request_head(
        self, reader: asyncio.StreamReader
    ) -> dict[str, Any] | None:
        """Read one bounded HTTP request head plus its body; None on clean EOF."""
        try:
            raw = await asyncio.wait_for(
                reader.readuntil(b"\r\n\r\n"), HTTP_RELAY_HEADER_TIMEOUT_SECONDS
            )
        except asyncio.IncompleteReadError:
            return None
        except asyncio.LimitOverrunError as exc:
            raise _RelayClientError(431, "relay request headers too large") from exc
        except asyncio.TimeoutError as exc:
            raise _RelayClientError(408, "relay request header timed out") from exc
        except ValueError as exc:
            raise _RelayClientError(400, "relay request head malformed") from exc
        if len(raw) > HTTP_RELAY_MAX_HEADER_BYTES:
            raise _RelayClientError(431, "relay request headers too large")
        lines = raw[:-4].split(b"\r\n")
        if not lines or len(lines[0].split(b" ")) < 2:
            raise _RelayClientError(400, "relay request line malformed")
        method_b, target_b, *_ = lines[0].split(b" ")
        method = method_b.decode("ascii", "replace")
        if method not in HTTP_RELAY_METHODS:
            raise _RelayClientError(405, f"relay does not support method {method}")
        target = target_b.decode("latin-1", "replace")
        headers: list[tuple[str, str]] = []
        content_length: int | None = None
        has_te = False
        for line in lines[1:]:
            if not line:
                continue
            colon = line.find(b":")
            if colon <= 0:
                raise _RelayClientError(400, "relay header malformed")
            name_b = line[:colon].strip()
            value_b = line[colon + 1:].strip()
            if not _HTTP_HEADER_NAME_RE.fullmatch(name_b):
                raise _RelayClientError(400, "relay header name invalid")
            if any(ch < 0x20 and ch != 0x09 for ch in value_b):
                raise _RelayClientError(400, "relay header value contains control bytes")
            name = name_b.decode("ascii")
            value = value_b.decode("latin-1", "replace")
            headers.append((name, value))
            lower = name.lower()
            if lower == "content-length":
                try:
                    content_length = int(value)
                except ValueError as exc:
                    raise _RelayClientError(400, "relay content-length invalid") from exc
                if content_length < 0:
                    raise _RelayClientError(400, "relay content-length invalid")
            elif lower == "transfer-encoding":
                has_te = True
        if has_te:
            raise _RelayClientError(501, "relay does not accept chunked request bodies")
        if content_length is None:
            content_length = 0
        if content_length > HTTP_RELAY_MAX_REQUEST_BYTES:
            raise _RelayClientError(413, "relay request body exceeds limit")
        body = bytearray()
        while len(body) < content_length:
            try:
                chunk = await asyncio.wait_for(
                    reader.read(content_length - len(body)),
                    HTTP_RELAY_IDLE_TIMEOUT_SECONDS,
                )
            except asyncio.TimeoutError as exc:
                raise _RelayClientError(408, "relay request body timed out") from exc
            if not chunk:
                raise _RelayClientError(400, "relay request body truncated")
            body.extend(chunk)
        return {
            "method": method,
            "target": target,
            "headers": headers,
            "body": bytes(body),
        }

    async def _relay_dispatch_request(
        self,
        head: dict[str, Any],
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> bool:
        method = str(head["method"])
        target = str(head["target"])
        split = _split_relay_target(target)
        if split is None:
            raise _RelayClientError(404, "relay path must be /mcp/<registered-id>")
        server_id, suffix, query = split
        if not self.link_ready.is_set():
            raise _RelayClientError(503, "peer bridge link is unavailable")
        request_id = "relay-" + uuid.uuid4().hex
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        queue: asyncio.Queue[tuple[Any, ...]] = asyncio.Queue()
        self.relay_start[request_id] = future
        self.relay_bodies[request_id] = queue
        started = False
        try:
            await self._send_frame(
                {
                    "type": "relay_request",
                    "request": request_id,
                    "target": server_id,
                    "method": method,
                    "suffix": suffix,
                    "query": query,
                    "headers": [[name, value] for name, value in head["headers"]],
                    "body": base64.b64encode(bytes(head["body"])).decode("ascii"),
                }
            )
            try:
                status, reason, response_headers = await asyncio.wait_for(
                    future, HTTP_RELAY_HEADER_TIMEOUT_SECONDS
                )
            except asyncio.TimeoutError as exc:
                try:
                    await self._send_frame(
                        {"type": "relay_cancel", "request": request_id}
                    )
                except BridgeError:
                    pass
                raise _RelayClientError(504, "relay upstream header timed out") from exc
            except _RelayUpstreamError as exc:
                raise _RelayClientError(exc.status, str(exc)) from exc
            except BridgeError as exc:
                raise _RelayClientError(502, str(exc)) from exc
            bodyless = (
                method == "HEAD"
                or 100 <= int(status) < 200
                or int(status) in {204, 304}
            )
            lines = [f"HTTP/1.1 {status} {_relay_safe_text(reason)}"]
            seen: set[str] = set()
            for name, value in response_headers:
                lower = str(name).lower()
                if lower in _HTTP_HOP_BY_HOP or lower in seen:
                    continue
                seen.add(lower)
                lines.append(f"{_relay_safe_text(name)}: {_relay_safe_text(value)}")
            lines.append("Connection: keep-alive")
            if not bodyless:
                lines.append("Transfer-Encoding: chunked")
            writer.write(
                ("\r\n".join(lines) + "\r\n\r\n").encode("latin-1", "replace")
            )
            await writer.drain()
            started = True
            if not bodyless:
                while True:
                    try:
                        item = await asyncio.wait_for(
                            queue.get(), HTTP_RELAY_IDLE_TIMEOUT_SECONDS
                        )
                    except asyncio.TimeoutError:
                        await self._relay_send_cancel(request_id)
                        return False
                    kind = item[0]
                    if kind == "body":
                        chunk = item[1]
                        writer.write(
                            f"{len(chunk):X}\r\n".encode("ascii") + chunk + b"\r\n"
                        )
                        await writer.drain()
                    elif kind == "abort":
                        await self._relay_send_cancel(request_id)
                        return False
                    else:  # end
                        writer.write(b"0\r\n\r\n")
                        await writer.drain()
                        break
            return True
        except (ConnectionError, OSError) as exc:
            if started:
                await self._relay_send_cancel(request_id)
                return False
            raise _RelayClientError(502, f"relay client connection failed: {exc}") from exc
        finally:
            self.relay_start.pop(request_id, None)
            self.relay_bodies.pop(request_id, None)

    async def _relay_send_cancel(self, request_id: str) -> None:
        try:
            await self._send_frame({"type": "relay_cancel", "request": request_id})
        except (BridgeError, OSError):
            pass

    async def _relay_write_simple(
        self, writer: asyncio.StreamWriter, status: int, message: str
    ) -> None:
        body = (_relay_safe_text(message) or "relay error").encode("utf-8")
        reason = _http_reason(status)
        payload = (
            f"HTTP/1.1 {status} {reason}\r\n"
            f"Content-Type: text/plain\r\n"
            f"Content-Length: {len(body)}\r\n"
            f"Connection: close\r\n\r\n"
        ).encode("latin-1", "replace") + body
        writer.write(payload)
        await writer.drain()

    def _handle_relay_start_frame(self, frame: dict[str, Any]) -> None:
        request_id = str(frame.get("request"))
        future = self.relay_start.get(request_id)
        if future is None or future.done():
            return
        status = frame.get("status")
        reason = frame.get("reason", "")
        headers = frame.get("headers")
        if not isinstance(status, int) or isinstance(status, bool) or not isinstance(headers, list):
            future.set_exception(BridgeError("relay upstream returned an invalid response head"))
            return
        future.set_result((status, str(reason), headers))

    def _handle_relay_body_frame(self, frame: dict[str, Any]) -> None:
        queue = self.relay_bodies.get(str(frame.get("request")))
        if queue is None:
            return
        encoded = frame.get("data")
        if not isinstance(encoded, str):
            return
        try:
            chunk = base64.b64decode(encoded)
        except (ValueError, TypeError):
            queue.put_nowait(("abort", "relay body frame invalid"))
            return
        if chunk:
            queue.put_nowait(("body", chunk))

    def _handle_relay_end_frame(self, frame: dict[str, Any]) -> None:
        queue = self.relay_bodies.get(str(frame.get("request")))
        if queue is not None:
            queue.put_nowait(("end", None))

    def _handle_relay_error_frame(self, frame: dict[str, Any]) -> None:
        request_id = str(frame.get("request"))
        message = str(frame.get("message", "relay upstream error"))
        status = frame.get("status")
        future = self.relay_start.get(request_id)
        if future is not None and not future.done():
            if isinstance(status, int) and not isinstance(status, bool):
                future.set_exception(_RelayUpstreamError(status, message))
            else:
                future.set_exception(BridgeError(f"relay upstream error: {message}"))
            return
        queue = self.relay_bodies.get(request_id)
        if queue is not None:
            queue.put_nowait(("abort", message))

    async def _handle_relay_request(self, frame: dict[str, Any]) -> None:
        request_id = str(frame.get("request"))
        try:
            target = str(frame.get("target"))
            method = str(frame.get("method"))
            suffix = str(frame.get("suffix", ""))
            query = str(frame.get("query", ""))
            headers = frame.get("headers")
            if not ID_PATTERN.fullmatch(target):
                raise BridgeError("relay target id invalid")
            if method not in HTTP_RELAY_METHODS:
                raise BridgeError("relay method not allowed")
            if suffix and not _HTTP_TARGET_SUFFIX_RE.fullmatch(suffix):
                raise BridgeError("relay target suffix invalid")
            if query and not _HTTP_QUERY_RE.fullmatch(query):
                raise BridgeError("relay query invalid")
            if not isinstance(headers, list) or len(headers) > HTTP_RELAY_MAX_HEADERS:
                raise BridgeError("relay headers exceed the bound")
            header_pairs: list[tuple[str, str]] = []
            for item in headers:
                if not isinstance(item, list) or len(item) != 2:
                    raise BridgeError("relay header malformed")
                name, value = str(item[0]), str(item[1])
                if not _HTTP_HEADER_NAME_RE.fullmatch(name.encode("ascii", "replace")):
                    raise BridgeError("relay header name invalid")
                if len(name) > 4096 or len(value) > HTTP_RELAY_MAX_HEADER_VALUE_BYTES:
                    raise BridgeError("relay header exceeds the bound")
                if any(ord(ch) < 0x20 and ch != "\t" for ch in value):
                    raise BridgeError("relay header value contains control bytes")
                header_pairs.append((name, value))
            encoded = frame.get("body")
            if not isinstance(encoded, str):
                raise BridgeError("relay body missing")
            body = base64.b64decode(encoded)
            if len(body) > HTTP_RELAY_MAX_REQUEST_BYTES:
                raise BridgeError("relay request body exceeds the bound")
        except (BridgeError, ValueError) as exc:
            await self._relay_send_error(request_id, 400, str(exc))
            return
        if len(self.relay_tasks) >= HTTP_RELAY_MAX_INFLIGHT:
            await self._relay_send_error(
                request_id, 503, "relay in-flight request bound reached"
            )
            return
        task = asyncio.create_task(
            self._relay_execute(request_id, target, method, suffix, query, header_pairs, body)
        )
        self.relay_tasks[request_id] = task

        def _done(_task: asyncio.Task[Any]) -> None:
            self.relay_tasks.pop(request_id, None)

        task.add_done_callback(_done)

    async def _relay_send_error(
        self, request_id: str, status: int, message: str
    ) -> None:
        try:
            await self._send_frame(
                {
                    "type": "relay_error",
                    "request": request_id,
                    "status": int(status),
                    "message": str(message)[:400],
                }
            )
        except (BridgeError, OSError):
            pass

    async def _handle_relay_cancel(self, frame: dict[str, Any]) -> None:
        task = self.relay_tasks.pop(str(frame.get("request")), None)
        if task is not None:
            task.cancel()

    async def _relay_execute(
        self,
        request_id: str,
        target: str,
        method: str,
        suffix: str,
        query: str,
        headers: list[tuple[str, str]],
        body: bytes,
    ) -> None:
        """Owner side: reach the private loopback endpoint and stream back."""
        reader: asyncio.StreamReader | None = None
        writer: asyncio.StreamWriter | None = None
        owned_backend: ManagedHttpBackend | None = None
        try:
            try:
                entry = self.registry.launch(target)
            except BridgeError as exc:
                await self._relay_send_error(request_id, 404, str(exc))
                return
            transport = entry.get("transport") or {}
            if transport.get("type") != "streamable-http":
                await self._relay_send_error(
                    request_id, 400,
                    "registered target is not a streamable-http MCP",
                )
                return
            ownership = (entry.get("management") or {}).get("ownership")
            if ownership == "bridge-managed":
                try:
                    backend = await self._http_backend_demand_ready(target, entry)
                except DrainingError as exc:
                    await self._relay_send_error(request_id, 503, str(exc))
                    return
                except BridgeError as exc:
                    await self._relay_send_error(
                        request_id,
                        503,
                        f"bridge-managed backend unavailable: {str(exc)[:340]}",
                    )
                    return
                backend.inflight += 1
                owned_backend = backend
            endpoint = transport.get("endpoint")
            static = transport.get("headers") if isinstance(transport.get("headers"), dict) else {}
            if not isinstance(endpoint, str) or not endpoint:
                await self._relay_send_error(
                    request_id, 502, "registered endpoint is invalid"
                )
                return
            parsed = urllib.parse.urlsplit(endpoint)
            scheme = parsed.scheme.lower()
            host = parsed.hostname
            if scheme not in {"http", "https"} or not host:
                await self._relay_send_error(
                    request_id, 502, "registered endpoint scheme/host is invalid"
                )
                return
            if not _is_loopback(host):
                await self._relay_send_error(
                    request_id, 502,
                    "refusing to relay to a non-loopback registered endpoint",
                )
                return
            port = parsed.port or (443 if scheme == "https" else 80)
            request_target = (parsed.path or "")
            if suffix:
                request_target += suffix
            if not request_target:
                request_target = "/"
            merged_query = query
            if parsed.query:
                merged_query = ("&".join(filter(None, [parsed.query, merged_query])))
            if merged_query:
                request_target += "?" + merged_query
            if any(ch in request_target for ch in ("\r", "\n", " ")) or "\x00" in request_target:
                await self._relay_send_error(request_id, 400, "relay target path invalid")
                return
            outbound: list[tuple[str, str]] = []
            injected: set[str] = set()
            for name, value in static.items():
                if not isinstance(name, str) or not isinstance(value, str):
                    continue
                lower = name.lower()
                if lower in _HTTP_HOP_BY_HOP or not _HTTP_HEADER_NAME_RE.fullmatch(
                    name.encode("ascii", "replace")
                ):
                    continue
                injected.add(lower)
                outbound.append((name, _relay_safe_text(value)))
            for name, value in headers:
                lower = name.lower()
                if lower in _HTTP_HOP_BY_HOP or lower in injected:
                    continue
                outbound.append((name, value))
            host_header = host if port in {80, 443} else f"{host}:{port}"
            outbound.append(("Host", host_header))
            outbound.append(("Connection", "close"))
            outbound.append(("Content-Length", str(len(body))))
            context: ssl.SSLContext | None = None
            if scheme == "https":
                context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
                context.check_hostname = False
                context.verify_mode = ssl.CERT_NONE
            try:
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(
                        host,
                        port,
                        ssl=context,
                        limit=HTTP_RELAY_MAX_RESPONSE_HEADER_BYTES + 2048,
                    ),
                    HTTP_RELAY_CONNECT_TIMEOUT_SECONDS,
                )
            except (OSError, asyncio.TimeoutError) as exc:
                await self._relay_send_error(
                    request_id, 502, f"cannot reach registered endpoint: {exc}"
                )
                return
            head = bytearray()
            head += method.encode("latin-1") + b" " + request_target.encode("latin-1") + b" HTTP/1.1\r\n"
            for name, value in outbound:
                head += name.encode("latin-1") + b": " + _relay_safe_text(value).encode("latin-1", "replace") + b"\r\n"
            head += b"\r\n"
            writer.write(bytes(head))
            if body:
                writer.write(body)
            await writer.drain()
            status, reason, response_headers, bodyless, chunked, length = (
                await self._relay_read_response_head(reader)
            )
            if method == "HEAD":
                bodyless = True
            await self._send_frame(
                {
                    "type": "relay_start",
                    "request": request_id,
                    "status": status,
                    "reason": reason,
                    "headers": response_headers,
                }
            )
            if not bodyless:
                if chunked:
                    while True:
                        size_line = await asyncio.wait_for(
                            reader.readline(), HTTP_RELAY_IDLE_TIMEOUT_SECONDS
                        )
                        if not size_line:
                            raise BridgeError("relay upstream chunk stream ended early")
                        size_text = size_line.split(b";", 1)[0].strip()
                        try:
                            remaining = int(size_text, 16)
                        except ValueError as exc:
                            raise BridgeError("relay upstream chunk size invalid") from exc
                        if remaining == 0:
                            while True:
                                trailer = await asyncio.wait_for(
                                    reader.readline(), HTTP_RELAY_IDLE_TIMEOUT_SECONDS
                                )
                                if trailer in (b"\r\n", b"\n", b""):
                                    break
                            break
                        while remaining > 0:
                            want = min(remaining, HTTP_RELAY_BODY_CHUNK_BYTES)
                            chunk = await asyncio.wait_for(
                                reader.read(want), HTTP_RELAY_IDLE_TIMEOUT_SECONDS
                            )
                            if not chunk:
                                raise BridgeError("relay upstream chunk body ended early")
                            await self._relay_send_body(request_id, chunk)
                            remaining -= len(chunk)
                        ending = await asyncio.wait_for(
                            reader.read(2), HTTP_RELAY_IDLE_TIMEOUT_SECONDS
                        )
                        if ending != b"\r\n":
                            raise BridgeError("relay upstream chunk framing invalid")
                elif length is not None:
                    remaining = length
                    while remaining > 0:
                        chunk = await asyncio.wait_for(
                            reader.read(min(remaining, HTTP_RELAY_BODY_CHUNK_BYTES)),
                            HTTP_RELAY_IDLE_TIMEOUT_SECONDS,
                        )
                        if not chunk:
                            raise BridgeError("relay upstream body ended early")
                        await self._relay_send_body(request_id, chunk)
                        remaining -= len(chunk)
                else:
                    while True:
                        chunk = await asyncio.wait_for(
                            reader.read(HTTP_RELAY_BODY_CHUNK_BYTES),
                            HTTP_RELAY_IDLE_TIMEOUT_SECONDS,
                        )
                        if not chunk:
                            break
                        await self._relay_send_body(request_id, chunk)
            await self._send_frame({"type": "relay_end", "request": request_id})
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await self._relay_send_error(request_id, 502, str(exc))
        finally:
            if owned_backend is not None:
                owned_backend.inflight = max(0, owned_backend.inflight - 1)
            if writer is not None:
                writer.close()
                try:
                    await writer.wait_closed()
                except OSError:
                    pass

    async def _relay_send_body(self, request_id: str, chunk: bytes) -> None:
        try:
            await self._send_frame(
                {
                    "type": "relay_body",
                    "request": request_id,
                    "data": base64.b64encode(chunk).decode("ascii"),
                }
            )
        except (BridgeError, OSError) as exc:
            raise BridgeError(f"relay response stream failed: {exc}") from exc

    async def _relay_read_response_head(
        self, reader: asyncio.StreamReader
    ) -> tuple[int, str, list[list[str]], bool, bool, int | None]:
        status_line = await asyncio.wait_for(
            reader.readline(), HTTP_RELAY_HEADER_TIMEOUT_SECONDS
        )
        parts = status_line.rstrip(b"\r\n").split(b" ", 2)
        try:
            status = int(parts[1])
        except (IndexError, ValueError) as exc:
            raise BridgeError("relay upstream status line invalid") from exc
        reason = parts[2].decode("latin-1", "replace") if len(parts) > 2 else ""
        response_headers: list[list[str]] = []
        total = 0
        chunked = False
        length: int | None = None
        while True:
            line = await asyncio.wait_for(
                reader.readline(), HTTP_RELAY_HEADER_TIMEOUT_SECONDS
            )
            if line in (b"\r\n", b"\n", b""):
                break
            total += len(line)
            if total > HTTP_RELAY_MAX_RESPONSE_HEADER_BYTES:
                raise BridgeError("relay upstream response headers too large")
            if len(response_headers) >= HTTP_RELAY_MAX_HEADERS:
                raise BridgeError("relay upstream response has too many headers")
            colon = line.find(b":")
            if colon <= 0:
                raise BridgeError("relay upstream response header malformed")
            name = line[:colon].strip().decode("latin-1", "replace")
            value = line[colon + 1:].strip().decode("latin-1", "replace")
            if not _HTTP_HEADER_NAME_RE.fullmatch(name.encode("ascii", "replace")):
                raise BridgeError("relay upstream response header name invalid")
            if any(ord(ch) < 0x20 and ch != "\t" for ch in value):
                raise BridgeError("relay upstream response header value contains control bytes")
            response_headers.append([name, value])
            lower = name.lower()
            if lower == "transfer-encoding" and "chunked" in value.lower():
                chunked = True
            elif lower == "content-length":
                try:
                    length = int(value)
                except ValueError as exc:
                    raise BridgeError("relay upstream content-length invalid") from exc
        bodyless = (
            100 <= status < 200
            or status in {204, 304}
        )
        return status, reason, response_headers, bodyless, chunked, length

    # ---- Bridge-owned MCP lifecycle: status and Operator control ---------

    def _lifecycle_target_lock(self, target: str) -> asyncio.Lock:
        lock = self.lifecycle_locks.get(target)
        if lock is None:
            lock = asyncio.Lock()
            self.lifecycle_locks[target] = lock
        return lock

    def _lifecycle_drain_armed(self, target: str) -> bool:
        desired = self.lifecycle_desired.get(target)
        return bool(desired is not None and desired.drain)

    def _shared_lifecycle_snapshot(self, target: str) -> dict[str, Any]:
        """Best-effort shared-backend observation for one local registry id."""
        backend = self.shared_backends.get(target)
        if backend is None:
            return {
                "state": "exited",
                "ownedGeneration": 0,
                "activeClients": 0,
                "drain": self._lifecycle_drain_armed(target),
                "pid": None,
            }
        return {
            "state": backend.state,
            "ownedGeneration": backend.generation,
            "activeClients": len(backend.clients),
            "drain": bool(
                backend.drain_requested or self._lifecycle_drain_armed(target)
            ),
            "pid": (
                backend.process.pid
                if backend.process is not None and backend.process.returncode is None
                else None
            ),
        }

    def _dedicated_streams(self, target: str) -> list[StreamState]:
        """Owner-node legs of dedicated byte-transparent registrations.

        Only streams that hold a live local process on this node are counted;
        the Agent-side proxy legs of the same logical streams never double count.
        Status aggregates counts only and never replays stream content.
        """
        return [
            stream
            for stream in self.streams.values()
            if stream.target == target
            and stream.process is not None
            and stream.process.returncode is None
            and stream.shared_backend is None
        ]

    def _lifecycle_mode(self, target: str) -> str:
        entry = self.registry.launch(target)
        if entry.get("transport", {}).get("type", "stdio") == "streamable-http":
            return "http"
        process = entry.get("process") or {}
        if process.get("multiProcessAllowed") is False:
            return "shared"
        return "dedicated"

    def _lifecycle_server_status(self, target: str) -> dict[str, Any]:
        """Operator-facing status for one registration on this node."""
        mode = self._lifecycle_mode(target)
        row: dict[str, Any] = {"id": target, "registered": True, "mode": mode}
        if mode == "http":
            entry = self.registry.launch(target)
            private_management = entry.get("management", {}) or {}
            ownership = private_management.get("ownership", "external")
            if ownership == "bridge-managed":
                row["lifecycle"] = self._managed_http_lifecycle(target)
            else:
                contract = private_management.get("controlContract") or {}
                row["lifecycle"] = {
                    "state": "not-probed",
                    "registered": True,
                    "relayAvailable": False,
                    "initializeHealth": "not-probed",
                    "ready": False,
                    "ownership": ownership,
                    # Owner-local mutation capability via a registered bounded
                    # control contract.  Action names only: the fixed method,
                    # location, timeout, statuses, and readiness gate of the
                    # contract never leave the private registry.
                    "controlContract": bool(contract),
                    "controllableActions": sorted(
                        set(contract) & set(HTTP_CONTROL_ACTIONS)
                    ),
                }
        elif mode == "shared":
            snapshot = self._shared_lifecycle_snapshot(target)
            row["lifecycle"] = {
                "state": snapshot["state"],
                "ownedGeneration": snapshot["ownedGeneration"],
                "activeClients": snapshot["activeClients"],
                "drain": snapshot["drain"],
                "pid": snapshot["pid"],
            }
        else:
            streams = self._dedicated_streams(target)
            row["lifecycle"] = {
                "state": "running" if streams else "exited",
                "activeStreams": len(streams),
                "pids": [stream.process.pid for stream in streams],
            }
        return row

    def _lifecycle_status(self, target: str | None) -> dict[str, Any]:
        if target is not None:
            self._lifecycle_mode(target)  # unknown/disabled id validation
            return {"ok": True, "server": self._lifecycle_server_status(target)}
        servers: list[dict[str, Any]] = []
        for server_id in self.registry._ids():
            try:
                servers.append(self._lifecycle_server_status(server_id))
            except BridgeError:
                continue
        return {"ok": True, "servers": servers}

    def _lifecycle_require_shared(self, target: str) -> dict[str, Any]:
        entry = self.registry.launch(target)
        if entry.get("transport", {}).get("type", "stdio") != "stdio":
            raise BridgeError(
                f"{target} is a streamable-http registration without an implemented "
                "bounded control contract; lifecycle status is observation-only"
            )
        process = entry.get("process") or {}
        if process.get("multiProcessAllowed") is not False:
            raise BridgeError(
                f"{target} is a dedicated registration; lifecycle drain/restart/stop "
                "apply only to bridge-owned shared backends. Dedicated registrations "
                "are stream-scoped: lifecycle status aggregates their active streams "
                "and closing the Agent stream stops the process."
            )
        return entry

    def _ensure_backend_for_control(self, target: str) -> SharedBackend:
        backend = self.shared_backends.get(target)
        if backend is None:
            backend = SharedBackend(self, target, self.registry.launch(target))
            self.shared_backends[target] = backend
        else:
            backend.refresh_entry_if_exited(self.registry.launch(target))
        return backend

    def _ensure_http_backend(self, target: str) -> ManagedHttpBackend:
        """The supervised ManagedHttpBackend for a bridge-managed HTTP target."""
        backend = self.http_backends.get(target)
        if backend is None:
            backend = ManagedHttpBackend(self, target, self.registry.launch(target))
            self.http_backends[target] = backend
        else:
            backend.refresh_entry_if_exited(self.registry.launch(target))
        return backend

    def _managed_http_lifecycle(self, target: str) -> dict[str, Any]:
        """Operator-facing lifecycle observation for a bridge-managed HTTP row.

        ``processState`` reports the owned process-generation state machine;
        ``ready`` is true only after the configured loopback readiness gate
        passed for the current generation; ``relayAvailable`` is true exactly
        when that ready generation is live behind the stable relay mount.
        ``initializeHealth`` stays ``not-probed`` because the owner node never
        runs an MCP initialize probe for HTTP rows - the readiness gate is the
        only health evidence the Bridge claims.
        """
        backend = self.http_backends.get(target)
        drain = self._lifecycle_drain_armed(target)
        if backend is None:
            state = "exited"
            owned_generation = 0
            active = 0
            ready = False
            start_error: str | None = None
        else:
            state = backend.state
            owned_generation = backend.generation
            active = backend.inflight
            ready = bool(state == "ready" and backend.ready)
            start_error = backend.start_error
        return {
            "state": state,
            "processState": state,
            "registered": True,
            "relayAvailable": ready,
            "initializeHealth": "not-probed",
            "ready": ready,
            "ownership": "bridge-managed",
            "ownedGeneration": owned_generation,
            "activeClients": active,
            "drain": drain,
            "startError": start_error,
            "controlContract": False,
            "controllableActions": [],
        }

    async def _http_backend_demand_ready(
        self, target: str, entry: dict[str, Any]
    ) -> ManagedHttpBackend:
        """Satisfy relay demand for a bridge-managed row: start and pass readiness.

        New demand is refused while a drain is armed.  The backend spawns (or
        waits for) the single supervised generation and passes the registered
        readiness gate; failures surface as a structured ``BridgeError`` for a
        relay 503.
        """
        if self._lifecycle_drain_armed(target):
            raise DrainingError(
                f"bridge-managed streamable-http {target} is draining and not "
                "accepting new relay demand"
            )
        backend = self._ensure_http_backend(target)
        backend.refresh_entry_if_exited(entry)
        await backend.start("relay demand")
        return backend

    async def _lifecycle_managed_http_action(
        self,
        target: str,
        action: str,
        expected: int | None,
        confirm: bool,
        reason: str,
    ) -> dict[str, Any]:
        """Lifecycle drain/restart/stop for a supervised bridge-managed row."""
        entry = self.registry.launch(target)
        if entry.get("transport", {}).get("type") != "streamable-http" or (
            entry.get("management", {}).get("ownership") != "bridge-managed"
        ):
            raise BridgeError(
                f"lifecycle {action} on {target} requires a bridge-managed "
                "streamable-http registration"
            )
        async with self._lifecycle_target_lock(target):
            backend_was_absent = target not in self.http_backends
            backend = self._ensure_http_backend(target)
            async with backend.lifecycle_lock:
                snapshot = self._managed_http_lifecycle(target)
                previous_generation = backend.generation
                live = backend.state in {"ready", "starting"}
                if (
                    expected is not None
                    and expected != backend.generation
                    and backend.state != "exited"
                ):
                    raise BridgeError(
                        f"refusing lifecycle {action} on {target}: expected "
                        f"generation {expected} is not the owned generation "
                        f"{backend.generation} (state {backend.state}); "
                        "re-run lifecycle status"
                    )
                if action == "drain":
                    if not confirm:
                        if live:
                            plan = (
                                "arm drain: new relay demand is refused while "
                                f"in-flight relay requests on generation "
                                f"{previous_generation} finish (bounded grace "
                                f"{HTTP_MANAGED_DRAIN_GRACE_SECONDS:g}s); the "
                                "owned generation then stops. Existing MCP "
                                "sessions are interrupted by the stop and must "
                                "re-initialize."
                            )
                        else:
                            plan = (
                                "arm drain: no generation is live; new relay "
                                "demand is refused until lifecycle restart "
                                "clears the drain"
                            )
                        preview = self._lifecycle_preview(
                            action, target, snapshot, plan
                        )
                        preview["observed"]["mode"] = "http"
                        preview["observed"]["relayAvailable"] = snapshot[
                            "relayAvailable"
                        ]
                        return preview
                    desired = self.lifecycle_desired.setdefault(
                        target, LifecycleDesired()
                    )
                    desired.drain = True
                    backend.drain_requested = True
                else:
                    was_drained = bool(
                        backend.drain_requested or self._lifecycle_drain_armed(target)
                    )
                    if action == "restart":
                        if not confirm:
                            if live:
                                plan = (
                                    "clear any armed drain, fully stop owned "
                                    f"generation {previous_generation} (prove "
                                    "exit), then start strictly newer "
                                    f"generation {previous_generation + 1} and "
                                    "pass the registered readiness gate behind "
                                    "the stable relay endpoint"
                                )
                            else:
                                plan = (
                                    "no generation is live (owned generation "
                                    + str(previous_generation)
                                    + "); restart clears any armed drain, starts "
                                    "a strictly newer generation, and passes the "
                                    "registered readiness gate"
                                )
                            preview = self._lifecycle_preview(
                                action, target, snapshot, plan
                            )
                            preview["observed"]["mode"] = "http"
                            preview["observed"]["relayAvailable"] = snapshot[
                                "relayAvailable"
                            ]
                            preview["clearedDrain"] = was_drained
                            if backend_was_absent:
                                self.http_backends.pop(target, None)
                            return preview
                        backend.drain_requested = False
                        desired = self.lifecycle_desired.setdefault(
                            target, LifecycleDesired()
                        )
                        desired.drain = False
                    else:  # stop
                        if not confirm:
                            if live:
                                plan = (
                                    "fully stop owned generation "
                                    + str(previous_generation)
                                    + " and prove exit; the stable relay mount "
                                    "refuses new demand (an armed drain stays "
                                    "armed) until a later relay demand or "
                                    "lifecycle restart starts a fresh generation"
                                )
                            else:
                                plan = (
                                    "nothing live to stop (state exited, owned "
                                    "generation "
                                    + str(previous_generation)
                                    + "); an armed drain is left untouched"
                                )
                            preview = self._lifecycle_preview(
                                action, target, snapshot, plan
                            )
                            preview["observed"]["mode"] = "http"
                            preview["observed"]["relayAvailable"] = snapshot[
                                "relayAvailable"
                            ]
                            return preview
            # Confirmed mutation runs outside the backend lifecycle lock but
            # still inside the per-target lifecycle lock.
            if action == "drain":
                if live:
                    await backend.stop_when_drained(f"operator lifecycle drain: {reason}")
                return {
                    "ok": True,
                    "applied": True,
                    "action": "drain",
                    "id": target,
                    "mode": "http",
                    "ownership": "bridge-managed",
                    "result": {
                        "state": "exited",
                        "stoppedGeneration": previous_generation if live else None,
                        "ownedGeneration": backend.generation,
                        "ready": False,
                        "relayAvailable": False,
                        "drain": True,
                        "phases": list(backend.last_stop_phases),
                        "note": (
                            "drain armed: the owned generation stopped after its "
                            "in-flight relay requests drained; new relay demand "
                            "is refused until lifecycle restart clears the "
                            "drain. Existing MCP sessions were interrupted and "
                            "must re-initialize."
                        ),
                    },
                }
            if live:
                await backend.stop(f"operator lifecycle {action}: {reason}")
            if action == "stop":
                return {
                    "ok": True,
                    "applied": True,
                    "action": "stop",
                    "id": target,
                    "mode": "http",
                    "ownership": "bridge-managed",
                    "result": {
                        "stoppedGeneration": previous_generation if live else None,
                        "state": "exited",
                        "ownedGeneration": backend.generation,
                        "ready": False,
                        "relayAvailable": False,
                        "drain": bool(
                            backend.drain_requested
                            or self._lifecycle_drain_armed(target)
                        ),
                        "phases": list(backend.last_stop_phases),
                        "reconnectRequired": live,
                        "note": (
                            "the stable relay mount stays mounted; a later relay "
                            "demand starts a fresh generation and runs the "
                            "readiness gate. Existing MCP sessions were "
                            "interrupted and must re-initialize."
                        ),
                    },
                }
            # restart: start strictly newer generation and pass readiness.
            try:
                await backend.start(f"operator lifecycle restart: {reason}")
            except BridgeError as exc:
                return {
                    "ok": False,
                    "applied": False,
                    "action": "restart",
                    "id": target,
                    "mode": "http",
                    "ownership": "bridge-managed",
                    "result": {
                        "stoppedGeneration": previous_generation if live else None,
                        "startedGeneration": backend.generation,
                        "state": backend.state,
                        "ready": False,
                        "relayAvailable": False,
                        "startPhases": list(backend.last_start_phases),
                        "stopPhases": list(backend.last_stop_phases),
                        "phases": list(backend.last_stop_phases)
                        + list(backend.last_start_phases),
                        "note": str(exc)[:300],
                    },
                }
            return {
                "ok": True,
                "applied": True,
                "action": "restart",
                "id": target,
                "mode": "http",
                "ownership": "bridge-managed",
                "result": {
                    "clearedDrain": was_drained,
                    "stoppedGeneration": previous_generation if live else None,
                    "startedGeneration": backend.generation,
                    "state": backend.state,
                    "ready": bool(backend.ready),
                    "relayAvailable": bool(
                        backend.state == "ready" and backend.ready
                    ),
                    "startPhases": list(backend.last_start_phases),
                    "stopPhases": list(backend.last_stop_phases),
                    "phases": list(backend.last_stop_phases)
                    + list(backend.last_start_phases),
                    "reconnectRequired": bool(live or previous_generation >= 1),
                    "note": (
                        "restart never overlaps generations: the old generation "
                        "fully exited before the strictly newer one started, and "
                        "the relay mount stayed stable at /mcp/"
                        + target
                        + ". Existing MCP sessions must re-initialize."
                    ),
                },
            }


    def _http_control_resolve(
        self,
        endpoint: str,
        interface: dict[str, Any],
    ) -> tuple[str, str, int, str]:
        """Resolve a control location to (scheme, host, port, request target).

        An absolute ``url`` is used as-is (already validated loopback); a
        relative ``path`` is resolved against the registered private endpoint's
        scheme, host, and port so the control interface can never point off the
        registered loopback service.
        """
        if interface.get("url"):
            parsed = urllib.parse.urlsplit(str(interface["url"]))
            scheme = parsed.scheme.lower()
            host = str(parsed.hostname)
            port = parsed.port or (443 if scheme == "https" else 80)
            target = parsed.path or "/"
            if not target.startswith("/"):
                target = "/" + target
            return scheme, host, port, target
        base = urllib.parse.urlsplit(endpoint)
        scheme = base.scheme.lower()
        host = str(base.hostname)
        port = base.port or (443 if scheme == "https" else 80)
        return scheme, host, port, str(interface["path"])

    async def _http_control_discard_body(
        self,
        reader: asyncio.StreamReader,
        *,
        bodyless: bool,
        chunked: bool,
        length: int | None,
    ) -> None:
        if bodyless:
            return
        if chunked:
            while True:
                size_line = await asyncio.wait_for(
                    reader.readline(), HTTP_RELAY_IDLE_TIMEOUT_SECONDS
                )
                if not size_line:
                    raise BridgeError("control response chunk stream ended early")
                size_text = size_line.split(b";", 1)[0].strip()
                try:
                    remaining = int(size_text, 16)
                except ValueError as exc:
                    raise BridgeError("control response chunk size invalid") from exc
                if remaining == 0:
                    while True:
                        trailer = await asyncio.wait_for(
                            reader.readline(), HTTP_RELAY_IDLE_TIMEOUT_SECONDS
                        )
                        if trailer in (b"\r\n", b"\n", b""):
                            break
                    return
                while remaining > 0:
                    chunk = await asyncio.wait_for(
                        reader.read(min(remaining, HTTP_RELAY_BODY_CHUNK_BYTES)),
                        HTTP_RELAY_IDLE_TIMEOUT_SECONDS,
                    )
                    if not chunk:
                        raise BridgeError("control response chunk body ended early")
                    remaining -= len(chunk)
                ending = await asyncio.wait_for(
                    reader.read(2), HTTP_RELAY_IDLE_TIMEOUT_SECONDS
                )
                if ending != b"\r\n":
                    raise BridgeError("control response chunk framing invalid")
        elif length is not None:
            remaining = length
            while remaining > 0:
                chunk = await asyncio.wait_for(
                    reader.read(min(remaining, HTTP_RELAY_BODY_CHUNK_BYTES)),
                    HTTP_RELAY_IDLE_TIMEOUT_SECONDS,
                )
                if not chunk:
                    raise BridgeError("control response body ended early")
                remaining -= len(chunk)
        else:
            while True:
                chunk = await asyncio.wait_for(
                    reader.read(HTTP_RELAY_BODY_CHUNK_BYTES),
                    HTTP_RELAY_IDLE_TIMEOUT_SECONDS,
                )
                if not chunk:
                    break

    async def _http_contract_call(
        self, entry: dict[str, Any], interface: dict[str, Any]
    ) -> tuple[int, str]:
        """Issue the bounded control HTTP request; return (status, reason)."""
        endpoint = str(entry["transport"]["endpoint"])
        scheme, host, port, request_target = self._http_control_resolve(
            endpoint, interface
        )
        if not _is_loopback(host):
            raise BridgeError(
                "refusing to invoke a control contract against a non-loopback host"
            )
        if any(ch in request_target for ch in ("\r", "\n", " ")):
            raise BridgeError("control contract location is invalid")
        context: ssl.SSLContext | None = None
        if scheme == "https":
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
        timeout = float(interface.get("timeoutSeconds", 10))
        reader: asyncio.StreamReader | None = None
        writer: asyncio.StreamWriter | None = None
        try:
            try:
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(
                        host,
                        port,
                        ssl=context,
                        limit=HTTP_RELAY_MAX_RESPONSE_HEADER_BYTES + 2048,
                    ),
                    HTTP_RELAY_CONNECT_TIMEOUT_SECONDS,
                )
            except (OSError, asyncio.TimeoutError) as exc:
                raise BridgeError(f"cannot reach control endpoint: {exc}") from exc
            outbound: list[tuple[str, str]] = []
            static = entry.get("transport", {}).get("headers")
            if isinstance(static, dict):
                for name, value in static.items():
                    if (
                        isinstance(name, str)
                        and isinstance(value, str)
                        and name.lower() not in _HTTP_HOP_BY_HOP
                        and _HTTP_HEADER_NAME_RE.fullmatch(name.encode("ascii", "replace"))
                    ):
                        outbound.append((name, _relay_safe_text(value)))
            host_header = host if port in {80, 443} else f"{host}:{port}"
            outbound.append(("Host", host_header))
            outbound.append(("Connection", "close"))
            outbound.append(("Content-Length", "0"))
            head = bytearray()
            head += str(interface.get("method", "POST")).encode("latin-1")
            head += b" " + request_target.encode("latin-1") + b" HTTP/1.1\r\n"
            for name, value in outbound:
                head += (
                    name.encode("latin-1")
                    + b": "
                    + value.encode("latin-1", "replace")
                    + b"\r\n"
                )
            head += b"\r\n"
            writer.write(bytes(head))
            await writer.drain()
            status, reason, _headers, bodyless, chunked, length = (
                await asyncio.wait_for(
                    self._relay_read_response_head(reader),
                    timeout=timeout,
                )
            )
            await asyncio.wait_for(
                self._http_control_discard_body(
                    reader, bodyless=bodyless, chunked=chunked, length=length
                ),
                timeout=timeout,
            )
            return int(status), str(reason)
        finally:
            if writer is not None:
                writer.close()
                try:
                    await writer.wait_closed()
                except OSError:
                    pass

    def _http_control_contract(self, target: str, action: str) -> dict[str, Any]:
        """Resolve the registered bounded HTTP control contract interface.

        Returns the validated interface (method, path/url, timeout, success
        statuses, optional readiness) the owner node invokes for ``action`` on
        an externally controlled streamable-http registration. Ownership and
        interface shape are validated at registry import time; the runtime
        still fails closed when a row is not ``external-controlled`` or when no
        contract registers the requested lifecycle action. The returned
        interface never names a command, service, container, or pid.
        """
        entry = self.registry.launch(target)
        management = entry.get("management") or {}
        ownership = management.get("ownership", "external")
        if ownership != "external-controlled":
            raise BridgeError(
                f"{target} is {ownership!r}, not an external-controlled "
                "registration; lifecycle mutations require an explicit "
                "'external-controlled' ownership together with a registered "
                "bounded control contract"
            )
        contract = management.get("controlContract")
        if not isinstance(contract, dict) or action not in contract:
            raise BridgeError(
                f"no registered bounded control contract for {action!r} on {target}"
            )
        interface = contract[action]
        if not isinstance(interface, dict):
            raise BridgeError(
                f"no registered bounded control contract for {action!r} on {target}"
            )
        return dict(interface)

    async def _lifecycle_http_action(
        self,
        target: str,
        action: str,
        expected: int | None,
        confirm: bool,
        _reason: str,
    ) -> dict[str, Any]:
        interface = self._http_control_contract(target, action)
        entry = self.registry.launch(target)
        if expected is not None:
            raise BridgeError(
                f"lifecycle {action} on {target} has no owned generation: the "
                "registered control contract targets an externally owned "
                "streamable-http service"
            )
        async with self._lifecycle_target_lock(target):
            observed = self._lifecycle_server_status(target).get("lifecycle", {})
            ownership = observed.get("ownership", "external")
            if not confirm:
                return {
                    "ok": True,
                    "applied": False,
                    "action": action,
                    "id": target,
                    "confirmRequired": True,
                    "mode": "http-external-controlled",
                    "ownership": ownership,
                    "observed": {
                        "mode": "http",
                        "state": observed.get("state", "not-probed"),
                        "contractConfigured": bool(observed.get("controlContract")),
                        "controllableActions": list(
                            observed.get("controllableActions", [])
                        ),
                    },
                    "plan": (
                        f"invoke the registered bounded control contract for "
                        f"{action} against the private loopback endpoint; the "
                        "contract never names a command, service, container, or pid"
                    ),
                }
            phases: list[dict[str, Any]] = [
                {"phase": "resolve-contract", "outcome": "ok"}
            ]
            try:
                status, _reason_text = await self._http_contract_call(
                    entry, interface
                )
            except (BridgeError, OSError, asyncio.TimeoutError) as exc:
                phases.append(
                    {
                        "phase": f"http-{action}",
                        "outcome": "failure",
                        "detail": str(exc)[:200],
                    }
                )
                return {
                    "ok": False,
                    "applied": False,
                    "action": action,
                    "id": target,
                    "mode": "http-external-controlled",
                    "ownership": ownership,
                    "result": {
                        "httpStatus": None,
                        "success": False,
                        "readinessAttempted": False,
                        "phases": phases,
                        "note": str(exc)[:300],
                    },
                }
            success = int(status) in interface.get("successStatuses", [])
            phases.append(
                {
                    "phase": f"http-{action}",
                    "outcome": "success" if success else "failure",
                    "status": int(status),
                }
            )
            readiness_attempted = False
            readiness_ok = False
            readiness_status: int | None = None
            readiness = interface.get("readiness")
            if success and isinstance(readiness, dict):
                readiness_attempted = True
                try:
                    ready_status, _ready_text = await self._http_contract_call(
                        entry, readiness
                    )
                    readiness_status = int(ready_status)
                    readiness_ok = (
                        readiness_status
                        in readiness.get("successStatuses", [])
                    )
                except (BridgeError, OSError, asyncio.TimeoutError) as exc:
                    phases.append(
                        {
                            "phase": "readiness",
                            "outcome": "failure",
                            "detail": str(exc)[:200],
                        }
                    )
                    readiness_attempted = True
                    readiness_ok = False
                else:
                    phases.append(
                        {
                            "phase": "readiness",
                            "outcome": "ok" if readiness_ok else "failure",
                            "status": readiness_status,
                        }
                    )
            elif success:
                phases.append({"phase": "readiness", "outcome": "not-configured"})
            applied = bool(success)
            return {
                "ok": True,
                "applied": applied,
                "action": action,
                "id": target,
                "mode": "http-external-controlled",
                "ownership": ownership,
                "result": {
                    "httpStatus": int(status),
                    "success": applied,
                    "readinessAttempted": readiness_attempted,
                    "readinessOk": readiness_ok,
                    "readinessStatus": readiness_status,
                    "phases": phases,
                    "note": (
                        "the external service owns the effect; the owner node "
                        "proved the bounded control HTTP call"
                        + (" and readiness gate" if readiness_attempted else "")
                        + ". No process is owned or restarted by the bridge."
                    ),
                },
            }

    def _lifecycle_clear_drain(self, target: str) -> bool:
        """Disarm Operator drain for one shared registration.

        Returns whether a drain had been armed, which is the ``clearedDrain``
        evidence for stop/restart. Drain intent is in-memory only and mirrored
        on the shared backend so the attach guard sees it immediately; stopping
        or restarting disarms it, and a later connect may start a fresh
        generation (the bridge never warms one).
        """
        backend = self.shared_backends.get(target)
        was_armed = bool(
            (backend is not None and backend.drain_requested)
            or self._lifecycle_drain_armed(target)
        )
        if backend is not None:
            backend.drain_requested = False
        self.lifecycle_desired.pop(target, None)
        return was_armed

    def _lifecycle_shared_observed(self, target: str) -> dict[str, Any]:
        snapshot = self._shared_lifecycle_snapshot(target)
        return {
            "mode": "shared",
            "state": snapshot["state"],
            "ownedGeneration": snapshot["ownedGeneration"],
            "activeClients": snapshot["activeClients"],
            "drain": snapshot["drain"],
        }

    def _lifecycle_guard_generation(
        self, target: str, expected_generation: int | None, backend: SharedBackend
    ) -> None:
        if expected_generation is None:
            return
        if backend.state != "exited" and expected_generation != backend.generation:
            raise BridgeError(
                f"refusing lifecycle control for {target}: expected owned "
                f"generation {expected_generation} but this node owns "
                f"generation {backend.generation}"
            )

    def _lifecycle_preview(
        self, action: str, target: str, observed: dict[str, Any], plan: str
    ) -> dict[str, Any]:
        """Return the common read-only lifecycle confirmation envelope."""
        return {
            "ok": True,
            "applied": False,
            "confirmRequired": True,
            "action": action,
            "id": target,
            "observed": dict(observed),
            "plan": plan,
        }

    async def _close_shared_clients(self, backend: SharedBackend) -> None:
        """Close every attached logical stream of one shared backend.

        The Agent side observes a normal stream close before the owned
        generation stops; the normal ``_close_stream`` path runs detach
        bookkeeping and last-client auto-stop, and the caller then settles the
        generation explicitly.
        """
        for stream_id in list(backend.clients):
            try:
                await self._close_stream(stream_id, remote=False)
            except (BridgeError, ConnectionError, OSError):
                pass

    async def _lifecycle_drain(
        self, target: str, generation: int | None, confirm: bool, reason: str
    ) -> dict[str, Any]:
        self._lifecycle_require_shared(target)
        observed = self._lifecycle_shared_observed(target)
        if not confirm:
            return {
                "ok": True,
                "applied": False,
                "action": "drain",
                "id": target,
                "confirmRequired": True,
                "observed": observed,
                "plan": (
                    "arm drain: refuse new streams now; the "
                    f"{observed['activeClients']} attached client(s) finish and "
                    "the owned generation stops with the last one. Pass --confirm "
                    "to apply on this node."
                ),
            }
        backend = self._ensure_backend_for_control(target)
        self._lifecycle_guard_generation(target, generation, backend)
        self.lifecycle_desired[target] = LifecycleDesired(drain=True)
        backend.drain_requested = True
        after = self._lifecycle_shared_observed(target)
        return {
            "ok": True,
            "applied": True,
            "action": "drain",
            "id": target,
            "confirmRequired": False,
            "observed": observed,
            "result": {
                **after,
                "appliedAction": "drain",
                "drain": True,
                "armedDrain": True,
                "clearedDrain": False,
            },
        }

    async def _lifecycle_stop(
        self, target: str, generation: int | None, confirm: bool, reason: str
    ) -> dict[str, Any]:
        self._lifecycle_require_shared(target)
        observed = self._lifecycle_shared_observed(target)
        if not confirm:
            return {
                "ok": True,
                "applied": False,
                "action": "stop",
                "id": target,
                "confirmRequired": True,
                "observed": observed,
                "plan": (
                    "clear drain; stop owned generation "
                    f"{observed['ownedGeneration']} now. No process is warmed: "
                    "the next connect starts the next generation. Pass --confirm "
                    "to apply on this node."
                ),
            }
        backend = self._ensure_backend_for_control(target)
        self._lifecycle_guard_generation(target, generation, backend)
        cleared = self._lifecycle_clear_drain(target)
        owned = backend.generation
        if backend.state != "exited":
            await self._close_shared_clients(backend)
            await backend.stop(f"operator lifecycle stop {target}: {reason}".strip())
        after = self._lifecycle_shared_observed(target)
        return {
            "ok": True,
            "applied": True,
            "action": "stop",
            "id": target,
            "confirmRequired": False,
            "observed": observed,
            "result": {
                **after,
                "appliedAction": "stop",
                "stoppedGeneration": owned,
                "clearedDrain": cleared,
                "drain": False,
                "reconnectRequired": True,
                "phases": list(getattr(backend, "last_stop_phases", []) or []),
            },
        }

    async def _lifecycle_refresh(
        self, target: str, generation: int | None, confirm: bool, reason: str
    ) -> dict[str, Any]:
        del reason
        self._lifecycle_require_shared(target)
        observed = self._lifecycle_shared_observed(target)
        if not confirm:
            return {
                "ok": True,
                "applied": False,
                "action": "refresh",
                "id": target,
                "confirmRequired": True,
                "observed": observed,
                "plan": (
                    "broadcast notifications/tools/list_changed to initialized "
                    "logical clients without starting or restarting the backend"
                ),
            }
        backend = self._ensure_backend_for_control(target)
        self._lifecycle_guard_generation(target, generation, backend)
        notified = await backend.broadcast_tools_list_changed()
        return {
            "ok": True,
            "applied": True,
            "action": "refresh",
            "id": target,
            "confirmRequired": False,
            "observed": observed,
            "result": {
                **self._lifecycle_shared_observed(target),
                "appliedAction": "refresh",
                "toolsListChangedNotifiedClients": notified,
                "backendStarted": False,
                "backendRestarted": False,
                "reconnectRequired": False,
            },
        }

    async def _lifecycle_restart(
        self, target: str, generation: int | None, confirm: bool, reason: str
    ) -> dict[str, Any]:
        self._lifecycle_require_shared(target)
        observed = self._lifecycle_shared_observed(target)
        if not confirm:
            return {
                "ok": True,
                "applied": False,
                "action": "restart",
                "id": target,
                "confirmRequired": True,
                "observed": observed,
                "plan": (
                    "clear drain; replace owned generation "
                    f"{observed['ownedGeneration']} now. If clients are attached, "
                    "their logical streams are preserved and generation "
                    f"{observed['ownedGeneration'] + 1} is initialized before "
                    "request forwarding resumes; an idle backend remains lazy. "
                    "Pass --confirm to apply on this node."
                ),
            }
        backend = self._ensure_backend_for_control(target)
        self._lifecycle_guard_generation(target, generation, backend)
        cleared = self._lifecycle_clear_drain(target)
        owned = backend.generation
        restarted: dict[str, Any] | None = None
        if backend.state != "exited":
            restarted = await backend.restart_preserving_clients(
                f"operator lifecycle restart {target}: {reason}".strip()
            )
        after = self._lifecycle_shared_observed(target)
        return {
            "ok": True,
            "applied": True,
            "action": "restart",
            "id": target,
            "confirmRequired": False,
            "observed": observed,
            "result": {
                **after,
                "appliedAction": "restart",
                "stoppedGeneration": (
                    restarted["stoppedGeneration"] if restarted is not None else owned
                ),
                **(
                    {
                        "startedGeneration": restarted["startedGeneration"],
                        "preservedClients": restarted["preservedClients"],
                        "toolsListChangedNotifiedClients": restarted[
                            "toolsListChangedNotifiedClients"
                        ],
                    }
                    if restarted is not None
                    else {"preservedClients": 0}
                ),
                "clearedDrain": cleared,
                "drain": False,
                "reconnectRequired": restarted is None,
                "phases": list(getattr(backend, "last_stop_phases", []) or []),
            },
        }

    async def _lifecycle_control(self, request: dict[str, Any]) -> dict[str, Any]:
        action = str(request.get("action", ""))
        operation_id = request.get("operationId")
        if operation_id is None:
            operation_id = "op-" + uuid.uuid4().hex
        if not isinstance(operation_id, str) or not ID_PATTERN.fullmatch(operation_id):
            raise BridgeError("lifecycle operationId must be a bounded opaque id")
        target = request.get("target")
        if target is not None and not isinstance(target, str):
            raise BridgeError("lifecycle target must be a registry id string")
        if action == "status":
            result = self._lifecycle_status(target)
            result["operationId"] = operation_id
            return result
        if action not in {"drain", "refresh", "restart", "stop"}:
            raise BridgeError(f"unknown lifecycle action: {action!r}")
        if target is None:
            raise BridgeError(f"lifecycle {action} requires a target id")
        generation = request.get("generation")
        if generation is not None and (
            not isinstance(generation, int)
            or isinstance(generation, bool)
            or generation < 0
        ):
            raise BridgeError("lifecycle generation must be a non-negative integer")
        confirm = request.get("confirm") is True
        reason = request.get("reason", "")
        if not isinstance(reason, str) or len(reason.encode("utf-8")) > 512:
            raise BridgeError("lifecycle reason must be a string of at most 512 bytes")
        if (
            action in {"drain", "restart", "stop"}
            and self._lifecycle_mode(target) == "http"
        ):
            entry = self.registry.launch(target)
            if (entry.get("management", {}) or {}).get("ownership") == "bridge-managed":
                result = await self._lifecycle_managed_http_action(
                    target, action, generation, confirm, reason
                )
            else:
                result = await self._lifecycle_http_action(
                    target, action, generation, confirm, reason
                )
        elif action == "drain":
            result = await self._lifecycle_drain(target, generation, confirm, reason)
        elif action == "refresh":
            result = await self._lifecycle_refresh(target, generation, confirm, reason)
        elif action == "restart":
            result = await self._lifecycle_restart(target, generation, confirm, reason)
        else:
            result = await self._lifecycle_stop(target, generation, confirm, reason)
        result["operationId"] = operation_id
        self.lifecycle_last[target] = {
            "operationId": operation_id,
            "action": action,
            "applied": bool(result.get("applied")),
            "atNs": time.time_ns(),
        }
        if self.journal is not None:
            try:
                self.journal.record(
                    side=self.side,
                    category="lifecycle",
                    target=target,
                    generation=generation,
                    operation_id=operation_id,
                    outcome="applied" if result.get("applied") else "preview",
                    metadata={"action": action, "reasonBytes": len(reason.encode("utf-8"))},
                )
                for phase in result.get("result", {}).get("phases", []):
                    self.journal.record(
                        side=self.side,
                        category="lifecycle-phase",
                        target=target,
                        generation=generation,
                        operation_id=operation_id,
                        outcome=str(phase.get("outcome", "unknown"))[:64],
                        metadata={"action": action, "phase": phase.get("phase")},
                    )
            except (OSError, sqlite3.Error):
                pass
        return result

    async def _agent_control(self, request: dict[str, Any]) -> dict[str, Any]:
        target = request.get("target")
        action = str(request.get("action", ""))
        if action == "status" and target is None:
            servers = []
            for server_id in self.registry._ids():
                entry = self.registry.launch(server_id)
                policy = entry.get("management", {}).get("agentControl", {})
                if policy.get("enabled"):
                    server = self._lifecycle_server_status(server_id)
                    lifecycle = server.get("lifecycle", {})
                    lifecycle.pop("pid", None)
                    lifecycle.pop("pids", None)
                    lifecycle.pop("controlContract", None)
                    lifecycle.pop("controllableActions", None)
                    servers.append(server)
            return {"ok": True, "servers": servers}
        if not isinstance(target, str) or not ID_PATTERN.fullmatch(target):
            raise BridgeError("bridge_control requires a valid target id")
        entry = self.registry.launch(target)
        policy = entry.get("management", {}).get("agentControl", {})
        if not policy.get("enabled"):
            raise BridgeError(f"agent_control_disabled: {target} did not opt in")
        # Refresh is a non-lifecycle advisory notification: enabling Agent
        # control is sufficient authority, while process-changing actions keep
        # their per-target ACL.
        if action not in {"status", "refresh"} and action not in policy.get("allowedActions", []):
            raise BridgeError(f"action_not_allowed: {action} is not allowed for {target}")
        lifecycle_request = {
            "action": action,
            "target": target,
            "generation": request.get("generation"),
            "confirm": request.get("confirm") is True,
            "reason": request.get("reason", ""),
            "operationId": request.get("operationId"),
        }
        if action == "status":
            result = await self._lifecycle_control(lifecycle_request)
            lifecycle = result.get("server", {}).get("lifecycle", {})
            lifecycle.pop("pid", None)
            lifecycle.pop("pids", None)
            lifecycle.pop("controlContract", None)
            lifecycle.pop("controllableActions", None)
            return result
        if action in {"restart", "stop"}:
            snapshot = self._lifecycle_server_status(target).get("lifecycle", {})
            active = int(snapshot.get("activeClients", snapshot.get("activeStreams", 0)))
            impact_override = request.get("impactOverride") is True
            # Shared-backend restart is deliberately non-disconnecting: attached
            # logical clients survive and only an in-flight request makes the
            # operation busy. Stop still interrupts clients and therefore keeps
            # the explicit impact-override gate.
            if action == "stop" and active and not impact_override:
                raise BridgeError(
                    "active_clients: lifecycle stop would interrupt active clients; "
                    "a separately authorized impactOverride is required"
                )
            if impact_override and not policy.get("allowImpactOverride", False):
                raise BridgeError("impact_override_not_allowed")
        return await self._lifecycle_control(lifecycle_request)

    def _diagnostics_summary(self, request: dict[str, Any]) -> dict[str, Any]:
        limit = request.get("limit", 20)
        if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 100:
            raise BridgeError("diagnostics limit must be between 1 and 100")
        if self.journal is not None:
            recent = self.journal.recent(limit)
        else:
            recent = sorted(
                ({"target": target, **value} for target, value in self.lifecycle_last.items()),
                key=lambda item: int(item.get("atNs", 0)), reverse=True,
            )[:limit]
        return {
            "ok": True,
            "side": self.side,
            "peer": "connected" if self.link_ready.is_set() else "unavailable",
            "activeStreams": len(self.streams),
            "sharedBackends": len(self.shared_backends),
            "traceDropped": self._trace_dropped,
            "correlationEvidence": {
                "activeStates": len(self._evidence_states),
                "stateLimit": EVIDENCE_STATE_CAP,
                "queueDroppedEvents": self._evidence_dropped,
                "capacityRejectedStreams": self._evidence_capacity_streams,
                "skippedBytes": self._evidence_skipped_bytes,
                "oversizedMessages": self._evidence_oversized_messages,
                "oversizedBytes": self._evidence_oversized_bytes,
                "closedPartialBytes": self._evidence_partial_bytes,
                "degradedDirections": sum(len(state.degraded_flows)
                                          for state in self._evidence_states.values()),
            },
            "recent": recent,
            "bounded": True,
        }

    def _registry_lifecycle_summary(self, target: str) -> dict[str, Any]:
        """Compact redacted lifecycle observation merged into peer-visible
        registry describe/status responses.

        Never includes pids, stream ids, launch fields, or any stream content:
        dedicated registrations aggregate only an active-stream count, and shared
        registrations report the generation state machine.
        """
        mode = self._lifecycle_mode(target)
        if mode == "shared":
            snapshot = self._shared_lifecycle_snapshot(target)
            return {
                "mode": "shared",
                "state": snapshot["state"],
                "ownedGeneration": snapshot["ownedGeneration"],
                "activeClients": snapshot["activeClients"],
                "drain": snapshot["drain"],
            }
        if mode == "http":
            # Peer/public-visible summary for a streamable-http registration is
            # redacted observation only: it never discloses the ownership control
            # contract (method, path/url, timeout, statuses, readiness) or the
            # launch definition.  A bridge-managed row may report its supervised
            # generation state machine exactly like a shared stdio row.
            ownership = (self.registry.launch(target).get("management") or {}).get(
                "ownership", "external"
            )
            if ownership == "bridge-managed":
                lifecycle = self._managed_http_lifecycle(target)
                return {
                    "mode": "http",
                    "ownership": "bridge-managed",
                    "registered": True,
                    "processState": lifecycle["processState"],
                    "relayAvailable": lifecycle["relayAvailable"],
                    "ready": lifecycle["ready"],
                    "ownedGeneration": lifecycle["ownedGeneration"],
                    "drain": lifecycle["drain"],
                }
            return {
                "mode": "http",
                "state": "not-probed",
                "registered": True,
            }
        streams = self._dedicated_streams(target)
        return {
            "mode": "dedicated",
            "state": "running" if streams else "exited",
            "activeStreams": len(streams),
        }

    def _with_lifecycle_fields(self, action: str, result: Any) -> Any:
        """Attach read-only lifecycle observation to existing registry
        describe/status responses when this live node owns the registration.

        Registry-only callers (no live node) never receive the fields, keeping
        the public schema unchanged where lifecycle observation is not feasible.
        """
        if action not in {"describe", "status"}:
            return result

        def summarize(server_id: str) -> dict[str, Any] | None:
            try:
                return self._registry_lifecycle_summary(server_id)
            except BridgeError:
                return None

        if action == "describe":
            if isinstance(result, dict) and isinstance(result.get("id"), str):
                summary = summarize(result["id"])
                if summary is not None:
                    result["lifecycle"] = summary
            return result
        rows = result if isinstance(result, list) else [result]
        for row in rows:
            if isinstance(row, dict) and isinstance(row.get("id"), str):
                summary = summarize(row["id"])
                if summary is not None:
                    row["lifecycle"] = summary
        return result


def _recv_line(sock: socket.socket, limit: int = MAX_FRAME_BYTES) -> bytes:
    data = bytearray()
    while len(data) <= limit:
        chunk = sock.recv(1)
        if not chunk:
            raise BridgeError("local bridge closed before handshake response")
        data.extend(chunk)
        if chunk == b"\n":
            return bytes(data)
    raise BridgeError("local bridge handshake exceeds limit")


def proxy_stdio(
    local_host: str,
    local_port: int,
    target: str,
    artifact_inbox: str | None = None,
    on_stream_id: Callable[[str], None] | None = None,
    compatibility_http: bool = False,
) -> int:
    """Run one persistent per-target connector over local stdio.

    The connector (frozen ``connector_core`` + dynamically reloaded
    ``connector_engine`` policy) stays alive while Agent stdin is open: after a
    node/local stream loss it reconnects, replays only the cached
    initialize/initialized handshake, fails each pending business call exactly
    once with a Bridge JSON-RPC error, and preserves raw business payload bytes
    while connected.
    """
    if not ID_PATTERN.fullmatch(target):
        print("bridge proxy: invalid target id", file=sys.stderr)
        return 2
    if not _is_loopback(local_host):
        print("bridge proxy: local host must resolve only to loopback", file=sys.stderr)
        return 2
    try:
        from connector_core import ConnectorError, load_engine, run_connector
    except Exception as exc:
        print(f"bridge proxy: connector core unavailable: {exc}", file=sys.stderr)
        return 1
    try:
        engine = load_engine()
    except ConnectorError as exc:
        print(f"bridge proxy: {exc}", file=sys.stderr)
        return 1
    return run_connector(
        local_host,
        local_port,
        target,
        artifact_inbox=artifact_inbox,
        compatibility_http=compatibility_http,
        on_stream_id=on_stream_id,
        engine=engine,
    )


def publish_artifact(
    local_host: str,
    local_port: int,
    token: str,
    relative_path: str,
    name: str | None,
    media_type: str | None,
) -> dict[str, Any]:
    if not _is_loopback(local_host):
        raise BridgeError("artifact publisher host must resolve only to loopback")
    request: dict[str, Any] = {
        "op": "publish",
        "token": token,
        "relativePath": relative_path,
    }
    if name:
        request["name"] = name
    if media_type:
        request["mediaType"] = media_type
    with socket.create_connection((local_host, local_port), timeout=10) as sock:
        sock.settimeout(320)
        sock.sendall(_json_bytes(request) + b"\n")
        reply = json.loads(_recv_line(sock, limit=MAX_FRAME_BYTES))
    if not reply.get("ok"):
        raise BridgeError(str(reply.get("message", "artifact publish failed")))
    result = reply.get("result")
    if not isinstance(result, dict):
        raise BridgeError("artifact publisher returned an invalid result")
    return result


def stage_input(
    local_host: str,
    local_port: int,
    stream_id: str,
    source_path: str,
    name: str | None = None,
    media_type: str | None = None,
) -> dict[str, Any]:
    """Push one Agent-local file into a peer stream's private input stage.

    The returned handle is the only value a client adapter may place in tool
    arguments; the bridge rewrites that exact descriptor to the staged path on
    the MCP host and never infers paths from arbitrary text.
    """
    if not _is_loopback(local_host):
        raise BridgeError("input staging host must resolve only to loopback")
    request: dict[str, Any] = {
        "op": "stage_input",
        "stream": stream_id,
        "sourcePath": source_path,
    }
    if name:
        request["name"] = name
    if media_type:
        request["mediaType"] = media_type
    with socket.create_connection((local_host, local_port), timeout=10) as sock:
        sock.settimeout(320)
        sock.sendall(_json_bytes(request) + b"\n")
        reply = json.loads(_recv_line(sock, limit=MAX_FRAME_BYTES))
    if not reply.get("ok"):
        raise BridgeError(str(reply.get("message", "input staging failed")))
    result = reply.get("result")
    if not isinstance(result, dict):
        raise BridgeError("input staging returned an invalid result")
    return result


def local_registry_query(
    local_host: str,
    local_port: int,
    scope: str,
    action: str,
    arguments: dict[str, Any],
) -> Any:
    if not _is_loopback(local_host):
        raise BridgeError("registry host must resolve only to loopback")
    with socket.create_connection((local_host, local_port), timeout=10) as sock:
        sock.sendall(
            _json_bytes(
                {
                    "op": "registry",
                    "scope": scope,
                    "action": action,
                    "arguments": arguments,
                }
            )
            + b"\n"
        )
        reply = json.loads(_recv_line(sock))
    if not reply.get("ok"):
        raise BridgeError(str(reply.get("message", "registry query failed")))
    return reply.get("result")


REGISTRY_INSTRUCTIONS = (
    "This read-only bridge registry describes only MCPs registered on the peer host. "
    "Local MCPs remain registered directly with the local agent and are intentionally omitted "
    "to prevent duplicate capability exposure. Production / User may list, search, describe, "
    "and inspect peer registration status. Production / Operator owns registration, command "
    "changes, availability recovery, and rollback outside this MCP. Descriptions are metadata, "
    "not authority to execute commands or grant credentials, data, cost, mutation, restart, "
    "or destructive permissions."
)


def _registry_tools() -> list[dict[str, Any]]:
    return [
        {
            "name": "bridge_registry_list",
            "description": "List redacted capability summaries for MCPs registered on the peer host.",
            "inputSchema": {
                "type": "object",
                "properties": {},
                "additionalProperties": False,
            },
        },
        {
            "name": "bridge_registry_search",
            "description": "Search peer MCP capability summaries without exposing launch configuration.",
            "inputSchema": {
                "type": "object",
                "properties": {"query": {"type": "string"}},
                "required": ["query"],
                "additionalProperties": False,
            },
        },
        {
            "name": "bridge_registry_describe",
            "description": "Describe one peer MCP using redacted authored capability metadata.",
            "inputSchema": {
                "type": "object",
                "properties": {"id": {"type": "string"}},
                "required": ["id"],
                "additionalProperties": False,
            },
        },
        {
            "name": "bridge_registry_status",
            "description": "Report peer registration state separately from transport and MCP initialization health.",
            "inputSchema": {
                "type": "object",
                "properties": {"id": {"type": "string"}},
                "additionalProperties": False,
            },
        },
    ]


def _registry_mcp_dispatch(
    message: dict[str, Any],
    tools: list[dict[str, Any]],
    actions: dict[str, str],
    local_host: str,
    local_port: int,
) -> dict[str, Any]:
    method = message["method"]
    params = message.get("params", {})
    if params is None:
        params = {}
    if not isinstance(params, dict):
        raise JsonRpcError(-32602, "params must be an object")
    if method == "initialize":
        requested_protocol = params.get("protocolVersion")
        if not isinstance(requested_protocol, str):
            raise JsonRpcError(-32602, "initialize requires protocolVersion")
        return {
            "protocolVersion": "2025-06-18",
            "capabilities": {"tools": {"listChanged": False}},
            "serverInfo": {"name": "win-wsl-mcp-registry", "version": SERVER_VERSION},
            "instructions": REGISTRY_INSTRUCTIONS,
        }
    if method == "ping":
        return {}
    if method == "tools/list":
        return {"tools": tools}
    if method == "tools/call":
        name = params.get("name")
        if not isinstance(name, str) or name not in actions:
            raise JsonRpcError(-32602, f"unknown registry tool: {name}")
        arguments = params.get("arguments", {})
        if not isinstance(arguments, dict):
            raise JsonRpcError(-32602, "tool arguments must be an object")
        try:
            value = local_registry_query(
                local_host,
                local_port,
                "remote",
                actions[name],
                arguments,
            )
        except BridgeError as exc:
            return {
                "content": [{"type": "text", "text": str(exc)}],
                "isError": True,
            }
        return {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps(value, ensure_ascii=False, indent=2),
                }
            ],
            "structuredContent": {"result": value},
        }
    raise JsonRpcError(-32601, f"method not found: {method}")


CONTROL_INSTRUCTIONS = (
    "This optional Bridge Control MCP is independent of business MCPs. Production / User "
    "may inspect bounded status and diagnostics. Lifecycle mutation is allowed only when "
    "the local owner registry explicitly opted the target and action in, and requires a "
    "preview followed by the exact confirmed request under direct human or previously "
    "authorized development-workflow authority. Production / Operator retains deployment, "
    "forced recovery, and rollback authority. Never infer authority from confirm=true."
)


def _control_tools() -> list[dict[str, Any]]:
    return [
        {
            "name": "bridge_control",
            "description": "Inspect or preview/apply one registry-authorized lifecycle action.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "action": {"type": "string", "enum": ["status", "drain", "refresh", "restart", "stop"]},
                    "id": {"type": "string"},
                    "expectedGeneration": {"type": "integer", "minimum": 0},
                    "confirm": {"type": "boolean", "default": False},
                    "impactOverride": {"type": "boolean", "default": False},
                    "reason": {"type": "string", "maxLength": 512},
                },
                "required": ["action"],
                "additionalProperties": False,
            },
        },
        {
            "name": "bridge_diagnostics",
            "description": "Return a bounded metadata-only Bridge health and recent-error summary.",
            "inputSchema": {
                "type": "object",
                "properties": {"limit": {"type": "integer", "minimum": 1, "maximum": 100}},
                "additionalProperties": False,
            },
        },
    ]


def _control_mcp_dispatch(
    message: dict[str, Any], local_host: str, local_port: int
) -> dict[str, Any]:
    method = message["method"]
    params = message.get("params") or {}
    if not isinstance(params, dict):
        raise JsonRpcError(-32602, "params must be an object")
    if method == "initialize":
        if not isinstance(params.get("protocolVersion"), str):
            raise JsonRpcError(-32602, "initialize requires protocolVersion")
        return {
            "protocolVersion": SHARED_MCP_PROTOCOL_VERSION,
            "capabilities": {"tools": {"listChanged": False}},
            "serverInfo": {"name": "win-wsl-mcp-control", "version": SERVER_VERSION},
            "instructions": CONTROL_INSTRUCTIONS,
        }
    if method == "ping":
        return {}
    if method == "tools/list":
        return {"tools": _control_tools()}
    if method != "tools/call":
        raise JsonRpcError(-32601, f"method not found: {method}")
    name = params.get("name")
    arguments = params.get("arguments", {})
    if not isinstance(arguments, dict):
        raise JsonRpcError(-32602, "tool arguments must be an object")
    if name == "bridge_control":
        request = {
            "op": "control",
            "action": arguments.get("action"),
            "target": arguments.get("id"),
            "generation": arguments.get("expectedGeneration"),
            "confirm": arguments.get("confirm") is True,
            "impactOverride": arguments.get("impactOverride") is True,
            "reason": arguments.get("reason", ""),
        }
    elif name == "bridge_diagnostics":
        request = {"op": "diagnostics", "limit": arguments.get("limit", 20)}
    else:
        raise JsonRpcError(-32602, f"unknown control tool: {name}")
    value = local_control_query(local_host, local_port, request)
    return {
        "content": [{"type": "text", "text": json.dumps(value, ensure_ascii=False, separators=(",", ":"))}],
        "structuredContent": value,
        **({"isError": True} if not value.get("ok", True) else {}),
    }


# ---------------------------------------------------------------------------
# P10: modern (2026-07-28) direct frontend for the Bridge-owned registry and
# control MCP endpoints, enabled per process with ``--protocol-era modern``.
#
# The frontend is local and fixed: it never talks to a remote business backend.
# ``server/discover`` advertises exactly the tools this endpoint actually
# offers with the endpoint's real instructions; ``tools/list`` returns those
# fixed tool definitions; ``tools/call`` strips the reserved modern ``_meta``
# envelope (keeping only a client progressToken) and then runs the *same*
# handler and gate logic as the legacy dispatchers (tool scope, read-only
# registry surface, bridge_control confirm/expectedGeneration mapping), so a
# confirmed control action still requires an explicit confirm=true argument
# and a preview stays a preview.  Results are shaped through bridge_protocol
# so the era envelope (resultType / _meta.serverInfo and, for the list family,
# CacheableResult ttlMs/cacheScope) is exact, and modern metadata/identity
# claims never reach handler outputs.  Malformed metadata, an unsupported
# requested revision, MRTR retry fields, or any method outside the tools-only
# surface (resources/prompts/logging, tasks, sampling/elicitation/roots,
# subscriptions) are answered with an error and never touch the handlers: no
# side effect on malformed or unsupported requests.  No optional modern family
# is advertised or executed.
# ---------------------------------------------------------------------------


def _registry_modern_server_info() -> dict[str, Any]:
    """Modern result identity for the registry endpoint (tools-only surface)."""
    return {
        "name": "win-wsl-mcp-registry",
        "version": SERVER_VERSION,
        "description": (
            "Bridge-owned read-only registry MCP; the modern surface is "
            "tools-only."
        ),
    }


def _control_modern_server_info() -> dict[str, Any]:
    """Modern result identity for the control endpoint (tools-only surface)."""
    return {
        "name": "win-wsl-mcp-control",
        "version": SERVER_VERSION,
        "description": "Bridge-owned control MCP; the modern surface is tools-only.",
    }


def _modern_frontend_fault(
    message: dict[str, Any], method: Any
) -> dict[str, Any] | None:
    """Validate one modern-era request; return the error body, or None if OK.

    A message without the namespaced per-request metadata is not a modern
    request at all: on a modern-era endpoint it is answered with -32601 and a
    hint (this server does not offer the legacy initialize handshake).  A
    message whose metadata is present but malformed is InvalidParams; a
    well-formed but unsupported requested revision is the spec-defined -32022.
    """
    if bridge_protocol.meta_of(message) is None:
        return bridge_protocol.method_not_found_error(
            method,
            "this endpoint runs in the modern 2026-07-28 era and does not offer "
            "the legacy initialize handshake; every request must carry "
            "io.modelcontextprotocol/protocolVersion in params._meta",
        )
    problems = bridge_protocol.validate_modern_request_meta(message)
    if problems:
        return bridge_protocol.invalid_params_error("; ".join(problems))
    _present, version = bridge_protocol.protocol_version_entry(message)
    if not isinstance(version, str) or not bridge_protocol.is_supported_modern_version(
        version
    ):
        return bridge_protocol.unsupported_version_error(version)
    return None


def _modern_frontend_response(
    message: dict[str, Any],
    *,
    server_info: dict[str, Any],
    instructions: str,
    legacy_dispatch: Any,
) -> dict[str, Any]:
    """Answer one modern-era request locally and return the full response.

    ``legacy_dispatch`` receives the request with its reserved ``_meta`` block
    removed and performs the endpoint's real handler work; this wrapper only
    adds the modern envelope and error mapping around it.
    """
    request_id = message.get("id")
    base: dict[str, Any] = {"jsonrpc": "2.0", "id": request_id}
    method = message.get("method")
    fault = _modern_frontend_fault(message, method)
    if fault is not None:
        base["error"] = fault
        return base
    if method == "server/discover":
        base["result"] = bridge_protocol.discover_result(
            server_info,
            backend_tools=True,
            backend_instructions=instructions,
        )
        return base
    if method == "ping":
        base["result"] = {"resultType": bridge_protocol.RESULT_TYPE_COMPLETE}
        return base
    if method not in bridge_protocol.FORWARDED_TOOLS_ONLY_METHODS:
        base["error"] = bridge_protocol.method_not_found_error(
            method,
            "method is not part of the advertised tools-only modern surface of "
            "this Bridge-owned endpoint",
        )
        return base
    if bridge_protocol.has_mrtr_retry_fields(message):
        base["error"] = bridge_protocol.invalid_params_error(
            "unexpected inputResponses/requestState retry fields on a request "
            "this endpoint never returned as input_required"
        )
        return base
    sanitized = bridge_protocol.sanitize_params_for_legacy(message.get("params"))
    if sanitized is None:
        base["error"] = bridge_protocol.invalid_params_error(
            "params must be an object"
        )
        return base
    inner = dict(message)
    inner["params"] = sanitized
    try:
        value = legacy_dispatch(inner)
    except JsonRpcError as exc:
        base["error"] = {"code": exc.code, "message": str(exc)}
        return base
    if method == "tools/list":
        base["result"] = bridge_protocol.shape_modern_result(
            method, value, server_info
        )
    else:
        base["result"] = bridge_protocol.shape_modern_tool_result(
            value, server_info
        )
    return base


def _registry_mcp_modern_response(
    message: dict[str, Any],
    tools: list[dict[str, Any]],
    actions: dict[str, str],
    local_host: str,
    local_port: int,
) -> dict[str, Any]:
    """Modern-era response for one registry endpoint request (read-only)."""
    return _modern_frontend_response(
        message,
        server_info=_registry_modern_server_info(),
        instructions=REGISTRY_INSTRUCTIONS,
        legacy_dispatch=lambda inner: _registry_mcp_dispatch(
            inner, tools, actions, local_host, local_port
        ),
    )


def _control_mcp_modern_response(
    message: dict[str, Any], local_host: str, local_port: int
) -> dict[str, Any]:
    """Modern-era response for one control endpoint request."""
    return _modern_frontend_response(
        message,
        server_info=_control_modern_server_info(),
        instructions=CONTROL_INSTRUCTIONS,
        legacy_dispatch=lambda inner: _control_mcp_dispatch(
            inner, local_host, local_port
        ),
    )


def local_control_query(
    local_host: str, local_port: int, request: dict[str, Any]
) -> dict[str, Any]:
    if not _is_loopback(local_host):
        raise BridgeError("control host must resolve only to loopback")
    with socket.create_connection((local_host, local_port), timeout=10) as sock:
        sock.sendall(_json_bytes(request) + b"\n")
        reply = json.loads(_recv_line(sock))
    if not reply.get("ok"):
        return {"ok": False, "code": "control_failed", "summary": str(reply.get("message", "control failed"))[:400]}
    value = reply.get("result")
    return value if isinstance(value, dict) else {"ok": True, "result": value}


class _CompatibilityDownstreamError(Exception):
    def __init__(self, error: dict[str, Any]):
        super().__init__(str(error.get("message", "downstream MCP request failed")))
        self.error = error


class _CompatibilityTransportError(Exception):
    """Downstream transport loss while talking to the business backend.

    ``outcome_unknown`` is True only when the affected request was a business
    tools/call whose bytes were written to the downstream before the loss; a
    client that retries such a call must treat the outcome as unknown.
    ``summary`` is a short bounded human reason.
    """

    def __init__(self, outcome_unknown: bool, summary: str):
        super().__init__(summary)
        self.outcome_unknown = bool(outcome_unknown)
        self.summary = str(summary)[:400]


class _CompatibilitySession:
    """Constant-surface MCP facade for clients without dynamic tool refresh.

    The facade keeps a tools-only last-known-good catalog plus the last
    successfully observed downstream initialize result inside one facade
    process.  Transient transport loss drops only the live downstream socket
    and never the last-known-good state, so the constant two-tool surface can
    keep answering (from cache, with explicit revision evidence) while the peer
    or business backend is unavailable.  No resource/prompt surface is added:
    this stays a tool-only constant facade by design.
    """

    def __init__(self, local_host: str, local_port: int, target: str):
        self.local_host = local_host
        self.local_port = local_port
        self.target = target
        self.sock: socket.socket | None = None
        self.next_id = 1
        #: Last-known-good downstream initialize result; replaced only by a
        #: successful fresh downstream initialize.  It survives transport loss
        #: so an outage never erases observed instructions.
        self.initialize_result: dict[str, Any] | None = None
        #: Last-known-good full tool catalog; replaced only by a complete
        #: successful re-observation (never a partial page mix).
        self.tools: list[dict[str, Any]] | None = None
        #: Number of complete catalog observations since this facade started.
        self.catalog_revision = 0
        #: Downstream session generation of the current catalog observation.
        self.catalog_gen = 0
        #: Downstream session generation (0 = never connected successfully).
        self.gen = 0
        #: Downstream signaled notifications/tools/list_changed after the last
        #: complete observation; pages keep serving the last-known-good
        #: catalog but report ``verified: False`` until a refresh succeeds.
        self.catalog_dirty = False

    def _drop_downstream(self) -> None:
        """Close only the live downstream socket; keep last-known-good state."""
        if self.sock is not None:
            try:
                self.sock.close()
            except OSError:
                pass
            self.sock = None

    def close(self) -> None:
        """Full teardown for process exit: drop the socket and reset state."""
        self._drop_downstream()
        self.tools = None
        self.initialize_result = None
        self.catalog_revision = 0
        self.catalog_gen = 0
        self.catalog_dirty = False
        self.gen = 0

    def _connect(self) -> None:
        if self.sock is not None:
            return
        if not ID_PATTERN.fullmatch(self.target) or not _is_loopback(self.local_host):
            raise BridgeError("compatibility MCP target or host is invalid")
        try:
            sock = socket.create_connection((self.local_host, self.local_port), timeout=10)
            sock.sendall(_json_bytes({"op": "connect", "target": self.target}) + b"\n")
            reply = json.loads(_recv_line(sock))
        except Exception as exc:
            raise _CompatibilityTransportError(
                False, f"bridge connect failed: {exc}"
            ) from exc
        if not reply.get("ok"):
            try:
                sock.close()
            except OSError:
                pass
            raise _CompatibilityTransportError(
                False, str(reply.get("message", "bridge open failed"))
            )
        sock.settimeout(300)
        self.sock = sock
        try:
            result = self.request(
                "initialize",
                {
                    "protocolVersion": SHARED_MCP_PROTOCOL_VERSION,
                    "capabilities": {},
                    "clientInfo": {
                        "name": "win-wsl-mcp-bridge-compatibility",
                        "version": SERVER_VERSION,
                    },
                },
            )
        except _CompatibilityTransportError as exc:
            # The handshake did not complete, so any outer business call was
            # never sent.  Drop the half-open socket and keep the LKG state.
            self._drop_downstream()
            raise _CompatibilityTransportError(
                False, f"downstream initialize did not complete: {exc.summary}"
            ) from exc
        except _CompatibilityDownstreamError:
            # The downstream answered with a JSON-RPC error and no usable
            # session exists; drop so the next attempt starts fresh.
            self._drop_downstream()
            raise
        if not isinstance(result, dict):
            self._drop_downstream()
            raise BridgeError("downstream initialize returned no result")
        try:
            sock.sendall(
                _json_bytes(
                    {"jsonrpc": "2.0", "method": "notifications/initialized", "params": {}}
                )
                + b"\n"
            )
        except Exception as exc:
            self._drop_downstream()
            raise _CompatibilityTransportError(
                False, f"initialized notification failed: {exc}"
            ) from exc
        self.gen += 1
        self.initialize_result = result

    def request(self, method: str, params: dict[str, Any]) -> Any:
        if self.sock is None:
            # A failed reconnect raises outcome_unknown=False: the outer
            # request was never written to any downstream session.
            self._connect()
        assert self.sock is not None
        request_id = f"compat-{self.next_id}"
        self.next_id += 1
        try:
            self.sock.sendall(
                _json_bytes(
                    {"jsonrpc": "2.0", "id": request_id, "method": method, "params": params}
                )
                + b"\n"
            )
        except Exception as exc:
            self._drop_downstream()
            raise _CompatibilityTransportError(
                method == "tools/call", f"downstream send failed: {exc}"
            ) from exc
        try:
            while True:
                message = json.loads(_recv_line(self.sock, MAX_SHARED_JSONRPC_BYTES))
                if message.get("method") == "notifications/tools/list_changed":
                    # Keep the last-known-good catalog; flag it stale until a
                    # full refresh re-observes the downstream catalog.
                    self.catalog_dirty = True
                    continue
                if message.get("id") != request_id:
                    continue
                if "error" in message:
                    error = message.get("error") or {}
                    raise _CompatibilityDownstreamError(
                        error if isinstance(error, dict) else {"message": str(error)}
                    )
                return message.get("result")
        except _CompatibilityDownstreamError:
            # A valid JSON-RPC error is an application/protocol result, not a
            # transport loss.  Preserve downstream session state and cache.
            raise
        except Exception as exc:
            self._drop_downstream()
            raise _CompatibilityTransportError(
                method == "tools/call", f"downstream request failed: {exc}"
            ) from exc

    def list_tools(self, *, refresh: bool = False) -> list[dict[str, Any]]:
        if self.tools is not None and not refresh:
            return self.tools
        tools: list[dict[str, Any]] = []
        cursor: str | None = None
        while True:
            params = {"cursor": cursor} if cursor is not None else {}
            result = self.request("tools/list", params)
            if not isinstance(result, dict) or not isinstance(result.get("tools"), list):
                raise BridgeError("downstream tools/list returned an invalid result")
            tools.extend(item for item in result["tools"] if isinstance(item, dict))
            cursor = result.get("nextCursor")
            if not isinstance(cursor, str) or not cursor:
                break
            if len(tools) > 1000:
                raise BridgeError("downstream tool catalog exceeds compatibility limit")
        self.tools = tools
        self.catalog_revision += 1
        self.catalog_gen = self.gen
        self.catalog_dirty = False
        return tools

    def _cache_evidence(self) -> dict[str, Any]:
        """Compact cache evidence attached to every bridge_capabilities result.

        ``observed`` is True once a complete catalog was successfully read in
        this facade process; ``revision`` counts those complete observations;
        ``verified`` is True only when the catalog was observed on the
        currently live downstream session with no intervening list_changed;
        ``reason`` is a short bounded tag.
        """
        observed = self.tools is not None
        live = self.sock is not None
        if not observed:
            return {"observed": False, "revision": 0, "verified": False, "reason": "unobserved"}
        if self.catalog_dirty:
            reason = "list_changed"
        elif not live:
            reason = "downstream_unavailable"
        elif self.catalog_gen != self.gen:
            reason = "reconnected"
        else:
            reason = "live"
        return {
            "observed": True,
            "revision": self.catalog_revision,
            "verified": reason == "live",
            "reason": reason,
        }


def _compatibility_tools() -> list[dict[str, Any]]:
    return [
        {
            "name": "bridge_capabilities",
            "description": "Search or describe this MCP's current capabilities. Cache results; use refresh only after a capability change.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "query": {"type": "string"},
                    "tool": {"type": "string"},
                    "cursor": {"type": "integer", "minimum": 0},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 50},
                    "refresh": {"type": "boolean", "default": False},
                },
                "additionalProperties": False,
            },
        },
        {
            "name": "bridge_call",
            "description": "Call one capability discovered with bridge_capabilities on this MCP target.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "tool": {"type": "string"},
                    "arguments": {"type": "object"},
                },
                "required": ["tool"],
                "additionalProperties": False,
            },
        },
    ]


def _compatibility_error(
    code: str, summary: str, *, retryable: bool, outcome_unknown: bool = False
) -> dict[str, Any]:
    payload = {
        "code": code,
        "retryable": retryable,
        "outcomeUnknown": outcome_unknown,
        "summary": summary[:400],
    }
    return {
        "content": [{"type": "text", "text": payload["summary"]}],
        "structuredContent": payload,
        "isError": True,
    }


def compatibility_mcp(local_host: str, local_port: int, target: str) -> int:
    session = _CompatibilitySession(local_host, local_port, target)
    try:
        while True:
            raw = sys.stdin.buffer.readline(MAX_FRAME_BYTES + 1)
            if not raw:
                return 0
            request_id: Any = None
            method: Any = None
            try:
                if len(raw) > MAX_FRAME_BYTES:
                    raise JsonRpcError(-32700, "MCP message exceeds the limit")
                message = json.loads(raw)
                if (
                    not isinstance(message, dict)
                    or message.get("jsonrpc") != "2.0"
                    or not isinstance(message.get("method"), str)
                ):
                    raise JsonRpcError(-32600, "invalid request")
                request_id = message.get("id")
                method = message.get("method")
                params = message.get("params") or {}
                if not isinstance(params, dict):
                    raise JsonRpcError(-32602, "params must be an object")
                if "id" not in message:
                    continue
                if method == "initialize":
                    try:
                        session._connect()
                    except Exception:
                        # Keep the constant surface registered while the peer or
                        # business backend is temporarily unavailable.  A failed
                        # attempt never discards the last-known-good downstream
                        # initialize/catalog; a later discovery/call lazily
                        # retries the downstream session.
                        pass
                    downstream = session.initialize_result or {}
                    result = {
                        "protocolVersion": SHARED_MCP_PROTOCOL_VERSION,
                        "capabilities": {"tools": {"listChanged": False}},
                        "serverInfo": {
                            "name": f"{target}-bridge-compatibility",
                            "version": SERVER_VERSION,
                        },
                        "instructions": downstream.get("instructions", "")
                        + "\nCompatibility mode exposes a stable two-tool surface. Discover once with bridge_capabilities, then invoke with bridge_call.",
                    }
                elif method == "ping":
                    result = {}
                elif method == "tools/list":
                    result = {"tools": _compatibility_tools()}
                elif method == "tools/call":
                    name = params.get("name")
                    arguments = params.get("arguments") or {}
                    if not isinstance(arguments, dict):
                        raise JsonRpcError(-32602, "tool arguments must be an object")
                    if name == "bridge_capabilities":
                        tools = session.list_tools(refresh=arguments.get("refresh") is True)
                        exact = arguments.get("tool")
                        query = str(arguments.get("query", "")).casefold()
                        if isinstance(exact, str):
                            matched = [item for item in tools if item.get("name") == exact]
                            payload: dict[str, Any] = {"tools": matched}
                        else:
                            summaries = [
                                {
                                    "name": item.get("name"),
                                    "description": (
                                        str(item.get("description", ""))[:300]
                                        + ("…" if len(str(item.get("description", ""))) > 300 else "")
                                    ),
                                }
                                for item in tools
                                if not query or query in (str(item.get("name", "")) + " " + str(item.get("description", ""))).casefold()
                            ]
                            cursor = arguments.get("cursor", 0)
                            limit = arguments.get("limit", 20)
                            if (
                                not isinstance(cursor, int)
                                or isinstance(cursor, bool)
                                or cursor < 0
                                or not isinstance(limit, int)
                                or isinstance(limit, bool)
                                or not 1 <= limit <= 50
                            ):
                                raise JsonRpcError(-32602, "cursor/limit are outside the compatibility bounds")
                            page = summaries[cursor : cursor + limit]
                            payload = {"tools": page, "total": len(summaries)}
                            if cursor + limit < len(summaries):
                                payload["nextCursor"] = cursor + limit
                        payload["cache"] = session._cache_evidence()
                        result = {
                            "content": [{"type": "text", "text": json.dumps(payload, ensure_ascii=False, separators=(",", ":"))}],
                            "structuredContent": payload,
                        }
                    elif name == "bridge_call":
                        tool = arguments.get("tool")
                        call_arguments = arguments.get("arguments", {})
                        if not isinstance(tool, str) or not isinstance(call_arguments, dict):
                            raise JsonRpcError(-32602, "bridge_call requires tool and object arguments")
                        if not any(item.get("name") == tool for item in session.list_tools()):
                            result = _compatibility_error(
                                "unknown_tool",
                                f"unknown downstream tool: {tool}",
                                retryable=False,
                            )
                        else:
                            result = session.request(
                                "tools/call",
                                {"name": tool, "arguments": call_arguments},
                            )
                    else:
                        raise JsonRpcError(-32602, f"unknown compatibility tool: {name}")
                else:
                    raise JsonRpcError(-32601, f"method not found: {method}")
                response = {"jsonrpc": "2.0", "id": request_id, "result": result}
            except JsonRpcError as exc:
                response = {"jsonrpc": "2.0", "id": request_id, "error": {"code": exc.code, "message": str(exc)}}
            except _CompatibilityDownstreamError as exc:
                downstream = dict(exc.error)
                payload = {
                    "code": "downstream_error",
                    "retryable": False,
                    "outcomeUnknown": False,
                    "summary": str(downstream.get("message", "downstream MCP request failed"))[:400],
                    "downstream": downstream,
                }
                result = {
                    "content": [{"type": "text", "text": payload["summary"]}],
                    "structuredContent": payload,
                    "isError": True,
                }
                response = {"jsonrpc": "2.0", "id": request_id, "result": result}
            except _CompatibilityTransportError as exc:
                if method == "tools/call":
                    result = _compatibility_error(
                        "backend_unavailable",
                        exc.summary,
                        retryable=True,
                        outcome_unknown=exc.outcome_unknown,
                    )
                    response = {"jsonrpc": "2.0", "id": request_id, "result": result}
                else:
                    response = {"jsonrpc": "2.0", "id": request_id, "error": {"code": -32603, "message": exc.summary[:400]}}
            except Exception as exc:
                if method == "tools/call":
                    # Non-transport failures (invalid catalog shape, refused or
                    # unknown target) mean no downstream business call was sent.
                    result = _compatibility_error(
                        "facade_error",
                        str(exc),
                        retryable=False,
                        outcome_unknown=False,
                    )
                    response = {"jsonrpc": "2.0", "id": request_id, "result": result}
                else:
                    response = {"jsonrpc": "2.0", "id": request_id, "error": {"code": -32603, "message": str(exc)[:400]}}
            _write_registry_mcp_response(response)
    finally:
        session.close()


def control_mcp(
    local_host: str, local_port: int, *, protocol_era: str = "legacy"
) -> int:
    while True:
        raw = sys.stdin.buffer.readline(MAX_FRAME_BYTES + 1)
        if not raw:
            return 0
        request_id: Any = None
        try:
            if len(raw) > MAX_FRAME_BYTES:
                # Drain the remainder of the overlong line so the next read
                # starts at a frame boundary; a remainder must never be parsed
                # or executed as a request of its own.
                while raw and not raw.endswith(b"\n"):
                    raw = sys.stdin.buffer.readline(MAX_FRAME_BYTES + 1)
                raise JsonRpcError(-32700, "MCP message exceeds the limit")
            message = json.loads(raw)
            if not isinstance(message, dict) or message.get("jsonrpc") != "2.0" or not isinstance(message.get("method"), str):
                raise JsonRpcError(-32600, "invalid request")
            if "id" not in message:
                continue
            request_id = message.get("id")
            if protocol_era == "modern":
                response = _control_mcp_modern_response(
                    message, local_host, local_port
                )
            else:
                result = _control_mcp_dispatch(message, local_host, local_port)
                response = {"jsonrpc": "2.0", "id": request_id, "result": result}
        except JsonRpcError as exc:
            response = {"jsonrpc": "2.0", "id": request_id, "error": {"code": exc.code, "message": str(exc)}}
        except Exception:
            response = {"jsonrpc": "2.0", "id": request_id, "error": {"code": -32603, "message": "internal control error"}}
        _write_registry_mcp_response(response)


def _write_registry_mcp_response(response: dict[str, Any]) -> None:
    encoded = json.dumps(response, ensure_ascii=False, separators=(",", ":"))
    if len(encoded.encode("utf-8")) + 1 > MAX_FRAME_BYTES:
        encoded = json.dumps(
            {
                "jsonrpc": "2.0",
                "id": response.get("id"),
                "error": {
                    "code": -32603,
                    "message": "registry MCP response exceeds the configured limit",
                },
            },
            separators=(",", ":"),
        )
    sys.stdout.write(encoded + "\n")
    sys.stdout.flush()


def registry_mcp(
    local_host: str, local_port: int, *, protocol_era: str = "legacy"
) -> int:
    tools = _registry_tools()
    actions = {
        "bridge_registry_list": "list",
        "bridge_registry_search": "search",
        "bridge_registry_describe": "describe",
        "bridge_registry_status": "status",
    }
    while True:
        raw = sys.stdin.buffer.readline(MAX_FRAME_BYTES + 1)
        if not raw:
            return 0
        if len(raw) > MAX_FRAME_BYTES:
            while raw and not raw.endswith(b"\n"):
                raw = sys.stdin.buffer.readline(MAX_FRAME_BYTES + 1)
            _write_registry_mcp_response(
                {
                    "jsonrpc": "2.0",
                    "id": None,
                    "error": {"code": -32700, "message": "MCP message exceeds the limit"},
                }
            )
            continue
        raw = raw.strip()
        if not raw:
            continue
        try:
            parsed = json.loads(raw)
        except (json.JSONDecodeError, UnicodeDecodeError):
            _write_registry_mcp_response(
                {
                    "jsonrpc": "2.0",
                    "id": None,
                    "error": {"code": -32700, "message": "parse error"},
                }
            )
            continue
        if (
            not isinstance(parsed, dict)
            or parsed.get("jsonrpc") != "2.0"
            or not isinstance(parsed.get("method"), str)
        ):
            _write_registry_mcp_response(
                {
                    "jsonrpc": "2.0",
                    "id": parsed.get("id") if isinstance(parsed, dict) else None,
                    "error": {"code": -32600, "message": "invalid request"},
                }
            )
            continue
        if "id" not in parsed:
            continue
        request_id = parsed.get("id")
        if isinstance(request_id, bool) or not isinstance(request_id, (str, int)):
            _write_registry_mcp_response(
                {
                    "jsonrpc": "2.0",
                    "id": None,
                    "error": {"code": -32600, "message": "invalid request id"},
                }
            )
            continue
        try:
            if protocol_era == "modern":
                response = _registry_mcp_modern_response(
                    parsed,
                    tools,
                    actions,
                    local_host,
                    local_port,
                )
            else:
                result = _registry_mcp_dispatch(
                    parsed,
                    tools,
                    actions,
                    local_host,
                    local_port,
                )
                response = {"jsonrpc": "2.0", "id": request_id, "result": result}
        except JsonRpcError as exc:
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": exc.code, "message": str(exc)},
            }
        except Exception as exc:
            print(
                f"registry MCP internal error: {type(exc).__name__}: {exc}",
                file=sys.stderr,
                flush=True,
            )
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -32603, "message": "internal registry error"},
            }
        _write_registry_mcp_response(response)


def _event_journal_path(registry_path: Path) -> Path:
    """Keep each registry's journal distinct even in a shared test directory."""
    return registry_path.with_name(registry_path.stem + ".events.sqlite3")


def default_registry_path(side: str) -> Path:
    if side == "win":
        base = Path(os.environ.get("LOCALAPPDATA", Path.home() / "AppData" / "Local"))
        return base / "WinWslMcpBridge" / "registry.sqlite3"
    base = Path(os.environ.get("XDG_STATE_HOME", Path.home() / ".local" / "state"))
    return base / "win-wsl-mcp-bridge" / "registry.sqlite3"


def deployment_diagnostics(
    *,
    side: str,
    registry_path: Path,
    local_host: str,
    local_port: int,
    link_host: str,
    link_port: int,
    artifact_roots: list[Path],
) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []

    def record(name: str, ok: bool, detail: str) -> None:
        checks.append({"name": name, "ok": ok, "detail": detail})

    record(
        "python",
        sys.version_info >= (3, 11),
        f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}",
    )
    record("localHost", _is_loopback(local_host), local_host)
    record("linkHost", _is_loopback(link_host), link_host)
    record(
        "localPort",
        isinstance(local_port, int) and 1 <= local_port <= 65535,
        str(local_port),
    )
    record(
        "linkPort",
        isinstance(link_port, int) and 1 <= link_port <= 65535,
        str(link_port),
    )
    try:
        Registry(registry_path)
    except Exception as exc:
        record("registry", False, str(exc))
    else:
        record("registry", True, str(registry_path))
    for index, root in enumerate(artifact_roots):
        expanded = root.expanduser()
        ok = expanded.is_absolute() and expanded.is_dir()
        record(f"artifactRoot[{index}]", ok, str(expanded))
    return {
        "ok": all(check["ok"] for check in checks),
        "version": SERVER_VERSION,
        "bridgeProtocol": BRIDGE_PROTOCOL,
        "side": side,
        "checks": checks,
    }


def _run_warehouse_command(args: argparse.Namespace) -> int:
    """Read-only local capability-warehouse helpers (P5 local slice).

    Only ever reads the Operator-supplied static index and, for dedupe/plan,
    the local registry. No network, no writes, no installation, no launch
    configuration can be produced from catalog content.
    """
    index_path = Path(args.index).expanduser().resolve()
    try:
        index = capability_index_load(index_path)
    except BridgeError as exc:
        print(f"warehouse {args.warehouse_command}: {exc}", file=sys.stderr)
        return 2
    registry = None
    if getattr(args, "registry", None):
        registry_path = Path(args.registry).expanduser().resolve()
        if not registry_path.exists():
            print(
                f"warehouse {args.warehouse_command}: registry database does not exist: {registry_path}",
                file=sys.stderr,
            )
            return 2
        try:
            registry = Registry(registry_path)
            registry.identities()  # cheap read-only validation of the sqlite file
        except (BridgeError, sqlite3.Error, OSError) as exc:
            print(
                f"warehouse {args.warehouse_command}: registry is unusable: {exc}",
                file=sys.stderr,
            )
            return 2
    try:
        if args.warehouse_command in ("search", "list"):
            results = capability_index_search(index, getattr(args, "query", ""), args.limit)
            for entry in results:
                identity = entry["identity"]
                print(
                    f"{entry['key']}  [{', '.join(entry['capabilityGroups'])}]  {entry['summary']}"
                )
            print(f"warehouse: {len(results)} result(s)", file=sys.stderr)
            return 0
        if args.warehouse_command == "dedupe":
            local = registry.identities() if registry is not None else []
            remaining = capability_index_dedupe(index, local)
            for entry in remaining:
                identity = entry["identity"]
                print(
                    f"{entry['key']}  [{', '.join(entry['capabilityGroups'])}]  {entry['summary']}"
                )
            print(
                f"warehouse: {len(remaining)} of {len(index['items'])} not locally registered",
                file=sys.stderr,
            )
            return 0
        plan = capability_install_plan(index, args.key, registry)
    except BridgeError as exc:
        print(f"warehouse {args.warehouse_command}: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(plan, ensure_ascii=False, indent=2))
    return 0


def build_parser(default_side: str, default_local_port: int, default_link_mode: str) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Bidirectional WIN-WSL MCP bridge component")
    parser.add_argument("--version", action="version", version=f"%(prog)s {SERVER_VERSION}")
    subparsers = parser.add_subparsers(dest="command", required=True)

    serve = subparsers.add_parser("serve", help="run the local node and peer link")
    serve.add_argument("--registry")
    serve.add_argument("--local-host", default="127.0.0.1")
    serve.add_argument("--local-port", type=int, default=default_local_port)
    serve.add_argument("--link-mode", choices=["listen", "connect"], default=default_link_mode)
    serve.add_argument("--link-host", default="127.0.0.1")
    serve.add_argument("--link-port", type=int, default=8767)
    serve.add_argument("--side", choices=["win", "wsl"], default=default_side)
    serve.add_argument("--allow-artifact-root", action="append", default=[])
    serve.add_argument("--artifact-spool-root")
    serve.add_argument("--max-artifact-bytes", type=int, default=DEFAULT_MAX_ARTIFACT_BYTES)
    serve.add_argument(
        "--artifact-resume-retention-seconds",
        type=float,
        default=ARTIFACT_RESUME_RETENTION_SECONDS,
        help="how long parked artifacts/2 resume state is kept for a link to return",
    )
    serve.add_argument(
        "--http-relay-port",
        type=int,
        default=0,
        help=(
            "opt-in native loopback Streamable HTTP relay port; each registered "
            "HTTP MCP is served at /mcp/<registered-id> (0 disables, the default)"
        ),
    )

    connect = subparsers.add_parser("connect", help="expose one remote registered MCP over local stdio")
    connect.add_argument("target")
    connect.add_argument("--local-host", default="127.0.0.1")
    connect.add_argument("--local-port", type=int, default=default_local_port)
    connect.add_argument(
        "--artifact-inbox",
        default=os.environ.get("WIN_WSL_MCP_BRIDGE_ARTIFACT_INBOX"),
    )
    connect.add_argument(
        "--print-stream-id",
        action="store_true",
        help="print the opened logical stream id to stderr so a staged-input adapter can target it",
    )
    connect_http = subparsers.add_parser(
        "connect-http",
        help="explicit HTTP-to-stdio compatibility facade for one typed peer HTTP MCP",
    )
    connect_http.add_argument("target")
    connect_http.add_argument("--local-host", default="127.0.0.1")
    connect_http.add_argument("--local-port", type=int, default=default_local_port)

    publish = subparsers.add_parser(
        "publish",
        help="publish one staged business-MCP artifact to the peer workspace",
    )
    publish.add_argument("relative_path")
    publish.add_argument("--name")
    publish.add_argument("--media-type")
    publish.add_argument(
        "--local-host",
        default=os.environ.get("WIN_WSL_MCP_BRIDGE_ARTIFACT_LOCAL_HOST", "127.0.0.1"),
    )
    publish.add_argument(
        "--local-port",
        type=int,
        default=int(
            os.environ.get(
                "WIN_WSL_MCP_BRIDGE_ARTIFACT_LOCAL_PORT",
                str(default_local_port),
            )
        ),
    )
    publish.add_argument(
        "--token",
        default=os.environ.get("WIN_WSL_MCP_BRIDGE_ARTIFACT_TOKEN"),
    )

    stage_input_cmd = subparsers.add_parser(
        "stage-input",
        help="stage one Operator-authorized Agent-local file into the peer MCP of an open stream",
    )
    stage_input_cmd.add_argument("source_path")
    stage_input_cmd.add_argument("--stream", required=True,
        help="logical stream id returned by the node for the open connect session")
    stage_input_cmd.add_argument("--name")
    stage_input_cmd.add_argument("--media-type")
    stage_input_cmd.add_argument("--local-host", default="127.0.0.1")
    stage_input_cmd.add_argument("--local-port", type=int, default=default_local_port)

    registry = subparsers.add_parser("registry-mcp", help="serve the read-only bridge registry over stdio MCP")
    registry.add_argument("--local-host", default="127.0.0.1")
    registry.add_argument("--local-port", type=int, default=default_local_port)
    registry.add_argument(
        "--protocol-era",
        choices=("legacy", "modern"),
        default="legacy",
        help="serve the legacy stdio MCP envelope (default) or the modern "
        "2026-07-28 per-request metadata tools-only envelope",
    )
    control = subparsers.add_parser("control-mcp", help="serve the optional two-tool Bridge Control MCP over stdio")
    control.add_argument("--local-host", default="127.0.0.1")
    control.add_argument("--local-port", type=int, default=default_local_port)
    control.add_argument(
        "--protocol-era",
        choices=("legacy", "modern"),
        default="legacy",
        help="serve the legacy stdio MCP envelope (default) or the modern "
        "2026-07-28 per-request metadata tools-only envelope",
    )
    compatibility = subparsers.add_parser(
        "compatibility-mcp",
        help="serve one registered target through a constant two-tool MCP facade",
    )
    compatibility.add_argument("target")
    compatibility.add_argument("--local-host", default="127.0.0.1")
    compatibility.add_argument("--local-port", type=int, default=default_local_port)
    deferred = subparsers.add_parser(
        "deferred-mcp", help="serve one registered stdio target with an opt-in legacy library view",
    )
    deferred.add_argument("target")
    deferred.add_argument("--local-host", default="127.0.0.1")
    deferred.add_argument("--local-port", type=int, default=default_local_port)

    trace = subparsers.add_parser("trace", help="manage bounded host-local development trace sessions")
    trace.add_argument("action", choices=["start", "recent", "records", "export", "compact"])
    trace.add_argument("--confirm", action="store_true", help="confirm explicit journal compaction")
    trace.add_argument("--maintenance-timeout", type=int, default=10,
                       help="compaction SQLite execution deadline in seconds (1-60)")
    trace.add_argument("--side", choices=["win", "wsl"], default=default_side)
    trace.add_argument("--journal")
    trace.add_argument("--output", help="export action: local metadata-only ZIP destination")
    trace.add_argument("--target", default="bridge")
    trace.add_argument("--level", choices=["envelope", "snippet", "complete"], default="envelope")
    trace.add_argument("--seconds", type=int, default=300)
    trace.add_argument("--byte-budget", type=int, default=1024 * 1024)
    trace.add_argument("--direction", choices=["inbound", "outbound"])
    trace.add_argument(
        "--method",
        action="append",
        default=[],
        help="record only messages of this JSON-RPC method (repeatable; reserved "
        "tokens request/notification/response/error/other match by class)",
    )
    trace.add_argument("--trace-id")
    trace.add_argument("--limit", type=int, default=20)
    trace.add_argument(
        "--include-payload",
        action="store_true",
        help="records action: return base64 payloads (sensitive; requires --confirm-sensitive)",
    )
    trace.add_argument("--confirm-sensitive", action="store_true")

    doctor = subparsers.add_parser(
        "doctor",
        help="validate local deployment configuration without starting listeners",
    )
    doctor.add_argument("--registry")
    doctor.add_argument("--side", choices=["win", "wsl"], default=default_side)
    doctor.add_argument("--local-host", default="127.0.0.1")
    doctor.add_argument("--local-port", type=int, default=default_local_port)
    doctor.add_argument("--link-host", default="127.0.0.1")
    doctor.add_argument("--link-port", type=int, default=8767)
    doctor.add_argument("--allow-artifact-root", action="append", default=[])

    registry_init = subparsers.add_parser(
        "registry-init",
        help="initialize or update the local SQLite registry from a manifest",
    )
    registry_init.add_argument("--registry")
    registry_init.add_argument("--manifest", required=True)
    registry_init.add_argument("--replace", action="store_true")
    registry_init.add_argument("--projection")

    warehouse = subparsers.add_parser(
        "warehouse",
        help="local capability-warehouse helpers over a static Operator-supplied index",
    )
    warehouse_sub = warehouse.add_subparsers(dest="warehouse_command", required=True)
    warehouse_search = warehouse_sub.add_parser(
        "search", help="bounded discovery over public summaries (read-only)"
    )
    warehouse_search.add_argument("--index", required=True)
    warehouse_search.add_argument("--limit", type=int, default=12)
    warehouse_search.add_argument("--query", default="")
    warehouse_list = warehouse_sub.add_parser(
        "list", help="list the static index's public summaries (read-only)"
    )
    warehouse_list.add_argument("--index", required=True)
    warehouse_list.add_argument("--limit", type=int, default=12)
    warehouse_dedupe = warehouse_sub.add_parser(
        "dedupe",
        help="catalog items not already directly registered in the local registry",
    )
    warehouse_dedupe.add_argument("--index", required=True)
    warehouse_dedupe.add_argument("--registry")
    warehouse_plan = warehouse_sub.add_parser(
        "plan", help="read-only install/import plan for one catalog identity"
    )
    warehouse_plan.add_argument("--index", required=True)
    warehouse_plan.add_argument("--key", required=True)
    warehouse_plan.add_argument("--registry")

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

    lifecycle = subparsers.add_parser(
        "lifecycle",
        help="inspect and control Bridge-owned MCP lifecycle state on this node "
        "(mutations are local-only and require --confirm)",
    )
    lifecycle.add_argument("--local-host", default="127.0.0.1")
    lifecycle.add_argument("--local-port", type=int, default=default_local_port)
    lifecycle_sub = lifecycle.add_subparsers(dest="lifecycle_command", required=True)

    def add_lifecycle_local_options(action_parser: argparse.ArgumentParser) -> None:
        action_parser.add_argument("--local-host", default="127.0.0.1")
        action_parser.add_argument("--local-port", type=int, default=default_local_port)

    lifecycle_status = lifecycle_sub.add_parser(
        "status",
        help="read-only lifecycle aggregation for registrations owned by this node",
    )
    add_lifecycle_local_options(lifecycle_status)
    lifecycle_status.add_argument("--id")
    for lifecycle_name, lifecycle_help in (
        (
            "drain",
            "arm refusal of new streams for one shared registration; active "
            "clients finish normally",
        ),
        (
            "refresh",
            "broadcast tools/list_changed to initialized clients without starting "
            "or restarting the backend",
        ),
        (
            "restart",
            "clear any armed drain and fully stop the live owned generation "
            "(next connect starts a fresh generation)",
        ),
        (
            "stop",
            "stop the live owned generation now; an armed drain is left untouched",
        ),
    ):
        lifecycle_action = lifecycle_sub.add_parser(lifecycle_name, help=lifecycle_help)
        add_lifecycle_local_options(lifecycle_action)
        lifecycle_action.add_argument("--id", required=True)
        lifecycle_action.add_argument("--generation", type=int)
        lifecycle_action.add_argument(
            "--confirm",
            action="store_true",
            help="apply the mutation; without it the command prints a read-only preview",
        )
    return parser


def component_main(default_side: str, default_local_port: int, default_link_mode: str) -> int:
    if os.name != "nt":
        os.umask(0o077)
    parser = build_parser(default_side, default_local_port, default_link_mode)
    args = parser.parse_args()
    if args.command == "connect":

        def announce_stream(stream_id: str) -> None:
            print(f"bridge stream: {stream_id}", file=sys.stderr)

        return proxy_stdio(
            args.local_host,
            args.local_port,
            args.target,
            artifact_inbox=args.artifact_inbox,
            on_stream_id=announce_stream if args.print_stream_id else None,
        )
    if args.command == "connect-http":
        return proxy_stdio(
            args.local_host,
            args.local_port,
            args.target,
            compatibility_http=True,
        )
    if args.command == "publish":
        if not args.token:
            print("artifact publisher: missing session token", file=sys.stderr)
            return 2
        try:
            result = publish_artifact(
                args.local_host,
                args.local_port,
                args.token,
                args.relative_path,
                args.name,
                args.media_type,
            )
        except Exception as exc:
            print(f"artifact publisher: {exc}", file=sys.stderr)
            return 1
        print(json.dumps(result, ensure_ascii=False, separators=(",", ":")))
        return 0
    if args.command == "stage-input":
        try:
            result = stage_input(
                args.local_host,
                args.local_port,
                args.stream,
                args.source_path,
                name=args.name,
                media_type=args.media_type,
            )
        except Exception as exc:
            print(f"stage-input: {exc}", file=sys.stderr)
            return 1
        print(json.dumps(result, ensure_ascii=False, separators=(",", ":")))
        return 0
    if args.command == "registry-mcp":
        return registry_mcp(
            args.local_host, args.local_port, protocol_era=args.protocol_era
        )
    if args.command == "control-mcp":
        return control_mcp(
            args.local_host, args.local_port, protocol_era=args.protocol_era
        )
    if args.command == "deferred-mcp":
        from deferred_tools import run_deferred_mcp
        return run_deferred_mcp(args.local_host, args.local_port, args.target)
    if args.command == "compatibility-mcp":
        return compatibility_mcp(args.local_host, args.local_port, args.target)
    if args.command == "trace":
        path = Path(args.journal).expanduser().resolve() if args.journal else _event_journal_path(default_registry_path(args.side))
        if args.action == "compact":
            from journal_maintenance import JournalMaintenanceError, compact_journal
            try:
                result = compact_journal(path, confirm=args.confirm,
                                         timeout_seconds=args.maintenance_timeout)
            except JournalMaintenanceError as exc:
                print(json.dumps({"applied": False, "error": str(exc)}))
                return 1
            print(json.dumps(result, ensure_ascii=False, separators=(",", ":")))
            return 0
        journal = EventJournal(path)
        if args.action == "start":
            result = journal.start_trace(
                args.target,
                args.level,
                args.seconds,
                args.byte_budget,
                confirm_sensitive=args.confirm_sensitive,
                direction=args.direction,
                methods=args.method or None,
            )
        elif args.action == "export":
            if not args.output:
                print("trace export requires --output", file=sys.stderr)
                return 2
            result = journal.export_bundle(Path(args.output), event_limit=args.limit)
        elif args.action == "records":
            if args.include_payload and not args.confirm_sensitive:
                result = {
                    "applied": False,
                    "warning": "trace record payload readback is sensitive and unverified",
                    "requiresConfirmation": True,
                }
            else:
                result = {
                    "records": journal.trace_records(
                        trace_id=args.trace_id,
                        limit=args.limit,
                        include_payload=args.include_payload,
                    ),
                    "bounded": True,
                }
        else:
            result = {"events": journal.recent(args.limit), "bounded": True}
        print(json.dumps(result, ensure_ascii=False, separators=(",", ":")))
        return 0
    if args.command == "projection":
        return _projection_cli_main(args, default_side=default_side)
    if args.command == "lifecycle":
        return _lifecycle_cli_main(args, default_local_port=default_local_port)
    if args.command == "warehouse":
        return _run_warehouse_command(args)
    registry_side = getattr(args, "side", default_side)
    registry_path = (
        Path(args.registry).expanduser().resolve()
        if args.registry
        else default_registry_path(registry_side)
    )
    if args.command == "doctor":
        report = deployment_diagnostics(
            side=args.side,
            registry_path=registry_path,
            local_host=args.local_host,
            local_port=args.local_port,
            link_host=args.link_host,
            link_port=args.link_port,
            artifact_roots=[Path(value) for value in args.allow_artifact_root],
        )
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 0 if report["ok"] else 1
    if args.command == "registry-init":
        Registry.initialize_database(
            registry_path,
            Path(args.manifest).expanduser().resolve(),
            replace=args.replace,
            projection_path=Path(args.projection).expanduser().resolve()
            if args.projection
            else None,
        )
        print(f"initialized registry: {registry_path}")
        return 0
    registry = Registry(registry_path)
    journal = EventJournal(_event_journal_path(registry_path))
    node = BridgeNode(
        side=args.side,
        registry=registry,
        local_host=args.local_host,
        local_port=args.local_port,
        link_mode=args.link_mode,
        link_host=args.link_host,
        link_port=args.link_port,
        allowed_artifact_roots=[Path(value) for value in args.allow_artifact_root],
        artifact_spool_root=Path(args.artifact_spool_root)
        if args.artifact_spool_root
        else None,
        max_artifact_bytes=args.max_artifact_bytes,
        journal=journal,
        http_relay_port=getattr(args, "http_relay_port", 0),
    )
    previous_sigterm: Any = None
    if os.name != "nt":
        def stop_on_sigterm(_signum: int, _frame: Any) -> None:
            raise KeyboardInterrupt

        previous_sigterm = signal.signal(signal.SIGTERM, stop_on_sigterm)
    try:
        asyncio.run(node.run())
    except KeyboardInterrupt:
        return 0
    finally:
        if previous_sigterm is not None:
            signal.signal(signal.SIGTERM, previous_sigterm)
    return 0


def _run_component(default_side: str, default_local_port: int, default_link_mode: str) -> int:
    try:
        return component_main(default_side, default_local_port, default_link_mode)
    except (BridgeError, OSError, sqlite3.Error, json.JSONDecodeError) as exc:
        print(f"{default_side} bridge: {exc}", file=sys.stderr)
        return 1


def win_main() -> int:
    return _run_component("win", 8768, "listen")


def wsl_main() -> int:
    return _run_component("wsl", 8769, "connect")


# =========================================================================
# P0-B: Agent-environment enrollment and per-server configuration projection
# =========================================================================
#
# Each host owns a host-local projection.sqlite3 authority. It enrolls that
# host's Codex / Claude Code / DeepSeek Harness (DSH) agent environments and
# keeps one independent MCP entry in each selected client configuration per
# registration of the *peer* registry that this host's agents reach through the
# bridge (`<bridge component> connect <server-id>`). Directly registered local
# MCPs are never duplicated through the peer bridge. Every write is
# deterministic code: an Agent config command, cwd, environment, or output path
# is never supplied by registry contents, scanned candidates, or remote
# callers. Projection-affecting registry commits append an outbox event in the
# same SQLite transaction as the registry change; reconciliation converges on
# the current desired state instead of replaying imperative operations.

BRIDGE_OWNED_ENV_KEY = "WIN_WSL_MCP_BRIDGE_OWNED"
BRIDGE_OWNED_ENV_VALUE = "1"
BRIDGE_SERVER_ENV_KEY = "WIN_WSL_MCP_BRIDGE_SERVER"

PROJECTION_SCHEMA_VERSION = 5
PROJECTION_HISTORY_LIMIT = 200
PROJECTION_SCAN_CANDIDATE_PREFIX = "cand-"
PROJECTION_IDENTITY_HASH_LIMIT = 16 * 1024 * 1024
PROJECTION_CONFIG_READ_LIMIT = 64 * 1024 * 1024
STDIO_HTTP_ENDPOINT_LIMIT = 64

ENV_STATUS_PENDING = "pending"
ENV_STATUS_CONFIGURED = "configured"
ENV_STATUS_NEXT_SESSION = "next_session"
ENV_STATUS_ERROR = "error"
ENV_STATUS_DRIFT = "drift"
ENV_STATUS_REMOVED = "removed"

CLIENT_KIND_CODEX = "codex"
CLIENT_KIND_CLAUDE = "claude"
CLIENT_KIND_DSH = "dsh"
SUPPORTED_CLIENT_KINDS = (CLIENT_KIND_CODEX, CLIENT_KIND_CLAUDE, CLIENT_KIND_DSH)

ADAPTER_OFFICIAL_CLI = "official-cli"
ADAPTER_BRIDGE_FILE = "bridge-file"
ADAPTER_UNAVAILABLE = "unavailable"

PROJECTION_SCHEMA = """
CREATE TABLE IF NOT EXISTS agent_environments (
    environment_id TEXT PRIMARY KEY,
    client_kind TEXT NOT NULL CHECK (client_kind IN ('codex', 'claude', 'dsh')),
    host_side TEXT NOT NULL CHECK (host_side IN ('win', 'wsl')),
    scope TEXT NOT NULL,
    config_path TEXT NOT NULL,
    discovery_source TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0, 1)),
    launcher_command TEXT NOT NULL,
    launcher_args_json TEXT NOT NULL,
    apply_adapter TEXT NOT NULL,
    file_sha256 TEXT NOT NULL,
    file_size INTEGER NOT NULL,
    file_mtime_ns INTEGER NOT NULL,
    may_contain_secrets INTEGER NOT NULL DEFAULT 0,
    transport_capabilities_json TEXT NOT NULL DEFAULT '{"stdio":true,"streamable-http":false}',
    compatibility_route TEXT NOT NULL DEFAULT 'native',
    relay_base_url TEXT NOT NULL DEFAULT '',
    stdio_http_endpoints_json TEXT NOT NULL DEFAULT '',
    tool_exposure TEXT NOT NULL DEFAULT 'native',
    harness_verification_json TEXT NOT NULL DEFAULT '{}',
    confirmed_at_ns INTEGER NOT NULL,
    created_at_ns INTEGER NOT NULL,
    updated_at_ns INTEGER NOT NULL,
    UNIQUE (client_kind, config_path)
);
CREATE TABLE IF NOT EXISTS agent_mcp_projections (
    environment_id TEXT NOT NULL
        REFERENCES agent_environments(environment_id) ON DELETE CASCADE,
    server_id TEXT NOT NULL,
    exposed_name TEXT NOT NULL,
    status TEXT NOT NULL,
    entry_fingerprint TEXT NOT NULL DEFAULT '',
    error_detail TEXT,
    desired_at_ns INTEGER NOT NULL,
    applied_at_ns INTEGER,
    updated_at_ns INTEGER NOT NULL,
    PRIMARY KEY (environment_id, server_id)
);
CREATE TABLE IF NOT EXISTS registry_projection_outbox (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    revision INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    environment_id TEXT,
    server_id TEXT,
    detail TEXT,
    occurred_at_ns INTEGER NOT NULL,
    processed INTEGER NOT NULL DEFAULT 0 CHECK (processed IN (0, 1))
);
CREATE TABLE IF NOT EXISTS config_apply_history (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    environment_id TEXT NOT NULL,
    revision INTEGER NOT NULL,
    action TEXT NOT NULL,
    server_id TEXT,
    exposed_name TEXT,
    ok INTEGER NOT NULL CHECK (ok IN (0, 1)),
    detail TEXT,
    at_ns INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS peer_projection_state (
    server_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    transport TEXT NOT NULL DEFAULT 'stdio'
        CHECK (transport IN ('stdio', 'streamable-http')),
    enabled INTEGER NOT NULL CHECK (enabled IN (0, 1)),
    revision INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS projection_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS agent_mcp_projections_env ON agent_mcp_projections(environment_id);
CREATE INDEX IF NOT EXISTS registry_projection_outbox_processed ON registry_projection_outbox(processed, seq);
CREATE INDEX IF NOT EXISTS config_apply_history_env ON config_apply_history(environment_id, seq);
"""


def default_projection_path(side: str) -> Path:
    """Host-local projection authority path, kept beside each host's registry."""
    return default_registry_path(side).with_name("projection.sqlite3")


def _bounded_sha256(path: Path) -> tuple[str, int]:
    """SHA-256 of a config document without unbounded memory growth."""
    size = 0
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        while True:
            chunk = handle.read(1 << 20)
            if not chunk:
                break
            size += len(chunk)
            digest.update(chunk)
            if size >= PROJECTION_IDENTITY_HASH_LIMIT:
                break
    return digest.hexdigest(), size


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _fingerprint(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _fsync_directory(directory: Path) -> None:
    if os.name == "nt":
        return
    try:
        descriptor = os.open(str(directory), os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _atomic_replace_bytes(path: Path, data: bytes, mode: int | None = None) -> None:
    """Locked same-directory temporary write, fsync, and atomic replace."""
    directory = path.parent
    directory.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=str(directory)
    )
    temp_path = Path(temp_name)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        if mode is not None:
            temp_path.chmod(mode)
        elif path.exists():
            temp_path.chmod(stat.S_IMODE(path.stat().st_mode))
        os.replace(temp_path, path)
        _fsync_directory(directory)
    finally:
        if temp_path.exists():
            try:
                temp_path.unlink()
            except OSError:
                pass


class ProjectionDatabase:
    """Host-local SQLite authority for agent enrollment and config projection."""

    SCHEMA_VERSION = PROJECTION_SCHEMA_VERSION
    SCHEMA = PROJECTION_SCHEMA

    def __init__(self, path: Path, *, observe_only: bool = False):
        self.path = path
        self.observe_only = observe_only
        if not path.is_file():
            raise BridgeError(
                f"projection database does not exist: {path}; run projection scan or reconcile"
            )
        if not observe_only and os.name != "nt":
            try:
                path.chmod(0o600)
            except OSError as exc:
                raise BridgeError(
                    f"projection database permissions could not be restricted: {path}"
                ) from exc
        from contextlib import closing
        with closing(self._connect()) as connection:
            version = int(connection.execute("PRAGMA user_version").fetchone()[0])
            if version != self.SCHEMA_VERSION:
                raise BridgeError(
                    f"unsupported projection schema version {version}; "
                    f"expected {self.SCHEMA_VERSION}"
                )
            connection.execute(
                "SELECT environment_id FROM agent_environments LIMIT 1"
            ).fetchall()

    @classmethod
    def ensure(cls, path: Path) -> None:
        previous_umask = os.umask(0o077) if os.name != "nt" else None
        connection: sqlite3.Connection | None = None
        try:
            path.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
            connection = sqlite3.connect(path, timeout=5)
            if os.name != "nt":
                path.chmod(0o600)
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("PRAGMA busy_timeout = 5000")
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute("PRAGMA synchronous = NORMAL")
            # Serialize upgrades before observing their version. A waiting
            # upgrader must see the preceding transaction's committed version.
            connection.execute("BEGIN IMMEDIATE")
            version = int(connection.execute("PRAGMA user_version").fetchone()[0])
            if version not in {0, 1, 2, 3, 4, cls.SCHEMA_VERSION}:
                raise BridgeError(f"cannot migrate projection schema version {version}")
            # executescript implicitly commits an existing transaction. This
            # schema contains plain DDL statements only; execute individually
            # so table creation, all ALTERs and user_version commit together.
            for statement in cls.SCHEMA.split(";"):
                if statement.strip():
                    connection.execute(statement)
            if version == 1:
                connection.execute(
                    "ALTER TABLE agent_environments ADD COLUMN "
                    "transport_capabilities_json TEXT NOT NULL DEFAULT "
                    "'{\"stdio\":true,\"streamable-http\":false}'"
                )
                connection.execute(
                    "ALTER TABLE agent_environments ADD COLUMN compatibility_route "
                    "TEXT NOT NULL DEFAULT 'native'"
                )
                connection.execute(
                    "ALTER TABLE peer_projection_state ADD COLUMN transport "
                    "TEXT NOT NULL DEFAULT 'stdio'"
                )
            if version in (1, 2):
                # v3 persists each environment's Operator-authorized loopback
                # native-Streamable-HTTP relay base URL (empty for stdio-only
                # environments). Only the base is stored; the /mcp/<server-id>
                # path is deterministic code, never registry or operator input.
                connection.execute(
                    "ALTER TABLE agent_environments ADD COLUMN relay_base_url "
                    "TEXT NOT NULL DEFAULT ''"
                )
            if version in (1, 2, 3):
                # v4 persists the Operator-supplied explicit stdio-to-HTTP
                # facade endpoint mapping (registered id -> loopback /mcp URL).
                # Facade provisioning and readiness stay an Operator
                # responsibility: the runtime only projects the configured URL
                # and never launches or supervises a facade process.
                connection.execute(
                    "ALTER TABLE agent_environments ADD COLUMN "
                    "stdio_http_endpoints_json TEXT NOT NULL DEFAULT ''"
                )
            if version in (1, 2, 3, 4):
                # Old environments retain complete catalogs and unknown client
                # capabilities. A recorded probe, not client identity, opts in.
                connection.execute(
                    "ALTER TABLE agent_environments ADD COLUMN "
                    "tool_exposure TEXT NOT NULL DEFAULT 'native'"
                )
                connection.execute(
                    "ALTER TABLE agent_environments ADD COLUMN "
                    "harness_verification_json TEXT NOT NULL DEFAULT '{}'"
                )
            connection.execute(f"PRAGMA user_version = {cls.SCHEMA_VERSION}")
            connection.commit()
        finally:
            if connection is not None:
                connection.close()
            if previous_umask is not None:
                os.umask(previous_umask)

    def _connect(self, *, write: bool = False) -> sqlite3.Connection:
        if write and self.observe_only:
            raise BridgeError("read-only projection database cannot be written")
        if self.observe_only:
            connection = sqlite3.connect(self.path.resolve().as_uri() + "?mode=ro", uri=True, timeout=5)
        else:
            connection = sqlite3.connect(self.path, timeout=5)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 5000")
        if not write:
            connection.execute("PRAGMA query_only = ON")
        return connection

    # ---- read helpers -----------------------------------------------------

    def meta_value(self, connection: sqlite3.Connection, key: str) -> str | None:
        row = connection.execute(
            "SELECT value FROM projection_meta WHERE key = ?", (key,)
        ).fetchone()
        return str(row["value"]) if row is not None else None

    def set_meta(self, connection: sqlite3.Connection, key: str, value: str) -> None:
        connection.execute(
            "INSERT INTO projection_meta (key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, value),
        )

    def current_revision(self, connection: sqlite3.Connection) -> int:
        row = connection.execute(
            "SELECT MAX(revision) AS revision FROM registry_projection_outbox"
        ).fetchone()
        value = int(row["revision"]) if row is not None and row["revision"] is not None else 0
        return value

    def append_outbox(
        self,
        connection: sqlite3.Connection,
        event_type: str,
        *,
        environment_id: str | None = None,
        server_id: str | None = None,
        detail: str | None = None,
        processed: bool = False,
    ) -> int:
        revision = self.current_revision(connection) + 1
        connection.execute(
            "INSERT INTO registry_projection_outbox "
            "(revision, event_type, environment_id, server_id, detail, occurred_at_ns, processed) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                revision,
                event_type,
                environment_id,
                server_id,
                detail,
                time.time_ns(),
                1 if processed else 0,
            ),
        )
        return revision

    def record_history(
        self,
        connection: sqlite3.Connection,
        *,
        environment_id: str,
        revision: int,
        action: str,
        ok: bool,
        server_id: str | None = None,
        exposed_name: str | None = None,
        detail: str | None = None,
    ) -> None:
        connection.execute(
            "INSERT INTO config_apply_history "
            "(environment_id, revision, action, server_id, exposed_name, ok, detail, at_ns) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                environment_id,
                revision,
                action,
                server_id,
                exposed_name,
                1 if ok else 0,
                detail,
                time.time_ns(),
            ),
        )
        connection.execute(
            "DELETE FROM config_apply_history WHERE seq NOT IN ("
            "SELECT seq FROM config_apply_history ORDER BY seq DESC LIMIT ?)",
            (PROJECTION_HISTORY_LIMIT,),
        )

    def environment_rows(self, connection: sqlite3.Connection) -> list[sqlite3.Row]:
        return connection.execute(
            "SELECT * FROM agent_environments ORDER BY client_kind, config_path"
        ).fetchall()

    def environment_row(
        self, connection: sqlite3.Connection, environment_id: str
    ) -> sqlite3.Row | None:
        return connection.execute(
            "SELECT * FROM agent_environments WHERE environment_id = ?",
            (environment_id,),
        ).fetchone()

    def projection_rows(
        self, connection: sqlite3.Connection, environment_id: str | None = None
    ) -> list[sqlite3.Row]:
        if environment_id is None:
            return connection.execute(
                "SELECT * FROM agent_mcp_projections ORDER BY environment_id, server_id"
            ).fetchall()
        return connection.execute(
            "SELECT * FROM agent_mcp_projections WHERE environment_id = ? "
            "ORDER BY server_id",
            (environment_id,),
        ).fetchall()

    def peer_rows(self, connection: sqlite3.Connection) -> list[sqlite3.Row]:
        return connection.execute(
            "SELECT server_id, name, transport, enabled, revision "
            "FROM peer_projection_state ORDER BY server_id"
        ).fetchall()


def _agent_connect_entry(
    *,
    launcher_command: str,
    launcher_args: list[str],
    server_id: str,
    compatibility: bool = False,
    deferred: bool = False,
) -> dict[str, Any]:
    """Adapter-owned launch descriptor for one projected peer registration."""
    if compatibility and deferred:
        raise BridgeError("constant and deferred tool surfaces cannot be combined")
    return {
        "command": launcher_command,
        "args": launcher_args
        + ["deferred-mcp" if deferred else "compatibility-mcp" if compatibility else "connect", server_id],
        "env": {
            BRIDGE_OWNED_ENV_KEY: BRIDGE_OWNED_ENV_VALUE,
            BRIDGE_SERVER_ENV_KEY: server_id,
        },
    }


def _relay_mcp_url(relay_base_url: str, server_id: str) -> str:
    """Deterministic Agent-visible URL of one peer HTTP MCP on this host's relay.

    The relay mounts every peer-registered ``streamable-http`` MCP at
    ``/mcp/<registered-id>`` on the Agent host. Only the loopback base is
    persisted (Operator-selected at enrollment); the path is deterministic
    code and never comes from the registry, a remote caller, or a document.
    """
    return f"{relay_base_url.rstrip('/')}/mcp/{server_id}"


def _normalize_loopback_host_for_url(host: str) -> str:
    lowered = host.lower().strip("[]")
    if lowered == "localhost":
        return "localhost"
    try:
        address = ipaddress.ip_address(lowered)
    except ValueError:
        raise BridgeError(f"relay URL host is not loopback: {host!r}")
    if not address.is_loopback:
        raise BridgeError(f"relay URL host is not loopback: {host!r}")
    if address.version == 6:
        return f"[{address.compressed}]"
    return str(address)


def _validate_relay_base_url(value: Any) -> str:
    """Validate and normalize an Operator-selected loopback relay base URL.

    A native HTTP environment points its Agent client at this host's own
    bridge relay (``serve --http-relay-port``). The accepted form is
    ``http(s)://<loopback-host>:<port>`` with no path, query, fragment, or
    userinfo; the code appends ``/mcp/<server-id>`` itself, so an Operator
    supplied path can never select a different mount.
    """
    if not isinstance(value, str) or not value.strip():
        raise BridgeError("a loopback relay base URL is required for native Streamable HTTP")
    parsed = urllib.parse.urlsplit(value)
    if parsed.scheme.lower() not in {"http", "https"}:
        raise BridgeError(
            f"relay base URL must use http or https: {value!r}"
        )
    if parsed.username is not None or parsed.password is not None:
        raise BridgeError("relay base URL must not carry userinfo")
    host = parsed.hostname
    if host is None:
        raise BridgeError(f"relay base URL has no host: {value!r}")
    try:
        port = parsed.port
    except ValueError:
        raise BridgeError(f"relay base URL has an invalid port: {value!r}")
    if port is None:
        raise BridgeError(
            f"relay base URL must name an explicit loopback port: {value!r}"
        )
    if not (0 < port < 65536):
        raise BridgeError(f"relay base URL port is out of range: {port}")
    if parsed.query or parsed.fragment:
        raise BridgeError("relay base URL must not carry a query or fragment")
    path = parsed.path
    if path not in {"", "/"}:
        raise BridgeError(
            "relay base URL must not carry a path; the bridge appends "
            f"/mcp/<server-id>: {value!r}"
        )
    normalized_host = _normalize_loopback_host_for_url(host)
    return f"{parsed.scheme.lower()}://{normalized_host}:{port}"


def _validate_stdio_http_endpoint_url(value: Any) -> str:
    """Validate/normalize one Operator-supplied stdio-to-HTTP facade endpoint.

    Each registered id maps to the full loopback ``/mcp`` Streamable HTTP URL
    of the Operator's already-provisioned ``stdio_http_facade.py`` instance for
    that id (``http(s)://<loopback>:<port>/mcp``). Only loopback endpoints are
    accepted, no peer registry endpoint or guessed port is ever used, and the
    runtime never probes or supervises the facade: the URL is configuration the
    Operator states to be ready.
    """
    if not isinstance(value, str) or not value.strip():
        raise BridgeError("a stdio-to-http endpoint URL must be a non-empty string")
    parsed = urllib.parse.urlsplit(value.strip())
    if parsed.scheme.lower() not in {"http", "https"}:
        raise BridgeError(
            f"stdio-to-http endpoint URL must use http or https: {value!r}"
        )
    if parsed.username is not None or parsed.password is not None:
        raise BridgeError("stdio-to-http endpoint URL must not carry userinfo")
    host = parsed.hostname
    if host is None:
        raise BridgeError(f"stdio-to-http endpoint URL has no host: {value!r}")
    try:
        port = parsed.port
    except ValueError:
        raise BridgeError(f"stdio-to-http endpoint URL has an invalid port: {value!r}")
    if port is None:
        raise BridgeError(
            f"stdio-to-http endpoint URL must name an explicit loopback port: {value!r}"
        )
    if not (0 < port < 65536):
        raise BridgeError(f"stdio-to-http endpoint URL port is out of range: {port}")
    if parsed.query or parsed.fragment:
        raise BridgeError("stdio-to-http endpoint URL must not carry a query or fragment")
    if parsed.path not in {"", "/", "/mcp", "/mcp/"}:
        raise BridgeError(
            "stdio-to-http endpoint URL path must be the facade's /mcp endpoint "
            f"(got {parsed.path!r})"
        )
    normalized_host = _normalize_loopback_host_for_url(host)
    return f"{parsed.scheme.lower()}://{normalized_host}:{port}/mcp"


def _parse_stdio_http_endpoints(value: Any) -> dict[str, str]:
    """Parse and validate the explicit per-target facade endpoint mapping.

    The argument is either an inline JSON object (preferred simple form,
    ``{\"id\": \"http://127.0.0.1:PORT/mcp\"}``) or the path to a local file
    containing that JSON. Values are validated/normalized loopback /mcp URLs
    and keys must be valid registered ids; the mapping is bounded so a single
    environment can never carry an unbounded endpoint table.
    """
    if not isinstance(value, str) or not value.strip():
        raise BridgeError(
            "--stdio-http-endpoints must be a JSON object mapping registered ids "
            "to loopback /mcp URLs, or the path of a local file containing one"
        )
    text = value.strip()
    try:
        mapping = json.loads(text)
    except ValueError:
        # Not inline JSON: the argument is the path of a local file containing
        # the JSON object mapping.
        try:
            text = Path(text).expanduser().read_text(encoding="utf-8")
        except OSError as exc:
            raise BridgeError(
                f"--stdio-http-endpoints file could not be read: {exc}"
            )
        try:
            mapping = json.loads(text)
        except ValueError as exc:
            raise BridgeError(f"--stdio-http-endpoints is not valid JSON: {exc}")
    if not isinstance(mapping, dict):
        raise BridgeError(
            "--stdio-http-endpoints must decode to a JSON object of "
            "registered id -> loopback /mcp URL"
        )
    if len(mapping) > STDIO_HTTP_ENDPOINT_LIMIT:
        raise BridgeError(
            f"--stdio-http-endpoints mapping is bounded to "
            f"{STDIO_HTTP_ENDPOINT_LIMIT} entries"
        )
    endpoints: dict[str, str] = {}
    for key, endpoint in mapping.items():
        if not isinstance(key, str) or not key:
            raise BridgeError("--stdio-http-endpoints ids must be non-empty strings")
        if len(key) > 128 or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", key):
            raise BridgeError(
                f"--stdio-http-endpoints id {key!r} is not a valid registered id"
            )
        endpoints[key] = _validate_stdio_http_endpoint_url(endpoint)
    return endpoints


def _agent_http_entry(*, relay_base_url: str, server_id: str) -> dict[str, Any]:
    """Adapter-owned native Streamable HTTP descriptor for one peer registration.

    The URL is deterministic code over the persisted loopback relay base; the
    loopback relay needs no client headers, so the canonical entry carries an
    empty header map (kept out of fingerprints when empty so readback and
    write representations agree across the client document formats).
    """
    return {
        "url": _relay_mcp_url(relay_base_url, server_id),
        "headers": {},
    }


def _env_row_field(row: Any, key: str, default: Any = "") -> Any:
    """Column access tolerant of both sqlite3.Row and plain-dict fixtures."""
    try:
        return row[key]
    except (KeyError, IndexError, TypeError):
        return default


def _row_stdio_http_endpoints(row: Any) -> dict[str, str]:
    """The persisted per-target facade endpoint mapping of one environment row."""
    raw = _env_row_field(row, "stdio_http_endpoints_json", "")
    if not isinstance(raw, str) or not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except ValueError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _harness_config_fingerprint(row: Any) -> str:
    path = Path(row["config_path"])
    if not path.is_file():
        return _fingerprint({"configPath": str(path.resolve()), "state": "absent"})
    if path.stat().st_size > min(PROJECTION_CONFIG_READ_LIMIT, PROJECTION_IDENTITY_HASH_LIMIT):
        raise BridgeError("client configuration exceeds the verification fingerprint bound")
    return _bounded_sha256(path)[0]


def _row_harness_verification(row: Any) -> dict[str, Any]:
    """Recorded probe evidence, never guessed from a client's product name."""
    raw = _env_row_field(row, "harness_verification_json", "{}")
    try:
        value = json.loads(raw)
    except (ValueError, TypeError):
        raise BridgeError("invalid recorded harness verification") from None
    if not isinstance(value, dict):
        raise BridgeError("invalid recorded harness verification")
    return value


def _effective_tool_exposure(row: Any) -> str:
    mode = _env_row_field(row, "tool_exposure", "native")
    if mode not in {"native", "auto", "deferred"}:
        raise BridgeError("invalid recorded tool exposure mode")
    if mode == "native":
        return mode
    state = _harness_evidence_state(row)
    capabilities = state["capabilities"]
    if not isinstance(capabilities, dict):
        raise BridgeError("invalid recorded harness capabilities")
    # An observation proves this exact environment only; no universal claim
    # about the named Harness. Expired/absent evidence cannot enable deferral.
    usable = (
        state["fresh"]
        and capabilities.get("toolsListChanged") == "supported"
        and capabilities.get("modelExposure") == "supported"
        and state["evidence"].get("protocolVersion") in SHARED_COMPATIBLE_PROTOCOL_VERSIONS
    )
    if mode == "deferred":
        if not usable:
            raise BridgeError("deferred exposure requires fresh client refresh and model-exposure probe evidence")
        return "deferred"
    if not usable or capabilities.get("nativeToolSearch") != "unsupported":
        return "native"
    return "deferred"


# ---- Client capability aspects (probe <-> registry adaptation) -------------
#
# One aspect names the Harness-side observation that can justify one registry or
# enrollment decision, so a Bridge Agent can explore how far an enrolled target
# environment really supports what this host projects.  The bounded fixture in
# harness_verification.py observes the whole environment in a single run; the
# aspect selects the decision the Agent intends to establish and therefore which
# prepared challenge it consumes.
#
# Scope boundary: this probe observes Harness capability only.  It never measures
# a business MCP's tool count, tool-definition volume, or the model-context token
# cost of exposing a catalog.  Those are per-MCP exposure observations derived
# from an observed catalog (see deferred_tools.py) and recorded separately, so no
# aspect here may be used as a proxy for a token or catalog-volume measurement.

CLIENT_CAPABILITY_ASPECTS: dict[str, dict[str, Any]] = {
    "refresh": {
        "capability": "toolsListChanged",
        "observation": "protocol-observed",
        "registryField": "tool_exposure",
        "decision": "deferred (library-collapsed) exposure requires an observed "
                    "post-notification catalog refresh",
        "runnable": True,
        "agentSteps": (
            "Prepare this aspect's challenge with projection probe-client --aspect refresh.",
            "In an explicitly authorized isolated session of the enrolled Harness, run the "
            "printed fixture command as the Harness's MCP server.",
            "Let the Harness initialize, call the bootstrap tool it discovers, process "
            "notifications/tools/list_changed, re-list, and call the canary with the proof "
            "from the refreshed catalog.",
            "Record the receipt with projection record-client-verification.",
        ),
    },
    "harness-protocol": {
        "capability": "protocolVersion",
        "observation": "protocol-observed",
        "registryField": "compatibility_route",
        "decision": "deferral requires a legacy revision this host also verified",
        "runnable": True,
        "supportedValues": tuple(sorted(SHARED_COMPATIBLE_PROTOCOL_VERSIONS)),
        "agentSteps": (
            "Run the same bounded fixture in an isolated session of the enrolled Harness.",
            "The negotiated revision is observed during initialize and recorded with the receipt.",
            "A revision outside this host's verified set keeps the environment on the configured route.",
        ),
    },
    "model-exposure": {
        "capability": "modelExposure",
        "observation": "challenge-bound-attestation",
        "registryField": "tool_exposure",
        "decision": "deferred exposure requires the model to be shown the discovered "
                    "tool definitions",
        "runnable": True,
        "agentSteps": (
            "Run the same bounded fixture in an isolated session of the enrolled Harness.",
            "Observe, in the model context, whether the post-notification tool definitions are "
            "actually visible to the model.",
            "The fixture alone cannot prove model exposure: record that observation as a "
            "challenge-bound attestation inside the receipt before recording it.",
        ),
    },
    "native-search": {
        "capability": "nativeToolSearch",
        "observation": "challenge-bound-attestation",
        "registryField": "tool_exposure",
        "decision": "automatic exposure collapses the catalog only when the Harness has "
                    "no native tool search",
        "runnable": True,
        "agentSteps": (
            "Run the same bounded fixture in an isolated session of the enrolled Harness.",
            "Observe whether the Harness exposes its own tool-search or hidden-tool invocation "
            "instead of a full catalog.",
            "Record that observation as a challenge-bound attestation inside the receipt; "
            "protocol traffic never proves it.",
        ),
    },
    "modern-protocol": {
        "capability": "protocolVersion",
        "observation": "not-observable",
        "registryField": "compatibility_route",
        "decision": "modern discovery/subscription support is not observable with this "
                    "probe version and stays unclaimed",
        "runnable": False,
        "agentSteps": (
            "Not runnable: the bounded fixture deliberately speaks legacy revisions only and "
            "answers server/discover and subscriptions/listen with an error.",
            "A modern revision must never be inferred from a Harness's product name; observe it "
            "with a separate, explicitly authorized fixture if it is needed.",
        ),
    },
}

_PROBE_META_PREFIX = "client_probe:"
#: Prepared challenges from before per-aspect keys.  The fixture is identical for
#: every aspect, so a legacy challenge is still recordable, but its intent is
#: unknown and is therefore never reported as a specific aspect.
_PROBE_META_LEGACY_ASPECT = "legacy"


def _harness_evidence_state(row: Any) -> dict[str, Any]:
    """Recorded client evidence plus exactly why it does or does not apply now.

    A recorded observation is bound to one environment, one client kind, and one
    configuration fingerprint, and it carries its own completion lifetime.  Any
    mismatch is reported as a reason instead of silently narrowing a catalog.
    """
    evidence = _row_harness_verification(row)
    reasons: list[str] = []
    if not evidence:
        reasons.append("no recorded client verification for this environment")
    else:
        if evidence.get("environmentId") != row["environment_id"]:
            reasons.append("recorded verification is bound to a different environment")
        if evidence.get("clientKind") != row["client_kind"]:
            reasons.append("recorded verification is bound to a different client kind")
        recorded = evidence.get(
            "projectionConfigFingerprint", evidence.get("configFingerprint")
        )
        if recorded != _harness_config_fingerprint(row):
            reasons.append("the enrolled client configuration changed after this verification")
        valid_until = evidence.get("validUntil")
        if not isinstance(valid_until, (int, float)):
            reasons.append("recorded verification records no completion lifetime")
        elif valid_until <= time.time():
            reasons.append("recorded verification expired")
    return {
        "evidence": evidence,
        "capabilities": evidence.get("capabilities", {}),
        "fresh": not reasons,
        "reasons": reasons,
        "verifiedAt": evidence.get("verifiedAt"),
        "validUntil": evidence.get("validUntil"),
    }


def _aspect_observation(aspect: str, state: dict[str, Any]) -> tuple[str, Any]:
    """Current state and observed value for one aspect.

    ``not-observable`` is a property of this probe version, never of the target
    Harness; ``unknown`` means the recorded run simply did not establish it.
    """
    definition = CLIENT_CAPABILITY_ASPECTS[aspect]
    if not definition["runnable"]:
        return "not-observable", None
    if not state["evidence"]:
        return "not-probed", None
    if not state["fresh"]:
        return "stale", None
    capability = definition["capability"]
    if "supportedValues" in definition:
        observed = state["evidence"].get(capability)
        if not isinstance(observed, str):
            return "unknown", None
        supported = observed in definition["supportedValues"]
        return ("supported" if supported else "unsupported"), observed
    capabilities = state["capabilities"]
    observed = capabilities.get(capability) if isinstance(capabilities, dict) else None
    if observed in {"supported", "unsupported"}:
        return str(observed), observed
    return "unknown", observed if isinstance(observed, str) else None


def _read_probe_record(raw: str) -> dict[str, Any]:
    """Accept both the enveloped per-aspect record and the legacy bare challenge."""
    try:
        value = json.loads(raw)
    except (TypeError, ValueError):
        raise BridgeError("invalid outstanding client probe record") from None
    if not isinstance(value, dict):
        raise BridgeError("invalid outstanding client probe record")
    if "challenge" in value:
        challenge = value["challenge"]
        if not isinstance(challenge, dict):
            raise BridgeError("invalid outstanding client probe challenge")
        return {
            "challenge": challenge,
            "aspect": value.get("aspect"),
            "probeFile": value.get("probeFile"),
            "receiptFile": value.get("receiptFile"),
            "enveloped": True,
        }
    return {
        "challenge": value, "aspect": None, "probeFile": None,
        "receiptFile": None, "enveloped": False,
    }


def _probe_records(
    connection: sqlite3.Connection, environment_id: str,
) -> dict[str, dict[str, Any]]:
    """Unconsumed prepared challenges for one environment, keyed by aspect."""
    prefix = _PROBE_META_PREFIX + environment_id
    rows = connection.execute(
        "SELECT key, value FROM projection_meta WHERE key LIKE ?",
        (_PROBE_META_PREFIX + "%",),
    ).fetchall()
    records: dict[str, dict[str, Any]] = {}
    for row in rows:
        key = str(row["key"])
        if key == prefix:
            aspect = _PROBE_META_LEGACY_ASPECT
        elif key.startswith(prefix + ":"):
            aspect = key[len(prefix) + 1:]
        else:
            continue
        record = _read_probe_record(str(row["value"]))
        record["aspect"] = aspect
        record["metaKey"] = key
        records[aspect] = record
    return records


def _outstanding_client_probes(
    connection: sqlite3.Connection, environment_id: str,
) -> dict[str, dict[str, Any]]:
    """Bounded summaries of unconsumed prepared challenges, keyed by aspect.

    Preparation alone proves nothing: this is local challenge state, not a
    capability.  Challenges are reported with their lifetime so a Bridge Agent
    can tell an outstanding, expired, or legacy challenge apart.
    """
    outstanding: dict[str, dict[str, Any]] = {}
    for aspect, record in _probe_records(connection, environment_id).items():
        challenge = record["challenge"]
        expires = challenge.get("expiresAt")
        binding = challenge.get("binding")
        binding = binding if isinstance(binding, dict) else {}
        outstanding[aspect] = {
            "aspect": aspect,
            "metaKey": record["metaKey"],
            "probeId": challenge.get("probeId"),
            "issuedAt": challenge.get("issuedAt"),
            "expiresAt": expires,
            "expiresInSeconds": (
                round(expires - time.time(), 3) if isinstance(expires, (int, float)) else None
            ),
            "expired": isinstance(expires, (int, float)) and expires <= time.time(),
            "probeFile": record["probeFile"],
            "receiptFile": record["receiptFile"],
            "configFingerprint": binding.get("configFingerprint"),
            "knownAspect": aspect in CLIENT_CAPABILITY_ASPECTS,
        }
    return outstanding


def _probe_next_action(challenge: dict[str, Any]) -> str:
    if challenge["expired"]:
        return ("this prepared challenge expired; prepare a fresh one for aspect "
                + str(challenge["aspect"]))
    if challenge["probeFile"]:
        return ("run the printed fixture command, then record-client-verification with "
                + str(challenge["receiptFile"]))
    return "run the prepared fixture in an isolated target session, then record the receipt"


def _capability_checklist(
    row: Any, outstanding: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    """Per-aspect adaptation report for one enrollment, with the next observation."""
    state = _harness_evidence_state(row)
    mode = _env_row_field(row, "tool_exposure", "native")
    try:
        effective = _effective_tool_exposure(row)
        exposure_error = None
    except BridgeError as exc:
        effective, exposure_error = "invalid", str(exc)
    capabilities = state["capabilities"] if isinstance(state["capabilities"], dict) else {}
    items: list[dict[str, Any]] = []
    actions: list[str] = []
    for aspect, definition in CLIENT_CAPABILITY_ASPECTS.items():
        observed_state, value = _aspect_observation(aspect, state)
        challenge = outstanding.get(aspect)
        if not definition["runnable"]:
            action = None
        elif challenge is not None:
            action = _probe_next_action(challenge)
        elif observed_state == "stale":
            action = ("re-observe this aspect: " + "; ".join(state["reasons"]))
        elif observed_state == "supported":
            action = ("evidence applies until " + _utc_text(state["validUntil"])
                      + "; re-observe before it expires")
        elif observed_state == "unsupported":
            action = ("observed negative; it remains recorded evidence, and re-observing is "
                      "the only way to change it")
        elif observed_state == "unknown":
            action = ("not established by the bounded fixture alone; "
                      + definition["agentSteps"][-1])
        else:
            action = ("prepare the aspect challenge with projection probe-client --aspect "
                      + aspect)
        items.append({
            "aspect": aspect,
            "state": observed_state,
            "capability": definition["capability"],
            "capabilityValue": value,
            "observation": definition["observation"],
            "registryField": definition["registryField"],
            "decision": definition["decision"],
            "outstandingChallenge": challenge,
            "nextAction": action,
            "agentSteps": list(definition["agentSteps"]),
        })
        if action is not None:
            actions.append(action)
    blockers: list[str] = []
    if mode == "native":
        blockers.append("this environment records native exposure: no deferral is requested")
    else:
        blockers.extend(state["reasons"])
        for aspect in ("refresh", "model-exposure"):
            capability = CLIENT_CAPABILITY_ASPECTS[aspect]["capability"]
            if capabilities.get(capability) != "supported":
                blockers.append(
                    aspect + " is not proven ("
                    + capability + "=" + str(capabilities.get(capability, "unknown")) + ")"
                )
        protocol = state["evidence"].get("protocolVersion")
        if protocol not in SHARED_COMPATIBLE_PROTOCOL_VERSIONS:
            blockers.append("observed protocol revision " + str(protocol)
                            + " is not a revision this host verified")
        if mode == "auto" and capabilities.get("nativeToolSearch") == "supported":
            blockers.append("the Harness has native tool search, so automatic exposure stays native")
        try:
            stdio = bool(json.loads(row["transport_capabilities_json"]).get("stdio"))
        except (TypeError, ValueError):
            stdio = False
        if row["compatibility_route"] != "native" or not stdio:
            blockers.append("deferred exposure requires the native stdio route and stdio capability")
    return {
        "environmentId": row["environment_id"],
        "toolExposure": mode,
        "effectiveToolExposure": effective,
        "toolExposureError": exposure_error,
        "toolExposureBlockers": blockers,
        "evidenceFresh": state["fresh"],
        "evidenceReasons": state["reasons"],
        "verifiedAt": state["verifiedAt"],
        "validUntil": state["validUntil"],
        "capabilities": items,
        "outstandingChallenges": sorted(
            outstanding.values(), key=lambda item: str(item["aspect"]),
        ),
        "nextActions": actions,
    }


def _utc_text(value: Any) -> str:
    if not isinstance(value, (int, float)):
        return "an unknown time"
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(value))



def _default_launcher(side: str) -> tuple[str, list[str]]:
    """Deterministic default bridge launcher for projected entries."""
    component = Path(__file__).resolve().parent / f"{side}-bridge-mcp" / "bridge.py"
    if component.is_file():
        return sys.executable, [str(component)]
    console = "win-wsl-mcp-win" if side == "win" else "win-wsl-mcp-wsl"
    return console, []


def _validate_launcher(side: str, value: str, args: list[str]) -> None:
    """Validate that the launcher references only this side's bridge component."""
    if not all(isinstance(item, str) for item in args):
        raise BridgeError("bridge launcher args must be strings")
    path = Path(value)
    if path.is_absolute():
        if not path.is_file():
            raise BridgeError(f"bridge launcher is not an existing file: {value}")
        component = Path(__file__).resolve().parent / f"{side}-bridge-mcp" / "bridge.py"
        references_component = any(
            Path(arg).is_absolute() and os.path.abspath(arg) == str(component)
            for arg in args
        )
        if not references_component:
            raise BridgeError(
                f"an absolute bridge launcher must name the {side} component "
                f"bridge.py in its args: {component}"
            )
    else:
        expected = "win-wsl-mcp-win" if side == "win" else "win-wsl-mcp-wsl"
        if value != expected:
            raise BridgeError(
                f"bridge launcher must be the {expected} console entry: {value}"
            )
        if shutil.which(value) is None:
            raise BridgeError(f"bridge launcher is not installed on PATH: {value}")
        if any(Path(arg).is_absolute() for arg in args):
            raise BridgeError(
                "console-script launchers must not carry absolute bridge args"
            )


def _scan_home(side: str) -> dict[str, Path]:
    del side
    home = Path.home()
    return {
        "codex": Path(os.environ.get("CODEX_HOME", home / ".codex")),
        "dsh": Path(os.environ.get("DSH_HOME", home / ".dsh")),
    }


def _existing_mcp_names_for_document(
    kind: str, path: Path
) -> tuple[list[str], list[str], bool]:
    """Read-only redacted enumeration of MCP names in one config document.

    Returns (existing_names, bridge_owned_names, parsed). Values are never
    returned: only names plus which names carry the bridge ownership marker.
    Codex TOML documents are enumerated with the stdlib tomllib reader when
    possible (python>=3.11) with a bounded line-inspection fallback for a
    document the strict parser rejects; configuration is never patched here.
    """
    try:
        size = path.stat().st_size
        if size > PROJECTION_CONFIG_READ_LIMIT:
            return [], [], False
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return [], [], False
    names: list[str] = []
    owned: list[str] = []
    if kind == CLIENT_KIND_CLAUDE or path.suffix.lower() == ".json":
        try:
            document = json.loads(text)
        except ValueError:
            return [], [], False
        servers = document.get("mcpServers", {}) if isinstance(document, dict) else None
        if not isinstance(servers, dict):
            return [], [], False
        for name, value in servers.items():
            if not isinstance(name, str):
                continue
            names.append(name)
            env = value.get("env") if isinstance(value, dict) else None
            if isinstance(env, dict) and BRIDGE_OWNED_ENV_KEY in env:
                owned.append(name)
        return names, owned, True
    try:
        import tomllib

        document = tomllib.loads(text)
    except Exception:
        document = None
    if isinstance(document, dict):
        servers = document.get("mcp_servers")
        if isinstance(servers, dict):
            for name, value in servers.items():
                if not isinstance(name, str):
                    continue
                names.append(name)
                env = value.get("env") if isinstance(value, dict) else None
                if isinstance(env, dict) and BRIDGE_OWNED_ENV_KEY in env:
                    owned.append(name)
        return names, owned, True
    section: str | None = None
    server_name: str | None = None
    marker = BRIDGE_OWNED_ENV_KEY
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("[") and line.endswith("]"):
            section = line[1:-1].strip()
            server_name = None
            if section.startswith("mcp_servers."):
                rest = section[len("mcp_servers."):].strip().strip("\"'")
                if rest and "." not in rest:
                    server_name = rest
        if server_name is None:
            continue
        if server_name not in names:
            names.append(server_name)
        if marker in line and "=" in line and server_name not in owned:
            owned.append(server_name)
    return names, owned, bool(text)


def _kind_for_path(path: Path) -> str | None:
    lowered = str(path).lower()
    name = path.name.lower()
    if name == "config.toml" and ".codex" in lowered:
        return CLIENT_KIND_CODEX
    if name in {".mcp.json", ".claude.json"}:
        return CLIENT_KIND_CLAUDE
    if name == "cordis-bridge-overlay.json" and ".dsh" in lowered:
        return CLIENT_KIND_DSH
    if name == "cordis.patch.yml" and ".dsh" in lowered:
        return CLIENT_KIND_DSH
    return None


def _candidate_id(kind: str, path: Path, digest: str, size: int, mtime_ns: int) -> str:
    raw = _fingerprint([kind, str(path), digest, size, mtime_ns])
    return f"{PROJECTION_SCAN_CANDIDATE_PREFIX}{raw[:20]}"


def _scan_single_candidate(
    kind: str,
    scope: str,
    path: Path,
    source: str,
    *,
    side: str,
) -> dict[str, Any]:
    exists = path.exists()
    stat_value = path.stat() if exists else None
    size = int(stat_value.st_size) if stat_value is not None else 0
    mtime_ns = int(stat_value.st_mtime_ns) if stat_value is not None else 0
    digest = ""
    names: list[str] = []
    owned: list[str] = []
    parsed = False
    readable = False
    conflicts: list[str] = []
    notes: list[str] = []
    if exists and stat_value is not None:
        if not stat.S_ISREG(stat_value.st_mode):
            conflicts.append("not-a-regular-file")
        readable = os.access(path, os.R_OK)
        if readable:
            try:
                digest, _ = _bounded_sha256(path)
                names, owned, parsed = _existing_mcp_names_for_document(kind, path)
            except OSError:
                readable = False
                conflicts.append("unreadable")
        if path.is_symlink():
            try:
                target = path.resolve(strict=True)
                if str(target) != str(path.resolve(strict=False)) and not str(
                    target
                ).startswith(str(path.parent.resolve())):
                    conflicts.append("symlink-outside-parent")
                else:
                    notes.append("symlink")
            except OSError:
                conflicts.append("symlink-unresolvable")
    else:
        notes.append("absent")
    writable = bool(path.parent.is_dir()) and os.access(path.parent, os.W_OK)
    if not writable:
        conflicts.append("not-writable")
    if kind == CLIENT_KIND_CLAUDE and path.name == ".claude.json":
        notes.append("may-contain-credentials")
    if kind == CLIENT_KIND_DSH and path.name == "cordis.patch.yml":
        notes.append(
            "dsh-profile-patch-yaml-is-not-stdlib-writable;"
            "project dsh through an explicit Bridge-owned cordis-bridge-overlay.json"
        )
    return {
        "candidateId": _candidate_id(kind, path, digest, size, mtime_ns),
        "clientKind": kind,
        "hostSide": side,
        "scope": scope,
        "configPath": str(path),
        "discoverySource": source,
        "exists": exists,
        "readable": readable,
        "writable": writable,
        "parsed": parsed,
        "size": size,
        "mtimeNs": mtime_ns,
        "fileSha256": digest,
        "mayContainSecrets": kind == CLIENT_KIND_CLAUDE and path.name == ".claude.json",
        "existingMcpNames": names,
        "bridgeOwnedMcpNames": owned,
        "conflicts": conflicts,
        "notes": notes,
    }


def projection_scan_candidates(
    side: str,
    *,
    extra_paths: tuple[Path, ...] = (),
) -> list[dict[str, Any]]:
    """Bounded read-only scanner for known Codex/Claude/DSH config locations.

    Never returns credentials, header/env values, or business-MCP launch
    definitions. An explicit path seeds the same scanner; the home directory is
    never walked recursively.
    """
    locations: list[tuple[str, str, Path, str]] = []
    bases = _scan_home(side)
    locations.append((CLIENT_KIND_CODEX, "user", bases["codex"] / "config.toml", "scan"))
    locations.append((CLIENT_KIND_CLAUDE, "user", Path.home() / ".claude.json", "scan"))
    profile_dir = bases["dsh"] / "profiles"
    if profile_dir.is_dir():
        for profile in sorted(item for item in profile_dir.iterdir() if item.is_dir()):
            patch = profile / "cordis.patch.yml"
            overlay = profile / "cordis-bridge-overlay.json"
            if patch.is_file():
                locations.append((CLIENT_KIND_DSH, "user", patch, "scan"))
                locations.append((CLIENT_KIND_DSH, "user", overlay, "scan"))
    for path in extra_paths:
        resolved = Path(path).expanduser()
        kind = _kind_for_path(resolved)
        if kind is None:
            continue
        locations.append((kind, "explicit", resolved, "explicit-path"))
    candidates: list[dict[str, Any]] = []
    seen: set[str] = set()
    for kind, scope, path, source in locations:
        try:
            resolved = path.resolve(strict=False)
        except OSError:
            continue
        key = f"{kind}|{resolved}"
        if key in seen:
            continue
        seen.add(key)
        candidates.append(
            _scan_single_candidate(
                kind, scope, resolved, source, side=side
            )
        )
    candidates.sort(key=lambda item: (item["clientKind"], item["configPath"]))
    return candidates


# ---- Client adapters ------------------------------------------------------
#
# Adapters are the only place an Agent config command/args/env is constructed.
# Official-CLI mode ("official-cli") delegates to the client's own mutation
# command when it provides a safe interface; bridge-file mode ("bridge-file")
# modifies only a Bridge-owned generated document (a wholly owned Codex config,
# the managed mcpServers keys of a Claude JSON document, or a Bridge-owned DSH
# overlay) through locked same-directory temporary write, fsync, atomic
# replace, readback, and rollback. Unmanaged configuration is never edited or
# deleted and no TOML/YAML text is patched ad hoc.


def _codex_cli_available() -> bool:
    return shutil.which("codex") is not None


def _claude_cli_available() -> bool:
    return shutil.which("claude") is not None


def _run_tool(argv: list[str], timeout: int = 30) -> tuple[int, str, str]:
    """Run an allowlisted official client read/mutation command (no shell)."""
    completed = subprocess.run(
        argv,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=timeout,
        check=False,
    )
    return completed.returncode, completed.stdout, completed.stderr


def _entry_http_fingerprint_payload(entry: dict[str, Any]) -> dict[str, Any]:
    """Canonical fingerprint payload of a native Streamable HTTP entry.

    URL/transport identity is the fingerprint: a transport transition is
    therefore a fingerprint change (remove/add, never in-place). Headers are
    included only when non-empty so an empty bridge-written header map
    fingerprints identically to a document readback that omitted it, while a
    user-added header still registers as drift on removal.
    """
    headers = entry.get("headers")
    payload: dict[str, Any] = {
        "transport": "streamable-http",
        "url": entry.get("url"),
    }
    if isinstance(headers, dict) and headers:
        payload["headers"] = {str(k): str(v) for k, v in sorted(headers.items())}
    return payload


def _entry_fingerprint(entry: dict[str, Any]) -> str:
    if "url" in entry:
        return _fingerprint(_entry_http_fingerprint_payload(entry))
    return _fingerprint(
        {
            "command": entry.get("command"),
            "args": entry.get("args", []),
            "env": entry.get("env", {}),
        }
    )


def _entry_fingerprint_for_kind(kind: str, entry: dict[str, Any]) -> str:
    """Document-representation fingerprint.

    Persisted fingerprints must match what a later read of the document
    produces. The DSH overlay representation stores no env for stdio entries,
    so env is dropped before hashing for DSH; Streamable HTTP entries
    fingerprint over their URL/transport identity uniformly.
    """
    if "url" in entry:
        return _fingerprint(_entry_http_fingerprint_payload(entry))
    if kind == CLIENT_KIND_DSH:
        entry = {**entry, "env": {}}
    return _fingerprint(
        {
            "command": entry.get("command"),
            "args": entry.get("args", []),
            "env": entry.get("env", {}),
        }
    )


def _looks_bridge_owned(
    entry: dict[str, Any],
    *,
    launcher_command: str,
    launcher_args: list[str],
    name: str | None = None,
    relay_base_url: str = "",
    stdio_http_endpoints: dict[str, str] | None = None,
) -> bool:
    """Whether a document entry was created by this projection authority.

    Stdio entries carry the ownership env marker (or the deterministic local
    ``connect`` launcher prefix). Streamable HTTP entries cannot carry an env
    marker, so ownership is the URL identity: the entry's URL must equal the
    deterministic relay URL the bridge would project for that exact server id
    on the environment's persisted loopback relay base, or the explicitly
    configured stdio-to-HTTP facade endpoint the Operator mapped to that id.
    An unmanaged entry of the same name with any other URL is a collision,
    never owned.
    """
    env = entry.get("env")
    if isinstance(env, dict) and BRIDGE_OWNED_ENV_KEY in env:
        return True
    url = entry.get("url")
    if isinstance(url, str) and name:
        if relay_base_url and url == _relay_mcp_url(relay_base_url, name):
            return True
        if stdio_http_endpoints and stdio_http_endpoints.get(name) == url:
            return True
    command = entry.get("command")
    args = entry.get("args")
    if not isinstance(command, str) or not isinstance(args, list):
        return False
    # DSH intentionally omits env markers. All per-target Bridge frontends
    # therefore need launcher ownership recognition; persisted fingerprints
    # still decide whether an observed entry is unchanged, drifted or removable.
    modes = {"connect", "connect-http", "compatibility-mcp", "deferred-mcp"}
    return (
        command == launcher_command
        and args[:len(launcher_args)] == launcher_args
        and len(args) > len(launcher_args)
        and args[len(launcher_args)] in modes
    )


def _resolve_adapter_mode(
    kind: str,
    config_path: Path,
    *,
    launcher_command: str,
    launcher_args: list[str],
) -> str:
    """Deterministic apply-adapter resolution for one environment."""
    del launcher_command, launcher_args
    if kind == CLIENT_KIND_CODEX:
        if _codex_cli_available():
            return ADAPTER_OFFICIAL_CLI
        if _adapter_file_mode_supported(kind, config_path):
            return ADAPTER_BRIDGE_FILE
        return ADAPTER_UNAVAILABLE
    if kind == CLIENT_KIND_CLAUDE:
        if _claude_cli_available():
            return ADAPTER_OFFICIAL_CLI
        if _adapter_file_mode_supported(kind, config_path):
            return ADAPTER_BRIDGE_FILE
        return ADAPTER_UNAVAILABLE
    if kind == CLIENT_KIND_DSH:
        if _adapter_file_mode_supported(kind, config_path):
            return ADAPTER_BRIDGE_FILE
        return ADAPTER_UNAVAILABLE
    return ADAPTER_UNAVAILABLE


def _adapter_file_mode_supported(kind: str, config_path: Path) -> bool:
    if kind == CLIENT_KIND_DSH:
        return config_path.name == "cordis-bridge-overlay.json"
    if kind == CLIENT_KIND_CLAUDE:
        return config_path.name in {".mcp.json", ".claude.json"}
    if kind == CLIENT_KIND_CODEX:
        if not config_path.exists():
            return True
        return _codex_document_is_bridge_owned(config_path)
    return False


def _codex_document_is_bridge_owned(path: Path) -> bool:
    try:
        head = path.read_text(encoding="utf-8", errors="replace")[:512]
    except OSError:
        return False
    return head.lstrip().startswith("# WIN-WSL-MCP-BRIDGE-MANAGED")


def _toml_basic_string(value: Any) -> str:
    """TOML basic-string literal escaping (mirrors the stdlib tomllib writer
    subset the bridge needs: quotes, backslashes, and control characters)."""
    rendered = ['"']
    for char in str(value):
        codepoint = ord(char)
        if char == '"':
            rendered.append('\\"')
        elif char == "\\":
            rendered.append("\\\\")
        elif char == "\b":
            rendered.append("\\b")
        elif char == "\t":
            rendered.append("\\t")
        elif char == "\n":
            rendered.append("\\n")
        elif char == "\f":
            rendered.append("\\f")
        elif char == "\r":
            rendered.append("\\r")
        elif codepoint < 0x20 or codepoint == 0x7F:
            rendered.append(f"\\u{codepoint:04X}")
        else:
            rendered.append(char)
    rendered.append('"')
    return "".join(rendered)


def _codex_owned_document_text(entries: dict[str, dict[str, Any]]) -> str:
    lines = [
        "# WIN-WSL-MCP-BRIDGE-MANAGED",
        "# Codex mcp_servers generated and owned by the win-wsl-mcp-bridge",
        "# projection authority. Reconcile regenerates this whole document.",
        "",
    ]
    for name in sorted(entries):
        entry = entries[name]
        lines.append(f"[mcp_servers.{name}]")
        if "url" in entry:
            # Native Streamable HTTP server: verified real Codex shape
            # (`codex mcp add <name> --url <url>` writes exactly this key).
            lines.append(f"url = {_toml_basic_string(entry['url'])}")
        else:
            lines.append(f"command = {_toml_basic_string(entry.get('command'))}")
            args = ", ".join(_toml_basic_string(item) for item in entry.get("args", []))
            lines.append(f"args = [{args}]")
            env_items = ", ".join(
                f"{_toml_basic_string(key)} = {_toml_basic_string(value)}"
                for key, value in sorted(entry.get("env", {}).items())
            )
            lines.append(f"env = {{{env_items}}}")
        lines.append("")
    return "\n".join(lines)


def _codex_owned_document_entries(text: str) -> dict[str, dict[str, Any]]:
    """Parse a Bridge-owned Codex config with the stdlib tomllib reader.

    The whole document is deterministic bridge output, so a full TOML parse is
    safe; values stay inside this authority and are never returned. A
    malformed owned document fails closed as a BridgeError instead of being
    half-parsed by a hand-rolled line reader.
    """
    import tomllib

    entries: dict[str, dict[str, Any]] = {}
    try:
        document = tomllib.loads(text)
    except tomllib.TOMLDecodeError as exc:
        raise BridgeError(f"codex configuration is not valid TOML: {exc}")
    servers = document.get("mcp_servers", {})
    if not isinstance(servers, dict):
        return entries
    for name, config in servers.items():
        if not isinstance(name, str) or not isinstance(config, dict):
            continue
        if "url" in config:
            url = config.get("url")
            if isinstance(url, str):
                entries[name] = {"url": url, "headers": {}}
            continue
        command = config.get("command")
        if not isinstance(command, str):
            continue
        args = config.get("args", [])
        env = config.get("env", {})
        if not isinstance(args, list):
            args = []
        if not isinstance(env, dict):
            env = {}
        entries[name] = {
            "command": command,
            "args": [str(item) for item in args if isinstance(item, str)],
            "env": {
                str(key): str(value)
                for key, value in env.items()
                if isinstance(key, str) and isinstance(value, str)
            },
        }
    return entries


def _claude_server_canonical(value: dict[str, Any]) -> dict[str, Any]:
    """Canonical entry of one Claude MCP server value.

    stdio entries canonicalize to command/args/env (type is implied); native
    Streamable HTTP entries (verified real shape: ``type: "http"`` plus
    ``url`` and optional ``headers``) canonicalize to url/headers so URL and
    transport identity participate in fingerprints and ownership.
    """
    server_type = value.get("type")
    if server_type in (None, "stdio"):
        command = value.get("command")
        return {
            "command": command if isinstance(command, str) else None,
            "args": value.get("args", []) if isinstance(value.get("args", []), list) else [],
            "env": value.get("env", {}) if isinstance(value.get("env", {}), dict) else {},
        }
    if server_type == "http":
        url = value.get("url")
        headers = value.get("headers")
        return {
            "url": url if isinstance(url, str) else None,
            "headers": headers if isinstance(headers, dict) else {},
        }
    # Unsupported formats (sse, ws, ...) canonicalize to nothing; they are
    # never claimed as Bridge-owned and never rewritten by this authority.
    return {}


def _claude_json_document(text: str) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    try:
        document = json.loads(text)
    except ValueError:
        raise BridgeError("claude configuration is not valid JSON")
    if not isinstance(document, dict):
        raise BridgeError("claude configuration must be a JSON object")
    servers = document.get("mcpServers")
    if servers is None:
        servers = {}
        document["mcpServers"] = servers
    if not isinstance(servers, dict):
        raise BridgeError("claude configuration mcpServers must be an object")
    entries: dict[str, dict[str, Any]] = {}
    for name, value in servers.items():
        if not isinstance(name, str) or not isinstance(value, dict):
            continue
        entries[name] = _claude_server_canonical(value)
    return entries, document


def _claude_server_value(entry: dict[str, Any]) -> dict[str, Any]:
    """Rendered Claude MCP server value for one canonical entry."""
    if "url" in entry:
        value: dict[str, Any] = {"type": "http", "url": entry["url"]}
        headers = entry.get("headers")
        if isinstance(headers, dict) and headers:
            value["headers"] = {str(key): str(value_item) for key, value_item in headers.items()}
        return value
    value = {"type": "stdio"}
    if isinstance(entry.get("command"), str):
        value["command"] = entry["command"]
    value["args"] = [str(item) for item in entry.get("args", [])]
    value["env"] = {str(key): str(item) for key, item in entry.get("env", {}).items()}
    return value


def _dsh_overlay_entries(path: Path) -> tuple[dict[str, dict[str, Any]], bytes]:
    if not path.is_file():
        return {}, b"[]\n"
    data = path.read_bytes()
    try:
        patches = json.loads(data.decode("utf-8"))
    except ValueError:
        raise BridgeError(f"dsh overlay is not valid JSON: {path}")
    entries: dict[str, dict[str, Any]] = {}
    if isinstance(patches, dict):
        patches = [patches]
    if not isinstance(patches, list):
        raise BridgeError(f"dsh overlay must be a JSON list: {path}")
    for patch in patches:
        if not isinstance(patch, dict):
            continue
        inserted = patch.get("insert")
        if isinstance(inserted, list):
            for item in inserted:
                if not isinstance(item, dict):
                    continue
                config = item.get("config")
                if not isinstance(config, dict):
                    continue
                server_name = config.get("serverName")
                if not isinstance(server_name, str):
                    continue
                transport = config.get("transport")
                if transport == "streamable-http":
                    url = config.get("url")
                    if not isinstance(url, str):
                        continue
                    headers = config.get("headers")
                    entries[server_name] = {
                        "url": url,
                        "headers": headers if isinstance(headers, dict) else {},
                    }
                else:
                    entries[server_name] = {
                        "command": config.get("command"),
                        "args": config.get("args", []),
                        "env": {},
                    }
    return entries, data


def _dsh_overlay_config(entry: dict[str, Any]) -> dict[str, Any]:
    """One dsh-mcp-client plugin config (verified installed plugin schema).

    stdio entries keep transport "stdio" plus command/args/env; native
    Streamable HTTP entries use transport "streamable-http" plus url/headers.
    """
    config: dict[str, Any] = {
        "toolCallTimeoutMs": 300000,
        "failOnStartupError": False,
        "reconnect": {
            "enabled": True,
            "initialDelayMs": 500,
            "maxDelayMs": 30000,
            "maxAttempts": 10,
        },
    }
    if "url" in entry:
        config["transport"] = "streamable-http"
        config["url"] = entry["url"]
        headers = entry.get("headers")
        if isinstance(headers, dict) and headers:
            config["headers"] = {str(key): str(value) for key, value in headers.items()}
    else:
        config["transport"] = "stdio"
        config["command"] = entry.get("command")
        config["args"] = list(entry.get("args", []))
        env = dict(entry.get("env", {}))
        if env:
            config["env"] = {str(key): str(value) for key, value in env.items()}
    return config


def _dsh_overlay_text(entries: dict[str, dict[str, Any]]) -> bytes:
    patches: list[dict[str, Any]] = []
    for server_id in sorted(entries):
        entry = entries[server_id]
        overlay_config = _dsh_overlay_config(entry)
        overlay_config["serverName"] = server_id
        patches.append(
            {
                "insert": [
                    {
                        "id": f"mcp-{server_id}",
                        "name": "@deepseek-ai/dsh-mcp-client",
                        "config": overlay_config,
                    }
                ]
            }
        )
    return (_canonical_json(patches) + "\n").encode("utf-8")


def _cli_claude_scope(config_path: Path) -> str:
    """Real Claude Code scope for a config path (verified CLI option --scope):
    user config is ``~/.claude.json``, project config is ``./.mcp.json``."""
    return "user" if config_path.name == ".claude.json" else "local"


def _cli_get_argv(kind: str, name: str) -> list[str]:
    binary = {"codex": "codex", "claude": "claude"}[kind]
    if kind == CLIENT_KIND_CODEX:
        # Real Codex CLI (verified 0.147): `codex mcp get --json <name>`
        # returns the structured transport object used for fingerprints.
        return [binary, "mcp", "get", "--json", name]
    return [binary, "mcp", "get", name]


def _cli_add_argv(
    kind: str, entry: dict[str, Any], *, scope: str | None = None
) -> list[str]:
    binary = {"codex": "codex", "claude": "claude"}[kind]
    argv = [binary, "mcp", "add"]
    if kind == CLIENT_KIND_CLAUDE and scope:
        argv += ["--scope", scope]
    if "url" in entry:
        # Evidence-backed real syntax:
        #   codex mcp add <name> --url <url>
        #   claude mcp add --transport http <name> <url>
        if kind == CLIENT_KIND_CODEX:
            argv += [entry["name"], "--url", entry["url"]]
        else:
            argv += ["--transport", "http", entry["name"], entry["url"]]
        headers = entry.get("headers")
        if kind == CLIENT_KIND_CLAUDE and isinstance(headers, dict) and headers:
            for key, value in sorted(headers.items()):
                argv += ["--header", f"{key}: {value}"]
        return argv
    if kind == CLIENT_KIND_CLAUDE:
        # Real Claude Code `-e/--env` is variadic (`<env...>`, verified 2.1.267):
        # every following non-option token is consumed as another KEY=VALUE, so a
        # server name placed after the flags is rejected as a malformed env value
        # ("Invalid environment variable format: <name>"). The name therefore
        # precedes the flags, and the `--` terminator ends the variadic run before
        # the command. Codex `--env` stays single-valued.
        env_flags: list[str] = []
        for key, value in sorted(entry.get("env", {}).items()):
            env_flags += ["-e", f"{key}={value}"]
        argv += (
            [entry["name"]] + env_flags + ["--", entry["command"]] + list(entry["args"])
        )
        return argv
    for key, value in sorted(entry.get("env", {}).items()):
        argv += ["--env", f"{key}={value}"]
    argv += [entry["name"], "--", entry["command"]] + list(entry["args"])
    return argv


def _cli_remove_argv(kind: str, name: str, *, scope: str | None = None) -> list[str]:
    binary = {"codex": "codex", "claude": "claude"}[kind]
    argv = [binary, "mcp", "remove", name]
    if kind == CLIENT_KIND_CLAUDE and scope:
        argv += ["--scope", scope]
    return argv


def _cli_list_entry_names(kind: str, output: str) -> list[str]:
    try:
        document = json.loads(output)
    except ValueError:
        document = None
    if isinstance(document, dict):
        servers = document.get("mcpServers")
        if isinstance(servers, list):
            return [
                str(item["name"])
                for item in servers
                if isinstance(item, dict) and isinstance(item.get("name"), str)
            ]
        if isinstance(servers, dict):
            return [str(name) for name in servers if isinstance(name, str)]
        return []
    if isinstance(document, list):
        return [
            str(item.get("name"))
            for item in document
            if isinstance(item, dict) and isinstance(item.get("name"), str)
        ]
    names: list[str] = []
    for line in output.splitlines():
        if not line.strip() or line.startswith((" ", "\t", "-", "*")):
            continue
        name = line.split(":", 1)[0].strip().split()[0].strip()
        if name:
            names.append(name)
    return names


def _cli_canonicalize_flat(candidate: dict[str, Any]) -> dict[str, Any] | None:
    """Canonical entry from one flat JSON server object (stdio or http)."""
    server_type = candidate.get("type")
    if server_type in (None, "stdio") and "command" in candidate:
        command = candidate.get("command")
        args = candidate.get("args", [])
        env = candidate.get("env", {})
        if not isinstance(command, str) or not isinstance(args, list) or not isinstance(env, dict):
            return None
        return {
            "command": command,
            "args": [str(item) for item in args],
            "env": {str(key): str(value) for key, value in env.items()},
        }
    if server_type in ("http", "streamable-http", "streamable_http"):
        url = candidate.get("url")
        if not isinstance(url, str):
            return None
        headers = candidate.get("headers")
        return {
            "url": url,
            "headers": headers if isinstance(headers, dict) else {},
        }
    return None


def _cli_canonicalize_codex(candidate: dict[str, Any]) -> dict[str, Any] | None:
    """Canonical entry from the real Codex ``mcp get --json`` transport object.

    Verified shape nests the transport: stdio carries command/args/env (with
    legacy env_vars merged) and streamable HTTP carries url.
    """
    transport = candidate.get("transport")
    if not isinstance(transport, dict):
        return None
    transport_type = transport.get("type")
    if transport_type in ("http", "streamable-http", "streamable_http"):
        url = transport.get("url")
        if not isinstance(url, str):
            return None
        return {"url": url, "headers": {}}
    if transport_type in (None, "stdio"):
        command = transport.get("command")
        if not isinstance(command, str):
            return None
        args = transport.get("args", [])
        env = transport.get("env")
        if not isinstance(args, list):
            return None
        merged_env: dict[str, str] = {}
        if isinstance(env, dict):
            for key, value in env.items():
                if isinstance(key, str) and isinstance(value, str):
                    merged_env[str(key)] = value
        env_vars = transport.get("env_vars", [])
        if isinstance(env_vars, list):
            for item in env_vars:
                if isinstance(item, dict) and isinstance(item.get("name"), str) and isinstance(item.get("value"), str):
                    merged_env[item["name"]] = item["value"]
        return {
            "command": command,
            "args": [str(item) for item in args],
            "env": merged_env,
        }
    return None


def _cli_parse_claude_text(output: str, name: str) -> dict[str, Any] | None:
    """Best-effort canonical parse of the real Claude Code ``mcp get`` text.

    The native Streamable HTTP block is exact (single-line ``URL:`` and
    ``Type: http``); stdio text (space-joined args, free-form env) cannot be
    reconstructed losslessly, so it returns None and removal stays
    conservative drift.
    """
    lines = output.splitlines()
    if lines and lines[0].strip().rstrip(":").strip() != name:
        return None
    server_type: str | None = None
    url: str | None = None
    for raw in lines[1:]:
        line = raw.strip()
        if not line:
            continue
        if line.startswith("Type:"):
            server_type = line[len("Type:"):].strip().lower()
        elif line.startswith("URL:"):
            url = line[len("URL:"):].strip()
    if server_type in ("http", "streamable-http", "streamable_http") and url:
        return {"url": url, "headers": {}}
    return None


def _cli_get_entry(output: str, name: str) -> dict[str, Any] | None:
    """Parse one official `get` result into a canonical entry.

    Accepts the structured JSON shapes of the hermetic fixtures and of the
    real Codex CLI (``mcp get --json``), plus the exact native-HTTP text block
    of the real Claude Code CLI. A vendor format that cannot be parsed returns
    None so cleanup stays conservative.
    """
    try:
        document = json.loads(output)
    except ValueError:
        document = None
    if isinstance(document, dict):
        candidate: dict[str, Any] | None = None
        if document.get("name") == name or "command" in document or "url" in document:
            candidate = document
        else:
            for _key, value in document.items():
                if isinstance(value, dict) and value.get("name") == name:
                    candidate = value
                    break
        if candidate is None:
            return None
        if isinstance(candidate.get("transport"), dict):
            return _cli_canonicalize_codex(candidate)
        return _cli_canonicalize_flat(candidate)
    return _cli_parse_claude_text(output, name)


def _copy_document_entry(entry: dict[str, Any]) -> dict[str, Any]:
    """Canonical entry copy for document writes (stdio or Streamable HTTP)."""
    if "url" in entry:
        return {"url": entry["url"], "headers": dict(entry.get("headers", {}))}
    return {
        "command": entry.get("command"),
        "args": list(entry.get("args", [])),
        "env": dict(entry.get("env", {})),
    }


def _adapter_read_entries(
    *,
    kind: str,
    mode: str,
    config_path: Path,
    launcher_command: str,
    launcher_args: list[str],
    relay_base_url: str = "",
    stdio_http_endpoints: dict[str, str] | None = None,
    unverifiable: set[str] | None = None,
) -> dict[str, dict[str, Any]]:
    """Current bridge-managed MCP entries in the target configuration.

    Streamable HTTP entries are Bridge-owned only when their URL equals the
    deterministic relay URL the bridge projects for that exact server name on
    this environment's persisted relay base, or the explicitly configured
    stdio-to-HTTP facade endpoint mapped to that name, so name-aware
    ownership is used.

    ``unverifiable`` (official-CLI mode only) collects every *listed* name that
    could not be positively established as Bridge-owned: the official ``get``
    failed, returned an unparseable document, or parsed to a server that does
    not match this environment's ownership evidence. Reconcile treats those as
    occupied names and never blindly adds or overwrites them.
    """
    if mode == ADAPTER_BRIDGE_FILE:
        if kind == CLIENT_KIND_CODEX:
            if not config_path.exists():
                return {}
            return _codex_owned_document_entries(
                config_path.read_text(encoding="utf-8")
            )
        if kind == CLIENT_KIND_CLAUDE:
            if not config_path.exists():
                return {}
            entries, _document = _claude_json_document(
                config_path.read_text(encoding="utf-8")
            )
            return {
                name: entry
                for name, entry in entries.items()
                if _looks_bridge_owned(
                    entry,
                    launcher_command=launcher_command,
                    launcher_args=launcher_args,
                    name=name,
                    relay_base_url=relay_base_url,
                    stdio_http_endpoints=stdio_http_endpoints,
                )
            }
        if kind == CLIENT_KIND_DSH:
            entries, _data = _dsh_overlay_entries(config_path)
            return {
                name: entry
                for name, entry in entries.items()
                if _looks_bridge_owned(
                    entry,
                    launcher_command=launcher_command,
                    launcher_args=launcher_args,
                    name=name,
                    relay_base_url=relay_base_url,
                    stdio_http_endpoints=stdio_http_endpoints,
                )
            }
        return {}
    if mode == ADAPTER_OFFICIAL_CLI:
        binary = {"codex": "codex", "claude": "claude"}[kind]
        code, out, err = _run_tool([binary, "mcp", "list"])
        if code != 0:
            raise BridgeError(f"{binary} mcp list failed: {err.strip()}")
        names = _cli_list_entry_names(kind, out)
        entries: dict[str, dict[str, Any]] = {}
        for name in names:
            code, out, _err = _run_tool(_cli_get_argv(kind, name))
            if code != 0:
                # listed but its state cannot be read: never treat as missing
                if unverifiable is not None:
                    unverifiable.add(name)
                continue
            entry = _cli_get_entry(out, name)
            if entry is None:
                if unverifiable is not None:
                    unverifiable.add(name)
                continue
            if not _looks_bridge_owned(
                entry,
                launcher_command=launcher_command,
                launcher_args=launcher_args,
                name=name,
                relay_base_url=relay_base_url,
                stdio_http_endpoints=stdio_http_endpoints,
            ):
                # parsed to a server this environment does not own: occupied
                if unverifiable is not None:
                    unverifiable.add(name)
                continue
            entries[name] = entry
        return entries
    raise BridgeError(f"no supported apply adapter for {mode}")


def _adapter_apply(
    *,
    kind: str,
    mode: str,
    config_path: Path,
    desired: list[dict[str, Any]],
    obsolete: list[dict[str, Any]],
    dry_run: bool,
    previous_entries: dict[str, dict[str, Any]] | None = None,
    verified_transition: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    """Apply desired entries and verified obsolete removals to one config."""
    actions: list[dict[str, Any]] = []
    if dry_run:
        for entry in desired:
            actions.append(
                {"action": "add-or-update", "serverId": entry["name"], "dryRun": True}
            )
        for item in obsolete:
            actions.append(
                {"action": "remove", "serverId": item["name"], "dryRun": True}
            )
        return actions
    if mode == ADAPTER_OFFICIAL_CLI:
        return _adapter_apply_official_cli(
            kind,
            desired,
            obsolete,
            config_path=config_path,
            previous_entries=previous_entries or {},
        )
    if mode == ADAPTER_BRIDGE_FILE:
        return _adapter_apply_file(kind, config_path, desired, obsolete, verified_transition=verified_transition)
    raise BridgeError(f"no supported apply adapter for {mode}")


def _adapter_apply_official_cli(
    kind: str,
    desired: list[dict[str, Any]],
    obsolete: list[dict[str, Any]],
    *,
    config_path: Path,
    previous_entries: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    """Apply through the client's own official mutation command.

    Add/remove argv are evidence-backed real syntax (Codex ``--url`` /
    ``--env``, Claude ``--transport http`` / ``-e`` / ``--scope``). A replace
    (transport transition) is remove-then-add: if the add fails after a
    successful remove, the previous entry is re-added best-effort so the
    transition rolls back instead of leaving the name absent.
    """
    actions: list[dict[str, Any]] = []
    scope = _cli_claude_scope(config_path) if kind == CLIENT_KIND_CLAUDE else None
    label = "codex" if kind == CLIENT_KIND_CODEX else "claude"
    for item in obsolete:
        name = item["name"]
        expected = _entry_fingerprint(item["entry"])
        code, out, _err = _run_tool(_cli_get_argv(kind, name))
        if code != 0:
            actions.append(
                {
                    "action": "drift",
                    "serverId": name,
                    "detail": "get failed; not removed",
                }
            )
            continue
        current = _cli_get_entry(out, name)
        if current is None or _entry_fingerprint(current) != expected:
            actions.append(
                {
                    "action": "drift",
                    "serverId": name,
                    "detail": "fingerprint mismatch; not removed",
                }
            )
            continue
        code, _out, err = _run_tool(_cli_remove_argv(kind, name, scope=scope))
        if code != 0:
            raise BridgeError(f"{label} mcp remove failed for {name}: {err.strip()}")
        actions.append({"action": "remove", "serverId": name})
    for entry in desired:
        name = entry["name"]
        argv = _cli_add_argv(kind, entry, scope=scope)
        replaced_previous: dict[str, Any] | None = None
        if entry.get("replace"):
            replaced_previous = previous_entries.get(name)
            code, _out, err = _run_tool(_cli_remove_argv(kind, name, scope=scope))
            if code != 0:
                raise BridgeError(
                    f"{label} mcp remove failed during replace for {name}: {err.strip()}"
                )
        code, _out, err = _run_tool(argv)
        if code != 0:
            if replaced_previous is not None:
                restore = _cli_add_argv(
                    kind, {"name": name, **replaced_previous}, scope=scope
                )
                _run_tool(restore)
            raise BridgeError(
                f"{label} mcp add failed for {name}: {err.strip()} "
                "(replaced entry was restored best-effort)"
            )
        actions.append({"action": "add-or-update", "serverId": name})
    return actions


def _adapter_apply_file(
    kind: str,
    config_path: Path,
    desired: list[dict[str, Any]],
    obsolete: list[dict[str, Any]],
    *, verified_transition: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    """Rewrite a Bridge-owned configuration document atomically with rollback.

    Claude documents are shared with the user: only Bridge-owned keys are
    removed or rewritten, every other mcpServers value (including unmanaged
    ``type: http`` servers and their headers) and every non-mcpServers field is
    preserved verbatim. Codex owned documents and DSH overlays are wholly
    Bridge-owned and are regenerated.
    """
    actions: list[dict[str, Any]] = []
    previous = config_path.read_bytes() if config_path.exists() else b""
    rendered: bytes | None = None
    if kind == CLIENT_KIND_CODEX:
        current = (
            _codex_owned_document_entries(previous.decode("utf-8", errors="replace"))
            if previous
            else {}
        )
        for item in obsolete:
            name = item["name"]
            if name not in current:
                continue
            if _entry_fingerprint(current[name]) == _entry_fingerprint(item["entry"]):
                current.pop(name, None)
                actions.append({"action": "remove", "serverId": name})
            else:
                actions.append(
                    {
                        "action": "drift",
                        "serverId": name,
                        "detail": "document fingerprint mismatch; not removed",
                    }
                )
        for entry in desired:
            current[entry["name"]] = _copy_document_entry(entry)
            actions.append({"action": "add-or-update", "serverId": entry["name"]})
        rendered = _codex_owned_document_text(current).encode("utf-8")
    elif kind == CLIENT_KIND_CLAUDE:
        if previous:
            document = json.loads(previous.decode("utf-8"))
            if not isinstance(document, dict):
                raise BridgeError("claude configuration must be a JSON object")
        else:
            document = {}
        servers = document.get("mcpServers")
        if servers is None:
            servers = {}
            document["mcpServers"] = servers
        if not isinstance(servers, dict):
            raise BridgeError("claude configuration mcpServers must be an object")
        canonical, _document = _claude_json_document(previous.decode("utf-8")) if previous else ({}, document)
        for item in obsolete:
            name = item["name"]
            if name not in canonical:
                continue
            if _entry_fingerprint(canonical[name]) == _entry_fingerprint(item["entry"]):
                servers.pop(name, None)
                canonical.pop(name, None)
                actions.append({"action": "remove", "serverId": name})
            else:
                actions.append(
                    {
                        "action": "drift",
                        "serverId": name,
                        "detail": "document fingerprint mismatch; not removed",
                    }
                )
        for entry in desired:
            servers[entry["name"]] = _claude_server_value(entry)
            actions.append({"action": "add-or-update", "serverId": entry["name"]})
        rendered = json.dumps(document, ensure_ascii=False, indent=2).encode("utf-8") + b"\n"
    elif kind == CLIENT_KIND_DSH:
        current, _data = _dsh_overlay_entries(config_path)
        for item in obsolete:
            name = item["name"]
            if name not in current:
                continue
            if _entry_fingerprint(current[name]) == _entry_fingerprint(item["entry"]):
                current.pop(name, None)
                actions.append({"action": "remove", "serverId": name})
            else:
                actions.append(
                    {
                        "action": "drift",
                        "serverId": name,
                        "detail": "document fingerprint mismatch; not removed",
                    }
                )
        for entry in desired:
            current[entry["name"]] = _copy_document_entry(entry)
            actions.append({"action": "add-or-update", "serverId": entry["name"]})
        rendered = _dsh_overlay_text(current)
    else:
        raise BridgeError(f"no bridge-file adapter for {kind}")
    assert rendered is not None
    try:
        _atomic_replace_bytes(config_path, rendered)
        readback = config_path.read_bytes()
        if readback != rendered:
            raise BridgeError(f"{kind} configuration readback mismatch: {config_path}")
    except BaseException:
        if previous:
            _atomic_replace_bytes(config_path, previous)
        raise
    return actions




# ---- Peer mirror sync -----------------------------------------------------

def _projection_facts_fingerprint(facts: list[tuple[str, str, str]]) -> str:
    return _fingerprint(
        [[server_id, name, transport] for server_id, name, transport in sorted(facts)]
    )


def _fetch_peer_projection_facts(
    *,
    source: str,
    side: str,
    peer_registry: Path | None,
    local_host: str,
    local_port: int,
) -> tuple[list[tuple[str, str, str]], str]:
    """Deterministic fetch of the peer registry's projection-relevant facts.

    source='registry-path' reads an explicit peer registry database (offline
    role simulation and single-host Operator runs); source='registry-remote'
    queries the peer registry through this host's live local node, which is how
    a deployment refreshes without a shared database.
    """
    del side
    if source == "registry-path":
        if peer_registry is None or not peer_registry.is_file():
            raise BridgeError("projection sync requires an existing --peer-registry path")
        connection = sqlite3.connect(peer_registry, timeout=5)
        try:
            connection.execute("PRAGMA busy_timeout = 5000")
            connection.execute("PRAGMA query_only = ON")
            version = int(connection.execute("PRAGMA user_version").fetchone()[0])
            if version < 4:
                raise BridgeError(
                    f"peer registry schema version {version} lacks typed transport data; migrate it first"
                )
            rows = connection.execute(
                "SELECT id, name, transport_json FROM servers "
                "WHERE enabled = 1 ORDER BY id"
            ).fetchall()
        finally:
            connection.close()
        facts = [
            (
                str(row[0]),
                str(row[1]),
                str(json.loads(row[2]).get("type", "stdio")),
            )
            for row in rows
        ]
        return facts, f"registry-path:{peer_registry}"
    if source == "registry-remote":
        summaries = local_registry_query(
            local_host, local_port, "remote", "list", {}
        )
        if not isinstance(summaries, list):
            raise BridgeError("peer registry list returned an invalid response")
        facts = []
        for summary in summaries:
            if not isinstance(summary, dict):
                continue
            server_id = summary.get("id")
            name = summary.get("name")
            transport = summary.get("transport", {})
            transport_type = (
                transport.get("type", "stdio")
                if isinstance(transport, dict)
                else "stdio"
            )
            if (
                isinstance(server_id, str)
                and ID_PATTERN.fullmatch(server_id)
                and transport_type in {"stdio", "streamable-http"}
            ):
                facts.append(
                    (
                        server_id,
                        name if isinstance(name, str) else server_id,
                        transport_type,
                    )
                )
        return facts, f"registry-remote:{local_host}:{local_port}"
    raise BridgeError(f"unknown projection refresh source: {source!r}")


def projection_sync_peer(
    *,
    projection: Path,
    source: str,
    side: str,
    peer_registry: Path | None = None,
    local_host: str = "127.0.0.1",
    local_port: int | None = None,
) -> dict[str, Any]:
    """Refresh the peer mirror and append an outbox event on change."""
    facts, source_label = _fetch_peer_projection_facts(
        source=source,
        side=side,
        peer_registry=peer_registry,
        local_host=local_host,
        local_port=int(local_port) if local_port is not None else (8769 if side == "wsl" else 8768),
    )
    new_fingerprint = _projection_facts_fingerprint(facts)
    ProjectionDatabase.ensure(projection)
    database = ProjectionDatabase(projection)
    with database._connect(write=True) as connection:
        old_rows = database.peer_rows(connection)
        old_facts = [
            (str(row["server_id"]), str(row["name"]), str(row["transport"]))
            for row in old_rows
        ]
        old_fingerprint = _projection_facts_fingerprint(old_facts)
        changed = old_fingerprint != new_fingerprint
        revision = database.current_revision(connection)
        if changed:
            connection.execute("DELETE FROM peer_projection_state")
            for server_id, name, transport in facts:
                connection.execute(
                    "INSERT INTO peer_projection_state "
                    "(server_id, name, transport, enabled, revision) "
                    "VALUES (?, ?, ?, 1, ?)",
                    (server_id, name, transport, revision + 1),
                )
            revision = database.append_outbox(
                connection,
                "peer_state_changed",
                detail=_canonical_json({"servers": [item[0] for item in facts]}),
            )
            database.set_meta(connection, "peer_fingerprint", new_fingerprint)
            database.set_meta(connection, "peer_source", source_label)
            database.set_meta(connection, "peer_observed_at_ns", str(time.time_ns()))
        else:
            database.set_meta(connection, "peer_source", source_label)
    return {
        "ok": True,
        "source": source_label,
        "changed": changed,
        "revision": revision,
        "serverCount": len(facts),
        "servers": [item[0] for item in facts],
        "fingerprint": new_fingerprint,
    }


def _environment_summary(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "environmentId": row["environment_id"],
        "clientKind": row["client_kind"],
        "hostSide": row["host_side"],
        "scope": row["scope"],
        "configPath": row["config_path"],
        "discoverySource": row["discovery_source"],
        "enabled": bool(row["enabled"]),
        "applyAdapter": row["apply_adapter"],
        "launcherCommand": row["launcher_command"],
        "launcherArgs": json.loads(row["launcher_args_json"]),
        "fileSha256": row["file_sha256"],
        "fileSize": row["file_size"],
        "fileMtimeNs": row["file_mtime_ns"],
        "transportCapabilities": json.loads(row["transport_capabilities_json"]),
        "compatibilityRoute": row["compatibility_route"],
        "toolExposure": _env_row_field(row, "tool_exposure", "native"),
        "harnessVerification": _row_harness_verification(row),
        "relayBaseUrl": _env_row_field(row, "relay_base_url", ""),
        "stdioHttpEndpoints": _row_stdio_http_endpoints(row),
        "confirmedAtNs": row["confirmed_at_ns"],
    }


def projection_enroll(
    *,
    projection: Path,
    side: str,
    candidate_id: str,
    extra_paths: tuple[Path, ...],
    launcher_command: str | None,
    launcher_args: list[str] | None,
    confirm: bool,
    transport_capabilities: dict[str, bool] | None = None,
    compatibility_route: str = "native",
    relay_base_url: str | None = None,
    stdio_http_endpoints: str | None = None,
) -> dict[str, Any]:
    """Enroll exactly one revalidated scanned candidate.

    ``relay_base_url`` is the Operator-selected loopback base URL of *this
    host's* native Streamable HTTP relay (``serve --http-relay-port``); it is
    never derived from the registry or the peer. It is required exactly when
    the environment records native ``streamable-http`` capability with the
    native route, optional for the ``stdio-to-http`` route (native HTTP peers
    then still reach the Agent through this host's relay), and the persisted
    value is validated/normalized to ``http(s)://<loopback>:<port>``.

    ``stdio_http_endpoints`` (``stdio-to-http`` route only) is the explicit
    per-target facade mapping: inline JSON ``{"id": "http://127.0.0.1:P/mcp"}``
    or the path of a local file containing it. Values are validated loopback
    /mcp URLs; facade provisioning and readiness are the Operator's explicit
    responsibility, reported as configured and never probed live.
    """
    if not confirm:
        raise BridgeError(
            "enrollment is a mutation: select the candidate id, revalidate the "
            "fresh scan output, and pass --confirm"
        )
    candidates = projection_scan_candidates(side, extra_paths=extra_paths)
    match: dict[str, Any] | None = None
    for candidate in candidates:
        if candidate["candidateId"] == candidate_id:
            match = candidate
            break
    if match is None:
        raise BridgeError(
            f"candidate {candidate_id} is stale or not from this scan; "
            "re-run projection scan and select a current candidate id"
        )
    if match["conflicts"]:
        raise BridgeError(
            f"candidate {candidate_id} has blocking conflicts: "
            + ", ".join(match["conflicts"])
        )
    if not match["exists"]:
        onboarding_owned = (
            match["clientKind"] == CLIENT_KIND_CODEX
            or (
                match["clientKind"] == CLIENT_KIND_DSH
                and Path(match["configPath"]).name == "cordis-bridge-overlay.json"
            )
        )
        if not onboarding_owned:
            raise BridgeError(
                f"candidate {candidate_id} refers to a missing configuration that "
                "is not a Bridge-created owned document: " + match["configPath"]
            )
    kind = match["clientKind"]
    config_path = Path(match["configPath"])
    effective_launcher, effective_launcher_args = _default_launcher(side)
    if launcher_command is not None:
        effective_launcher = launcher_command
    if launcher_args is not None:
        effective_launcher_args = list(launcher_args)
    _validate_launcher(side, effective_launcher, effective_launcher_args)
    mode = _resolve_adapter_mode(
        kind,
        config_path,
        launcher_command=effective_launcher,
        launcher_args=effective_launcher_args,
    )
    capabilities = (
        {"stdio": True, "streamable-http": False}
        if transport_capabilities is None
        else dict(transport_capabilities)
    )
    if set(capabilities) != {"stdio", "streamable-http"} or not all(
        isinstance(value, bool) for value in capabilities.values()
    ):
        raise BridgeError(
            "transport capabilities must contain boolean stdio and streamable-http values"
        )
    if compatibility_route not in {
        "native",
        "http-to-stdio",
        "stdio-to-http",
        "constant-two-tool",
    }:
        raise BridgeError("unknown compatibility route")
    if compatibility_route == "http-to-stdio" and not capabilities["stdio"]:
        raise BridgeError("http-to-stdio conversion requires stdio client support")
    if compatibility_route == "stdio-to-http" and not capabilities["streamable-http"]:
        raise BridgeError("stdio-to-http conversion requires streamable-http client support")
    effective_endpoints: dict[str, str] = {}
    if stdio_http_endpoints is not None and stdio_http_endpoints.strip():
        effective_endpoints = _parse_stdio_http_endpoints(stdio_http_endpoints)
    if effective_endpoints and compatibility_route != "stdio-to-http":
        raise BridgeError(
            "--stdio-http-endpoints is only meaningful with the stdio-to-http "
            "compatibility route"
        )
    effective_relay_base = ""
    if relay_base_url is not None:
        if not (
            capabilities["streamable-http"]
            and compatibility_route in {"native", "stdio-to-http"}
        ):
            raise BridgeError(
                "--relay-url is only meaningful when streamable-http capability "
                "is enrolled with the native or stdio-to-http compatibility route"
            )
        # The Operator-selected loopback relay base of this host; no
        # peer/registry endpoint is ever used.
        effective_relay_base = _validate_relay_base_url(relay_base_url)
    elif capabilities["streamable-http"] and compatibility_route == "native":
        # Native HTTP projection requires the Operator-selected loopback relay
        # base of this host; no peer/registry endpoint is ever used.
        raise BridgeError(
            "native Streamable HTTP enrollment requires --relay-url "
            "(the loopback base URL of this host's serve --http-relay-port)"
        )
    ProjectionDatabase.ensure(projection)
    database = ProjectionDatabase(projection)
    environment_id = "env-" + secrets.token_hex(12)
    now = time.time_ns()
    with database._connect(write=True) as connection:
        existing = connection.execute(
            "SELECT environment_id FROM agent_environments "
            "WHERE client_kind = ? AND config_path = ?",
            (kind, str(config_path)),
        ).fetchone()
        if existing is not None:
            raise BridgeError(
                f"an environment for {kind} at {config_path} is already enrolled "
                f"({existing['environment_id']}); unenroll it first"
            )
        connection.execute(
            "INSERT INTO agent_environments ("
            "environment_id, client_kind, host_side, scope, config_path, "
            "discovery_source, enabled, launcher_command, launcher_args_json, "
            "apply_adapter, file_sha256, file_size, file_mtime_ns, "
            "may_contain_secrets, transport_capabilities_json, compatibility_route, "
            "relay_base_url, stdio_http_endpoints_json, "
            "confirmed_at_ns, created_at_ns, updated_at_ns"
            ") VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                environment_id,
                kind,
                side,
                match["scope"],
                str(config_path),
                match["discoverySource"],
                effective_launcher,
                _canonical_json(effective_launcher_args),
                mode,
                match["fileSha256"],
                match["size"],
                match["mtimeNs"],
                1 if match["mayContainSecrets"] else 0,
                _canonical_json(capabilities),
                compatibility_route,
                effective_relay_base,
                _canonical_json(effective_endpoints),
                now,
                now,
                now,
            ),
        )
        database.append_outbox(
            connection,
            "enrollment_changed",
            environment_id=environment_id,
            detail=f"enrolled {kind} environment {environment_id}",
        )
    row = database.environment_row(
        database._connect(), environment_id
    )
    return {
        "ok": True,
        "environment": _environment_summary(row) if row is not None else {},
        "adapterUnavailable": mode == ADAPTER_UNAVAILABLE,
        "note": (
            "enrollment persisted; reconcile applies the peer registry. "
            "No environment is enrolled automatically from a scan result."
        ),
    }


def projection_probe_client(
    *, projection: Path, environment_id: str, probe_file: Path,
    aspect: str = "refresh", confirm: bool = False, dry_run: bool = False,
) -> dict[str, Any]:
    """Prepare a bound harmless probe; the Bridge Agent owns target execution.

    Preparing never invokes a model, changes a target client profile, or assumes
    any client feature from its name. Only confirmed local challenge state is
    written. The target runs the fixture in an explicitly authorized session.
    ``aspect`` selects which registry decision the prepared challenge is meant to
    establish; the fixture observes the whole environment either way.
    """
    from harness_verification import create_probe
    if aspect not in CLIENT_CAPABILITY_ASPECTS:
        raise BridgeError(
            "unknown client capability aspect; choose one of: "
            + ", ".join(sorted(CLIENT_CAPABILITY_ASPECTS))
        )
    if not CLIENT_CAPABILITY_ASPECTS[aspect]["runnable"]:
        raise BridgeError(
            "aspect " + aspect + " is not observable with this probe version: "
            + " ".join(CLIENT_CAPABILITY_ASPECTS[aspect]["agentSteps"])
        )
    if not dry_run and not confirm:
        raise BridgeError("preparing client verification requires --confirm or --dry-run")
    database = ProjectionDatabase(projection, observe_only=dry_run)
    with database._connect() as connection:
        row = database.environment_row(connection, environment_id)
    if row is None or not row["enabled"]:
        raise BridgeError("client verification requires an enabled enrolled environment")
    fingerprint = _harness_config_fingerprint(row)
    if not Path(row["config_path"]).is_file() and not _adapter_file_mode_supported(
        row["client_kind"], Path(row["config_path"])
    ):
        raise BridgeError("missing client configuration is not a supported owned-file enrollment")
    challenge = create_probe({
        "environmentId": environment_id, "clientKind": row["client_kind"],
        "configFingerprint": fingerprint,
    })
    probe_file = probe_file.expanduser().resolve()
    if probe_file.exists():
        raise BridgeError("probe destination already exists; choose a new file")
    receipt_file = probe_file.with_name(probe_file.name + ".receipt.json")
    if receipt_file.exists():
        raise BridgeError("probe receipt destination already exists")
    if not dry_run:
        probe_file.parent.mkdir(parents=True, exist_ok=True)
        with database._connect(write=True) as connection:
            connection.execute("BEGIN IMMEDIATE")
            if _harness_config_fingerprint(row) != fingerprint:
                raise BridgeError("client configuration changed while preparing verification")
            # O_EXCL preserves any concurrently created local file. A failed
            # database commit leaves an inert challenge, never a capability.
            descriptor = os.open(probe_file, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
                json.dump(challenge, handle, ensure_ascii=False, sort_keys=True)
                handle.flush()
                os.fsync(handle.fileno())
            database.set_meta(
                connection, _PROBE_META_PREFIX + environment_id + ":" + aspect,
                _canonical_json({
                    "aspect": aspect, "challenge": challenge,
                    "probeFile": str(probe_file), "receiptFile": str(receipt_file),
                    "preparedAtNs": time.time_ns(),
                }),
            )
            database.record_history(
                connection, environment_id=environment_id,
                revision=database.current_revision(connection), action="client_probe",
                ok=True, detail="prepared bound client probe " + challenge["probeId"]
                + " for aspect " + aspect,
            )
    definition = CLIENT_CAPABILITY_ASPECTS[aspect]
    record_command = [
        "projection", "record-client-verification", environment_id,
        "--receipt", str(receipt_file), "--aspect", aspect,
        "--tool-exposure", "auto", "--confirm",
    ]
    return {
        "ok": True, "dryRun": dry_run, "environmentId": environment_id,
        "aspect": aspect, "capability": definition["capability"],
        "observation": definition["observation"],
        "registryField": definition["registryField"],
        "decision": definition["decision"],
        "probeId": challenge["probeId"], "probeFile": str(probe_file),
        "receiptFile": str(receipt_file),
        "fixture": {"command": sys.executable, "args": [
            str(Path(__file__).resolve().with_name("harness_verification.py")),
            "--probe", str(probe_file),
            "--receipt", str(receipt_file),
        ]},
        "agentSteps": list(definition["agentSteps"]),
        "recordCommand": record_command,
        "next": "Bridge Agent: run this harmless fixture in an authorized isolated target Harness "
                "session, then record the receipt with record-client-verification "
                "(same Bridge CLI entry point as this command). Preparation does not run the "
                "target or verify its capabilities, and this probe never measures a business "
                "MCP's tool count or token cost.",
    }


def projection_record_client_verification(
    *, projection: Path, environment_id: str, receipt_file: Path,
    tool_exposure: str = "auto", aspect: str | None = None,
    confirm: bool = False, dry_run: bool = False,
) -> dict[str, Any]:
    """Validate and consume one probe receipt before recording client evidence.

    One receipt observes the whole environment, so ``aspect`` only selects which
    prepared challenge is consumed.  When it is omitted, a single outstanding
    challenge is consumed and several require an explicit choice.
    """
    from harness_verification import validate_probe_receipt
    if not dry_run and not confirm:
        raise BridgeError("recording client verification requires --confirm or --dry-run")
    if tool_exposure not in {"native", "auto", "deferred"}:
        raise BridgeError("unknown tool exposure mode")
    if aspect is not None and aspect not in CLIENT_CAPABILITY_ASPECTS:
        raise BridgeError(
            "unknown client capability aspect; choose one of: "
            + ", ".join(sorted(CLIENT_CAPABILITY_ASPECTS))
        )
    if not receipt_file.is_file() or receipt_file.stat().st_size > 1024 * 1024:
        raise BridgeError("verification receipt must be a bounded regular file")
    receipt = json.loads(receipt_file.read_text(encoding="utf-8"))
    database = ProjectionDatabase(projection, observe_only=dry_run)
    with database._connect(write=not dry_run) as connection:
        if not dry_run:
            connection.execute("BEGIN IMMEDIATE")
        row = database.environment_row(connection, environment_id)
        if row is None or not row["enabled"]:
            raise BridgeError("client verification requires an enabled enrolled environment")
        records = _probe_records(connection, environment_id)
        outstanding = _outstanding_client_probes(connection, environment_id)
        if not records:
            raise BridgeError("no unconsumed client probe for this environment")
        if aspect is None:
            if len(records) > 1:
                raise BridgeError(
                    "several unconsumed client probes; select one with --aspect: "
                    + ", ".join(sorted(records))
                )
            aspect = next(iter(records))
        record = records.get(aspect)
        if record is None:
            raise BridgeError(
                "no unconsumed client probe for aspect " + aspect + "; outstanding: "
                + ", ".join(sorted(records))
            )
        if outstanding[aspect]["expired"]:
            raise BridgeError(
                "the prepared client probe for aspect " + aspect
                + " expired; prepare a fresh one"
            )
        challenge = record["challenge"]
        evidence = validate_probe_receipt(challenge, receipt)
        if (
            evidence.get("environmentId") != environment_id
            or evidence.get("clientKind") != row["client_kind"]
            or evidence.get("configFingerprint") != _harness_config_fingerprint(row)
        ):
            raise BridgeError("verification does not match the current enrolled client configuration")
        updated = dict(row)
        updated["tool_exposure"] = tool_exposure
        updated["harness_verification_json"] = _canonical_json(evidence)
        effective = _effective_tool_exposure(updated)
        if effective == "deferred" and (
            row["compatibility_route"] != "native"
            or not json.loads(row["transport_capabilities_json"]).get("stdio")
        ):
            if tool_exposure == "deferred":
                raise BridgeError("deferred exposure requires the native stdio route")
            effective = "native"
        if not dry_run:
            connection.execute(
                "UPDATE agent_environments SET tool_exposure = ?, "
                "harness_verification_json = ?, updated_at_ns = ? WHERE environment_id = ?",
                (tool_exposure, updated["harness_verification_json"], time.time_ns(), environment_id),
            )
            connection.execute("DELETE FROM projection_meta WHERE key = ?", (record["metaKey"],))
            database.append_outbox(
                connection, "enrollment_changed", environment_id=environment_id,
                detail="recorded client probe " + evidence["probeId"],
            )
        remaining = [
            item for key, item in _outstanding_client_probes(connection, environment_id).items()
            if key != aspect
        ]
    definition = CLIENT_CAPABILITY_ASPECTS.get(aspect, {})
    capability = definition.get("capability")
    capabilities = evidence.get("capabilities", {})
    capabilities = capabilities if isinstance(capabilities, dict) else {}
    return {
        "ok": True, "dryRun": dry_run, "environmentId": environment_id,
        "aspect": aspect, "capability": capability,
        "capabilityValue": capabilities.get(capability) if capability else None,
        "toolExposure": tool_exposure, "effectiveToolExposure": effective,
        "verification": evidence, "configurationApplied": False,
        "outstandingChallenges": sorted(remaining, key=lambda item: str(item["aspect"])),
    }


def projection_unenroll(
    *,
    projection: Path,
    environment_id: str,
    remove_entries: bool,
    confirm: bool,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Stop synchronizing an environment, optionally removing owned entries."""
    if not confirm:
        raise BridgeError(
            "unenrollment is a mutation: pass --confirm with the chosen plan "
            "(--keep-entries to stop sync, --remove-entries to also delete "
            "matching Bridge-owned entries)"
        )
    ProjectionDatabase.ensure(projection)
    database = ProjectionDatabase(projection)
    with database._connect() as connection:
        row = database.environment_row(connection, environment_id)
        if row is None:
            raise BridgeError(f"unknown environment: {environment_id}")
        summary = _environment_summary(row)
    if dry_run:
        return {"ok": True, "environment": summary, "dryRun": True}
    actions: list[dict[str, Any]] = []
    if remove_entries:
        entries = _projection_current_entries(database, row)
        kind = str(row["client_kind"])
        with database._connect() as connection:
            stored = {
                str(item["server_id"]): item
                for item in database.projection_rows(connection, environment_id)
            }
        obsolete: list[dict[str, Any]] = []
        drifted: list[str] = []
        for name, entry in sorted(entries.items()):
            stored_row = stored.get(name)
            if stored_row is None or (
                _entry_fingerprint_for_kind(kind, entry)
                != str(stored_row["entry_fingerprint"])
            ):
                drifted.append(name)
                continue
            obsolete.append({"name": name, "entry": entry})
        if drifted:
            with database._connect(write=True) as connection:
                _record_environment_error(
                    database,
                    connection,
                    environment_id,
                    row,
                    None,
                    "drift during unenrollment removal: " + ", ".join(drifted),
                )
            raise BridgeError(
                f"unenrollment removal hit drift for {environment_id} "
                f"({', '.join(drifted)}); unmanaged entries were not deleted; "
                "re-run after review"
            )
        if obsolete:
            try:
                applied = _apply_environment_entries(
                    database, row, desired=[], obsolete=obsolete, dry_run=False
                )
            except BridgeError as exc:
                raise BridgeError(
                    f"unenrollment removal failed before any change: {exc}"
                )
            actions = applied["actions"]
    with database._connect(write=True) as connection:
        if remove_entries and not any(item["action"] == "drift" for item in actions):
            connection.execute(
                "DELETE FROM agent_environments WHERE environment_id = ?",
                (environment_id,),
            )
            event = "unenrollment_removed"
        else:
            connection.execute(
                "UPDATE agent_environments SET enabled = 0, updated_at_ns = ? "
                "WHERE environment_id = ?",
                (time.time_ns(), environment_id),
            )
            event = "unenrollment_stopped"
        database.append_outbox(
            connection,
            event,
            environment_id=environment_id,
            detail=_canonical_json(actions),
        )
    return {
        "ok": True,
        "environmentId": environment_id,
        "removedEntries": remove_entries,
        "actions": actions,
    }




# ---- Reconcile engine -----------------------------------------------------

def _projection_current_entries(
    database: ProjectionDatabase,
    row: sqlite3.Row,
    *,
    unverifiable: set[str] | None = None,
) -> dict[str, dict[str, Any]]:
    mode = str(row["apply_adapter"])
    if mode == ADAPTER_UNAVAILABLE:
        mode = _resolve_adapter_mode(
            str(row["client_kind"]),
            Path(row["config_path"]),
            launcher_command=str(row["launcher_command"]),
            launcher_args=json.loads(row["launcher_args_json"]),
        )
    if mode == ADAPTER_UNAVAILABLE:
        raise BridgeError(
            f"no supported apply adapter for {row['client_kind']} at "
            f"{row['config_path']}; the client's official CLI is required when "
            "the configuration is not Bridge-owned"
        )
    raw_endpoints = _env_row_field(row, "stdio_http_endpoints_json", "")
    endpoints = (
        json.loads(raw_endpoints) if isinstance(raw_endpoints, str) and raw_endpoints else {}
    )
    return _adapter_read_entries(
        kind=str(row["client_kind"]),
        mode=mode,
        config_path=Path(row["config_path"]),
        launcher_command=str(row["launcher_command"]),
        launcher_args=json.loads(row["launcher_args_json"]),
        relay_base_url=_env_row_field(row, "relay_base_url", ""),
        stdio_http_endpoints=endpoints,
        unverifiable=unverifiable,
    )


def _projection_unowned_names(
    row: sqlite3.Row,
) -> set[str]:
    """Names present in a shared Claude document that the bridge does not own.

    Only shared Claude JSON documents carry unmanaged MCP servers beside
    Bridge-owned ones, so the conservative collision probe applies there and
    nowhere else (Codex owned documents and DSH overlays are wholly
    Bridge-owned and regenerated deterministically). Official-CLI mode defers
    to the client's own registry and is probed by the client itself.
    """
    mode = str(row["apply_adapter"])
    if (
        str(row["client_kind"]) != CLIENT_KIND_CLAUDE
        or mode != ADAPTER_BRIDGE_FILE
    ):
        return set()
    config_path = Path(row["config_path"])
    if not config_path.exists():
        return set()
    relay_base_url = _env_row_field(row, "relay_base_url", "")
    raw_endpoints = _env_row_field(row, "stdio_http_endpoints_json", "")
    endpoints = (
        json.loads(raw_endpoints) if isinstance(raw_endpoints, str) and raw_endpoints else {}
    )
    launcher_command = str(row["launcher_command"])
    launcher_args = json.loads(row["launcher_args_json"])
    try:
        entries, _document = _claude_json_document(
            config_path.read_text(encoding="utf-8")
        )
    except OSError as exc:
        raise BridgeError(
            f"claude configuration could not be read for the collision probe: {exc}"
        )
    return {
        name
        for name, entry in entries.items()
        if not _looks_bridge_owned(
            entry,
            launcher_command=launcher_command,
            launcher_args=launcher_args,
            name=name,
            relay_base_url=relay_base_url,
            stdio_http_endpoints=endpoints,
        )
    }


def _desired_entry_descriptors(
    row: sqlite3.Row, mirror_facts: list[tuple[str, str, str]]
) -> list[dict[str, Any]]:
    launcher_command = str(row["launcher_command"])
    launcher_args = json.loads(row["launcher_args_json"])
    capabilities = json.loads(row["transport_capabilities_json"])
    route = str(row["compatibility_route"])
    tool_exposure = _effective_tool_exposure(row)
    if tool_exposure == "deferred" and (
        route != "native" or not capabilities.get("stdio")
        or any(transport != "stdio" for _, _, transport in mirror_facts)
    ):
        if _env_row_field(row, "tool_exposure", "native") == "auto":
            tool_exposure = "native"
        else:
            raise BridgeError("deferred exposure supports only native stdio peer registrations")
    relay_base_url = _env_row_field(row, "relay_base_url", "")
    raw_endpoints = _env_row_field(row, "stdio_http_endpoints_json", "")
    stdio_http_endpoints = (
        json.loads(raw_endpoints) if isinstance(raw_endpoints, str) and raw_endpoints else {}
    )
    descriptors: list[dict[str, Any]] = []
    unsupported: list[str] = []
    for server_id, _display_name, transport in mirror_facts:
        if transport == "stdio" and route == "stdio-to-http":
            # Explicit converted projection: the Agent (which consumes MCP over
            # Streamable HTTP) is pointed at the Operator-supplied loopback
            # /mcp URL of the already-provisioned stdio-to-HTTP facade for this
            # exact registered id. No port is guessed and no process is
            # launched or supervised here; a missing mapping fails the whole
            # environment closed below.
            endpoint = stdio_http_endpoints.get(server_id)
            if endpoint:
                descriptor = {"url": endpoint, "headers": {}}
                descriptor["projectionMode"] = "converted"
            else:
                unsupported.append(f"{server_id}:stdio")
                continue
        elif transport == "stdio" and capabilities.get("stdio"):
            descriptor = _agent_connect_entry(
                launcher_command=launcher_command,
                launcher_args=launcher_args,
                server_id=server_id,
                compatibility=(route == "constant-two-tool"),
                deferred=(tool_exposure == "deferred"),
            )
            descriptor["projectionMode"] = (
                "converted" if route == "constant-two-tool" or tool_exposure == "deferred" else "native"
            )
        elif (
            transport == "streamable-http"
            and route in {"native", "stdio-to-http"}
            and capabilities.get("streamable-http")
            and relay_base_url
        ):
            # Native (or stdio-to-http routed) peer HTTP still reaches the
            # Agent through this host's loopback relay at /mcp/<server-id> when
            # the Operator explicitly supplied its relay base; no peer registry
            # endpoint, header, or credential ever reaches the client config.
            descriptor = _agent_http_entry(
                relay_base_url=relay_base_url, server_id=server_id
            )
            descriptor["projectionMode"] = "native"
        elif transport == "streamable-http" and route == "http-to-stdio":
            descriptor = _agent_connect_entry(
                launcher_command=launcher_command,
                launcher_args=launcher_args,
                server_id=server_id,
            )
            descriptor["args"] = launcher_args + ["connect-http", server_id]
            descriptor["projectionMode"] = "converted"
        else:
            unsupported.append(f"{server_id}:{transport}")
            continue
        descriptor["transport"] = (
            "streamable-http" if "url" in descriptor else "stdio"
        )
        descriptor["sourceTransport"] = transport
        descriptor["name"] = server_id
        descriptors.append(descriptor)
    descriptors.sort(key=lambda item: item["name"])
    return descriptors


def _apply_environment_entries(
    database: ProjectionDatabase,
    row: sqlite3.Row,
    *,
    desired: list[dict[str, Any]],
    obsolete: list[dict[str, Any]],
    dry_run: bool,
    previous_entries: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    mode = str(row["apply_adapter"])
    if mode == ADAPTER_UNAVAILABLE:
        mode = _resolve_adapter_mode(
            str(row["client_kind"]),
            Path(row["config_path"]),
            launcher_command=str(row["launcher_command"]),
            launcher_args=json.loads(row["launcher_args_json"]),
        )
    if mode == ADAPTER_UNAVAILABLE:
        raise BridgeError(
            f"no supported apply adapter for {row['client_kind']} at "
            f"{row['config_path']}"
        )
    actions = _adapter_apply(
        kind=str(row["client_kind"]),
        mode=mode,
        config_path=Path(row["config_path"]),
        desired=desired,
        obsolete=obsolete,
        dry_run=dry_run,
        previous_entries=previous_entries,
    )
    return {"actions": actions, "mode": mode}


def _record_environment_error(
    database: ProjectionDatabase,
    connection: sqlite3.Connection,
    environment_id: str,
    env_row: sqlite3.Row,
    _record: sqlite3.Row | None,
    message: str,
) -> None:
    revision = database.current_revision(connection)
    database.record_history(
        connection,
        environment_id=environment_id,
        revision=revision,
        action="error",
        ok=False,
        detail=message,
    )
    connection.execute(
        "UPDATE agent_mcp_projections SET status = ?, error_detail = ?, "
        "updated_at_ns = ? WHERE environment_id = ?",
        (ENV_STATUS_ERROR, message[:4000], time.time_ns(), environment_id),
    )
    connection.execute(
        "UPDATE agent_environments SET updated_at_ns = ? WHERE environment_id = ?",
        (time.time_ns(), environment_id),
    )
    database.append_outbox(
        connection,
        "projection_changed",
        environment_id=environment_id,
        detail=f"error: {message[:400]}",
    )


def _mark_outbox_processed(connection: sqlite3.Connection) -> None:
    connection.execute(
        "UPDATE registry_projection_outbox SET processed = 1 WHERE processed = 0"
    )


def _reconcile_one_environment(
    database: ProjectionDatabase,
    connection: sqlite3.Connection,
    env_row: sqlite3.Row,
    mirror_facts: list[tuple[str, str, str]],
    *,
    dry_run: bool,
) -> dict[str, Any]:
    environment_id = str(env_row["environment_id"])
    try:
        environment_summary = _environment_summary(env_row)
        verification = _row_harness_verification(env_row)
        verification_matches_before = bool(verification) and (
            verification.get("projectionConfigFingerprint", verification.get("configFingerprint"))
            == _harness_config_fingerprint(env_row)
        )
        desired_descriptors = _desired_entry_descriptors(env_row, mirror_facts)
    except (BridgeError, ValueError, TypeError, OSError) as exc:
        # Client-specific policy failures must never roll back bookkeeping for
        # earlier environments whose configuration files already changed.
        message = "invalid_client_verification: " + str(exc)
        if not dry_run:
            _record_environment_error(database, connection, environment_id, env_row, None, message)
        return {
            "environmentId": environment_id, "clientKind": env_row["client_kind"],
            "status": ENV_STATUS_ERROR, "errorDetail": message,
            "actions": [], "conflicts": [], "drift": [], "servers": [],
        }
    desired_by_name = {item["name"]: item for item in desired_descriptors}
    unsupported = [
        {"serverId": server_id, "transport": transport, "code": "unsupported_transport"}
        for server_id, _name, transport in mirror_facts
        if server_id not in desired_by_name
    ]
    # Validate the whole environment before a transport change can remove its old entry.
    if unsupported:
        message = "unsupported_transport: projection unchanged; unsupported targets: " + ", ".join(
            f"{item['serverId']}:{item['transport']}" for item in unsupported
        )
        if not dry_run:
            _record_environment_error(
                database, connection, environment_id, env_row, None, message
            )
        environment_summary.update({
            "status": ENV_STATUS_ERROR,
            "errorDetail": message,
            "unsupportedTransports": unsupported,
            "actions": [],
            "conflicts": [],
            "drift": [],
            "servers": [
                {"serverId": item["serverId"], "status": ENV_STATUS_ERROR}
                for item in unsupported
            ],
        })
        return environment_summary
    current: dict[str, dict[str, Any]] = {}
    unverifiable: set[str] = set()
    try:
        current = _projection_current_entries(
            database, env_row, unverifiable=unverifiable
        )
    except BridgeError as exc:
        message = str(exc)
        if not dry_run:
            _record_environment_error(
                database, connection, environment_id, env_row, None, message
            )
        environment_summary["status"] = ENV_STATUS_ERROR
        environment_summary["errorDetail"] = message
        environment_summary["actions"] = []
        return environment_summary
    # Conservative name-collision probe: a shared Claude document may hold
    # unmanaged servers (stdio or http) whose names collide with desired
    # projections; those are reported and never overwritten. An unreadable or
    # malformed shared document fails the whole environment closed instead of
    # silently skipping the probe.
    try:
        unowned_names = _projection_unowned_names(env_row)
    except BridgeError as exc:
        message = str(exc)
        if not dry_run:
            _record_environment_error(
                database, connection, environment_id, env_row, None, message
            )
        environment_summary["status"] = ENV_STATUS_ERROR
        environment_summary["errorDetail"] = message
        environment_summary["actions"] = []
        return environment_summary
    stored = {
        str(item["server_id"]): item
        for item in database.projection_rows(connection, environment_id)
    }
    launcher_command = str(env_row["launcher_command"])
    launcher_args = json.loads(env_row["launcher_args_json"])

    conflicts: list[str] = []
    to_add: list[dict[str, Any]] = []
    keep_names: set[str] = set()
    obsolete: list[dict[str, Any]] = []
    drift_names: list[str] = []
    per_server: dict[str, str] = {}

    for name, descriptor in sorted(desired_by_name.items()):
        kind = str(env_row["client_kind"])
        current_entry = current.get(name)
        stored_row = stored.get(name)
        if current_entry is None:
            if name in unowned_names or name in unverifiable:
                conflicts.append(name)
                per_server[name] = ENV_STATUS_ERROR
                continue
            item = dict(descriptor)
            item["replace"] = False
            to_add.append(item)
            per_server[name] = "add"
            continue
        current_fp = _entry_fingerprint_for_kind(kind, current_entry)
        desired_fp = _entry_fingerprint_for_kind(kind, descriptor)
        if stored_row is None:
            # No persisted evidence. Ownership for HTTP is decided by persisted
            # entry evidence, never by URL shape alone; the crash-adoption
            # exception applies only when the live entry is byte-exact with the
            # expected descriptor we would write.
            if current_fp == desired_fp:
                keep_names.add(name)
                per_server[name] = ENV_STATUS_CONFIGURED
            else:
                conflicts.append(name)
                per_server[name] = ENV_STATUS_ERROR
            continue
        stored_fp = str(stored_row["entry_fingerprint"])
        if current_fp != stored_fp:
            # Persisted evidence says we wrote one entry and the document holds
            # another (user-edited header/env/args/url): fail closed as drift
            # and never overwrite the user's edit or the stored fingerprint.
            drift_names.append(name)
            per_server[name] = ENV_STATUS_DRIFT
            continue
        if current_fp == desired_fp:
            keep_names.add(name)
            per_server[name] = ENV_STATUS_CONFIGURED
            continue
        item = dict(descriptor)
        item["replace"] = True
        to_add.append(item)
        per_server[name] = "update"

    for name, entry in current.items():
        if name in desired_by_name:
            continue
        stored_row = stored.get(name)
        if stored_row is None:
            drift_names.append(name)
            per_server[name] = ENV_STATUS_DRIFT
            continue
        expected = str(stored_row["entry_fingerprint"])
        if _entry_fingerprint_for_kind(str(env_row["client_kind"]), entry) != expected:
            drift_names.append(name)
            per_server[name] = ENV_STATUS_DRIFT
            continue
        obsolete.append({"name": name, "entry": entry})
        per_server[name] = "remove"

    apply_result: dict[str, Any] = {"actions": []}
    apply_failed_message: str | None = None
    if to_add or obsolete:
        try:
            apply_result = _apply_environment_entries(
                database,
                env_row,
                desired=to_add,
                obsolete=obsolete,
                dry_run=dry_run,
                previous_entries=current,
            )
        except BridgeError as exc:
            apply_failed_message = str(exc)
        actions = apply_result["actions"]
        for action in actions:
            if action["action"] == "drift":
                server_id = action.get("serverId")
                if server_id:
                    drift_names.append(server_id)
    if apply_failed_message is not None:
        if not dry_run:
            _record_environment_error(
                database,
                connection,
                environment_id,
                env_row,
                None,
                f"apply failed: {apply_failed_message}",
            )
        environment_summary["status"] = ENV_STATUS_ERROR
        environment_summary["errorDetail"] = f"apply failed: {apply_failed_message}"
        environment_summary["actions"] = apply_result["actions"]
        environment_summary["conflicts"] = conflicts
        environment_summary["drift"] = drift_names
        return environment_summary
    applied_remove_names = {
        action.get("serverId")
        for action in apply_result["actions"]
        if action["action"] == "remove"
    }

    # Persist projection bookkeeping (same transaction as history/outbox).
    if not dry_run:
        if verification_matches_before and not conflicts and not drift_names and (to_add or obsolete):
            # Only our successful fingerprint-checked configuration rewrite may
            # advance this binding. Keep the actual probe fingerprint intact.
            verification["projectionConfigFingerprint"] = _harness_config_fingerprint(env_row)
            connection.execute(
                "UPDATE agent_environments SET harness_verification_json = ? WHERE environment_id = ?",
                (_canonical_json(verification), environment_id),
            )
            environment_summary["harnessVerification"] = verification
        now = time.time_ns()
        revision = database.current_revision(connection)
        for name, descriptor in sorted(desired_by_name.items()):
            status = per_server.get(name, ENV_STATUS_PENDING)
            if status in ("add", "update"):
                status = ENV_STATUS_CONFIGURED
            if status == ENV_STATUS_ERROR:
                error_detail = f"unmanaged name collision for {name}"
            elif status == ENV_STATUS_DRIFT:
                error_detail = (
                    "drift: persisted fingerprint no longer matches the document; "
                    "user edit was not overwritten"
                )
            else:
                error_detail = None
            applied_at = (
                now if status == ENV_STATUS_CONFIGURED else None
            )
            if status == ENV_STATUS_CONFIGURED:
                # Applied or verified-unchanged: persist the current fingerprint.
                connection.execute(
                    "INSERT INTO agent_mcp_projections ("
                    "environment_id, server_id, exposed_name, status, entry_fingerprint, "
                    "error_detail, desired_at_ns, applied_at_ns, updated_at_ns"
                    ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) "
                    "ON CONFLICT(environment_id, server_id) DO UPDATE SET "
                    "exposed_name = excluded.exposed_name, status = excluded.status, "
                    "entry_fingerprint = excluded.entry_fingerprint, "
                    "error_detail = excluded.error_detail, "
                    "desired_at_ns = excluded.desired_at_ns, "
                    "applied_at_ns = excluded.applied_at_ns, "
                    "updated_at_ns = excluded.updated_at_ns",
                    (
                        environment_id,
                        name,
                        name,
                        status,
                        _entry_fingerprint_for_kind(
                            str(env_row["client_kind"]), descriptor
                        ),
                        error_detail,
                        now,
                        applied_at,
                        now,
                    ),
                )
            else:
                # Drift / conflict: never overwrite the stored fingerprint with
                # the desired one; keep the persisted evidence intact so a later
                # reconcile can still verify and, after the user resolves the
                # edit, transition cleanly.
                updated = connection.execute(
                    "UPDATE agent_mcp_projections SET status = ?, "
                    "error_detail = ?, updated_at_ns = ? "
                    "WHERE environment_id = ? AND server_id = ?",
                    (
                        status,
                        error_detail,
                        now,
                        environment_id,
                        name,
                    ),
                )
                if updated.rowcount == 0:
                    connection.execute(
                        "INSERT INTO agent_mcp_projections ("
                        "environment_id, server_id, exposed_name, status, "
                        "entry_fingerprint, error_detail, desired_at_ns, "
                        "applied_at_ns, updated_at_ns"
                        ") VALUES (?, ?, ?, ?, '', ?, ?, NULL, ?)",
                        (
                            environment_id,
                            name,
                            name,
                            status,
                            error_detail,
                            now,
                            now,
                        ),
                    )
        for name in per_server:
            if name not in desired_by_name:
                if per_server[name] == "remove" and name in applied_remove_names:
                    connection.execute(
                        "DELETE FROM agent_mcp_projections "
                        "WHERE environment_id = ? AND server_id = ?",
                        (environment_id, name),
                    )
                else:
                    connection.execute(
                        "UPDATE agent_mcp_projections SET status = ?, "
                        "error_detail = ?, updated_at_ns = ? "
                        "WHERE environment_id = ? AND server_id = ?",
                        (
                            ENV_STATUS_DRIFT if per_server[name] == ENV_STATUS_DRIFT else ENV_STATUS_ERROR,
                            (
                                "drift: persisted fingerprint no longer matches the document"
                                if per_server[name] == ENV_STATUS_DRIFT
                                else None
                            ),
                            now,
                            environment_id,
                            name,
                        ),
                    )
        environment_status = ENV_STATUS_CONFIGURED
        if conflicts or drift_names:
            environment_status = (
                ENV_STATUS_DRIFT if drift_names and not conflicts else ENV_STATUS_ERROR
            )
        if str(env_row["client_kind"]) == CLIENT_KIND_DSH:
            environment_status = ENV_STATUS_NEXT_SESSION
        connection.execute(
            "UPDATE agent_environments SET updated_at_ns = ? WHERE environment_id = ?",
            (now, environment_id),
        )
        history_action = "apply"
        database.record_history(
            connection,
            environment_id=environment_id,
            revision=revision,
            action=history_action,
            ok=not (conflicts or drift_names),
            detail=_canonical_json(
                {
                    "mode": apply_result.get("mode"),
                    "actions": apply_result["actions"],
                    "conflicts": conflicts,
                    "drift": drift_names,
                }
            ),
        )
        database.append_outbox(
            connection,
            "projection_changed",
            environment_id=environment_id,
            detail=_canonical_json(
                {
                    "status": environment_status,
                    "added": len(to_add),
                    "removed": len(applied_remove_names),
                }
            ),
        )
    else:
        environment_status = ENV_STATUS_CONFIGURED
    environment_summary["status"] = environment_status
    environment_summary["unsupportedTransports"] = []
    environment_summary["conflicts"] = conflicts
    environment_summary["drift"] = drift_names
    environment_summary["actions"] = apply_result["actions"]
    environment_summary["servers"] = [
        {"serverId": name, "status": status}
        for name, status in sorted(per_server.items())
    ]
    return environment_summary


def projection_reconcile(
    *,
    projection: Path,
    side: str,
    refresh_source: str | None = None,
    peer_registry: Path | None = None,
    local_host: str = "127.0.0.1",
    local_port: int | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Deterministically converge enrolled environments on the peer mirror."""
    if dry_run:
        from contextlib import closing
        # Upgrade and refresh only a disposable snapshot. Source enrollment,
        # outbox, permissions and even a missing source directory stay intact.
        with tempfile.TemporaryDirectory(prefix="bridge-projection-preview-") as temporary:
            preview = Path(temporary) / "projection.sqlite3"
            if projection.exists():
                with closing(sqlite3.connect(
                    projection.resolve().as_uri() + "?mode=ro", uri=True, timeout=5,
                )) as source, closing(sqlite3.connect(preview, timeout=5)) as destination:
                    source.backup(destination)
            ProjectionDatabase.ensure(preview)
            sync_result = None
            if refresh_source is not None:
                sync_result = projection_sync_peer(
                    projection=preview, source=refresh_source, side=side,
                    peer_registry=peer_registry, local_host=local_host, local_port=local_port,
                )
            database = ProjectionDatabase(preview, observe_only=True)
            with closing(database._connect()) as connection:
                mirror_facts = [(str(row["server_id"]), str(row["name"]), str(row["transport"]))
                                for row in database.peer_rows(connection)]
                environments = [
                    _reconcile_one_environment(database, connection, row, mirror_facts, dry_run=True)
                    for row in database.environment_rows(connection) if int(row["enabled"]) == 1
                ]
            errors = [outcome.get("errorDetail", "environment error") for outcome in environments
                      if outcome.get("status") in {ENV_STATUS_ERROR, ENV_STATUS_DRIFT}]
            return {
                "ok": not errors, "dryRun": True, "sync": sync_result,
                "mirrorServers": [item[0] for item in mirror_facts],
                "environments": environments, "errors": errors,
            }
    ProjectionDatabase.ensure(projection)
    database = ProjectionDatabase(projection)
    if refresh_source is not None:
        sync_result = projection_sync_peer(
            projection=projection,
            source=refresh_source,
            side=side,
            peer_registry=peer_registry,
            local_host=local_host,
            local_port=local_port,
        )
    else:
        sync_result = None
    with database._connect() as connection:
        mirror_rows = database.peer_rows(connection)
        env_rows = [
            row
            for row in database.environment_rows(connection)
            if int(row["enabled"]) == 1
        ]
    mirror_facts = [
        (
            str(row["server_id"]),
            str(row["name"]),
            str(row["transport"]),
        )
        for row in mirror_rows
    ]
    environments: list[dict[str, Any]] = []
    errors: list[str] = []
    with database._connect(write=True) as connection:
        for env_row in env_rows:
            outcome = _reconcile_one_environment(
                database, connection, env_row, mirror_facts, dry_run=False
            )
            environments.append(outcome)
            if outcome.get("status") in {ENV_STATUS_ERROR, ENV_STATUS_DRIFT}:
                errors.append(
                    f"{outcome.get('environmentId')}: {outcome.get('status')} "
                    + (outcome.get("errorDetail") or
                       f"{outcome.get('conflicts')} {outcome.get('drift')}")
                )
        if env_rows:
            _mark_outbox_processed(connection)
    return {
        "ok": not errors,
        "dryRun": False,
        "sync": sync_result,
        "mirrorServers": [item[0] for item in mirror_facts],
        "environments": environments,
        "errors": errors,
    }


def projection_watch(
    *,
    projection: Path,
    side: str,
    refresh_source: str,
    peer_registry: Path | None,
    local_host: str,
    local_port: int,
    interval_seconds: float,
    max_rounds: int | None = None,
) -> int:
    """Optional polling: refresh the peer mirror and reconcile on change."""
    rounds = 0
    try:
        while max_rounds is None or rounds < max_rounds:
            rounds += 1
            result = projection_reconcile(
                projection=projection,
                side=side,
                refresh_source=refresh_source,
                peer_registry=peer_registry,
                local_host=local_host,
                local_port=local_port,
            )
            changed = bool(result.get("sync", {}).get("changed", False))
            if max_rounds is None:
                print(
                    json.dumps(
                        {
                            "revision": result.get("sync", {}).get("revision"),
                            "changed": changed,
                            "ok": result.get("ok"),
                            "errors": result.get("errors", []),
                        },
                        ensure_ascii=False,
                        separators=(",", ":"),
                    )
                )
            if max_rounds is not None and rounds >= max_rounds:
                break
            time.sleep(interval_seconds)
    except KeyboardInterrupt:
        return 0
    except BridgeError as exc:
        print(f"projection watch: {exc}", file=sys.stderr)
        return 1
    return 0


def _safe_capability_report(
    row: Any, outstanding: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    """Capability checklist for read-only reporting; never fails a whole report."""
    try:
        return _capability_checklist(row, outstanding)
    except BridgeError as exc:
        return {
            "environmentId": row["environment_id"],
            "error": str(exc),
            "toolExposure": _env_row_field(row, "tool_exposure", "native"),
            "effectiveToolExposure": None,
            "toolExposureBlockers": [str(exc)],
            "evidenceFresh": False,
            "evidenceReasons": [str(exc)],
            "capabilities": [],
            "outstandingChallenges": sorted(
                outstanding.values(), key=lambda item: str(item["aspect"]),
            ),
            "nextActions": [],
        }


def projection_probe_status(
    *, projection: Path, environment_id: str | None = None,
) -> dict[str, Any]:
    """Read-only adaptation report for Bridge Agents.

    Reports, per enrolled environment, which registry decision each capability
    aspect has actually established, why recorded evidence does or does not
    still apply, which prepared challenges are outstanding, and the next concrete
    observation.  It never writes, never launches a target, and never accepts a
    claimed capability from a client's product name.  It also never reports a
    business MCP's tool count or token cost: that is a separate per-MCP exposure
    measurement.
    """
    database = ProjectionDatabase(projection, observe_only=True)
    with database._connect() as connection:
        rows = database.environment_rows(connection)
        selected = [
            (row, _outstanding_client_probes(connection, row["environment_id"]))
            for row in rows
            if environment_id is None or row["environment_id"] == environment_id
        ]
        reports = [
            (row, _safe_capability_report(row, outstanding)) for row, outstanding in selected
        ]
    if environment_id is not None and not reports:
        raise BridgeError(f"unknown environment: {environment_id}")
    environments = []
    for row, report in reports:
        environments.append({
            "environmentId": row["environment_id"],
            "clientKind": row["client_kind"],
            "configPath": row["config_path"],
            "enabled": bool(row["enabled"]),
            "applyAdapter": row["apply_adapter"],
            "toolExposure": report["toolExposure"],
            "effectiveToolExposure": report["effectiveToolExposure"],
            "toolExposureBlockers": report["toolExposureBlockers"],
            "evidenceFresh": report["evidenceFresh"],
            "evidenceReasons": report["evidenceReasons"],
            "verifiedAt": report.get("verifiedAt"),
            "validUntil": report.get("validUntil"),
            "capabilities": report["capabilities"],
            "outstandingChallenges": report["outstandingChallenges"],
            "nextActions": report["nextActions"],
        })
    return {
        "database": str(database.path),
        "environmentCount": len(environments),
        "environments": environments,
        "aspects": [
            {
                "aspect": name,
                "capability": definition["capability"],
                "observation": definition["observation"],
                "registryField": definition["registryField"],
                "decision": definition["decision"],
                "runnable": bool(definition["runnable"]),
                "agentSteps": list(definition["agentSteps"]),
            }
            for name, definition in CLIENT_CAPABILITY_ASPECTS.items()
        ],
        "scope": "Harness capability evidence only; this probe never measures a business MCP's "
                 "tool count, tool-definition volume, or model-context token cost.",
    }


def projection_status(*, projection: Path) -> dict[str, Any]:
    ProjectionDatabase.ensure(projection)
    database = ProjectionDatabase(projection)
    with database._connect() as connection:
        rows = database.environment_rows(connection)
        environments = [_environment_summary(row) for row in rows]
        capability = {
            row["environment_id"]: _safe_capability_report(
                row, _outstanding_client_probes(connection, row["environment_id"]),
            )
            for row in rows
        }
        for summary in environments:
            report = capability.get(summary["environmentId"], {})
            summary["effectiveToolExposure"] = report.get("effectiveToolExposure")
            summary["capabilityStates"] = {
                item["aspect"]: item["state"] for item in report.get("capabilities", [])
            }
            summary["outstandingChallenges"] = [
                item["aspect"] for item in report.get("outstandingChallenges", [])
            ]
        projections = [
            {
                "environmentId": row["environment_id"],
                "serverId": row["server_id"],
                "exposedName": row["exposed_name"],
                "status": row["status"],
                "errorDetail": row["error_detail"],
                "updatedAtNs": row["updated_at_ns"],
            }
            for row in database.projection_rows(connection)
        ]
        mirror = [
            {
                "serverId": row["server_id"],
                "name": row["name"],
                "transport": row["transport"],
            }
            for row in database.peer_rows(connection)
        ]
        outbox = connection.execute(
            "SELECT COUNT(*) AS count FROM registry_projection_outbox WHERE processed = 0"
        ).fetchone()
        revision = database.current_revision(connection)
        fingerprint = database.meta_value(connection, "peer_fingerprint")
        peer_source = database.meta_value(connection, "peer_source")
    return {
        "database": str(database.path),
        "revision": revision,
        "peerFingerprint": fingerprint,
        "peerSource": peer_source,
        "outboxUnprocessed": int(outbox["count"]),
        "environments": environments,
        "projections": projections,
        "mirrorServers": mirror,
    }




# ---- Registry-commit integration helpers ----------------------------------

def _registry_projection_facts(
    connection: sqlite3.Connection,
) -> list[tuple[str, str, int, str]]:
    """Projection-affecting facts (id, name, enabled, transport)."""
    try:
        rows = connection.execute(
            "SELECT id, name, enabled, transport_json FROM servers ORDER BY id"
        ).fetchall()
    except sqlite3.Error:
        return []
    facts = []
    for row in rows:
        try:
            transport = json.loads(row[3]).get("type", "stdio")
        except (TypeError, ValueError):
            transport = "stdio"
        facts.append((str(row[0]), str(row[1]), int(row[2]), str(transport)))
    return facts


def _append_registry_change_event(
    connection: sqlite3.Connection,
    before: list[tuple[str, str, int, str]],
    after: list[tuple[str, str, int, str]],
) -> None:
    """Append one registry_changed outbox event inside an open transaction.

    The projection database is ATTACHed to the registry connection, so this row
    commits atomically with the registry change; a rollback of either file
    rolls back both.
    """
    before_map = {item[0]: item for item in before}
    after_map = {item[0]: item for item in after}
    added = sorted(set(after_map) - set(before_map))
    removed = sorted(set(before_map) - set(after_map))
    changed = sorted(
        server_id
        for server_id in set(before_map) & set(after_map)
        if before_map[server_id] != after_map[server_id]
    )
    connection.execute(
        "INSERT INTO projection.registry_projection_outbox "
        "(revision, event_type, environment_id, server_id, detail, occurred_at_ns, processed) "
        "SELECT COALESCE(MAX(revision), 0) + 1, 'registry_changed', NULL, NULL, ?, ?, 0 "
        "FROM projection.registry_projection_outbox",
        (
            _canonical_json(
                {
                    "added": added,
                    "removed": removed,
                    "changed": changed,
                }
            ),
            time.time_ns(),
        ),
    )


def _projection_side_local_port(side: str) -> int:
    return 8769 if side == "wsl" else 8768


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


def lifecycle_control_query(
    local_host: str,
    local_port: int,
    action: str,
    *,
    target: str | None,
    generation: int | None,
    confirm: bool,
) -> Any:
    """One bounded local lifecycle control request on a node's loopback socket.

    The node is authoritative: it resolves the id only against its own registry,
    enforces preview-before-mutation, and rejects non-loopback callers. This
    function never carries commands, arguments, environments, or remote targets.
    """
    if not _is_loopback(local_host):
        raise BridgeError("lifecycle host must resolve only to loopback")
    request: dict[str, Any] = {"op": "lifecycle", "action": action}
    if target is not None:
        request["target"] = target
    if generation is not None:
        request["generation"] = generation
    if action in {"drain", "refresh", "restart", "stop"}:
        request["confirm"] = confirm
    with socket.create_connection((local_host, local_port), timeout=10) as sock:
        sock.sendall(_json_bytes(request) + b"\n")
        reply = json.loads(_recv_line(sock))
    if not reply.get("ok"):
        raise BridgeError(str(reply.get("message", "lifecycle request failed")))
    return reply.get("result")


def _lifecycle_cli_main(args: argparse.Namespace, *, default_local_port: int) -> int:
    """Operator CLI for Bridge-owned MCP lifecycle state and control.

    status is read-only; drain/refresh/restart/stop print a read-only preview unless
    --confirm is passed, and the node enforces the same rule again.
    """
    command = str(getattr(args, "lifecycle_command", ""))
    local_host = str(getattr(args, "local_host", "127.0.0.1"))
    local_port = getattr(args, "local_port", None)
    if local_port is None:
        local_port = default_local_port
    local_port = int(local_port)
    target = getattr(args, "id", None)
    generation = getattr(args, "generation", None)
    confirm = bool(getattr(args, "confirm", False))
    try:
        if command == "status":
            result = lifecycle_control_query(
                local_host,
                local_port,
                "status",
                target=target,
                generation=None,
                confirm=False,
            )
            print(json.dumps(result, ensure_ascii=False, separators=(",", ":")))
            return 0
        if command not in {"drain", "refresh", "restart", "stop"}:
            raise BridgeError(f"unknown lifecycle command: {command}")
        if target is None:
            raise BridgeError(f"lifecycle {command} requires --id")
        result = lifecycle_control_query(
            local_host,
            local_port,
            command,
            target=target,
            generation=generation,
            confirm=confirm,
        )
        print(json.dumps(result, ensure_ascii=False, separators=(",", ":")))
        if not confirm and not bool(result.get("applied")):
            print(
                f"lifecycle {command}: preview only; pass --confirm to apply the "
                "mutation to this node",
                file=sys.stderr,
            )
        return 0 if result.get("ok") else 1
    except (BridgeError, OSError, sqlite3.Error, ValueError) as exc:
        print(f"lifecycle {command}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(
        "run the win-wsl-mcp-win or win-wsl-mcp-wsl entry point, or a component bridge.py"
    )
