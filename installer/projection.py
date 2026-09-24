"""Installer-owned enrollment, client adapters, and configuration projection.

Only explicit installation operations use this module. The peer transport and
MCP serving runtime never select behavior by client kind or edit client files.
"""
from __future__ import annotations

import hashlib
import ipaddress
import json
import os
import re
import secrets
import shutil
import sqlite3
import stat
import subprocess
import sys
import tempfile
import time
import urllib.parse
from pathlib import Path
from typing import Any

from bridge_runtime import (
    BridgeError, ID_PATTERN, SHARED_COMPATIBLE_PROTOCOL_VERSIONS,
    _canonical_json, default_registry_path, local_registry_query,
)

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

# One DeepSeek Harness installation is several enrolled profiles (web / dsh-tui /
# headless) that share one MCP client package on one host. A recorded
# observation of that client therefore covers the family: the recording carries
# this explicit scope plus the environment it was observed in, and sibling
# profiles with no observation of their own adopt it. Adoption is never
# inferred from a product name, and a profile's own observation always wins.
FAMILY_EVIDENCE_SCOPE = "dsh-family"

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
    # Evidence is bound to one environment, one client kind, one configuration
    # and one version identity; the only recorded exception is an explicitly
    # family-scoped DSH observation adopted by a sibling Harness profile. No
    # product name ever enables anything, absent or identity-mismatched
    # evidence cannot enable deferral, and nothing here expires on a clock.
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


def _default_enrolled_tool_exposure(client_kind: str) -> str:
    """The exposure mode a *new* enrollment records for one client kind.

    ``native`` pins the complete catalog; ``auto`` leaves the decision to this
    host's exposure policy, which still yields the complete catalog until
    applicable probe evidence exists and only then collapses it per library.
    Recording ``auto`` therefore never widens or narrows a catalog by itself.

    A DeepSeek Harness installation is one same-host client family whose
    profiles (``web`` / ``dsh-tui`` / ``headless``) are enrolled as separate
    environments.  Leaving those rows on the schema default hard-pinned each
    profile to ``native``, so two profiles of the same Harness behaved
    differently for no client-side reason.  DSH enrollment therefore records
    ``auto``; every other client kind keeps the historical ``native`` default
    because bridge deferral is not layered on those clients by default.

    ``deferred`` is never a default: it stays reachable only through recorded
    probe evidence for that environment (see ``_effective_tool_exposure``).
    """
    return "auto" if client_kind == CLIENT_KIND_DSH else "native"


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


def _family_evidence_applies(evidence: dict[str, Any], row: Any) -> bool:
    """Whether an observation of *another* environment may cover this row.

    A DeepSeek Harness installation is one client family: its profiles share one
    MCP client package on one host, so one observation of that client proves the
    family.  Adoption is never inferred from a product or client name - the
    recording must carry an explicit family scope, name the environment it was
    observed in, and agree on the observed client identity.  Only a DSH row may
    adopt, and only that row's own recorded mode decides what it means.
    """
    if evidence.get("scope") != FAMILY_EVIDENCE_SCOPE:
        return False
    if row["client_kind"] != CLIENT_KIND_DSH:
        return False
    if evidence.get("clientKind") != CLIENT_KIND_DSH:
        return False
    if not str(evidence.get("recordedEnvironmentId") or ""):
        return False
    identity = evidence.get("versionIdentity")
    if not isinstance(identity, dict):
        return False
    observed_client = identity.get("observedClient")
    return isinstance(observed_client, dict) and bool(observed_client.get("name"))


def _adopt_family_evidence(
    connection: sqlite3.Connection,
    database: "ProjectionDatabase",
    *,
    source_row: Any,
    evidence_json: str,
) -> list[dict[str, str]]:
    """Offer one DSH observation to sibling Harness profiles that have none.

    Only a sibling with no recorded observation of its own adopts, so a profile
    that was observed directly is never overwritten by another profile's
    observation.  The adopting row's recorded mode becomes ``auto``: the
    observation is evidence, not a directive, so this host's exposure policy
    still decides what it means for that profile.
    """
    adopted: list[dict[str, str]] = []
    siblings = connection.execute(
        "SELECT environment_id, client_kind, host_side, tool_exposure, "
        "harness_verification_json FROM agent_environments "
        "WHERE client_kind = ? AND host_side = ? AND enabled = 1 AND environment_id != ?",
        (CLIENT_KIND_DSH, source_row["host_side"], source_row["environment_id"]),
    ).fetchall()
    for sibling in siblings:
        if _row_harness_verification(sibling):
            continue
        if sibling["tool_exposure"] not in {"native", "auto"}:
            continue
        connection.execute(
            "UPDATE agent_environments SET tool_exposure = 'auto', "
            "harness_verification_json = ?, updated_at_ns = ? WHERE environment_id = ?",
            (evidence_json, time.time_ns(), sibling["environment_id"]),
        )
        database.append_outbox(
            connection, "enrollment_changed",
            environment_id=sibling["environment_id"],
            detail="adopted DSH family verification from " + source_row["environment_id"],
        )
        adopted.append({
            "environmentId": sibling["environment_id"],
            "adoptedFrom": source_row["environment_id"],
        })
    return adopted


def _harness_evidence_state(row: Any) -> dict[str, Any]:
    """Recorded client evidence plus exactly why it does or does not apply now.

    A recorded observation is bound to one environment, one client kind, one
    enrolled configuration fingerprint, and the version identity it was observed
    for.  It carries no validity window: evidence stops applying when that
    identity changes, and a Bridge Agent re-observes it when a client, its
    configuration, or the peer server set changes.  Any mismatch is reported as
    a reason instead of silently narrowing a catalog.

    One recorded exception: an explicitly family-scoped DSH observation written
    into a sibling Harness profile (see ``FAMILY_EVIDENCE_SCOPE``) applies to
    that sibling although the environment id and configuration fingerprint
    differ, because it proves the shared client rather than one profile's file.
    The recording environment keeps the per-configuration guard for its own
    observation, and the adopting row is reported as adopted.
    """
    from installer.harness_verification import ProbeValidationError, version_fingerprint
    evidence = _row_harness_verification(row)
    reasons: list[str] = []
    adopted_from: str | None = None
    if not evidence:
        reasons.append("no recorded client verification for this environment")
    else:
        if evidence.get("environmentId") != row["environment_id"]:
            if _family_evidence_applies(evidence, row):
                adopted_from = str(evidence.get("recordedEnvironmentId"))
            else:
                reasons.append("recorded verification is bound to a different environment")
        if evidence.get("clientKind") != row["client_kind"]:
            reasons.append("recorded verification is bound to a different client kind")
        recorded = evidence.get(
            "projectionConfigFingerprint", evidence.get("configFingerprint")
        )
        if adopted_from is not None:
            # The adopted observation proves the shared DSH MCP client, not this
            # profile's own document, and profiles of one Harness differ by
            # design. The recording environment still guards its own evidence.
            pass
        elif recorded != _harness_config_fingerprint(row):
            reasons.append("the enrolled client configuration changed after this verification")
        identity = evidence.get("versionIdentity")
        fingerprint = evidence.get("versionFingerprint")
        if not isinstance(identity, dict) or not identity:
            reasons.append("recorded verification carries no version identity")
        elif not isinstance(fingerprint, str):
            reasons.append("recorded verification carries no version fingerprint")
        else:
            try:
                derived = version_fingerprint(identity)
            except ProbeValidationError:
                derived = None
            if derived != fingerprint:
                reasons.append(
                    "recorded version fingerprint does not match its version identity"
                )
    return {
        "evidence": evidence,
        "capabilities": evidence.get("capabilities", {}),
        "fresh": not reasons,
        "reasons": reasons,
        "familyAdopted": adopted_from is not None,
        "adoptedFromEnvironmentId": adopted_from,
        "versionFingerprint": evidence.get("versionFingerprint"),
        "versionIdentity": evidence.get("versionIdentity"),
        # Legacy tolerance stays read-only: an old blob may still report when it
        # was observed, but without a version fingerprint it never applies.
        "observedAt": evidence.get("observedAt", evidence.get("verifiedAt")),
        "recordedAt": evidence.get("recordedAt"),
    }


def _peer_recheck_state(evidence: Any, current: list[str] | None) -> dict[str, Any]:
    """Whether a Bridge Agent should re-observe because the peer set changed.

    A new or retired peer MCP does not change what a Harness itself can do, so
    this never invalidates recorded evidence and never narrows a catalog by
    itself - it asks for a fresh bounded observation of the same environment.
    """
    recorded: list[str] | None = None
    if isinstance(evidence, dict) and isinstance(evidence.get("peerServers"), list):
        recorded = sorted({str(item) for item in evidence["peerServers"]})
    if recorded is None or current is None:
        return {
            "recheckRequired": False,
            "recheckReasons": [],
            "newPeerServers": [],
            "retiredPeerServers": [],
            "peerServersObserved": recorded,
        }
    listed = sorted({str(item) for item in current})
    new = sorted(set(listed) - set(recorded))
    retired = sorted(set(recorded) - set(listed))
    reasons: list[str] = []
    if new:
        reasons.append(
            "new peer MCP entered this bridge after the verification: " + ", ".join(new)
        )
    if retired:
        reasons.append(
            "a peer MCP left this bridge after the verification: " + ", ".join(retired)
        )
    return {
        "recheckRequired": bool(reasons),
        "recheckReasons": reasons,
        "newPeerServers": new,
        "retiredPeerServers": retired,
        "peerServersObserved": recorded,
    }


def _mirrored_peer_server_ids(connection: sqlite3.Connection) -> list[str] | None:
    """Local mirrored peer ids; None when this projection has never been synced.

    An empty list means "synced, and no peer MCP was registered"; None means
    "not comparable", so a recorded peer set is never compared against a mirror
    that was simply never refreshed.
    """
    if _mirrored_peer_fingerprint(connection) is None:
        return None
    try:
        rows = connection.execute(
            "SELECT server_id FROM peer_projection_state WHERE enabled = 1"
        ).fetchall()
    except sqlite3.Error:
        return None
    return sorted(str(row["server_id"]) for row in rows)


def _mirrored_peer_fingerprint(connection: sqlite3.Connection) -> str | None:
    try:
        row = connection.execute(
            "SELECT value FROM projection_meta WHERE key = 'peer_fingerprint'"
        ).fetchone()
    except sqlite3.Error:
        return None
    return str(row["value"]) if row is not None else None



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
    peer_servers: list[str] | None = None,
) -> dict[str, Any]:
    """Per-aspect adaptation report for one enrollment, with the next observation."""
    state = _harness_evidence_state(row)
    recheck = _peer_recheck_state(state["evidence"], peer_servers)
    mode = _env_row_field(row, "tool_exposure", "native")
    try:
        effective = _effective_tool_exposure(row)
        exposure_error = None
    except BridgeError as exc:
        effective, exposure_error = "invalid", str(exc)
    capabilities = state["capabilities"] if isinstance(state["capabilities"], dict) else {}
    items: list[dict[str, Any]] = []
    actions: list[str] = []
    if recheck["recheckRequired"]:
        actions.append(
            "re-observe this environment with a fresh prepared challenge: "
            + "; ".join(recheck["recheckReasons"])
        )
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
            action = ("recorded " + str(_utc_text(state["observedAt"]))
                      + " under version fingerprint "
                      + str(state["versionFingerprint"] or "unknown")
                      + "; no expiry applies - re-observe when the client, its "
                      "configuration, or the peer MCP set changes")
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
        "evidenceFamilyAdopted": state["familyAdopted"],
        "evidenceAdoptedFromEnvironmentId": state["adoptedFromEnvironmentId"],
        "observedAt": state["observedAt"],
        "recordedAt": state["recordedAt"],
        "versionFingerprint": state["versionFingerprint"],
        "versionIdentity": state["versionIdentity"],
        "recheckRequired": recheck["recheckRequired"],
        "recheckReasons": recheck["recheckReasons"],
        "newPeerServers": recheck["newPeerServers"],
        "retiredPeerServers": recheck["retiredPeerServers"],
        "peerServersObserved": recheck["peerServersObserved"],
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
    component = Path(__file__).resolve().parents[1] / f"{side}-bridge-mcp" / "bridge.py"
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
        component = Path(__file__).resolve().parents[1] / f"{side}-bridge-mcp" / "bridge.py"
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
    tool_exposure: str | None = None,
) -> dict[str, Any]:
    """Enroll exactly one revalidated scanned candidate.

    ``tool_exposure`` is the exposure mode recorded for the new environment.
    ``None`` selects the per-client-kind default
    (``_default_enrolled_tool_exposure``): ``auto`` for DeepSeek Harness
    profiles, ``native`` for every other client. ``deferred`` is rejected here
    because it is only reachable through probe evidence recorded for that
    environment after enrollment.

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
    enrolled_tool_exposure = (
        _default_enrolled_tool_exposure(kind) if tool_exposure is None else tool_exposure
    )
    if enrolled_tool_exposure not in {"native", "auto", "deferred"}:
        raise BridgeError("unknown tool exposure mode")
    if enrolled_tool_exposure == "deferred":
        raise BridgeError(
            "deferred exposure cannot be selected at enrollment: it requires "
            "refresh and model-exposure probe evidence recorded for this "
            "environment; enroll with auto and record a client verification"
        )
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
            "confirmed_at_ns, created_at_ns, updated_at_ns, tool_exposure"
            ") VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
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
                enrolled_tool_exposure,
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
    from installer.harness_verification import create_probe
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


def _declared_client_version(value: Any) -> str | None:
    """Bounded, optional product version the recording Agent observed.

    It is pinned inside the version fingerprint for later comparison, and it is
    never treated as capability evidence: only the observation is.
    """
    if value is None or value == "":
        return None
    if not isinstance(value, str) or not 1 <= len(value) <= 64:
        raise BridgeError("declared client version must be 1-64 printable characters")
    if not all(32 <= ord(character) < 127 for character in value):
        raise BridgeError("declared client version must be printable ASCII")
    return value


def projection_record_client_verification(
    *, projection: Path, environment_id: str, receipt_file: Path,
    tool_exposure: str = "auto", aspect: str | None = None,
    client_version: str | None = None, confirm: bool = False, dry_run: bool = False,
) -> dict[str, Any]:
    """Validate and consume one probe receipt before recording client evidence.

    One receipt observes the whole environment, so ``aspect`` only selects which
    prepared challenge is consumed.  When it is omitted, a single outstanding
    challenge is consumed and several require an explicit choice.

    The record pins the observed version identity and the timestamp, plus the
    peer server set that was mirrored at recording time.  It records no validity
    window: later peer registration is answered by re-observing, not by expiry.

    A DeepSeek Harness observation is recorded as family-scoped and is offered to
    sibling Harness profiles that have no observation of their own; see
    ``_adopt_family_evidence``.
    """
    from installer.harness_verification import validate_probe_receipt
    family_adopted: list[dict[str, str]] = []
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
        # Pin what was observed - identity and time - instead of a validity
        # window, and pin the mirrored peer set the Agent can re-check against.
        from installer.harness_verification import version_fingerprint
        declared = _declared_client_version(client_version)
        identity = dict(evidence["versionIdentity"])
        identity["declaredClientVersion"] = declared
        evidence["versionIdentity"] = identity
        evidence["versionFingerprint"] = version_fingerprint(identity)
        peer_fingerprint = _mirrored_peer_fingerprint(connection)
        evidence["peerFingerprint"] = peer_fingerprint
        evidence["peerServers"] = (
            _mirrored_peer_server_ids(connection) if peer_fingerprint is not None else None
        )
        if row["client_kind"] == CLIENT_KIND_DSH:
            # One Harness on one host has several enrolled profiles that share
            # one MCP client. Record the observation as family-scoped so an
            # identical-seeming sibling is not asked for a second probe of the
            # same client, and name the environment it was actually observed in.
            evidence["scope"] = FAMILY_EVIDENCE_SCOPE
            evidence["recordedEnvironmentId"] = environment_id
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
            if row["client_kind"] == CLIENT_KIND_DSH:
                family_adopted = _adopt_family_evidence(
                    connection, database, source_row=row,
                    evidence_json=updated["harness_verification_json"],
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
        "observedAt": evidence.get("observedAt"), "recordedAt": evidence.get("recordedAt"),
        "versionFingerprint": evidence.get("versionFingerprint"),
        "declaredClientVersion": (
            evidence.get("versionIdentity", {}).get("declaredClientVersion")
            if isinstance(evidence.get("versionIdentity"), dict) else None
        ),
        "peerServers": evidence.get("peerServers"),
        "familyAdoptedEnvironments": family_adopted,
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
    peer_servers: list[str] | None = None,
) -> dict[str, Any]:
    """Capability checklist for read-only reporting; never fails a whole report."""
    try:
        return _capability_checklist(row, outstanding, peer_servers)
    except BridgeError as exc:
        return {
            "environmentId": row["environment_id"],
            "error": str(exc),
            "toolExposure": _env_row_field(row, "tool_exposure", "native"),
            "effectiveToolExposure": None,
            "toolExposureError": str(exc),
            "toolExposureBlockers": [str(exc)],
            "evidenceFresh": False,
            "evidenceReasons": [str(exc)],
            "recheckRequired": False,
            "recheckReasons": [],
            "newPeerServers": [],
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
        peer_servers = _mirrored_peer_server_ids(connection)
        selected = [
            (row, _outstanding_client_probes(connection, row["environment_id"]))
            for row in rows
            if environment_id is None or row["environment_id"] == environment_id
        ]
        reports = [
            (row, _safe_capability_report(row, outstanding, peer_servers))
            for row, outstanding in selected
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
            "toolExposureError": report.get("toolExposureError"),
            "toolExposureBlockers": report["toolExposureBlockers"],
            "evidenceFresh": report["evidenceFresh"],
            "evidenceReasons": report["evidenceReasons"],
            "evidenceFamilyAdopted": bool(report.get("evidenceFamilyAdopted")),
            "evidenceAdoptedFromEnvironmentId": report.get(
                "evidenceAdoptedFromEnvironmentId"
            ),
            "observedAt": report.get("observedAt"),
            "recordedAt": report.get("recordedAt"),
            "versionFingerprint": report.get("versionFingerprint"),
            "versionIdentity": report.get("versionIdentity"),
            "recheckRequired": bool(report.get("recheckRequired")),
            "recheckReasons": report.get("recheckReasons", []),
            "newPeerServers": report.get("newPeerServers", []),
            "retiredPeerServers": report.get("retiredPeerServers", []),
            "peerServersObserved": report.get("peerServersObserved"),
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
        peer_servers = _mirrored_peer_server_ids(connection)
        environments = [_environment_summary(row) for row in rows]
        capability = {
            row["environment_id"]: _safe_capability_report(
                row, _outstanding_client_probes(connection, row["environment_id"]),
                peer_servers,
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
            summary["versionFingerprint"] = report.get("versionFingerprint")
            summary["observedAt"] = report.get("observedAt")
            summary["evidenceFamilyAdopted"] = bool(report.get("evidenceFamilyAdopted"))
            summary["evidenceAdoptedFromEnvironmentId"] = report.get(
                "evidenceAdoptedFromEnvironmentId"
            )
            summary["recheckRequired"] = bool(report.get("recheckRequired"))
            summary["recheckReasons"] = report.get("recheckReasons", [])
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


def _projection_side_local_port(side: str) -> int:
    return 8769 if side == "wsl" else 8768
