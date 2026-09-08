#!/usr/bin/env python3
"""Pure MCP protocol-era constants and shaping for the shared dual-era
projection (P10 tools-only boundary): the modern ``2026-07-28`` envelope plus
the frozen legacy ``2024-11-05`` .. ``2025-11-25`` tools-profile field gates.

This module is deliberately pure: it encodes protocol-era contracts that
``bridge_runtime.SharedBackend`` must emit toward Agent clients (modern per-
request metadata/discovery and, for legacy clients that completed the legacy
initialize handshake, four-profile projection of backend tool catalogs and
call results).  It imports nothing from the runtime, talks to no backend, and
holds no state, so the runtime stays a thin caller and the contracts stay
independently unit-testable.  It is standard-library only.

Normative primary sources read before coding (2026-07-28 and frozen legacy
revisions):

- Machine schema (source of truth for required fields):
  https://github.com/modelcontextprotocol/specification/blob/2026-07-28/schema/2026-07-28/schema.ts
  (legacy revisions likewise read from the tagged schema snapshots
  ``2024-11-05`` / ``2025-03-26`` / ``2025-06-18`` / ``2025-11-25``).
- Spec pages:
  https://modelcontextprotocol.io/specification/2026-07-28/changelog
  https://modelcontextprotocol.io/specification/2026-07-28/basic/versioning
  https://modelcontextprotocol.io/specification/2026-07-28/server/discover
  https://modelcontextprotocol.io/specification/2026-07-28/basic/transports/stdio
  https://modelcontextprotocol.io/specification/2026-07-28/basic/index

Contracts relied on here, as written by those sources:

- ``RequestMetaObject`` requires ``_meta.io.modelcontextprotocol/protocolVersion``
  (string) and ``_meta.io.modelcontextprotocol/clientCapabilities`` (object);
  ``clientInfo`` is optional (SHOULD).  ``ResultMetaObject.serverInfo`` is
  optional (SHOULD) on results.
- Every result response MUST carry ``resultType`` ("complete" | "input_required"
  | string); when a client receives a result from an earlier-protocol server
  that lacks ``resultType`` it MUST treat the result as "complete".
- ``DiscoverResult`` extends ``CacheableResult`` and MUST carry ``resultType``,
  ``ttlMs``, ``cacheScope`` ("public"|"private"), ``supportedVersions``
  (string[]), and ``capabilities`` (ServerCapabilities); ``instructions`` and
  ``_meta.serverInfo`` are optional.
- ``UnsupportedProtocolVersionError`` is code ``-32022`` with message
  "Unsupported protocol version" and ``data = {supported: string[], requested}``.
- Error-code policy (basic/index): ``-32000..-32019`` are implementation-defined;
  ``-32020..-32099`` are spec-reserved; the retired codes ``-32002`` and
  ``-32042`` MUST NOT be emitted.  ``MethodNotFoundError`` (``-32601``) is the
  specified error for a method the server does not implement, including one
  gated behind a server capability it did not advertise — which is exactly how
  this tools-only projection answers resources/prompts/completions/other modern
  methods.
- ``ListToolsResult`` extends ``PaginatedResult`` and ``CacheableResult``
  (``nextCursor?``); ``CallToolResult`` extends ``Result`` with ``content``
  (ContentBlock[], required), ``structuredContent?`` and ``isError?``.
- ``ProgressNotificationParams`` = {progressToken, progress, total?, message?};
  ``CancelledNotificationParams`` = {requestId, reason?}.  ``progressToken`` is
  a string or number and remains an un-namespaced ``_meta`` key in this era.

Boundary: this module defines NO optional modern families.  The projection must
never advertise tasks (now the ``io.modelcontextprotocol/tasks`` extension),
MRTR (``inputRequests``/``inputResponses``), ``subscriptions/listen``,
sampling/elicitation/roots flows, or logging, and must never emit a field or
capability that this module does not back.  Forwarded results are projected
through exact per-method allow-lists (tools/nextCursor; content/structuredContent/
isError), so legacy-era task-augmented or unknown top-level fields are withheld
from modern clients, and retired MCP error codes (-32002/-32042) are normalized
before they reach a modern client.
"""

from __future__ import annotations

from typing import Any

#: The single modern revision this projection implements.
MODERN_MCP_PROTOCOL_VERSION = "2026-07-28"
#: Modern revisions this projection supports for per-request (stateless) use.
#: Legacy revisions (2024-11-05 .. 2025-11-25) are served through the separate
#: initialize handshake in the runtime and are intentionally NOT listed here:
#: a modern client must not pick a legacy revision for per-request use.
SUPPORTED_MODERN_PROTOCOL_VERSIONS = ("2026-07-28",)

# ``_meta`` keys (RequestMetaObject / ResultMetaObject naming rules).
META_PROTOCOL_VERSION_KEY = "io.modelcontextprotocol/protocolVersion"
META_CLIENT_INFO_KEY = "io.modelcontextprotocol/clientInfo"
META_CLIENT_CAPABILITIES_KEY = "io.modelcontextprotocol/clientCapabilities"
META_SERVER_INFO_KEY = "io.modelcontextprotocol/serverInfo"
META_PROGRESS_TOKEN_KEY = "progressToken"

# Standard JSON-RPC codes.
JSONRPC_PARSE_ERROR = -32700
JSONRPC_INVALID_REQUEST = -32600
JSONRPC_METHOD_NOT_FOUND = -32601
JSONRPC_INVALID_PARAMS = -32602
JSONRPC_INTERNAL_ERROR = -32603

# MCP-specified codes in the -32020..-32099 reserved range (basic/index).
MCP_HEADER_MISMATCH = -32020
MCP_MISSING_REQUIRED_CLIENT_CAPABILITY = -32021
MCP_UNSUPPORTED_PROTOCOL_VERSION = -32022

#: Retired MCP codes that a modern server MUST NOT emit (basic/index).  A legacy
#: physical backend may still answer with them (e.g. -32042 "connection closed"
#: style transport errors); the modern-facing projection normalizes them.
RETIRED_MCP_ERROR_CODES = frozenset({-32002, -32042})

UNSUPPORTED_PROTOCOL_VERSION_MESSAGE = "Unsupported protocol version"

RESULT_TYPE_COMPLETE = "complete"
RESULT_TYPE_INPUT_REQUIRED = "input_required"

CACHE_SCOPE_PUBLIC = "public"
CACHE_SCOPE_PRIVATE = "private"
#: ttlMs == 0 means "immediately stale" (schema CacheableResult): the client MAY
#: re-fetch every time.  Honest for a live legacy backend whose list changes the
#: modern surface does not push (no subscriptions/listen in this phase).
CACHE_TTL_MS_ZERO = 0

#: Modern list-family results that MUST carry the CacheableResult fields once
#: the projection forwards them.  Tools-only intersection: just tools/list.
CACHEABLE_LIST_METHODS = frozenset({"tools/list"})
#: Modern methods this tools-only projection forwards to the physical legacy
#: backend (one serialized 2025-06-18 session).
FORWARDED_TOOLS_ONLY_METHODS = frozenset({"tools/list", "tools/call"})
#: Modern methods answered locally without touching the physical backend.
LOCAL_MODERN_METHODS = frozenset({"server/discover"})

#: MRTR retry fields that a client could only add after an InputRequiredResult
#: (resultType "input_required"); this projection never returns one, so their
#: presence means the client is retrying something this server never asked for.
_MRTR_RETRY_FIELDS = ("inputResponses", "requestState")


def is_supported_modern_version(version: str) -> bool:
    """True when ``version`` is a modern revision this projection implements."""
    return version in SUPPORTED_MODERN_PROTOCOL_VERSIONS


def params_of(message: Any) -> dict[str, Any] | None:
    """Return the request ``params`` object, or None when absent/not an object."""
    if not isinstance(message, dict):
        return None
    params = message.get("params")
    return params if isinstance(params, dict) else None


def meta_of(message: Any) -> dict[str, Any] | None:
    """Return the request ``params._meta`` object, or None when absent."""
    params = params_of(message)
    if params is None:
        return None
    meta = params.get("_meta")
    return meta if isinstance(meta, dict) else None


def protocol_version_entry(message: Any) -> tuple[bool, str | None]:
    """Return ``(present, version)`` for the modern per-request version key.

    ``present`` is True only when ``_meta.io.modelcontextprotocol/protocolVersion``
    exists; ``version`` is its value when it is a string, else None.  When the
    key is absent the message is not a modern-flagged request at all.
    """
    meta = meta_of(message)
    if meta is None or META_PROTOCOL_VERSION_KEY not in meta:
        return False, None
    value = meta[META_PROTOCOL_VERSION_KEY]
    return True, value if isinstance(value, str) else None


def validate_modern_request_meta(message: Any) -> list[str]:
    """Validate required per-request modern metadata; return problem strings.

    Checks only the fields the schema marks required or that this projection
    depends on for era isolation: ``protocolVersion`` must be a non-empty
    string, ``clientCapabilities`` must be an object (may be empty), and an
    optional ``clientInfo`` must be an object.  ``clientInfo`` contents are
    self-reported and MUST NOT change behavior, so only its type is checked.
    """
    problems: list[str] = []
    meta = meta_of(message)
    if meta is None:
        return ["_meta is required on every modern request"]
    version = meta.get(META_PROTOCOL_VERSION_KEY)
    if not isinstance(version, str) or not version:
        problems.append(f"{META_PROTOCOL_VERSION_KEY} must be a non-empty string")
    capabilities = meta.get(META_CLIENT_CAPABILITIES_KEY)
    if not isinstance(capabilities, dict):
        problems.append(f"{META_CLIENT_CAPABILITIES_KEY} must be an object")
    client_info = meta.get(META_CLIENT_INFO_KEY)
    if client_info is not None and not isinstance(client_info, dict):
        problems.append(f"{META_CLIENT_INFO_KEY} must be an object when present")
    return problems


def has_mrtr_retry_fields(message: Any) -> bool:
    """True when a modern request carries MRTR retry fields we never issued.

    The projection never returns ``resultType: "input_required"``, so a retry
    carrying ``inputResponses``/``requestState`` is invalid here and must be
    rejected rather than silently stripped (which would mis-handle the retry).
    """
    params = params_of(message)
    if params is None:
        return False
    return any(name in params for name in _MRTR_RETRY_FIELDS)


def sanitize_params_for_legacy(params: Any) -> dict[str, Any] | None:
    """Build the backend-bound params for a forwarded modern request.

    Drops the whole modern ``_meta`` block except an un-namespaced
    ``progressToken`` (string/number), so no client capability/identity claims
    ever reach the physical 2025-06-18 backend (whose client profile the runtime
    already fixes to an empty capability set).  Non-``_meta`` params such as
    ``cursor``, ``name``, and ``arguments`` pass through untouched.  Returns
    None when ``params`` is not an object (callers answer InvalidParams).
    """
    if not isinstance(params, dict):
        return None
    out = dict(params)
    meta = out.get("_meta")
    if isinstance(meta, dict):
        filtered: dict[str, Any] = {}
        token = meta.get(META_PROGRESS_TOKEN_KEY)
        if isinstance(token, (str, int, float)) and not isinstance(token, bool):
            filtered[META_PROGRESS_TOKEN_KEY] = token
        if filtered:
            out["_meta"] = filtered
        else:
            out.pop("_meta", None)
    return out


def unsupported_version_error(requested: Any) -> dict[str, Any]:
    """Build the exact ``UnsupportedProtocolVersionError`` (-32022) body."""
    return {
        "code": MCP_UNSUPPORTED_PROTOCOL_VERSION,
        "message": UNSUPPORTED_PROTOCOL_VERSION_MESSAGE,
        "data": {
            "supported": sorted(SUPPORTED_MODERN_PROTOCOL_VERSIONS),
            "requested": requested,
        },
    }


def invalid_params_error(message: str) -> dict[str, Any]:
    """Build a JSON-RPC ``InvalidParams`` (-32602) error body."""
    return {"code": JSONRPC_INVALID_PARAMS, "message": message}


def method_not_found_error(method: Any, reason: str) -> dict[str, Any]:
    """Build a JSON-RPC ``MethodNotFoundError`` (-32601) error body.

    Per the schema, -32601 is the error for a method the server does not
    implement, including one gated behind a capability it did not advertise.
    """
    name = method if isinstance(method, str) else "unknown"
    return {
        "code": JSONRPC_METHOD_NOT_FOUND,
        "message": f"{name}: {reason}",
    }


def normalize_backend_error(error: Any) -> Any:
    """Normalize a legacy backend error body for the modern-facing projection.

    Returns the error unchanged unless it uses a retired MCP code (-32002 or
    -32042) that modern servers MUST NOT emit; those become a JSON-RPC internal
    error (-32603) with the original message preserved and the retired code kept
    in ``data`` for diagnosis.  Legacy-era clients never pass through this
    function and keep the exact legacy error bytes.
    """
    if not isinstance(error, dict) or error.get("code") not in RETIRED_MCP_ERROR_CODES:
        return error
    message = error.get("message")
    normalized: dict[str, Any] = {
        "code": JSONRPC_INTERNAL_ERROR,
        "message": message if isinstance(message, str) else "legacy backend error",
        "data": {
            "code": "retired_mcp_error_code_normalized",
            "retiredCode": error["code"],
        },
    }
    if "data" in error:
        normalized["data"]["originalData"] = error["data"]
    return normalized


def discover_result(
    server_info: dict[str, Any],
    *,
    backend_tools: bool | None = None,
    backend_instructions: str | None = None,
) -> dict[str, Any]:
    """Build the modern ``DiscoverResult`` from an observed physical backend.

    Capability advertising is an intersection of what this projection can serve
    to a modern client and what the physical backend actually offered during its
    (Bridge-owned) initialize: ``tools`` is advertised only when ``backend_tools``
    is True, never invented.  ``listChanged`` is omitted (no modern change push
    in this phase).  Resources/prompts/completions/logging/tasks are never
    advertised regardless of what the backend offers, because this phase does not
    project those families with modern semantics.  Downstream ``instructions``
    are preserved verbatim when the backend supplied them and are never replaced
    by projection boilerplate.
    """
    capabilities: dict[str, Any] = {}
    if backend_tools:
        capabilities["tools"] = {}
    result: dict[str, Any] = {
        "resultType": RESULT_TYPE_COMPLETE,
        "supportedVersions": sorted(SUPPORTED_MODERN_PROTOCOL_VERSIONS),
        "capabilities": capabilities,
        "ttlMs": CACHE_TTL_MS_ZERO,
        "cacheScope": CACHE_SCOPE_PRIVATE,
        "_meta": {META_SERVER_INFO_KEY: server_info},
    }
    if isinstance(backend_instructions, str) and backend_instructions:
        result["instructions"] = backend_instructions
    return result


#: Keys the modern tools-only projection is allowed to surface on a forwarded
#: backend result, per the exact modern result schemas.  Everything else the
#: legacy backend emits (for example the 2025-11-25-era task augmentation) is
#: withheld: the projection never advertises those families, so it must never
#: fabricate or leak their fields onto the modern side.  ``_meta`` is kept only
#: to merge the serverInfo envelope; all other modern ``_meta`` keys are owned
#: by this module.
_LIST_RESULT_ALLOWED_KEYS = frozenset({"tools", "nextCursor", "_meta"})
_CALL_RESULT_ALLOWED_KEYS = frozenset({"content", "structuredContent", "isError", "_meta"})


def shape_modern_result(
    method: Any, result: Any, server_info: dict[str, Any]
) -> dict[str, Any]:
    """Shape one forwarded backend result for a modern request; fail closed.

    Raises ``ValueError`` when the result is not an object (a modern client must
    never receive a raw malformed result).  The projected surface is the exact
    modern result schema intersection for the method: ``tools.list`` keeps only
    tools/nextCursor, ``tools/call`` keeps only content/structuredContent/isError,
    and any other (legacy task-augmented or unknown) top-level field is withheld.
    The era-required envelope is enforced, never trusted from the legacy backend:
    ``resultType`` is set to "complete" and the CacheableResult ``ttlMs``/
    ``cacheScope`` (list family) are always the Bridge-owned 0/"private" values.
    """
    if not isinstance(result, dict):
        raise ValueError(
            "shared backend returned a non-object result for a modern request"
        )
    if method == "tools/list":
        allowed = _LIST_RESULT_ALLOWED_KEYS
    elif method == "tools/call":
        allowed = _CALL_RESULT_ALLOWED_KEYS
    else:
        raise ValueError(
            f"cannot project an unadvertised modern method result: {method!r}"
        )
    shaped = {key: result[key] for key in allowed if key in result}
    shaped["resultType"] = RESULT_TYPE_COMPLETE
    if method in CACHEABLE_LIST_METHODS:
        shaped["ttlMs"] = CACHE_TTL_MS_ZERO
        shaped["cacheScope"] = CACHE_SCOPE_PRIVATE
    meta = shaped.get("_meta")
    meta = dict(meta) if isinstance(meta, dict) else {}
    meta.setdefault(META_SERVER_INFO_KEY, server_info)
    shaped["_meta"] = meta
    return shaped


def shape_modern_tool_result(
    legacy_result: dict[str, Any], server_info: dict[str, Any]
) -> dict[str, Any]:
    """Shape a locally produced tool result (policy rejection, isError) modern.

    Adds the era-required ``resultType: "complete"`` and ``_meta.serverInfo``
    around an already-complete legacy tool-result object.  Fails closed on a
    non-object input, which only a local bug could produce.
    """
    if not isinstance(legacy_result, dict):
        raise ValueError("modern tool-result shaping requires an object result")
    shaped = dict(legacy_result)
    shaped["resultType"] = RESULT_TYPE_COMPLETE
    meta = shaped.get("_meta")
    meta = dict(meta) if isinstance(meta, dict) else {}
    meta.setdefault(META_SERVER_INFO_KEY, server_info)
    shaped["_meta"] = meta
    return shaped


# ---------------------------------------------------------------------------
# Frozen legacy (2024-11-05 .. 2025-11-25) tools-profile field gates
# ---------------------------------------------------------------------------
# Normative frozen-schema anchors (verified against the tagged schemas before
# coding, and shared with the standalone reverse-era adapter):
#   * Tool.annotations                        >= 2025-03-26
#   * CallToolResult.structuredContent        >= 2025-06-18
#   * Tool.outputSchema / Tool.title          >= 2025-06-18
#   * Tool.icons (plural list of Icon, each   >= 2025-11-25
#     carrying a string ``src``)
# ``name``/``description``/``inputSchema`` and ``content``/``isError`` exist in
# every frozen profile.  A negotiated revision never leaks a field that its
# schema does not define: these gates strip newer-era fields when a physical
# legacy backend that negotiated a higher revision answers a lower-revision
# logical client.  Capability families are NOT gated here: the shared runtime
# replays only what the observed physical backend actually advertised, so it
# never fabricates (task/sampling/elicitation support is never invented).
LEGACY_PROTOCOL_VERSIONS = (
    "2024-11-05",
    "2025-03-26",
    "2025-06-18",
    "2025-11-25",
)
#: Newest verified legacy revision (canonical legacy baseline).
LEGACY_CANONICAL_PROTOCOL_VERSION = LEGACY_PROTOCOL_VERSIONS[-1]

LEGACY_TOOL_ANNOTATIONS_MIN_VERSION = "2025-03-26"
LEGACY_TOOL_TITLE_OUTPUT_SCHEMA_MIN_VERSION = "2025-06-18"
LEGACY_STRUCTURED_CONTENT_MIN_VERSION = "2025-06-18"
LEGACY_TOOL_ICONS_MIN_VERSION = "2025-11-25"

#: Content-item kinds inside ``CallToolResult.content`` per frozen schema,
#: mapped to the first frozen revision whose ``ContentBlock`` union accepts
#: them (verified against the tagged schema snapshots):
#: 2024-11-05 = text/image/EmbeddedResource("resource");
#: 2025-03-26 adds AudioContent("audio");
#: 2025-06-18 adds ResourceLink("resource_link"); 2025-11-25 keeps the same
#: five kinds.  A kind a frozen profile cannot represent must never be
#: silently dropped from a successful business result -- the projection fails
#: explicitly instead.
LEGACY_CONTENT_KIND_MIN_VERSIONS = {
    "text": "2024-11-05",
    "image": "2024-11-05",
    "resource": "2024-11-05",
    "audio": "2025-03-26",
    "resource_link": "2025-06-18",
}
#: Content items and nested ``ResourceContents`` gained an optional ``_meta``
#: only in 2025-06-18; on older profiles that field is stripped so the item
#: stays representable.  (Content-item ``annotations`` exists on every frozen
#: profile and is never gated.)
LEGACY_CONTENT_ITEM_META_MIN_VERSION = "2025-06-18"
LEGACY_RESOURCE_CONTENTS_META_MIN_VERSION = "2025-06-18"

#: ServerCapabilities families per frozen schema, mapped to the first frozen
#: revision whose ``ServerCapabilities`` interface accepts them (verified
#: against the tagged schema snapshots):
#: 2024-11-05 = experimental/logging/prompts/resources/tools;
#: 2025-03-26 adds completions; 2025-11-25 adds tasks (list/cancel/requests).
#: Every family leaf set is identical across the four profiles otherwise
#: (tools = {listChanged?} only -- there is no ServerCapabilities-level
#: ``execution``; Tool-level ``execution`` is gated at tool projection).
LEGACY_CAPABILITY_FAMILY_MIN_VERSIONS = {
    "experimental": "2024-11-05",
    "logging": "2024-11-05",
    "prompts": "2024-11-05",
    "resources": "2024-11-05",
    "tools": "2024-11-05",
    "completions": "2025-03-26",
    "tasks": "2025-11-25",
}
#: The families the shared virtualizer actually implements for legacy logical
#: clients: tools/list + tools/call routing and the tools/list_changed
#: notification broadcast.  Every other family -- even one a 2025-11-25
#: physical backend legitimately advertises, such as ``tasks`` -- has no
#: implemented virtual route and is never replayed to a logical client.
LEGACY_IMPLEMENTED_SERVER_CAPABILITY_FAMILIES = ("tools",)


def legacy_server_capability_families(version: str) -> frozenset[str]:
    """ServerCapabilities families one frozen profile can represent."""
    if not is_verified_legacy_protocol_version(version):
        raise ValueError(f"not a verified legacy protocol version: {version!r}")
    return frozenset(
        family
        for family, introduced in LEGACY_CAPABILITY_FAMILY_MIN_VERSIONS.items()
        if legacy_revision_at_least(version, introduced)
    )


def project_legacy_server_capabilities(
    version: str, capabilities: Any
) -> dict[str, Any]:
    """Intersect an observed physical backend's capabilities onto one frozen
    logical profile.

    Only families the virtualizer implements are ever replayed, and only when
    the backend actually advertised them.  The physical backend may run at a
    newer actual revision (server-choose) and advertise ``tasks`` or other
    families the virtualizer does not implement -- those are withheld from
    every logical client, including a 2025-11-25 client.  Families the
    negotiated profile itself cannot represent (for example ``tasks`` at
    2025-06-18) and any unknown/ad-hoc family key are withheld identically, so
    an unrepresentable capability field never leaks.  ``tools`` is kept with
    its only frozen leaf ``listChanged`` when the backend advertises it; all
    other ``tools`` leaves are withheld.  The result never fabricates a family
    or leaf the backend did not advertise and never depends on unobserved
    revision data.
    """
    if not is_verified_legacy_protocol_version(version):
        raise ValueError(f"not a verified legacy protocol version: {version!r}")
    if not isinstance(capabilities, dict):
        return {}
    observed_tools = capabilities.get("tools")
    if not isinstance(observed_tools, dict):
        return {}
    projected: dict[str, Any] = {"tools": {}}
    if observed_tools.get("listChanged") is True:
        projected["tools"]["listChanged"] = True
    return projected


def is_verified_legacy_protocol_version(version: Any) -> bool:
    """True when ``version`` is one of the four frozen legacy profiles."""
    return isinstance(version, str) and version in LEGACY_PROTOCOL_VERSIONS


def legacy_revision_at_least(version: str, anchor: str) -> bool:
    """True when legacy ``version`` is at or newer than ``anchor``.

    Protocol dates compare lexicographically, which equals chronological order
    for this fixed set of ``YYYY-MM-DD`` revisions.
    """
    return version >= anchor


def legacy_tools_profile(version: str) -> dict[str, bool]:
    """Return the per-field era gates for one legacy tools profile.

    Keys: ``annotations``, ``title``, ``outputSchema``, ``structuredContent``,
    ``icons``.  Every other Tool/CallToolResult field exists in every frozen
    profile and is never gated here.
    """
    if not is_verified_legacy_protocol_version(version):
        raise ValueError(f"not a verified legacy protocol version: {version!r}")
    return {
        "annotations": legacy_revision_at_least(
            version, LEGACY_TOOL_ANNOTATIONS_MIN_VERSION
        ),
        "title": legacy_revision_at_least(
            version, LEGACY_TOOL_TITLE_OUTPUT_SCHEMA_MIN_VERSION
        ),
        "outputSchema": legacy_revision_at_least(
            version, LEGACY_TOOL_TITLE_OUTPUT_SCHEMA_MIN_VERSION
        ),
        "structuredContent": legacy_revision_at_least(
            version, LEGACY_STRUCTURED_CONTENT_MIN_VERSION
        ),
        "icons": legacy_revision_at_least(version, LEGACY_TOOL_ICONS_MIN_VERSION),
    }


def _project_legacy_tool_for_profile(
    profile: dict[str, bool], tool: Any
) -> dict[str, Any] | None:
    """Project one backend tool entry onto a computed legacy profile.

    Keeps the era-carrying keys only when the profile can represent them and
    withholds newer-era or unknown extension keys.  Returns None for a
    malformed entry (not a dict with a string ``name``) so a broken catalog
    fails cleanly instead of leaking.
    """
    if not isinstance(tool, dict) or not isinstance(tool.get("name"), str):
        return None
    out: dict[str, Any] = {"name": tool["name"]}
    if isinstance(tool.get("description"), str):
        out["description"] = tool["description"]
    if isinstance(tool.get("inputSchema"), dict):
        out["inputSchema"] = tool["inputSchema"]
    if profile["annotations"] and isinstance(tool.get("annotations"), dict):
        out["annotations"] = tool["annotations"]
    if profile["title"] and isinstance(tool.get("title"), str):
        out["title"] = tool["title"]
    if profile["outputSchema"] and isinstance(tool.get("outputSchema"), dict):
        out["outputSchema"] = tool["outputSchema"]
    if profile["icons"] and isinstance(tool.get("icons"), list):
        out["icons"] = tool["icons"]
    return out


def project_legacy_tool(version: str, tool: Any) -> dict[str, Any] | None:
    """Project one backend tool entry onto a frozen legacy profile.

    Validates the version, then applies the profile's era gates.  See
    ``_project_legacy_tool_for_profile`` for the field rules.
    """
    return _project_legacy_tool_for_profile(legacy_tools_profile(version), tool)


def project_legacy_tools_list(version: str, result: Any) -> dict[str, Any]:
    """Project a legacy ``tools/list`` result onto one frozen profile.

    Replaces the ``tools`` array with era-filtered copies of each entry
    (malformed entries are withheld rather than leaked).  Non-tools keys such
    as ``nextCursor`` pass through untouched.  Fails closed (ValueError) on a
    non-object result or an unverified legacy version.
    """
    if not isinstance(result, dict):
        raise ValueError("legacy tools/list projection requires an object result")
    # Validates the version even when the tools array is absent or empty.
    profile = legacy_tools_profile(version)
    tools = result.get("tools")
    if tools is None:
        return dict(result)
    if not isinstance(tools, list):
        raise ValueError("legacy tools/list projection requires a tools array")
    projected: list[dict[str, Any]] = []
    for tool in tools:
        entry = _project_legacy_tool_for_profile(profile, tool)
        if entry is not None:
            projected.append(entry)
    out = dict(result)
    out["tools"] = projected
    return out


def legacy_content_kinds(version: str) -> frozenset[str]:
    """The content-item kinds one frozen profile's CallToolResult can carry."""
    if not is_verified_legacy_protocol_version(version):
        raise ValueError(f"not a verified legacy protocol version: {version!r}")
    return frozenset(
        kind
        for kind, introduced in LEGACY_CONTENT_KIND_MIN_VERSIONS.items()
        if legacy_revision_at_least(version, introduced)
    )


def _project_content_item_meta(item: dict[str, Any], meta_allowed: bool) -> dict[str, Any]:
    """Copy one content item, keeping every payload field untouched and gating
    only the nested metadata fields the frozen profile cannot represent.

    ``annotations`` exists on content items in every frozen profile and always
    stays.  Item-level ``_meta`` (and, inside an EmbeddedResource, the nested
    ``resource._meta``) only exist from 2025-06-18 and are removed on older
    profiles so the otherwise-representable item remains valid.
    """
    out = dict(item)
    if not meta_allowed:
        out.pop("_meta", None)
        resource = out.get("resource")
        if isinstance(resource, dict):
            resource = dict(resource)
            resource.pop("_meta", None)
            out["resource"] = resource
    return out


def project_legacy_content_item(version: str, item: Any) -> dict[str, Any]:
    """Project one ``CallToolResult.content`` item onto a frozen legacy profile.

    A representable kind (text/image/resource in every profile; audio from
    2025-03-26; resource_link from 2025-06-18) passes through with its payload
    untouched except for nested metadata fields the profile cannot carry.  An
    unrepresentable or malformed item raises ValueError naming the kind and
    profile: the caller reports an explicit error instead of silently dropping
    business output and reporting success.
    """
    if not isinstance(item, dict) or not isinstance(item.get("type"), str):
        raise ValueError(
            f"content item is malformed on legacy profile {version!r}: "
            f"expected an object with a string type"
        )
    kind = item["type"]
    if kind not in legacy_content_kinds(version):
        raise ValueError(
            f"content kind {kind!r} is not representable on legacy profile "
            f"{version!r}"
        )
    meta_allowed = legacy_revision_at_least(
        version, LEGACY_CONTENT_ITEM_META_MIN_VERSION
    )
    return _project_content_item_meta(item, meta_allowed)


def project_legacy_tools_call_result(version: str, result: Any) -> dict[str, Any]:
    """Project a legacy ``tools/call`` result onto one frozen profile.

    ``content`` items are projected per kind (see ``project_legacy_content_item``);
    ``structuredContent`` is withheld below 2025-06-18; ``isError`` and every
    other top-level key pass through verbatim -- top-level ``Result`` objects
    carry an index signature plus ``_meta`` in every frozen profile, so unlike
    Tool entries they need no unknown-key stripping.  Fails closed (ValueError)
    on an unverified legacy version, a non-object result, a non-array content
    value, or any unrepresentable/malformed content item -- never a silent
    partial success.
    """
    if not isinstance(result, dict):
        raise ValueError("legacy tools/call projection requires an object result")
    profile = legacy_tools_profile(version)
    out = dict(result)
    if not profile["structuredContent"]:
        out.pop("structuredContent", None)
    if "content" in out:
        content = out["content"]
        if content is None:
            out.pop("content", None)
        else:
            if not isinstance(content, list):
                raise ValueError("legacy tools/call content must be an array")
            out["content"] = [
                project_legacy_content_item(version, item) for item in content
            ]
    return out
