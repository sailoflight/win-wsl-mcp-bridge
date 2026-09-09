# MCP Tool Exposure and Harness Verification

The default remains a complete, deterministic per-server tool catalog. A Harness
with native tool search can use that catalog while sending only selected schemas
to its model. MCP discovery, model-visible schemas, and process startup are
separate costs; a successful `tools/list` says nothing about model-context cost.

An optional **legacy stdio** `deferred-mcp <registered-id>` surface keeps one
library entry visible until the Agent selects it. This is a connection-scoped
view, not a permission boundary or a guarantee of per-conversation isolation.
Two facade processes have independent views; if a Harness shares one MCP
connection across multiple Agents, those Agents also share that view.

## Research basis

Research retrieved on 2026-09-08, before implementation:

| Harness | Evidence | Implication |
|---|---|---|
| Installed DSH 0.1.1-rc.2 | Packaged `@deepseek-ai/dsh-mcp-client/lib/index.js` eagerly connects and drains tool pages (150–172, 636–642); notification handler queues refresh (539–551, 626–633). Native `@deepseek-ai/dsh-tools/lib/index.js` emits all visible definitions (2713–2719). No native MCP deferred-search configuration was found in the inspected client/core. | Full discovery currently produces full native model schemas in the inspected path. Refresh support must still be verified in the actual Agent environment. Other plugins or execution modes are not covered by that inspection. |
| Claude Code | [Current Tool Search documentation](https://code.claude.com/docs/en/mcp#scale-with-mcp-tool-search) says unset `ENABLE_TOOL_SEARCH` defers MCP tools by default; `auto` is the optional 10% threshold mode. [Dynamic updates](https://code.claude.com/docs/en/mcp#dynamic-tool-updates) are documented. | Prefer complete native discovery and the Harness's own search. Documentation does not prove the installed model/settings use it. |
| Codex | [Pinned source](https://github.com/openai/codex/blob/d6489472f3c15e87d2d7763a5fde033545c530f8/codex-rs/core/src/tools/handlers/tool_search_spec.rs#L93) implements source-summary search and conditional schema deferral. At the same pin, [the notification handler](https://github.com/openai/codex/blob/d6489472f3c15e87d2d7763a5fde033545c530f8/codex-rs/rmcp-client/src/logging_client_handler.rs#L86) only logs tool-list changes. | Native search and catalog refresh are independent capabilities. Do not assume expansion works in a stock Codex build merely because search exists. |
| OpenAI API | [Tool Search](https://developers.openai.com/api/docs/guides/tools-tool-search) supports deferred namespaces/server summaries and selected definitions. | Host/API optimization substantially overlaps library expansion and is usually finer-grained. It is not MCP capability negotiation. |

DSH's MCP SDK dispatches notifications asynchronously, while its Agent loop
snapshots schemas before `agent/pre-step`. Sending a notification before a tool
response is **not** an acknowledgement that the next model request includes the
new schemas. An expansion result reports that refresh was requested; it never
claims model readiness. Verification must observe the model boundary as well as
protocol traffic. An unchanged initial catalog can preserve the Harness's cached
prompt prefix; changed schemas can invalidate that prefix.

## Protocol and compatibility boundary

The [legacy tools contract](https://modelcontextprotocol.io/specification/2025-11-25/server/tools)
advertises `tools.listChanged` on the **server**. Neither this flag, an empty
client capability object, `clientInfo.name`, nor a product version proves that a
Harness re-fetches or updates its model schema list.

The [2026-07-28 contract](https://modelcontextprotocol.io/specification/2026-07-28/server/tools)
uses a different subscription mechanism and forbids tool catalogs varying per
connection or as a side effect of other requests on that connection. This facade
does not claim modern discovery or subscriptions support. Modern/native HTTP
routes retain their existing behavior. An explicit deferred selection cannot be
projected onto an incompatible route; automatic selection falls back to native.
The ordinary byte-transparent `connect` and fixed two-tool `compatibility-mcp`
remain available and retain their contracts.

The deferred facade is tools-only. It preserves required downstream initialization
instructions before the Agent's first tool decision, projects only its supported
legacy tools surface, and does not advertise unimplemented resources, prompts,
tasks, or callback families. Descriptions use the peer registry's redacted
metadata. Tool listing never weakens downstream lease, confirmation, or mutation
checks. It never accepts a remote command, environment, or endpoint.

## Agent-driven verification and registration

Client capability data belongs to the **Agent environment enrollment**, not to the
business MCP's registration. There is no `hardness` column. Projection schema v5
adds `tool_exposure` and `harness_verification_json` to `agent_environments`;
transport capabilities and compatibility routes remain separate. Migrating v1–v4
keeps existing environment IDs, transport fields, projection fingerprints, and
outbox records. Old rows default to native exposure and empty/unknown evidence.

The Bridge-side Agent receives a verification task, prepares a harmless fixture,
runs it in an explicitly authorized isolated target Harness session, observes
that session, and records its evidence. No user-entered support boolean or
product-name lookup enables this feature. Preparing a probe does not invoke a
model, change a real Harness profile, or spend API quota.

1. `projection probe-client <environment-id> --probe-file <new-local-file> --dry-run`
   previews preparation. With `--confirm`, it writes a short-lived challenge,
   binds it to the enrolled environment and current configuration SHA-256, and
   returns the exact fixture command plus the expected receipt path.
2. The Bridge Agent runs the fixture in the authorized target session. The
   fixture starts with a bootstrap tool, emits a directory-change notification,
   observes a subsequent list containing a new canary schema, and requires the
   schema's proof token for the canary call. It records bounded metadata, not
   prompts, credentials, business calls, or full tool results. Preparing alone is
   never a successful verification.
3. The Agent separately inspects the target's actual model-context evidence.
   Model exposure and native search observations are bound attestations with an
   evidence reference/digest, and are labeled separately from protocol evidence.
   A protocol client can read a schema and call a tool without a model seeing it;
   protocol traffic alone must leave model exposure unknown.
4. `projection record-client-verification <environment-id> --receipt <file>
   --tool-exposure auto --dry-run` validates the complete result. `--confirm`
   consumes the current challenge and stores verified evidence. Expired probes,
   changed configuration fingerprints, foreign environments, and replayed
   receipts are rejected. This updates enrollment only; applying a client
   configuration remains a separate `projection reconcile` operation.

The fixture is an offline stdio MCP, exposed by `python -m harness_verification`.
The Bridge Agent chooses a target Harness's supported isolation/configuration
workflow; the bridge does not invent a universal CLI for launching DSH, Codex,
or Claude Code, and does not execute any Harness or model implicitly. Actual
installation, client-profile changes and model costs require their own authority.
An unavailable observation remains unknown instead of being inferred from docs.

Evidence is scoped to the observed client build, protocol, configuration, and
model-context attestation. Challenge validity is 15 minutes; recorded verification
is valid for 24 hours. Re-verify after target build/model/settings changes. This is
a trusted-local observation contract: receipt validation checks binding and event
consistency, not cryptographic authentication against the local user or Agents.

| Recorded policy | Effective behavior |
|---|---|
| `native` (existing default) | Full catalog; let the Harness choose model exposure. |
| `auto` with native search supported, or any required evidence unknown/stale | Full catalog. |
| `auto` with fresh refresh and model-exposure evidence, native search observed unsupported, and a compatible route | Per-library deferred catalog. |
| `deferred` | Requires fresh refresh plus model-exposure evidence and the supported legacy stdio route; incompatible or stale evidence is an error. |

Configuration changes invalidate eligibility. A successful fingerprint-checked
Bridge projection may advance `projectionConfigFingerprint` while preserving the
original probe's `configFingerprint`; this allows its own approved exposure switch
without laundering an unrelated edit into verification evidence. There is no
renewal of the evidence lifetime during projection.

## Deferred view behavior

Initial listing contains only a compact library entry. Expansion reads a complete
bounded catalog transaction before publishing it, keeps exact tool identities and
schemas, and returns status rather than copying the full schema set into the tool
result. Collapse restores the small entry. Catalog errors, reserved-name
collisions, duplicate names, repeated cursors, and budget overflow cannot publish
a partial tool set. Listing follows the schema defined in `deferred_tools.py`;
this document intentionally does not duplicate the complete tool schema.

Backend notifications invalidate the cached catalog, including when idle. A
short connection loss preserves expansion intent, but requires reinitialization
and fresh discovery on demand. Business calls are not replayed after an uncertain
failure. A changed initialization contract requires a fresh client session.

## Verification and rollback

Development tests cover receipt/event validation separately from model
attestation, stale/foreign/replayed evidence, conservative policy selection,
old-schema migration, per-target projection, catalog paging and failures,
connection isolation, notifications and recovery. Run from the repository root:

```sh
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest tests.test_client_enrollment_verification tests.test_harness_verification tests.test_deferred_tools
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s tests -t .
```

Fixture success is not installed DSH/Codex/Claude acceptance. In particular it
does not resolve the DSH next-request scheduling race. Compare actual before/after
model requests to measure tool-schema count/bytes and whether native search or
bridge expansion supplied the savings. Historical tool results are unaffected.

The pre-feature source checkpoint is `eddd254`; the pre-checkpoint source archive
is `release-artifacts/before-deferred-20260908-181342.tar.gz`. Before any real
schema migration or client projection, the Operator must back up that host's
SQLite database and affected client configuration. To roll back a deployment,
restore a matching database/configuration snapshot with the older code. Do not
open schema v5 with old code or discard verification/history rows in place.
