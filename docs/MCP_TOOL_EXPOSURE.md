# MCP Tool Exposure and Harness Verification

The fallback for unverified environments remains a complete, deterministic
per-server catalog. The verified DSH Web environment is approved and already
configured for bridge dynamic (`deferred`) exposure. A Harness with native tool
search can use a complete catalog while sending selected schemas to its model.
MCP discovery, model-visible schemas, and process startup are separate costs; a
successful `tools/list` says nothing about model-context cost.

An optional **legacy stdio** `deferred-mcp <registered-id>` surface keeps one
library entry visible until the Agent selects it. This is a connection-scoped
view, not a permission boundary or a guarantee of per-conversation isolation.
Each connector process owns one MCP connection and therefore one independent
view; if a Harness shares one MCP connection across multiple Agents, those
Agents also share that view.

## Environment adaptation decision (revised 2026-09-09)

| Environment | Tool exposure decision | Evidence and activation boundary |
|---|---|---|
| DSH Web 0.1.1-rc.2, observed DS4F route | **Dynamic mode activation approved; Onshape and Taobao frontends already configured as `deferred-mcp`.** Expand only as needed and keep the view stable during related work. | User-observed Onshape request schemas completed the 54 -> 134 -> 54 round trip below. Taobao's collapsed entry was verified; no Taobao expansion/business-operation acceptance is inferred. |
| Other DSH profiles, builds, model/settings combinations | Eligible for dynamic mode after verifying that environment; retain native fallback until then. | Web-session evidence is not a field test of the TUI or headless profile. The earlier headless probe remained incomplete, rather than proving DSH incapable of refresh. |
| Claude Code | Keep complete native MCP discovery and prefer its native Tool Search. | Vendor documents native search and list-change support; exact local build/route/settings have not been field-tested here. Do not layer bridge deferral on top by default. |
| Codex | Keep complete native MCP discovery and use native search when enabled. | Inspected source implements tool search but only logs list-change notifications. Bridge dynamic activation is not verified for the installed client. |
| Other/unknown clients | Complete native catalog; fixed two-tool compatibility facade remains an explicit alternative. | No model-name or `clientInfo` heuristic activates dynamic mode. |
| Modern MCP 2026-07-28 / native HTTP routes | Keep the existing stable catalog and protocol-specific route. | This connection-scoped legacy stdio facade is not a supported dynamic mode for those contracts. |

Here DSH `tools.mode=native` is the model's tool-call representation, while bridge
`deferred-mcp` controls the size of `tools/list`. They can and do coexist; do not
switch DSH into `code` or `both` to enable this feature. Activation means the
frontends are ready with collapsed library entries, not that every MCP is expanded.
Runtime views are shared by Agents using the same MCP connection. Multiple DSH
TUI processes and multiple Codex processes normally have separate connectors,
so their views are independent even though all connectors attach to the same
host node and may share a business backend.

For TUI clients, expand/collapse is not a terminal widget operation. The
connector exposes the small library entry or the expanded downstream tools to
that TUI connection, and the client must refresh its catalog before using an
expanded set. Codex should use ordinary `connect` with its native complete
catalog or the explicit two-tool `compatibility-mcp` route; do not infer that
Codex's native search implies support for bridge-driven dynamic add/remove.

The Web profile activation is an explicitly approved configuration override using
the repository frontend, not a synthesized `harness_verification_json` receipt.
The database's `auto` policy still consumes fresh, bound verification evidence;
this revised compatibility decision neither invents a successful old canary
receipt nor overrides that validation by client name.

## Research basis

Research retrieved on 2026-09-08, before implementation:

| Harness | Evidence | Implication |
|---|---|---|
| Installed DSH 0.1.1-rc.2 | Packaged `@deepseek-ai/dsh-mcp-client/lib/index.js` eagerly connects and drains tool pages (150–172, 636–642); notification handler queues refresh (539–551, 626–633). Native `@deepseek-ai/dsh-tools/lib/index.js` emits all visible definitions (2713–2719). No native MCP deferred-search configuration was found in the inspected client/core. | Full discovery produces full native schemas in the inspected path. Subsequent user-observed Web/DS4F request snapshots verify bridge dynamic expansion and collapse (see below); that environment is activated. Other profiles/builds/plugins remain separately scoped. |
| Claude Code | [Current Tool Search documentation](https://code.claude.com/docs/en/mcp#scale-with-mcp-tool-search) says unset `ENABLE_TOOL_SEARCH` defers MCP tools by default; `auto` is the optional 10% threshold mode. [Dynamic updates](https://code.claude.com/docs/en/mcp#dynamic-tool-updates) are documented. | Prefer complete native discovery and the Harness's own search. Documentation does not prove the installed model/settings use it. |
| Codex | [Pinned source](https://github.com/openai/codex/blob/d6489472f3c15e87d2d7763a5fde033545c530f8/codex-rs/core/src/tools/handlers/tool_search_spec.rs#L93) implements source-summary search and conditional schema deferral. At the same pin, [the notification handler](https://github.com/openai/codex/blob/d6489472f3c15e87d2d7763a5fde033545c530f8/codex-rs/rmcp-client/src/logging_client_handler.rs#L86) only logs tool-list changes. | Native search and catalog refresh are independent capabilities. Do not assume expansion works in a stock Codex build merely because search exists. |
| OpenAI API | [Tool Search](https://developers.openai.com/api/docs/guides/tools-tool-search) supports deferred namespaces/server summaries and selected definitions. | Host/API optimization substantially overlaps library expansion and is usually finer-grained. It is not MCP capability negotiation. |

DSH's MCP SDK dispatches notifications asynchronously, while its Agent loop
snapshots schemas before `agent/pre-step`. The supported interaction is a
**request-boundary transition**: expand/collapse commits the bridge directory in
the calling step and requests re-listing; the refreshed definitions are used by a
subsequent model request. A later step in the same conversation turn is sufficient;
a new human message is not required. Do not batch a view switch and a downstream
call in one model request. Continue only with tools present in the refreshed
request's definitions; stale definitions mean pending client refresh, not a
failed directory switch. Do not repeatedly expand/collapse to force a refresh.

Notification delivery is not a synchronous model-readiness barrier, nor is such
a barrier required for this mode. `refreshRequested` is local to the response:
true means that call requested a notification, and false (including `status`)
is **not** evidence of client acknowledgement. `state`, `toolCount`, and catalog
revision describe the bridge only. Neither successful dispatch of a known tool
name nor a model's retrospective enumeration proves schema injection. Prefer the
actual request's Context Browser tool-definition snapshot; a model may describe
historical tools even after they have been removed from the current request.
An unchanged catalog can preserve the cached prompt prefix; changed schemas can
invalidate that prefix.

### Observed DSH + DS4F round trip (2026-09-09)

The user supplied Context Browser request snapshots from the `context-4f`
sub-session (`newapi/deepseek-v4-flash`) after explicitly switching the Web
Onshape/Taobao connections to the repository facade:

| Request observation | Tool definitions | Estimated tool tokens | Onshape | Taobao |
|---|---:|---:|---|---|
| Initial collapsed snapshot | 54 | 11.4k | Library entry only | Library entry only |
| Turn 4, step 1, 10:46:39, after expand | 134 (+80) | 27.7k (+16.3k) | 80 downstream tools plus library entry | Library entry only |
| Turn 5, step 5, 10:50:32, after collapse | 54 (-80) | 11.4k (-16.3k) | Library entry only | Library entry only |

Expanded request total: estimated 34.5k, actual prompt 33.3k. Post-collapse
request total: estimated 25.4k, actual prompt 25.1k. Total prompt figures also
include accumulated conversation history, so only the tool-definition component
supports the stated 16.3k removal. `toolCount=80` excludes the bridge entry;
81 visible Onshape definitions is an exact accounting distinction, not tolerance.
The user also supplied upstream usage records: at 10:50:32, 783 uncached input
plus 24,320 cached input tokens equals 25,103, consistent with the browser's
25.1k actual prompt. Adjacent upstream input totals changed as well. Cache-hit
counts are corroborating request-accounting evidence, not a schema inventory or
proof of why the model made its stale self-report. Recalling names from prior
conversation is a plausible explanation; the request snapshot is authoritative.

**Field verdict:** expansion and collapse both reached the observed model-request
schema surface. The earlier isolated run ended with an incomplete canary probe;
that remains an incomplete observation and must not be rewritten as a completed
receipt. The newer user-supplied request snapshots establish a successful Web
round trip, not a universal latency guarantee or proof for other builds/routes.
The DS4F self-report claiming old schemas remained after collapse conflicts with
the turn-5 snapshot and is not used as the oracle. No Onshape model mutation or
Taobao operation was required. This field evidence does not automatically write
or renew the production enrollment database.

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
A *new* enrollment records the per-client-kind default instead of that legacy
schema default (`enroll --tool-exposure`, `auto` for DeepSeek Harness profiles
and `native` for every other client), so enrolling a second profile of one
Harness no longer pins that profile to the complete fixed catalog on its own.
Recording `auto` changes no catalog by itself: without applicable evidence it
still resolves to `native`. `deferred` is never an enrollment default.

The Bridge-side Agent receives a verification task, prepares a harmless fixture,
runs it in an explicitly authorized isolated target Harness session, observes
that session, and records its evidence. No user-entered support boolean or
product-name lookup enables this feature. Preparing a probe does not invoke a
model, change a real Harness profile, or spend API quota.

1. `projection probe-client <environment-id> --probe-file <new-local-file>
   --aspect <aspect> --dry-run` previews preparation. With `--confirm`, it writes
   a short-lived challenge, binds it to the enrolled environment and current
   configuration SHA-256, and returns the exact fixture command, the expected
   receipt path, the Agent steps for that aspect, and the matching
   `record-client-verification` argv.
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
   protocol traffic alone must leave model exposure unknown. A refreshed later
   request (including another step of the same turn) is valid evidence; do not
   require instantaneous visibility in the call that triggered expansion. The
   complete bound probe and model-context evidence remain required for enrollment.
4. `projection record-client-verification <environment-id> --receipt <file>
   --aspect <aspect> --tool-exposure auto --dry-run` validates the complete
   result. `--confirm` consumes that aspect's challenge and stores verified
   evidence. Expired probes, changed configuration fingerprints, foreign
   environments, and replayed receipts are rejected. This updates enrollment
   only; applying a client configuration remains a separate `projection
   reconcile` operation.

The fixture is an offline stdio MCP, exposed by `python -m harness_verification`.
The Bridge Agent chooses a target Harness's supported isolation/configuration
workflow; the bridge does not invent a universal CLI for launching DSH, Codex,
or Claude Code, and does not execute any Harness or model implicitly. Actual
installation, client-profile changes and model costs require their own authority.
An unavailable observation remains unknown instead of being inferred from docs.

### Capability aspects: exploring registry support from an Agent

`projection probe-status` is the read-only starting point. It reports, per
enrolled environment, which registry decision each **aspect** has actually
established, why recorded evidence does or does not still apply, the recorded
version fingerprint and observation timestamps, whether the peer MCP set changed
since that observation, which prepared challenges are outstanding, and the next
concrete observation. It never writes, never launches a target, and never accepts
a capability from a product name:

```bash
python3 wsl-bridge-mcp/bridge.py projection probe-status \
    --projection ~/.local/state/win-wsl-mcp-bridge/projection.sqlite3
python3 wsl-bridge-mcp/bridge.py projection probe-status <environment-id> \
    --projection ~/.local/state/win-wsl-mcp-bridge/projection.sqlite3
```

An aspect names the Harness-side observation that can justify one registry or
enrollment decision. One bounded fixture run observes the whole environment, so
an aspect selects *which prepared challenge an Agent consumes and which decision
it is establishing*, not a different fixture.

| Aspect | Registry field | Observation | Establishes |
|---|---|---|---|
| `refresh` | `tool_exposure` | protocol-observed | The Harness re-lists after `notifications/tools/list_changed`. |
| `harness-protocol` | `compatibility_route` | protocol-observed | The negotiated revision is one this host also verified. |
| `model-exposure` | `tool_exposure` | challenge-bound attestation | The model is actually shown the discovered tool definitions. |
| `native-search` | `tool_exposure` | challenge-bound attestation | The Harness has (or lacks) its own tool search, which decides automatic deferral. |
| `modern-protocol` | `compatibility_route` | not observable here | Deliberately unclaimed: the bounded fixture speaks legacy revisions only. |

Each aspect is prepared and consumed as its own challenge (meta key
`client_probe:<environment-id>:<aspect>`), so an Agent can work one aspect at a
time without a later preparation overwriting an earlier one. A challenge prepared
before per-aspect keys (`client_probe:<environment-id>`) stays readable and
recordable and is reported as `legacy`: its intent is unknown, so it is never
reported as a specific aspect. `probe-client --aspect` defaults to `refresh`;
`record-client-verification` consumes the only outstanding challenge when there
is exactly one and otherwise requires `--aspect`. Every prepared challenge stays
visible in `probe-status` until it is consumed.

Each aspect state is one of:

| State | Meaning |
|---|---|
| `supported` / `unsupported` | Recorded evidence for this exact environment, client kind, configuration fingerprint, and version identity — or, for DeepSeek Harness profiles only, an explicitly family-scoped observation adopted from a sibling profile of the same installation (reported as `evidenceFamilyAdopted` with its recording environment). |
| `unknown` | The recorded run did not establish it; never a negative claim. |
| `stale` | Evidence exists, but a configuration change, a missing/mismatched version identity, or a binding mismatch means it does not apply (reasons are reported). |
| `not-probed` | No recorded verification for this environment yet. |
| `not-observable` | A property of this probe version, never of the target Harness. |

A negative refresh verdict is deliberately narrow: it requires an *observed*
`notifications/tools/list_changed` followed by no post-notification catalog and a
closed window that kept running (the fixture's own deadline elapsed, or the
Harness exhausted its message budget). A session that merely ended, a challenge
that expired, and every fixture-side failure stay `unknown`, because a catalog
request the Harness pipelined before the notification is indistinguishable from
no request at all. This probe therefore observes Harness capability only: it
never measures a business MCP's tool count, tool-definition volume, or the
model-context token cost of exposing a catalog. Those are separate per-MCP
exposure observations derived from an observed catalog, and no aspect is a proxy
for them.

## Recorded evidence: version fingerprint plus timestamps

The registry records *what* was observed and *when*, never a validity window:

| Field | Meaning |
|---|---|
| `versionIdentity` | `clientKind`, the MCP client identity seen on the wire (`observedClient`), the negotiated `protocolVersion`, the enrolled `configFingerprint`, and an optional `declaredClientVersion` supplied with `record-client-verification --client-version` (pinned, and never capability evidence). |
| `versionFingerprint` | SHA-256 over that canonical identity (`harness_verification.version_fingerprint`). Any component change is a different identity. |
| `observedAt` | When the bounded observation window ended. |
| `recordedAt` | When this host accepted and recorded the receipt. |
| `peerServers` / `peerFingerprint` | The mirrored peer MCP set at recording time, or `null` when this projection had never synced a peer mirror. |

Evidence does not expire. It stops applying when the identity it was observed for
no longer matches — in practice the enrolled configuration content (which this
host can re-derive locally) — and a Bridge Agent re-observes instead of guessing.
Age alone is never a reason: `probe-status` reports the timestamps so an Agent can
judge how old an observation is.

**One recorded exception, for DeepSeek Harness only.** One Harness installation is
several enrolled profiles (`web` / `dsh-tui` / `headless`) that share one MCP
client package on one host, so an observation of that client covers the family
rather than one profile's file. A recorded DSH observation therefore carries
`scope: dsh-family` and the `recordedEnvironmentId` it was observed in, and it is
offered to sibling Harness profiles that have **no observation of their own**; a
profile observed directly is never overwritten, and the recording profile keeps
the per-configuration guard for its own evidence. Adoption is never inferred from
a product or client name, only DSH rows may adopt, and the adopted row's recorded
mode becomes `auto` — the observation is evidence, so this host's policy still
decides what it means. `probe-status` and `projection status` report
`evidenceFamilyAdopted` and `evidenceAdoptedFromEnvironmentId` so an adopted
catalog is never presented as a profile's own observation. Non-DSH clients keep
the exact-environment binding.

A **new peer MCP entering the bridge is the re-check trigger**: `probe-status` and
`projection status` compare the current local peer mirror with the recorded
`peerServers` and report `recheckRequired`, `recheckReasons`, `newPeerServers`,
and `retiredPeerServers`, plus a *re-observe* next action. This is a request for a
fresh observation, not an invalidation: a newly registered business MCP says
nothing about what the Harness itself can do, so it never turns `deferred`
exposure into `native` on its own. Re-recording the environment against the
current peer set clears the request, and a mirror that was never refreshed is
reported as not comparable rather than as a change.

Challenge preparation lifetime is separate and unchanged: a prepared challenge
must be consumed within 15 minutes (and within the challenge's own 24-hour
verification bound). That bounds the challenge, not the recorded evidence.
Re-verify after target build, model, or settings changes. This is a
trusted-local observation contract: receipt validation checks binding and event
consistency, not cryptographic authentication against the local user or Agents.

| Recorded policy | Effective behavior |
|---|---|
| `native` (existing default) | Full catalog; let the Harness choose model exposure. |
| `auto` with native search supported, or any required evidence unknown/stale | Full catalog. |
| `auto` with applicable refresh and model-exposure evidence, native search observed unsupported, and a compatible route | Per-library deferred catalog. |
| `deferred` | Requires applicable refresh plus model-exposure evidence and the supported legacy stdio route; incompatible evidence is an error. |

Configuration changes invalidate eligibility. A successful fingerprint-checked
Bridge projection may advance `projectionConfigFingerprint` while preserving the
original probe's `configFingerprint`; this allows its own approved exposure switch
without laundering an unrelated edit into verification evidence. Projection never
rewrites the recorded identity or its timestamps.

## Deferred view behavior

Initial listing contains only a compact library entry. Expansion reads a complete
bounded catalog transaction before publishing it, keeps exact tool identities and
schemas, and returns status plus name-only directory changes rather than copying
full schemas or descriptions into the tool result. `addedTools` accompanies
expand; `removedTools` accompanies collapse and also an expand that refreshes
away old names. Repeated unchanged actions report empty name deltas. `status`
remains metadata-only. B′ guidance permits use in a subsequent model request
after client refresh and explicitly tells the Agent to re-expand through the
retained `bridge_library` entry when a remaining task needs removed tools; it
does not add A′'s mandatory proactive-definition-inspection step. Name deltas and
guidance are included in content text for every supported legacy version, and
also in structuredContent where supported. They are bridge-view deltas, not a
client acknowledgement or a diff against an observed model request.

The bridge retains only the last published names across catalog invalidation so
collapse can report what it is withdrawing without contacting an unavailable
backend. Those names never authorize calls or supply stale schemas. Successful
catalog refreshes update that name snapshot. The existing notification behavior
is preserved, including an explicit expand requesting re-listing even when
already expanded; do not use repeated expand as a refresh polling loop.

Collapse restores the small entry. Catalog errors, reserved-name
collisions, duplicate names, repeated cursors, and budget overflow cannot publish
a partial tool set. Listing follows the schema defined in `deferred_tools.py`;
this document intentionally does not duplicate the complete tool schema.

Backend notifications invalidate the cached catalog, including when idle. A
short connection loss preserves expansion intent, but requires reinitialization
and fresh discovery on demand. Business calls are not replayed after an uncertain
failure. A changed initialization contract requires a fresh client session.

## Cache-aware view lifetime

Use dynamic discovery to avoid loading libraries that are never needed, not as a
per-call open/close routine. Expand once when a task needs the library, group its
related work, and retain the expanded view across follow-up requests. Do not
collapse merely because a tool call or conversation turn ended. Collapse on user
request, or when the library will remain unused long enough that reduced context
outweighs a cache rebuild. Consider other Agents on the shared connection before
collapsing. Do not cycle views to poll refresh readiness or save a transient token
count; preserve tool order and a stable schema prefix where possible.

The user reports that, under the current billing conditions, a switch's uncached
input cost is approximately equivalent to **12.5 rounds of cached input**. Treat
this as an environment-specific cost signal, not a universal provider price or a
hard-coded 12.5-turn eviction timer. The invalidated prefix may include more than
the changed tool schemas, and cache-hit size and prices vary by request/provider.
As a planning comparison, leaving S unused schema tokens cached for N future
requests costs roughly `N * S * cached_input_rate`; weigh that against the
incremental uncached-prefix rebuild cost of collapse and any later re-expansion.
No exact break-even can be derived without measuring those sizes and rates.

The runtime guidance applies this policy without inventing a billing API, TTL,
automatic end-of-turn cleanup or forced collapse. A new connection still starts
collapsed; an already-expanded connection stays expanded until an explicit view
change (or connection lifecycle event). Current response schemas and safety gates
remain compatible.

## Verification and rollback

Development tests cover receipt/event validation separately from model
attestation, stale/foreign/replayed evidence, conservative policy selection,
old-schema migration, per-target projection, catalog paging and failures,
connection isolation, notifications and recovery. Run from the repository root:

```sh
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest tests.test_client_enrollment_verification tests.test_harness_verification tests.test_deferred_tools
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s tests -t .
```

Fixture success alone is not installed DSH/Codex/Claude acceptance. The separate
DSH field snapshots above establish expansion/collapse for the observed Web
session; they do not prove a synchronous next-request barrier or acceptance of
other clients. Compare actual request tool definitions, not historical names or
model self-reports, and record token/cache/latency observations separately.
Historical tool results and previously injected names are not erased by collapse.

The pre-feature source checkpoint is `eddd254`; the pre-checkpoint source archive
is `release-artifacts/before-deferred-20260908-181342.tar.gz`. Before any real
schema migration or client projection, the Operator must back up that host's
SQLite database and affected client configuration. To roll back a deployment,
restore a matching database/configuration snapshot with the older code. Do not
open schema v5 with old code or discard verification/history rows in place.
