# Milestone Design Baseline

This preserves the complete original development-plan requirements and historical
implementation notes. Status paragraphs below may predate later implementation;
consult `DEVELOPMENT_PLAN.md` for remaining gates, `IMPLEMENTATION_STATUS.md` for
run evidence, and `MCP_COVERAGE.md` for current capabilities. Requirements are not
removed when their implementation completes.

## Current completed baseline

- Standard stdio MCP is relayed in both directions over one WSL-initiated link,
  with per-stream acknowledgements and independent backpressure.
- Dedicated registrations remain byte-transparent. Shared registrations use a
  deterministic Bridge-owned initialize profile, heterogeneous supported MCP
  protocol revisions, one physical backend generation, serialized requests,
  capability-aware server-request routing, leases, and a fixed shared view.
- Supported Agent environments can be scanned without secrets, explicitly
  enrolled, projected, reconciled, and optionally polled through a separate
  per-host `projection.sqlite3`; official client CLIs are preferred and managed
  entries are fingerprinted and fail closed on drift.
- MCP-generated regular files use negotiated `artifacts/1`; resumable,
  content-addressed delivery and durable receipts use the separate negotiated
  `artifacts/2` revision without changing v1 semantics.
- Operator-authorized Agent-local files can be staged into a remote dedicated MCP
  through negotiated `artifact-inputs/1`; only exact minted descriptor literals
  are rewritten and both directions are byte/digest verified.
- `streamable_http_stdio.py` supplies a separately testable stdio-to-Streamable-
  HTTP adapter with sessions, SSE, cancellation, bounded errors, secret-safe
  diagnostics, and serialized 404 reinitialization.
- `archive_profile.py` supplies deterministic files-only ZIP packages with a
  hash manifest and strict bounded atomic extraction.
- The per-target `connect` proxy is a persistent connector (frozen
  `connector_core.py` core + reloadable `connector_engine.py` policy): it stays
  alive while Agent stdin is open, reconnects after node/local-stream loss,
  replays only the cached initialize/initialized handshake, never replays a
  business call, fails pending calls exactly once with Bridge JSON-RPC errors,
  preserves connected business payload bytes, carries `connectorVersion` in the
  connect handshake and `coreVersion` in the node reply, and warns on a stale
  node core only through initialize `instructions` and Bridge errors. Engine
  updates are swapped from an immutable manifest+hash-scanned directory with
  rollback to the built-in engine.
- A bounded static capability-catalog slice supports read-only list/search,
  direct-registration deduplication, and non-authoritative import planning while
  rejecting catalog launch, install, credential, and mutation authority.
- Public Registry MCP discovery remains peer-only and does not duplicate local
  MCPs. Versioned wheel/sdist metadata, per-host console entry points, `doctor`,
  and ephemeral Windows/WSL fixture acceptance are defined and tested locally.

## Next milestone: transport-equivalent projection, Agent control, and development evidence

### Product invariant

The next milestone keeps one invariant above implementation convenience:

> A peer MCP is projected into an Agent environment using the same MCP transport
> family by which it is registered on its owning host. A stdio registration
> appears as stdio; a Streamable HTTP registration appears as a loopback
> Streamable HTTP endpoint. The Bridge may relay that transport, but must not
> silently replace its session, initialization, cancellation, notification, or
> error semantics with a different transport.

This is registration/result equivalence, not an impossible promise that an
arbitrary Agent client will preserve an in-flight session across a process or
network restart. In particular, restarting the local stdio `connect` proxy alone
must never be reported as restarting the peer business MCP.

### P6 — Typed registry and transport-equivalent projection

**Implementation status:** contract slice implemented and covered offline: registry schema v4,
private typed transport/management data, redacted summaries, HTTP lifecycle observation with an
optional private registered bounded control contract for `external-controlled` rows, projection
schema v2 capability recording, explicit per-server unsupported-transport reporting with an
environment-wide preflight that preserves prior configuration and fingerprints (including DSH
failure reporting, dry-run state preservation, and recovery after a supported transport returns),
an executable HTTP-to-stdio compatibility route using the verified stdlib adapter, and an opt-in
native loopback HTTP relay with owner-local endpoint/header resolution, bounded request envelopes,
POST/GET/DELETE, JSON and SSE response streaming, and session-header forwarding. Managed HTTP
readiness/supervision is implemented with a private bounded policy, one owned process generation,
readiness-gated relay demand, no-overlap lifecycle control, and structured status; per-client native
projection remains open. The explicit stdio-to-HTTP
conversion data plane is implemented by the loopback `stdio_http_facade.py`, with an isolated
`bridge.py connect` subprocess per MCP session, bounded POST/GET/DELETE and JSON/SSE behavior;
automatic per-environment projection of that facade remains open. The plain `connect` proxy is now a
persistent connector (frozen `connector_core.py` core plus reloadable `connector_engine.py` policy):
it stays alive while Agent stdin is open, reconnects after node/local-stream loss, replays only the
cached initialize/initialized handshake, never replays a business call, fails each pending call
exactly once with a Bridge JSON-RPC error, keeps connected business payload bytes transparent, and
surfaces a stale node core only through initialize `instructions` and Bridge errors; the engine may
also be swapped from an immutable manifest+hash-scanned directory with rollback to the built-in
engine. The separate opt-in cached JSON-RPC compatibility facade with a constant two-tool surface for
clients that prune failed startup remains open.

#### Contract

Replace the implicit command-only registry row with a tagged, private transport
and ownership model:

- `stdio`: an Operator-authorized local command/args/cwd/env launch definition;
- `streamable-http`: an Operator-authorized owner-host loopback endpoint;
- HTTP ownership is explicit: `external` means availability/probe only, while
  `external-controlled` additionally registers a private bounded control
  contract the owning node may invoke for lifecycle actions, and `bridge-managed`
  additionally supplies a local launch definition and bounded readiness/shutdown
  policy owned by the Bridge (supervision still roadmap).

No peer request may supply an endpoint, command, arguments, environment, working
directory, headers, credentials, or ownership policy. Public summaries expose
only the transport family and redacted management capability, never the URL,
headers, launch definition, PID, or secret-bearing readiness details.

#### Relay and projection

- Stdio continues to project as the local component's `connect <registry-id>`
  command. Dedicated byte transparency and explicit shared JSON-RPC mode remain
  separate contracts.
- Streamable HTTP projects as a stable Agent-host loopback URL owned by the local
  Bridge node. Requests, responses, JSON/SSE bodies, MCP session identifiers,
  protocol-version headers, cancellation, and server events are relayed to the
  owning node, which connects only to the endpoint stored in its local registry.
- The initial supported HTTP scope is MCP Streamable HTTP over owner-host
  loopback. Public endpoints, OAuth/DCR, arbitrary reverse proxying, WebSocket,
  and legacy HTTP+SSE are not implied by this phase.
- Every enrolled Agent environment records a tested transport capability matrix.
  If its MCP client cannot represent native Streamable HTTP, enrollment or
  reconciliation reports `unsupported_transport` (for example, "this harness
  does not support HTTP MCP") and records that an explicit compatibility route
  is required; this is a Bridge compatibility finding, not an error emitted by
  the remote MCP.
- A later compatibility slice supplies both explicit conversion directions:
  `HTTP -> stdio` presents a resilient local stdio facade backed by an HTTP MCP,
  while `stdio -> HTTP` presents a stable loopback Streamable HTTP facade backed
  by a stdio MCP. Conversion is selected per enrolled environment and registry
  policy, is visible in projection/status as `converted`, and is never described
  as native transport equivalence. It must preserve the representable MCP
  initialize/tools/request/result/cancellation/event semantics and report any
  unavoidable semantic downgrade before configuration is applied.
- Projection fingerprints include the transport family and Bridge-owned endpoint
  identity. A transport change is a remove/add transition with rollback, not an
  in-place reinterpretation of an existing entry.
- The HTTP relay listener is loopback-only, has bounded headers/bodies/connections,
  isolates MCP ids, and stays available independently of one business endpoint so
  a temporary backend failure produces a structured HTTP outcome rather than
  removing the control plane.

#### Connector/node isolation and resilient facades

The long-running per-host Bridge `node` is the controller and transport owner;
each stdio `connect` process is a disposable Agent-client facade. Closing stdin,
killing the connector, an Agent session exit, or a client reconnect attempt may
close that logical facade but must never terminate the node, peer link, unrelated
streams, or a shared/HTTP backend solely because the connector disappeared.
Backend cleanup follows the registration's dedicated/shared policy, not the
connector's OS-parent relationship.

For Agent environments that prune an MCP after startup failure, prefer keeping a
projected connector initialized and protocol-responsive through transient peer or
backend outages. This requires an explicit resilient-facade contract rather than
pretending raw byte forwarding can synthesize unavailable business behavior:

- retain a bounded, revisioned last-known-good downstream initialize result and
  tool/resource/prompt catalogs only after successful observation;
- expose cached capabilities only when the client can receive list-changed
  notifications or the cache revision is pinned for that connector session;
- return compact structured MCP errors such as `backend_unavailable`,
  `backend_restarting`, `peer_unavailable`, and `reinitialize_required` from
  attempted operations instead of terminating the facade;
- never report a tool call as executed when it did not reach the matching backend
  generation, and never replay a call with uncertain completion;
- when no trustworthy catalog has ever been observed, keep the control MCP
  available and report the business endpoint as unavailable, but do not invent
  business tools or a fabricated initialize identity.

This resilient facade is necessarily a JSON-RPC-aware compatibility mode. A
bounded raw-mode slice is now implemented inside plain `connect` (persistent
connector): while the downstream session is healthy the relay stays
byte-transparent, and after an established session loss it reconnects and
replays only the cached initialize/initialized handshake, so a fresh dedicated
downstream process becomes initialized without the Agent re-issuing initialize;
uncertain in-flight business calls fail once and are never replayed. It does
not retain last-known-good tool/resource/prompt catalogs, so it cannot answer
`tools/list` while the downstream is absent, and it cannot serve a client that
permanently pruned the registration during startup. The full cached
JSON-RPC facade (retaining a revisioned last-known-good initialize result and
catalogs, constant two-tool exposure for clients that prune failed startup) is
the remaining compatibility slice; enrollment records which mode was selected
and its restart/pruning tradeoff.

#### Lifecycle matrix

| Registration | Bridge lifecycle authority | Restart meaning |
|---|---|---|
| stdio dedicated | Owns only processes attached to matching logical streams | Drain, gracefully close and then terminate matching process trees; a fresh instance is created behind a still-running connector where the compatibility facade supports it. No request replay claim. |
| stdio shared | Owns one generation | Drain/stop the exact generation, prove exit, start a strictly newer generation, re-initialize it, and keep the connector/facade available to the Agent wherever protocol state permits. |
| HTTP external without control contract | No process authority | Availability/readiness only; restart is refused. |
| HTTP external with registered control contract | Owns only the bounded control operation, not an arbitrary process | Invoke the locally registered restart/drain interface, then prove readiness; never infer control from the endpoint alone. |
| HTTP bridge-managed | Owns one supervised process generation | Drain, stop the exact generation, prove exit, start and pass readiness behind the stable HTTP relay endpoint. Existing MCP sessions may still require re-initialize. |

A backend is not `ready` merely because `Popen` succeeded. Managed HTTP needs its
configured readiness gate; shared stdio needs successful physical MCP
initialization; dedicated stdio can report only stream/process state until its
client completes initialization. Status must distinguish `registered`,
`relayAvailable`, `processState`, `initializeHealth`, and `ready` without
pretending that an unprobed server is healthy.

#### Acceptance

- The same fixture registered as stdio and Streamable HTTP is projected with the
  corresponding native client configuration in every supported adapter.
- Native HTTP JSON responses, long-lived SSE, concurrent POSTs, cancellation,
  session deletion, 404 session loss, and server requests survive both Bridge
  directions without conversion to stdio.
- An unsupported Agent client fails reconciliation explicitly and preserves its
  prior configuration.
- Restarting a `connect` process is never counted as a peer MCP generation
  restart. HTTP restart fails closed unless the Bridge owns its process or the
  Operator registered a bounded, verified control interface for that endpoint.
- Killing or closing a connector cannot stop, crash, or corrupt either Bridge
  node. Connector lifetime is a client-session concern; node lifetime and all
  unrelated logical streams remain independent.
- No listener resolves outside loopback, and private HTTP/launch data never
  appears in peer registry, projection status, control results, or logs.

### P7 — One explicit stdio Bridge Control MCP

**Implementation status:** first control slice implemented: constant two-tool stdio surface,
owner-registry opt-in, preview/confirm lifecycle reuse, impact-override policy, peer-routed control
frames, operation identifiers correlated in both requester/owner journals, per-target lifecycle locks,
reconnect-required results, structured stdio shutdown phase/escalation evidence, and registered
streamable-http bounded control-contract execution for `external-controlled` rows (private validated
contract persistence, owner-side fixed-method loopback invocation with bounded timeout/success
statuses, optional bounded GET readiness gate, and preview/confirm with operation ids). Managed
HTTP readiness/supervision for `bridge-managed` rows is implemented with exact-generation guards,
bounded startup/readiness/shutdown, drain, stop/restart without overlap, and per-phase journaling.

#### Position and tool budget

Add one optional, independently registered stdio MCP served by the local Bridge
component. It is not injected into any business MCP and it initializes without
probing the peer or a business endpoint, so an unavailable business MCP cannot
cause clients that prune failed startup registrations to remove the control MCP.
The same resilience rule applies to every projected stdio facade: the connector
must remain a valid, protocol-responsive MCP endpoint when the peer link or real
backend is temporarily unavailable wherever an initialized facade can safely do
so. It should return structured MCP tool errors and expose recovery state rather
than exit merely because the real MCP failed. A connector EOF or process kill
must affect only that facade/session and must never take down a Bridge node.
Its Agent-visible surface is constant regardless of the number of registered
MCPs:

1. `bridge_control` — bounded status plus preview/apply of lifecycle actions;
2. `bridge_diagnostics` — bounded event/error summaries and explicitly authorized
   development traces.

Do not add one lifecycle tool per business MCP, copy the full peer registry into
tool descriptions, or dynamically enumerate ids in JSON Schema. Exact opaque
registry ids are returned only by bounded status/search results. Where the Agent
environment exposes MCP registration names, projection keeps them equal to the
peer registry id so the same identifier can be reused without another mapping.
Measure the serialized `initialize` instructions and `tools/list` byte/token
estimate as a release regression budget.

#### Authority and request model

`bridge_control` is an Agent-authorized development capability, not the complete
Operator role. The owning registry must explicitly opt each target into Agent
control and list allowed actions. The control MCP talks to its local node; for a
peer target the existing authenticated/trusted local Bridge link carries only a
bounded lifecycle request to the owning node. The owner re-resolves the id and
policy locally. It never accepts a PID, URL, command, args, cwd, env, header,
credential, arbitrary service name, or kill pattern.

Use one compact two-phase request model:

```json
{"action":"status","id":"optional-peer-id"}
{"action":"restart","id":"peer-id","expectedGeneration":7,"confirm":false,"reason":"loaded development build"}
{"action":"restart","id":"peer-id","expectedGeneration":7,"confirm":true,"reason":"loaded development build"}
```

The preview reports only the affected target, transport/mode, observed generation
or endpoint revision, active-client/session impact, allowed/refused result, and
whether reconnect/re-initialize is required. Apply repeats all owner-side checks.
`confirm=true` expresses an exact requested mutation but does not manufacture
human authority: the canonical control-MCP instructions require a direct user
request or an already-authorized development workflow.

#### Concurrency and outcome rules

- Serialize lifecycle changes per registry id, not globally across unrelated
  MCPs. Assign every accepted mutation an operation id and record before/after
  generation, initiator class, reason, and terminal outcome.
- Require `expectedGeneration` for destructive control of a live managed
  generation. Stale previews fail closed. HTTP external endpoints use a registry
  revision/readiness observation and remain non-restartable.
- Default Agent restart refuses when it would interrupt other active clients or
  sessions. A stronger impact override is separate, registry-authorized, visible
  in preview, and never inferred from `confirm=true`; Operator CLI retains forced
  recovery authority.
- Every stop/restart is a graceful state machine rather than an immediate kill:
  enter drain; refuse new work; allow or cancel in-flight work according to a
  bounded policy; request protocol/transport shutdown where available; close
  stdin or the registered HTTP control interface; wait; terminate the exact owned
  process tree; and force-kill only after the configured deadline. Each phase and
  forced escalation is reflected in the structured result and event journal.
- Drain prevents new attaches/sessions while existing work settles. Stop/restart
  never replay a tool call whose completion is unknown, never mix late responses
  into a new generation, and never overlap owned generations.
- The `tools/call` JSON result/error is the authoritative control notification
  because it is part of the MCP exchange every supported Agent client must return
  to the model. Keep it compact and structured, with a short human summary rather
  than a long prose log. Correctness never depends on the Agent client exposing
  connector startup/termination stderr, exit codes, or implementation-specific
  lifecycle logs. Best-effort MCP logging notifications remain optional UX only.
- A shared-stdio backend restart preserves attached Bridge logical streams when
  the backend is idle: it fully reaps the old generation, starts and initializes
  the next generation, resumes forwarding, and reports `reconnectRequired=false`.
  It never replays an interrupted tool call. `stop`, dedicated stdio teardown,
  Bridge-node replacement, and any path that actually closes the connector must
  report `reconnectRequired=true` where applicable and must not claim that a
  client which permanently removed a disconnected MCP will reconnect.
  Client-specific reconnect settings remain compatibility aids, not a protocol
  guarantee.

#### Acceptance

- The control MCP remains initialized and callable while the peer link is down,
  a target fails startup, or a business MCP is draining/stopped.
- Preview is side-effect free; confirmed stale/concurrent mutations have exactly
  one winner; unrelated targets remain available.
- A remote control request can affect only an owner-registry id that opted in,
  and both nodes record the same operation id and terminal outcome.
- Dedicated/shared stdio and external/managed HTTP follow the lifecycle matrix;
  no call is replayed after interruption.
- Registry MCP remains its current four read-only tools. Business MCP
  `initialize` and `tools/list` remain unchanged. The only optional Agent token
  cost is the constant two-tool control MCP.

### P8 — Structured evidence journal and privacy-bounded tracing

**Implementation status:** foundation slice implemented: host-local owner-only SQLite event journal,
bounded metadata records, recent-event diagnostics, deterministic row retention, actual bounded
chunk capture at envelope/snippet/complete levels, method/direction filtering, nonblocking automatic
recording hooks, sensitive capture/readback confirmation, and metadata-only diagnostic ZIP bundles
with member and archive digests, plus deterministic row/age/logical-byte retention. Explicit
confirmation-gated physical compaction is implemented in `journal_maintenance.py`; bounded
offline correlation of explicitly exported operation metadata is in `journal_evidence.py`.
General per-business-call cross-host correlation remains open.

#### Always-on event evidence

Replace stderr-only diagnosis with a host-local bounded event journal. Stderr
remains a human convenience, never the sole evidence path. Events use stable
categories and correlation ids across proxy stream, peer operation, backend
generation, HTTP session, lifecycle operation, and artifact transfer. Default
events contain metadata only:

- monotonic/wall time, side, target id, transport and mode;
- generation/session alias, event category, MCP method class, byte counts and
  duration;
- readiness transition, exit classification, bounded error category/code, and
  retryability;
- no command, path, env, headers, credentials, raw request/result, business
  stderr body, stream id exposed to Agents, or conversation text.

Use a separate host-local SQLite journal (WAL, owner-only permissions), bounded
row/text sizes, retention by age and bytes, and deterministic pruning. Bridge
stdout remains MCP-protocol clean. A bounded `bridge_diagnostics` query returns
summary-first counts and selected recent errors; it never dumps the journal.
Operator CLI can export a diagnostic bundle with manifest/digests and explicit
scope.

#### Development trace sessions

Full MCP messages can contain prompts, credentials, personal data, model output,
and destructive tool arguments; generic redaction cannot prove them safe.
Therefore payload capture is off by default, but the Bridge must provide the
capability for MCP/Bridge development and it is never enabled merely because a
registration is marked development. An explicit, time-bounded trace session can
select envelope-only, bounded snippets, or complete payload capture; it must also
select target, direction/method classes, byte budget, retention, and permitted
content classes. Snippet or complete capture requires confirmation and an
explicit sensitive-data warning, is recorded as an event, stays on the observing
host, and automatically expires. Complete capture is exact evidence up to the
configured byte boundary—not a claim that its contents are safe or redacted.

- Prefer structural metadata, JSON-RPC envelope fields, hashes, sizes, timing,
  error objects, and schema-validation differences over bodies.
- If snippets are explicitly enabled, apply configured key/header redaction and
  truncation but label the result `sensitive-unverified`; do not claim arbitrary
  secrets were removed.
- Never mirror raw transcripts across the Bridge link by default. Correlate the
  two host journals with opaque operation ids and export each side explicitly.
- Business stderr is captured only into a separately capped, target/generation
  channel; Agent queries receive categories and bounded sanitized excerpts, not
  an unbounded log tail.
- Diagnostic content is evidence, never executable instruction. Tool results
  mark captured MCP/server text as untrusted.

#### Acceptance

- Startup failure, initialization failure, HTTP 4xx/5xx, session loss, timeout,
  cancellation, backend crash, stale generation, drain refusal, and peer-link
  loss are distinguishable through structured queries even when client stderr is
  discarded.
- Correlation reconstructs one call's path across both nodes without exposing
  launch definitions or relying on raw conversation capture.
- Default-mode tests inject canary secrets into headers, args, results, stderr,
  paths, and environment and prove none appear in journal queries or exports.
- Trace byte/time/row limits, expiry, pruning, crash recovery, concurrent writers,
  and malformed payloads are tested. Capture cannot block or reorder the MCP data
  plane.
- Diagnostics responses and token estimates are bounded independently of journal
  size.

### P9 — Compatibility and field acceptance

**Implementation status:** offline contract checks now cover the fixed two-tool surface, bounded
serialized prompt/catalog size, typed HTTP redaction, observation-only readiness, bounded journal,
and sensitive trace confirmation. Real Windows/WSL and installed-client field acceptance remains
open and must not be claimed from the offline fixtures.

Build a client/transport/lifecycle matrix rather than claiming universal
transparency:

- Codex, Claude Code, and DSH: stdio projection, native Streamable HTTP projection
  where their installed versions support it, startup-failure behavior,
  reconnect/pruning behavior, control-MCP visibility, instructions visibility,
  and configuration rollback;
- Windows and WSL in both ownership directions;
- dedicated/shared stdio and external/managed HTTP;
- peer loss, node restart, target crash, control-MCP restart, concurrent Agent
  control, and clients that discard stderr.

The release gate requires fixture evidence first and separately approved real-MCP
migration evidence later. Node restart/session resumption is not called seamless
until the physical link, logical streams, HTTP sessions, and lifecycle intent all
have a tested persistence/reconciliation contract. Durable disable belongs to the
registry; transient drain/control intent remains runtime state unless a separate
Operator-approved persistence design proves safe boot behavior.

### Explicit non-goals for this milestone

- Adding lifecycle tools to every business MCP.
- Treating service self-shutdown tools as a universal lifecycle mechanism.
- Claiming the Bridge can force an arbitrary Agent client to remount an MCP after
  the client permanently pruned it. The Bridge must nevertheless minimize this
  outcome by keeping projected facades responsive and returning structured tool
  errors through MCP rather than exiting on transient backend failure.
- Preserving in-flight stdio calls or replaying side-effecting calls across
  restart.
- Restarting an HTTP endpoint for which the Bridge has neither process ownership
  nor an Operator-registered bounded control interface; arbitrary peer-supplied
  processes, Windows services, containers, commands, and kill targets remain
  outside the contract.
- Default recording or cross-host collection of complete Agent/MCP conversations;
  explicit bounded development capture remains a required capability.
- Silently representing an HTTP registration as stdio, or claiming a compatibility
  adapter is native transport equivalence.

## P10 — Dual-era MCP protocol boundary

**Implementation status:** roadmap only. The current shared backend implementation
accepts the verified legacy revisions `2024-11-05`, `2025-03-26`, `2025-06-18`,
and `2025-11-25`, while normalizing its physical backend connection at
`2025-06-18`. It does not implement or advertise the `2026-07-28` lifecycle.
Dedicated registrations remain byte-transparent and therefore leave version
negotiation to the Agent client and business MCP.

### Version policy

Treat protocol dates as frozen compatibility profiles, not as separate business
implementations:

- **Legacy canonical revision:** `2025-11-25`. Once the physical shared-backend
  baseline is upgraded and accepted, older legacy clients are decoded into one
  canonical legacy model and results are projected back to their negotiated
  revision. Supporting an older revision means applying its real frozen field,
  capability, schema, error, and notification rules; changing only the reported
  version string is forbidden.
- **Modern canonical revision:** `2026-07-28`. This is a separate protocol-era
  adapter because discovery, per-request metadata, version errors/retry, and
  subscription/task-extension semantics cannot be implemented as field deletion
  from a legacy initialize session.
- **Compatibility profiles:** retain `2025-06-18`; retain the already implemented
  `2025-03-26` and `2024-11-05` profiles while their regression cost remains
  bounded. Frozen profiles may be completed once and kept without receiving new
  optional features.
- Negotiate by protocol revision and capability intersection, never by the names
  Codex, Claude Code, DSH, or a business MCP.

The intended steady state is therefore two protocol engines—legacy
`2025-11-25` and modern `2026-07-28`—plus small, tested legacy projections. It is
not five copies of the Bridge or five copies of each MCP handler.

### Shared and third-party backend policy

Agent-facing and backend-facing negotiation are independent:

- Bridge-owned shared virtualization may accept heterogeneous Agent revisions but
  keeps one physical backend generation and one selected backend revision. It
  must not create one process/session per protocol date merely to claim support.
- Self-developed MCPs should eventually expose dual-era protocol frontends when
  they also need to work by direct registration, but their tools, resources,
  prompts, authorization, confirmation, and business handlers remain one
  implementation.
- Prefer an existing official or mature SDK that demonstrably implements the
  required client/server era, transport, negotiation, and conformance behavior.
  Add only a bounded policy wrapper where project rules differ. Do not implement
  a private protocol stack merely for API symmetry.
- For an introduced third-party MCP, discover and record its actual transport,
  supported revisions, capabilities, and lifecycle behavior. Dedicated mode
  remains byte-transparent. Shared mode may adapt only the representable
  intersection; it must withhold or reject semantics it cannot faithfully
  virtualize.
- Tasks, multi-round task results, URL elicitation, sampling-with-tools, and
  subscription semantics are never inferred from a higher revision number. They
  require advertised capabilities plus an implemented end-to-end owner.

A standalone, non-authoritative recommendation for MCP authors is maintained at
`../MCP_DEVELOPMENT_RECOMMENDATIONS.md`. This project plan remains authoritative
for Bridge work.

### Delivery sequence

1. Add fixtures for Claude Code and Codex modern probes that prove clean legacy
   fallback, no hang, no duplicate business call, and no false capability claim.
2. Define a version-independent internal request/result and capability model,
   with explicit legacy profiles and lossless/unsupported classifications.
3. Upgrade the shared physical backend baseline from `2025-06-18` to
   `2025-11-25` only after self-developed backends, schemas, rollback, and both
   host directions pass acceptance. This upgrade is useful but is not a
   prerequisite for the modern Agent-facing adapter.
4. Implement the `2026-07-28` Agent-facing adapter initially for the tools-only
   intersection: discovery, required per-request metadata, unsupported-version
   error/retry behavior, cancellation, progress, notifications, and era
   isolation. Continue to advertise no Tasks extension.
5. Validate modern Agent-to-legacy-backend and legacy Agent-to-modern-backend
   projections with official SDK/conformance fixtures where available.
6. Enable optional capabilities one family at a time only after end-to-end
   ownership, failure, reconnect, and token-cost gates pass. Tasks require a
   separate persistence/idempotency design and are not implied by this phase.

### Acceptance

- Current Codex legacy `2025-06-18`, Claude Code legacy `2025-11-25`, and DSH
  `2025-11-25` paths pass initialize/list/call and reconnect fixtures.
- A modern probe sent to a legacy-only Bridge receives the specified clean
  unsupported/method-not-found behavior and falls back where the client supports
  fallback; it never poisons or duplicates the shared backend generation.
- Once modern support is enabled, every `2026-07-28` request is validated using
  modern per-request semantics; the Bridge never answers modern discovery before
  that complete gate passes.
- Each older legacy profile has golden initialize, capability, tool schema,
  result, error, notification, and unknown-method fixtures. A canonical response
  cannot leak a field or capability unavailable in the negotiated revision.
- Agent-facing capability claims equal the safely representable intersection of
  client, Bridge, and backend capabilities. Unsupported optional semantics are
  absent or rejected, never fabricated.
- One business call is executed at most once across version retry, reconnect,
  backend generation replacement, and era fallback. Only the allowed handshake
  state may be replayed.
- Dedicated third-party registrations remain byte-transparent. Shared
  third-party registrations fail closed when their protocol/capability behavior
  cannot be represented honestly.
- Tool names and schemas remain direct per-MCP registrations; this work adds no
  global Adapter, no mega-tool, and no client-specific tool surface or material
  token overhead.

## Recommended implementation sequence

1. **P6 contract slice:** registry migration, tagged transport/ownership schema,
   redacted public summaries, projection capability matrix, and lifecycle matrix
   tests—without enabling HTTP relay yet.
2. **P8 foundation slice:** structured in-memory/event types and bounded local
   journal so all later failures and lifecycle operations have authoritative
   outcomes independent of stderr.
3. **P7 control slice:** optional two-tool stdio control MCP, registration opt-in,
   peer-routed owner enforcement, per-target concurrency, preview/confirm, audit,
   and token-surface regression tests.
4. **P6 transport slice:** stable local HTTP relay, owner endpoint connector,
   Streamable HTTP semantics, managed readiness/supervision, per-client native
   projection, and explicit `HTTP -> stdio` / `stdio -> HTTP` compatibility routes
   for enrolled environments whose MCP client lacks the native transport.
5. **P8 trace slice:** explicit time-bounded development capture and diagnostic
   bundle export only after metadata-only evidence is complete.
6. **P9 field slice:** real Windows/WSL and supported-client matrix, then update
   current-capability documents.
7. **P10 protocol-era slice:** prove modern-probe fallback first, introduce the
   canonical legacy profiles, then implement and validate the tools-only Modern
   adapter before enabling any optional capability family. Until each gate
   passes, these sections remain roadmap only.

## Optional hardening: untrusted-local artifact boundary

This phase is not required by the supported local-only profile, which trusts the
local OS user, Agents, and registered MCPs. It applies only if a future deployment
admits mutually untrusted local principals.

### Work

- Replace loopback publisher control with a mode-0600 Unix domain socket on WSL.
- Implement an owner-ACL Windows named pipe publisher endpoint.
- Bind publisher sessions to service identity, logical stream, target MCP,
  physical-link generation, random nonce, and expiry.
- Anchor destination creation to opened directory handles on WSL and use
  reparse-point-safe native handles on Windows.
- Add global/per-identity spool quotas and bounded retention metrics.
- Validate no-overwrite behavior on real NTFS and ext4.

### Acceptance

- A different OS user cannot publish, select an inbox, reuse a token, or access a
  stage even when it can reach loopback.
- Concurrent directory replacement cannot escape an authorized workspace root.
- Killing either node during snapshot, transfer, commit, or acknowledgement
  leaves no unverified final file.
- Real Windows/WSL evidence proves both transfer directions and rollback.

## P5 external capability-repository integration blockers

The repository-local slice is complete. The remaining work requires contracts
and authority that do not exist in this repository and therefore must not be
invented here:

- An Operator-owned remote catalog transport and signature/revocation trust
  anchor. The standard-library runtime intentionally contains no repository
  network client or embedded trust root.
- A DSH/Codex contract for dynamic tool add/replace/remove and exact schema
  loading on selection. Neither client exposes that contract here.
- An explicitly approved mapping from selected catalog identity to P0 projection
  enrollment. Catalog metadata itself cannot grant launch or installation
  authority.

### Acceptance once external contracts exist

- A directly registered local MCP never appears a second time through the
  warehouse (already covered by the local deduplication primitive).
- Repository content cannot grant credentials, mutation, cost, restart, or
  installation authority (already covered by catalog validation and plans).
- Dynamic tool add/replace/remove works for DSH and Codex without stale schemas.
- Signature, revocation, transport failure, and rollback are verified against the
  Operator-provided trust contract.

## Compatibility validation track

These are environment/third-party validation tasks, not missing Bridge semantics:

- Validate future MCP protocol revisions and task-related methods through the
  transparent data plane when those revisions are published.
- Add official SDK fixtures for Python, TypeScript, C#, Java, and Rust where the
  SDKs and conformance runners are available.
- Run the Streamable HTTP adapter against the official conformance server.
- Add real Windows/WSL CI; Linux role simulation remains necessary but
  insufficient for Windows filesystem and process behavior.

## Deferred non-goals

- Arbitrary peer filesystem browse/read/write.
- Inferring files by scanning tool text, JSON strings, or `file://` paths.
- Automatic dependency installation from capability metadata.
- Public-network exposure of the peer or publisher control protocols.
- Claiming business-level lifecycle or singleton guarantees when the registration
  does not explicitly select `multiProcessAllowed=false` bridge enforcement.
