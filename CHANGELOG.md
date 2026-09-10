# Changelog

All notable changes to this project are recorded here.

## 0.4.0 - Unreleased

### Claude Code official-CLI adapter argument order

- Fixed the Claude official-CLI `mcp add` argv: real Claude Code `-e/--env` is
  variadic (`claude mcp add <name> -e K=V -e K=V -- <command> <args...>`, verified
  2.1.267), so a server name emitted after the env flags was consumed as a
  malformed env value and `projection reconcile` failed the whole Claude
  environment with `Invalid environment variable format: <name>` (fingerprint
  guarded, no partial write). The name now precedes the flags and the command
  stays after the `--` terminator; Codex `--env` remains single-valued and its
  argv is unchanged, as is the native Streamable-HTTP branch.
- Added regression coverage instead of relying on the previously permissive fake
  CLI: the harness fake Claude CLI now models the variadic contract (non-option
  tokens consumed as env values, `--` ends the run, a token without `=` is
  rejected), plus a focused argv assertion and an end-to-end reconcile test. The
  pre-fix argv reproduces the real failure against that fake. This records a code
  and test fix only: no client configuration, enrollment or deployment is claimed
  by this entry.

### B′ name-delta guidance

- Deferred view switches now return raw `addedTools` / `removedTools` names in
  model-visible content text, without duplicating descriptions or full schemas.
  The retained library entry explicitly offers re-expansion to finish the task;
  B′ does not require a separate proactive definition-inspection step.
- Preserve last-published names through invalidation for accurate withdrawal
  guidance, without using them for execution or stale schema discovery. Status
  remains metadata-only; unchanged actions have empty name deltas.
- Recorded isolated DS4F A′/B′ evidence (six successes and 20 model requests per
  arm; zero invalid calls) and the separate generic-efficiency roadmap requiring
  real-task, harness-specific cost validation before production. Fixture wording
  tests are not model acceptance of this exact runtime build; no restart,
  deployment, plugin installation or new real-model run accompanies this change.

### Corrected dynamic tool guidance

- Revised Harness adaptation decisions: the verified DSH Web/DS4F legacy stdio
  environment is approved/activated for dynamic exposure, while Claude/Codex
  retain native discovery for host tool search and other profiles remain scoped
  to their own verification. No product-name inference bypasses enrollment.
- Added cache-aware Agent guidance: expand on demand, retain the view throughout
  related work, and avoid per-call/end-of-turn collapse. The user's approximate
  12.5 cached-round cost equivalence is documented as local billing context,
  never hard-coded as a provider rate or automatic eviction threshold.

- Deferred expand/collapse now instructs Agents to use a subsequent model request
  after client refresh (a later step in the same turn is sufficient), with compact
  `nextStep` guidance in switch responses. Status is bridge-local, and
  `refreshRequested=false` is not a refresh acknowledgement. Historical tool
  names are not current callable definitions.
- Recorded user-observed DSH/DS4F request snapshots for a successful round trip:
  54 -> 134 -> 54 tool definitions, +80/-80 Onshape tools, approximately
  +16.3k/-16.3k tool-schema tokens. Taobao remained collapsed. Added regression
  coverage for later-request acceptance and rejecting stale calls after collapse;
  no automatic production verification receipt or database update is inferred.

### Added

- Projection schema v4 persists explicit loopback relay and per-target converted
  facade endpoints; native HTTP and converted stdio-to-HTTP configuration now
  round-trip through Codex, Claude and DSH adapters. Unsupported routes fail
  the whole environment before writes; user drift preserves fingerprints.
- Tools-only modern `2026-07-28` shared-stdio projection with per-request metadata,
  observation-backed discovery/instructions, result/error projection and legacy
  fallback isolation. The physical shared generation now requests `2025-11-25`
  and validates the backend's observed legacy choice. Logical tools/content and
  capability output is projected to four frozen profiles; non-tools shared
  capability advertisements are withheld pending separate acceptance.
- Explicit reverse-era stdio adapter and modern modes on both HTTP conversion
  directions, plus Bridge-owned registry/control frontends. Bounded schema-header
  preflight, cancellation and uncertainty reporting never replay business calls.
- Pinned official SDK 2.0.0 interoperability harness and recorded development
  dependencies; runtime remains standard-library-only.
- Revisioned last-known-good compatibility catalogs with explicit cache evidence,
  atomic refresh and no replay after uncertain business-call failure.
- Explicit journal compaction preview/confirm, bounded offline operation/call
  correlation, and opaque passive stream aliases with origin/id-reuse/loss gates.
  Capacity never resets a live stream's aliases; diagnostic counters expose drops.
- Approved `docs/`, `tests/` and `tests/fixtures/` support layout, preserving two
  runtime components and root shared-module imports; distribution inventory and
  source-byte validation now covers source-distribution fixtures.

- First P6-P9 foundation slices: registry schema v4 with private typed stdio/Streamable-HTTP and management policy fields plus redacted public summaries; projection schema v2 transport capability/compatibility-route evidence and explicit unsupported-transport refusal; optional constant two-tool `control-mcp` with owner-registry opt-in, preview/confirm, impact policy, peer control routing, operation identifiers, and per-target locks; an owner-only bounded SQLite event journal with explicit time/byte-bounded sensitive trace authorization; an opt-in native loopback Streamable HTTP relay (`serve --http-relay-port`, default off) that mounts peer-registered `streamable-http` MCPs at `/mcp/<registered-id>` with owner-side endpoint/header resolution and bounded request envelopes over the peer link; the explicit loopback `stdio-to-http` facade (`stdio_http_facade.py`, one disposable `bridge.py connect` per HTTP MCP session); and bounded registered `streamable-http` lifecycle control contracts for `external-controlled` rows: private validated contract persistence (fixed method, relative path or loopback url, bounded timeout/statuses, optional loopback GET readiness), owner-side execution with preview/confirm and operation-id journaling, and no public/peer disclosure. `bridge-managed` HTTP rows now add a private bounded supervision policy, one owned process generation, readiness-gated relay demand, drain/stop/no-overlap restart, exact-generation guards, and per-phase journaling. Native per-client HTTP and explicit stdio-to-HTTP endpoint projection are now implemented; real-client field acceptance remains separate.
- Bridge-enforced single shared backend generations for registrations with
  `multiProcessAllowed=false`.
- JSON-RPC initialize virtualization, request/progress ID rewriting, response,
  error, cancellation, notification, and server-request routing for shared
  clients.
- Deterministic heterogeneous-client initialization profiles, supported protocol
  revision normalization, downstream instruction replay, and capability-aware
  routing of backend-initiated requests.
- Generic (never client-specific) MCP `initialize` revision negotiation on
  shared backends: verified logical revisions (`2024-11-05`, `2025-03-26`,
  `2025-06-18`, `2025-11-25`) are accepted verbatim; newer well-formed
  unverified revisions (after `2025-11-25`) are negotiated down to the newest
  verified revision that does not exceed the request per the MCP versioning
  rule instead of being falsely claimed; and unrecognized, malformed, or
  older-than-supported revisions fail with the structured diagnostic.
  `2025-11-25` acceptance is deliberately capability-subset support: the shared
  JSON-RPC/tools core is wire-compatible, the Bridge and the normalized backend
  advertise no tasks and no tool `execution.taskSupport`, the fixed empty
  physical client capability profile never solicits sampling/elicitation/tasks,
  and replayed initialize results and tool catalogs never fabricate the
  optional task surface (regression-tested: accepted `2025-11-25` sessions
  journal as accepted and a task-only client advertisement cannot falsely
  enable sampling/elicitation/roots flows). Each accepted/downgraded/rejected
  outcome records one metadata-only EventJournal row (target plus requested,
  negotiated, and normalized backend versions; no payload). The physical backend
  requests `2025-11-25` and records its actual verified legacy choice. A
  downgraded client's session continues safely (initialized notification,
  catalog, and calls).
- Registration-driven exclusive client leases and fixed shared tool-view policy.
- Generation-scoped artifact publication and owned process-tree cleanup.
- Offline concurrent-client, lease transfer, disconnect, backend crash, bridge
  restart, generation ordering, final-response drain, and independent-target
  acceptance fixtures.
- Artifact delivery revision artifacts/2 (P4): content-addressed inbox
  destination (`.mcp-artifacts/<sha256>/<name>`), per-chunk digests verified
  before write plus a full-file digest, durable per-inbox journal
  (`.v2-journal.json`, atomic + fsync) recording partial and committed receipts,
  receiver-minted random resume tokens with park/resume that never re-sends
  acknowledged bytes, link-loss parking (dedicated publishes) vs abort
  otherwise, tokenless/fresh begins never revealing prior state, end-time
  idempotence so a redelivered artifact converges on the existing committed file
  without overwrite, and a negotiated offer/echo (`highestVersion`) keeping
  artifacts/1 peers on the v1 path unchanged.
- Local capability-warehouse slice (P5): read-only `Registry.identities()`
  inventory, static Operator-supplied capability index loading that fails closed
  on any launch/install authority key, bounded summary-only discovery, dedup of
  catalog items already directly registered locally, and a read-only
  install/import plan that never invents launch configuration; exposed as the
  read-only `warehouse list|search|dedupe|plan` bridge CLI.
- Bridge-owned MCP lifecycle status/control: read-only lifecycle aggregation for
  registrations owned by the local node (`lifecycle status`), Operator-only
  preview-then-`--confirm` mutations (`lifecycle drain|restart|stop`) applied
  through the loopback local control operation, exact owned-generation guards
  that refuse stale control, and restart/stop transitions that never overlap
  generations. Shared registrations report state, owned generation, active
  clients, and drain; dedicated registrations aggregate active-stream counts and
  never expose or replay stream content. Compact read-only `lifecycle` fields
  are merged into existing registry `describe`/`status` answers only when a live
  owning node answers, keeping `list` and Registry-only callers unchanged, and
  no Agent-visible business-MCP or registry tool is added.
- Persistent per-target stdio connector: `connect <registered-id>` is now a
  frozen-core (`connector_core.py`) + reloadable-engine
  (`connector_engine.py`) process that stays alive while Agent stdin is open and
  reconnects after node/socket/peer-link/backend loss; it replays only the
  cached MCP initialize/initialized handshake to the fresh downstream session,
  never replays business calls, fails calls pending at loss exactly once with a
  concise Bridge JSON-RPC error (`-32000`), and preserves connected business
  payload bytes. The connect handshake carries `connectorVersion` and the node
  reply carries `coreVersion`, paired by simple direct equality: a node core
  that differs from the engine's `CORE_VERSION` surfaces one short warning only
  in initialize result `instructions` and Bridge-generated errors (business
  results untouched unless the engine policy opts into a single-line note).
  Engines load explicit-file-first
  (`WIN_WSL_MCP_BRIDGE_CONNECTOR_ENGINE`, fails closed) or from a scanned
  engine directory (`WIN_WSL_MCP_BRIDGE_CONNECTOR_ENGINE_DIR`) of immutable
  `*.engine.json` manifest bundles with SHA-256-verified, versioned module
  loading, newest-verified-first selection, and rollback to the next candidate
  or the built-in engine. No third component directory or Agent-visible Adapter
  is added.

### Changed

- Shared backend requests are globally serialized for synchronous MCP runtimes.
- Public registry process metadata now reports enforcement outcomes without
  exposing private lease tool patterns or release matching rules.
- Agent-environment enrollment and per-server configuration projection (P0-B):
  `projection scan|enroll|unenroll|sync|reconcile|status` on each host; a
  read-only bounded scanner over Codex/Claude/DSH configuration locations;
  host-local projection.sqlite3 authority with `agent_environments`,
  `agent_mcp_projections`, `registry_projection_outbox`, and bounded
  `config_apply_history`; atomic registry-commit outbox events for
  projection-affecting changes only; deterministic convergence of one
  `connect <server-id>` entry per Agent-reachable peer registry registration;
  official client CLI or Bridge-owned-file adapters with fingerprint-verified
  removal, drift detection, rollback, and per-environment failure isolation;
  and optional polling via `--watch-seconds`.

## 0.3.1 - 2026-08-31

### Fixed

- Windows listener callbacks now treat normal peer EOF and bounded bridge/socket failures as handled link termination instead of emitting an unhandled asyncio traceback.

## 0.3.0 - 2026-08-31

### Added

- Per-stream data acknowledgements and bounded end-to-end backpressure.
- Standard JSON-RPC error handling and `ping` support for the Registry MCP.
- Installable Windows and WSL console entry points plus read-only `doctor` preflight.
- Dedicated installed-mode `bridge_publisher.py` contract for Windows and POSIX venvs.
- Reproducible real Windows/WSL ephemeral fixture acceptance test.
- Offline sdist/wheel metadata, deployment guide, and Windows/Linux CI matrix.
- Trusted-local deployment profile and stricter loopback enforcement.
- Owner-only POSIX registry and artifact-partial permissions.

### Changed

- Bridge peer protocol upgraded to `win-wsl-mcp-bridge/0.2`; older peers fail the handshake cleanly.
- Registry responses that exceed the bridge frame limit now fail the query without dropping the peer link.
- Local client EOF now has a bounded grace period before the remote stream is terminated.
- Remote-first stream close now exits the local proxy even while client stdin remains open.
- Peer registry/artifact setup handlers no longer block unrelated link frames.
- POSIX SIGTERM and node cancellation perform bounded child-process cleanup.
- Startup removes stale workspace `.partial` files beneath configured artifact roots without deleting committed outputs.
- Artifact receive setup now aborts immediately if its `artifact_ready` response cannot reach the peer.

### Fixed

- Artifact senders are released immediately when a stream closes during a chunk acknowledgement.
- Manifest `enabled` values and reserved artifact environment keys are validated strictly.

## 0.2.0

- Initial bidirectional stdio bridge and negotiated `artifacts/1` workspace-push implementation.
