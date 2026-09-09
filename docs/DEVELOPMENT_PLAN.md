# Development Plan

## Completion Rule

The complete original P6-P10 requirements and optional tracks are preserved in
[Milestone design](MILESTONE_DESIGN.md). That is the design baseline, not a current
capability claim. Current behavior belongs to [MCP coverage](MCP_COVERAGE.md) and
[Architecture](ARCHITECTURE.md). Run evidence belongs to the
[Acceptance ledger](IMPLEMENTATION_STATUS.md). Nothing is complete merely because
it disappeared from a roadmap paragraph.

## Repository Completion Gates

The integrated source passed **506 tests**. Packaging remains a separate gate:
run the exact inventory/source-byte and isolated-install checks documented in
[Verification](VERIFICATION.md); final artifact receipts live under
`release-artifacts/` and are not evidence of production installation.

| Gate | Repository implementation | Acceptance scope |
|---|---|---|
| P6 transport projection | Native HTTP and both explicit converted routes; schema v4 enrollment | Mixed transport, migration, fingerprint drift, rollback and privacy tests |
| P6 cached facade | Revisioned in-memory last-known-good catalog and atomic refresh | Outage/reconnect, actual instructions, no business replay |
| P8 maintenance/evidence | Bounded compaction; explicit-export operation/call joins; passive stream correlation | Integrity, origin/id-reuse/loss/capacity bounds, counters, byte transparency |
| P10 dual stdio eras | Modern-to-legacy shared projection and explicit legacy-to-modern adapter | Four frozen tools profiles, actual discovery/instructions, cancellation and no replay |
| P10 canonical legacy | Request 2025-11-25 and validate the observed backend revision | Four Agent profiles, capabilities/content goldens, one generation and restart tests |
| P10 modern HTTP | Explicit bounded modern modes on both conversion adapters | Metadata/headers, schema preflight, stateless requests, cleanup and uncertainty |
| P10 Bridge-owned frontends | Modern registry/control mode | Fixed real tools/instructions, confirmation/generation gates, bounded framing |
| Structure | docs/, tests/, tests/fixtures/; two runtime components preserved | Source launchers, module inventory, sdist-only development assets |
| SDK interoperability | Official mcp==2.0.0 in approved isolated development environment | Five registry/control cases; see [SDK evidence](SDK_INTEROP.md) |
| Release | Fresh sdist-built wheel and isolated installation checks | Exact source bytes and both installed entry points; separate artifact receipt |

## Protocol and Evidence Remainder

These requirements are not implied by the tools-only stdio adapters:

- Broader legacy field/capability profiles beyond the verified tools intersection
  still require schema-specific goldens and ownership contracts. The canonical
  physical request is now `2025-11-25`, with actual backend negotiation validated;
  this does not grant unsupported protocol families.
- Both HTTP conversion directions implement bounded modern modes. Full HTTP
  conformance, arbitrary JSON Schema support, resumption and extension semantics
  remain separate claims; native relays do not become era converters.
- Link-stream call correlation and lifecycle operation correlation are implemented
  separately. The stream classifier is bounded and metadata-only; it does not
  claim general native-HTTP payload analysis or lossless evidence retention.
- Optional modern Tasks, MRTR, subscriptions, sampling/elicitation/roots and
  resource/prompt projection require separate ownership/failure contracts and
  remain unadvertised on the modern tools-only interface.
- The constant two-tool facade caches tools in memory for its own lifetime.
  Disk-persistent or resource/prompt catalogs need explicit privacy, revision and
  invalidation rules.

## P9 Field Acceptance

The DSH Web/DS4F legacy-stdio dynamic directory round trip has now been observed
in actual user-supplied request snapshots and activated by explicit configuration.
Maintain the environment-specific decisions in [Tool exposure](MCP_TOOL_EXPOSURE.md);
remaining work is verification of other DSH profiles/builds and installed
Codex/Claude routes, not re-proving that every DSH client lacks dynamic support.
Minimize view transitions during normal work because they can invalidate cached
input. The successful Web experiment is separate from automatic enrollment
receipts and does not establish modern-protocol or native-HTTP dynamic support.

Hermetic Linux fixture tests do not establish real Windows filesystem/process
behavior, installed-client runtime loading or production recovery. Before field
work approve exact hosts/versions, temporary fixture registries/ports/workspaces,
backup/recovery and cleanup, stop conditions and both ownership directions.
Then verify startup pruning, reconnect, canonical instructions visibility,
control scope, generation shutdown and client configuration rollback.
No real business-MCP mutation is authorized by repository development.

Official SDK/conformance runs must identify pinned runner/spec versions and are
separate from hand-written fixture coverage.

## Generic MCP Efficiency Roadmap — User Decision 2026-09-09

**Status: planned, not implemented or production-approved.** This track improves
universal bridge capabilities first. It does not authorize plugin installation,
production configuration changes, real browser access or business mutations.
The work order is binding:

1. Design and validate generic discovery, search, definition inspection and
   disclosure capabilities; use real model tasks before production acceptance.
2. Later, fully split the bridge implementation by functional responsibility
   (connection management, catalog/indexing, trimming/projection, search,
   folding/disclosure and execution routing), with explicit contracts and tests.
3. Only after that functional modularization, consider DSH-specific cooperation
   as a separate future development track. Do **not** reserve DSH interfaces,
   Agent mappings, view adapters or plugin scaffolding in the current work.

Functional modularization must preserve the existing two-runtime-component and
stdlib-only boundaries unless a separate architecture change is approved. The
current generic baseline remains logical-connection isolation with a shared
catalog within each connection; per-Agent disclosure is not a current goal.

### Design: Separate Discovery, Disclosure and Execution

- Search returns bounded candidates (identity, short purpose, owning library and
  necessary operation/risk metadata). It does not expand tools, warm all matches
  into the next request, or send `notifications/tools/list_changed`.
- Definition inspection returns the exact selected tool schema without changing
  the exposed directory. Search and inspection use a revisioned, invalidated
  catalog; stale entries are not evidence that a tool remains executable.
- Disclosure changes are explicit and batched. Keep selected tools visible for
  the current task phase; do not automatically prune on every turn, after every
  call, or after a small number of unused turns. One Agent's inactivity does not
  establish that another Agent sharing the connection no longer needs a tool.
- Reapplying an unchanged selection must not emit another change notification.
  Keep ordering, descriptions and schema serialization stable; do not add
  per-request timestamps or counters to the tool-definition prefix.
- Retain a small, directly discoverable library control entry and a clear route
  back from a collapsed state. The tested B′ approach (explicit added/removed
  names plus the reopening path) is the selected source wording; it is not a
  guarantee of model compliance or a production deployment claim.
- Start search with local exact-name/keyword matching, normalization, bounded
  aliases and library filters. Do not add an external rerank model by default;
  consider it only if measured discovery failures justify its extra cost.
- Reuse useful ideas from tool-search/meta-tool plugins without stacking duplicate
  discovery layers or changing business MCPs. Metadata and policies must come
  from generic contracts, not hard-coded browser/Onshape/Taobao behavior.

### Candidate Strategies and Compatibility Boundaries

Keep the existing whole-library approach as the baseline; finer granularity must
prove useful rather than become the automatic default.

| Candidate | Behavior | Benefit to verify | Cost or boundary to verify |
|---|---|---|---|
| Whole library, retained | Expand when needed and keep open during sustained use | Few discovery steps and stable repeated use | Initial full-library schema cost |
| Stable meta-tool dispatch | Fixed entries for discovery/definition/execution, available from initialization | Execution need not change the native tool list | Discovery/history cost; underlying tool-name approvals, audit and result fidelity may differ |
| Batched fine-grained disclosure | Add a selected group of tools, then call natively | Less unused schema while preserving native calls | Harness refresh behavior, switch frequency and cache invalidation |

Fixed meta-tool dispatch is not presumed equivalent to native execution. Verify
schema validation, confirmation, cancellation, errors, images/attachments and
structured results. A bridge-forwarded call does not by itself preserve a
harness policy keyed to the original tool name. Never claim that prompt hiding
is a permission boundary or that a wrapper transparently preserves host guards.

Universal dynamic disclosure depends heavily on how each harness responds to
MCP changes, especially `notifications/tools/list_changed`. Establish capabilities
for the exact harness version and mode; never infer them from the client name:

| Observed evidence | Eligible strategy |
|---|---|
| Only initial discovery is reliable | Fixed entries present at initialization, or stable full native exposure |
| Harness rereads tools/list, but request schemas are unverified | Do not enable dynamic fine-grained disclosure by default |
| Additions reach the relevant subsequent model requests | Eligible to test additive disclosure |
| Additions, removals and same-name schema updates reach model requests | Eligible to test full dynamic disclosure |
| Unknown version/mode or incomplete evidence | Treat dynamic model exposure as unverified |

Test additions, removals, same-name schema revisions, rapid consecutive updates,
reconnection and shared-connection Agent interleaving. Record bridge state,
harness refresh and actual request-bound tool definitions separately. A catalog
revision or sent notification is not a client acknowledgement; tools/list,
status/toolCount, GUI labels and model self-enumeration do not prove model-visible
exposure. DSH Web evidence must not be generalized to other harnesses, profiles
or transport modes. Do not introduce an assumed acknowledgement or a DSH-only
workaround as a universal MCP contract.

### Real-Task Model Evaluation — Mandatory Before Production

Protocol fixtures remain necessary, but isolated expand/collapse exercises are
insufficient to judge fine-grained efficiency. Prepare real tasks with verifiable
completion criteria and natural tool selection:

| Task family | Example scope | Main observation |
|---|---|---|
| Sustained single-library work | Find official information, navigate related pages, verify conditions and provide sources | Discovery overhead and reuse of already exposed tools |
| Multiple task stages | Locate an item, read details and cross-check another source | Whether selected batches avoid repeated incremental disclosure |
| Switch and return | Inspect source A, then B, then return to A to verify a detail | Repeated folding/reopening and cache churn |
| Shared-connection interleaving | Multiple Agents alternate approved real tasks on one connection | Conflicting needs and directory oscillation without per-Agent isolation |

Prompts specify the goal, limits and deliverable, not target tool names or a forced
search/expand/call sequence. Real browser use requires human approval of the
concrete task, sites, allowed operations, identities, budget and cleanup before
execution. Prefer public read-only tasks initially; approval for browsing is not
authorization for login, messages, purchases or other business writes.

- Use only **DS4F / GLM-3.8F** for model testing. DS4F is the primary screening
  route; use GLM-3.8F for bounded follow-up on harder selection behavior, accounting
  for its slower execution. Verify exact configured model identifiers before
  invocation; do not silently substitute models or change the default route.
- Compare full native exposure, whole-library expansion retained, stable meta-tool
  dispatch and batched fine-grained disclosure on the same tasks. Stratify cold
  discovery and sustained/warm work; balance run order and document changing web
  data, provider cache effects, model/version and sampling differences.
- Predeclare session/request/output/time budgets and stop conditions for each
  batch. Preserve failed, abandoned and retried work; do not silently exclude
  trials, repeatedly tune wording mid-batch, or add runs until a candidate passes.
- Measure task quality/completeness and source accuracy first, then model requests,
  discovery/definition calls, directory transitions, invalid calls, repeated
  expansion, early stopping, uncached input, cache reads/writes, reasoning/output
  and elapsed time. Capture actual tool definitions around each change.
- Use verified provider/adapter accounting and a verified rate table for financial
  comparisons. Otherwise report disjoint token categories without invented prices.
  Full schema size on every request is not the same as fully uncached input cost.
- Compare the total cost of completing equivalent work, including failures and
  recovery. A smaller exposed catalog or an early-aborted task is not a saving.
  If repeated discovery/changes invalidate caches enough to exceed the retained
  whole-library baseline, reject the strategy or restrict its applicable cases.

### Production Gates and Delivery Sequence

1. **Design/task specification:** fix generic contracts, harness evidence matrix,
   task acceptance criteria and real-browser approval materials. No DSH reserves.
2. **Isolated prototype/protocol checks:** validate catalog freshness, batched
   changes, no-change/no-notification behavior, reconnect and result fidelity.
3. **Budgeted real-model comparison:** screen candidates, then perform limited
   follow-up with DS4F/GLM-3.8F; do not auto-expand the approved test budget.
4. **Production decision:** require acceptable task quality, preserved execution
   and result semantics, request-level compatibility evidence, no persistent
   directory churn, and a demonstrated whole-task benefit over the existing
   retained-library baseline. Keep a concrete rollback to that baseline and obtain
   separate approval before production changes. A per-scenario strategy is valid;
   there need not be one universally best disclosure mode.
5. **Later architecture track:** complete functional modularization; only then
   design and evaluate a DSH cooperation plugin for Agent-scoped views or other
   host-specific capabilities. It is outside the current generic implementation.

### Prior Evidence and Its Limits

The isolated B′ versus A′ DS4F experiment recorded six successful sessions per
arm, 20 requests each and zero invalid calls/early abandonment. B′ had fewer
output tokens but more total input in that small sample. These are wording and
recovery observations, not evidence that fine-grained disclosure or any third-party
plugin is production-ready. Preserve the original and improved experiment records
under `release-artifacts/tool-guidance-ab/` and
`release-artifacts/tool-guidance-ab-v2/` (including REPORT.md, CAPTAIN_REVIEW.md,
PLAN_V2.md and final-results.json where applicable). Experimental artifacts may
be local and excluded from release packages; they are not production receipts.

## External and Optional Tracks

P5 remote catalogs still need an Operator-owned signature/revocation trust
contract and a concrete client dynamic-tool contract. Catalog metadata never
grants installation, launch, credentials or mutation authority.

Untrusted-local artifact hardening (owner-ACL sockets/pipes, directory handles,
reparse-point-safe APIs, cross-user quotas) remains an optional architecture
track, not a requirement of the supported local-user-trusted deployment profile.
