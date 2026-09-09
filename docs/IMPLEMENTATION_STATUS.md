# Implementation Acceptance Ledger

This records implementation evidence separately from the complete requirements
in [Milestone design](MILESTONE_DESIGN.md) and remaining [Development plan](DEVELOPMENT_PLAN.md).
Linux role simulation never proves installed-client or real Windows behavior.

## Current Status

| Area | Delivered | Evidence or remaining gate |
|---|---|---|
| DSH Web / DS4F dynamic exposure | Activated repository legacy stdio frontends; Onshape expand/collapse visible in actual requests, Taobao remains collapsed | User Context Browser: 54 -> 134 -> 54 definitions, +/-80 tools and ~16.3k schema tokens. Keep views stable for cache reuse; exact environment scope in [Tool exposure](MCP_TOOL_EXPOSURE.md). |
| Other Harness environments | Claude/Codex native discovery retained; other DSH profiles unverified | Native search and bridge refresh are separate capabilities; no client-name override or fabricated receipt. |
| P6 native HTTP projection | Explicit relay-base enrollment, Codex/Claude/DSH adapters, transport transitions, persisted-fingerprint drift protection | Included in full 350-test migrated-suite pass |
| P6 HTTP-to-stdio | Owner-host connect-http adapter route | Real two-node fixture initialize/list/call regression passed |
| P6 stdio-to-HTTP | Schema v4 per-target facade URL map and converted projection | Explicit provisioned endpoints only; no auto-spawn/liveness claim; projection tests passed |
| P6 cached facade | Revisioned in-memory last-known-good catalog, stale evidence, atomic refresh, uncertainty-aware errors | Six resilience tests plus existing constant-surface tests passed |
| P7 control | Shared stdio and managed HTTP generation lifecycle, bounded external HTTP contracts | Offline full suite passed; production recovery unverified |
| P8 compaction | Explicit read-only preview and confirmed bounded VACUUM | Six tests: preserved records, busy failure, deadline interruption, CLI/no creation |
| P8 correlation | Opaque origin/id-occurrence evidence for bounded link-stream JSON-RPC, plus explicit-export call/operation joins | 44 combined evidence/export/facade regression tests passed; capacity rejection never resets a live stream, duplicate-id floods degrade immediately; counters exposed in diagnostics |
| P10 modern to legacy | Shared tools-only 2026-07-28 requests to one observed legacy backend generation | Required metadata, actual instructions/capabilities, cancel/progress and no-replay gates passed |
| P10 legacy to modern | Explicit standalone tools-only adapter and modern-only fixture | 32 focused tests passed, including whole-result content-version refusal, nested metadata gates and bounded cancellation state |
| P10 modern HTTP-to-stdio | Explicit modern POST-only path with bounded schema-header preflight and execution-uncertainty evidence | 24 modern + 15 legacy adapter tests passed; owner registration spawn gate also passed |
| P10 modern registry/control | Explicit modern direct frontends with actual tools/instructions and preserved confirmation gates | 29 focused tests passed, including oversized-frame drain and zero side effects on invalid requests |
| P10 canonical legacy baseline | Request 2025-11-25, validate the actual legacy backend choice, project four frozen tools profiles | 25 profile tests and 22 shared regressions passed; unimplemented capabilities withheld, mixed content fails explicitly when unrepresentable |
| Official SDK interoperability | Approved isolated mcp==2.0.0 client against temporary Bridge-owned stdio frontends | Five real SDK cases passed; exact tools/instructions and fallback verified; see [SDK evidence](SDK_INTEROP.md); not full conformance |
| P10 modern stdio-to-HTTP | Explicit stateless façade using only registered connect proxies | 13 modern + 15 legacy façade tests passed; schema/header preflight, real two-node fixtures, cancellation/process cleanup and uncertainty |
| Structure | docs/, tests/, tests/fixtures/; root runtime modules; preserved two host components | Final integrated suite: 506 tests passed |
| Distribution | Approved isolated tooling; sdist-built wheel and temporary isolated installation | 12 runtime modules and 60 source files verified byte-for-byte; both installed 0.4.0 entry points passed; final hashes in `release-artifacts/acceptance.json` |
| P9 field | User-approved ephemeral real Windows/WSL fixtures passed | Both invocation and artifact directions passed; temp cleanup confirmed; installed Agent sessions and production recovery remain unverified |

## DSH dynamic-directory follow-up (2026-09-09)

User-supplied Context Browser snapshots from the DS4F Web child session confirm
54 -> 134 -> 54 tool definitions through an Onshape expand/collapse round trip.
The expanded turn-4/step-1 request added 80 tools and ~16.3k estimated tool tokens;
the collapsed turn-5/step-5 request removed the same 80 tools/~16.3k. Taobao
remained a single library entry. Upstream input accounting for 10:50:32 was
783 uncached + 24,320 cached = 25,103 tokens, matching the 25.1k actual prompt.
See [Tool exposure](MCP_TOOL_EXPOSURE.md) for evidence scope and timestamps.

The dynamic facade guidance now distinguishes bridge-side transition in the
calling step from client-refreshed schema visibility in a subsequent model
request. No new human turn or synchronous notification acknowledgement is
required. A model's claim to remember tool names does not override request
schema evidence; `refreshRequested=false` is not a client completion receipt.

Targeted verification after the correction:

```text
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest tests.test_deferred_tools tests.test_harness_verification tests.test_client_enrollment_verification -q
Ran 51 tests in 11.537s -- OK
```

This targeted pass is not a new full-suite or wheel-install acceptance. The
field test used the already-approved repository frontends. No automatic
capability enrollment, installed-wheel upgrade or service restart accompanied
this guidance correction; already-running frontends retain their loaded wording
until their next initialization. The earlier incomplete isolated probe remains
incomplete and is not repackaged as a fresh successful receipt.

## B′ runtime follow-through (2026-09-09)

B′ is now implemented in the repository's deferred facade: name-only deltas in
content text (and supported structuredContent), explicit re-expansion guidance,
and last-published-name tracking across invalidation. Delta names never serve
as execution authority. Status and notification acknowledgement semantics remain
unchanged. This is a source change, not a production restart or installed-wheel
upgrade.

The prior isolated DS4F V2 experiment used three synthetic task scenarios and two
AB/BA pairs per scenario: A′ and B′ each completed 6/6 tasks in 20 model requests
with no invalid calls. A′ used 6,567 output tokens and 149,334 total input tokens;
B′ used 3,145 output and 155,941 total input. This supports the wording choice,
not a universal cost claim. The production-source wording adds an explicit
client-refresh qualification; this exact runtime build has not been model-tested.
Local raw evidence remains in `release-artifacts/tool-guidance-ab-v2/`; future
fine-grained real-task/model acceptance is a separate roadmap gate.

Focused source regressions after implementation:

```text
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest tests.test_deferred_tools tests.test_harness_verification tests.test_client_enrollment_verification -q
Ran 54 tests in 13.377s -- OK
```

Includes all four legacy profiles, text/structured parity, repeated actions,
invalidated catalogs, replacement deltas and refreshed-name withdrawal.

Full-suite attempts on this source both ran 569 tests and were not clean:

- First attempt (195.282s): 568 passed; the two-component-directory assertion
  found a leftover root `__pycache__/harness_verification.cpython-312.pyc`.
  Removed that generated cache before the next run.
- Second attempt (169.470s): 568 passed; the unchanged modern HTTP test
  `test_bounded_request_response_and_http_worker_lifecycle` raised
  `BrokenPipeError` while its client sent a request to the saturated-worker
  server (test line 426). This test passed in the first attempt. The observation
  suggests timing sensitivity, not a verified root-cause diagnosis; its source
  and the HTTP implementation were not modified for this commit.

The isolated follow-up `PYTHONDONTWRITEBYTECODE=1 python3 -m unittest
tests.test_modern_http_facade -q` passed all 13 tests in 19.298s without changes.
It does not erase the full-run failure or establish a clean full-suite,
packaging or production gate.

## Verification

Executed after implementation freeze:

```text
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s tests -t . -q
Ran 506 tests in 149.949s -- OK
```

Fault-injection diagnostics and deliberately malformed-input warnings appeared as
expected; the suite had no failed tests. Final real Windows/WSL fixture acceptance
also passed again, and temporary-directory removal was checked independently.

A previous wheel validated the new isolated-install harness, but only the fresh
sdist/wheel inventory and source-byte checks certify the final source. The
separate machine-readable artifact receipt is written under `release-artifacts/`
after those commands complete.

Final verification uses the commands in [Verification](VERIFICATION.md), including
an sdist-built wheel, exact runtime/source inventories, and isolated installation.
No source or fixture-only test is described as official SDK conformance.

## Real Windows/WSL Fixture Receipt

With explicit user approval, executed:

```text
PYTHONDONTWRITEBYTECODE=1 python3 -m tests.field_test --timeout 30
ok=true; wslToWindows=true; windowsToWsl=true; artifactsBothDirections=true
windowsInstalledRunner=false; wslInstalledRunner=false
```

The test used temporary registries/workspaces and random loopback ports. Its cleanup
completed; a separate Windows `Test-Path` check confirmed the generated temporary
root no longer existed. This is source-launcher fixture evidence, not installed
Agent or managed-production acceptance.

## External Boundaries

No live service restart, production registry mutation, real Agent-profile edit,
credentials access or business-MCP mutation was performed by this development task.
P5 catalog signature/revocation remains external. DSH Web legacy dynamic-tool
exposure now has the scoped field evidence above; other clients/protocols remain
subject to their own verification.
Optional Tasks/MRTR/subscription and untrusted-local security extensions remain
unadvertised unless their separate complete contracts and acceptance gates pass.
