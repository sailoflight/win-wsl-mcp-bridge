# Implementation Acceptance Ledger

This records implementation evidence separately from the complete requirements
in [Milestone design](MILESTONE_DESIGN.md) and remaining [Development plan](DEVELOPMENT_PLAN.md).
Linux role simulation never proves installed-client or real Windows behavior.

## Current Status

| Area | Delivered | Evidence or remaining gate |
|---|---|---|
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
P5 catalog signature/revocation and dynamic-client contracts remain external.
Optional Tasks/MRTR/subscription and untrusted-local security extensions remain
unadvertised unless their separate complete contracts and acceptance gates pass.
