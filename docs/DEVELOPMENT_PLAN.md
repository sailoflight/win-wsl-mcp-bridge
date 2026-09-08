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

Hermetic Linux fixture tests do not establish real Windows filesystem/process
behavior, installed-client runtime loading or production recovery. Before field
work approve exact hosts/versions, temporary fixture registries/ports/workspaces,
backup/recovery and cleanup, stop conditions and both ownership directions.
Then verify startup pruning, reconnect, canonical instructions visibility,
control scope, generation shutdown and client configuration rollback.
No real business-MCP mutation is authorized by repository development.

Official SDK/conformance runs must identify pinned runner/spec versions and are
separate from hand-written fixture coverage.

## External and Optional Tracks

P5 remote catalogs still need an Operator-owned signature/revocation trust
contract and a concrete client dynamic-tool contract. Catalog metadata never
grants installation, launch, credentials or mutation authority.

Untrusted-local artifact hardening (owner-ACL sockets/pipes, directory handles,
reparse-point-safe APIs, cross-user quotas) remains an optional architecture
track, not a requirement of the supported local-user-trusted deployment profile.
