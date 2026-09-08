# Project Documentation

Read one task entry, then the linked implementation and tests. Roadmap entries
are not current capabilities; fixture checks are not installed-client evidence.

| Task | Canonical entry |
|---|---|
| Product and command overview | [README](../README.md) |
| Protocol, transport, trust and module boundaries | [Architecture](ARCHITECTURE.md) |
| Current supported and unsupported behavior | [MCP coverage](MCP_COVERAGE.md) |
| Remaining development work | [Development plan](DEVELOPMENT_PLAN.md) |
| Current completion evidence and open gates | [Acceptance ledger](IMPLEMENTATION_STATUS.md) |
| Offline checks and field acceptance | [Verification](VERIFICATION.md) |
| Pinned official SDK interoperability | [SDK evidence](SDK_INTEROP.md) |
| Installation, configuration, recovery and maintenance | [Deployment](DEPLOYMENT.md) |
| Release changes | [Changelog](../CHANGELOG.md) |

## Repository Boundaries

`win-bridge-mcp/` and `wsl-bridge-mcp/` are the only runtime components. Shared
stdlib-only runtime modules remain at root so source launchers and wheel imports
use the same module identities. `docs/` and `tests/` are support directories, not
additional runtime components. Development fixtures are excluded from wheels and
included in source distributions for reproducible verification.
