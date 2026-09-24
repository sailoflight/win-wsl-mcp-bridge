# Project Documentation

Read one task entry, then the linked implementation and tests. Roadmap entries
are not current capabilities; fixture checks are not installed-client evidence.

| Task | Canonical entry |
|---|---|
| Product and command overview | [README](../README.md) |
| Protocol, transport, trust and module boundaries | [Architecture](ARCHITECTURE.md) |
| Current supported and unsupported behavior | [MCP coverage](MCP_COVERAGE.md) |
| Axis-based development, behavior combinations, dependencies and release gates | [未来开发总计划](DEVELOPMENT_PLAN.md) |
| Current completion evidence and open gates | [Acceptance ledger](IMPLEMENTATION_STATUS.md) |
| Offline checks and field acceptance | [Verification](VERIFICATION.md) |
| Pinned official SDK interoperability | [SDK evidence](SDK_INTEROP.md) |
| Harness capability probes and MCP tool exposure | [Tool exposure](MCP_TOOL_EXPOSURE.md) |
| Ten behavior axes, composition constraints, environment evidence and implementation gaps | [桥行为轴与组合矩阵](MCP_EXPOSURE_TAXONOMY.md) |
| Installation, configuration, recovery and maintenance | [Deployment](DEPLOYMENT.md) |
| Client-specific installation and configuration adapter ownership | [Installer](../installer/README.md) |
| Release changes | [Changelog](../CHANGELOG.md) |

## Repository Boundaries

`win-bridge-mcp/` and `wsl-bridge-mcp/` are the only runtime components. Shared
stdlib-only runtime modules remain at root. `installer/` is an internal support
package shipped in the same wheel; client-specific discovery and configuration
adapters belong there. `docs/` and `tests/` are support directories, not additional
runtime components. Development fixtures are excluded from wheels and included
in source distributions for reproducible verification.
