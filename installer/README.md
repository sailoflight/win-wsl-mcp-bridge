# Internal installer

This directory owns the client-specific installation and configuration code
shipped with WIN-WSL MCP Bridge. It is versioned in the same wheel, without a
separate service, listener, registry MCP, or plugin system.

## Ownership

| Module | Responsibility |
|---|---|
| `projection.py` | Host-local enrollment/projection database, bounded client discovery, Codex/Claude/DSH configuration adapters, launcher selection, ownership/drift checks, reconciliation and installation capability evidence |
| `cli.py` | Parser and dispatcher for the existing `projection ...` commands |
| `harness_verification.py` | Isolated MCP client capability fixture and receipt validation |

Client installation choices belong here: known configuration locations, JSON or
TOML dialects, official client CLI syntax, profile overlays and observed client
capability policies. Additional client-specific installation support must extend
this boundary rather than add client-kind branches to the bridge runtime.

The root runtime continues to own generic node/transport behavior, registry and
control MCPs, shared-process routing, protocol negotiation, and protocol-driven
compatibility/deferred frontends. Those mechanisms do not select behavior using
DSH, Codex, Claude, or business-MCP identities.

## Compatibility and dependencies

- Existing `win-wsl-mcp-win projection ...`, `win-wsl-mcp-wsl projection ...` and
  source `bridge.py projection ...` commands keep their options and output.
- Normal node/proxy/MCP commands do not import this package. CLI dispatch loads
  it for `projection`; explicit `registry-init --projection` may load the
  projection database to preserve the existing atomic registry/outbox commit.
- `projection.py` uses generic runtime definitions (`BridgeError`, registry
  queries/paths, protocol constants and canonical JSON). It does not start nodes.
- The root `harness_verification.py` is a compatibility entrypoint and module
  alias. New code imports `installer.harness_verification`; both module launch
  paths and the old source-file launch path remain usable. Newly prepared probe
  commands name the self-contained probe file in this directory.
- Source launcher resolution goes up to the repository root. Wheel deployments
  fall back to the existing installed console commands when component source
  launchers are absent.

This extraction preserves the database schema, saved launcher/exposure choices,
confirmation/dry-run gates, ownership fingerprints and configuration rendering.
It does not enroll environments, rewrite installed client profiles, change
listeners, or deploy a new wheel.

## Verification and rollback

Run the repository's [offline verification](../docs/VERIFICATION.md).
`tests/test_installer_boundary.py` checks ordinary command isolation, projection
CLI loading, probe compatibility and source launcher resolution. Existing
projection/probe tests exercise the moved implementation. Distribution checks
include the package, and `tests/smoke_install.py` tests an isolated installed wheel
outside the source checkout, including installer dry-runs and probe launch.

For this source-only extraction, rollback means restoring the previous source
revision and its matching build metadata/tests/docs, then removing the newly
introduced installer directory. No installed state or database migration is
needed. After an explicit future deployment, roll back the complete matching
wheel using the [deployment procedure](../docs/DEPLOYMENT.md), not individual
Python files.

## Follow-up boundary for the reported observations

The reported launcher normalization, exposure-policy explanation, configuration
ownership/merge behavior, missing-file status and file fingerprints belong to
this installer layer. Their behavior has deliberately not changed in this move.

Two of those reports are now handled here:

- `dsh_node_registry_entry.py` is tracked as this package's client-specific DSH
  stack supervisor. It decides per start whether to own the bridge stack, to
  attach to a ready stack another DSH profile already owns (serving only
  `registry-mcp` and never starting or stopping a node), or to fail when the
  occupied ports do not answer a local registry query. Previously every DSH
  profile tried to own the stack, so every profile after the first printed
  `required bridge port already accepts connections` and exited without the
  registry tools.
- `projection enroll` records a per-client-kind exposure default
  (`--tool-exposure`, `auto` for DeepSeek Harness profiles and `native` for
  every other client). Leaving Harness profiles on the schema default pinned
  each profile of one Harness to the complete fixed catalog independently.
  `deferred` is still only reachable through recorded probe evidence.

The reported server-policy revision mismatch crosses server/host instruction
ownership. Its solution must use generic structured policy metadata and host
validation, rather than adding business policy text to the bridge transport.
These reports remain follow-up work, not fixes claimed by this extraction.
