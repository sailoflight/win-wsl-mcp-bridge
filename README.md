# WIN-WSL MCP Bridge

A standalone, bidirectional bridge for ordinary stdio MCP servers. Business MCP
repositories do not need a WSL half, a TCP listener, bridge launch scripts, or a
special MCP protocol version.

## Components

This project intentionally has only two component directories:

```text
win-bridge-mcp/   Windows node, local proxy, and local registry
wsl-bridge-mcp/   WSL node, local proxy, and local registry
```

Shared protocol/runtime code and project documents are root files so no third
component directory is introduced.

## Bidirectional model

WSL establishes one full-duplex link to Windows. Either node may then open a
logical stream over that same link:

```text
WSL agent -> WSL node -> Windows node -> Windows stdio MCP
Windows agent -> Windows node -> WSL node -> WSL stdio MCP
```

Windows therefore does not need to initiate a separate network connection to
WSL. This works in environments where only WSL-to-Windows loopback establishment
is available.

## Install

Versioned deployments should install the same wheel into dedicated Windows and
WSL virtual environments. The wheel exposes `win-wsl-mcp-win` and
`win-wsl-mcp-wsl`; source-checkout launchers remain available for development.
Run `doctor` on both hosts before `serve`.

```text
python -m pip install win_wsl_mcp_bridge-0.4.0-py3-none-any.whl
win-wsl-mcp-win --version
win-wsl-mcp-wsl --version
```

See `DEPLOYMENT.md` for per-host state placement, registry initialization,
foreground startup, fixture acceptance, rollback, and uninstall procedures.

## Quick start

Initialize each side's local SQLite registry from the example manifest. The
manifest is import input; SQLite is the runtime authority.

```text
# Windows: defaults to %LOCALAPPDATA%\WinWslMcpBridge\registry.sqlite3
python win-bridge-mcp/bridge.py registry-init \
  --manifest win-bridge-mcp/registry.example.json --replace

# WSL: defaults to $XDG_STATE_HOME/win-wsl-mcp-bridge/registry.sqlite3
# or ~/.local/state/win-wsl-mcp-bridge/registry.sqlite3
python3 wsl-bridge-mcp/bridge.py registry-init \
  --manifest wsl-bridge-mcp/registry.example.json --replace
```

Then start the Windows listener and WSL connector:

```text
# Windows
python win-bridge-mcp/bridge.py serve

# WSL
python3 wsl-bridge-mcp/bridge.py serve
```

Expose a Windows MCP to a WSL client:

```text
python3 wsl-bridge-mcp/bridge.py connect <windows-registry-id>
```

Expose a WSL MCP to a Windows client over the same link:

```text
python win-bridge-mcp/bridge.py connect <wsl-registry-id>
```

Expose the read-only peer Registry MCP on either side. It intentionally omits
local MCPs because local agents normally register those directly:

```text
python3 wsl-bridge-mcp/bridge.py registry-mcp
python win-bridge-mcp/bridge.py registry-mcp
```

## Artifact workspace delivery

For an MCP that produces durable files, set this private manifest field before
`registry-init`:

```json
"artifactDelivery": {
  "enabled": true,
  "maxBytes": 536870912
}
```

The receiving node must be started with an Operator-authorized workspace root,
and the local MCP proxy selects an existing inbox beneath that root:

```text
# node service configuration
python3 wsl-bridge-mcp/bridge.py serve \
  --allow-artifact-root /home/user/authorized-workspaces

# MCP client configuration for one workspace
python3 wsl-bridge-mcp/bridge.py connect <windows-registry-id> \
  --artifact-inbox /home/user/authorized-workspaces/project-a
```

The bridge gives only the launched business MCP a private staging directory and
random publish token. The MCP writes a completed file there and invokes:

```text
python bridge.py publish result.step \
  --name result.step --media-type model/step
```

The command blocks until the file is SHA-256 verified and atomically committed
under the caller's local `.mcp-artifacts/` directory, then returns a standard MCP
`resource_link`. The Agent does not fetch from the remote MCP. Plain text that
looks like a path, an undeclared `file://` link, and arbitrary remote paths never
trigger transfer.

See `MCP_COVERAGE.md` for the capability/result coverage matrix and
`ARCHITECTURE.md` for the publisher and security contract.

## Future capability warehouse

The current Registry MCP exposes only peer-installed MCP summaries. A future
opt-in mode may integrate with an external AI capability repository so an agent
configures only the Bridge, searches a larger capability warehouse, and loads a
selected MCP/schema on demand. That mode is not implemented. It requires stable
publisher/name/version identities, local-capability inventory exchange,
deduplication, trust/signature policy, and client support for dynamic tools. All
remaining unsupported MCP/file modes and their acceptance gates are planned in
`DEVELOPMENT_PLAN.md`; they are not current capabilities.

## Current status

Implemented and tested with both the offline Linux-role integration suite and a
real Windows/WSL ephemeral fixture deployment. `field_test.py` starts a Windows
Python node and a WSL Python node with host-local temporary registries, verifies
initialize/list/call and artifact delivery in both directions, and removes all
temporary state:

- one full-duplex link with logical stream multiplexing and per-stream data acknowledgements;
- standard MCP stdio byte forwarding with independent backpressure in both directions;
- local SQLite allowlist registries on both sides;
- peer-only read-only list/search/describe/status Registry MCP;
- redaction of command, args, cwd, and env from public metadata;
- loopback-only listener enforcement;
- bridge-enforced shared JSON-RPC backends for `multiProcessAllowed=false`, with one spawn future and non-overlapping lifecycle generations;
- per-client request-ID, cancellation, progress, response, error, notification, and server-request routing;
- globally serialized shared-backend requests and writes for synchronous business MCPs;
- optional registration-driven exclusive client leases and fixed shared tool-view enforcement;
- generation-scoped process-tree and artifact cleanup without profile-lock deletion or process-name kills;
- negotiated `artifacts/1` workspace-push delivery in both directions;
- explicit business-MCP publishing with no Agent-side remote-file fetch;
- per-stream staging, source snapshot, size limits, SHA-256, and atomic inbox commit;
- independent downstream MCP identity, instructions, capabilities, and tools;
- versioned sdist/wheel packaging, Windows/WSL console scripts, and read-only `doctor` preflight.

The supported security profile is a trusted local host: the local OS user,
Agents, and registered MCPs are trusted, and all bridge clients/listeners are
restricted to loopback. Cross-user local isolation is outside this profile.

Additional managed-production work, not required for the supported foreground
local deployment:

- Windows service/VBS installation and recovery;
- DSH-specific dynamic projection of downstream `initialize.instructions`;
- stream resumption after physical-link loss;
- production log rotation and deployment generation management;
- real business-MCP registration, lease metadata, and rollback evidence.

See `ARCHITECTURE.md` for the protocol and trust boundaries, `DEPLOYMENT.md` for
installation and rollback, and `VERIFICATION.md` for exact checks.
