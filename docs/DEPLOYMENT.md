# Deployment

## Supported profile

The bridge is local-only. The local OS user, Agents, and registered business MCPs
are trusted. Every bridge listener and client must resolve only to loopback. Do
not expose the bridge through a LAN bind, port proxy, container publish rule, or
public tunnel.

Both hosts must run the same bridge protocol release. Version 0.4.0 uses
`win-wsl-mcp-bridge/0.2` and intentionally rejects older peers.

## Build once

Build a wheel and source archive in CI, WSL, or Windows:

```text
python -m pip install build
python -m build
```

The wheel contains the root modules and internal `installer` package declared by
`pyproject.toml`, plus package metadata. Client-specific configuration adapters
and capability probes are owned by [the installer](../installer/README.md);
existing `projection ...` commands remain available. Source-only component
launchers and development tests are not installed. Keep released wheels in an
Operator-owned versions directory for rollback. Do not build directly in the
production checkout. The repository does
not grant an external redistribution license; choose and record one before
publishing artifacts outside the owner-controlled deployment.

## Install on Windows

```text
py -3 -m venv %LOCALAPPDATA%\WinWslMcpBridge\runtime
%LOCALAPPDATA%\WinWslMcpBridge\runtime\Scripts\pip.exe install <wheel-path>
%LOCALAPPDATA%\WinWslMcpBridge\runtime\Scripts\win-wsl-mcp-win.exe --version
```

Initialize the Windows-local registry from an Operator-maintained manifest:

```text
%LOCALAPPDATA%\WinWslMcpBridge\runtime\Scripts\win-wsl-mcp-win.exe registry-init ^
  --manifest C:\path\to\windows-registry.json --replace
```

The default database is
`%LOCALAPPDATA%\WinWslMcpBridge\registry.sqlite3`. Keep it on NTFS under the
Windows user profile, never in `\\wsl$`.

## Install on WSL

```text
python3 -m venv ~/.local/share/win-wsl-mcp-bridge/runtime
~/.local/share/win-wsl-mcp-bridge/runtime/bin/pip install <wheel-path>
~/.local/share/win-wsl-mcp-bridge/runtime/bin/win-wsl-mcp-wsl --version
```

Initialize the WSL-local registry:

```text
~/.local/share/win-wsl-mcp-bridge/runtime/bin/win-wsl-mcp-wsl registry-init \
  --manifest /path/to/wsl-registry.json --replace
```

The default database is beneath `$XDG_STATE_HOME/win-wsl-mcp-bridge` or
`~/.local/state/win-wsl-mcp-bridge`. Never place it under `/mnt/c`.

## Shared backend registration

Use explicit `multiProcessAllowed=false` only for newline-delimited JSON-RPC stdio
MCPs that must have one backend generation per registration and node. The bridge
normalizes enforcement to `bridge-shared-backend`. This mode parses and rewrites
JSON-RPC; `true` and `null` retain dedicated byte-transparent streams.

A generic exclusive resource and fixed shared view can be configured without
putting business-specific logic in the bridge:

```json
{
  "process": {
    "multiProcessAllowed": false,
    "clientLease": {
      "toolPatterns": ["browser_*"],
      "releaseTool": "browser_session",
      "releaseArguments": {"action": "release"},
      "releasedResultPath": ["structuredContent", "profileReleased"],
      "cleanupTimeoutSeconds": 15
    },
    "sharedState": {
      "mode": "fixed",
      "rejectTools": ["mcp_tool_view"]
    }
  }
}
```

The names above are registration data, not built-in bridge knowledge. Adjust them
to the MCP's actual downstream tool names and verified release result. A lease
conflict returns structured `client_lease_busy`; a fixed-view mutation returns
`shared_view_fixed`. Owner disconnect invokes the configured release call. If it
cannot confirm cleanup, only that registration's owned process generation is
stopped. The bridge never deletes browser profile locks or kills Edge by process
name.

## Agent-local file input registration

A business MCP that should accept an Agent-local file (for example a model file
to analyze) opts into the negotiated `artifact-inputs/1` extension with a private
manifest field. It must remain a dedicated process; shared
(`multiProcessAllowed=false`) registrations reject `inputDelivery.enabled` at
`registry-init` because input staging for shared backends is not implemented:

```json
{
  "id": "analysis-mcp",
  "command": "...",
  "process": {"multiProcessAllowed": true},
  "inputDelivery": {"enabled": true, "maxBytes": 268435456}
}
```

The node that launches this MCP creates a private per-stream input stage and
exports `WIN_WSL_MCP_BRIDGE_INPUT_STAGE` and
`WIN_WSL_MCP_BRIDGE_INPUT_PROTOCOL=artifact-inputs/1` to the child. Inputs can
be staged from either direction. For the stdio proxy flow, open the remote MCP
with `connect --print-stream-id` (the node prints the logical stream id to
stderr), then stage one file with the explicit CLI:

```text
win-wsl-mcp-win connect <wsl-registry-id> --local-port 8768 --print-stream-id
win-wsl-mcp-win stage-input C:\AuthorizedWorkspaces\model.step \
  --stream <stream id> --local-port 8768 --name model.step --media-type model/step
```

The `stage-input` command exits 0 with one JSON receipt containing the explicit
`bridge-input://input-...` handle, and the adapter puts that handle verbatim in
the next `tools/call`; the bridge rewrites only that exact literal to the staged
path on the MCP host. A raw adapter instead issues one bounded local
`stage_input` request. Either way the source must be a regular, non-symlink file
beneath that node's configured `--allow-artifact-root`. Reserved
`WIN_WSL_MCP_BRIDGE_INPUT_*` names must never appear in a manifest `env`.

## Agent-environment projection (P0-B)

On each host, after both registries are initialized and the bridge link is up:

```bash
# read-only: enumerate enrollable Codex / Claude Code / DSH configs
python3 wsl-bridge-mcp/bridge.py projection scan --side wsl
# enroll exactly one revalidated candidate (no environment is auto-enrolled)
python3 wsl-bridge-mcp/bridge.py projection enroll <candidate-id> \
    --side wsl --projection ~/.local/state/win-wsl-mcp-bridge/projection.sqlite3 \
    --confirm
# project the PEER registry into each enrolled environment
python3 wsl-bridge-mcp/bridge.py projection sync --side wsl \
    --projection ~/.local/state/win-wsl-mcp-bridge/projection.sqlite3 \
    --source registry-remote
python3 wsl-bridge-mcp/bridge.py projection reconcile --side wsl \
    --projection ~/.local/state/win-wsl-mcp-bridge/projection.sqlite3
# optional polling (Ctrl-C to stop); inspect state with:
python3 wsl-bridge-mcp/bridge.py projection status --side wsl \
    --projection ~/.local/state/win-wsl-mcp-bridge/projection.sqlite3
# read-only Agent view of proved client capabilities and the next observation
python3 wsl-bridge-mcp/bridge.py projection probe-status --side wsl \
    --projection ~/.local/state/win-wsl-mcp-bridge/projection.sqlite3
```

`projection probe-status` writes nothing: it reports per environment which
capability aspect is proved, stale, unprobed, or not observable with this probe
version, the recorded version fingerprint and timestamps, which prepared
challenges are outstanding, whether the peer MCP set changed since the recorded
observation (`recheckRequired` / `newPeerServers`), and the next concrete step.
See [Tool exposure](MCP_TOOL_EXPOSURE.md) for the aspect table and the exact
meaning of a negative observation.

`registry-init --projection <path>` creates the authority and appends outbox
events atomically with projection-affecting registry commits. Each host keeps
its own projection database on its own filesystem (never a shared
Windows/WSL location). Reconcile errors are per-environment: an environment
whose config cannot be touched (e.g. an unmanaged Codex config with no official
CLI) reports `error` and never blocks the others. See `ARCHITECTURE.md`.

Native HTTP enrollment is explicit: first provision this host's loopback relay with
`serve --http-relay-port <port>`, then select a tested HTTP-capable client and enroll
with `--native-http --relay-url http://127.0.0.1:<port> --confirm`. The URL is this
host's relay, never the peer's private business endpoint. Enrollment/reconcile do
not start the relay or prove client runtime loading. Same-name transport changes
verify persisted fingerprints, remove/add through the adapter, and roll back
failed writes. User edits are reported as drift rather than overwritten.

## Explicit protocol-era conversion

The HTTP-to-stdio adapter defaults to the legacy session protocol. For a modern
`2026-07-28` HTTP backend, explicitly set owner-local `transport.protocolEra` to
`"modern"` in that registration before running `connect-http`. This selects the
adapter's `--protocol-era modern` path; the field is private and never changes
peer transport summaries. Native relays do not perform protocol-era conversion.

For a legacy stdio Agent consuming a modern-only stdio backend, use the explicit
`legacy_modern_stdio.py` wrapper with a local Operator-provided backend command.
This tools-only wrapper is separate from raw dedicated relay behavior; it never
turns a peer id or endpoint into an arbitrary launch definition.

The Bridge-owned `registry-mcp` and `control-mcp` commands also accept explicit
`--protocol-era modern`. Their modern surface uses per-request metadata and
`server/discover` with the real endpoint instructions and fixed tools. Control
confirmation and exact-generation requirements are unchanged. Without the flag,
the existing legacy frontend remains selected.

Modern HTTP-to-stdio `tools/call` requests perform a fresh, bounded read-only
`tools/list` preflight before sending the business request. This validates the
backend's schema-driven `Mcp-Param-*` headers; unsupported annotations, missing
tools or catalog bounds fail before execution. A transport failure after the
business send starts reports `outcomeUnknown=true`; that is never replay advice.

## Optional control and development evidence

Register `control-mcp` separately from all business MCPs only when Agent lifecycle control is
desired. It has a fixed two-tool surface; each target remains deny-by-default until its local
manifest enables `management.agentControl` and enumerates allowed actions. Run calls once as a
preview and confirm only the reviewed exact operation. Operator service recovery continues to
use the component `lifecycle` CLI.

Serving nodes create `<registry-stem>.events.sqlite3` beside their host-local registry. Do not place this file on
a Windows/WSL shared filesystem. Metadata retention is bounded. Development payload capture is
default-off; `trace start --level snippet|complete` previews a sensitive-data warning unless
`--confirm-sensitive` is supplied, and every session has explicit seconds and byte limits.
Streamable HTTP rows can be projected natively into explicitly enrolled HTTP-capable
clients using the configured local relay base. The owner-host `connect-http` compatibility
facade is the explicit conversion alternative. The opt-in `serve --http-relay-port`
relay mounts each peer HTTP registration at `/mcp/<registered-id>`. For a loopback
HTTP-only Agent consuming a peer stdio registration, run `stdio_http_facade.py --side <side>
--target <registered-id> --listen-port <port>`; this explicit converted route owns one disposable
`bridge.py connect` subprocess per HTTP MCP session and does not expose peer launch definitions.

To let the owning node run `lifecycle drain|restart|stop` against an externally owned
`streamable-http` service, register `management.ownership: "external-controlled"` plus a private
`management.controlContract` naming the allowed lifecycle actions. Each action names one fixed
HTTP method, a relative `path` (resolved against the registered private endpoint) or an absolute
loopback `url`, a bounded `timeoutSeconds`, bounded `successStatuses`, and optionally a bounded
loopback GET `readiness` gate. The contract never names a command, service, container, or pid
and is never disclosed to peers. Without it, `external` HTTP rows remain observation-only
and mutations are refused. A `bridge-managed` row instead registers an owner-local launch
definition and `management.supervision` policy; the Bridge owns one process generation,
gates readiness, and requires exact-generation lifecycle checks before stop/restart.
Never change a deployed ownership policy merely to make a refused control operation succeed.

### Journal space reclamation

Logical retention is automatic; physical compaction is a separate, explicit maintenance
operation. Select the existing host-local journal, confirm the service identity and recovery
plan, and quiesce journal readers/writers before applying. Preview:

```text
python3 wsl-bridge-mcp/bridge.py trace compact --journal <local-journal.sqlite3>
python3 wsl-bridge-mcp/bridge.py trace compact --journal <local-journal.sqlite3> --confirm --maintenance-timeout 10
```

Preview does not create, migrate, or prune the journal. Confirmed compaction uses SQLite's
transactional `VACUUM`, a bounded lock wait, and an execution progress deadline (1-60 seconds).
SQLite rolls back an interrupted VACUUM; OS filesystem stalls are not covered by that deadline.
Busy readers/writers fail closed. A writer arriving after committed compaction may leave a busy
final checkpoint, reported separately. This reclaims pages without intentionally deleting any
logical record; it is neither retention pruning nor secure erasure of sensitive data.

### Offline cross-host evidence

After separately authorized metadata-only exports on each host, an Operator may
place those completed bundles locally and run:

```text
python3 -m journal_evidence win-events.zip wsl-events.zip --limit 20
```

This reads only the explicitly selected files, checks their member digests and
bounds, and correlates lifecycle operation ids. It never fetches a peer path or
raw journal. Results omit payload metadata and paths; source clocks are not assumed
synchronized, and absent evidence does not prove that an operation never ran.
The digest proves consistency, not authenticity. General business-call correlation
is not implied by lifecycle-operation correlation.

## Persistent stdio connector and reloadable engine

Every per-target `connect <registered-id>` process is a persistent connector:
it keeps its stdio MCP endpoint alive while the Agent's stdin is open, and after
the node, local socket, peer link, or downstream backend closes it reconnects to
its local node, replays only the cached MCP `initialize` +
`notifications/initialized` handshake to the fresh downstream session, never
replays a business call, and fails each call that was pending at the moment of
loss exactly once with a concise Bridge JSON-RPC error. Connected business
payloads stay byte-transparent; no new Agent-visible tool or Adapter is
introduced. The `connect` handshake carries `connectorVersion`; the node reply
carries `coreVersion`. Versions pair by simple direct equality (no ordering):
when the node `coreVersion` differs from the engine's `CORE_VERSION`, the
connector appends one short warning to the `initialize` result `instructions`
and to Bridge-generated JSON-RPC errors (business results stay untouched unless
the engine policy explicitly opts into a single-line note). No Operator action
is required for this default behavior.

Engine updates are optional. The engine is a plain stdlib policy module loaded by
the frozen core:

- `WIN_WSL_MCP_BRIDGE_CONNECTOR_ENGINE=<file>` selects one explicit engine module
  file. It fails closed (startup exits 1) when that file cannot load, so use it
  only for pinned, verified engines.
- `WIN_WSL_MCP_BRIDGE_CONNECTOR_ENGINE_DIR=<directory>` scans an engine
  directory. Every candidate is an immutable bundle: a module file plus a
  sibling `<name>-<version>.engine.json` manifest declaring `module` (a `.py`
  file name inside the directory), `version`, `coreVersion`, and `sha256` of the
  module bytes. The connector verifies the digest, imports the module under a
  unique immutable name, requires the module's own `ENGINE_VERSION` and
  `Recovery.CORE_VERSION` to equal the manifest, and selects the newest verified
  version. A candidate that is tampered, out of bounds, unloadable, or
  inconsistent is skipped; if none loads, the connector rolls back to the
  built-in `connector_engine.py`.
- Both environment variables are read by the Agent-side `connect` process, so
  set them in the environment of the client/session that launches `connect`
  (for example the enrollment/service launch environment), not on the peer
  node.

Upgrade/rollback procedure for one host:

```text
# 1. stage the new bundle(s) into the scanned directory (module + manifest)
mkdir -p "$XDG_STATE_HOME/win-wsl-mcp-bridge/connector-engines"
cp connector_engine-1.1.0.py  "$XDG_STATE_HOME/win-wsl-mcp-bridge/connector-engines/"
cp connector_engine-1.1.0.engine.json "$XDG_STATE_HOME/win-wsl-mcp-bridge/connector-engines/"
# 2. verify with one connect session (initialize/list/call), then roll out
# 3. rollback = remove the newer bundle; the next connect picks the previous
#    verified version, or the built-in engine when nothing else remains
```

`VERIFICATION.md` lists the exact offline checks; the bundled fixtures never
touch a real business MCP or production registry.

## Preflight

Run `doctor` on each host before starting either node. Supply every artifact root
that will be accepted by that node:

```text
win-wsl-mcp-win doctor --allow-artifact-root C:\AuthorizedWorkspaces
win-wsl-mcp-wsl doctor --allow-artifact-root /home/user/authorized-workspaces
```

`doctor` exits zero only when Python, protocol identity, loopback hosts, ports,
registry schema, and configured artifact roots pass local checks. It does not
start listeners or probe business MCPs.

## Start and verify

Start the Windows listener first, then the WSL connector in foreground terminals:

```text
win-wsl-mcp-win serve --allow-artifact-root C:\AuthorizedWorkspaces
win-wsl-mcp-wsl serve --allow-artifact-root /home/user/authorized-workspaces
```

Expected diagnostics include the local control listener, Windows peer listener,
and `peer link established` on both sides. WSL must be able to reach the Windows
listener through `127.0.0.1`; use WSL mirrored networking or another supported
same-loopback configuration. The bridge deliberately rejects non-loopback
fallback addresses.

Bridge-owned shared backends are observed and controlled per owning node through
the loopback Operator CLI (no Agent-visible tool is added, and a remote MCP stays
equivalent to a locally registered one):

```text
win-wsl-mcp-win lifecycle status
win-wsl-mcp-win lifecycle status --id <windows-registered-id>
win-wsl-mcp-win lifecycle drain   --id <id>                 # read-only preview
win-wsl-mcp-win lifecycle restart --id <id> --generation 3 --confirm
win-wsl-mcp-win lifecycle stop    --id <id> --confirm
```

Run the component of the host whose node owns the registration. Mutations apply
only with `--confirm`; the optional `--generation` refuses stale-ownership
control. Dedicated registrations appear in status only as active-stream counts
and refuse lifecycle control (their stream close already stops them).

Verify both directions with non-production fixture MCPs before registering a
business MCP:

```text
win-wsl-mcp-wsl connect <windows-fixture-id>
win-wsl-mcp-win connect <wsl-fixture-id>
```

Then verify the peer Registry MCP and an artifact round trip into an explicitly
authorized temporary workspace. Do not migrate a real MCP until its initialize,
tools/list, one read-only call, cancellation, shutdown, and any artifact workflow
all pass.

From a WSL source checkout, the ephemeral field harness automates that fixture
acceptance and cleans both hosts:

```text
PYTHONDONTWRITEBYTECODE=1 python3 tests/field_test.py
```

To validate installed wheel entry points rather than source launchers, create the
two temporary venvs first and pass their runners:

```text
python3 tests/field_test.py \
  --windows-runner /mnt/c/.../Scripts/win-wsl-mcp-win.exe \
  --wsl-runner /tmp/.../bin/win-wsl-mcp-wsl
```

## Stop, rollback, and uninstall

Foreground nodes stop with Ctrl+C. On POSIX, SIGTERM performs bounded stream,
lease, backend-generation, and owned process-group cleanup. Windows shared backends are assigned to a
kill-on-close Job Object; cleanup first uses the configured release call and EOF,
then closes only that generation's job, with an exact backend-PID tree fallback
during failed startup. It never removes profile locks or kills an image name
globally. Windows service installation and recovery are not part of this release; an
Operator may wrap the foreground command only after separate service acceptance
testing.

Before upgrade, retain the previous wheel, registry manifest, and a stopped copy
of each local SQLite database. Roll back per host:

```text
pip install --force-reinstall <previous-wheel>
```

If a newer release changed the registry schema, preserve the newer database as a
backup and recreate the older schema with the last-known-good manifest and
`registry-init --replace`. Committed `.mcp-artifacts` outputs are never removed by
rollback.

To uninstall, stop the node and run `pip uninstall win-wsl-mcp-bridge`, or delete
the dedicated venv. Removing registries, manifests, spools, or committed workspace
artifacts is a separate destructive Operator action and is never automatic.
