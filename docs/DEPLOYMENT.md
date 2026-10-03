# Deployment

## Supported profile

The bridge is local-only. The local OS user, Agents, and registered business MCPs
are trusted. Every bridge listener and client must resolve only to loopback. Do
not expose the bridge through a LAN bind, port proxy, container publish rule, or
public tunnel.

Both hosts must run the same bridge protocol release. Version 0.4.2 uses
`win-wsl-mcp-bridge/0.2` and intentionally rejects older peers. The package
version is not a compatibility check between halves; `bridge_diagnostics`
reports the runtime revision each side actually loaded, because a working tree
and an installed release both answer with this version string.

## Release identity and additional machines

The supported deployment is one Windows + WSL pair on one machine, because every
listener and client resolves only to loopback. "Another device" therefore means
installing this same release on that machine's own two halves, not bridging two
machines over a network.

Current release:

| Item | Value |
|---|---|
| Version | 0.4.2 (`bridge_runtime.SERVER_VERSION`) |
| Wheel | `win_wsl_mcp_bridge-0.4.2-py3-none-any.whl` |
| sha256 | `9e1bb3f26c4ad42a3ffdddd421c0270d81cf6532848d447c3d80ea931b6fa936` |
| Staged copies | `%LOCALAPPDATA%\WinWslMcpBridge\releases\0.4.2-017f988\` and `~/.local/share/win-wsl-mcp-bridge/releases/0.4.2-017f988\`, each with a `SHA256SUMS` that verifies |
| Requirements | Python >= 3.11, no third-party dependencies |
| Console scripts | `win-wsl-mcp-win`, `win-wsl-mcp-wsl` |
| Bridge protocol | `win-wsl-mcp-bridge/0.2`; older peers are rejected |
| Runtime revision | `0170f156361c` (what `bridge_diagnostics` compares) |
| Rollback | 0.4.1 wheel in `releases/0.4.1-ec65715/`; pre-0.4.1 restore point in `backups/20261002T120851Z-pre-0.4.1/` |

Install on an additional machine:

1. Copy the wheel and its `SHA256SUMS` there and verify the digest
   (`sha256sum -c SHA256SUMS` on WSL, `Get-FileHash -Algorithm SHA256` on
   Windows). This host also keeps a copy in the ignored `dist/` directory.
2. Install the *same* wheel into a dedicated virtual environment on **both**
   halves, with the commands in "Install on Windows" and "Install on WSL" below.
   Each half must answer version `0.4.2`.
3. Initialize that machine's own registries from its own manifests
   (`win-bridge-mcp/registry.example.json`, `wsl-bridge-mcp/registry.example.json`;
   the current four rows are recorded in
   [the registration record](REGISTRATION_RECORD_20260930.md) §3). Never point a
   registry at another host's database or at a shared filesystem.
4. Start both halves and compare `bridge_diagnostics`: the revision check must
   read `match` on the same `runtimeRevision`. The package version string alone
   proves nothing, because a working tree answers with it too.
5. Enroll that machine's clients (`projection scan` -> `enroll` -> `reconcile`);
   see "Agent-environment projection" below.

Both halves of this host were verified against this exact artifact on
2026-10-03: fresh virtual environments on WSL (Python 3.12.3) and Windows
(Python 3.14.6) installed the wheel and answered `doctor ok: true`,
version `0.4.2`, runtime revision `0170f156361c`.

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

## Registration mode: the concurrency axis

A registration declares `process.concurrency`, one of three modes. The mode says how
many logical clients may share how many backend processes, and it is the Operator's
main risk decision:

| `concurrency` | clients : backends | what it asks of the business MCP |
|---|---|---|
| `one-to-one` (default) | 1 : 1 | nothing: no concurrency, no release tool, no tool names |
| `many-to-one` | N : 1 | its session survives several clients; a resource lease wherever clients share a resource |
| `many-to-many` | N : N | it survives several instances: no fixed output path, no single-instance lock, no device or licence seat conflict |

`one-to-one` is the default because it is the only mode that needs nothing proven on
either side. Writing the field explicitly is allowed and clearer; leaving it out
selects it. An explicit `multiProcessAllowed: null` is not a third state — it means
"not declared" and lands on the same default.

Promotion is earned, not assumed. Declaring `many-to-one` or `many-to-many` also
requires `process.concurrencyEvidence`, a short string naming the observation that
justifies it — two clients sharing one session with no interference, or two instances
running side by side with no interference. `registry-init` rejects the promotion
without it. An MCP nobody has observed stays `one-to-one`; the bridge does not guess.

Why the safest mode first: the second instance or the second client is what breaks
quietly. An MCP with a fixed output path, a single lock file, a device session, or a
license that refuses a second seat can collide with its own copy, and the symptom is a
wrong or half-written result rather than an error the caller can see. `one-to-one`
removes the question entirely: one client, one backend, neither side needing any
concurrency support.

What `one-to-one` does: the second client's `connect` is refused with
`client_admission_exclusive` (marked retryable), and its `initialize` never reaches the
MCP. When the owner detaches the generation stops, so the next client gets a fresh
backend. Nothing is asked of the MCP: no release tool, no tool names, no cooperation.

What promotion costs, so that it is a real decision: `many-to-one` makes the bridge
parse and rewrite JSON-RPC instead of passing bytes through; it needs
`sharedState.mode=fixed` for connection-scoped tool views; it rejects `inputDelivery`,
so an Agent-local file can only be staged to a dedicated process; and it serializes
calls from different clients, so one long call delays the others. `many-to-many` keeps
bytes transparent and isolates clients, at the price of N processes, N licenses or
caches, and exposure to the MCP's own concurrency.

Neither mode is a higher security level. The supported profile already trusts the local
OS user, Agents, and registered business MCPs, and does not defend them against one
another; the modes change fidelity and capability, not trust. One of the shared modes'
costs is a long-term debt rather than a one-time price: because the bridge interprets
the protocol instead of forwarding bytes, it must keep up with every protocol detail
the MCP uses (cancellation, progress, `_meta`, JSON-RPC error shape, future methods),
and every gap is a compatibility defect the transparent path cannot have.

One exception: a bridge-managed `streamable-http` registration defaults to
`many-to-many`, because the bridge-owned backend is stdio-only.

## Shared backend registration

Use `concurrency: "many-to-one"` (or the historical `multiProcessAllowed: false`) for
registrations that run one shared backend generation per registration and node. The
bridge normalizes enforcement to `bridge-shared-backend`. This mode parses and rewrites
JSON-RPC, needs `sharedState.mode=fixed` when the MCP exposes connection-scoped views,
and cannot use `inputDelivery` (below).

`multiProcessAllowed` and `enforcement` are derived: the bridge rewrites both on every
registration it accepts, so read `concurrency` as the authored field. `true` maps to
`many-to-many` and keeps a dedicated byte-transparent stream; a registration that
declares both `concurrency` and `multiProcessAllowed` must agree.

A generic exclusive resource and fixed shared view can be configured without
putting business-specific logic in the bridge:

```json
{
  "process": {
    "concurrency": "many-to-one",
    "concurrencyEvidence": "two DSH profiles shared one browser session, no interference",
    "clientLease": {
      "toolPatterns": ["browser_*"],
      "releaseTool": "browser_session",
      "releaseArguments": {"action": "release"},
      "releasedResultPath": ["structuredContent", "profileReleased"],
      "cleanupTimeoutSeconds": 15
    },
    "sharedState": {
      "mode": "fixed",
      "rejectTools": [
        "mcp_tool_view",
        {"tool": "mcp_tool_invoke", "path": ["name"], "equals": ["mcp_tool_view"]}
      ]
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

A `rejectTools` entry is either a tool name — rejected on the name alone — or one
bounded rule `{"tool": …, "path": […], "equals": […]}` that rejects a call only
when the exact string reached by walking `path` through the call arguments is one
of `equals`. The second form exists for the ordinary shape where one downstream
tool is a call-by-name door: it takes the real target in an argument field, so a
name-only rejection of `mcp_tool_view` would not cover
`mcp_tool_invoke` with `{"name": "mcp_tool_view"}`. Matching is exact string
equality over at most four argument keys and thirty-two values per rule, with at
most thirty-two rules; the bridge gates a call shape and never interprets
business arguments, so patterns and regexes are rejected at `registry-init`. A
plain name list keeps its historical serialized shape, and the public listing
never exposes the rules.

## Agent-local file input registration

A business MCP that should accept an Agent-local file (for example a model file
to analyze) opts into the negotiated `artifact-inputs/1` extension with a private
manifest field. It must remain a dedicated process; shared-backend
registrations (the default `one-to-one`, or `many-to-one`) reject
`inputDelivery.enabled` at `registry-init` because input staging for shared
backends is not implemented:

```json
{
  "id": "analysis-mcp",
  "command": "...",
  "process": {
    "concurrency": "many-to-many",
    "concurrencyEvidence": "two instances observed side by side, no interference"
  },
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

## Deployment form of the projected launcher

A projected entry decides which bridge code a *business client* runs, so its
default is the deployment form: the console script this host installed.

```bash
# inspect what each enrolled environment actually launches
python3 wsl-bridge-mcp/bridge.py projection status --side wsl \
    --projection ~/.local/state/win-wsl-mcp-bridge/projection.sqlite3
```

`enroll` resolves the launcher in this order and records one of three forms:

1. the installed console script of this side, as an absolute path
   (`$XDG_DATA_HOME/win-wsl-mcp-bridge/runtime/bin/win-wsl-mcp-wsl`, or
   `%LOCALAPPDATA%\WinWslMcpBridge\runtime\Scripts\win-wsl-mcp-win.exe`) —
   derived the way this host's registry path is derived, so one host resolves
   one installation for both its registry and its launcher;
2. the working-tree component (`sys.executable <tree>/<side>-bridge-mcp/bridge.py`)
   only when this host owns no installation: a development fallback, never a
   deployment;
3. the bare console entry name when an installation exists that this module
   cannot see from here.

Forms 1 and 3 launch the installed artifact; the console form carries no Python
arguments at all, so it also removes the bytecode and interpreter-path concerns
of a tree launcher. Form 2 is accepted but is a deviation: the working tree is
mutable, and a client pinned to it keeps following edits while the node keeps
running the release that was installed. `SERVER_VERSION` cannot detect that,
because both report the same version string.

Two surfaces make a deviation visible instead:

* `bridge_diagnostics` returns the node's `runtimeRevision`, a 12-hex digest of
  every top-level runtime module the node loaded, and `revisionCheck` compares
  it with the digest of the code the *client-side* frontend is running:
  `verdict` is `match` (both halves run byte-identical runtime code), `differs`
  (one side was edited or installed differently), or `unknown` (the node
  reported no digest and predates this probe). Only `match` is healthy, and it
  is a code identity rather than a location guarantee: a working-tree launcher
  whose bytes still equal the installed release reports `match` too, which is
  why the projected launcher is pinned instead of merely monitored. This
  compares one host's node with the client-side frontend talking to it;
  cross-host alignment is checked by running `doctor` on both hosts and
  comparing the reported `runtimeRevision`;
* `doctor` prints this host's `runtimeRevision`, so the two hosts can be
  compared directly.

Re-pinning an enrolled environment (there is no in-place launcher editor, and a
recorded launcher is never silently rewritten) is a deliberate re-enrollment,
where `--launcher` / `--launcher-args` express an explicit deviation:

```bash
python3 wsl-bridge-mcp/bridge.py projection unenroll <environment-id> \
    --remove-entries --confirm
python3 wsl-bridge-mcp/bridge.py projection scan --side wsl
python3 wsl-bridge-mcp/bridge.py projection enroll <candidate-id> --side wsl \
    --projection ~/.local/state/win-wsl-mcp-bridge/projection.sqlite3 --confirm
python3 wsl-bridge-mcp/bridge.py projection reconcile --side wsl \
    --projection ~/.local/state/win-wsl-mcp-bridge/projection.sqlite3
```

`unenroll --remove-entries` deletes the entries this environment owns and the
environment row, and the following `enroll` records the current default
launcher; `reconcile` then writes the entries again through the environment's
adapter. The scan is repeated because removing and re-adding entries changes the
document a candidate is derived from. Do **not** use `--keep-entries` here: it
leaves the old entries in the document while deleting the projection rows that
recorded them, so the next reconcile sees the names as occupied and reports an
*unmanaged name collision* (or drift) instead of rewriting them. After the
re-pin, re-run `projection status` to confirm `launcherCommand` points at the
installed console script, and `bridge_diagnostics` to confirm
`revisionCheck.verdict` is `match`. Refresh the installation before re-pinning:
the pinned artifact is what every client of that environment will run.

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

## Which session the Windows node runs in

A business MCP that needs a person can only show that person a window if its own
process lives in that person's interactive Windows session. A Windows process
cannot move itself: children inherit the session of whoever started them, and a
process in session 0 can never reach a desktop. "Run the node as a service (or
from a service-like context)" and "show a browser window to a person" are
therefore mutually exclusive. Installing the node as a service, or creating a
process in another session (`WTSQueryUserToken` + `CreateProcessAsUser`), is
outside this release and outside its supported profile.

The bridge itself never chooses a session and never hides a window: it starts
downstream servers with only `CREATE_NEW_PROCESS_GROUP` and no
`CREATE_NEW_CONSOLE`, `DETACHED_PROCESS`, or hidden-window flag. The browser a
GUI-needing MCP opens lands in whatever session that server inherited, and a
stack started through WSL interop inherits the WSL interop chain's session.

To run the Windows node inside the signed-on user's session, register one logon
task with an interactive principal (no elevation required). The action must be a
launcher that gives the node **no console window at all**; hosting the console
application itself creates a window a person can close, and closing it terminates
the node with `STATUS_CONTROL_C_EXIT` (`0xC000013A`), which takes the whole
loopback stack down and leaves every client with a bridge error. Deploy
`C:\MCP\win-wsl-bridge\start-windows-node.pyw`:

```python
"""Hidden launcher for the WIN-WSL bridge Windows node (pythonw, no console)."""
from __future__ import annotations

import os
import sys

_ROOT = os.path.join(os.environ["LOCALAPPDATA"], "WinWslMcpBridge")
_LOG_DIR = os.path.join(_ROOT, "logs")
_LOG = os.path.join(_LOG_DIR, "windows-node.log")
_PREV = _LOG + ".prev"


def _log_stream() -> object:
    try:
        os.makedirs(_LOG_DIR, exist_ok=True)
        if os.path.exists(_LOG):
            if os.path.exists(_PREV):
                os.remove(_PREV)
            os.replace(_LOG, _PREV)
        return open(_LOG, "a", encoding="utf-8", errors="replace", buffering=1)
    except OSError:
        return open(os.devnull, "a", encoding="utf-8")


sys.stdout = _log_stream()
sys.stderr = sys.stdout

from bridge_runtime import win_main  # noqa: E402  (after the stream redirect)

if __name__ == "__main__":
    raise SystemExit(win_main())
```

then register the task:

```text
$pyw  = "$env:LOCALAPPDATA\WinWslMcpBridge\runtime\Scripts\pythonw.exe"
$args = "C:\MCP\win-wsl-bridge\start-windows-node.pyw serve --registry $env:LOCALAPPDATA/WinWslMcpBridge/registry.sqlite3 --local-port 8768 --link-port 8770"
Register-ScheduledTask -TaskName WinWslMcpBridgeWindowsNode `
  -Action    (New-ScheduledTaskAction -Execute $pyw -Argument $args -WorkingDirectory 'C:\MCP') `
  -Trigger   (New-ScheduledTaskTrigger -AtLogOn -User $env:USERNAME) `
  -Principal (New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive -RunLevel Limited) `
  -Settings  (New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -ExecutionTimeLimit (New-TimeSpan -Seconds 0) -MultipleInstances IgnoreNew -StartWhenAvailable -RestartCount 5 -RestartInterval (New-TimeSpan -Minutes 1))
```

`ExecutionTimeLimit` must be unlimited (`PT0S`): the default three-day limit
would kill a long-running node. Through the `pythonw` shim the action lives
exactly as long as the node, so the task reads `Running` while the node serves
and `RestartCount`/`RestartInterval` (five attempts, one minute apart) really do
retry a non-zero exit; `pythonw` also keeps the entry point
(`bridge_runtime:win_main`), the arguments, and the interactive session. The task
coexists with the DSH supervisor: when the Windows node answers and the local WSL
registry does not, the supervisor's `_plan` returns `reclaim`, so it starts the
WSL node rather than a second Windows node. `Start-ScheduledTask -TaskName
WinWslMcpBridgeWindowsNode` switches a running deployment over without a reboot,
and the node's own log lands in
`%LOCALAPPDATA%\WinWslMcpBridge\logs\windows-node.log` (previous run `.prev`).
Stop it deliberately with `Stop-ScheduledTask -TaskName
WinWslMcpBridgeWindowsNode`, or `taskkill /PID <node pid> /T /F`.

Consequences, alternatives, and rollback:

- With no signed-on session the node does not start. Signing out stops it; a
  disconnected RDP session does not. Register the task for the user whose session
  must host the window, and restart the node in a different session if the human
  moves, because a console session and an RDP session are different sessions.
- A person may instead launch the browser themselves in their own session using
  the same profile the MCP uses; the MCP can still drive it over CDP at
  `127.0.0.1:<port>`, because loopback TCP is machine-wide, not session-scoped.
- Roll back by stopping the task's process, unregistering the task, and letting
  the supervisor's `own` path start the stack as before.
- Known regression path: after a sign-out removes the task's node, a client that
  starts the stack from a session-0 context makes the supervisor `own`, so it
  launches the Windows node itself (session 0 again) and the task's node later
  loses the port race and exits. There is no supervisor knob to prevent that in
  this release. After a sign-out/sign-in cycle, confirm the node's `SessionId`;
  if it is 0, stop that stray node and re-run the task.
- Observed on 2026-10-03 after switching this host over: the node and all four
  registered MCP servers ran in session 2, the peer link re-established itself,
  and `bridge_diagnostics` reported `revisionCheck: match`. The first attempt
  hosted the node's console window directly; a person closed that window, the
  task's last result was `0xC000013A`, and both nodes stopped until the task was
  started again. The `pythonw` launcher above is the corrected form.

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
