# Architecture

## Objective

The bridge makes a remote standard stdio MCP appear local without requiring a
special MCP protocol version. Ordinary MCPs require no bridge-specific code.
An MCP that produces durable files may opt into the generic `artifacts/1`
publisher contract so completed outputs are pushed into the caller's authorized
local workspace before the MCP returns a standard `resource_link`. The same
deployment supports the reverse direction: a Windows agent can invoke a
WSL-hosted MCP and receive its files through the link already established by
WSL.

## Source Ownership

The user-approved layout has exactly two runtime components, `win-bridge-mcp/`
and `wsl-bridge-mcp/`, plus `docs/` and `tests/` support directories. Shared runtime
modules remain root-level Python modules shipped in the same wheel. `bridge_runtime`
owns node/registry/control orchestration; transport adapters and the persistent
connector are separate modules; `bridge_protocol` owns pure protocol-era projections;
`journal_maintenance` and `journal_evidence` own explicit local maintenance/offline
correlation. None of these introduces business-MCP logic or a third runtime component.
Fixtures and verification utilities are development-only source-distribution assets.

## Topology

```text
                    one full-duplex local link
WSL bridge node  =================================  Windows bridge node
  local :8769                                           local :8768
      |                                                     |
      +-- WSL agent -> remote Windows MCP                    +-- Windows agent -> remote WSL MCP
      +-- WSL registry                                      +-- Windows registry
```

The Windows node listens on loopback port `8767`; the WSL node actively
connects. Once established, both nodes can emit `open` frames, so direction is a
property of a logical stream rather than the TCP connection initiator.

## Protocol layers

### Local control

A local stdio proxy connects to its node's loopback control socket and sends one
bounded request:

```json
{"op":"connect","target":"registered-id","artifactInbox":"/authorized/workspace","connectorVersion":"1.0.0"}
```

`artifactInbox` is optional and is accepted only beneath an Operator-configured
local `--allow-artifact-root`. `connectorVersion` is the exact version string of
the connector engine policy the proxy loaded (see "Persistent stdio connector"
below). After an `ok` response, the socket carries raw MCP stdio bytes. The node
`ok` reply includes `coreVersion` (the node's own core version, currently its
`SERVER_VERSION`) so the connector can detect a node core that differs from
its
engine requires. Registry queries use the separate `registry` local operation.
The Operator-only `lifecycle` local operation (component CLI `lifecycle`)
inspects and controls bridge-owned backends on this node; mutations require an
explicit confirmation field and resolve ids only against this node's own
registry. A file-capable business MCP uses the token-authenticated local
`publish` operation; the Agent never calls that operation. An Agent-side adapter
that wants to feed a local file to a remote MCP uses the local `stage_input`
operation (or the equivalent `stage-input` component CLI together with
`connect --print-stream-id`), naming the stream id it received from `connect`
and a source path that must resolve to a regular file beneath the node's
Operator-authorized workspace roots.

### Peer link

The nodes negotiate `win-wsl-mcp-bridge/0.2` and exchange newline-framed bridge
messages:

- `open` / `open_ok` / `open_error`
- ordered `data` / `data_ok`
- `eof` / `close`
- `registry_request` / `registry_response`
- `artifact_begin` / `artifact_ready`
- ordered `artifact_chunk` / `artifact_chunk_ok`
- `artifact_end` / `artifact_ok` / `artifact_error` / `artifact_cancel`
- `input_begin` / `input_ready`
- ordered `input_chunk` / `input_chunk_ok`
- `input_end` / `input_ok` / `input_error` / `input_cancel`

Peers negotiate the optional `artifacts/1` and `artifact-inputs/1` extensions in
`hello`/`hello_ok` (`artifactInputs` echoes back only when both sides offer it).
Downgrade leaves ordinary MCP streams operational and withholds publisher
credentials from the business MCP. `data` and `artifact_chunk` payloads are
base64 encoded so arbitrary stdio bytes remain valid inside bounded JSON frames.
Each logical stream permits one unacknowledged `data` frame per direction; the
receiver sends `data_ok` only after draining the bytes into its local socket or
business process. This gives slow streams independent backpressure without
blocking the peer read loop or unrelated streams. After local client EOF, the
remote MCP receives EOF and has a bounded five-second grace period before the
stream is terminated. The bridge protocol revision is independent of downstream
MCP protocol negotiation.

### Business MCP modes

For an incoming `open`, the receiving node resolves only the supplied registry
id. The remote caller cannot provide command, args, cwd, or env.

Registrations whose `multiProcessAllowed` is `true` or `null` retain the dedicated
byte-transparent behavior: each logical stream starts its own locally configured
command, and the bridge does not parse or rewrite its MCP messages.

An explicit `multiProcessAllowed=false` selects the bridge-enforced shared mode.
All clients for that registry id on that node attach to one backend generation
and await the same atomic spawn future. The bridge parses newline-delimited
JSON-RPC only in this mode, virtualizes one physical initialize exchange using a
deterministic Bridge-owned protocol/capability profile, rewrites request IDs and
progress tokens, routes responses/errors/cancellation/server requests only to an
owning client that advertised the required capability, and serializes client
requests until each response completes. Shared clients negotiate their MCP
revision generically from the requested `initialize.protocolVersion` (never from
client identity), following the MCP versioning rule that a server may answer an
unsupported requested revision with one it supports:

- requested revisions verified for the logical shared surface (`2024-11-05`,
  `2025-03-26`, `2025-06-18`, `2025-11-25`) are accepted. The physical generation
  requests canonical legacy `2025-11-25`, then validates and records the backend's
  observed choice from those four revisions. Missing, unknown or modern-only
  initialize results fail closed. Logical clients receive their own revision
  and a copy projected to its frozen tools profile. Tool fields, content kinds
  and nested metadata are version-gated; an unrepresentable complete result is
  explicitly rejected, never silently truncated or replayed. Shared capability
  advertisements are restricted to the verified tools intersection; Tasks and
  other unverified families are withheld. The physical client capabilities remain
  empty and are never expanded from a logical client's claims;
- well-formed newer revisions the runtime has not verified (a future revision
  after `2025-11-25`) are *downgraded*: the session operates under the newest
  verified revision that does not exceed the request, so the bridge never
  echoes a revision whose feature changes it has not verified, and the client
  continues safely under that older revision;
- unrecognized, malformed, or older-than-supported revisions fail with a
  structured diagnostic and never start or poison a backend generation.

Every logical outcome records metadata-only `shared-initialize` evidence.
`shared-backend-session` separately records the requested and observed physical
revision, or a rejected observation without inventing a negotiated version.
The compatibility floor used before observation is not proof of backend support.
Legacy list-changed notifications stay with legacy logical clients; the modern
surface does not advertise legacy subscriptions. Request-scoped progress stays
with its owning request. Invalid or ambiguous routing fails closed instead of
spawning another backend.

Shared mode preserves downstream results and schemas but is intentionally not
byte-transparent because collision-free multiplexing requires JSON-RPC rewriting.

### Persistent stdio connector and reloadable engine

The Agent-facing `connect <registered-id>` process is a *persistent connector*
(frozen standard-library core in `connector_core.py` plus a dynamically
reloadable stdlib policy module, `connector_engine.py`). It intentionally does
not add any Agent-visible Adapter or mega-tool: the per-target tool surface is
unchanged and connected business payloads remain byte-transparent.

- **Aliveness.** The connector exits only when the Agent closes stdin (or the
  process is killed). When the local node, its control socket, the peer link, or
  the downstream backend closes while stdin is open, the connector reconnects to
  its local node with backoff instead of exiting.
- **Replay boundary.** On reconnect it replays only the cached MCP `initialize`
  request plus `notifications/initialized` handshake bytes to the fresh
  downstream session (the replayed initialize answer is absorbed, never
  forwarded a second time). Business calls are never replayed: a call pending at
  the moment of loss, or one whose send may have reached the node, fails exactly
  once with a concise Bridge-generated JSON-RPC error (`code -32000`, message
  stating the stream was lost and the request was not replayed), and requests
  queued while disconnected are delivered only after the replayed handshake
  completes. Requests arriving while reconnecting that cannot be queued within
  the bounded input queue fail with a Bridge error as well.
- **Version strings.** The `connect` handshake carries `connectorVersion` (the
  loaded engine's exact version) and the node reply carries `coreVersion` (the
  node core, currently `SERVER_VERSION`). Versions pair by simple direct
  equality (no ordering): when the node `coreVersion` differs from the engine's
  `CORE_VERSION`, the connector treats the core as mismatched and appends one
  short warning to the forwarded `initialize` result `instructions` and to
  Bridge-generated JSON-RPC errors; ordinary business results stay untouched
  unless the engine policy explicitly opts into a single-line result note
  (`NOTE_RESULTS_WHEN_STALE`, off by default) because correctness cannot be
  assured against the mismatched core.
- **Engine reload.** The JSON-RPC-aware recovery (handshake caching, replay
  absorb, fail-once, warnings) lives in the dynamic engine module; the frozen
  core (`connector_core.load_engine`) loads it. Selection order: (1) an
  explicit engine file (argument or `WIN_WSL_MCP_BRIDGE_CONNECTOR_ENGINE`) that
  fails closed; (2) an engine directory (argument or
  `WIN_WSL_MCP_BRIDGE_CONNECTOR_ENGINE_DIR`) holding immutable engine bundles —
  each `<name>.engine.json` manifest names its module file and declares the
  exact `version` and paired `coreVersion` plus the module's SHA-256, which is
  verified before the module is imported under a unique immutable module name;
  the newest verified version is selected, a candidate that fails digest,
  containment, import, or manifest self-consistency checks is skipped, and when
  no candidate loads the connector rolls back to the built-in
  `connector_engine.py`; (3) the built-in engine. Operators update or roll back
  engines by staging/removing bundles in the scanned directory; the next
  `connect` picks the newest verified bundle.
- **Scope.** Replay is stream-scoped. Dedicated registrations start a fresh
  downstream process per reconnect, which the replayed handshake initializes.
  Shared registrations still enforce their own node-side attach, lease, and
  fixed-view policies on the new stream; the connector never bypasses them. An
  initial `connect` refusal (for example the node is not yet listening) remains
  a fatal startup error with the historical exit contract; only an established
  session loss triggers reconnection.

## True bidirectionality

True bidirectional invocation is feasible without Windows opening a new socket
to WSL:

1. WSL opens the physical TCP connection to Windows.
2. The connection remains full duplex.
3. A Windows local proxy asks the Windows node to open a WSL registry id.
4. The Windows node sends an `open` frame upstream on the existing connection.
5. The WSL node starts its locally registered stdio MCP.
6. MCP bytes flow in both directions on that logical stream.

This avoids dependency on Windows-to-WSL address discovery, NAT port forwarding,
or mirrored-networking support for a second connection.

## Registry storage and capability summaries

Each side owns a separate local SQLite database for MCPs installed on that side:

```text
Windows: %LOCALAPPDATA%\WinWslMcpBridge\registry.sqlite3
WSL:    $XDG_STATE_HOME/win-wsl-mcp-bridge/registry.sqlite3
        or ~/.local/state/win-wsl-mcp-bridge/registry.sqlite3
```

The databases are never a shared `/mnt/c`, UNC, or WSL-hosted SQLite file. SQLite
locking and WAL semantics must stay on the filesystem owned by the process using
the database. Keeping separate stores also prevents private Windows commands and
environment configuration from being copied into WSL, and vice versa. Artifact
spools use a dedicated `spool/<side>-artifacts-v1` directory under the same
per-host state base by default, never the source checkout or a shared filesystem.

The implementation uses the recognized SQLite engine through Python's standard
`sqlite3` library, WAL mode, `PRAGMA user_version` schema versioning, bounded
busy timeouts, and read-only runtime connections. JSON manifests are local
Operator import inputs; they are not the runtime authority.

Local agents normally register local MCPs directly. Therefore the public
Registry MCP exposes **only the peer registry**. The local database is used to
resolve incoming peer `open(target=id)` requests and for local Operator
management; it is not merged into the Agent-visible list. This avoids duplicate
capabilities and prevents an agent from choosing the bridge for a local MCP.

Public Registry MCP responses exclude all storage and launch fields, including:

- command;
- args;
- cwd;
- env;
- database path and implementation-only columns.

Public metadata contains an authored summary, capability groups, and business
process declarations. Example:

```json
{
  "id": "onshape",
  "name": "Onshape MCP",
  "summary": "Onshape browser and modeling tools.",
  "process": {
    "multiProcessAllowed": false,
    "enforcement": "bridge-shared-backend",
    "clientLease": {
      "enabled": true,
      "busyPolicy": "error",
      "releaseOnDisconnect": true
    },
    "sharedState": {"mode": "fixed"}
  }
}
```

`multiProcessAllowed` may be `null` only while registration metadata is not yet
verified; the bridge never converts a missing declaration into `true`. Explicit
`false` is normalized to `bridge-shared-backend` and is enforced by the node that
owns the registration. Public summaries reveal only the enforcement outcome and
whether a lease/fixed view applies; exact tool patterns, release arguments, result
paths, commands, and environment remain private. Registry status reports
`registered` separately from transport connectivity and successful MCP
initialization.

When a live node owns the registration it answers `describe` and `status` with a
compact read-only `lifecycle` field merged into the existing response (see
"Bridge-owned lifecycle status and Operator control"): shared registrations add
`mode`, `state`, `ownedGeneration`, `activeClients`, and `drain`; dedicated
registrations add `mode`, `state`, and `activeStreams`. The merged summary never
contains pids, stream identifiers, launch fields, or any stream content, and the
`list` action and Registry-only (no node) callers are unchanged.

## Typed transport, Control MCP, and evidence foundation

Registry schema v4 keeps the endpoint/launch definition private and exposes only the tagged
transport family plus a redacted management capability. Stdio remains the active data plane.
A Streamable HTTP row is observation-only and rejects stdio `open`; an opt-in serve flag
(`--http-relay-port`, disabled by default) starts a loopback HTTP relay that mounts each
peer-registered `streamable-http` MCP at `/mcp/<registered-id>`. The consumer validates the
request, sends a bounded `relay_request` envelope (id, method, path suffix, query, headers,
base64 body) over the peer link, and streams the response back as status/header + body frames;
the owner re-resolves the target in its local registry, revalidates the private loopback
endpoint, and injects the owner-registry static headers when building the outbound request, so
endpoint URLs, headers, and credentials never cross the peer link or reach the Agent.

For lifecycle mutation of an externally owned HTTP service, the owner may register a private
`management.controlContract` on an `external-controlled` `streamable-http` row. The contract is
a bounded fixed HTTP interface: each allowed lifecycle action (`drain`, `restart`, `stop`)
names one fixed method, one loopback location (a relative `path` resolved against the registered
private endpoint, or an absolute loopback `url`), a bounded `timeoutSeconds`, bounded
`successStatuses`, and an optional bounded loopback GET `readiness` gate. It never names a
command, service, container, pid, or control definition. The contract is validated and
normalized at registry write, persisted only in the private management field, and never appears
in public/peer registry views; the merged `lifecycle` summary for an HTTP row exposes only
`mode: http`, `state: not-probed`, and `registered`. The owning node executes a confirmed
mutation with the same preview/confirm and operation-id journal discipline as stdio lifecycle
actions, injecting the registry's static owner headers. Explicit stdio-to-HTTP conversion is the
separate loopback `stdio_http_facade.py` converted facade.

The optional stdio Control MCP is a disposable facade with two static tools. It reaches the
long-running node over loopback; closing its stdin cannot terminate the node or any business
backend. Peer control frames carry only a bounded action, registry id, generation guard,
confirmation, impact flag, reason, and opaque operation id. The owning node always re-resolves
both target and opt-in policy from its local registry. Lifecycle locks are per target.

Each serving node owns a sibling `events.sqlite3` metadata journal on the host-local state
filesystem. Default log hooks store categories and byte counts rather than raw business stderr
or MCP payloads. Trace sessions are explicit, time/byte bounded, expire automatically, and
snippet/complete capture requires sensitive-data confirmation; this foundation does not yet
record protocol payloads automatically.

## Artifact workspace delivery

MCP supports inline text/image/audio, embedded resources, and resource links, but
a host-local path or `file://` URI is not portable across Windows and WSL.
Client `roots` advertise filesystem scope; they are not a mount or transfer
mechanism. The bridge therefore never scans tool text/JSON for path-looking
strings and never lets an Agent request an arbitrary peer path.

The implemented model follows the managed-file pattern used by OpenAI container
files: publication is explicit, source access is confined, transfer is verified,
and the consumer receives a local handle/path only after commit.

### Responsibilities

- The business MCP decides that an output is exportable, writes one completed
  regular file into the staging directory supplied by the bridge, and calls the
  token-authenticated local publisher before returning its result. Dedicated
  backends receive a per-stream stage; a shared backend receives a
  generation-owned stage and publication is accepted only while one serialized
  client request is active.
- The bridge snapshots the already-open staged file, enforces limits and path
  rules, transfers it, verifies SHA-256 and byte count, and atomically commits it
  beneath the receiving workspace inbox.
- The Agent does not fetch from the MCP host. It receives a standard
  `resource_link` whose URI and `_meta.localPath` refer to the already committed
  file on the Agent's own host.
- The Operator enables `artifactDelivery` per MCP, configures the spool limit,
  and supplies local `--allow-artifact-root` boundaries.

### Publisher contract

When `artifactDelivery.enabled=true` and both peers negotiated `artifacts/1`, the
bridge launches the MCP with:

```text
WIN_WSL_MCP_BRIDGE_ARTIFACT_STAGE
WIN_WSL_MCP_BRIDGE_ARTIFACT_TOKEN
WIN_WSL_MCP_BRIDGE_ARTIFACT_LOCAL_HOST
WIN_WSL_MCP_BRIDGE_ARTIFACT_LOCAL_PORT
WIN_WSL_MCP_BRIDGE_ARTIFACT_PROTOCOL=artifacts/1
WIN_WSL_MCP_BRIDGE_ARTIFACT_PYTHON
WIN_WSL_MCP_BRIDGE_ARTIFACT_PUBLISHER
```

The MCP writes a single-component filename under `ARTIFACT_STAGE`, invokes the
component's `publish <relative-name>` command or equivalent local operation, and
waits. Success returns a ready-to-embed standard MCP `resource_link`; failure is
reported to the business MCP and no remote path is exposed.

### Confinement and integrity

- Source names reject absolute paths, separators, `..`, encoded traversal,
  Windows drive/ADS syntax, reserved device names, NUL, and trailing dot/space.
- Source objects must be regular files with no symlink and no detectable extra
  hard link; directories must be archived by the business MCP first.
- The bridge copies from one opened source handle into an unguessable private
  temporary snapshot while hashing and enforcing both MCP and node size limits.
- Only artifact id, display name, media type, size, digest, and chunks cross the
  peer link. The source staging path never crosses. Each chunk is acknowledged
  after bounded-queue disk write, so a slow receiver applies flow control without
  blocking unrelated logical streams; node/stream concurrency and aggregate
  announced bytes are capped.
- The receiver revalidates its configured inbox, writes a unique owner-only
  `.partial`, verifies exact byte count and SHA-256, fsyncs, and atomically
  renames inside `.mcp-artifacts/<artifact-id>/` without overwrite.
- Partial files and source staging are deleted on ordinary failure or stream close.
  Source staging is deleted on restart, and startup removes stale workspace
  `.partial` files only beneath configured artifact roots. Successfully committed
  workspace files are never auto-deleted.
- Version 1 has no resume. Link loss fails the transfer closed.

The supported deployment profile is local-only: the local OS user, Agents, and
registered business MCPs are trusted, and every bridge client and listener is
restricted to loopback. The per-process publisher token prevents accidental
cross-stream publication; it is not an OS-user authentication boundary. An
owner-authenticated Unix socket, owner-ACL Windows named pipe, directory-handle
anchoring, and reparse-point-safe native APIs are optional hardening only for a
different deployment profile that admits mutually untrusted local principals.
They are not required for the supported trusted-local profile.

## Agent-local file input staging (artifact-inputs/1)

`artifact-inputs/1` is the mirror of `artifacts/1`: it lets an Agent-side adapter
feed one Operator-authorized local file to a remote business MCP as an ordinary
local path on the MCP host. It is a separate negotiated extension and never
reuses output publication authority.

### Responsibilities

- Only the Agent-side adapter uploads. `stage_input` requires the open local
  stream id and a source that resolves, without symlinks, to a regular file
  beneath that node's Operator-configured `--allow-artifact-root` workspace
  roots. Absolute paths outside the roots, symlinks, hardlinks, devices,
  directories, and oversized files fail closed locally before any byte leaves
  the host.
- The Operator enables `inputDelivery` per business MCP and bounds the aggregate
  staged size. `inputDelivery.enabled` requires a dedicated business process
  (`multiProcessAllowed` is not `false`); shared-backend input staging is not
  implemented and the manifest is rejected at `registry-init`.
- The transfer mirrors the artifact protocol in the reverse direction: the
  sender snapshots one opened regular-file handle, and only input id, display
  name, media type, size, digest, and acknowledged chunks cross the peer link.
- The receiving node commits each input into the stream's private input stage
  (`.../<generation>/<stream-id>-inputs`, owner-only) and hands the business MCP
  only environment variables, never handles or sender paths.

### Business MCP contract

When `inputDelivery.enabled=true` and both peers negotiated `artifact-inputs/1`,
the bridge launches the dedicated MCP with:

```text
WIN_WSL_MCP_BRIDGE_INPUT_STAGE
WIN_WSL_MCP_BRIDGE_INPUT_PROTOCOL=artifact-inputs/1
WIN_WSL_MCP_BRIDGE_INPUT_ENABLED
```

The business MCP reads committed input files only beneath
`WIN_WSL_MCP_BRIDGE_INPUT_STAGE`; a tool receives a descriptor string
(`bridge-input://input-...`) from the Agent in its ordinary arguments, and the
bridge rewrites exactly that JSON string literal to the staged absolute path on
the MCP host before the bytes reach the business process.

### Confinement and integrity

- The rewrite engages only on a dedicated receiver stream whose registration
  enabled `inputDelivery` and whose peer negotiated the extension. Only exact
  bridge-minted descriptor literals inside a `tools/call` are replaced in raw
  bytes; every other message and every other byte of the message are forwarded
  unchanged, so ordinary tool arguments stay byte-for-byte identical and
  path-looking text is never inferred.
- An unknown, foreign, or expired handle, an ambiguous duplicate occurrence, or
  a descriptor outside `tools/call` fails closed with a JSON-RPC error instead
  of reaching the MCP.
- Receivers enforce per-stream concurrency, aggregate reserved-byte limits, the
  configured `maxBytes`, name safety (same rules as artifact names, no
  overwrite), digest and exact byte-count verification, owner-only partials and
  final files, and cleanup on abort, failure, or stream close. The stage is a
  per-stream private directory created only when the extension is active.
- Inline embedded MCP content and `resources/read` remain preferred for small
  inputs; this extension exists for files too large or too binary for inline
  delivery.

## Agent-environment projection (P0-B)

Each host keeps a second host-local SQLite authority, `projection.sqlite3`
(sibling of its registry), that enrolls that host's Codex / Claude Code /
DeepSeek Harness (DSH) agent environments and mirrors the **peer** registry's
projection-relevant facts (`id`, display `name`, `enabled`). Reconciliation is
deterministic, event-driven, and convergent on current desired state: a
projection-affecting registry commit (add/remove/rename/enable/disable of a
registration) appends an outbox row in the *same* SQLite transaction as the
registry write (via `ATTACH DATABASE`), while runtime-only changes
(command/args/cwd/env/credentials/lease/backend policy) never churn agent
configuration.

For each enrolled environment the engine keeps exactly one MCP entry per
Agent-reachable **peer** registration whose fixed local command is the local
component's `connect <server-id>` (the peer resolves the id in *its* own local
registry). Directly registered local MCPs are never duplicated through the peer
bridge. Entry construction lives only inside per-client adapters; registry
contents, scanned candidates, and remote callers can never supply a config
command, cwd, environment, or output path. Adapters prefer the client's official
mutation CLI and otherwise rewrite only Bridge-owned documents
(a `# WIN-WSL-MCP-BRIDGE-MANAGED` Codex file, managed `mcpServers` keys of a
Claude JSON document, or a Bridge-owned `cordis-bridge-overlay.json` per DSH
profile) with locked same-directory temporary write, fsync, atomic replace,
readback, and rollback — never ad-hoc TOML/YAML patching. Entries carry
`WIN_WSL_MCP_BRIDGE_OWNED`/`WIN_WSL_MCP_BRIDGE_SERVER` markers, removals require
an exact persisted command/args/env fingerprint (divergence is reported as
drift, never overwritten), and applied state is `configured` (DSH:
`next_session`) — the bridge never claims a runtime-loaded Agent session.

The reconciliation preflight validates every enabled peer target against the
selected environment's implemented transport route before reading or applying
client configuration. Any `unsupported_transport` fails that environment as a
unit: no entry is added, replaced, or removed, and stored entry fingerprints are
retained. Dry-run reports the same failure without writing reconciliation state;
a real attempt records error history and marks existing projections as errors.
This applies equally to DSH (never `next_session` on an unsupported transport).
Other enrolled environments continue independently, and reconciliation converges
normally once every target has a supported route.

The operator flow is `projection scan` (read-only bounded scanner with opaque
candidate ids; never credentials or launch definitions) -> explicit `enroll
<id> --confirm` with candidate revalidation -> `sync` (refresh the peer mirror
from a registry path or over the live remote registry query) -> `reconcile`
(optionally `--watch-seconds` polling). `registry-init --projection <path>`
creates the projection authority and writes outbox events atomically.

## Security boundary

- Both local control listeners bind only to loopback.
- The peer listener binds only to loopback.
- Only an allowlisted registry id crosses the peer link before process start.
- Commands and environment values are never accepted from the peer.
- Private registry launch fields are never returned by the Registry MCP.
- Business stderr goes to bridge diagnostics, never MCP stdout.
- No business credentials or persistent state are copied into the peer registry.
- Staged Agent inputs are private per-stream directories whose path never leaves
  the MCP host; the sender sees only opaque handles, names, sizes, and digests.

This contract trusts the local OS user, Agents, registered MCPs, and local
WIN-WSL communication. It does not claim isolation between mutually untrusted
local principals and provides no remote-network authentication. Every client and
listener therefore fails closed unless its configured host resolves only to
loopback, and the bridge must never be exposed outside the local host.

## Shared-backend lifecycle and leases

Shared backends are owned by one `BridgeNode` and keyed by that node's local
registry id. There is no module-global pool, so equal ids in the other host's
registry or another node cannot share a process. Each slot executes only these
generation transitions:

```text
exited -> starting -> running -> stopping -> exited
```

Concurrent attaches in `starting` await the same shielded future. An attach in
`stopping` waits for the old process, its owned process tree, protocol tasks,
artifact stage, and publisher token to be reaped before a new generation may
enter `starting`. Spawn failure, backend crash, last-client EOF, peer loss, and
node shutdown all pass through `stopping -> exited`; stale generation callbacks
cannot publish a replacement state.

Every shared backend has one stdin write lock and one request dispatcher. The
dispatcher does not send the next client request until the current response has
arrived, which prevents concurrent calls into synchronous MCP/Playwright
implementations. Cancellation and responses to backend-initiated requests bypass
the request queue under the same write lock so nested exchanges do not deadlock.

A registration may define a generic `process.clientLease` with private tool
patterns, one release tool/argument match, and a result path that must confirm
release. The first matching call atomically owns the lease. Another client gets a
structured retryable `client_lease_busy` tool error and no second process is
started. A confirmed release clears ownership; owner disconnect triggers the
configured release call. If cancellation or cleanup does not settle within the
bound, the bridge stops that registration's generation rather than risking two
profile owners. The bridge never deletes profile locks and never kills processes
by image name. Graceful cleanup is attempted first; POSIX uses the generation's
process group and Windows assigns the backend to a kill-on-close Job Object, with
an exact-PID tree fallback only if job assignment is unavailable during failed
startup.

Connection-scoped dynamic tool views cannot be silently shared. A shared
registration that cannot virtualize views declares `sharedState.mode=fixed` and
private mutation tool names. Those calls receive `shared_view_fixed`; list-changed
notifications remain consistent for all attached clients.

Registrations whose `multiProcessAllowed` is `true` or `null` keep the dedicated
lifecycle: the bridge performs only the process start needed for that logical
stream and does not claim business-level singleton safety.

### Bridge-owned lifecycle status and Operator control

Backends owned by a node are observed and controlled only through that node's
loopback local control operation `lifecycle`, exposed to Operators as the
component CLI `lifecycle status|drain|refresh|restart|stop`. No additional Agent-visible tool is
added: shared clients keep the identical virtualized initialize profile and
tool catalog, dedicated streams stay byte-transparent, and a remote MCP reached
through the bridge remains equivalent to one registered directly with the local
Agent.

- `status` aggregates every enabled registration owned by the node. Shared
  registrations report the generation state machine, the exact owned
  generation, active-client count, drain flag, and pid. Dedicated registrations
  aggregate only their active-stream count on the owning node; status never
  replays or mirrors stream content and never includes per-stream identifiers
  or bytes.
- `drain` arms refusal of new streams for one shared registration; attached
  clients finish normally and the owned generation stops when the last client
  disconnects. The drain persists in memory across generations (surviving even
  a fully exited backend) until `restart` or `stop` clears it. A refused attach
  is a redacted `open_error`; it never starts a generation.
- `stop` clears any armed drain and ends the live owned generation now. Shared
  registrations are demand-started and never warmed, so a later connect starts
  a fresh generation.
- `restart` clears any armed drain and replaces only the live owned shared
  backend generation. Attached logical client streams stay open; after the old
  process tree is fully reaped, the bridge starts the strictly newer generation,
  replays its Bridge-owned canonical initialize exchange, sends the downstream
  initialized notification, and resumes request forwarding. Restart is refused
  while a request is in flight, never replays a tool call, releases any active
  client lease before replacement, and reports `reconnectRequired=false` on this
  transparent path. If no generation is live, restart only clears drain and the
  next connect remains demand-started. After successful replacement it broadcasts
  `notifications/tools/list_changed` to every initialized logical client, allowing
  DSH and other capable clients to fetch the new generation's catalog immediately.
  `stop` remains the disconnecting action.
- `refresh` does not touch the backend generation. After preview/confirmation it
  broadcasts `notifications/tools/list_changed` to attached initialized clients
  and reports the delivered recipient count; with no attached clients it is a
  zero-recipient no-op and never demand-starts the backend.
- An unexpected shared-backend exit uses the same virtual-session boundary:
  every pending client call receives one structured `backend_generation_lost`
  error with `outcomeUnknown=true` and is never replayed; attached initialized
  streams are held while a bounded replacement generation is started and
  initialized. On success the bridge emits `notifications/tools/list_changed`
  and resumes the same streams. This notification costs no additional tool
  schema tokens in clients that ignore it, lets DSH and supported Claude Code
  versions refresh, and is not required for correctness because the virtualized
  client session remains valid. Exhausted or failed recovery closes the streams.
- Control is exact-ownership: an optional expected `generation` refuses the
  operation when the backend's owned generation already changed (for example a
  new client reconnected after the Operator's preview).
- Mutations are preview-then-confirm at both layers: the CLI prints a read-only
  preview without `--confirm`, and the node itself only mutates when the
  control request carries explicit confirmation.
- Dedicated registrations refuse `drain`/`restart`/`stop`: their processes are
  stream-scoped and closing the Agent stream already stops them. The bridge
  never synthesizes or transparently replays a dedicated session.
- Externally owned Streamable HTTP registrations refuse lifecycle mutation unless
  the row is `external-controlled` with a registered bounded control contract for
  that action. A confirmed contract mutation is one bounded fixed-method loopback
  HTTP request resolved owner-side, with optional readiness, per-target locking
  and operation-id journaling. The Bridge owns no process for these rows.
  Separately, `bridge-managed` HTTP registrations have an owner-local launch and
  supervision policy: readiness-gated relay demand and exact-generation
  drain/stop/restart prove exit before a new generation starts.

If the physical link drops, current logical streams fail closed and all shared
leases are cleaned before their backends exit. New streams can be opened after
the WSL connector re-establishes the link. Existing streams are never resumed;
transparent link parking is feasible only as a separately negotiated extension,
because replaying even the single unacknowledged frame could duplicate a
side-effecting request.

For clients without dynamic catalog refresh, an optional per-target compatibility
facade preserves the topology invariant: every registration still identifies one
business MCP. It exposes two constant tools (`bridge_capabilities`, `bridge_call`),
caches the downstream catalog, supports bounded summary search/exact-schema
lookup and explicit refresh, and never aggregates multiple targets. Native
clients continue to receive the original tools directly, avoiding the extra
capability lookup and preserving their normal token surface.

## Bounded Cross-Node Call Evidence

A passive observer classifies bounded newline-delimited JSON-RPC envelopes on
link-crossing stdio streams without changing their bytes or persisting payloads.
Opaque aliases combine shared stream identity, canonical sender side, typed id
and occurrence. Replies consume outstanding observations; ambiguous id reuse or
framing loss degrades that direction instead of guessing a pair.

Admission is bounded to 64 active stream states. A refused stream is not admitted
later in its lifetime; an admitted live state is never evicted and restarted at
occurrence one. Closing a stream releases its state. Oversized lines (256 KiB),
queue drops and skipped/degraded observations are reported as bounded diagnostic
counters. Default evidence is best effort, not proof that an absent call did not
execute. `journal_evidence.py` joins only explicitly exported metadata; digests do
not authenticate the source and cross-host clocks do not establish causal order.
This classifier does not claim general native-HTTP payload correlation.

## Modern Tools Boundary

The shared stdio backend accepts modern `2026-07-28` requests independently from
legacy initialize sessions. `bridge_protocol.py` owns metadata validation and
result/error projection; the shared runtime owns serialization and exactly one
physical legacy backend generation (canonical `2025-11-25` request, validated
observed backend choice). A supported `server/discover`
bootstraps that backend once, preserves its instructions, and advertises tools
only when the observed backend does. Unsupported/malformed probes do not initialize
or execute the backend. Modern business requests require per-request version and
client capabilities; they cannot bypass the gate by omitting metadata. An explicit
legacy initialize remains a valid fallback on the same logical stream.

Only tools/list and tools/call are projected in this modern slice. Results carry
resultType=complete, private zero-TTL list caching, and server metadata. Optional
Tasks, MRTR, resources/prompts, subscriptions, logging, roots, sampling and elicitation
are not advertised as modern capabilities. Cancellation and progress retain routed
request ownership; business calls are never replayed. Dedicated relays remain
byte-transparent and do not apply this protocol conversion.

## Runtime prompt boundary

The proxy forwards downstream `initialize.instructions` unchanged. Clients that
consume instructions natively receive the original business policy. DSH
versions that list tools without projecting instructions still need a shared
bridge adapter; dynamic namespaced projection is planned but is not implemented
by this stdio prototype.

The Registry MCP has its own bounded read-only User/Operator instructions. Its
capability summary never substitutes for downstream business instructions.

## Future capability warehouse mode

The current bridge is peer-only discovery and requires clients to register the
logical MCP endpoints they intend to use. A future opt-in mode may cooperate
with an external AI capability repository and let an Agent configure only this
Bridge:

```text
Agent -> Bridge Registry -> external capability repository
      -> bounded search -> exact capability/schema -> selected MCP connection
```

This is a roadmap extension, not current behavior. It must not reintroduce local
capability duplication. Before enabling it, the adapter needs:

- a stable capability identity such as publisher/name/version plus manifest
  digest, never display-name matching;
- an inventory of MCP identities already registered directly with the local
  client so warehouse results can be deduplicated;
- explicit trust/signature and installation policy for external manifests;
- bounded capability search and on-demand schema loading rather than exposing a
  warehouse's complete tool surface;
- client-specific dynamic tool registration, replacement, and disposal;
- clear separation between discovering metadata and authorizing installation or
  execution.

The two local SQLite registries remain host deployment authorities. An external
repository supplies public capability metadata and signed acquisition inputs; it
does not receive private command, cwd, env, credentials, or local runtime state.

## Reuse and migration

The implementation reuses the existing bridge pattern:

- stdlib socket/select-style byte forwarding;
- WSL-initiated loopback connection;
- Windows listener and local diagnostics;
- fast local stdio proxy processes;
- reconnect of the physical WSL connector.

It generalizes the old fixed-port, in-process business dispatch into registry id
selection and standard stdio process forwarding.

Real Onshape/Taobao registration and deployment remain separate Operator changes.
This repository supplies only generic process policy, JSON-RPC routing, leases,
and cleanup; it contains no business tool names, profile paths, credentials, or
cloud behavior.

## Shared transport and result components (P2/P3)

Two standard-library root modules extend the transport and result profile beyond
what the node runtime implements. Both are standalone and intentionally not yet
imported by the runtime; registry launch of an HTTP MCP and profile-aware result
publishing are explicit follow-ups in `DEVELOPMENT_PLAN.md` P2/P3.

### Streamable HTTP stdio adapter

`streamable_http_stdio.py` implements the MCP 2025-06-18 Streamable HTTP
transport boundary on one side of a stdio JSON-RPC session:

- the local client speaks newline-delimited stdio JSON-RPC; the adapter is one
  logical downstream client of the remote HTTP endpoint;
- every local message becomes one HTTP POST with `Accept: application/json,
  text/event-stream`; the remote `Mcp-Session-Id` and the negotiated
  `MCP-Protocol-Version` (from `initialize` result) are attached to every
  subsequent request, including the GET SSE stream;
- request responses are consumed as either a JSON object or an SSE stream; any
  notifications/requests a server emits before the matching response are relayed
  to the local client, and JSON-RPC responses are never forwarded from a POST
  stream;
- the GET stream (established after the session exists) carries
  server-initiated notifications and requests; responses the local client sends
  back are POSTed as normal messages;
- `notifications/cancelled` from the local client reaches the in-flight POST,
  and per-request concurrency is bounded (`--max-inflight`);
- HTTP 404 means session loss: the cached session is invalidated and a fresh
  sessionless `initialize` is made once before the request is retried; other HTTP
  errors map to structured JSON-RPC errors (categories `authentication`,
  `rate-limited`, `server`, `remote-error`, `bad-response`, `timeout`) without
  silent retries;
- the local stdio side remains protocol-clean: only JSON-RPC frames are written
  to stdout; diagnostics go to stderr with static header values (including
  `Authorization`) and response bodies never logged.

### Deterministic archive profile

`archive_profile.py` gives a business MCP a deterministic, safe way to turn a
directory-shaped result into exactly one regular file, and to give an Agent an
auditable way to unpack such a file:

- `build_archive` walks a source directory and produces the `archive-v1` profile:
  fixed-time (`1980-01-01`), `ZIP_STORED`, `create_system=3`, mode `0o100644`,
  UTF-8 member names, one name-sorted member order, and a SHA-256 manifest member
  (`manifest.sha256.json`) carrying profile, member count, and per-member size
  and digest — identical trees yield byte-identical archives;
- `extract_archive` is strict and atomic: it requires an absent destination,
  validates member names (no traversal/absolute/drive names, duplicates,
  case-folded collisions, or ancestor conflicts), rejects every non-regular
  member type up front, enforces entry count and expanded/total byte limits,
  verifies the manifest before and after writing, fsyncs files, and commits by
  `os.rename` from a sibling partial directory; any failure removes the partial
  state.

Delivery remains the existing single-regular-file model: the MCP publishes the
archive file through `artifacts/1`, and the Agent (or a caller) strictly
extracts it into a destination it controls.

## Known limitations

- The standard suite still simulates both roles in one Linux namespace;
  `tests/field_test.py` separately verifies real Windows/WSL loopback, processes, and
  artifact delivery with ephemeral fixtures on the deployment host. Broader OS
  versions and managed-service recovery remain unverified.
- One active peer link is accepted at a time.
- SQLite registrations are imported through the local Operator CLI; there is no
  public or hot mutation MCP API.
- Base64 JSON framing favors simplicity over peak throughput.
- Current status is registration metadata, not an active initialize probe.
- The Streamable HTTP stdio adapter (`streamable_http_stdio.py`) is verified by
  an offline fixture-based conformance suite only: it has not run against the
  official SDK conformance server, implements no OAuth/DCR flows (static
  credentials only), and does not resume SSE via `Last-Event-ID`. The explicit
  `connect-http` route launches it on the registered endpoint's owning host.
- Directory-shaped results use the deterministic `archive_profile.py` profile;
  publishing a multi-member archive still transfers one regular file, and
  native directory transfer is not implemented.
- The persistent `connect` connector reconnects and replays only the
  initialize/initialized handshake after an established session loss. A client
  that prunes the MCP during the initial window before the local node is
  reachable still loses it (the initial open refusal keeps the historical fatal
  exit contract), and a downstream backend that rejects the replayed initialize
  cannot be restored; the connector keeps bounded reconnect attempts while the
  Agent's stdin stays open.
- No managed Windows/WSL service installation or recovery is included yet.
