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
{"op":"connect","target":"registered-id","artifactInbox":"/authorized/workspace"}
```

`artifactInbox` is optional and is accepted only beneath an Operator-configured
local `--allow-artifact-root`. After an `ok` response, the socket carries raw MCP
stdio bytes. Registry queries use the separate `registry` local operation. A
file-capable business MCP uses the token-authenticated local `publish` operation;
the Agent never calls that operation.

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

Peers negotiate the optional `artifacts/1` extension in `hello`/`hello_ok`.
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
JSON-RPC only in this mode, virtualizes one physical initialize exchange, rewrites
request IDs and progress tokens, routes responses/errors/cancellation/server
requests to the owning client, and serializes client requests until each response
completes. Global list-changed notifications are broadcast; request-scoped
notifications stay with the active client. Invalid or ambiguous routing fails
closed instead of spawning another backend.

Shared mode preserves downstream results and schemas but is intentionally not
byte-transparent because collision-free multiplexing requires JSON-RPC rewriting.

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

## Security boundary

- Both local control listeners bind only to loopback.
- The peer listener binds only to loopback.
- Only an allowlisted registry id crosses the peer link before process start.
- Commands and environment values are never accepted from the peer.
- Private registry launch fields are never returned by the Registry MCP.
- Business stderr goes to bridge diagnostics, never MCP stdout.
- No business credentials or persistent state are copied into the peer registry.

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

If the physical link drops, current logical streams fail closed and all shared
leases are cleaned before their backends exit. New streams can be opened after
the WSL connector re-establishes the link. Existing streams are never resumed.

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

## Known limitations

- The standard suite still simulates both roles in one Linux namespace;
  `field_test.py` separately verifies real Windows/WSL loopback, processes, and
  artifact delivery with ephemeral fixtures on the deployment host. Broader OS
  versions and managed-service recovery remain unverified.
- One active peer link is accepted at a time.
- SQLite registrations are imported through the local Operator CLI; there is no
  public or hot mutation MCP API.
- Base64 JSON framing favors simplicity over peak throughput.
- Current status is registration metadata, not an active initialize probe.
- No managed Windows/WSL service installation or recovery is included yet.
