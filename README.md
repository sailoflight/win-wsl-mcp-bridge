# WIN-WSL MCP Bridge

A standalone, bidirectional bridge for ordinary stdio MCP servers. Business MCP
repositories do not need a WSL half, a TCP listener, bridge launch scripts, or a
special MCP protocol version.

## Components

This project intentionally has only two runtime component directories:

```text
win-bridge-mcp/   Windows node, local proxy, and local registry
wsl-bridge-mcp/   WSL node, local proxy, and local registry
docs/            Contracts, development plan, deployment and verification
tests/           Offline tests, fixtures and distribution checks
```

Shared stdlib-only runtime modules remain at root, preserving source-launcher and
wheel import identities. Documentation and tests are support directories, not
additional runtime components. Start at [the documentation index](docs/INDEX.md)
for task-specific contracts and the current acceptance ledger.

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

See `docs/DEPLOYMENT.md` for per-host state placement, registry initialization,
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

## Optional Bridge Control MCP and typed registrations

Registry schema v4 records each private registration as `stdio` (the backward-compatible
default) or `streamable-http`, together with a deny-by-default management policy. Peer
Registry results disclose the transport family and redacted management capability but never
the endpoint, headers, command, environment, or credentials. Streamable HTTP registrations are
visible for lifecycle management. External rows remain observation-only unless the owner
manifest registers a bounded control contract; bridge-managed rows use owner-local supervision. An
explicit `http-to-stdio` projection uses `connect-http` and the verified stdlib adapter on the
owner host; ordinary `connect` still refuses to reinterpret HTTP as raw stdio. An opt-in native
Streamable HTTP relay (`serve --http-relay-port <port>`, disabled by default) mounts each
peer-registered `streamable-http` MCP at `/mcp/<registered-id>`; bounded request envelopes cross
the peer link and the owner host connects to its private loopback endpoint, injecting the
owner-registry static headers there, so endpoint URLs, headers, and credentials never cross the
peer or reach the Agent. For an HTTP-only client, the explicit `stdio-to-http` compatibility
route is provided by `stdio_http_facade.py`: its loopback `/mcp` endpoint creates one isolated
`bridge.py connect <registered-id>` subprocess per MCP session and is always labeled converted,
never native. It supports bounded POST/GET/DELETE, JSON/SSE, and idle/session teardown.

Native HTTP projection is supported by the Codex, Claude and DSH configuration adapters.
Explicit enrollment records client HTTP capability and an Operator-selected local relay base
(`projection enroll ... --native-http --relay-url http://127.0.0.1:<relay-port> --confirm`).
It does not start a listener or claim the installed client has loaded the configuration. During
`projection reconcile`, any peer target without an implemented route for an enrolled
environment returns `unsupported_transport` and preserves that environment's entire
existing configuration. This also applies to DSH; no partial additions or removals
are applied. Other enrolled environments reconcile independently. Dry-run reports the
same failure without recording reconciliation changes.

An Operator who wants lifecycle `drain`/`restart`/`stop` on an `external-controlled`
`streamable-http` registration may register a private `management.controlContract`: a bounded,
fixed HTTP interface per allowed action (fixed method; a relative `path` resolved against the
registered private endpoint or an absolute loopback `url`; bounded `timeoutSeconds`; bounded
`successStatuses`; and an optional bounded loopback GET `readiness` gate). The contract never
names a command, service, container, pid, or control definition, and it is never disclosed in
public or peer-visible registry views (only owner-local Operator lifecycle status shows that a
contract exists and which action names it allows). The owning node executes the contract
owner-side with the same preview/confirm and operation-id discipline as stdio lifecycle actions,
and refuses mutation without the contract. Separately, `bridge-managed` HTTP registrations use
an owner-local launch definition and bounded supervision policy: readiness gates relay demand,
and drain/stop/restart operate on one owned generation without overlap. External HTTP rows
without a registered control contract remain observation-only.

An Agent environment may optionally register one independent, constant-size control MCP:

```text
python3 wsl-bridge-mcp/bridge.py control-mcp
python win-bridge-mcp/bridge.py control-mcp
```

It exposes exactly `bridge_control` and `bridge_diagnostics`. Lifecycle mutation is available
only for a target whose owner-host manifest explicitly enables `management.agentControl` and
allows the requested action; a `streamable-http` target additionally requires a registered
bounded external control contract or a bridge-managed supervision policy. Calls preview unless
`confirm=true`. Shared-stdio `restart` preserves attached logical client streams, replaces only
an idle backend generation, and then broadcasts `notifications/tools/list_changed` so capable
clients such as DSH immediately re-read the restarted backend's tools. `refresh` broadcasts the
same notification without starting or restarting the backend; it reports zero recipients when no
initialized clients are attached. `stop` remains disconnecting, and interrupting active clients with it
also needs separately registered impact-override permission. The existing Operator CLI remains
the deployment and forced-recovery path. The node records bounded metadata in the host-local
`<registry-stem>.events.sqlite3`; payload traces are default-off and require an explicit bounded trace command
and confirmation for snippet or complete capture/readback. `trace export --output <file.zip>`
creates a metadata-only diagnostic bundle with deterministic member digests; it never includes
the trace payload table or registry database.

Clients that cannot refresh a changed MCP catalog (notably current Codex CLI) may register each
business target through `compatibility-mcp <target>`, or enroll that Agent environment with
`--compatibility-route constant-two-tool`. This remains one local MCP registration per
target, but its public surface is always exactly `bridge_capabilities` and `bridge_call`.
`bridge_capabilities` returns compact cached name/description pages and returns a full schema only
for an exact tool name; `refresh=true` explicitly re-reads downstream `tools/list`. Native DSH and
Claude Code registrations should continue using `connect <target>` so normal direct tools and their
schemas remain available without an extra discovery call. Compatibility mode is opt-in rather than
a global mega-tool and forwards the downstream initialization instructions unchanged.

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

See `docs/MCP_COVERAGE.md` for the capability/result coverage matrix and
`docs/ARCHITECTURE.md` for the publisher and security contract. Unimplemented Bridge
work, including the planned `2025-11-25` Legacy / `2026-07-28` Modern dual-era
boundary, is tracked in `docs/DEVELOPMENT_PLAN.md`. General SDK-first advice for MCP
server authors is kept separately at `../MCP_DEVELOPMENT_RECOMMENDATIONS.md` and
is not a statement of current Bridge capability.

## Agent-local file inputs (artifact-inputs/1)

A remote business MCP that accepts local files (for example a model file to
analyze) can receive them through the separate negotiated `artifact-inputs/1`
extension without the remote MCP ever touching the sender workspace. Output
publishing authority is not overloaded: `artifactDelivery` still governs files
leaving the MCP host, while the private manifest field below governs files
entering it:

```json
"inputDelivery": {
  "enabled": true,
  "maxBytes": 268435456
}
```

The business MCP must be a dedicated process (`multiProcessAllowed` not `false`);
shared-backend input staging is not implemented and such a manifest is rejected
at `registry-init`. When a peer link negotiated `artifact-inputs/1`, the host
that launches such an MCP gives the process a private per-stream input stage via
`WIN_WSL_MCP_BRIDGE_INPUT_STAGE` and `WIN_WSL_MCP_BRIDGE_INPUT_PROTOCOL`.

An Agent-local file is uploaded only when an adapter on the Agent side
explicitly stages it. The source must be a regular file beneath an
Operator-authorized workspace root of that node (`--allow-artifact-root`), so
absolute paths, symlinks, hardlinks, devices, directories, traversal, and
oversized files fail closed before any byte leaves the host. The adapter issues
one bounded local-control request on the Agent-side node for the open stream:

```json
{"op":"stage_input","stream":"<connect stream id>","sourcePath":"/authorized/path.bin",
 "name":"path.bin","mediaType":"application/octet-stream"}
```

Only an opaque input handle, name, media type, size, and SHA-256 cross the link;
the staged path stays on the MCP host.

**Explicit descriptor contract.** A staged input is referenced only by the
exact bridge-minted string literal below. The bridge rewrites that literal to
the staged path on the MCP host; it never infers or rewrites arbitrary
path-looking text, file URLs, or any other argument value.

```text
descriptor := "bridge-input://input-<32 hex chars>"   # e.g. "bridge-input://input-1f0e..."
```

A tool call is composed with the descriptor as an ordinary argument value:

```json
{"jsonrpc":"2.0","id":9,"method":"tools/call",
 "params":{"name":"read_input","arguments":{"path":"bridge-input://input-1f0e..."}}}
```

On the MCP host the business process receives the exact same message with only
that value replaced by the committed staged path (for example
`/home/user/.../inputs-<stream>/payload.step`); every other byte of every
message is forwarded unchanged. A descriptor that is unknown, expired, foreign
to the stream, ambiguous, or outside a `tools/call` fails closed with a JSON-RPC
error instead of reaching the MCP.

**Explicit CLI (self-contained adapter flow).** The Agent-side adapter can use
the component CLI instead of the raw local operation. First open the remote MCP
as a stdio proxy while asking the node to print the logical stream id:

```text
# side that owns the Agent (here WSL) — run in the background or as a child
python3 wsl-bridge-mcp/bridge.py connect <windows-registry-id> \
  --local-port 8769 --print-stream-id        # prints: bridge stream: <stream id>  (stderr)
```

Then stage one authorized local file into that stream (a separate process may
call this while the proxy stays connected):

```text
python3 wsl-bridge-mcp/bridge.py stage-input /authorized-workspaces/payload.step \
  --stream <stream id> --local-port 8769 \
  --name payload.step --media-type model/step
```

The command prints one JSON receipt, e.g.
`{"handle":"bridge-input://input-1f0e...","name":"payload.step","size":4823,"sha256":"..."}`
and exits 0; a source outside the node's `--allow-artifact-root`, a symlink, a
directory, a missing file, or an unknown/closed stream exits nonzero with a
diagnostic on stderr. The adapter places the receipt's `handle` in the next
`tools/call` sent through the proxy. The raw equivalent is one bounded local
request on the Agent-side node for the open stream:

```json
{"op":"stage_input","stream":"<connect stream id>","sourcePath":"/authorized/path.bin",
 "name":"path.bin","mediaType":"application/octet-stream"}
```

Inline embedded MCP content and `resources/read` remain the preferred path for
small inputs.

See `docs/MCP_COVERAGE.md` for the capability/result coverage matrix and
`docs/ARCHITECTURE.md` for the staging and confinement contract.

## Bridge-owned MCP lifecycle status and control

Bridge-owned MCP processes are started by the node that owns each registration.
Lifecycle observation is aggregated and Operator-only: nothing is added to a
business MCP's Agent-visible catalog, and a remote MCP reached through the
bridge remains equivalent to one registered directly with the local Agent.

Inspect what this node currently owns (read-only, no confirmation needed):

```text
python3 wsl-bridge-mcp/bridge.py lifecycle status                  # all local registrations
python3 wsl-bridge-mcp/bridge.py lifecycle status --id my-shared-mcp
```

Shared registrations report the generation state machine
(`exited|starting|running|draining|stopping`), the exact owned generation,
active-client count, drain flag, and pid. Dedicated registrations aggregate
only their active-stream count - never stream content and never a transparent
replay of a session.

Operator mutations run only on the node that owns the registration and only
after a read-only preview: run the command once to print the plan (nothing
changes), inspect the observed owned generation, then re-run with `--confirm`:

```text
python3 wsl-bridge-mcp/bridge.py lifecycle drain   --id my-shared-mcp                 # preview
python3 wsl-bridge-mcp/bridge.py lifecycle drain   --id my-shared-mcp --confirm       # apply
python3 wsl-bridge-mcp/bridge.py lifecycle restart --id my-shared-mcp --generation 3 --confirm
python3 wsl-bridge-mcp/bridge.py lifecycle stop    --id my-shared-mcp --confirm
```

Run the command on the host whose node owns the registration: the WSL component
controls the WSL node and the Windows component controls the Windows node
(`--local-port` overrides the default). Semantics:

- `drain` arms refusal of new streams for the registration; already-attached
  clients finish normally and the owned generation stops when the last client
  disconnects. The drain persists until `restart` or `stop` clears it.
- `restart` clears any armed drain and replaces the live owned shared generation
  without closing attached logical streams. The old process fully exits before
  the replacement starts and is initialized; requests then resume on the same
  Agent connector. An unexpected shared-backend exit uses the same bounded
  recovery path: in-flight calls receive `backend_generation_lost` and are never
  replayed, while later calls continue after recovery. A compact
  `notifications/tools/list_changed` prompts capable clients to refresh schemas.
- `stop` clears any armed drain and ends the live owned generation now. Shared
  registrations are demand-started and never warmed, so a later connect starts
  a fresh generation.
- An optional `--generation` makes control exact: it refuses when the observed
  owned generation has already changed (for example a new client reconnected
  after your preview).

The node enforces the preview rule again: a control request mutates only when
it carries explicit confirmation. Dedicated registrations refuse lifecycle
control (their processes are stream-scoped and the Agent's own stream close
stops them); `lifecycle status` aggregates them instead.

Peer-visible registry `describe`/`status` answers add a compact read-only
`lifecycle` field (`mode`, `state`, owned generation, active clients, drain, or
active-stream count) only when a live owning node answers; `list` rows and
Registry-only callers are unchanged.

## Shared components: Streamable HTTP adapter and archive profile

Standard-library root modules implement explicit transport conversion and archive
publication. `connect-http` launches the HTTP adapter on the registered endpoint's
owner host; stdio-to-HTTP uses an explicitly provisioned per-target façade URL.
Native HTTP projection and these converted routes retain distinct contracts.
Both HTTP conversion modules default to the legacy protocol described below and
also provide explicit `--protocol-era modern` (`2026-07-28`) bounded POST-only
modes. See [deployment](docs/DEPLOYMENT.md) and [coverage](docs/MCP_COVERAGE.md)
for schema preflight, cancellation, uncertainty and unsupported semantics.

- `streamable_http_stdio.py` adapts one stdio MCP session to a remote MCP
  Streamable HTTP endpoint (protocol 2025-06-18): a local stdio client
  initializes, and the adapter relays JSON-RPC messages as HTTP POSTs with
  session and negotiated-version headers, consumes `application/json` or
  `text/event-stream` responses (relaying pre-response notifications and
  server-initiated events over the GET stream), delivers cancellations
  mid-request, maps HTTP errors to structured JSON-RPC errors, and performs the
  spec-mandated fresh `initialize` after HTTP 404 session loss. Credentials are
  static (`--header`/environment, never logged). Run it in place of an ordinary
  stdio MCP command:

  ```text
  python3 streamable_http_stdio.py --url https://mcp.example/mcp \
    --header 'Authorization: Bearer <token>'
  ```

- `archive_profile.py` builds and safely extracts one deterministic ZIP profile
  (`archive-v1`): fixed-time, uncompressed, name-sorted members plus a SHA-256
  manifest, so identical directories produce byte-identical archives. Extraction
  is strict and atomic: name gates reject traversal/absolute/duplicate/conflict
  members before any write, per-file size and decompression limits apply, and
  the result is committed by rename only after the manifest verifies. A business
  MCP can therefore deliver a directory-shaped result as one regular file
  through the existing `artifacts/1` publication.

- `stdio_http_facade.py` is the explicit Agent-facing stdio-to-Streamable-HTTP
  facade (protocol 2025-06-18, server role) for an agent that consumes MCP over
  HTTP instead of stdio. Every HTTP session is answered by exactly one isolated
  `bridge.py connect <registered id>` stdio subprocess against the *local*
  Bridge node, so the peer never contributes command/argv/env/credentials and
  process isolation per MCP session matches the stdio connectors. It issues
  `Mcp-Session-Id`/`MCP-Protocol-Version` on initialize, answers JSON-RPC
  request POSTs as JSON (or as a streamed SSE body when the client only accepts
  `text/event-stream`), relays notifications with HTTP 202, pushes backend
  notifications/server requests over the session `GET` SSE stream (refusing
  undeliverable backend requests so they never hang), and closes the session's
  backend on `DELETE`. Loopback-only listeners, bounded bodies/sessions/
  in-flight/SSE queues, an idle reaper, and structured JSON-RPC transport errors
  on backend loss. Run it as an explicit compatibility route (`stdio-to-http`)
  for HTTP-capable agents; the projection layer does not yet emit a descriptor
  for it, so this module is driven by its own CLI listener:

  ```text
  python3 stdio_http_facade.py --side wsl --target <registered-id> \
    --node-port 8769 --listen-port 8937
  ```

Both components and the facade ship in the wheel and are covered by their own
offline suites (`test_streamable_http_stdio.py`, `test_archive_profile.py`,
`test_stdio_http_facade.py`); `docs/MCP_COVERAGE.md` tracks their capability
boundaries.

## Future capability warehouse

The current Registry MCP exposes only peer-installed MCP summaries. A future
opt-in mode may integrate with an external AI capability repository so an agent
configures only the Bridge, searches a larger capability warehouse, and loads a
selected MCP/schema on demand. That mode is not implemented. The repository now
ships the *local* slice of that contract (P5): a read-only
`Registry.identities()` capability inventory, a static Operator-supplied
capability index whose loader fails closed on any launch/install authority key,
bounded summary-only discovery, dedup against directly registered local MCPs,
and a read-only `warehouse list|search|dedupe|plan` bridge CLI whose import
plans never invent launch configuration. Wiring in a *remote* repository (fetch
and signature trust) and dynamic tool add/replace/remove for DSH/Codex remain
external-contract blockers tracked in `docs/DEVELOPMENT_PLAN.md`, not current
capabilities. All remaining unsupported MCP/file modes and their acceptance
gates are planned in `docs/DEVELOPMENT_PLAN.md`.

## Current status

Implemented and tested with both the offline Linux-role integration suite and a
real Windows/WSL ephemeral fixture deployment. `tests/field_test.py` starts a Windows
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
- deterministic shared-backend initialization virtualization for heterogeneous supported MCP clients, preserving downstream instructions and server capabilities;
- generic (never client-specific) MCP initialize revision negotiation on shared backends: verified logical revisions (`2024-11-05`, `2025-03-26`, `2025-06-18`, `2025-11-25`) are accepted, future unverified revisions are negotiated down to the newest verified revision, and unusable revisions fail with a structured diagnostic; the `2025-11-25` contract exposes only the wire-compatible tools subset (no fabricated tasks/sampling/URL-elicitation capabilities), every outcome is journaled metadata-only, and the physical generation requests canonical `2025-11-25`, validates the backend's observed legacy choice, and projects each logical client's frozen tools profile; unrepresentable results fail explicitly, and unverified capability families are withheld;
- per-client capability-aware request-ID, cancellation, progress, response, error, notification, and server-request routing;
- globally serialized shared-backend requests and writes for synchronous business MCPs;
- optional registration-driven exclusive client leases and fixed shared tool-view enforcement;
- generation-scoped process-tree and artifact cleanup without profile-lock deletion or process-name kills;
- negotiated `artifacts/1` workspace-push delivery in both directions;
- explicit business-MCP publishing with no Agent-side remote-file fetch;
- per-stream staging, source snapshot, size limits, SHA-256, and atomic inbox commit;
- negotiated `artifact-inputs/1` Agent-local file input staging in both
  directions, with exact descriptor-only tool-argument rewriting and
  fail-closed rejection of foreign, expired, or unauthorized staging;
- independent downstream MCP identity, instructions, capabilities, and tools;
- `streamable_http_stdio.py`: stdlib stdio-to-Streamable-HTTP adapter (sessions,
  version negotiation, JSON/SSE responses, GET server-initiated events,
  cancellation, one retry on session loss, structured HTTP error mapping);
- `archive_profile.py`: deterministic safe ZIP profile (fixed-time STORED
  members, SHA-256 manifest) with strict atomic extraction for directory-shaped
  results;
- versioned sdist/wheel packaging, Windows/WSL console scripts, and read-only `doctor` preflight;
- per-host agent-environment enrollment and configuration projection (`projection scan|enroll|unenroll|sync|reconcile|status`): a read-only bounded scanner over Codex/Claude Code/DSH configuration locations, a host-local `projection.sqlite3` authority, atomic registry-commit outbox events for projection-affecting changes, and deterministic reconciliation of one `connect <server-id>` MCP entry per Agent-reachable **peer** registration through official client CLIs or Bridge-owned files with drift detection and rollback.
- Bridge-owned MCP lifecycle status/control (`lifecycle status|drain|restart|stop`): read-only lifecycle aggregation for locally owned registrations and Operator-only preview-then-`--confirm` mutations with exact owned-generation guards and no generation overlap; dedicated registrations aggregate stream counts only (no transparent replay), and peer Registry `describe`/`status` answers carry a compact redacted `lifecycle` field without adding any Agent-visible tool or changing `list`.
- persistent per-target stdio connector: each `connect <server-id>` process (raw relay while connected) now stays alive while Agent stdin is open, reconnects after node/local stream loss, replays only the cached initialize/initialized handshake, never replays a business call, and fails each pending call exactly once with a concise Bridge JSON-RPC error; the local handshake carries `connectorVersion` and the node reply carries `coreVersion`, and a mismatched node core (simple direct equality) is reported only through `initialize` result instructions and Bridge-generated errors (optional single-line tool-result note only when the engine policy allows it). The JSON-RPC-aware recovery (handshake replay, absorb, fail-once, stale warnings) lives in the dynamically reloadable `connector_engine.py`; `connector_core.py` is a minimal frozen message-agnostic supervisor. Versions pair by simple direct equality: the node `coreVersion` must equal the engine's `CORE_VERSION`. Engine selection is explicit-file-first (`WIN_WSL_MCP_BRIDGE_CONNECTOR_ENGINE`, fails closed) and otherwise scans an engine directory (`WIN_WSL_MCP_BRIDGE_CONNECTOR_ENGINE_DIR`) of immutable `*.engine.json` manifest bundles whose module bytes are SHA-256 verified: the newest verified version is loaded, tampered/broken candidates are skipped, and if none loads the connector rolls back to the built-in engine; no third component directory or Adapter is introduced.

The supported security profile is a trusted local host: the local OS user,
Agents, and registered MCPs are trusted, and all bridge clients/listeners are
restricted to loopback. Cross-user local isolation is outside this profile.

Additional managed-production work, not required for the supported foreground
local deployment:

- Windows service/VBS installation and recovery;
- DSH-specific dynamic projection of downstream `initialize.instructions`;
- node-level logical-stream resumption after physical-link loss (the persistent
  `connect` connector reconnects and re-initializes a fresh downstream stream,
  but in-flight business-call state is never resumed);
- production log rotation and deployment generation management;
- real business-MCP registration, lease metadata, and rollback evidence.

See `docs/ARCHITECTURE.md` for the protocol and trust boundaries, `docs/DEPLOYMENT.md` for
installation and rollback, and `docs/VERIFICATION.md` for exact checks.
