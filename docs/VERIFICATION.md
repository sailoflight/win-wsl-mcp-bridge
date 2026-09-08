# Verification

The core runtime checks are offline and use only Python's standard library.
Packaging and real Windows/WSL field checks are listed separately.

## Syntax without cache directories

```bash
python3 - <<'PY'
from pathlib import Path
for path in (list(Path('.').glob('*.py')) + list(Path('tests').rglob('*.py'))
             + [Path('win-bridge-mcp/bridge.py'), Path('wsl-bridge-mcp/bridge.py')]):
    compile(path.read_text(encoding='utf-8'), str(path), 'exec')
print('syntax ok')
PY
```

## Unit and integration tests

```bash
# From the repository root; includes every tests/test_*.py module.
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s tests -t . -v

# One focused suite
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest -v tests.test_bridge
```

The `test_bridge.py` suite verifies:

- typed stdio/Streamable-HTTP registry validation and public redaction, native HTTP relay and registered external HTTP control contracts, the constant two-tool Control MCP catalog/token budget, bounded event retention, and sensitive trace confirmation;
- only the two requested component directories exist;
- SQLite manifests reject invalid ids and runtime queries redact launch fields;
- each host uses a separate local SQLite database and the public Registry MCP
  exposes peer summaries without a local/remote/all aggregation switch;
- listeners and all local bridge clients reject non-loopback addresses;
- registry databases and artifact partials use owner-only POSIX permissions, and
  manifest booleans/reserved environment keys fail closed;
- WSL proxy -> Windows fixture MCP initialize/list/call;
- Windows proxy -> WSL fixture MCP over the same physical link;
- Registry MCP initialize/tool list/remote list, `ping`, standard JSON-RPC error
  codes, bounded input/output, and oversized peer-query rejection without link loss;
- the opt-in per-target compatibility MCP keeps an exactly two-tool constant surface,
  preserves downstream initialization instructions, pages/searches the current catalog,
  returns exact schemas on selection, invokes the bound target, and projects one
  independent registration per peer rather than a global mega-tool;
- explicit `multiProcessAllowed=false` enforcement with one shared spawn future,
  one backend/profile owner, heterogeneous-client initialize virtualization using
  a deterministic Bridge profile, downstream instruction/capability replay,
  paginated tool-catalog agreement, capability-aware server-request routing,
  collision-free per-client ids, serialized requests, cancellation/progress
  routing, and structured lease busy/fixed-view errors;
- generic (never client-specific) shared initialize revision negotiation: verified
  logical revisions (`2024-11-05`, `2025-03-26`, `2025-06-18`, `2025-11-25`) are
  accepted, a `2025-11-25` initialize is accepted at `2025-11-25` with its
  session continuing and without inventing the optional task surface
  (no `tasks` capability, no tool `execution.taskSupport`, physical profile
  still empty `2025-06-18`), a future-shaped revision is negotiated down to
  `2025-11-25`, an unusable revision yields the structured diagnostic without
  poisoning the backend, the physical backend initialize stays normalized at
  `2025-06-18`, and each accepted/downgraded/rejected outcome is journaled
  metadata-only with requested/negotiated/backend versions and no payload;
- lease transfer after explicit release and abnormal client disconnect;
- backend crash and bridge restart cleanup of the fixture's owned child process,
  strict old-exit-before-new-start generation evidence, restart-triggered
  `tools/list_changed`, manual refresh without generation replacement, and an
  independent registry target remaining callable throughout;
- backend exit drains the final JSON-RPC response before closing client streams;
- ordinary stream data is sequence-acknowledged only after downstream drain, slow
  handlers do not block unrelated peer frames, and close fails active data/artifact
  acknowledgements immediately;
- local-client EOF has a bounded grace period, node shutdown terminates active
  business processes, and the per-target `connect` connector stays alive while
  Agent stdin is open: it reconnects after node/local stream loss, replays only
  the cached initialize/initialized handshake, never replays business calls,
  fails each pending call exactly once with a Bridge JSON-RPC error, and keeps
  raw business payloads byte-preserved while connected;
- the connect handshake carries `connectorVersion` and the node reply carries
  `coreVersion`, paired by simple direct equality (no version ordering): a node
  core that differs from the engine's `CORE_VERSION` is surfaced only through
  the initialize result instructions and Bridge-generated JSON-RPC errors, with
  the optional single-line tool-result note gated behind engine policy;
- the connector engine loader (`connector_core.load_engine`) selects an explicit
  engine file (argument or `WIN_WSL_MCP_BRIDGE_CONNECTOR_ENGINE`) and fails
  closed on it, scans an engine directory (`engine_dir` or
  `WIN_WSL_MCP_BRIDGE_CONNECTOR_ENGINE_DIR`) of immutable `*.engine.json`
  manifest bundles whose module bytes are SHA-256 verified before versioned
  import, picks the newest verified version, rolls back to the next candidate
  and finally to the built-in engine when a newer version is tampered, cannot
  load, escapes the engine directory, or disagrees with its manifest, and keeps
  identical engine basenames in different directories import-isolated;
- public summaries never expose command, args, cwd, env, or fixture paths;
- ordinary path-looking JSON never triggers source reads or file creation;
- MCP responses larger than one bridge frame remain transparent;
- negotiated artifact push works Windows-role -> WSL workspace and WSL-role ->
  Windows workspace over the same physical link, including concurrent transfers
  in both directions alongside an ordinary tool call;
- the business MCP publishes before returning and the Agent receives a local
  standard `resource_link` without fetching from the remote MCP;
- source snapshot, multi-chunk transfer, SHA-256, byte count, owner-only partial,
  fsync, and atomic commit preserve the delivered bytes;
- missing inbox, guessed publisher token, reserved publisher env, traversal/device
  names, symlinks, detectable hardlinks, replaced source pathnames, oversized
  files, premature commit receipts, failed `artifact_ready` delivery, and chunks
  after terminal metadata fail closed;
- per-chunk acknowledgement provides bounded artifact flow control; destination
  commit is no-overwrite and cancellation waits for any in-flight commit;
- negotiated `artifact-inputs/1` agent input staging works Windows-role ->
  WSL MCP and WSL-role -> Windows MCP over the same physical link: the staged
  bytes and SHA-256 match the sender's file in both directions;
- the remote MCP can read only the staged input: it receives one committed path
  under its private `WIN_WSL_MCP_BRIDGE_INPUT_STAGE` and never a sender path;
- the bridge rewrites exactly the minted descriptor and leaves every other byte
  of ordinary tool arguments unchanged; path-looking text is never inferred;
- input sources outside the Operator-authorized roots, symlinked sources,
  missing files, staging after session close, duplicate staged names, and
  foreign/expired/ambiguous handles fail closed (unknown-handle tool calls get a
  JSON-RPC error instead of reaching the MCP);
- manifest `inputDelivery` requires a dedicated business process: enabling it on
  a shared backend, oversized/typed `maxBytes`, and reserved
  `WIN_WSL_MCP_BRIDGE_INPUT_*` environment keys are rejected at registry
  import;
- the explicit `stage-input` CLI stages one authorized file and prints a JSON
  receipt containing the minted handle, `connect --print-stream-id` prints the
  logical stream id for stdio-proxy adapters, and an end-to-end proxy test
  drives initialize -> stage-input -> descriptor-bearing `tools/call` through
  the real node pair;
- the per-host projection authority (P0-B) is covered by hermetic role-simulation
  tests: the bounded scanner redacts values and launch definitions while
  enumerating existing MCP names, ownership, and conflicts; candidate ids are
  stable and stale ids require a fresh scan; enrollment revalidates the selected
  candidate, requires confirmation, and rejects duplicates;
- `registry-init --projection` appends one `registry_changed` outbox event in the
  same SQLite transaction as a projection-affecting change, runtime-only
  command/args/env changes append none, and a failed manifest commit leaves both
  the registry and the outbox unchanged;
- deterministic reconciliation (file-mode and official-CLI via fake binaries)
  converges one `connect <server-id>` entry per enabled peer registration,
  preserves unrelated configuration, is idempotent, converges after a simulated
  crash at the apply boundary without duplicates, removes peer-disabled entries
  only after fingerprint verification, detects user drift and never deletes a
  tampered/unmanaged entry, rolls back a failed write to the previous document,
  isolates per-environment errors, keeps secrets out of every status/reconcile
  output, and reports outbox/history/mirror state via `projection status`;
- unsupported-transport reconciliation is fail-closed for Claude, Codex, and DSH:
  a stdio-to-HTTP transition preserves every existing configuration byte and entry
  fingerprint, applies no partial additions/removals, returns an error even for
  DSH, leaves reconciliation state unchanged in dry-run, records errors on a real
  attempt, and converges once the peer returns to a supported transport; explicit
  HTTP-to-stdio enrollment still projects the converted target successfully;
- unenrollment either stops synchronization (entries retained) or removes only
  matching Bridge-owned entries, and the bounded `--watch-seconds` poller runs
  deterministic rounds;
- Bridge-owned MCP lifecycle status/control is covered offline and end to end
  against a live owning-node/agent-node pair through the loopback Operator CLI:
  `lifecycle status` aggregates idle shared registrations (state/owned
  generation/drain) and dedicated ones (active-stream counts) without leaking
  launch fields or stream content; previews of `drain|restart|stop` stay
  read-only until `--confirm`; confirmed `stop` terminates exactly the owned
  generation and the next connect starts one new generation with no overlap;
  `drain` refuses new streams without spawning, keeps the active client's
  identical tool catalog, persists across generations, and is cleared only by
  `restart` or `stop`; a stale `--generation` is refused without touching the
  newer owned generation; `restart` preserves attached logical streams, fully
  reaps the old process before starting and initializing exactly one newer
  generation, and the same client immediately completes another tool call with
  `reconnectRequired=false`; an unexpected backend exit returns one structured
  unknown-outcome error for its interrupted call, initializes a bounded newer
  generation, emits `notifications/tools/list_changed`, and the same client then
  calls it successfully; `stop` still closes streams; dedicated registrations refuse
  lifecycle control and only aggregate; and peer Registry `describe`/`status`
  answers merge a compact redacted `lifecycle` field while `list` rows and
  Registry-only callers stay unchanged;
- package metadata (`pyproject.toml`) declares exactly the root runtime modules;
  layout tests reject undeclared root Python files or runtime imports of tests;
  console versions match the runtime, `doctor` reports valid and
  invalid configurations, and expected CLI errors remain traceback-free.

The `test_archive_profile.py` suite verifies the deterministic safe ZIP profile:

- identical directory trees build byte-identical `archive-v1` archives (fixed
  time, stored, name-sorted, SHA-256 manifest member), also across `os.utime`
  perturbation, and extraction round-trips content and digests;
- unsafe member names (traversal, absolute, drive, backslash) are rejected, and
  crafted archives with duplicate names, case-folded collisions, ancestor
  conflicts, directory members, or symlink/device/FIFO/socket members fail closed
  at the correct gate before any destination write;
- zip-slip/absolute/drive paths never escape the destination (real-path
  containment re-check), decompression bombs and oversized members/totals are
  bounded, and entry-count overflow is rejected at build and at extract;
- a missing or wrong manifest (profile, member count, member hashes/sizes) is
  rejected, a destination that already exists is never overwritten, and a
  CRC-tampered archive leaves no partial directory behind.

The `test_streamable_http_stdio.py` suite verifies the stdio-to-Streamable-HTTP
adapter against a local `http.server` fixture implementing the MCP 2025-06-18
transport:

- session establishment with `Mcp-Session-Id` and negotiated
  `MCP-Protocol-Version` headers on every subsequent POST and GET; DELETE ends
  the session and the process exits cleanly on stdio EOF;
- JSON and SSE (`text/event-stream`) request responses, including progress
  notifications relayed before the response on the same POST stream;
- `notifications/cancelled` reaches an in-flight request (observed mid-request
  cancellation), protocol downgrade negotiation, and an unsupported-version
  rejection that never creates a session;
- server-initiated notifications and requests over the GET stream, with the
  local client's JSON-RPC response relayed back and recorded by the fixture;
- HTTP 400/401/429/500 map to structured JSON-RPC errors with `httpStatus` data
  while the stream stays healthy; HTTP 404 invalidates the session, performs one
  fresh sessionless `initialize`, and retries the request exactly once;
- request timeouts fail only their own request (concurrent requests complete);
- static `Authorization`/secret headers and server error bodies never appear on
  adapter stdout or stderr, and a failing logical stream does not affect a
  healthy one on the same host.

## Distribution build

Use an explicitly approved isolated build environment; build tools are not runtime
dependencies. The root layout guard permits generated build output directories.

```bash
PYTHONDONTWRITEBYTECODE=1 .venv-build/bin/python -m build --no-isolation --outdir release-artifacts
PYTHONDONTWRITEBYTECODE=1 python3 -m tests.verify_distribution --root . \
  --wheel release-artifacts/win_wsl_mcp_bridge-0.4.0-py3-none-any.whl \
  --sdist release-artifacts/win_wsl_mcp_bridge-0.4.0.tar.gz
PYTHONDONTWRITEBYTECODE=1 python3 -m tests.smoke_install \
  release-artifacts/win_wsl_mcp_bridge-0.4.0-py3-none-any.whl
```

`build` first creates the sdist, then builds the wheel from it. The verifier derives
runtime modules from `pyproject.toml`, requires exact wheel source bytes, verifies
all docs/tests/fixtures in the sdist, and rejects development code inside wheels.
Install the wheel into an isolated environment and run both console entry points
with `--version`; this is not production installation.

## Real Windows/WSL field test

Follow `DEPLOYMENT.md` with temporary per-host registries, workspaces, spools, and
fixture MCPs. Run initialize/list/call, artifact publication, and staged local
file input in both directions, then stop both foreground nodes and confirm their
child processes exit. Never use a real business MCP, production registry, or
service during this test.

## Managed-production and real-MCP migration gates

Before migrating a real MCP or wrapping the foreground commands in a managed
service, add and pass the applicable checks:

- real Windows shared-backend process-tree and browser-profile cleanup tests;
- concurrent artifact fairness, global spool quota, and janitor telemetry tests;
- large frame and backpressure tests beyond the current bounded fixtures;
- installed DSH model-visible runtime-instruction tests;
- installed Codex and Claude Code compatibility tests, including startup pruning,
  native-HTTP capability detection, and control-MCP visibility;
- rollback to the existing per-project bridge.

The offline configuration fixtures do cover deterministic discovery/enrollment,
stdio projection, preservation of unrelated entries, drift detection, and rollback
for Codex, Claude Code, and DSH-shaped configurations. They do not prove the
behavior of any currently installed client binary, and must not be reported as
real client/OS field acceptance.

If a deployment later admits mutually untrusted local principals, it also needs
owner-authenticated WSL Unix-socket and Windows named-pipe publisher tests,
different-OS-user denial tests, destination directory-handle race tests, and
Windows reparse-point tests. Those are outside the supported trusted-local
profile.
