# Verification

The core runtime checks are offline and use only Python's standard library.
Packaging and real Windows/WSL field checks are listed separately.

## Syntax without cache directories

```bash
python3 - <<'PY'
from pathlib import Path
for path in [
    Path('bridge_runtime.py'),
    Path('bridge_publisher.py'),
    Path('fixture_mcp.py'),
    Path('shared_fixture_mcp.py'),
    Path('test_bridge.py'),
    Path('win-bridge-mcp/bridge.py'),
    Path('wsl-bridge-mcp/bridge.py'),
]:
    compile(path.read_text(encoding='utf-8'), str(path), 'exec')
print('syntax ok')
PY
```

## Unit and integration tests

```bash
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest -v test_bridge.py
```

The suite verifies:

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
- explicit `multiProcessAllowed=false` enforcement with one shared spawn future,
  one backend/profile owner, compatible initialize virtualization, collision-free
  per-client ids, serialized requests, cancellation/progress/server-request
  routing, and structured lease busy/fixed-view errors;
- lease transfer after explicit release and abnormal client disconnect;
- backend crash and bridge restart cleanup of the fixture's owned child process,
  strict old-exit-before-new-start generation evidence, and an independent
  registry target remaining callable throughout;
- backend exit drains the final JSON-RPC response before closing client streams;
- ordinary stream data is sequence-acknowledged only after downstream drain, slow
  handlers do not block unrelated peer frames, and close fails active data/artifact
  acknowledgements immediately;
- local-client EOF has a bounded grace period, remote-first close exits the proxy,
  and node shutdown terminates active business processes;
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
- package metadata exposes only `bridge_runtime` and `bridge_publisher`, console
  versions match the runtime, `doctor` reports valid and invalid configurations,
  and expected CLI errors remain traceback-free.

## Distribution build

Build in an isolated checkout or CI workspace so generated metadata cannot affect
the two-component source-tree assertion:

```bash
python -m pip install build
python -m build
```

Require both `win_wsl_mcp_bridge-<version>.tar.gz` and
`win_wsl_mcp_bridge-<version>-py3-none-any.whl`. Inspect the wheel to require only
`bridge_runtime.py`, `bridge_publisher.py`, and `.dist-info`; build the wheel again
from the sdist and run both installed console scripts with `--version`.

## Real Windows/WSL field test

Follow `DEPLOYMENT.md` with temporary per-host registries, workspaces, spools, and
fixture MCPs. Run initialize/list/call and artifact publication in both directions,
then stop both foreground nodes and confirm their child processes exit. Never use
a real business MCP, production registry, or service during this test.

## Managed-production and real-MCP migration gates

Before migrating a real MCP or wrapping the foreground commands in a managed
service, add and pass the applicable checks:

- real Windows shared-backend process-tree and browser-profile cleanup tests;
- concurrent artifact fairness, global spool quota, and janitor telemetry tests;
- agent-to-remote-MCP file-input staging tests;
- large frame and backpressure tests beyond the current bounded fixtures;
- DSH model-visible runtime instruction tests;
- Codex compatibility tests;
- rollback to the existing per-project bridge.

If a deployment later admits mutually untrusted local principals, it also needs
owner-authenticated WSL Unix-socket and Windows named-pipe publisher tests,
different-OS-user denial tests, destination directory-handle race tests, and
Windows reparse-point tests. Those are outside the supported trusted-local
profile.
