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

The wheel contains only `bridge_runtime.py`, `bridge_publisher.py`, and package
metadata. Keep released wheels in an Operator-owned versions directory for
rollback. Do not build directly in the production checkout. The repository does
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
PYTHONDONTWRITEBYTECODE=1 python3 field_test.py
```

To validate installed wheel entry points rather than source launchers, create the
two temporary venvs first and pass their runners:

```text
python3 field_test.py \
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
