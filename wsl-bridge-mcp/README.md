# WSL bridge component

This component actively establishes the full-duplex peer link and exposes a
local control socket to WSL MCP clients.

```bash
win-wsl-mcp-wsl --version
win-wsl-mcp-wsl registry-init --manifest registry.example.json --replace
win-wsl-mcp-wsl doctor --allow-artifact-root /home/user/authorized-workspaces
win-wsl-mcp-wsl serve --allow-artifact-root /home/user/authorized-workspaces
win-wsl-mcp-wsl connect example-win-mcp --artifact-inbox /home/user/authorized-workspaces/project-a
win-wsl-mcp-wsl registry-mcp
```

From a source checkout, replace `win-wsl-mcp-wsl` with `python3 bridge.py`.

A file-capable MCP must opt in with `artifactDelivery.enabled=true`. The node
provides its child process with a private stage and publish token; the MCP calls
`python3 bridge.py publish <single-filename>` and embeds the returned
`resource_link`. The Agent never supplies a remote source path.

The default database is
`$XDG_STATE_HOME/win-wsl-mcp-bridge/registry.sqlite3`, falling back to
`~/.local/state/win-wsl-mcp-bridge/registry.sqlite3`. Do not share the Windows
SQLite file through `/mnt/c`; each side owns its local registry.

`connect` is a standard stdio MCP proxy. The same physical link also carries
Windows-initiated streams targeting MCPs registered on WSL, so Windows never
needs to open a new network connection to WSL.

The default local control port is `8769`; the peer link connects to loopback port
`8767`. The WSL node reconnects when the Windows peer is temporarily absent.
