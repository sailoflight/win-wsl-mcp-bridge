# Windows bridge component

This component listens for the one full-duplex peer link and exposes a local
control socket to Windows MCP clients.

```powershell
win-wsl-mcp-win --version
win-wsl-mcp-win registry-init --manifest registry.example.json --replace
win-wsl-mcp-win doctor --allow-artifact-root C:\AuthorizedWorkspaces
win-wsl-mcp-win serve --allow-artifact-root C:\AuthorizedWorkspaces
win-wsl-mcp-win connect example-wsl-mcp --artifact-inbox C:\AuthorizedWorkspaces\ProjectA
win-wsl-mcp-win registry-mcp
```

From a source checkout, replace `win-wsl-mcp-win` with `python bridge.py`.

A file-capable MCP must opt in with `artifactDelivery.enabled=true`. The node
provides its child process with a private stage and publish token; the MCP calls
`python bridge.py publish <single-filename>` and embeds the returned
`resource_link`. The Agent never supplies a remote source path.

The default database is
`%LOCALAPPDATA%\WinWslMcpBridge\registry.sqlite3`. Override it with `--registry`
for development or testing; do not place the production database on a WSL/UNC
shared path.

`connect` is a standard stdio MCP proxy. The selected target is resolved only by
the WSL-side registry; callers cannot supply its command, cwd, args, or env.

The default local control port is `8768`; the peer link listens on loopback port
`8767`. Bind addresses are rejected unless they resolve to loopback.
