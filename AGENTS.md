<!-- agent-project-guides:v3:start -->
## Project governance routing

Project ID: `win-wsl-mcp-bridge`; variant: `shared-runtime.pinned`; pinned release: `3.0.3` / `sha256:f3dc0ca9cd50d27deac2b4e9c063d243dd3ce20127edc88d9f8b4c3aac4bd603`; manifest: `sha256:9d8cdbd2c59887817e00cf9469e3837af5d8da331c5d4221db447697edc94b04`.

Before work, run `apg context --target . --task <current-task> --format context` and use only the returned governance content. Resolve any ambiguity before protected work. The shared CLI and exact packed digest are runtime dependencies; missing content fails explicitly and never falls back to `latest`. Returned sources are intended context and do not prove model-effective context.
<!-- agent-project-guides:v3:end -->
# Agent instructions

This repository delivers one shared, bidirectional WIN-WSL MCP bridge. It has
exactly two component directories: `win-bridge-mcp/` and `wsl-bridge-mcp/`.
Do not add another component directory without an explicit architecture change.

Start with `README.md`, then read `ARCHITECTURE.md` for protocol or trust-boundary
work and `VERIFICATION.md` for checks. Business MCPs remain ordinary stdio MCPs;
do not add project-specific Onshape, Taobao, browser, credential, or cloud logic.

Keep the runtime standard-library-only. Bind listeners to loopback, resolve only
pre-registered ids, never accept remote command/args/env, keep MCP stdout
protocol-clean, and expose only the peer registry's redacted metadata. The
supported deployment profile is local-only and trusts the local OS user, Agents,
and registered business MCPs; it does not defend against those trusted local
principals attacking one another. Do not expose any bridge listener outside
loopback. Each host owns its own local SQLite registry; never place a database on
a shared Windows/WSL filesystem or aggregate local MCPs into the Agent-visible
peer list.

Artifact delivery is explicit workspace push, never remote-path scraping. A
business MCP may publish only a completed regular file in its bridge-created
stream or shared-generation stage. The bridge snapshots, limits, hashes,
transfers, and atomically commits only beneath an Operator-authorized local
inbox. Agents never submit peer source paths. Preserve raw MCP byte transparency
for registrations whose `multiProcessAllowed` is true or unknown. An explicit
false selects the generic bridge-owned shared JSON-RPC backend, ID routing,
request serialization, lifecycle state machine, and optional registration-driven
client lease; never add business-specific matching logic.

Run tests with `PYTHONDONTWRITEBYTECODE=1` so verification does not create a
third top-level directory. No production deployment, Windows service changes,
credentials, or real business MCP mutation are authorized by repository work.
