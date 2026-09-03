# Changelog

All notable changes to this project are recorded here.

## 0.4.0 - Unreleased

### Added

- Bridge-enforced single shared backend generations for registrations with
  `multiProcessAllowed=false`.
- JSON-RPC initialize virtualization, request/progress ID rewriting, response,
  error, cancellation, notification, and server-request routing for shared
  clients.
- Registration-driven exclusive client leases and fixed shared tool-view policy.
- Generation-scoped artifact publication and owned process-tree cleanup.
- Offline concurrent-client, lease transfer, disconnect, backend crash, bridge
  restart, generation ordering, final-response drain, and independent-target
  acceptance fixtures.

### Changed

- Shared backend requests are globally serialized for synchronous MCP runtimes.
- Public registry process metadata now reports enforcement outcomes without
  exposing private lease tool patterns or release matching rules.

## 0.3.1 - 2026-08-31

### Fixed

- Windows listener callbacks now treat normal peer EOF and bounded bridge/socket failures as handled link termination instead of emitting an unhandled asyncio traceback.

## 0.3.0 - 2026-08-31

### Added

- Per-stream data acknowledgements and bounded end-to-end backpressure.
- Standard JSON-RPC error handling and `ping` support for the Registry MCP.
- Installable Windows and WSL console entry points plus read-only `doctor` preflight.
- Dedicated installed-mode `bridge_publisher.py` contract for Windows and POSIX venvs.
- Reproducible real Windows/WSL ephemeral fixture acceptance test.
- Offline sdist/wheel metadata, deployment guide, and Windows/Linux CI matrix.
- Trusted-local deployment profile and stricter loopback enforcement.
- Owner-only POSIX registry and artifact-partial permissions.

### Changed

- Bridge peer protocol upgraded to `win-wsl-mcp-bridge/0.2`; older peers fail the handshake cleanly.
- Registry responses that exceed the bridge frame limit now fail the query without dropping the peer link.
- Local client EOF now has a bounded grace period before the remote stream is terminated.
- Remote-first stream close now exits the local proxy even while client stdin remains open.
- Peer registry/artifact setup handlers no longer block unrelated link frames.
- POSIX SIGTERM and node cancellation perform bounded child-process cleanup.
- Startup removes stale workspace `.partial` files beneath configured artifact roots without deleting committed outputs.
- Artifact receive setup now aborts immediately if its `artifact_ready` response cannot reach the peer.

### Fixed

- Artifact senders are released immediately when a stream closes during a chunk acknowledgement.
- Manifest `enabled` values and reserved artifact environment keys are validated strictly.

## 0.2.0

- Initial bidirectional stdio bridge and negotiated `artifacts/1` workspace-push implementation.
