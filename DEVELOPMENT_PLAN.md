# Development Plan

## Planning rule

This file contains only unimplemented work. Current behavior and evidence remain
in `README.md`, `ARCHITECTURE.md`, `MCP_COVERAGE.md`, and `VERIFICATION.md`.
Roadmap entries must not be exposed as current Registry capabilities until their
acceptance gates pass.

## Current completed baseline

- Standard stdio MCP is relayed in both directions over one WSL-initiated link,
  with per-stream acknowledgements and independent backpressure.
- Lifecycle, tools, prompts, resources, completion, roots, sampling, elicitation,
  logging, progress, cancellation, notifications, and unknown downstream MCP
  methods remain byte-transparent.
- MCP-generated regular files can opt into negotiated `artifacts/1` workspace
  push. The business MCP publishes; the Bridge snapshots, transfers, verifies,
  and commits; the Agent receives a local `resource_link` and does not fetch a
  peer path.
- Public Registry MCP discovery is peer-only and does not duplicate local MCPs.
- Versioned wheel/sdist packaging, per-host console entry points, `doctor`, and
  ephemeral real Windows/WSL fixture acceptance are implemented.

## Optional hardening: untrusted-local artifact boundary

This phase is not required by the supported local-only profile, which trusts the
local OS user, Agents, and registered MCPs. It applies only if a future
deployment admits mutually untrusted local principals. It hardens the completed
file-output capability; it does not add a new MCP result type.

### Work

- Replace loopback publisher control with a mode-0600 Unix domain socket on WSL.
- Implement an owner-ACL Windows named pipe publisher endpoint.
- Bind publisher sessions to service identity, logical stream, target MCP,
  physical-link generation, random nonce, and expiry.
- Anchor destination creation to opened directory handles on WSL using
  `openat`/`mkdirat`/`linkat`/`renameat` with no-follow flags.
- Implement reparse-point-safe Windows directory and file creation using native
  handle APIs; reject junctions and reparse points throughout the path.
- Add global spool quota, per-identity quota, metrics, and bounded retention beyond
  the current stale-workspace-partial startup janitor.
- Validate the Windows no-overwrite branch on NTFS and the WSL branch on ext4.

### Acceptance

- A different OS user cannot publish, select an inbox, reuse a token, or access a
  stage even when it can reach loopback.
- Concurrent directory replacement cannot escape an authorized workspace root.
- Killing either node during snapshot, transfer, commit, or acknowledgement
  leaves no unverified final file.
- Real Windows/WSL evidence proves both transfer directions and rollback.

## P1: Agent-local file input to a remote MCP

A local path supplied as a remote tool argument is currently only a string. This
phase adds safe client-to-MCP staging without making paths remotely readable.

### Design

- Add a separate negotiated `artifact-inputs/1` extension; do not overload
  output publication authority.
- The local client adapter explicitly uploads one file already authorized by the
  client workspace policy.
- Transfer only an opaque input handle, name, media type, size, and SHA-256.
- The remote Bridge commits into a private per-stream input stage and gives the
  business MCP a local staged path/URI.
- Tool arguments carry an explicit descriptor or standard resource link, never a
  rewritten arbitrary string path.
- Inline/embedded MCP content and `resources/read` remain the preferred path for
  small inputs.

### Acceptance

- Absolute paths, traversal, symlinks, devices, directories, hardlinks, size
  overflow, foreign handles, and expired sessions fail closed.
- The remote MCP can access only the staged input, not the sender workspace.
- Input bytes and SHA-256 match in both Windows-to-WSL and WSL-to-Windows tests.
- Existing ordinary tool arguments remain byte-for-byte unchanged.

## P2: Streamable HTTP MCP adapter

The Bridge currently launches stdio MCPs only. HTTP support must be an explicit
adapter, not hidden protocol conversion inside the byte relay.

### Design

- Add a separately testable local stdio-to-Streamable-HTTP adapter component
  without adding a third Bridge runtime directory.
- Keep OAuth/API credentials with the adapter/business MCP deployment, not in
  public Registry metadata or peer frames.
- Preserve MCP session ids, protocol negotiation, server notifications,
  cancellation, resumability semantics, and HTTP error mapping.
- Treat legacy HTTP+SSE as an optional compatibility adapter only if a concrete
  registered MCP still requires it.

### Acceptance

- Official Streamable HTTP conformance scenarios pass through the stdio-facing
  adapter.
- Disconnect, reconnect, notification, cancellation, and authentication failures
  do not affect other logical streams.
- No Authorization, cookie, token, URL query secret, or response body enters
  Bridge diagnostics by default.

## P3: Directory and multi-file result packages

Current behavior requires the business MCP to create one archive and publish it
as a regular file.

### Design

- Prefer a deterministic archive profile first: ZIP or tar with normalized
  relative paths, no symlinks/devices, bounded entry count, bounded expanded
  size, and a manifest of per-entry hashes.
- Consider native multi-file packages only after a real consumer requires random
  access; use one package handle and one atomic destination directory commit.
- Never recursively copy a peer-supplied directory path.

### Acceptance

- Zip-slip, absolute members, duplicate/case-colliding names, symlinks, devices,
  decompression bombs, and entry-count overflow are rejected.
- Extraction is atomic and cannot overwrite existing workspace content.

## P4: Large-file resume and durable delivery receipts

`artifacts/1` is fail-closed and has no resume.

### Design

- Add a new extension revision rather than changing `artifacts/1` in place.
- Use content-addressed snapshots, fixed chunk hashes, durable receipt state, and
  explicit retention/expiry.
- Resume only within the same authenticated publisher/receiver identities and
  declared artifact digest.
- Define acknowledgement-loss semantics so a committed but unacknowledged file
  can be reconciled without duplicate overwrite.

### Acceptance

- Link loss at every chunk boundary resumes without byte duplication.
- Foreign/expired sessions cannot probe whether a digest exists.
- Completed files are exactly-once by destination identity; partials remain
  invisible.

## P5: Capability warehouse integration

This is the future single-Bridge configuration mode discussed for cooperation
with an external AI capability repository.

### Design

- Use stable publisher/name/version plus signed manifest digest identities.
- Exchange local client capability inventory for deterministic deduplication.
- Search bounded public summaries first; load exact schemas and connect only on
  selection.
- Separate discovery, trust, installation, execution authorization, and removal.
- Keep private SQLite launch data, credentials, paths, and runtime state local.

### Acceptance

- A directly registered local MCP never appears a second time through the
  warehouse.
- Repository content cannot grant credentials, mutation, cost, restart, or
  installation authority.
- Dynamic tool add/replace/remove works for DSH and Codex without stale schemas.

## Compatibility validation track

These are tests and adapters, not new Bridge semantics:

- Validate future MCP protocol revisions and task-related methods through the
  transparent data plane.
- Add official SDK fixtures for Python, TypeScript, C#, Java, and Rust stdio
  servers where available.
- Verify inline text/image/audio, embedded resources, resource links,
  `resources/read`, prompts, sampling, elicitation, roots, progress,
  cancellation, notifications, and messages larger than one Bridge frame.
- Add real Windows/WSL CI; Linux role simulation remains necessary but
  insufficient for Windows filesystem and process behavior.

## Deferred non-goals

- Arbitrary peer filesystem browse/read/write.
- Inferring files by scanning tool text, JSON strings, or `file://` paths.
- Automatic dependency installation from capability metadata.
- Public-network exposure of the peer or publisher control protocols.
- Claiming business-level lifecycle or singleton guarantees when the registration
  does not explicitly select `multiProcessAllowed=false` bridge enforcement.
