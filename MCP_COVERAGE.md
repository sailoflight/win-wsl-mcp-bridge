# MCP Coverage Matrix

## Scope

This matrix separates downstream MCP semantics from the bridge's private
transport. Dedicated registrations are standard stdio byte relays; explicit
`multiProcessAllowed=false` registrations use the documented shared JSON-RPC
adapter. The peer TCP protocol is not exposed as an MCP transport. Evidence is the MCP 2025-06-18
specification, the standard transport/resource definitions, current source, and
offline tests.

References:

- https://modelcontextprotocol.io/specification/2025-06-18
- https://modelcontextprotocol.io/specification/2025-06-18/basic/transports
- https://modelcontextprotocol.io/specification/2025-06-18/server/resources
- https://developers.openai.com/api/docs/mcp
- https://developers.openai.com/api/docs/guides/tools-code-interpreter
- https://developers.openai.com/api/reference/resources/containers/subresources/files/methods/retrieve

## Transport coverage

| MCP transport or deployment | Current support | Boundary |
|---|---|---|
| Standard stdio MCP | Native | Registry launches one local command; all MCP bytes remain downstream-owned. |
| Windows stdio MCP called from WSL | Yes | WSL proxy opens a logical stream to the Windows registry id. |
| WSL stdio MCP called from Windows | Yes | Reverse `open` uses the same WSL-initiated physical link. |
| Streamable HTTP MCP | No native support | Requires an explicit stdio adapter or a separate bridge transport implementation. |
| Legacy HTTP+SSE MCP | No native support | Same as Streamable HTTP; it is not silently converted. |
| In-process SDK server | Not directly | It must expose stdio or be wrapped by an explicit adapter. |

## MCP capability coverage

| Capability/message family | Current support | Notes |
|---|---|---|
| Lifecycle and `initialize` | Dedicated: transparent; shared: virtualized | Shared mode forwards one compatible physical initialize and returns the cached result with each client's original id. |
| Tools and structured results | Yes | Dedicated mode is transparent. Shared mode preserves content/schema values while rewriting routing ids. |
| Prompts and resources | Yes | Dedicated mode is transparent. Shared requests are serialized and global list-changed notifications are broadcast. URI portability remains external. |
| Completion | Yes | Dedicated mode is transparent; shared requests use the same serialized router. |
| Logging and notifications | Routed | Business stderr remains bridge diagnostics. Shared request-scoped notifications go to the active client; explicit global list changes are broadcast. |
| Progress and cancellation | Routed | Shared mode rewrites progress/request tokens and restores the client values; dedicated mode stays byte-transparent. |
| Client roots | Transparent metadata only | A root declaration is not a filesystem mount, transfer grant, or remote path authorization. |
| Sampling and elicitation | Yes | Shared backend requests route only to the active client with rewritten ids; ambiguous ownership returns an error. |
| Pagination/cursors | Yes | Values are preserved; shared requests are serialized. |
| New/unknown JSON-RPC methods | Mode-dependent | Dedicated mode is byte-transparent. Shared requests route generically; unknown notifications without an unambiguous owner are not broadcast. |
| Authentication for remote HTTP MCP | Not provided | The current business transport is stdio; the bridge never acquires HTTP/OAuth credentials. |

## Result and file coverage

| Result/output form | Current support | Delivery behavior |
|---|---|---|
| Text and structured JSON | Yes | Raw MCP result. |
| Inline image/audio base64 | Yes | Raw MCP result; base64 and framing overhead apply. |
| Embedded resource text/blob | Yes | Raw MCP result. The client decides whether and where to save it. |
| HTTP(S) resource link reachable by the client | Yes | Link is forwarded; network reachability and authorization stay external. |
| Host-local path in text/JSON | No file semantics | It remains an untrusted string and never triggers a read or copy. |
| Host-local `file://` resource link | Not portable by itself | Use explicit workspace-push publication or MCP `resources/read`. |
| Durable MCP-generated regular file | Yes, opt-in | `artifacts/1` pushes a published staged file into the caller's authorized local workspace before the tool returns. |
| Directory/tree result | Not directly | The business MCP must create one archive file and publish that regular file. |
| Symlink, junction/reparse point, device, FIFO, or detectable hardlink | Rejected | These are not valid publishable artifacts. |
| File larger than configured limits | Rejected | Both the MCP registration and receiving node enforce byte limits. |
| Interrupted artifact transfer | Fail closed | No resume in version 1; partial destination and source stage are removed. |
| Agent-local file passed as a remote MCP tool input | Not yet | Inline/blob or a resource readable by the remote MCP works; local-path upload/staging needs a separate future input contract. |

## Workspace-push behavior

OpenAI container files use managed container/file identifiers and controlled
content retrieval rather than trusting model-supplied server paths. The bridge
applies the same authority model while changing the final delivery direction:
the business MCP explicitly publishes, and the bridge commits into the client's
workspace. The Agent does not fetch from the MCP host.

1. Operator enables `artifactDelivery` for the business MCP and configures
   allowed receiving workspace roots.
2. The local proxy binds one existing inbox to the logical MCP stream.
3. The bridge gives a dedicated MCP a private per-stream stage, or a shared MCP a
   generation-owned stage whose publishes bind to the uniquely active serialized
   client request, plus a random local publisher token.
4. The MCP writes one completed regular file and invokes `publish` using a
   single-component relative name.
5. The bridge opens once, rejects unsafe object types, snapshots while hashing,
   and enforces limits.
6. The existing full-duplex link transfers ordered 64 KiB chunks under the
   negotiated `artifacts/1` extension.
7. The receiver writes an owner-only partial file, checks size and SHA-256,
   fsyncs, and atomically commits under
   `.mcp-artifacts/<artifact-id>/<name>` without overwrite.
8. Publish returns a standard MCP `resource_link` pointing to the already-local
   committed file; the business MCP includes it in its normal tool result.

## Responsibility boundary

- **Business MCP:** generate, decide exportability, stage, publish, and return the
  delivered resource link. It must fall back or report a bounded error when the
  artifact environment is absent.
- **Bridge:** authorize session and destination, snapshot, transfer, verify,
  commit, and clean temporary state. It never infer paths from business output.
- **Agent:** consume the returned local resource link. It never requests an
  arbitrary remote source path.
- **Operator:** configure registry opt-in, size limits, spool ownership, allowed
  workspace roots, service identity, retention, and recovery.

## Remaining gaps and optional extensions

`field_test.py` now provides real Windows/WSL fixture evidence for both invocation
and artifact directions using temporary host-local registries and workspaces. It
does not claim managed-service recovery or real business-MCP migration evidence.

- The supported profile trusts the local OS user, Agents, and registered MCPs and
  restricts all bridge traffic to loopback. Owner-authenticated Unix sockets,
  owner-ACL Windows named pipes, directory-handle anchoring, and reparse-point-safe
  native APIs are optional extensions for deployments with mutually untrusted
  local principals, not requirements of this profile.
- Agent-to-remote-MCP local file input staging is not implemented.
- Artifact resume, directory-native transfer, cross-user brokering, global spool
  quotas, and production janitor telemetry are not implemented.
- Streamable HTTP/SSE transport adaptation is not implemented.
- Dynamic capability-warehouse integration remains a roadmap item.

Implementation order and acceptance gates for every remaining gap are defined in
`DEVELOPMENT_PLAN.md`; none of those entries are current Registry capabilities.
