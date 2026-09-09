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
| Streamable HTTP MCP | Typed in Registry; relay, explicit adapters, and bounded lifecycle control contracts | Registry schema v4 validates owner-host loopback endpoints and redacts endpoint/headers from peer summaries. Raw stdio `open` is refused instead of silently converting transport. An opt-in loopback relay (`serve --http-relay-port`) mounts peer `streamable-http` MCPs at `/mcp/<registered-id>` with owner-side endpoint/header resolution; `streamable_http_stdio.py` is the verified adapter for the explicit `connect-http` HTTP-to-stdio route; `stdio_http_facade.py` is the explicit loopback stdio-to-HTTP converted facade. `external-controlled` rows may register a private bounded `controlContract` so the owner node can execute lifecycle `drain`/`restart`/`stop` (fixed method, relative path or loopback url, bounded timeout/statuses, optional GET readiness); the contract is never disclosed publicly. Managed `bridge-managed` HTTP registrations use an owner-local launch definition, bounded readiness/shutdown policy, exact-generation control, and no-overlap process supervision (offline fixture verified). |
| Legacy HTTP+SSE MCP | No native support | Same as Streamable HTTP; it is not silently converted. |
| In-process SDK server | Not directly | It must expose stdio or be wrapped by an explicit adapter. |

## MCP capability coverage

| Capability/message family | Current support | Notes |
|---|---|---|
| Lifecycle and `initialize` | Dedicated: transparent; shared: virtualized | One physical generation requests canonical legacy `2025-11-25` and validates the backend's observed choice from `2024-11-05`, `2025-03-26`, `2025-06-18`, `2025-11-25`. Logical clients receive their own verified tools profile; newer unverified legacy requests may negotiate down, unusable results fail closed. Capability advertisements are the observed tools intersection, with Tasks and other unverified families withheld. Physical client capabilities stay empty. Requested/observed versions and rejection are journaled separately; no business calls are replayed. |
| Tools and structured results | Yes, profile-bounded in shared mode | Dedicated mode is transparent. Shared mode projects frozen field/content rules; unrepresentable complete output fails explicitly instead of being silently truncated. |
| Prompts and resources | Dedicated transparency | Generic legacy routing remains, but the shared compatibility profile does not advertise these unverified families. URI portability remains external. |
| Completion | Dedicated transparency | Not advertised by the shared tools-only profile. |
| Logging and notifications | Mode-dependent | Dedicated bytes stay transparent. Legacy internal notification routing remains, but logging is not advertised by the shared profile; modern requests receive only implemented scoped progress. |
| Progress and cancellation | Routed | Shared mode rewrites progress/request tokens and restores the client values; dedicated mode stays byte-transparent. |
| Client roots | Transparent metadata only | A root declaration is not a filesystem mount, transfer grant, or remote path authorization. |
| Sampling and elicitation | Yes | Shared backend requests route only to the active client with rewritten ids; ambiguous ownership returns an error. |
| Pagination/cursors | Yes | Values are preserved; shared requests are serialized. |
| New/unknown JSON-RPC methods | Mode-dependent | Dedicated mode is byte-transparent. Shared requests route generically; unknown notifications without an unambiguous owner are not broadcast. |
| Authentication for remote HTTP MCP | Static credentials only | The adapter accepts Operator-supplied `--header`/environment credentials and never logs them; it implements no OAuth/DCR flows and the bridge itself never acquires HTTP/OAuth credentials. |

## Protocol-era coverage

Both era-conversion directions advertise only the verified tools intersection.
Dedicated streams keep downstream protocol ownership.

| Agent / backend | Current route | Verified boundary |
|---|---|---|
| Legacy / legacy | Shared initialize or dedicated relay | Negotiated legacy profile; dedicated bytes unchanged |
| Modern 2026-07-28 / legacy | Shared tools-only projection | Per-request metadata, observed discover/instructions, tools/list/call, progress/cancel; no unsolicited legacy subscriptions or logging |
| Legacy / modern-only | Explicit `legacy_modern_stdio.py` | Frozen tools profiles; observed discovery/instructions; bounded framing, queue and deadlines; cancellation/id-reuse and no-replay gates passed |
| Modern / modern | Dedicated byte relay | Backend/client retain protocol ownership; native relay does not rewrite the era |

Modern shared capabilities are the observed tools intersection. Resources,
prompts, Tasks/MRTR, subscriptions, logging, roots, sampling and elicitation are
not advertised there. Discovery preserves the backend's canonical instructions.
Modern HTTP conversion is explicitly selected in either direction. HTTP-to-stdio
uses owner-private `transport.protocolEra="modern"`; stdio-to-HTTP uses
`stdio_http_facade.py --protocol-era modern` and only a registered target. These
are bounded POST-only profiles: schema preflight/header validation, actual
results/instructions, request cancellation, and uncertainty without replay. They
are not complete HTTP conformance claims: schema pages/header bindings, payloads
and deadlines are bounded; resumption and implicit MRTR fulfillment are absent.
Native forwarding retains its separate transparent contract.

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
| Directory/tree result | As one regular archive file | `archive_profile.py` builds a deterministic, manifest-bearing ZIP (`archive-v1`) from a source directory and strictly/atomically extracts it; the MCP publishes that one file through `artifacts/1`. Native directory transfer is not implemented. |
| Symlink, junction/reparse point, device, FIFO, or detectable hardlink | Rejected | These are not valid publishable artifacts. |
| File larger than configured limits | Rejected | Both the MCP registration and receiving node enforce byte limits. |
| Interrupted artifact transfer | Fail closed | No resume in version 1; partial destination and source stage are removed. |
| Agent-local file passed as a remote MCP tool input | Yes, opt-in | `artifact-inputs/1` stages one Operator-authorized Agent-local file into the remote MCP's private per-stream input stage and rewrites only the minted handle; the tool reads the committed local path. Inline/blob and `resources/read` remain preferred for small inputs. |

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
  arbitrary remote source path. For inputs, the Agent-side adapter uploads only
  a local file explicitly authorized by the Operator's workspace roots and
  embeds the returned handle; it never sends a sender-local path string as a
  tool argument.
- **Operator:** configure registry opt-in, size limits, spool ownership, allowed
  workspace roots, service identity, retention, and recovery.

## Agent-local input staging behavior

`artifact-inputs/1` reverses the direction of workspace push while keeping the
same authority split: the Agent side uploads an explicitly authorized local
file, and the remote MCP reads it from a bridge-created stage.

1. Operator enables `inputDelivery` for a dedicated business MCP
   (`multiProcessAllowed` is not `false`); shared-backend input staging is
   rejected at `registry-init`.
2. Peers negotiate the `artifact-inputs/1` extension on the link
   (`artifactInputs` echoes only when both sides offer it).
3. The receiving node launches the MCP with `WIN_WSL_MCP_BRIDGE_INPUT_STAGE`
   pointing at a private per-stream stage directory.
4. The Agent-side adapter calls the `stage_input` local operation for its open
   stream with a source that resolves, without symlinks, to a regular file
   beneath the node's Operator-authorized workspace root.
5. Only input handle, name, media type, size, digest, and ordered chunks cross
   the link; the staged path never leaves the MCP host.
6. The receiver verifies limits, name safety, byte count, and SHA-256, then
   commits the owner-only file into the stage and acknowledges.
7. The adapter puts the returned handle (`bridge-input://input-...`) verbatim in
   the tool arguments; the bridge rewrites exactly that JSON string literal to
   the staged path, and every other byte of every message is forwarded
   unchanged.
8. The business MCP reads the staged path under `WIN_WSL_MCP_BRIDGE_INPUT_STAGE`
   and returns its normal result.

Unknown, foreign, expired, or ambiguous handles fail closed with a JSON-RPC
error instead of reaching the MCP. Staging that violates the root/symlink/type
rules fails before any byte leaves the host, and the per-stream stage is removed
on stream close. The bidirectional integration suite verifies real
Windows-to-WSL and WSL-to-Windows input staging with byte and SHA-256 matches.

## Harness tool-exposure compatibility

The verified DSH Web/DS4F environment supports and has activated the legacy stdio
bridge dynamic facade: user request snapshots show 54 -> 134 -> 54 definitions
through Onshape expansion and collapse. Changes appear in subsequent model
requests after client refresh, potentially later in the same turn. Native DSH
tool-call representation remains compatible with this dynamic catalog.

Claude Code and Codex retain complete discovery to use their native tool search;
their exact installed dynamic-refresh behavior is not field-verified here.
Other DSH profiles and unknown clients require their own evidence. Modern MCP
and native HTTP routes are outside the legacy dynamic-facade contract. Keep
expanded libraries stable across related work to avoid repeated cache rebuilds.
See [the environment decision matrix and cache policy](MCP_TOOL_EXPOSURE.md)
for the canonical evidence, activation scope and enrollment distinction.

## Remaining gaps and optional extensions

`tests/field_test.py` now provides real Windows/WSL fixture evidence for both invocation
and artifact directions using temporary host-local registries and workspaces. It
does not claim managed-service recovery or real business-MCP migration evidence.

- The supported profile trusts the local OS user, Agents, and registered MCPs and
  restricts all bridge traffic to loopback. Owner-authenticated Unix sockets,
  owner-ACL Windows named pipes, directory-handle anchoring, and reparse-point-safe
  native APIs are optional extensions for deployments with mutually untrusted
  local principals, not requirements of this profile.
- Agent-to-remote-MCP local file input staging is implemented for dedicated
  business MCPs in both directions; input staging for shared-mode backends,
  transfer resume, and cross-session input persistence are not implemented.
- Artifact output resume is implemented under negotiated `artifacts/2`; directory-native
  transfer, cross-user brokering, global spool quotas, and production janitor telemetry
  are not implemented.
- Streamable HTTP support now includes the opt-in native peer relay, the standalone
  HTTP-to-stdio adapter (`streamable_http_stdio.py`), and the explicit stdio-to-HTTP
  converted facade (`stdio_http_facade.py`), each with offline fixture conformance.
  Native HTTP configuration projection and managed HTTP supervision have offline tests.
  OAuth/DCR, SSE resumption via `Last-Event-ID`, official SDK conformance and installed-client
  native HTTP behavior remain unverified.
- Directory-shaped results are produced and consumed through the deterministic
  `archive_profile.py` profile; publishing a multi-member archive still
  transfers one regular file, and directory-native transfer is not implemented.
- Dynamic capability-warehouse integration remains a roadmap item.

Implementation order and acceptance gates for every remaining gap are defined in
`DEVELOPMENT_PLAN.md`; none of those entries are current Registry capabilities.
