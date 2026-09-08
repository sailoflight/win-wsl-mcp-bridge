# Official SDK Interoperability Evidence

The explicitly approved development environment uses **mcp 2.0.0** and Python
3.12.3 on Linux. Resolved development dependencies are recorded in
[SDK requirements](../tests/sdk-requirements.txt); they are not project runtime
dependencies. Pip's environment bootstrap version was 24.0.

The pinned official [SDK release source](https://github.com/modelcontextprotocol/python-sdk/tree/v2.0.0)
and its `Client`, `StdioServerParameters`, and `stdio_client` public APIs provide
an independent peer implementation. This is interoperability evidence, **not a
claim of passing the complete official conformance suite**.

## Executed cases

| Endpoint | SDK mode / server era | Observed revision | Result |
|---|---|---|---|
| Registry | auto / modern | 2026-07-28 | PASS |
| Registry | legacy / legacy | 2025-06-18 | PASS |
| Registry | auto / legacy | 2025-06-18 (discover-to-initialize fallback) | PASS |
| Control | auto / modern | 2026-07-28 | PASS |
| Control | legacy / legacy | 2025-06-18 | PASS |

Every case verified server identity, exact tool names/count, and byte-equal
instructions against the repository runtime. Registry exposes four tools;
control exposes two. Control tool calls were structurally excluded. No production
listener or registry was queried; the optional read-only registry tool probe was
skipped. Each SDK transport owned and closed its temporary source-launcher process.

## Reproduce after approving development dependency installation

```sh
PYTHONDONTWRITEBYTECODE=1 python3 -m venv .venv-build/sdk-validation
.venv-build/sdk-validation/bin/python -m pip install -r tests/sdk-requirements.txt
PYTHONDONTWRITEBYTECODE=1 .venv-build/sdk-validation/bin/python tests/sdk_interop.py --timeout 20
```

The requirements capture the tested Linux environment, rather than a universal
Windows dependency lock. The harness uses `wsl-bridge-mcp/bridge.py`; root
`bridge_runtime.py` deliberately refuses direct execution. The script prints a
JSON report and exits nonzero on failure. An SDK installation is never performed
by the harness. Removing only `.venv-build/sdk-validation/` rolls back this
development dependency environment.

These checks do not prove installed Agent behavior, production recovery, the
whole HTTP compatibility matrix, or support for unadvertised protocol families.
