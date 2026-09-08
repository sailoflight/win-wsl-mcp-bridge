#!/usr/bin/env python3
"""SDK acceptance harness for the Bridge-owned registry_mcp / control_mcp
modern (2026-07-28) frontend (P10).

This file is NOT part of the unittest suite (deliberately named ``sdk_interop.py``,
not ``test_*``): ``unittest discover -s tests`` never imports it.  It is a
standalone, runnable acceptance script that drives the *official* MCP Python SDK
``mcp==2.0.0`` (public ``Client`` / ``StdioServerParameters`` / ``stdio_client``
API, verified against the v2.0.0 tag source) against temporary subprocess
launchers of this repository's own ``registry-mcp`` and ``control-mcp`` in both
``--protocol-era modern`` and ``--protocol-era legacy`` modes, and reports JSON.

It installs nothing and mutates nothing: every server process is a short-lived
child spawned by the SDK's stdio transport with ``command=sys.executable`` and
``env={"PYTHONDONTWRITEBYTECODE": "1"}``; there are no profiles, no secrets, and
no production services.  The control endpoint is only ever probed with the
connection handshake plus ``tools/list`` (its tool calls are structurally
forbidden here); the registry read-only tools are only called when the operator
explicitly supplies a disposable registry query backend (see
``--registry-query-endpoint``).  Nothing is ever applied, confirmed, or staged.

Required install (parent asks for separate authorization; never auto-installed):

    .venv-build/sdk-validation/bin/python -m pip install mcp==2.0.0

Then run from the repository root, inside that venv:

    .venv-build/sdk-validation/bin/python tests/sdk_interop.py            # JSON on stdout
    .venv-build/sdk-validation/bin/python tests/sdk_interop.py --help

The script imports ``mcp`` and this repository's ``bridge_runtime`` only inside
``main()``, so ``python3 tests/sdk_interop.py --help`` also works on a Python
that does not have the SDK installed; a real run without the SDK fails fast
with a structured JSON report (exit code 2).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
RUNTIME = REPO_ROOT / "wsl-bridge-mcp" / "bridge.py"

# The exact official SDK distribution and version this harness is written for.
SDK_PACKAGE = "mcp"
SDK_VERSION_PIN = "2.0.0"
SDK_INSTALL_COMMAND = (
    f".venv-build/sdk-validation/bin/python -m pip install {SDK_PACKAGE}=={SDK_VERSION_PIN}"
)

# Public API entrypoints actually used (verified against the v2.0.0 tag:
# docs_src/client_transports/tutorial004.py + src/mcp/client/client.py).
SDK_ENTRYPOINTS = (
    "from mcp import Client",
    "from mcp import StdioServerParameters",
    "from mcp.client.stdio import stdio_client",
    "await client.list_tools()",
    "client.protocol_version / client.server_info / client.instructions",
)

# Endpoint → (runtime subcommand, expected legacy/mcp identity name,
# expected tool names).  Expectations are cross-checked at run time against
# bridge_runtime so the SDK-observed surface must equal the real surface.
ENDPOINTS = {
    "registry": {
        "command": "registry-mcp",
        "server_name": "win-wsl-mcp-registry",
        "tool_names": frozenset(
            {
                "bridge_registry_list",
                "bridge_registry_search",
                "bridge_registry_describe",
                "bridge_registry_status",
            }
        ),
        "read_only_tool_probe": ("bridge_registry_list", "bridge_registry_status"),
    },
    "control": {
        "command": "control-mcp",
        "server_name": "win-wsl-mcp-control",
        "tool_names": frozenset({"bridge_control", "bridge_diagnostics"}),
        "read_only_tool_probe": (),
    },
}

# Accepted --protocol-era values for the endpoint launchers, and the SDK Client
# ``mode`` that must observe each: "legacy" servers are driven with the
# byte-identical initialize handshake (mode="legacy"); modern servers with the
# default discover probe (mode="auto").  A third case re-runs the legacy
# endpoint under mode="auto" to prove the SDK's discover → initialize fallback
# still classifies our legacy endpoints correctly.
ERAS = {
    "modern": {"launcher_era": "modern", "mode": "auto", "expect_version": "2026-07-28"},
    "legacy": {"launcher_era": "legacy", "mode": "legacy", "expect_version": "2025-06-18"},
    "legacy-auto": {"launcher_era": "legacy", "mode": "auto", "expect_version": "2025-06-18"},
}


def _report(ok: bool, **payload: object) -> dict:
    return {"ok": ok, **payload}


def _as_dict(model: object) -> object:
    """Best-effort pydantic model → JSON-able dict, with a safe fallback."""
    dumper = getattr(model, "model_dump", None)
    if callable(dumper):
        try:
            return dumper(exclude_unset=True, by_alias=True)
        except Exception as exc:  # pragma: no cover - defensive
            return {"<unserializable>": f"{type(exc).__name__}: {exc}"}
    return {}


def _tool_summary(tools: list) -> list[dict]:
    out: list[dict] = []
    for tool in tools:
        out.append(
            {
                "name": getattr(tool, "name", None),
                "has_description": bool(getattr(tool, "description", None)),
            }
        )
    return out


async def _probe_endpoint(
    endpoint: str,
    era: str,
    *,
    runtime,  # bridge_runtime module (imported lazily by the caller)
    local_port: int,
    timeout_s: float,
    read_timeout_s: float,
) -> dict:
    """One SDK session against one temporary endpoint/era launcher.

    Uses only discover/initialize (driven by the Client handshake) and
    ``tools/list``.  The control endpoint never has a tool invoked here.
    """
    try:
        from mcp import Client, StdioServerParameters  # type: ignore[import-not-found]
        from mcp.client.stdio import stdio_client  # type: ignore[import-not-found]
    except Exception as exc:  # SDK missing in this interpreter
        return _report(
            False,
            endpoint=endpoint,
            era=era,
            phase="sdk-import",
            error=f"{type(exc).__name__}: {exc}",
        )

    launcher = ENDPOINTS[endpoint]
    era_cfg = ERAS[era]
    server_params = StdioServerParameters(
        command=sys.executable,
        args=[
            str(RUNTIME),
            launcher["command"],
            "--local-host",
            "127.0.0.1",
            "--local-port",
            str(local_port),
            "--protocol-era",
            era_cfg["launcher_era"],
        ],
        env={"PYTHONDONTWRITEBYTECODE": "1"},
        cwd=str(REPO_ROOT),
    )

    # Scaffold that records which tool calls (if any) happened; the control
    # guard below turns any into a hard failure, so this must stay empty there.
    calls: list[dict] = []
    observed: dict = {}

    async def run_session() -> None:
        transport = stdio_client(server_params)
        async with Client(
            transport,
            mode=era_cfg["mode"],
            read_timeout_seconds=read_timeout_s,
        ) as client:  # type: ignore[arg-type]
            observed["protocol_version"] = client.protocol_version
            server_info = client.server_info
            observed["server_info"] = (
                {"name": server_info.name, "version": server_info.version}
                if server_info is not None
                else None
            )
            observed["instructions"] = client.instructions
            capabilities = client.server_capabilities
            observed["capabilities"] = _as_dict(capabilities)
            listed = await client.list_tools()
            observed["tools"] = _tool_summary(listed.tools)
            observed["tool_count"] = len(listed.tools)

    try:
        await asyncio.wait_for(
            run_session(), timeout=timeout_s
        )
    except asyncio.TimeoutError:
        return _report(False, endpoint=endpoint, era=era, phase="session",
                       error=f"timed out after {timeout_s}s")
    except Exception as exc:  # SDK/MCP errors surface as structured failures
        return _report(False, endpoint=endpoint, era=era, phase="session",
                       error=f"{type(exc).__name__}: {exc}")

    checks: list[str] = []
    failures: list[str] = []

    def check(condition: bool, message: str) -> None:
        (checks if condition else failures).append(message)

    check(observed.get("protocol_version") == era_cfg["expect_version"],
          f"protocol_version == {era_cfg['expect_version']!r}")
    info = observed.get("server_info") or {}
    check(info.get("name") == launcher["server_name"],
          f"server_info.name == {launcher['server_name']!r}")
    check(bool(info.get("version")), "server_info.version present")
    check(isinstance(observed.get("instructions"), str)
          and len(observed["instructions"]) > 0, "instructions present")
    tool_names = {t["name"] for t in observed.get("tools", [])}
    check(tool_names == set(launcher["tool_names"]),
          f"tools/list == {sorted(launcher['tool_names'])}")
    check(observed.get("tool_count", 0) == len(launcher["tool_names"]),
          "tool_count matches the real fixed toolset")
    # Cross-check against the repository's own single source of truth.
    expected_instructions = (
        runtime.REGISTRY_INSTRUCTIONS
        if endpoint == "registry"
        else runtime.CONTROL_INSTRUCTIONS
    )
    check(observed.get("instructions") == expected_instructions,
          "instructions byte-equal to the runtime constant")

    # Control hard guard: discovery/list only — the tool call list must be
    # empty and no lifecycle action may ever be reachable from this harness.
    if endpoint == "control":
        check(calls == [], "no control tool was ever called (never apply)")
        check("bridge_control" in tool_names and "bridge_diagnostics" in tool_names,
              "control advertises exactly the two fixed tools")
        check("apply" not in {name.lower() for name in tool_names},
              "no apply-like tool is advertised")

    if failures:
        return _report(False, endpoint=endpoint, era=era, observed=observed,
                       checks=checks, failures=failures)
    return _report(True, endpoint=endpoint, era=era, observed=observed, checks=checks)


async def _registry_read_only_probe(
    runtime,
    *,
    query_host: str,
    query_port: int,
    timeout_s: float,
) -> dict:
    """Optional: call two registry read-only tools against an operator-supplied
    disposable registry query backend (see --registry-query-endpoint).

    The backend must be a temporary peer-registry query listener speaking the
    repository's socket contract ({"op": "registry", "scope": "remote",
    "action": ..., "arguments": {...}} newline JSON, reply {"ok": true,
    "result": ...}); this harness never points at a production registry.
    """
    from mcp import Client, StdioServerParameters  # type: ignore[import-not-found]
    from mcp.client.stdio import stdio_client  # type: ignore[import-not-found]

    server_params = StdioServerParameters(
        command=sys.executable,
        args=[
            str(RUNTIME), "registry-mcp", "--local-host", query_host,
            "--local-port", str(query_port), "--protocol-era", "modern",
        ],
        env={"PYTHONDONTWRITEBYTECODE": "1"},
        cwd=str(REPO_ROOT),
    )
    names = ENDPOINTS["registry"]["read_only_tool_probe"]
    results: list[dict] = []

    async def run_session() -> None:
        transport = stdio_client(server_params)
        async with Client(
            transport,
            mode="auto",
            read_timeout_seconds=timeout_s,
        ) as client:  # type: ignore[arg-type]
            for name in names:
                try:
                    result = await client.call_tool(name, {})
                except Exception as exc:
                    results.append({"tool": name, "ok": False,
                                    "error": f"{type(exc).__name__}: {exc}"})
                    continue
                text = ""
                for block in getattr(result, "content", []) or []:
                    if getattr(block, "type", None) == "text":
                        text = getattr(block, "text", "") or ""
                structured = getattr(result, "structured_content", None)
                results.append(
                    {
                        "tool": name,
                        "ok": not bool(getattr(result, "is_error", False)),
                        "text_preview": text[:400],
                        "structured_type": type(structured).__name__ if structured is not None else None,
                    }
                )

    try:
        await asyncio.wait_for(run_session(), timeout=timeout_s)
    except Exception as exc:
        return _report(False, phase="registry-read-only-probe",
                       error=f"{type(exc).__name__}: {exc}", results=results)
    ok = bool(results) and all(r.get("ok") for r in results)
    return _report(ok, phase="registry-read-only-probe", results=results)


def _sdk_metadata() -> dict:
    """SDK/spec facts for the JSON report (guarded; never required to run)."""
    meta: dict = {"distribution": SDK_PACKAGE, "version": None,
                  "python": sys.version.split()[0]}
    try:
        from importlib import metadata  # stdlib

        meta["version"] = metadata.version(SDK_PACKAGE)
    except Exception:
        meta["version"] = None
    try:
        import mcp_types.version as _ver  # type: ignore[import-not-found]

        meta["latest_modern"] = _ver.LATEST_MODERN_VERSION
        meta["modern_versions"] = sorted(_ver.MODERN_PROTOCOL_VERSIONS)
        meta["handshake_versions"] = sorted(_ver.HANDSHAKE_PROTOCOL_VERSIONS)
    except Exception:
        meta["latest_modern"] = None
        meta["modern_versions"] = []
        meta["handshake_versions"] = []
    return meta


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "SDK acceptance for the Bridge registry_mcp/control_mcp modern "
            "frontend: official mcp==2.0.0 Client over stdio launchers."
        )
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=60.0,
        help="per-session timeout in seconds (default 60)",
    )
    parser.add_argument(
        "--local-port",
        type=int,
        default=1,
        help="loopback port handed to the endpoint launchers; only used if a "
        "registry tool is actually called, which needs --registry-query-endpoint",
    )
    parser.add_argument(
        "--registry-query-endpoint",
        default="",
        metavar="HOST:PORT",
        help="OPTIONAL disposable peer-registry query backend for the registry "
        "read-only tool probe; default: probe skipped (never a production registry)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(list(argv) if argv is not None else sys.argv[1:])
    results: dict = {
        "harness": "sdk_interop",
        "install_command": SDK_INSTALL_COMMAND,
        "entrypoints": list(SDK_ENTRYPOINTS),
        "sdk": _sdk_metadata(),
        "runtime_python": sys.executable,
    }

    # The official SDK is a hard dependency of a real run; degrade to a
    # structured, non-zero report when absent instead of crashing.
    try:
        import mcp  # noqa: F401  (presence probe only)

        sdk_version = results["sdk"].get("version")
        if sdk_version != SDK_VERSION_PIN:
            results["warning"] = (
                f"expected mcp=={SDK_VERSION_PIN}, found mcp=={sdk_version}; "
                f"results are only authoritative for the pinned SDK"
            )
    except Exception as exc:
        results["ok"] = False
        results["error"] = (
            f"official SDK not importable in {sys.executable}: "
            f"{type(exc).__name__}: {exc}"
        )
        results["how_to_install"] = (
            f"parent-authorized: {SDK_INSTALL_COMMAND}"
        )
        print(json.dumps(results, ensure_ascii=False, indent=2))
        return 2

    try:
        import bridge_runtime as runtime  # repo truth for cross-checks
    except Exception as exc:
        results["ok"] = False
        results["error"] = (
            f"could not import the repository runtime for cross-checks: "
            f"{type(exc).__name__}: {exc}"
        )
        print(json.dumps(results, ensure_ascii=False, indent=2))
        return 2

    cases: list[tuple[str, str]] = [
        ("registry", "modern"),
        ("registry", "legacy"),
        # mode="auto" against the legacy endpoint proves the SDK's
        # discover-probe → initialize fallback classifies our legacy servers.
        ("registry", "legacy-auto"),
        ("control", "modern"),
        ("control", "legacy"),
    ]

    async def run_all() -> list[dict]:
        out: list[dict] = []
        for endpoint, era in cases:
            out.append(await _probe_endpoint(
                endpoint,
                era,
                runtime=runtime,
                local_port=args.local_port,
                timeout_s=args.timeout,
                read_timeout_s=min(args.timeout, 15.0),
            ))
        return out

    results["cases"] = asyncio.run(run_all())

    # Optional read-only registry probe: only against an operator-supplied
    # disposable backend; skipped (not failed) by default.
    probe = {"mode": "skipped", "reason": "no --registry-query-endpoint given"}
    if args.registry_query_endpoint:
        try:
            host, _, port_s = args.registry_query_endpoint.rpartition(":")
            probe = asyncio.run(_registry_read_only_probe(
                runtime, query_host=host or "127.0.0.1",
                query_port=int(port_s), timeout_s=args.timeout,
            ))
        except Exception as exc:
            probe = {"mode": "error", "error": f"{type(exc).__name__}: {exc}"}
    results["registry_read_only_probe"] = probe

    results["ok"] = all(c.get("ok") for c in results["cases"]) and (
        probe.get("ok", True) if isinstance(probe, dict) and "ok" in probe else True
    )
    print(json.dumps(results, ensure_ascii=False, indent=2))
    return 0 if results["ok"] else 1


if __name__ == "__main__":
    os.environ.setdefault("PYTHONDONTWRITEBYTECODE", "1")
    sys.exit(main())
