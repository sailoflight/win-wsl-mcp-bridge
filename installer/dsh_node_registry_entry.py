#!/usr/bin/env python3
"""Client-specific lifecycle wrapper for the installed generic bridge nodes.

DeepSeek Harness (DSH) profiles use this script as their ``registry`` MCP entry:
each profile's ``@deepseek-ai/dsh-mcp-client`` entry launches it, and the first
process that finds the three loopback ports free becomes the owner of the whole
bridge stack (Windows node + WSL node + ``registry-mcp``). This is installer
territory because the entry name, the profile wiring, and the host's installed
paths are client-specific; the bridge runtime itself never selects behavior by
client kind.

An install has *several* DSH profiles on one host (``web``, ``dsh-tui``,
``headless``) that are enrolled as separate environments but share one bridge
stack. Ownership is therefore decided per start:

``own``
    No listener answers on any of the three ports: start both nodes, wait until
    the peer registry exposes the expected remote ids, then serve
    ``registry-mcp``. Stopping this process stops the stack it started.
``attach``
    A ready stack already answers: start no node, stop no node, and just serve
    ``registry-mcp`` against the running local node. Every further DSH profile
    keeps its registry tools, and the profile that merely attaches never tears
    the shared stack down. If the owner disappears later, this attached
    ``registry-mcp`` loses the node, exits, and the Harness reconnect restarts
    this entry, which then finds the ports free and becomes the owner.
``fail``
    Ports are occupied but the local node does not answer a registry query (a
    foreign or half-dead listener): report and exit non-zero instead of guessing
    or starting a second stack.

Configuration comes from this module's own defaults and environment overrides,
never from a remote caller: no registry/peer content selects a command, path, or
port here.
"""
from __future__ import annotations

import os
import signal
import socket
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from bridge_runtime import BridgeError, local_registry_query

DEFAULT_WINDOWS_LOCAL_PORT = 8768
DEFAULT_WSL_LOCAL_PORT = 8769
DEFAULT_LINK_PORT = 8770
STARTUP_TIMEOUT_SECONDS = 30
ATTACH_WAIT_POLL_SECONDS = 0.5
EXPECTED_REMOTE_IDS = frozenset({"onshape", "taobao"})
PORT_PROBE_TIMEOUT_SECONDS = 0.2
WINDOWS_INTEROP_ROOT = Path("/mnt/c/Users")
WINDOWS_INSTALL_TAIL = Path("AppData/Local/WinWslMcpBridge")
WINDOWS_NODE_TAIL = Path("runtime/Scripts/win-wsl-mcp-win.exe")
WINDOWS_REGISTRY_NAME = "registry.sqlite3"

_stopping = False


@dataclass(frozen=True)
class SupervisorConfig:
    """One resolved supervisor configuration (paths, ports, log root)."""

    windows_node: Path
    wsl_node: Path
    windows_registry: str
    wsl_registry: Path
    log_root: Path
    windows_local_port: int = DEFAULT_WINDOWS_LOCAL_PORT
    wsl_local_port: int = DEFAULT_WSL_LOCAL_PORT
    link_port: int = DEFAULT_LINK_PORT


def _request_stop(_signum: int, _frame: object) -> None:
    global _stopping
    _stopping = True


def _port_accepts(port: int) -> bool:
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=PORT_PROBE_TIMEOUT_SECONDS):
            return True
    except OSError:
        return False


def _plan(occupied: Sequence[int], stack_ready: bool) -> str:
    """Decide this start's role from the ports and local-node readiness."""
    if not occupied:
        return "own"
    if stack_ready:
        return "attach"
    return "fail"


def _stack_ready(wsl_local_port: int) -> bool:
    """True when the running local node answers a registry query.

    A successful answer proves an *operable* bridge stack owns the ports, which
    is what attach mode needs. It makes no claim about which business MCPs are
    registered: a freshly started stack with an empty peer mirror is still a
    stack this process may serve.
    """
    try:
        value = local_registry_query(
            "127.0.0.1", wsl_local_port, "remote", "list", {},
        )
    except (BridgeError, OSError):
        return False
    return isinstance(value, list)


def _wait_for_peer(
    windows_process: subprocess.Popen[bytes],
    wsl_process: subprocess.Popen[bytes],
    wsl_local_port: int,
) -> None:
    deadline = time.monotonic() + STARTUP_TIMEOUT_SECONDS
    while time.monotonic() < deadline and not _stopping:
        if windows_process.poll() is not None:
            raise RuntimeError(
                f"Windows bridge node exited during startup ({windows_process.returncode})"
            )
        if wsl_process.poll() is not None:
            raise RuntimeError(
                f"WSL bridge node exited during startup ({wsl_process.returncode})"
            )
        try:
            value = local_registry_query(
                "127.0.0.1", wsl_local_port, "remote", "list", {},
            )
            visible_ids = {
                item.get("id") for item in value if isinstance(item, dict)
            }
            if EXPECTED_REMOTE_IDS.issubset(visible_ids):
                return
        except (BridgeError, OSError):
            pass
        time.sleep(0.1)
    if _stopping:
        raise RuntimeError("bridge node startup was cancelled")
    raise RuntimeError("bridge peer did not become ready before the startup deadline")


def _rotate_log(path: Path) -> None:
    try:
        if path.stat().st_size >= 5 * 1024 * 1024:
            previous = path.with_suffix(path.suffix + ".1")
            previous.unlink(missing_ok=True)
            path.replace(previous)
    except FileNotFoundError:
        pass


def _terminate(process: subprocess.Popen[bytes] | None) -> None:
    if process is None or process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def _windows_path(node: Path, *suffix: str) -> str:
    """Render a ``/mnt/<drive>/...`` path as the Windows path of *suffix* there.

    The WSL-side path of the installed Windows node is used only to derive the
    sibling registry path the Windows node is told to open; a path outside the
    interop mount is returned as a plain POSIX path.
    """
    parts = node.parts
    install_tail = len(WINDOWS_NODE_TAIL.parts)
    if len(parts) > 3 + install_tail and parts[1] == "mnt" and len(parts[2]) == 1:
        drive = parts[2].upper()
        tail = list(parts[3:-install_tail]) + list(suffix)
        return drive + ":\\" + "\\".join(tail)
    return str(node.parent.parent.parent.joinpath(*suffix))


def _discover_windows_node() -> Path:
    """Bounded local discovery of the installed Windows node.

    Only the standard per-user interop location is searched, and only for the
    exact installed executable name; a host with several Windows profiles
    resolves this with ``WIN_WSL_MCP_BRIDGE_WIN_NODE`` instead of guessing.
    """
    matches = sorted(
        WINDOWS_INTEROP_ROOT.glob(f"*/{WINDOWS_INSTALL_TAIL}/{WINDOWS_NODE_TAIL}")
    )
    if len(matches) == 1:
        return matches[0]
    return WINDOWS_INTEROP_ROOT / "Public" / WINDOWS_INSTALL_TAIL / WINDOWS_NODE_TAIL


def resolve_config(environ: Mapping[str, str] | None = None) -> SupervisorConfig:
    """Resolve this host's paths and ports from defaults plus env overrides."""
    env = os.environ if environ is None else environ

    def _override(name: str, default: str) -> str:
        value = env.get(name, "")
        return value if value else default

    data_home = Path(_override("XDG_DATA_HOME", str(Path.home() / ".local" / "share")))
    state_home = Path(_override("XDG_STATE_HOME", str(Path.home() / ".local" / "state")))
    install_root = data_home / "win-wsl-mcp-bridge"
    windows_node = Path(
        _override("WIN_WSL_MCP_BRIDGE_WIN_NODE", str(_discover_windows_node()))
    )
    return SupervisorConfig(
        windows_node=windows_node,
        wsl_node=Path(
            _override(
                "WIN_WSL_MCP_BRIDGE_WSL_NODE",
                str(install_root / "runtime" / "bin" / "win-wsl-mcp-wsl"),
            )
        ),
        windows_registry=_override(
            "WIN_WSL_MCP_BRIDGE_WIN_REGISTRY",
            _windows_path(windows_node, WINDOWS_REGISTRY_NAME),
        ),
        wsl_registry=Path(
            _override(
                "WIN_WSL_MCP_BRIDGE_WSL_REGISTRY",
                str(state_home / "win-wsl-mcp-bridge" / "registry.sqlite3"),
            )
        ),
        log_root=Path(
            _override(
                "WIN_WSL_MCP_BRIDGE_LOG_ROOT",
                str(state_home / "win-wsl-mcp-bridge" / "logs"),
            )
        ),
        windows_local_port=int(
            _override("WIN_WSL_MCP_BRIDGE_WIN_LOCAL_PORT", str(DEFAULT_WINDOWS_LOCAL_PORT))
        ),
        wsl_local_port=int(
            _override("WIN_WSL_MCP_BRIDGE_WSL_LOCAL_PORT", str(DEFAULT_WSL_LOCAL_PORT))
        ),
        link_port=int(_override("WIN_WSL_MCP_BRIDGE_LINK_PORT", str(DEFAULT_LINK_PORT))),
    )


def _serve_registry(
    config: SupervisorConfig,
    environment: dict[str, str],
    *,
    windows_process: subprocess.Popen[bytes] | None,
    wsl_process: subprocess.Popen[bytes] | None,
) -> int:
    """Serve ``registry-mcp`` against the local node and mirror its exit code.

    Only nodes started by this process are torn down afterwards, so an
    attaching profile never stops the stack it attached to.
    """
    registry_process = subprocess.Popen(
        [
            str(config.wsl_node),
            "registry-mcp",
            "--local-port",
            str(config.wsl_local_port),
        ],
        env=environment,
    )
    try:
        while not _stopping:
            try:
                return registry_process.wait(timeout=ATTACH_WAIT_POLL_SECONDS)
            except subprocess.TimeoutExpired:
                if windows_process is not None and windows_process.poll() is not None:
                    raise RuntimeError(
                        f"Windows bridge node exited ({windows_process.returncode})"
                    )
                if wsl_process is not None and wsl_process.poll() is not None:
                    raise RuntimeError(f"WSL bridge node exited ({wsl_process.returncode})")
        return 0
    finally:
        _terminate(registry_process)


def run(
    config: SupervisorConfig,
    *,
    port_accepts: Callable[[int], bool] = _port_accepts,
    stack_ready: Callable[[], bool] | None = None,
) -> int:
    """Run one supervisor start and return the process exit code."""
    for path in (config.windows_node, config.wsl_node, Path(config.wsl_registry)):
        if not path.is_file():
            print(f"bridge supervisor: required path is missing: {path}", file=sys.stderr)
            return 1
    ports = (
        config.windows_local_port,
        config.wsl_local_port,
        config.link_port,
    )
    occupied = [port for port in ports if port_accepts(port)]
    if stack_ready is None:
        stack_ready = lambda: _stack_ready(config.wsl_local_port)  # noqa: E731
    role = _plan(occupied, bool(stack_ready()) if occupied else False)
    if role == "fail":
        print(
            "bridge supervisor: required port already accepts connections but the "
            f"local registry does not answer: {occupied}",
            file=sys.stderr,
        )
        return 1

    try:
        config.log_root.mkdir(parents=True, mode=0o700, exist_ok=True)
        config.log_root.chmod(0o700)
    except OSError:
        pass
    environment = os.environ.copy()
    environment["PYTHONDONTWRITEBYTECODE"] = "1"

    if role == "attach":
        try:
            return _serve_registry(
                config, environment, windows_process=None, wsl_process=None,
            )
        except Exception as exc:
            if not _stopping:
                print(f"bridge supervisor: {type(exc).__name__}: {exc}", file=sys.stderr)
            return 1

    windows_log_path = config.log_root / "windows-node.log"
    wsl_log_path = config.log_root / "wsl-node.log"
    _rotate_log(windows_log_path)
    _rotate_log(wsl_log_path)

    windows_process: subprocess.Popen[bytes] | None = None
    wsl_process: subprocess.Popen[bytes] | None = None
    with windows_log_path.open("ab", buffering=0) as windows_log, wsl_log_path.open(
        "ab", buffering=0
    ) as wsl_log:
        try:
            windows_process = subprocess.Popen(
                [
                    str(config.windows_node),
                    "serve",
                    "--registry",
                    config.windows_registry,
                    "--local-port",
                    str(config.windows_local_port),
                    "--link-port",
                    str(config.link_port),
                ],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=windows_log,
                env=environment,
            )
            wsl_process = subprocess.Popen(
                [
                    str(config.wsl_node),
                    "serve",
                    "--registry",
                    str(config.wsl_registry),
                    "--local-port",
                    str(config.wsl_local_port),
                    "--link-port",
                    str(config.link_port),
                ],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=wsl_log,
                env=environment,
            )
            _wait_for_peer(windows_process, wsl_process, config.wsl_local_port)
            if _stopping:
                return 0
            return _serve_registry(
                config, environment,
                windows_process=windows_process, wsl_process=wsl_process,
            )
        except Exception as exc:
            if not _stopping:
                print(f"bridge supervisor: {type(exc).__name__}: {exc}", file=sys.stderr)
            return 1
        finally:
            _terminate(wsl_process)
            _terminate(windows_process)


def main(argv: Sequence[str] | None = None) -> int:
    del argv  # MCP stdio entry: no command-line surface.
    return run(resolve_config())


if __name__ == "__main__":
    for signal_number in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signal_number, _request_stop)
    raise SystemExit(main(sys.argv[1:]))
