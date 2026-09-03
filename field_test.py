#!/usr/bin/env python3
"""Ephemeral real Windows/WSL acceptance test for the bridge."""

from __future__ import annotations

import argparse
import atexit
import base64
import hashlib
import json
import ntpath
import os
import socket
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path

from bridge_runtime import BridgeError, Registry, local_registry_query

ROOT = Path(__file__).resolve().parent
WSL_BRIDGE = ROOT / "wsl-bridge-mcp" / "bridge.py"
FIXTURE = ROOT / "fixture_mcp.py"


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def rpc_messages(value: str) -> str:
    messages = [
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": {"name": "field-test", "version": "1"},
            },
        },
        {"jsonrpc": "2.0", "method": "notifications/initialized"},
        {"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}},
        {
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": {"name": "echo", "arguments": {"value": value}},
        },
    ]
    return "".join(json.dumps(item, separators=(",", ":")) + "\n" for item in messages)


def artifact_messages(text: str) -> str:
    messages = [
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": {"name": "field-artifact-test", "version": "1"},
            },
        },
        {"jsonrpc": "2.0", "method": "notifications/initialized"},
        {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {"name": "create_artifact", "arguments": {"text": text}},
        },
    ]
    return "".join(json.dumps(item, separators=(",", ":")) + "\n" for item in messages)


def parse_responses(process: subprocess.CompletedProcess[str]) -> list[dict]:
    if process.returncode != 0:
        raise RuntimeError(process.stderr.strip() or f"proxy exited {process.returncode}")
    return [json.loads(line) for line in process.stdout.splitlines() if line]


def require_response(
    process: subprocess.CompletedProcess[str], index: int, label: str
) -> dict:
    responses = parse_responses(process)
    if len(responses) <= index:
        raise RuntimeError(
            f"{label} expected response index {index}, got {len(responses)}; "
            f"stdout={process.stdout!r}; stderr={process.stderr!r}"
        )
    return responses[index]


def write_wsl_registry(database: Path, manifest: Path) -> None:
    manifest.write_text(
        json.dumps(
            {
                "servers": [
                    {
                        "id": "wsl-field-fixture",
                        "name": "WSL field fixture",
                        "command": sys.executable,
                        "args": [str(FIXTURE)],
                        "env": {
                            "FIXTURE_MCP_NAME": "wsl-field-fixture",
                            "FIXTURE_EXIT_AFTER_CALL": "1",
                        },
                        "process": {
                            "multiProcessAllowed": False,
                            "enforcement": "bridge-shared-backend",
                        },
                        "capabilityGroups": ["field-test"],
                        "artifactDelivery": {"enabled": True, "maxBytes": 1048576},
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    Registry.initialize_database(database, manifest, replace=True)


def windows_python(
    launcher: Path,
    *arguments: str,
    input_text: str | None = None,
    timeout: int = 30,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [str(launcher), "-3", "-B", *arguments],
        input=input_text,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=timeout,
        check=False,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--powershell",
        default="/mnt/c/Windows/System32/WindowsPowerShell/v1.0/powershell.exe",
    )
    parser.add_argument(
        "--windows-py",
        default="",
        help="WSL path to py.exe; default discovers Get-Command py through PowerShell",
    )
    parser.add_argument("--windows-runner", default="")
    parser.add_argument("--wsl-runner", default="")
    parser.add_argument("--timeout", type=int, default=30)
    args = parser.parse_args()
    powershell = Path(args.powershell)
    if not powershell.is_file():
        print("field test requires Windows PowerShell through WSL interop", file=sys.stderr)
        return 2
    if args.windows_py:
        windows_py = Path(args.windows_py)
    else:
        try:
            discovered = subprocess.check_output(
                [
                    str(powershell),
                    "-NoProfile",
                    "-Command",
                    "(Get-Command py -ErrorAction Stop).Source",
                ],
                text=True,
                timeout=20,
            ).strip().replace("\r", "")
            windows_py = Path(
                subprocess.check_output(
                    ["wslpath", "-u", discovered],
                    text=True,
                    timeout=10,
                ).strip()
            )
        except (OSError, subprocess.SubprocessError):
            print("field test could not discover a working Windows py.exe", file=sys.stderr)
            return 2
    if not windows_py.is_file():
        print("field test requires a working Windows py.exe", file=sys.stderr)
        return 2
    windows_runner = Path(args.windows_runner) if args.windows_runner else None
    wsl_runner = Path(args.wsl_runner) if args.wsl_runner else None
    for label, runner in (("Windows", windows_runner), ("WSL", wsl_runner)):
        if runner is not None and not runner.is_file():
            print(f"field test {label} runner does not exist: {runner}", file=sys.stderr)
            return 2

    repo_windows = subprocess.check_output(
        ["wslpath", "-w", str(ROOT)], text=True, timeout=10
    ).strip()
    fixture_windows = ntpath.join(repo_windows, "fixture_mcp.py")
    win_bridge = ntpath.join(repo_windows, "win-bridge-mcp", "bridge.py")
    win_py_command = subprocess.check_output(
        ["wslpath", "-w", str(windows_py)], text=True, timeout=10
    ).strip()

    def run_windows_bridge(
        *bridge_args: str,
        input_text: str | None = None,
        timeout: int = args.timeout,
    ) -> subprocess.CompletedProcess[str]:
        if windows_runner is not None:
            return subprocess.run(
                [str(windows_runner), *bridge_args],
                input=input_text,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=timeout,
                check=False,
            )
        return windows_python(
            windows_py,
            win_bridge,
            *bridge_args,
            input_text=input_text,
            timeout=timeout,
        )

    wsl_bridge_command = (
        [str(wsl_runner)]
        if wsl_runner is not None
        else [sys.executable, str(WSL_BRIDGE)]
    )
    temp_name = f"win-wsl-mcp-field-{uuid.uuid4().hex}"
    create = subprocess.run(
        [
            str(powershell),
            "-NoProfile",
            "-Command",
            f"$p=Join-Path $env:TEMP '{temp_name}'; New-Item -ItemType Directory -Path $p -Force | Out-Null; Write-Output $p",
        ],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=20,
        check=True,
    )
    win_root = create.stdout.strip().replace("\r", "")

    def cleanup_windows_root() -> None:
        subprocess.run(
            [
                str(powershell),
                "-NoProfile",
                "-Command",
                f"Remove-Item -LiteralPath '{win_root}' -Recurse -Force -ErrorAction SilentlyContinue",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=20,
            check=False,
        )

    atexit.register(cleanup_windows_root)
    win_registry = ntpath.join(win_root, "registry.sqlite3")
    win_manifest = ntpath.join(win_root, "manifest.json")
    win_workspace = ntpath.join(win_root, "workspace")
    win_spool = ntpath.join(win_root, "spool")
    windows_python(
        windows_py,
        "-c",
        "import pathlib,sys; [pathlib.Path(p).mkdir(parents=True, exist_ok=True) for p in sys.argv[1:]]",
        win_workspace,
        win_spool,
        timeout=args.timeout,
    )
    win_manifest_value = {
        "servers": [
            {
                "id": "win-field-fixture",
                "name": "Windows field fixture",
                "command": win_py_command,
                "args": ["-3", "-B", fixture_windows],
                "env": {
                    "FIXTURE_MCP_NAME": "win-field-fixture",
                    "FIXTURE_EXIT_AFTER_CALL": "1",
                    "PYTHONDONTWRITEBYTECODE": "1",
                },
                "process": {
                    "multiProcessAllowed": False,
                    "enforcement": "business-mcp",
                },
                "capabilityGroups": ["field-test"],
                "artifactDelivery": {"enabled": True, "maxBytes": 1048576},
            }
        ]
    }
    encoded_manifest = base64.b64encode(json.dumps(win_manifest_value).encode()).decode()
    written = windows_python(
        windows_py,
        "-c",
        "import base64,pathlib,sys; pathlib.Path(sys.argv[1]).write_bytes(base64.b64decode(sys.argv[2]))",
        win_manifest,
        encoded_manifest,
        timeout=args.timeout,
    )
    if written.returncode != 0:
        raise RuntimeError(written.stderr)
    initialized = run_windows_bridge(
        "registry-init",
        "--registry",
        win_registry,
        "--manifest",
        win_manifest,
        "--replace",
        timeout=args.timeout,
    )
    if initialized.returncode != 0:
        raise RuntimeError(initialized.stderr)

    port_result = windows_python(
        windows_py,
        "-c",
        "import json,socket; s=[socket.socket(),socket.socket()]; [x.bind(('127.0.0.1',0)) for x in s]; print(json.dumps([x.getsockname()[1] for x in s])); [x.close() for x in s]",
        timeout=args.timeout,
    )
    if port_result.returncode != 0:
        raise RuntimeError(port_result.stderr)
    link_port, win_local_port = json.loads(port_result.stdout)
    wsl_local_port = free_port()

    win_node: subprocess.Popen[str] | None = None
    wsl_node: subprocess.Popen[str] | None = None
    try:
        win_node_command = (
            [str(windows_runner)]
            if windows_runner is not None
            else [str(windows_py), "-3", "-B", win_bridge]
        )
        win_node = subprocess.Popen(
            [
                *win_node_command,
                "serve",
                "--registry",
                win_registry,
                "--local-port",
                str(win_local_port),
                "--link-port",
                str(link_port),
                "--allow-artifact-root",
                win_workspace,
                "--artifact-spool-root",
                win_spool,
            ],
            text=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        with tempfile.TemporaryDirectory(prefix="win-wsl-mcp-field-") as temp:
            wsl_root = Path(temp)
            wsl_registry = wsl_root / "registry.sqlite3"
            wsl_manifest = wsl_root / "manifest.json"
            wsl_workspace = wsl_root / "workspace"
            wsl_spool = wsl_root / "spool"
            wsl_workspace.mkdir()
            write_wsl_registry(wsl_registry, wsl_manifest)
            environment = os.environ.copy()
            environment["PYTHONDONTWRITEBYTECODE"] = "1"
            wsl_node = subprocess.Popen(
                [
                    *wsl_bridge_command,
                    "serve",
                    "--registry",
                    str(wsl_registry),
                    "--local-port",
                    str(wsl_local_port),
                    "--link-port",
                    str(link_port),
                    "--allow-artifact-root",
                    str(wsl_workspace),
                    "--artifact-spool-root",
                    str(wsl_spool),
                ],
                text=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                env=environment,
            )
            deadline = time.monotonic() + args.timeout
            while time.monotonic() < deadline:
                if win_node.poll() is not None or wsl_node.poll() is not None:
                    raise RuntimeError("a bridge node exited before link establishment")
                try:
                    value = local_registry_query(
                        "127.0.0.1",
                        wsl_local_port,
                        "remote",
                        "describe",
                        {"id": "win-field-fixture"},
                    )
                    if value.get("id") == "win-field-fixture":
                        break
                except (OSError, BridgeError):
                    time.sleep(0.1)
            else:
                raise RuntimeError("real Windows/WSL peer link did not establish")

            wsl_call = subprocess.run(
                [
                    *wsl_bridge_command,
                    "connect",
                    "win-field-fixture",
                    "--local-port",
                    str(wsl_local_port),
                ],
                input=rpc_messages("wsl-to-windows"),
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=args.timeout,
                check=False,
            )
            wsl_response = require_response(wsl_call, 2, "WSL-to-Windows call")
            if (
                wsl_response["result"]["structuredContent"]["value"]
                != "wsl-to-windows"
            ):
                raise RuntimeError("WSL-to-Windows fixture response mismatch")

            win_call = run_windows_bridge(
                "connect",
                "wsl-field-fixture",
                "--local-port",
                str(win_local_port),
                input_text=rpc_messages("windows-to-wsl"),
                timeout=args.timeout,
            )
            win_response = require_response(win_call, 2, "Windows-to-WSL call")
            if (
                win_response["result"]["structuredContent"]["value"]
                != "windows-to-wsl"
            ):
                raise RuntimeError("Windows-to-WSL fixture response mismatch")

            wsl_content = "artifact from real Windows node"
            wsl_artifact_call = subprocess.run(
                [
                    *wsl_bridge_command,
                    "connect",
                    "win-field-fixture",
                    "--local-port",
                    str(wsl_local_port),
                    "--artifact-inbox",
                    str(wsl_workspace),
                ],
                input=artifact_messages(wsl_content),
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=args.timeout,
                check=False,
            )
            wsl_artifact = require_response(
                wsl_artifact_call, 1, "Windows-to-WSL artifact"
            )["result"]["content"][0]
            wsl_path = Path(
                wsl_artifact["_meta"]["io.win-wsl-mcp-bridge/artifact"]["localPath"]
            )
            if wsl_path.read_text(encoding="utf-8") != wsl_content:
                raise RuntimeError("Windows-to-WSL artifact content mismatch")
            expected_wsl_sha256 = hashlib.sha256(wsl_content.encode()).hexdigest()
            if (
                wsl_artifact["_meta"]["io.win-wsl-mcp-bridge/artifact"]["sha256"]
                != expected_wsl_sha256
            ):
                raise RuntimeError("Windows-to-WSL artifact digest mismatch")

            win_content = "artifact from real WSL node"
            win_artifact_call = run_windows_bridge(
                "connect",
                "wsl-field-fixture",
                "--local-port",
                str(win_local_port),
                "--artifact-inbox",
                win_workspace,
                input_text=artifact_messages(win_content),
                timeout=args.timeout,
            )
            win_artifact = require_response(
                win_artifact_call, 1, "WSL-to-Windows artifact"
            )["result"]["content"][0]
            win_path = win_artifact["_meta"]["io.win-wsl-mcp-bridge/artifact"]["localPath"]
            verified = windows_python(
                windows_py,
                "-c",
                "import pathlib,sys; print(pathlib.Path(sys.argv[1]).read_text(encoding='utf-8'))",
                win_path,
                timeout=args.timeout,
            )
            if verified.returncode != 0 or verified.stdout.strip() != win_content:
                raise RuntimeError(verified.stderr or "Windows artifact content mismatch")

            print(
                json.dumps(
                    {
                        "ok": True,
                        "windowsRegistry": initialized.stdout.strip(),
                        "linkPort": link_port,
                        "wslToWindows": True,
                        "windowsToWsl": True,
                        "artifactsBothDirections": True,
                        "windowsInstalledRunner": windows_runner is not None,
                        "wslInstalledRunner": wsl_runner is not None,
                    },
                    indent=2,
                )
            )
            return 0
    except Exception as exc:
        print(f"field test failed: {exc}", file=sys.stderr)
        for name, process in (("windows", win_node), ("wsl", wsl_node)):
            if process is not None and process.stderr is not None:
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill()
                detail = process.stderr.read().strip()
                if detail:
                    print(f"{name} node diagnostics:\n{detail}", file=sys.stderr)
        return 1
    finally:
        for process in (wsl_node, win_node):
            if process is not None and process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
        cleanup_windows_root()
        atexit.unregister(cleanup_windows_root)


if __name__ == "__main__":
    raise SystemExit(main())
