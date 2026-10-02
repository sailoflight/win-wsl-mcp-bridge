"""Unit tests for the installer-owned DSH registry stack supervisor."""
from __future__ import annotations

import io
import contextlib
import subprocess
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest import mock

from bridge_runtime import BridgeError
from installer import dsh_node_registry_entry as supervisor


class _FakeProcess:
    def __init__(self, argv, returncode: int = 0) -> None:
        self.argv = [str(item) for item in argv]
        self._returncode = returncode
        self.terminated = False

    def poll(self):
        return None

    def wait(self, timeout=None):
        return self._returncode

    def terminate(self) -> None:
        self.terminated = True

    def kill(self) -> None:
        self.terminated = True


class SupervisorPlanTest(unittest.TestCase):
    def test_plan_covers_owner_attach_reclaim_and_fail(self) -> None:
        self.assertEqual(supervisor._plan([], False), "own")
        self.assertEqual(supervisor._plan([], True), "own")
        self.assertEqual(supervisor._plan([8769], True), "attach")
        self.assertEqual(supervisor._plan([8768, 8769, 8770], True), "attach")
        self.assertEqual(supervisor._plan([8768, 8770], False), "fail")
        self.assertEqual(supervisor._plan([8769], False), "fail")
        # A half-dead stack: the peer node answers while the local node is gone.
        self.assertEqual(supervisor._plan([8768, 8770], False, True), "reclaim")
        self.assertEqual(supervisor._plan([8768, 8769, 8770], False, True), "reclaim")
        # An answering local node always wins over a reclaim decision.
        self.assertEqual(supervisor._plan([8768, 8770], True, True), "attach")
        # Free ports are never a reclaim: this start owns the whole stack.
        self.assertEqual(supervisor._plan([], False, True), "own")


class SupervisorPathTest(unittest.TestCase):
    def test_windows_path_renders_interop_mount_as_a_windows_path(self) -> None:
        node = Path(
            "/mnt/c/Users/example/AppData/Local/WinWslMcpBridge/"
            "runtime/Scripts/win-wsl-mcp-win.exe"
        )
        self.assertEqual(
            supervisor._windows_path(node, "registry.sqlite3"),
            "C:\\Users\\example\\AppData\\Local\\WinWslMcpBridge\\registry.sqlite3",
        )
        self.assertEqual(
            supervisor._windows_path(node, "nested", "file.txt"),
            "C:\\Users\\example\\AppData\\Local\\WinWslMcpBridge\\nested\\file.txt",
        )

    def test_windows_path_falls_back_to_a_sibling_for_non_interop_paths(self) -> None:
        node = Path("/opt/bridge/runtime/Scripts/win-wsl-mcp-win.exe")
        self.assertEqual(
            supervisor._windows_path(node, "registry.sqlite3"),
            "/opt/bridge/registry.sqlite3",
        )

    def test_discovery_prefers_the_single_installed_node(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            installed = (
                root / "alice" / supervisor.WINDOWS_INSTALL_TAIL
                / supervisor.WINDOWS_NODE_TAIL
            )
            installed.parent.mkdir(parents=True)
            installed.write_bytes(b"exe")
            with mock.patch.object(supervisor, "WINDOWS_INTEROP_ROOT", root):
                self.assertEqual(supervisor._discover_windows_node(), installed)

    def test_discovery_falls_back_when_several_profiles_are_installed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for user in ("alice", "bob"):
                installed = (
                    root / user / supervisor.WINDOWS_INSTALL_TAIL
                    / supervisor.WINDOWS_NODE_TAIL
                )
                installed.parent.mkdir(parents=True)
                installed.write_bytes(b"exe")
            with mock.patch.object(supervisor, "WINDOWS_INTEROP_ROOT", root):
                self.assertEqual(
                    supervisor._discover_windows_node(),
                    root / "Public" / supervisor.WINDOWS_INSTALL_TAIL
                    / supervisor.WINDOWS_NODE_TAIL,
                )

    def test_resolve_config_honours_explicit_overrides(self) -> None:
        config = supervisor.resolve_config({
            "XDG_DATA_HOME": "/data",
            "XDG_STATE_HOME": "/state",
            "WIN_WSL_MCP_BRIDGE_WIN_NODE": "/mnt/d/bridge/win-wsl-mcp-win.exe",
            "WIN_WSL_MCP_BRIDGE_WIN_REGISTRY": "D:\\bridge\\registry.sqlite3",
            "WIN_WSL_MCP_BRIDGE_WSL_NODE": "/opt/wsl-node",
            "WIN_WSL_MCP_BRIDGE_WSL_REGISTRY": "/state/registry.sqlite3",
            "WIN_WSL_MCP_BRIDGE_LOG_ROOT": "/state/logs",
            "WIN_WSL_MCP_BRIDGE_WIN_LOCAL_PORT": "9768",
            "WIN_WSL_MCP_BRIDGE_WSL_LOCAL_PORT": "9769",
            "WIN_WSL_MCP_BRIDGE_LINK_PORT": "9770",
        })
        self.assertEqual(config.windows_node, Path("/mnt/d/bridge/win-wsl-mcp-win.exe"))
        self.assertEqual(config.wsl_node, Path("/opt/wsl-node"))
        self.assertEqual(config.windows_registry, "D:\\bridge\\registry.sqlite3")
        self.assertEqual(config.wsl_registry, Path("/state/registry.sqlite3"))
        self.assertEqual(config.log_root, Path("/state/logs"))
        self.assertEqual(
            (config.windows_local_port, config.wsl_local_port, config.link_port),
            (9768, 9769, 9770),
        )

    def test_resolve_config_derives_the_standard_install_paths(self) -> None:
        config = supervisor.resolve_config({
            "XDG_DATA_HOME": "/data",
            "XDG_STATE_HOME": "/state",
            "WIN_WSL_MCP_BRIDGE_WIN_NODE": (
                "/mnt/c/Users/example/AppData/Local/WinWslMcpBridge/"
                "runtime/Scripts/win-wsl-mcp-win.exe"
            ),
        })
        self.assertEqual(
            config.wsl_node,
            Path("/data/win-wsl-mcp-bridge/runtime/bin/win-wsl-mcp-wsl"),
        )
        self.assertEqual(
            config.windows_registry,
            "C:\\Users\\example\\AppData\\Local\\WinWslMcpBridge\\registry.sqlite3",
        )
        self.assertEqual(
            config.wsl_registry,
            Path("/state/win-wsl-mcp-bridge/registry.sqlite3"),
        )
        self.assertEqual(
            config.log_root, Path("/state/win-wsl-mcp-bridge/logs")
        )
        self.assertEqual(
            (config.windows_local_port, config.wsl_local_port, config.link_port),
            (8768, 8769, 8770),
        )


class SupervisorRunTest(unittest.TestCase):
    def setUp(self) -> None:
        self._temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self._temporary.cleanup)
        root = Path(self._temporary.name)
        self.windows_node = root / "win-wsl-mcp-win.exe"
        self.wsl_node = root / "win-wsl-mcp-wsl"
        self.wsl_registry = root / "registry.sqlite3"
        for path in (self.windows_node, self.wsl_node, self.wsl_registry):
            path.write_bytes(b"installed")
        self.config = supervisor.SupervisorConfig(
            windows_node=self.windows_node,
            wsl_node=self.wsl_node,
            windows_registry="C:\\Bridge\\registry.sqlite3",
            wsl_registry=self.wsl_registry,
            log_root=root / "logs",
        )
        self.started: list[_FakeProcess] = []

    def _fake_popen(self, argv, *args, **kwargs):
        process = _FakeProcess(argv)
        self.started.append(process)
        return process

    def _run(self, *, port_accepts, stack_ready, peer_answers=None):
        with mock.patch.object(
            supervisor.subprocess, "Popen", self._fake_popen
        ), contextlib.redirect_stderr(io.StringIO()) as stderr:
            code = supervisor.run(
                self.config,
                port_accepts=port_accepts,
                stack_ready=stack_ready,
                peer_answers=peer_answers or (lambda: False),
            )
        return code, stderr.getvalue()

    def test_owner_start_launches_both_nodes_then_the_registry(self) -> None:
        with mock.patch.object(
            supervisor,
            "local_registry_query",
            lambda *args, **kwargs: [{"id": "onshape"}, {"id": "taobao"}],
        ):
            code, stderr = self._run(
                port_accepts=lambda _port: False, stack_ready=lambda: False,
            )
        self.assertEqual(code, 0)
        self.assertEqual(stderr, "")
        self.assertEqual(
            self.started[0].argv,
            [
                str(self.windows_node), "serve",
                "--registry", "C:\\Bridge\\registry.sqlite3",
                "--local-port", "8768",
                "--link-port", "8770",
            ],
        )
        self.assertEqual(
            self.started[1].argv,
            [
                str(self.wsl_node), "serve",
                "--registry", str(self.wsl_registry),
                "--local-port", "8769",
                "--link-port", "8770",
            ],
        )
        self.assertEqual(
            self.started[2].argv,
            [str(self.wsl_node), "registry-mcp", "--local-port", "8769"],
        )
        self.assertTrue(all(process.terminated for process in self.started))

    def test_attach_start_serves_the_registry_without_touching_the_stack(self) -> None:
        code, stderr = self._run(port_accepts=lambda _port: True, stack_ready=lambda: True)
        self.assertEqual(code, 0)
        self.assertEqual(stderr, "")
        self.assertEqual(
            [process.argv for process in self.started],
            [[str(self.wsl_node), "registry-mcp", "--local-port", "8769"]],
        )
        # The attached profile must never stop the shared stack it borrowed.
        self.assertEqual(
            [process.terminated for process in self.started], [True],
        )

    def test_occupied_but_unanswerable_ports_fail_without_starting_anything(self) -> None:
        code, stderr = self._run(port_accepts=lambda _port: True, stack_ready=lambda: False)
        self.assertEqual(code, 1)
        self.assertEqual(self.started, [])
        self.assertIn("does not answer", stderr)
        self.assertIn("8768", stderr)

    def _wsl_log_text(self) -> str:
        return (self.config.log_root / "wsl-node.log").read_text(
            encoding="utf-8", errors="replace"
        )

    def test_half_dead_stack_reclaims_by_starting_only_the_local_node(self) -> None:
        """An owner that died without reaping its Windows child must not wedge later starts."""
        with mock.patch.object(
            supervisor,
            "local_registry_query",
            lambda *args, **kwargs: [{"id": "onshape"}, {"id": "taobao"}],
        ):
            code, stderr = self._run(
                port_accepts=lambda _port: True,
                stack_ready=lambda: False,
                peer_answers=lambda: True,
            )
        self.assertEqual(code, 0)
        self.assertEqual(stderr, "")
        self.assertEqual(
            [process.argv for process in self.started],
            [
                [
                    str(self.wsl_node), "serve",
                    "--registry", str(self.wsl_registry),
                    "--local-port", "8769",
                    "--link-port", "8770",
                ],
                [str(self.wsl_node), "registry-mcp", "--local-port", "8769"],
            ],
        )
        # The surviving peer node was not started by this process, so this
        # process must never launch, replace, or stop it.
        self.assertFalse(
            any(str(self.windows_node) in process.argv for process in self.started)
        )
        self.assertTrue(all(process.terminated for process in self.started))
        self.assertIn("reclaim", self._wsl_log_text())

    def test_a_foreign_occupant_of_the_peer_port_still_fails_closed(self) -> None:
        code, stderr = self._run(
            port_accepts=lambda _port: True,
            stack_ready=lambda: False,
            peer_answers=lambda: False,
        )
        self.assertEqual(code, 1)
        self.assertEqual(self.started, [])
        self.assertIn("does not answer", stderr)

    def test_free_ports_never_probe_the_local_or_peer_node(self) -> None:
        def _unexpected(*_args, **_kwargs):
            raise AssertionError("a port-free start must not probe any node")

        with mock.patch.object(
            supervisor,
            "local_registry_query",
            lambda *args, **kwargs: [{"id": "onshape"}, {"id": "taobao"}],
        ):
            code, _stderr = self._run(
                port_accepts=lambda _port: False,
                stack_ready=_unexpected,
                peer_answers=_unexpected,
            )
        self.assertEqual(code, 0)
        self.assertEqual(len(self.started), 3)

    def test_an_answering_local_node_never_probes_the_peer(self) -> None:
        def _unexpected(*_args, **_kwargs):
            raise AssertionError("attach must not probe the peer node")

        code, _stderr = self._run(
            port_accepts=lambda _port: True,
            stack_ready=lambda: True,
            peer_answers=_unexpected,
        )
        self.assertEqual(code, 0)
        self.assertEqual(len(self.started), 1)

    def test_missing_required_paths_fail_before_any_socket_work(self) -> None:
        config = replace(self.config, windows_node=self.config.windows_node.with_suffix(".gone"))
        probes: list[int] = []

        def _port_accepts(port: int) -> bool:
            probes.append(port)
            return False

        with contextlib.redirect_stderr(io.StringIO()) as stderr:
            code = supervisor.run(config, port_accepts=_port_accepts)
        self.assertEqual(code, 1)
        self.assertEqual(probes, [])
        self.assertIn("required path is missing", stderr.getvalue())


def _raising(error: BaseException):
    def _call(*_args, **_kwargs):
        raise error

    return _call


class SupervisorPeerProbeTest(unittest.TestCase):
    """The reclaim probe must be bounded, identifying, and never fatal."""

    def setUp(self) -> None:
        self.config = supervisor.SupervisorConfig(
            windows_node=Path("/opt/bridge/win-wsl-mcp-win.exe"),
            wsl_node=Path("/opt/bridge/win-wsl-mcp-wsl"),
            windows_registry="C:\\Bridge\\registry.sqlite3",
            wsl_registry=Path("/opt/bridge/registry.sqlite3"),
            log_root=Path("/opt/bridge/logs"),
        )

    def test_a_registry_answer_identifies_the_peer_node(self) -> None:
        seen: dict[str, object] = {}

        def _answer(*args, **kwargs):
            seen["args"] = args
            return [{"id": "onshape"}, {"id": "taobao"}]

        with mock.patch.object(supervisor, "local_registry_query", _answer):
            self.assertTrue(supervisor._peer_node_answers(self.config))
        args = seen["args"]
        # The peer's own local port, asked for its own registrations.
        self.assertEqual(args[0], "127.0.0.1")
        self.assertEqual(args[1], self.config.windows_local_port)
        self.assertEqual(args[2], "local")
        self.assertEqual(args[3], "list")

    def test_any_non_answer_is_false_instead_of_fatal(self) -> None:
        for behaviour in (
            _raising(BridgeError("registry query failed")),
            _raising(ConnectionRefusedError("connection refused")),
            _raising(TimeoutError("accepted but never answered")),
            _raising(ValueError("garbage reply from a foreign listener")),
            lambda *args, **kwargs: {"ok": False},
            lambda *args, **kwargs: "not a registry list",
        ):
            with self.subTest(behaviour=behaviour):
                with mock.patch.object(supervisor, "local_registry_query", behaviour):
                    self.assertFalse(supervisor._peer_node_answers(self.config))


class _PollingProcess(_FakeProcess):
    """A child that keeps running for a fixed number of polls, then exits."""

    def __init__(self, argv, polls: int = 3, returncode: int = 0) -> None:
        super().__init__(argv, returncode)
        self.polls = polls

    def wait(self, timeout=None):
        if self.polls > 0:
            self.polls -= 1
            raise subprocess.TimeoutExpired(cmd="fake", timeout=timeout or 0)
        return self._returncode


class SupervisorAttachWatchdogTest(unittest.TestCase):
    """An attached entry must not keep serving a node that is gone."""

    def setUp(self) -> None:
        self._temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self._temporary.cleanup)
        root = Path(self._temporary.name)
        self.wsl_node = root / "win-wsl-mcp-wsl"
        self.wsl_node.write_bytes(b"installed")
        self.config = supervisor.SupervisorConfig(
            windows_node=root / "win-wsl-mcp-win.exe",
            wsl_node=self.wsl_node,
            windows_registry="C:\\Bridge\\registry.sqlite3",
            wsl_registry=root / "registry.sqlite3",
            log_root=root / "logs",
        )
        (self.config.windows_node).write_bytes(b"installed")
        self.config.wsl_registry.write_bytes(b"installed")
        self.started: list[_PollingProcess] = []

    def _fake_popen(self, argv, *args, **kwargs):
        process = _PollingProcess(argv)
        self.started.append(process)
        return process

    def _run_attach(self, *, node_answers: bool):
        with mock.patch.object(
            supervisor.subprocess, "Popen", self._fake_popen
        ), mock.patch.object(
            supervisor, "LOCAL_NODE_CHECK_SECONDS", 0.0
        ), mock.patch.object(
            supervisor, "ATTACH_WAIT_POLL_SECONDS", 0.0
        ), mock.patch.object(
            supervisor, "_stack_ready", lambda _port: node_answers
        ), contextlib.redirect_stderr(io.StringIO()) as stderr:
            code = supervisor.run(
                self.config,
                port_accepts=lambda _port: True,
                stack_ready=lambda: True,
                peer_answers=lambda: False,
            )
        return code, stderr.getvalue()

    def test_attach_exits_non_zero_when_the_borrowed_node_goes_away(self) -> None:
        code, stderr = self._run_attach(node_answers=False)
        self.assertEqual(code, 1)
        # Only registry-mcp was started, and it was torn down on the way out.
        self.assertEqual(
            [process.argv for process in self.started],
            [[str(self.wsl_node), "registry-mcp", "--local-port", "8769"]],
        )
        self.assertTrue(all(process.terminated for process in self.started))
        self.assertIn("stopped answering", stderr)

    def test_attach_keeps_serving_while_the_borrowed_node_answers(self) -> None:
        code, stderr = self._run_attach(node_answers=True)
        self.assertEqual(code, 0)
        self.assertEqual(stderr, "")
        self.assertEqual(len(self.started), 1)


if __name__ == "__main__":
    unittest.main()
