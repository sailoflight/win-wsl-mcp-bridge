"""Installer isolation, compatibility entrypoints, and source launcher contracts."""
from pathlib import Path
import os
import subprocess
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]


class InstallerBoundaryTest(unittest.TestCase):
    def run_python(self, code, *args):
        environment = os.environ.copy()
        environment["PYTHONDONTWRITEBYTECODE"] = "1"
        result = subprocess.run(
            [sys.executable, "-B", "-c", code, *args], cwd=ROOT,
            env=environment, capture_output=True, text=True, timeout=15,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        return result

    def test_runtime_entrypoints_do_not_load_installer(self):
        # Help exercises real source entrypoint parsing without opening listeners
        # or consulting an installed registry/client configuration.
        for side in ("win", "wsl"):
            for command in ("serve", "connect", "registry-mcp", "control-mcp",
                            "deferred-mcp", "doctor", "registry-init"):
                with self.subTest(side=side, command=command):
                    self.run_python(
                        "import runpy, sys\n"
                        "sys.argv = [sys.argv[1], sys.argv[2], '--help']\n"
                        "try:\n"
                        "    runpy.run_path(sys.argv[0], run_name='__main__')\n"
                        "except SystemExit as exc:\n"
                        "    assert exc.code == 0, exc.code\n"
                        "assert not any(n == 'installer' or n.startswith('installer.') "
                        "for n in sys.modules), sorted(n for n in sys.modules if 'installer' in n)\n",
                        str(ROOT / (side + "-bridge-mcp") / "bridge.py"), command,
                    )

    def test_projection_cli_loads_installer_and_keeps_options(self):
        for side in ("win", "wsl"):
            with self.subTest(side=side):
                result = self.run_python(
                    "import runpy, sys\n"
                    "sys.argv = [sys.argv[1], 'projection', 'probe-client', '--help']\n"
                    "try:\n"
                    "    runpy.run_path(sys.argv[0], run_name='__main__')\n"
                    "except SystemExit as exc:\n"
                    "    assert exc.code == 0, exc.code\n"
                    "assert 'installer.cli' in sys.modules\n"
                    "assert 'installer.projection' in sys.modules\n",
                    str(ROOT / (side + "-bridge-mcp") / "bridge.py"),
                )
                self.assertIn("--aspect", result.stdout)
                self.assertIn("--dry-run", result.stdout)

    def test_projection_entrypoint_rejects_missing_or_unknown_subcommands(self):
        for side in ("win", "wsl"):
            for args in (["projection"], ["projection", "unknown-subcommand"]):
                with self.subTest(side=side, args=args):
                    result = subprocess.run(
                        [sys.executable, "-B", str(ROOT / (side + "-bridge-mcp") / "bridge.py"), *args],
                        cwd=ROOT, capture_output=True, text=True, timeout=15,
                    )
                    self.assertEqual(result.returncode, 2, result.stderr)
                    self.assertNotIn("Traceback", result.stderr)
                    self.assertIn("projection", result.stderr)

    def test_probe_compatibility_module_preserves_mutable_identity(self):
        self.run_python(
            "import harness_verification as old\n"
            "from installer import harness_verification as new\n"
            "assert old is new\n"
            "old.MAX_RUNTIME_SECONDS = 1.0\n"
            "assert new.MAX_RUNTIME_SECONDS == 1.0\n"
            "assert old._Fixture is new._Fixture\n"
        )

    def test_probe_source_and_module_entrypoints(self):
        for command in (
            [str(ROOT / "harness_verification.py")],
            [str(ROOT / "installer" / "harness_verification.py")],
            ["-m", "harness_verification"],
            ["-m", "installer.harness_verification"],
        ):
            with self.subTest(command=command):
                result = subprocess.run(
                    [sys.executable, "-B", *command, "--help"], cwd=ROOT,
                    capture_output=True, text=True, timeout=15,
                )
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn("--receipt", result.stdout)

    def test_source_launcher_still_resolves_component_directory(self):
        from installer.projection import _default_launcher, _validate_launcher
        for side in ("win", "wsl"):
            command, args = _default_launcher(side)
            self.assertEqual(command, sys.executable)
            self.assertEqual(args, [str(ROOT / (side + "-bridge-mcp") / "bridge.py")])
            _validate_launcher(side, command, args)


if __name__ == "__main__":
    unittest.main()
