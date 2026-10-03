"""Repository layout and release-source consistency gates."""

from pathlib import Path
import ast
import os
import re
import shutil
import subprocess
import sys
import unittest
import tomllib

ROOT = Path(__file__).resolve().parents[1]


class RepositoryLayoutTest(unittest.TestCase):
    def test_launchers_never_leave_bytecode_in_the_repository(self):
        """Spawning a launcher without PYTHONDONTWRITEBYTECODE must not produce
        bytecode here.

        The two runtime component directories are a working copy, not an
        installed package directory, and a client that spawns the launcher on
        its own (Claude Code's MCP health check) controls that environment.
        """
        if (ROOT / "__pycache__").exists():
            self.skipTest("the repository already carries a root __pycache__")
        # Registered before the checks: cleanups run even when an assertion
        # fails, and this removes only what this test's own subprocesses made.
        self.addCleanup(shutil.rmtree, ROOT / "__pycache__", True)
        environment = {
            key: value
            for key, value in os.environ.items()
            if key != "PYTHONDONTWRITEBYTECODE"
        }
        for launcher in ("wsl-bridge-mcp", "win-bridge-mcp"):
            with self.subTest(launcher=launcher):
                subprocess.run(
                    [sys.executable, str(ROOT / launcher / "bridge.py"), "--help"],
                    cwd=ROOT,
                    env=environment,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    check=False,
                )
                self.assertFalse(
                    (ROOT / "__pycache__").exists(),
                    f"{launcher} wrote bytecode into the repository",
                )

    def test_root_python_files_are_exactly_declared_runtime(self):
        project = tomllib.loads((ROOT / "pyproject.toml").read_text())
        modules = project["tool"]["setuptools"]["py-modules"]
        self.assertEqual(len(modules), len(set(modules)))
        self.assertEqual({path.stem for path in ROOT.glob("*.py")}, set(modules))
        self.assertEqual(project["project"]["dependencies"], [])

    def test_installer_package_is_explicitly_shipped(self):
        project = tomllib.loads((ROOT / "pyproject.toml").read_text())
        self.assertEqual(project["tool"]["setuptools"]["packages"], ["installer"])
        self.assertTrue((ROOT / "installer" / "__init__.py").is_file())
        self.assertIn("recursive-include installer", (ROOT / "MANIFEST.in").read_text())

    def test_runtime_does_not_import_development_tests(self):
        for path in list(ROOT.glob("*.py")) + list((ROOT / "installer").rglob("*.py")):
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            for node in ast.walk(tree):
                names = []
                if isinstance(node, ast.Import):
                    names = [alias.name for alias in node.names]
                elif isinstance(node, ast.ImportFrom) and node.module:
                    names = [node.module]
                for name in names:
                    self.assertFalse(name == "tests" or name.startswith("tests."), str(path))

    def test_canonical_document_index_links_exist(self):
        index = ROOT / "docs" / "INDEX.md"
        for link in re.findall(r"\]\(([^)]+)\)", index.read_text(encoding="utf-8")):
            self.assertTrue((index.parent / link).is_file(), link)
        for name in ("ARCHITECTURE", "DEVELOPMENT_PLAN", "VERIFICATION", "DEPLOYMENT",
                     "MCP_COVERAGE", "IMPLEMENTATION_STATUS", "MILESTONE_DESIGN"):
            self.assertTrue((ROOT / "docs" / (name + ".md")).is_file())
            self.assertFalse((ROOT / (name + ".md")).exists())

    def test_fixture_files_are_development_only(self):
        fixtures = ROOT / "tests" / "fixtures"
        for name in ("fixture_mcp.py", "shared_fixture_mcp.py", "managed_http_fixture.py",
                     "protocol_fixture_mcp.py"):
            self.assertTrue((fixtures / name).is_file(), name)
            self.assertFalse((ROOT / name).exists(), name)
        manifest = (ROOT / "MANIFEST.in").read_text()
        self.assertIn("recursive-include tests", manifest)
        self.assertIn("recursive-include docs", manifest)


    def test_every_windows_child_spawn_is_console_free(self):
        source = (ROOT / "bridge_runtime.py").read_text(encoding="utf-8")
        # The single bare read of the process-group flag is the helper itself;
        # every spawn site must go through it so no child can allocate a console
        # window of its own, and the helper is defined plus four call sites.
        self.assertEqual(
            source.count('getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)'), 1
        )
        self.assertEqual(source.count('getattr(subprocess, "CREATE_NO_WINDOW", 0)'), 1)
        self.assertEqual(source.count("_windows_child_creation_flags()"), 5)


if __name__ == "__main__":
    unittest.main()
