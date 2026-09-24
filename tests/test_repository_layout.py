"""Repository layout and release-source consistency gates."""

from pathlib import Path
import ast
import re
import unittest
import tomllib

ROOT = Path(__file__).resolve().parents[1]


class RepositoryLayoutTest(unittest.TestCase):
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
        for link in re.findall(r"\]\(([^)]+)\)", index.read_text()):
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


if __name__ == "__main__":
    unittest.main()
