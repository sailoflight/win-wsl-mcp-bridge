#!/usr/bin/env python3
"""Focused tests for connector engine scanning, hashing, and rollback.

These cover the immutable manifest+hash engine-directory loader in
``connector_core.load_engine`` (newest verified version wins, rollback to the
next candidate and finally to the built-in engine).  The JSON-RPC-aware
recovery engine itself and the persistent-connector wire behavior are covered
in ``test_bridge.py``.
"""

from __future__ import annotations

import hashlib
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from connector_core import ConnectorError, load_engine  # noqa: E402


def _engine_source(name: str, version: str, core_version: str) -> str:
    """A minimal loadable engine module (never executed by these tests)."""
    return "\n".join(
        [
            f"ENGINE_NAME = {name!r}",
            f"ENGINE_VERSION = {version!r}",
            "",
            "class Recovery:",
            f"    CORE_VERSION = {core_version!r}",
            "",
            "def make_recovery(core_version=None):",
            "    return None",
            "",
        ]
    ) + "\n"


def _write_bundle(
    directory: Path,
    *,
    name: str,
    version: str,
    core_version: str,
    module_name: str | None = None,
    tamper: bool = False,
    declared_version: str | None = None,
    declared_core: str | None = None,
) -> tuple[Path, Path]:
    """Write one immutable engine bundle (module + ``.engine.json`` manifest).

    Returns ``(module_path, manifest_path)``.  ``tamper`` flips one byte of the
    module after the manifest digest was computed so the on-disk digest no
    longer matches.  ``declared_*`` override the module's own version
    constants, which then disagree with the manifest.
    """
    module = directory / f"{name}-{version}.py"
    pristine = _engine_source(
        name,
        declared_version if declared_version is not None else version,
        declared_core if declared_core is not None else core_version,
    )
    digest = hashlib.sha256(pristine.encode("utf-8")).hexdigest()
    module.write_text(pristine, encoding="utf-8")
    if tamper:
        data = bytearray(module.read_bytes())
        data[0] = ord("\n") if data[0] != ord("\n") else ord("#")
        module.write_bytes(bytes(data))
    manifest = directory / f"{name}-{version}.engine.json"
    manifest.write_text(
        json.dumps(
            {
                "module": module_name if module_name is not None else module.name,
                "version": version,
                "coreVersion": core_version,
                "sha256": digest,
            },
            separators=(",", ":"),
        ),
        encoding="utf-8",
    )
    return module, manifest


class EngineScanTest(unittest.TestCase):
    def test_scan_picks_newest_verified_engine(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            _write_bundle(
                directory, name="engine-a", version="1.0.0", core_version="0.4.0"
            )
            _write_bundle(
                directory, name="engine-b", version="2.3.1", core_version="0.4.0"
            )
            engine = load_engine(engine_dir=directory)
            self.assertEqual(engine.ENGINE_NAME, "engine-b")
            self.assertEqual(engine.ENGINE_VERSION, "2.3.1")
            self.assertEqual(engine.Recovery.CORE_VERSION, "0.4.0")

    def test_scan_rolls_back_when_newest_engine_is_tampered(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            _write_bundle(
                directory,
                name="engine-old",
                version="1.0.0",
                core_version="0.4.0",
            )
            _write_bundle(
                directory,
                name="engine-new",
                version="9.0.0",
                core_version="0.9.0",
                tamper=True,
            )
            engine = load_engine(engine_dir=directory)
            self.assertEqual(engine.ENGINE_NAME, "engine-old")
            self.assertEqual(engine.ENGINE_VERSION, "1.0.0")

    def test_scan_rejects_self_declared_version_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            _write_bundle(
                directory,
                name="engine-bad",
                version="1.0.0",
                core_version="0.4.0",
                declared_version="2.0.0",
            )
            engine = load_engine(engine_dir=directory)
            # The only candidate disagrees with its manifest: built-in fallback.
            self.assertEqual(engine.ENGINE_NAME, "connector-engine")
            self.assertEqual(engine.ENGINE_VERSION, "1.0.0")

    def test_scan_rejects_module_escaping_the_engine_directory(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            directory = root / "engines"
            directory.mkdir()
            outside = root / "outside.py"
            outside.write_text(
                _engine_source("engine-escape", "1.0.0", "0.4.0"), encoding="utf-8"
            )
            (directory / "engine-escape-1.0.0.engine.json").write_text(
                json.dumps(
                    {
                        "module": "../outside.py",
                        "version": "1.0.0",
                        "coreVersion": "0.4.0",
                        "sha256": hashlib.sha256(outside.read_bytes()).hexdigest(),
                    },
                    separators=(",", ":"),
                ),
                encoding="utf-8",
            )
            engine = load_engine(engine_dir=directory)
            self.assertEqual(engine.ENGINE_NAME, "connector-engine")

    def test_scan_rejects_invalid_manifest_fields(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            _write_bundle(
                directory, name="engine-x", version="1.0.0", core_version="0.4.0"
            )
            (directory / "broken.engine.json").write_text(
                json.dumps(
                    {"module": "does-not-exist.py", "version": "not-a-version"},
                    separators=(",", ":"),
                ),
                encoding="utf-8",
            )
            engine = load_engine(engine_dir=directory)
            # The valid bundle still wins; the broken manifest is ignored.
            self.assertEqual(engine.ENGINE_NAME, "engine-x")
            self.assertEqual(engine.ENGINE_VERSION, "1.0.0")

    def test_scan_all_candidates_rejected_falls_back_to_builtin(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            _write_bundle(
                directory,
                name="engine-only",
                version="1.0.0",
                core_version="0.4.0",
                tamper=True,
            )
            engine = load_engine(engine_dir=directory)
            self.assertEqual(engine.ENGINE_NAME, "connector-engine")

    def test_missing_engine_directory_falls_back_to_builtin(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            missing = Path(temp) / "no-such-engine-dir"
            engine = load_engine(engine_dir=missing)
            self.assertEqual(engine.ENGINE_NAME, "connector-engine")

    def test_engine_directory_environment_variable_is_scanned(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            _write_bundle(
                directory,
                name="engine-env",
                version="4.2.0",
                core_version="0.4.0",
            )
            with mock.patch.dict(
                os.environ,
                {"WIN_WSL_MCP_BRIDGE_CONNECTOR_ENGINE_DIR": str(directory)},
                clear=False,
            ):
                engine = load_engine()
            self.assertEqual(engine.ENGINE_NAME, "engine-env")
            self.assertEqual(engine.ENGINE_VERSION, "4.2.0")

    def test_explicit_engine_file_takes_precedence_over_engine_directory(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            directory = root / "engines"
            directory.mkdir()
            _write_bundle(
                directory,
                name="engine-high",
                version="9.0.0",
                core_version="0.4.0",
            )
            explicit = root / "explicit.py"
            explicit.write_text(
                _engine_source("engine-explicit", "1.0.0", "0.4.0"), encoding="utf-8"
            )
            with mock.patch.dict(
                os.environ,
                {"WIN_WSL_MCP_BRIDGE_CONNECTOR_ENGINE_DIR": str(directory)},
                clear=False,
            ):
                engine = load_engine(explicit)
            self.assertEqual(engine.ENGINE_NAME, "engine-explicit")
            self.assertEqual(engine.ENGINE_VERSION, "1.0.0")

    def test_explicit_broken_engine_still_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            _write_bundle(
                directory,
                name="engine-good",
                version="9.0.0",
                core_version="0.4.0",
            )
            broken = directory / "broken.py"
            broken.write_text("not valid python !!!\n", encoding="utf-8")
            with self.assertRaises(ConnectorError):
                load_engine(broken)

    def test_identical_basenames_in_different_directories_do_not_collide(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            first = root / "engines-a"
            second = root / "engines-b"
            first.mkdir()
            second.mkdir()
            _write_bundle(
                first, name="engine", version="1.0.0", core_version="0.4.0"
            )
            _write_bundle(
                second, name="engine", version="2.0.0", core_version="0.4.0"
            )
            engine_a = load_engine(engine_dir=first)
            engine_b = load_engine(engine_dir=second)
            self.assertEqual(
                (engine_a.ENGINE_NAME, engine_a.ENGINE_VERSION), ("engine", "1.0.0")
            )
            self.assertEqual(
                (engine_b.ENGINE_NAME, engine_b.ENGINE_VERSION), ("engine", "2.0.0")
            )


if __name__ == "__main__":
    unittest.main(verbosity=2)
