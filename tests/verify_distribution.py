"""Verify built Bridge wheel/sdist inventories against their authoritative sources.

Run after a build. This reads archive members without extracting any file and
rejects stale runtime bytes, missing test fixtures, or unexpected wheel code.
"""

from __future__ import annotations

import argparse
from pathlib import Path
import tarfile
import tomllib
import zipfile


def verify_distributions(root: Path, wheel: Path, sdist: Path) -> dict[str, int]:
    project = tomllib.loads((root / "pyproject.toml").read_text(encoding="utf-8"))
    runtime = {name + ".py" for name in project["tool"]["setuptools"]["py-modules"]}
    for package in project["tool"]["setuptools"].get("packages", []):
        directory = root / package.replace(".", "/")
        runtime.update(path.relative_to(root).as_posix() for path in directory.glob("*.py"))
    with zipfile.ZipFile(wheel) as archive:
        names = archive.namelist()
        if len(names) != len(set(names)):
            raise ValueError("wheel contains duplicate members")
        code = {name for name in names if name.endswith(".py")}
        if code != runtime:
            raise ValueError("wheel code does not match declared runtime modules")
        for name in runtime:
            if archive.read(name) != (root / name).read_bytes():
                raise ValueError("wheel contains stale runtime bytes: " + name)
        if any(name.startswith(("tests/", "docs/", "fixture_", "test_")) for name in names):
            raise ValueError("wheel unexpectedly includes development sources")
    expected = runtime | {"README.md", "AGENTS.md", "pyproject.toml", "MANIFEST.in"}
    for directory in ("docs", "tests", "installer", "win-bridge-mcp", "wsl-bridge-mcp"):
        for path in (root / directory).rglob("*"):
            if path.is_file() and path.suffix in {".py", ".md", ".json", ".txt"}:
                expected.add(path.relative_to(root).as_posix())
    with tarfile.open(sdist, "r:gz") as archive:
        members = archive.getmembers()
        prefixes = {member.name.split("/", 1)[0] for member in members}
        if len(prefixes) != 1:
            raise ValueError("sdist must have one package root")
        prefix = next(iter(prefixes)) + "/"
        files = {member.name[len(prefix):]: member for member in members if member.isfile()}
        for name in expected:
            member = files.get(name)
            if member is None:
                raise ValueError("sdist is missing source: " + name)
            handle = archive.extractfile(member)
            if handle is None or handle.read() != (root / name).read_bytes():
                raise ValueError("sdist contains stale source bytes: " + name)
        if any("/.venv" in member.name or "__pycache__" in member.name for member in members):
            raise ValueError("sdist contains local environment/cache files")
    return {"runtimeModules": len(runtime), "verifiedSourceFiles": len(expected)}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--wheel", type=Path, required=True)
    parser.add_argument("--sdist", type=Path, required=True)
    args = parser.parse_args()
    try:
        result = verify_distributions(args.root.resolve(), args.wheel, args.sdist)
    except (OSError, ValueError, tarfile.TarError, zipfile.BadZipFile) as exc:
        parser.exit(1, "distribution verification failed: " + str(exc) + "\n")
    print(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
