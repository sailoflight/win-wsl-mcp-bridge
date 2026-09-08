"""Install one local wheel into a temporary environment and smoke both entrypoints.

No package index, production environment or existing installation is used.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import venv


def smoke_install(wheel: Path) -> dict[str, object]:
    wheel = wheel.resolve(strict=True)
    environment = os.environ.copy()
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    environment.pop("PYTHONPATH", None)
    with tempfile.TemporaryDirectory(prefix="bridge-wheel-acceptance-") as temporary:
        root = Path(temporary)
        target = root / "venv"
        venv.EnvBuilder(with_pip=True).create(target)
        scripts = target / ("Scripts" if os.name == "nt" else "bin")
        python = scripts / ("python.exe" if os.name == "nt" else "python")
        install = subprocess.run(
            [str(python), "-m", "pip", "install", "--no-index", "--no-deps",
             "--disable-pip-version-check", str(wheel)],
            cwd=root, env=environment, capture_output=True, text=True, timeout=60,
        )
        if install.returncode:
            raise RuntimeError("isolated wheel installation failed: " + install.stderr[-1000:])
        versions = {}
        for name in ("win-wsl-mcp-win", "win-wsl-mcp-wsl"):
            executable = scripts / (name + (".exe" if os.name == "nt" else ""))
            result = subprocess.run([str(executable), "--version"], cwd=root,
                                    env=environment, capture_output=True, text=True, timeout=15)
            if result.returncode or not result.stdout.strip():
                raise RuntimeError("installed console smoke failed: " + name)
            versions[name] = result.stdout.strip()
        result = subprocess.run(
            [str(python), "-I", "-c", "import json, pathlib, bridge_runtime; "
             "print(json.dumps({'version': bridge_runtime.SERVER_VERSION, "
             "'installed': 'site-packages' in pathlib.Path(bridge_runtime.__file__).parts}))"],
            cwd=root, env=environment, capture_output=True, text=True, timeout=15,
        )
        if result.returncode:
            raise RuntimeError("installed runtime import failed")
        runtime = json.loads(result.stdout)
        if not runtime["installed"] or not all(runtime["version"] in value for value in versions.values()):
            raise RuntimeError("installed runtime identity/version mismatch")
        return {"ok": True, "isolatedInstall": True, "runtimeVersion": runtime["version"],
                "entrypoints": versions, "temporaryEnvironmentRemovedOnReturn": True}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("wheel", type=Path)
    args = parser.parse_args()
    print(json.dumps(smoke_install(args.wheel), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
