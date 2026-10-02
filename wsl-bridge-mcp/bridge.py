#!/usr/bin/env python3
"""WSL-side launcher for the shared bridge runtime."""

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# The repository tree is a mutable working copy, not an installed package
# directory, so never leave bytecode caches in it. A client that spawns this
# launcher without PYTHONDONTWRITEBYTECODE — Claude Code's own MCP health check
# does exactly that — would otherwise drop __pycache__ next to the runtime
# modules and trip the repository-layout gate.
sys.dont_write_bytecode = True

from bridge_runtime import wsl_main


if __name__ == "__main__":
    raise SystemExit(wsl_main())
