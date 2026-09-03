#!/usr/bin/env python3
"""WSL-side launcher for the shared bridge runtime."""

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from bridge_runtime import wsl_main


if __name__ == "__main__":
    raise SystemExit(wsl_main())
