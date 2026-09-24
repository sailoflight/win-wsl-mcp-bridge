"""Compatibility entrypoint for the installer-owned client capability probe.

New code imports installer.harness_verification. Existing ``python -m
harness_verification`` commands and source-file launchers remain supported.
"""
import sys
from installer import harness_verification as _implementation

if __name__ == "__main__":
    raise SystemExit(_implementation.main())

# Preserve module identity, including callers that tune fixture bounds or patch
# helpers. Copying exports would leave functions reading a different namespace.
sys.modules[__name__] = _implementation
