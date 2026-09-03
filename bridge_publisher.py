#!/usr/bin/env python3
"""Installed-mode artifact publisher used by launched business MCPs."""

from __future__ import annotations

import argparse
import json
import os
import sys

from bridge_runtime import BridgeError, publish_artifact


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Publish one bridge-staged artifact to the peer workspace"
    )
    parser.add_argument("command", choices=["publish"])
    parser.add_argument("relative_path")
    parser.add_argument("--name")
    parser.add_argument("--media-type")
    parser.add_argument(
        "--local-host",
        default=os.environ.get("WIN_WSL_MCP_BRIDGE_ARTIFACT_LOCAL_HOST", "127.0.0.1"),
    )
    parser.add_argument(
        "--local-port",
        type=int,
        default=int(os.environ.get("WIN_WSL_MCP_BRIDGE_ARTIFACT_LOCAL_PORT", "0")),
    )
    parser.add_argument(
        "--token",
        default=os.environ.get("WIN_WSL_MCP_BRIDGE_ARTIFACT_TOKEN"),
    )
    args = parser.parse_args()
    if not args.token or not 1 <= args.local_port <= 65535:
        print("artifact publisher: missing bridge session configuration", file=sys.stderr)
        return 2
    try:
        result = publish_artifact(
            args.local_host,
            args.local_port,
            args.token,
            args.relative_path,
            args.name,
            args.media_type,
        )
    except (BridgeError, OSError, ValueError) as exc:
        print(f"artifact publisher: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, ensure_ascii=False, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
