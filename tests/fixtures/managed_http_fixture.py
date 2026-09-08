#!/usr/bin/env python3
"""Test-only bridge-managed streamable-http fixture process (stdlib only).

This small loopback HTTP server models a business MCP that the Bridge owns as a
``bridge-managed`` streamable-http registration: it binds one loopback port,
answers a fixed GET readiness gate, and can shut itself down gracefully on a
fixed POST path.  It is a plain HTTP process - no MCP semantics beyond a stable
loopback endpoint - so supervision tests can observe process identity,
readiness, restart, and shutdown without depending on a real MCP server.

Usage:
    python3 managed_http_fixture.py --port 39001 [options]

Options mirror the private owner-registry supervision policy keys so tests can
drive every supervision path:

    --host HOST                 bind host (loopback only, default 127.0.0.1)
    --port PORT                 bind port (required)
    --ready-path PATH           GET readiness gate (default /ready)
    --ready-code CODE           readiness status code (default 200)
    --shutdown-path PATH        graceful POST shutdown (default /shutdown)
    --die-fast                  exit immediately with code 7 (startup failure)
    --never-ready               serve readiness gate with --ready-code forever
    --slow-ready SECONDS        serve 503 for SECONDS before serving 200

Every other GET/POST/DELETE answers a small JSON document carrying the fixture
pid so a caller can detect process generations across a supervised restart.
"""

import argparse
import json
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

READY = 200
NOT_READY = 503


def _json_bytes(value: object) -> bytes:
    return json.dumps(value, separators=(",", ":")).encode("utf-8")


def _pid_document(path: str, body_len: int) -> bytes:
    return _json_bytes(
        {
            "pid": os.getpid(),
            "path": path,
            "bodyBytes": body_len,
            "readyCode": None,
        }
    )


class _Handler(BaseHTTPRequestHandler):
    server_version = "ManagedHttpFixture/0.1"
    protocol_version = "HTTP/1.1"

    def log_message(self, *_args):  # keep test output clean
        pass

    def _respond(self, status: int, payload: bytes, headers: list[tuple[str, str]]) -> None:
        try:
            self.send_response(status)
            self.send_header("Content-Length", str(len(payload)))
            for name, value in headers:
                self.send_header(name, value)
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(payload)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def _read_body(self) -> int:
        length = self.headers.get("Content-Length")
        if length is None:
            return 0
        try:
            want = int(length)
        except ValueError:
            return 0
        if want <= 0 or want > 512 * 1024:
            return 0
        return len(self.rfile.read(want))

    def _dispatch(self) -> None:
        body_len = self._read_body()
        path = self.path.split("?", 1)[0]
        ready_path = str(self.server.ready_path)
        shutdown_path = str(self.server.shutdown_path)
        if self.command == "GET" and path == ready_path:
            now = time.monotonic()
            deadline = getattr(self.server, "slow_ready_until", 0.0)
            code = self.server.ready_code
            if deadline and now < deadline:
                code = NOT_READY
            payload = _json_bytes({"ready": code == READY, "pid": os.getpid()})
            self._respond(code, payload, [("Content-Type", "application/json")])
            return
        if self.command == "POST" and path == shutdown_path:
            self.server.shutdown_requested = True
            self._respond(202, _json_bytes({"stopping": True}), [("Content-Type", "application/json")])
            threading.Thread(target=self.server.request_shutdown, daemon=True).start()
            return
        if getattr(self.server, "die_fast", False):
            # Startup-failure probe: never binds a listener at all.
            os._exit(7)
        if path in {"/shutdown", "/"} and self.command == "GET":
            self._respond(200, _json_bytes({"pid": os.getpid()}), [("Content-Type", "application/json")])
            return
        payload = _pid_document(path, body_len)
        self._respond(200, payload, [("Content-Type", "application/json")])

    do_GET = _dispatch
    do_POST = _dispatch
    do_DELETE = _dispatch


class ManagedHttpFixtureServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, address, handler, *, ready_path, ready_code, shutdown_path):
        super().__init__(address, handler)
        self.ready_path = ready_path
        self.ready_code = int(ready_code)
        self.shutdown_path = shutdown_path
        self.shutdown_requested = False
        self.slow_ready_until = 0.0
        self.die_fast = False

    def request_shutdown(self) -> None:
        time.sleep(0.05)
        self.shutdown()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--ready-path", default="/ready")
    parser.add_argument("--ready-code", type=int, default=READY)
    parser.add_argument("--shutdown-path", default="/shutdown")
    parser.add_argument("--die-fast", action="store_true")
    parser.add_argument("--never-ready", action="store_true")
    parser.add_argument("--slow-ready", type=float, default=0.0)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.host not in {"127.0.0.1", "::1", "localhost"}:
        raise SystemExit("fixture host must be loopback")
    if args.die_fast:
        # Startup-failure probe: exit immediately without ever binding.
        os._exit(7)
    server = ManagedHttpFixtureServer(
        (args.host, args.port),
        _Handler,
        ready_path=args.ready_path,
        ready_code=args.ready_code,
        shutdown_path=args.shutdown_path,
    )
    if args.never_ready:
        server.ready_code = NOT_READY
    if args.slow_ready and args.slow_ready > 0:
        server.slow_ready_until = time.monotonic() + args.slow_ready
    server.serve_forever()
    server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
