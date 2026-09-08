"""Probe facade-selected installed proxies using only a disposable loopback node.

Run with the target interpreter and ``-I`` so neither this script's directory nor
an ambient PYTHONPATH supplies runtime modules. No package installation occurs.
"""

from __future__ import annotations

import json
import socket
import subprocess
import sys
import threading

import bridge_runtime
import stdio_http_facade as facade


def probe(side: str, era: str) -> dict[str, object]:
    target = "installed-facade-fixture"
    method = "initialize" if era == "legacy" else "server/discover"
    request = {"jsonrpc": "2.0", "id": 1, "method": method, "params": {}}
    if era == "modern":
        request["params"] = {"_meta": {
            "io.modelcontextprotocol/protocolVersion": "2026-07-28",
            "io.modelcontextprotocol/clientCapabilities": {},
        }}
    expected = {"jsonrpc": "2.0", "id": 1, "result": {"fixture": side + "/" + era}}
    observed = []
    errors = []
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        listener.settimeout(10)
        port = listener.getsockname()[1]
        options = facade.FacadeOptions(side=side, target=target, node_port=port,
                                       protocol_era=era)
        assert options.backend_command is None
        if era == "modern":
            try:
                facade.FacadeOptions(side=side, target=target, node_port=port,
                                     protocol_era=era, backend_command=["arbitrary"])
            except facade.FacadeError:
                pass
            else:
                raise AssertionError("modern facade accepted a command override")

        def serve() -> None:
            try:
                connection, _ = listener.accept()
                with connection, connection.makefile("rb") as reader:
                    connection.settimeout(10)
                    observed.append(json.loads(reader.readline()))
                    connection.sendall(json.dumps({
                        "ok": True, "stream": "installed-fixture-stream",
                        "coreVersion": bridge_runtime.SERVER_VERSION,
                    }).encode() + b"\n")
                    for line in reader:
                        observed.append(json.loads(line))
                        connection.sendall(json.dumps(expected).encode() + b"\n")
            except Exception as exc:
                errors.append(repr(exc))

        worker = threading.Thread(target=serve, daemon=True)
        worker.start()
        argv = facade.default_backend_command(side, "127.0.0.1", port, target)
        result = subprocess.run(argv, input=json.dumps(request) + "\n", text=True,
                                capture_output=True, timeout=15)
        worker.join(timeout=11)
        assert not worker.is_alive(), "fixture node did not finish"
        assert not errors, errors
        assert result.returncode == 0, result.stderr
        assert [json.loads(line) for line in result.stdout.splitlines()] == [expected], result.stdout
        assert len(observed) == 2, observed
        assert observed[0]["op"] == "connect", observed
        assert observed[0]["target"] == target, observed
        assert set(observed[0]) <= {"op", "target", "connectorVersion"}, observed
        assert observed[1] == request, observed
    return {"side": side, "era": era, "proxyRoundTrip": True}


if __name__ == "__main__":
    print(json.dumps([probe(side, era) for side in ("win", "wsl")
                      for era in ("legacy", "modern")]))
