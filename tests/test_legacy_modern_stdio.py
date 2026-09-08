#!/usr/bin/env python3
"""Tests for the reverse-era projection ``legacy_modern_stdio.py``.

The adapter is exercised as a REAL stdio subprocess: each test session spawns
``legacy_modern_stdio.py`` (the legacy-facing server under test, at the
repository root) which itself spawns ``tests/fixtures/modern_only_fixture_mcp.py``
(a genuine modern-only 2026-07-28 backend subprocess) on first initialize.  A
small in-process "legacy Agent" client drives the adapter over its
stdin/stdout pipes and asserts the frozen legacy profiles, envelope
stripping, error normalization, serialization, progress relay, cancellation
mapping, queued-cancellation purging, bounded timeouts and no-replay
guarantees.

Run from the repository root:

    PYTHONDONTWRITEBYTECODE=1 python3 -m unittest -v tests.test_legacy_modern_stdio
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from collections import deque
from pathlib import Path

# Repository root: this test lives in tests/, one level below the root.  The
# runtime adapter stays at the repository root; its fixture backend lives in
# tests/fixtures/ (both are spawned as plain subprocesses with sys.executable).
ROOT = Path(__file__).resolve().parents[1]
ADAPTER = ROOT / "legacy_modern_stdio.py"
FIXTURE = ROOT / "tests" / "fixtures" / "modern_only_fixture_mcp.py"

ADAPTER_SERVER_INFO = {"name": "legacy-modern-stdio", "version": "0.1.0"}
LEGACY_VERSIONS = ("2024-11-05", "2025-03-26", "2025-06-18", "2025-11-25")
MODERN = "2026-07-28"

# The adapter module is imported only for its frozen pure wrapper gates
# (_LegacyProfile.project_call_result); importing it starts no subprocess.
sys.path.insert(0, str(ROOT))
import legacy_modern_stdio as stdio_mod  # noqa: E402


def read_jsonl_lines(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def _initialize_request(version: str, request_id: int) -> dict:
    return {
        "jsonrpc": "2.0",
        "id": request_id,
        "method": "initialize",
        "params": {
            "protocolVersion": version,
            "capabilities": {},
            "clientInfo": {"name": "test-legacy-agent", "version": "1.0"},
        },
    }


class AdapterSession:
    """One adapter subprocess plus a thread-safe frame stream."""

    def __init__(self, proc: subprocess.Popen, state_path: Path, config_path: Path) -> None:
        self.proc = proc
        self.state_path = state_path
        self.config_path = config_path
        self._frames: deque = deque()
        self._side_frames: list[dict] = []
        self._condition = threading.Condition()
        self._reader = threading.Thread(target=self._read_loop, daemon=True)
        self._reader.start()

    def _read_loop(self) -> None:
        assert self.proc.stdout is not None
        for raw in self.proc.stdout:
            raw = raw.strip()
            if not raw:
                continue
            try:
                frame = json.loads(raw)
            except ValueError:
                continue
            with self._condition:
                self._frames.append(frame)
                self._condition.notify_all()
        with self._condition:
            self._condition.notify_all()

    def send_raw(self, text: str) -> None:
        """Write raw bytes/text straight to the adapter's stdin (no newline
        appended): used to send oversized or malformed frames verbatim."""
        assert self.proc.stdin is not None
        self.proc.stdin.write(text)
        self.proc.stdin.flush()

    def send_line(self, message: dict) -> None:
        assert self.proc.stdin is not None
        self.proc.stdin.write(json.dumps(message, separators=(",", ":")) + "\n")
        self.proc.stdin.flush()

    def _pop_frame(self, timeout: float) -> dict | None:
        deadline = time.monotonic() + timeout
        with self._condition:
            while not self._frames and time.monotonic() < deadline:
                self._condition.wait(0.1)
            if not self._frames:
                return None
            return self._frames.popleft()

    def read_until_id(self, request_id, timeout: float = 20.0) -> dict:
        """Read frames until the response for ``request_id`` arrives.

        Non-matching frames (progress notifications etc.) are preserved on
        ``side_frames`` for later assertions.
        """
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            frame = self._pop_frame(deadline - time.monotonic())
            if frame is None:
                break
            if frame.get("id") == request_id:
                return frame
            self._side_frames.append(frame)
        raise AssertionError(f"no response for request id {request_id!r} within {timeout}s")

    def read_until(self, predicate, timeout: float = 20.0) -> dict:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            frame = self._pop_frame(deadline - time.monotonic())
            if frame is None:
                break
            if predicate(frame):
                return frame
            self._side_frames.append(frame)
        raise AssertionError(f"no frame matching predicate within {timeout}s")

    @property
    def side_frames(self) -> list[dict]:
        return list(self._side_frames)

    def initialize(self, version: str, request_id: int = 1) -> dict:
        self.send_line(_initialize_request(version, request_id))
        return self.read_until_id(request_id)

    def tools_list(self, request_id: int) -> dict:
        self.send_line(
            {"jsonrpc": "2.0", "id": request_id, "method": "tools/list", "params": {}}
        )
        return self.read_until_id(request_id)

    def tools_call(self, name: str, arguments: dict, request_id: int, meta: dict | None = None) -> dict:
        params: dict = {"name": name, "arguments": arguments}
        if meta is not None:
            params["_meta"] = meta
        self.send_line(
            {"jsonrpc": "2.0", "id": request_id, "method": "tools/call", "params": params}
        )
        return self.read_until_id(request_id)

    def send_cancel(self, request_id: Any, reason: str = "cancelled") -> None:
        params: dict = {"requestId": request_id}
        if reason is not None:
            params["reason"] = reason
        self.send_line(
            {"jsonrpc": "2.0", "method": "notifications/cancelled", "params": params}
        )

    def drain(self, timeout: float = 0.8) -> list[dict]:
        """Collect any frames that arrive within ``timeout`` (non-blocking
        beyond it).  Used to prove no response/notification appeared."""
        frames: list[dict] = []
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            frame = self._pop_frame(max(0.05, deadline - time.monotonic()))
            if frame is None:
                break
            frames.append(frame)
        return frames

    def events(self) -> list[dict]:
        return read_jsonl_lines(self.state_path)

    def count_events(self, *, method: str | None = None, event: str | None = None) -> int:
        count = 0
        for item in self.events():
            if event is not None and item.get("event") != event:
                continue
            if method is not None and item.get("method") != method:
                continue
            count += 1
        return count

    def close(self) -> None:
        if self.proc.poll() is None and self.proc.stdin is not None:
            try:
                self.proc.stdin.close()
            except OSError:
                pass
        try:
            self.proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                pass
        for stream in (self.proc.stdin, self.proc.stdout, self.proc.stderr):
            if stream is not None:
                try:
                    stream.close()
                except OSError:
                    pass


def start_session(
    fixture_config: dict | None = None,
    *,
    startup_timeout: float | None = None,
    request_timeout: float | None = None,
    max_frame_bytes: int | None = None,
    queue_capacity: int | None = None,
) -> AdapterSession:
    """Launch one adapter+backend session in a fresh temporary directory."""
    tmp = Path(tempfile.mkdtemp(prefix="lms-"))
    state_path = tmp / "state.jsonl"
    config_path = tmp / "config.json"
    config_path.write_text(json.dumps(fixture_config or {}, separators=(",", ":")))
    environment = os.environ.copy()
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    proc = subprocess.Popen(
        [
            sys.executable,
            str(ADAPTER),
            *(("--startup-timeout-seconds", str(startup_timeout)) if startup_timeout else ()),
            *(("--request-timeout-seconds", str(request_timeout)) if request_timeout else ()),
            *(("--max-frame-bytes", str(max_frame_bytes)) if max_frame_bytes else ()),
            *(("--queue-capacity", str(queue_capacity)) if queue_capacity else ()),
            "--backend-command",
            sys.executable,
            "--backend-args",
            str(FIXTURE),
            "--state",
            str(state_path),
            "--config",
            str(config_path),
        ],
        cwd=ROOT,
        env=environment,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
    )
    session = AdapterSession(proc, state_path, config_path)
    session._tmp = tmp
    return session


class LegacyModernStdioTest(unittest.TestCase):
    def tearDown(self) -> None:
        session = getattr(self, "_session", None)
        if session is not None:
            session.close()
            self._session = None

    def _new_session(
        self,
        fixture_config: dict | None = None,
        *,
        startup_timeout: float | None = None,
        request_timeout: float | None = None,
        max_frame_bytes: int | None = None,
        queue_capacity: int | None = None,
    ) -> AdapterSession:
        self._session = start_session(
            fixture_config,
            startup_timeout=startup_timeout,
            request_timeout=request_timeout,
            max_frame_bytes=max_frame_bytes,
            queue_capacity=queue_capacity,
        )
        return self._session

    # -- initialize / profiles ------------------------------------------

    def test_initialize_all_four_profiles_preserve_instructions_and_identity(self) -> None:
        session = self._new_session(
            {"instructions": "modern-fixture-instructions", "list_changed": True}
        )
        for index, version in enumerate(LEGACY_VERSIONS, start=1):
            response = session.initialize(version, request_id=index)
            self.assertNotIn("error", response, response)
            result = response["result"]
            # Exact negotiated identity: never the downstream serverInfo.
            self.assertEqual(result["serverInfo"], ADAPTER_SERVER_INFO)
            self.assertEqual(result["protocolVersion"], version)
            # Tools-only capability intersection, no invented families.
            self.assertEqual(result["capabilities"], {"tools": {"listChanged": True}})
            # Downstream instructions preserved verbatim on every profile.
            self.assertEqual(result["instructions"], "modern-fixture-instructions")
        # One bootstrap discover serves every subsequent initialize.
        self.assertEqual(session.count_events(method="server/discover"), 1)
        self.assertEqual(session.count_events(event="recv", method="initialize"), 0)

    def test_initialize_capability_intersection_no_list_changed(self) -> None:
        session = self._new_session({"instructions": "", "list_changed": False})
        response = session.initialize("2025-11-25")
        self.assertNotIn("error", response, response)
        result = response["result"]
        self.assertEqual(result["capabilities"], {"tools": {"listChanged": False}})
        self.assertNotIn("instructions", result)  # backend supplied none

    def test_version_negotiation_downgrade_and_clean_reject(self) -> None:
        session = self._new_session()
        downgraded = session.initialize("2026-02-14", request_id=10)
        self.assertNotIn("error", downgraded)
        # Newer well-formed unverified revision: negotiated down to canonical.
        self.assertEqual(downgraded["result"]["protocolVersion"], "2025-11-25")
        rejected = session.initialize("2025-11-25-rc1", request_id=11)
        self.assertIn("error", rejected)
        self.assertEqual(rejected["error"]["code"], -32602)
        self.assertEqual(
            rejected["error"]["data"]["code"], "unsupported_protocol_version"
        )
        # Session remains usable afterwards.
        ok = session.initialize("2024-11-05", request_id=12)
        self.assertNotIn("error", ok)
        self.assertEqual(ok["result"]["protocolVersion"], "2024-11-05")

    def test_initialize_backend_unsupported_modern_clean_failure(self) -> None:
        session = self._new_session({"supported_versions": ["2026-03-01"]})
        response = session.initialize("2025-11-25")
        self.assertIn("error", response)
        error = response["error"]
        self.assertEqual(error["code"], -32603)
        self.assertEqual(error["data"]["code"], "backend_session_unavailable")
        self.assertFalse(error["data"]["retryable"])
        self.assertFalse(error["data"]["outcomeUnknown"])
        # The backend only ever saw a modern discover; nothing was downgraded
        # or converted, and no business method was attempted.
        self.assertEqual(session.count_events(method="server/discover"), 1)
        self.assertEqual(session.count_events(method="tools/call"), 0)

    # -- tools/list profile projection -----------------------------------

    def _assert_tool_list_shape_for(self, version: str, session: AdapterSession) -> None:
        response = session.tools_list(request_id=50)
        self.assertNotIn("error", response, response)
        result = response["result"]
        # Modern CacheableResult envelope fully stripped.
        self.assertEqual(set(result.keys()), {"tools"})
        by_name = {tool["name"]: tool for tool in result["tools"]}
        echo = by_name["echo"]
        for key in ("resultType", "ttlMs", "cacheScope", "_meta", "nextCursor"):
            self.assertNotIn(key, result, key)
        if version == "2024-11-05":
            self.assertEqual(
                set(echo.keys()), {"name", "description", "inputSchema"}
            )
        elif version == "2025-03-26":
            self.assertEqual(
                set(echo.keys()),
                {"name", "description", "inputSchema", "annotations"},
            )
        elif version == "2025-06-18":
            self.assertEqual(
                set(echo.keys()),
                {"name", "description", "inputSchema", "annotations",
                 "outputSchema", "title"},
            )
        else:  # 2025-11-25
            self.assertEqual(
                set(echo.keys()),
                {"name", "description", "inputSchema", "annotations",
                 "outputSchema", "title", "icons"},
            )
            # Tool.icons is a plural list of Icon objects (schema-verified:
            # Tool extends Icons with ``icons?: Icon[]``), forwarded verbatim.
            self.assertIsInstance(echo["icons"], list)
            for icon in echo["icons"]:
                self.assertIsInstance(icon.get("src"), str)
        # Every tool entry is a clean legacy object.
        for tool in result["tools"]:
            self.assertNotIn("_meta", tool)

    def test_tools_list_strips_modern_envelope_per_frozen_profile(self) -> None:
        session = self._new_session({"instructions": "modern-fixture-instructions"})
        for version in LEGACY_VERSIONS:
            with self.subTest(version=version):
                init = session.initialize(version, request_id=100)
                self.assertNotIn("error", init, init)
                self._assert_tool_list_shape_for(version, session)

    # -- tools/call projection -------------------------------------------

    def test_tools_call_structured_content_gated_by_profile(self) -> None:
        session = self._new_session()
        session.initialize("2024-11-05", request_id=1)
        old = session.tools_call("echo", {"value": "abc"}, request_id=2)
        self.assertNotIn("error", old, old)
        # 2024-11-05 has no structuredContent: it must be withheld, and the
        # modern _meta.serverInfo envelope must never surface.
        self.assertEqual(
            old["result"],
            {"content": [{"type": "text", "text": "echo:abc"}]},
        )
        session.initialize("2025-06-18", request_id=3)
        mid = session.tools_call("echo", {"value": "xyz"}, request_id=4)
        self.assertNotIn("error", mid, mid)
        self.assertEqual(
            mid["result"],
            {
                "content": [{"type": "text", "text": "echo:xyz"}],
                "structuredContent": {"echo": "xyz"},
            },
        )
        session.initialize("2025-11-25", request_id=5)
        current = session.tools_call("echo", {"value": "late"}, request_id=6)
        self.assertNotIn("error", current, current)
        self.assertEqual(
            current["result"],
            {
                "content": [{"type": "text", "text": "echo:late"}],
                "structuredContent": {"echo": "late"},
            },
        )

    def test_tools_call_is_error_and_tool_error_result_survive(self) -> None:
        session = self._new_session()
        session.initialize("2024-11-05")
        response = session.tools_call("tool_error", {}, request_id=7)
        self.assertNotIn("error", response, response)
        result = response["result"]
        self.assertTrue(result["isError"])
        # structuredContent withheld on the 2024 profile even for errors.
        self.assertNotIn("structuredContent", result)

    # -- unknown methods / errors / no replay ----------------------------

    def test_unknown_methods_clean_errors_and_no_backend_touch(self) -> None:
        session = self._new_session()
        session.initialize("2025-11-25")
        for index, method in enumerate(
            (
                "resources/list",
                "prompts/list",
                "completions/complete",
                "elicitation/create",
                "roots/list",
                "sampling/createMessage",
                "tasks/list",
                "not/a/method",
            ),
            start=20,
        ):
            session.send_line(
                {"jsonrpc": "2.0", "id": index, "method": method, "params": {}}
            )
            response = session.read_until_id(index)
            self.assertIn("error", response, response)
            self.assertEqual(response["error"]["code"], -32601)
        # None of those methods ever reached the modern backend.
        backend_methods = {
            item.get("method")
            for item in session.events()
            if item.get("event") == "recv"
        }
        self.assertEqual(backend_methods, {"server/discover"})
        # Adapter is still alive and responsive.
        session.send_line({"jsonrpc": "2.0", "id": 99, "method": "ping", "params": {}})
        ping = session.read_until_id(99)
        self.assertNotIn("error", ping)
        self.assertEqual(ping["result"], {})

    def test_backend_error_passthrough_and_retired_code_normalized(self) -> None:
        session = self._new_session()
        session.initialize("2025-11-25")
        bad = session.tools_call("bad_call", {}, request_id=30)
        self.assertIn("error", bad)
        self.assertEqual(bad["error"]["code"], -32602)
        self.assertEqual(bad["error"]["message"], "invalid parameters for bad_call")
        self.assertEqual(bad["error"]["data"], {"detail": "fixture-sent"})
        retired = session.tools_call("retired_error", {}, request_id=31)
        self.assertIn("error", retired)
        # Retired modern code -32042 must never reach a legacy client.
        self.assertEqual(retired["error"]["code"], -32603)
        self.assertEqual(retired["error"]["data"]["originalCode"], -32042)
        self.assertEqual(retired["error"]["data"]["code"], "backend_protocol_error")
        self.assertEqual(session.count_events(method="tools/call"), 2)

    def test_backend_crash_mid_call_no_replay_outcome_unknown(self) -> None:
        session = self._new_session()
        session.initialize("2025-11-25")
        crashed = session.tools_call("crash_on_call", {}, request_id=40)
        self.assertIn("error", crashed)
        data = crashed["error"]["data"]
        self.assertEqual(data["code"], "backend_session_unavailable")
        self.assertTrue(data["retryable"])
        self.assertTrue(data["outcomeUnknown"])
        # Exactly one business call reached the backend; never replayed.
        self.assertEqual(session.count_events(method="tools/call"), 1)
        # Subsequent requests error cleanly without a hidden respawn/replay.
        again = session.tools_list(request_id=41)
        self.assertIn("error", again)
        self.assertFalse(again["error"]["data"]["outcomeUnknown"])
        self.assertEqual(session.count_events(method="tools/call"), 1)
        # Adapter process is still alive.
        session.send_line({"jsonrpc": "2.0", "id": 42, "method": "ping", "params": {}})
        self.assertNotIn("error", session.read_until_id(42))

    # -- serialization / progress / cancellation --------------------------

    def test_requests_serialized_with_progress_relay(self) -> None:
        session = self._new_session()
        session.initialize("2025-11-25")
        session.send_line(
            {
                "jsonrpc": "2.0",
                "id": 60,
                "method": "tools/call",
                "params": {
                    "name": "slow",
                    "arguments": {},
                    "_meta": {"progressToken": "tok-1"},
                },
            }
        )
        session.send_line(
            {
                "jsonrpc": "2.0",
                "id": 61,
                "method": "tools/call",
                "params": {
                    "name": "slow",
                    "arguments": {},
                    "_meta": {"progressToken": "tok-2"},
                },
            }
        )
        session.send_line(
            {
                "jsonrpc": "2.0",
                "id": 62,
                "method": "tools/call",
                "params": {
                    "name": "slow",
                    "arguments": {},
                    "_meta": {"progressToken": "tok-3"},
                },
            }
        )
        seen_tokens: list = []
        responses: dict = {}
        deadline = time.monotonic() + 25
        while time.monotonic() < deadline and len(responses) < 3:
            frame = session._pop_frame(5.0)
            if frame is None:
                continue
            if frame.get("method") == "notifications/progress":
                params = frame.get("params", {})
                if params.get("progressToken") in ("tok-1", "tok-2", "tok-3"):
                    seen_tokens.append(params["progressToken"])
                continue
            if frame.get("id") in (60, 61, 62):
                self.assertNotIn("error", frame, frame)
                responses[frame["id"]] = frame["result"]
        self.assertEqual(set(responses.keys()), {60, 61, 62})
        for result in responses.values():
            self.assertEqual(
                result["content"], [{"type": "text", "text": "slow-done"}]
            )
        for token in ("tok-1", "tok-2", "tok-3"):
            self.assertIn(token, seen_tokens)
        # Backend calls never overlap: strict start/end alternation proves the
        # adapter serialized requests (no concurrent second in-flight call).
        lifecycle = [
            item
            for item in session.events()
            if item.get("event") in ("call-start", "call-end")
            and item.get("tool") == "slow"
        ]
        self.assertEqual(len(lifecycle), 6)
        for index, item in enumerate(lifecycle):
            expected = "call-start" if index % 2 == 0 else "call-end"
            self.assertEqual(item["event"], expected)

    def test_cancellation_forwarded_with_mapped_backend_id_no_replay(self) -> None:
        session = self._new_session()
        session.initialize("2025-11-25")
        session.send_line(
            {
                "jsonrpc": "2.0",
                "id": 70,
                "method": "tools/call",
                "params": {
                    "name": "slow",
                    "arguments": {},
                    "_meta": {"progressToken": "cancel-tok"},
                },
            }
        )
        # Wait until the backend is demonstrably working on the call.
        deadline = time.monotonic() + 10
        saw_progress = False
        while time.monotonic() < deadline and not saw_progress:
            frame = session._pop_frame(2.0)
            if frame is None:
                continue
            if (
                frame.get("method") == "notifications/progress"
                and frame.get("params", {}).get("progressToken") == "cancel-tok"
            ):
                saw_progress = True
        self.assertTrue(saw_progress, "no progress notification seen")
        session.send_line(
            {
                "jsonrpc": "2.0",
                "method": "notifications/cancelled",
                "params": {"requestId": 70, "reason": "user aborted"},
            }
        )
        response = session.read_until_id(70)
        # The backend aborted the call with its own mapped error.
        self.assertIn("error", response, response)
        self.assertEqual(response["error"]["code"], -32800)
        cancelled_events = [
            item for item in session.events() if item.get("event") == "cancelled"
        ]
        self.assertEqual(len(cancelled_events), 1)
        backend_request_id = cancelled_events[0]["params"]["requestId"]
        # The backend saw the ADAPTER's mapped request id, never the legacy id.
        self.assertTrue(str(backend_request_id).startswith("lm-"))
        self.assertNotEqual(backend_request_id, 70)
        self.assertEqual(cancelled_events[0]["params"]["reason"], "user aborted")
        # One business call total; cancellation did not replay anything.
        self.assertEqual(session.count_events(method="tools/call"), 1)
        self.assertEqual(
            len(
                [
                    item
                    for item in session.events()
                    if item.get("event") == "call-cancelled-observed"
                ]
            ),
            1,
        )
        # The session remains usable afterwards.
        ok = session.tools_call("echo", {"value": "after"}, request_id=71)
        self.assertNotIn("error", ok, ok)
        self.assertEqual(session.count_events(method="tools/call"), 2)

    # -- bounded drains / timeouts / queued cancellation / shutdown --------

    def test_stderr_flood_never_deadlocks_the_adapter(self) -> None:
        # The fixture writes far more than an OS pipe buffer to stderr before
        # answering anything; an undrained PIPE would block the backend and
        # the first initialize would never be answered.
        session = self._new_session(
            {"stderr_flood_bytes": 4 * 1024 * 1024}
        )
        start = time.monotonic()
        response = session.initialize("2025-11-25")
        self.assertLess(time.monotonic() - start, 10.0)
        self.assertNotIn("error", response, response)
        # The adapter is still fully usable after the flood.
        listed = session.tools_list(request_id=2)
        self.assertNotIn("error", listed, listed)
        called = session.tools_call("echo", {"value": "ok"}, request_id=3)
        self.assertNotIn("error", called, called)

    def test_queued_cancellation_is_never_executed(self) -> None:
        session = self._new_session()
        session.initialize("2025-11-25")
        # First call occupies the adapter (serialized, ~1s in flight)...
        session.send_line(
            {
                "jsonrpc": "2.0",
                "id": 80,
                "method": "tools/call",
                "params": {"name": "slow", "arguments": {}},
            }
        )
        # ...second call is queued behind it...
        session.send_line(
            {
                "jsonrpc": "2.0",
                "id": 81,
                "method": "tools/call",
                "params": {"name": "echo", "arguments": {"value": "never"}},
            }
        )
        # ...and a cancellation arrives while 81 is still queued.
        session.send_cancel(81, reason="skip it")
        first = session.read_until_id(80)
        self.assertNotIn("error", first, first)
        # The queued request was purged: no response for it, and the backend
        # never executed it (exactly one tools/call recv total: the slow one).
        self.assertEqual(session.count_events(method="tools/call"), 1)
        self.assertEqual(
            session.count_events(event="call-start", method=None), 1
        )
        late = session.drain(timeout=0.6)
        self.assertFalse([f for f in late if f.get("id") == 81], late)
        # Adapter remains alive and usable; still no replay of 81.
        session.send_line({"jsonrpc": "2.0", "id": 82, "method": "ping", "params": {}})
        ping = session.read_until_id(82)
        self.assertNotIn("error", ping)
        self.assertEqual(session.count_events(method="tools/call"), 1)

    def test_unknown_and_completed_cancel_ids_are_ignored_and_reusable(self) -> None:
        # A cancel must never leave a tombstone that poisons a later reuse of
        # the same numeric id, and floods of cancels for ids that are unknown
        # (never submitted) or already completed must not grow any state.
        session = self._new_session()
        session.initialize("2025-11-25")
        # Flood: cancels for ids never submitted, distinct and repeated.
        for request_id in list(range(1000, 1150)) * 2:
            session.send_cancel(request_id, reason="flood")
        # A cancel for an id that already completed and got its response.
        first = session.tools_call("echo", {"value": "one"}, request_id=700)
        self.assertNotIn("error", first, first)
        session.send_cancel(700, reason="too late")
        # An id that was flooded with unknown cancels is fully reusable.
        reused_unknown = session.tools_call(
            "echo", {"value": "reuse-unknown"}, request_id=1000
        )
        self.assertNotIn("error", reused_unknown, reused_unknown)
        self.assertEqual(
            reused_unknown["result"]["content"][0]["text"], "echo:reuse-unknown"
        )
        # The completed-and-cancelled id is fully reusable too.
        reused_completed = session.tools_call(
            "echo", {"value": "reuse-completed"}, request_id=700
        )
        self.assertNotIn("error", reused_completed, reused_completed)
        # Control id untouched by any cancel.
        control = session.tools_call("echo", {"value": "ctrl"}, request_id=2000)
        self.assertNotIn("error", control, control)
        # Exactly the four real business calls executed; nothing was dropped.
        self.assertEqual(session.count_events(method="tools/call"), 4)

    def test_non_scalar_cancel_requestid_ignored_without_crash(self) -> None:
        # dict/list/other requestId values are never valid cancel targets and
        # must be ignored without raising (a TypeError in the reader thread
        # would silently kill it and freeze the session).
        session = self._new_session({"hang_call": True})
        session.initialize("2025-11-25")
        for bad in ({"nested": 1}, [1, 2, 3], True, None, 3.5, "not-an-int"):
            session.send_line(
                {
                    "jsonrpc": "2.0",
                    "method": "notifications/cancelled",
                    "params": {"requestId": bad},
                }
            )
        # The legacy reader survived all garbage cancels: real requests still
        # get answers promptly.
        alive = session.tools_call("echo", {"value": "after-bad"}, request_id=300)
        self.assertNotIn("error", alive, alive)
        self.assertEqual(alive["result"]["content"][0]["text"], "echo:after-bad")
        # And real in-flight cancellation still works after the garbage.
        session.send_line(
            {
                "jsonrpc": "2.0",
                "id": 301,
                "method": "tools/call",
                "params": {"name": "slow", "arguments": {}},
            }
        )
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline and not any(
            e.get("event") == "call-start" for e in session.events()
        ):
            time.sleep(0.05)
        self.assertTrue(
            any(e.get("event") == "call-start" for e in session.events()),
            "slow call never started",
        )
        session.send_cancel(301, reason="now real")
        cancelled = session.read_until_id(301)
        self.assertIn("error", cancelled, cancelled)
        self.assertEqual(cancelled["error"]["code"], -32800)
        self.assertEqual(session.count_events(method="tools/call"), 2)

    def test_inflight_cancelled_id_reusable_immediately(self) -> None:
        # A genuinely cancelled in-flight request answers with the backend
        # cancel error; reusing that same numeric id for a NEW request must
        # execute normally (never silently dropped by stale state).
        session = self._new_session({"hang_call": True})
        session.initialize("2025-11-25")
        session.send_line(
            {
                "jsonrpc": "2.0",
                "id": 900,
                "method": "tools/call",
                "params": {"name": "slow", "arguments": {}},
            }
        )
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline and not any(
            e.get("event") == "call-start" for e in session.events()
        ):
            time.sleep(0.05)
        self.assertTrue(
            any(e.get("event") == "call-start" for e in session.events()),
            "slow call never started",
        )
        session.send_cancel(900, reason="abort")
        aborted = session.read_until_id(900)
        self.assertIn("error", aborted, aborted)
        self.assertEqual(aborted["error"]["code"], -32800)
        # Same id, new business call: must execute, not be swallowed.
        reused = session.tools_call("echo", {"value": "reused"}, request_id=900)
        self.assertNotIn("error", reused, reused)
        self.assertEqual(reused["result"]["content"][0]["text"], "echo:reused")
        self.assertEqual(session.count_events(method="tools/call"), 2)

    def test_hanging_discover_bounded_startup_timeout(self) -> None:
        session = self._new_session(
            {"hang_discover": True}, startup_timeout=1.0
        )
        start = time.monotonic()
        response = session.initialize("2025-11-25")
        self.assertLess(time.monotonic() - start, 10.0)
        self.assertIn("error", response)
        error = response["error"]
        self.assertEqual(error["code"], -32603)
        self.assertEqual(error["data"]["code"], "backend_session_unavailable")
        # Nothing business was ever sent; discovery was attempted exactly once
        # and is never retried/replayed by this initialize.
        self.assertFalse(error["data"]["retryable"])
        self.assertFalse(error["data"]["outcomeUnknown"])
        self.assertIn("timed out", error["data"]["detail"])
        self.assertEqual(session.count_events(method="server/discover"), 1)
        self.assertEqual(session.count_events(method="tools/call"), 0)

    def test_hanging_call_bounded_timeout_no_replay(self) -> None:
        session = self._new_session({"hang_call": True}, request_timeout=1.2)
        init = session.initialize("2025-11-25")
        self.assertNotIn("error", init, init)
        start = time.monotonic()
        response = session.tools_call("slow", {}, request_id=90)
        self.assertLess(time.monotonic() - start, 10.0)
        self.assertIn("error", response)
        data = response["error"]["data"]
        self.assertEqual(data["code"], "backend_session_unavailable")
        self.assertTrue(data["retryable"])
        # The business call bytes were sent once: outcome is genuinely
        # unknown, but the request is never replayed.
        self.assertTrue(data["outcomeUnknown"])
        self.assertIn("not replayed", data["detail"])
        self.assertEqual(session.count_events(method="tools/call"), 1)
        self.assertEqual(
            len([e for e in session.events() if e.get("event") == "call-start"]), 1
        )
        # The timed-out backend was stopped: a further request fails cleanly
        # and no second business call ever reaches the fixture.
        after = session.tools_list(request_id=91)
        self.assertIn("error", after)
        self.assertEqual(after["error"]["data"]["code"], "backend_session_unavailable")
        self.assertEqual(session.count_events(method="tools/call"), 1)
        # Adapter itself remains alive.
        session.send_line({"jsonrpc": "2.0", "id": 92, "method": "ping", "params": {}})
        self.assertNotIn("error", session.read_until_id(92))

    def test_stdin_eof_stops_backend_promptly(self) -> None:
        session = self._new_session({"hang_call": True})
        init = session.initialize("2025-11-25")
        self.assertNotIn("error", init, init)
        # Start a never-completing in-flight call, then close stdin while it
        # is still awaiting: the adapter must abandon the exchange, stop the
        # owned backend process, and exit promptly (no drain of queued work,
        # no waiting out the request timeout).
        session.send_line(
            {
                "jsonrpc": "2.0",
                "id": 100,
                "method": "tools/call",
                "params": {"name": "slow", "arguments": {}},
            }
        )
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline and not any(
            e.get("event") == "call-start" for e in session.events()
        ):
            time.sleep(0.05)
        self.assertTrue(
            any(e.get("event") == "call-start" for e in session.events()),
            "backend never started the slow call",
        )
        assert session.proc.stdin is not None
        start = time.monotonic()
        session.proc.stdin.close()
        rc = session.proc.wait(timeout=8)
        self.assertLess(time.monotonic() - start, 8.0)
        self.assertEqual(rc, 0)
        # Exactly one call started, none completed, nothing replayed.
        self.assertEqual(session.count_events(method="tools/call"), 1)
        self.assertEqual(
            len([e for e in session.events() if e.get("event") == "call-start"]), 1
        )
        self.assertEqual(
            len([e for e in session.events() if e.get("event") == "call-end"]), 0
        )

    # -- bounded framing / queue capacity ---------------------------------

    def test_oversized_backend_frame_rejected_whole_never_truncated(self) -> None:
        # The echo answer is one newline-free blob far beyond the adapter's
        # frame cap.  The adapter must refuse it whole (bounded read, never
        # truncated/parsed into a partial result) and close the backend
        # session with a structured error.
        session = self._new_session(
            {"oversized_result_bytes": 300_000}, max_frame_bytes=2048
        )
        init = session.initialize("2025-11-25")
        self.assertNotIn("error", init, init)
        called = session.tools_call("echo", {"value": "big"}, request_id=95)
        self.assertIn("error", called)
        data = called["error"]["data"]
        self.assertEqual(data["code"], "backend_session_unavailable")
        # The call bytes were sent once, so the outcome is genuinely unknown.
        self.assertTrue(data["retryable"])
        self.assertTrue(data["outcomeUnknown"])
        # No truncated fragment of the oversized payload ever reached the
        # legacy client (the frame was rejected whole).
        self.assertNotIn("E" * 8, json.dumps(called))
        self.assertEqual(session.count_events(method="tools/call"), 1)
        # Session is closed for the backend: further requests fail cleanly,
        # and nothing was replayed.
        again = session.tools_list(request_id=96)
        self.assertIn("error", again)
        self.assertEqual(again["error"]["data"]["code"], "backend_session_unavailable")
        self.assertFalse(again["error"]["data"]["outcomeUnknown"])
        self.assertEqual(session.count_events(method="tools/call"), 1)
        # Adapter process itself stays alive.
        session.send_line({"jsonrpc": "2.0", "id": 97, "method": "ping", "params": {}})
        self.assertNotIn("error", session.read_until_id(97))

    def test_oversized_legacy_frame_rejected_never_executed(self) -> None:
        # One huge no-newline request line from the legacy client: bounded
        # framing must answer a structured parse error (id null, nothing
        # executed) and keep serving smaller frames.
        session = self._new_session(max_frame_bytes=2048)
        init = session.initialize("2025-11-25")
        self.assertNotIn("error", init, init)
        huge = (
            '{"jsonrpc":"2.0","id":55,"method":"ping","params":{"pad":"'
            + ("x" * 200_000)
            + '"}}'
        )
        session.send_raw(huge)
        session.send_raw("\n")
        refused = session.read_until(
            lambda frame: isinstance(frame.get("error"), dict)
            and frame["error"].get("code") == -32700
        )
        self.assertIsNone(refused.get("id"))
        self.assertIn("limit", refused["error"]["message"])
        # The oversized request was never executed and the session is intact.
        self.assertEqual(session.count_events(method="server/discover"), 1)
        self.assertEqual(session.count_events(method="tools/call"), 0)
        session.send_line({"jsonrpc": "2.0", "id": 56, "method": "ping", "params": {}})
        self.assertNotIn("error", session.read_until_id(56))

    def test_queue_saturation_refused_not_executed(self) -> None:
        # Capacity 2, one never-completing in-flight call: exactly the two
        # requests that arrive after the queue is full must receive a
        # structured JSON-RPC refusal and never reach the backend.
        session = self._new_session({"hang_call": True}, queue_capacity=2)
        init = session.initialize("2025-11-25")
        self.assertNotIn("error", init, init)
        session.send_line(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {"name": "slow", "arguments": {}},
            }
        )
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline and not any(
            e.get("event") == "call-start" for e in session.events()
        ):
            time.sleep(0.05)
        self.assertTrue(
            any(e.get("event") == "call-start" for e in session.events()),
            "slow call never started",
        )
        for request_id in (2, 3):  # fit within capacity
            session.send_line(
                {"jsonrpc": "2.0", "id": request_id, "method": "ping", "params": {}}
            )
        refused_ids: list[Any] = []
        for request_id in (4, 5):  # queue is full: structured refusal
            session.send_line(
                {"jsonrpc": "2.0", "id": request_id, "method": "ping", "params": {}}
            )
            response = session.read_until(
                lambda frame: frame.get("id") in (4, 5)
                and isinstance(frame.get("error"), dict)
            )
            refused_ids.append(response["id"])
            self.assertEqual(response["error"]["code"], -32000)
            self.assertIn("queue", response["error"]["message"])
        self.assertEqual(set(refused_ids), {4, 5})
        # Only the slow call ever executed; the refused pings never reached
        # the backend.
        self.assertEqual(session.count_events(method="tools/call"), 1)
        # Cancel the in-flight call, then the queued pings drain normally.
        session.send_cancel(1)
        cancelled = session.read_until_id(1)
        self.assertIn("error", cancelled)
        self.assertEqual(cancelled["error"]["code"], -32800)
        for request_id in (2, 3):
            self.assertNotIn("error", session.read_until_id(request_id))
        session.send_line({"jsonrpc": "2.0", "id": 6, "method": "ping", "params": {}})
        self.assertNotIn("error", session.read_until_id(6))
        self.assertEqual(session.count_events(method="tools/call"), 1)

    def test_backend_stderr_never_leaks_into_mcp_errors(self) -> None:
        # The fixture writes a distinctive marker on its stderr; none of it
        # may ever surface in any MCP frame the legacy client receives.
        session = self._new_session({"stderr_token": "LEAK-MARKER-77"})
        init = session.initialize("2025-11-25")
        self.assertNotIn("error", init, init)
        self.assertNotIn("LEAK-MARKER-77", json.dumps(init))
        listed = session.tools_list(request_id=93)
        self.assertNotIn("error", listed, listed)
        self.assertNotIn("LEAK-MARKER-77", json.dumps(listed))
        crashed = session.tools_call("crash_on_call", {}, request_id=94)
        self.assertIn("error", crashed)
        self.assertNotIn("LEAK-MARKER-77", json.dumps(crashed))
        # And nothing else on the protocol stream contains it either.
        for frame in session.drain(timeout=0.5):
            self.assertNotIn("LEAK-MARKER-77", json.dumps(frame))

    # -- fixture sanity ---------------------------------------------------

    def test_modern_fixture_rejects_legacy_initialize_without_meta(self) -> None:
        tmp = Path(tempfile.mkdtemp(prefix="lms-fx-"))
        state_path = tmp / "state.jsonl"
        environment = os.environ.copy()
        environment["PYTHONDONTWRITEBYTECODE"] = "1"
        proc = subprocess.Popen(
            [
                sys.executable,
                str(FIXTURE),
                "--state",
                str(state_path),
            ],
            cwd=ROOT,
            env=environment,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
        )
        try:
            proc.stdin.write(
                json.dumps(_initialize_request("2025-11-25", 1)) + "\n"
            )
            proc.stdin.flush()
            line = proc.stdout.readline()
            response = json.loads(line)
            self.assertEqual(response["id"], 1)
            self.assertIn("error", response)
            # A modern-only server does not implement the legacy initialize
            # handshake at all: it answers method-not-found, never a session.
            self.assertEqual(response["error"]["code"], -32601)
            self.assertIn("Method not found: initialize", response["error"]["message"])
        finally:
            try:
                proc.stdin.close()
            except OSError:
                pass
            proc.wait(timeout=10)
            for stream in (proc.stdout, proc.stderr):
                if stream is not None:
                    try:
                        stream.close()
                    except OSError:
                        pass


class LegacyProfileContentGateTest(unittest.TestCase):
    """Wrapper-level content-variant/_meta gates of the standalone adapter.

    ``legacy_modern_stdio._LegacyProfile.project_call_result`` must apply the
    SAME frozen content acceptance as the runtime's shared projection: kinds a
    profile cannot represent (audio before 2025-03-26, resource_link before
    2025-06-18, or any unknown kind) make the WHOLE result unprojectable
    (None -> the dispatch path writes an explicit ``unprojectable_result``
    error), so business output is never partially or silently dropped and a
    success is never fabricated from unrepresentable content.  No wrapper
    state is involved: a failed projection never replays or poisons a later
    call.
    """

    def _profile(self, version: str) -> Any:
        return stdio_mod._LegacyProfile(version)

    def test_call_result_common_kinds_verbatim_every_profile(self) -> None:
        content = [
            {"type": "text", "text": "hello", "annotations": {"audience": ["user"]}},
            {"type": "image", "data": "aGVsbG8=", "mimeType": "image/png"},
            {
                "type": "resource",
                "resource": {"uri": "file:///a.txt", "text": "body"},
            },
        ]
        for profile in LEGACY_VERSIONS:
            with self.subTest(profile=profile):
                projected = self._profile(profile).project_call_result(
                    {"content": content}
                )
                self.assertEqual(
                    projected, {"content": content}, profile
                )

    def test_call_result_audio_kind_gated_by_profile(self) -> None:
        audio = {"type": "audio", "data": "AQID", "mimeType": "audio/wav"}
        self.assertIsNone(
            self._profile("2024-11-05").project_call_result({"content": [audio]})
        )
        for profile in ("2025-03-26", "2025-06-18", "2025-11-25"):
            with self.subTest(profile=profile):
                self.assertEqual(
                    self._profile(profile).project_call_result({"content": [audio]}),
                    {"content": [audio]},
                )

    def test_call_result_resource_link_kind_gated_by_profile(self) -> None:
        link = {"type": "resource_link", "uri": "file:///a.txt", "text": "x"}
        for profile in ("2024-11-05", "2025-03-26"):
            with self.subTest(profile=profile):
                self.assertIsNone(
                    self._profile(profile).project_call_result({"content": [link]})
                )
        for profile in ("2025-06-18", "2025-11-25"):
            with self.subTest(profile=profile):
                self.assertEqual(
                    self._profile(profile).project_call_result({"content": [link]}),
                    {"content": [link]},
                )

    def test_call_result_unknown_kind_never_representable(self) -> None:
        future = {"type": "hologram", "text": "later era"}
        for profile in LEGACY_VERSIONS:
            with self.subTest(profile=profile):
                self.assertIsNone(
                    self._profile(profile).project_call_result({"content": [future]})
                )

    def test_call_result_item_and_resource_meta_stripped_before_2025_06_18(self) -> None:
        content = [
            {
                "type": "text",
                "text": "t",
                "annotations": {"audience": ["user"]},
                "_meta": {"fixture": True},
            },
            {
                "type": "resource",
                "resource": {
                    "uri": "file:///a.txt",
                    "text": "b",
                    "_meta": {"fixture": True},
                },
                "_meta": {"fixture": True},
            },
        ]
        for profile in ("2024-11-05", "2025-03-26"):
            with self.subTest(profile=profile):
                projected = self._profile(profile).project_call_result(
                    {"content": content}
                )
                self.assertEqual(
                    projected,
                    {
                        "content": [
                            {
                                "type": "text",
                                "text": "t",
                                "annotations": {"audience": ["user"]},
                            },
                            {
                                "type": "resource",
                                "resource": {
                                    "uri": "file:///a.txt",
                                    "text": "b",
                                },
                            },
                        ]
                    },
                )
        for profile in ("2025-06-18", "2025-11-25"):
            with self.subTest(profile=profile):
                self.assertEqual(
                    self._profile(profile).project_call_result(
                        {"content": content}
                    ),
                    {"content": content},
                )

    def test_call_result_unrepresentable_whole_result_stateless_no_replay(self) -> None:
        profile = self._profile("2024-11-05")
        # A mixed result containing audio is unrepresentable on 2024-11-05:
        # the WHOLE result is refused (None); audio is never silently dropped
        # to hand back a "successful" partial result.
        self.assertIsNone(
            profile.project_call_result(
                {
                    "content": [
                        {"type": "text", "text": "keep me?"},
                        {"type": "audio", "data": "AQID", "mimeType": "audio/wav"},
                    ]
                }
            )
        )
        # The wrapper keeps no partial state: the same profile instance
        # projects a fully representable result exactly, untouched, on a later
        # call (the dispatch path sends that result once; nothing is replayed
        # and no stale partial output leaks).
        after = profile.project_call_result(
            {
                "content": [{"type": "text", "text": "fine"}],
                "isError": False,
            }
        )
        self.assertEqual(
            after, {"content": [{"type": "text", "text": "fine"}], "isError": False}
        )

    def test_call_result_structured_content_gate_with_content_projection(self) -> None:
        text = [{"type": "text", "text": "hello"}]
        structured = {"summary": "s"}
        old = self._profile("2025-03-26").project_call_result(
            {"content": text, "structuredContent": structured}
        )
        self.assertEqual(old, {"content": text})  # withheld < 2025-06-18
        new = self._profile("2025-06-18").project_call_result(
            {"content": text, "structuredContent": structured}
        )
        self.assertEqual(
            new, {"content": text, "structuredContent": structured}
        )


if __name__ == "__main__":
    unittest.main()
