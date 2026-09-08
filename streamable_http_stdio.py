#!/usr/bin/env python3
"""Explicit local stdio-to-Streamable-HTTP MCP adapter.

The Bridge launches stdio MCPs only.  This module is the separately testable
adapter component that lets one local MCP client talk to one remote MCP server
that speaks the MCP 2025-06-18 Streamable HTTP transport::

    local MCP client <-> stdio (this process) <-> HTTP POST/GET (remote MCP)

To the local client this process is an ordinary stdio MCP server
(newline-delimited JSON-RPC on stdin/stdout, protocol-clean stdout).  To the
remote server it is an ordinary Streamable HTTP MCP client, implementing the
2025-06-18 transport rules:

* every outbound JSON-RPC message is one HTTP POST to the MCP endpoint with
  ``Accept: application/json, text/event-stream``;
* a JSON-RPC request response is accepted either as one ``application/json``
  body or as a ``text/event-stream`` body that ends with the matching
  response; any JSON-RPC notifications sent before the response on that same
  stream are relayed to the local client;
* a JSON-RPC notification or response POST is accepted as HTTP 202 with no
  body;
* session ids returned in the ``Mcp-Session-Id`` header during initialization
  are attached to every subsequent request, and the negotiated protocol
  version from the ``InitializeResult`` is sent as the ``MCP-Protocol-Version``
  header on every subsequent request;
* an optional HTTP GET SSE stream relays server-initiated requests and
  notifications; server ``requests`` are answered by the local client, whose
  JSON-RPC response the adapter POSTs back;
* HTTP 404 for a session-bearing request triggers the spec-mandated fresh
  ``initialize`` *without* a session id, and the request is retried once on the
  fresh session; all other HTTP failures map to structured JSON-RPC errors;
* on stdin EOF the adapter best-effort DELETEs the remote session and exits.

Explicit ``--protocol-era modern`` selects the 2026-07-28 request-metadata
binding instead. It requires modern stdio input and performs stateless POSTs:
no synthesized discovery, initialize, session, GET, DELETE, fallback or replay.
JSON/MRTR results retain backend fields; SSE progress is forwarded immediately
and stdio cancellation interrupts only the matching HTTP socket. Request bytes,
aggregate response bytes, concurrent requests and wall-clock duration are bounded.
The legacy path and defaults above are unchanged. Before each modern tools/call,
the adapter intentionally performs additional read-only tools/list RPCs as a
preflight (up to four pages, bounded aggregate catalog bytes, sharing the original
metadata/deadline/cancellation). These disclosed preflight RPCs are not business
call retries. It mirrors schema-declared x-mcp-header fields reachable solely through properties:
string, boolean and safe integer values become Mcp-Param-* headers using the spec
encoding; absent/null values omit them. Invalid or unsupported annotation schemas
are excluded from public tools/list, and direct calls fail before the business POST.
The bounded profile supports at most 64 bindings per tool; catalog failure, missing
definition or exceeded bounds also fail closed. It deliberately has no schema
cache or automatic MRTR fulfillment. Adapter-generated transport errors carry
error.data.outcomeUnknown=true once a tools/call POST socket-write attempt begins:
connection loss or deadline failure can leave completed side effects. Preflight
failures, local refusals and recognized remote protocol refusals report false.
This flag describes uncertainty about this request; it is never replay advice.
No failure triggers automatic business replay. This is a tested bounded profile,
not a claim of complete 2026-07-28 HTTP client conformance.

Static credentials (e.g. ``Authorization``) are supplied through local adapter
arguments or environment variables; they are never logged and never enter
Bridge peer frames or Registry metadata.  Diagnostics are stderr-only and
never include header values, tokens, cookies, response bodies or URL query
strings.  This module is standard-library only and imports nothing from the
Bridge runtime, so it can be tested and deployed independently.
"""

from __future__ import annotations

import argparse
import base64
import math
import json
import os
import re
import ssl
import socket
import sys
import threading
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from http.client import HTTPConnection, HTTPException, HTTPResponse
from typing import Any, Callable, Iterator, TextIO

ADAPTER_VERSION = "0.1.0"
PROFILE_NAME = "streamable-http-stdio"
DEFAULT_PROTOCOL_VERSION = "2025-06-18"
MODERN_PROTOCOL_VERSION = "2026-07-28"
MODERN_VERSION_META = "io.modelcontextprotocol/protocolVersion"
MODERN_CAPABILITIES_META = "io.modelcontextprotocol/clientCapabilities"
ACCEPT_JSON_AND_EVENT = "application/json, text/event-stream"
ACCEPT_EVENT = "text/event-stream"
SESSION_HEADER = "Mcp-Session-Id"
PROTOCOL_HEADER = "MCP-Protocol-Version"
CONTENT_TYPE_JSON = "application/json"
CONTENT_TYPE_EVENT = "text/event-stream"

#: Bounded default message size mirroring the runtime's JSON-RPC frame limit.
DEFAULT_MAX_MESSAGE_BYTES = 16 * 1024 * 1024
DEFAULT_CONNECT_TIMEOUT_S = 10.0
DEFAULT_IDLE_TIMEOUT_S = 60.0
DEFAULT_SSE_IDLE_TIMEOUT_S = 300.0
DEFAULT_REQUEST_TIMEOUT_S = 600.0
DEFAULT_MAX_INFLIGHT = 8
#: Header names the adapter manages itself; static caller headers never win.
_RESERVED_HEADERS = {
    "accept",
    "content-type",
    "content-length",
    SESSION_HEADER.lower(),
    PROTOCOL_HEADER.lower(),
}
_HEADERS_ENV = "WIN_WSL_MCP_BRIDGE_HTTP_HEADERS"
_AUTHORIZATION_ENV = "WIN_WSL_MCP_BRIDGE_HTTP_AUTHORIZATION"
#: JSON-RPC error codes used for transport-level failures.
ERR_SERVER = -32000  # implementation-defined server/transport error


class AdapterError(Exception):
    """Base adapter failure carrying a stable machine-readable ``category``."""


class HttpTransportError(AdapterError):
    """HTTP-level failure carrying only a category and status.

    Instances never carry header values, tokens, cookies or response bodies.
    """

    def __init__(self, category: str, http_status: int | None, message: str):
        super().__init__(message)
        self.category = category
        self.http_status = http_status

    def to_jsonrpc(self, request_id: Any) -> dict[str, Any]:
        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {
                "code": ERR_SERVER,
                "message": f"streamable HTTP transport error: {self.category}",
                "data": {"httpStatus": self.http_status} if self.http_status else None,
            },
        }


class SessionLostError(HttpTransportError):
    """The remote terminated the session (HTTP 404 with a session id)."""

    def __init__(self, message: str):
        super().__init__("session-lost", 404, message)


@dataclass(frozen=True)
class AdapterOptions:
    endpoint_url: str
    headers: dict[str, str] = field(default_factory=dict)
    protocol_version: str = DEFAULT_PROTOCOL_VERSION
    connect_timeout_s: float = DEFAULT_CONNECT_TIMEOUT_S
    idle_timeout_s: float = DEFAULT_IDLE_TIMEOUT_S
    sse_idle_timeout_s: float = DEFAULT_SSE_IDLE_TIMEOUT_S
    request_timeout_s: float = DEFAULT_REQUEST_TIMEOUT_S
    max_message_bytes: int = DEFAULT_MAX_MESSAGE_BYTES
    max_inflight: int = DEFAULT_MAX_INFLIGHT
    sse_reconnect_max_delay_s: float = 5.0
    sse_max_continuous_failures: int = 20
    protocol_era: str = "legacy"

    def __post_init__(self) -> None:
        if self.protocol_era not in ("legacy", "modern"):
            raise AdapterError("unsupported protocol era")
        if self.protocol_era == "modern" and any(
            not math.isfinite(value) or value <= 0 for value in (
                self.connect_timeout_s, self.idle_timeout_s,
                self.sse_idle_timeout_s, self.request_timeout_s,
            )
        ):
            raise AdapterError("modern timeouts must be finite and positive")
        parsed = urllib.parse.urlsplit(self.endpoint_url)
        if parsed.scheme not in ("http", "https"):
            raise AdapterError(f"unsupported endpoint scheme {parsed.scheme!r}")
        if not parsed.hostname:
            raise AdapterError("endpoint URL has no host")
        if self.max_message_bytes < 1024 or self.max_inflight < 1:
            raise AdapterError("invalid bounds")


def _json_bytes(message: Any) -> bytes:
    return json.dumps(message, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def _classify(message: dict[str, Any]) -> str:
    """``request`` / ``notification`` / ``response`` for one JSON-RPC message."""
    if isinstance(message.get("method"), str):
        return "request" if "id" in message else "notification"
    if "id" in message and ("result" in message or "error" in message):
        return "response"
    # Defensive: an object with an id but neither method nor result/error is
    # treated as a request so its id is preserved in any error reply.
    return "request" if "id" in message else "notification"


class _LockedWriter:
    """Serialized, protocol-clean stdout writer for JSON-RPC lines."""

    def __init__(self, out: TextIO):
        self._out = out
        self._binary = getattr(out, "buffer", out)
        self._lock = threading.Lock()

    def write_message(self, message: dict[str, Any]) -> None:
        payload = _json_bytes(message) + b"\n"
        with self._lock:
            self._binary.write(payload)
            self._binary.flush()


class _Diag:
    """Thread-safe stderr diagnostics that never carry secret material."""

    def __init__(self, err: TextIO):
        self._err = err
        self._lock = threading.Lock()

    def info(self, message: str) -> None:
        self._write("info", message)

    def warn(self, message: str) -> None:
        self._write("warn", message)

    def _write(self, level: str, message: str) -> None:
        with self._lock:
            self._err.write(f"[{PROFILE_NAME}] {level}: {message}\n")
            self._err.flush()


class _JsonLineReader:
    """Bounded newline-delimited JSON-RPC reader over a binary stream.

    Uses line framing (the MCP stdio transport delimits messages by newline).
    A greedy block read would stall on interactive pipes, so each message is
    read one line at a time with a hard size cap.
    """

    def __init__(self, stdin: TextIO, max_bytes: int):
        self._stream = getattr(stdin, "buffer", stdin)
        self._max_bytes = max_bytes

    def __iter__(self) -> Iterator[dict[str, Any]]:
        return self

    def __next__(self) -> dict[str, Any]:
        while True:
            limit = self._max_bytes + 1
            line = self._stream.readline(limit)
            if not line:
                raise StopIteration
            has_newline = line.endswith(b"\n")
            content = line[:-1] if has_newline else line
            if not has_newline and len(line) >= limit:
                raise AdapterError("stdio message exceeds max_message_bytes")
            if not content.strip():
                continue  # blank padding line between messages
            if len(content) > self._max_bytes:
                raise AdapterError("stdio message exceeds max_message_bytes")
            return _parse_line(content)


def _parse_line(line: bytes) -> dict[str, Any]:
    stripped = line.strip()
    if not stripped:
        raise AdapterError("empty stdio message")
    try:
        message = json.loads(stripped.decode("utf-8"))
    except (ValueError, UnicodeDecodeError) as exc:
        raise AdapterError(f"malformed JSON-RPC stdio message: {exc}") from exc
    if not isinstance(message, dict):
        raise AdapterError("stdio JSON message is not an object")
    return message


class _SseParser:
    """Minimal SSE parser over an ``HTTPResponse`` body.

    Yields ``(event_name, data)`` tuples; comments and ``retry``/``id`` fields
    are consumed per the SSE standard (ids are not used by this adapter: it
    does not request stream resumption with ``Last-Event-ID``).
    """

    def __init__(self, response: HTTPResponse):
        self._response = response

    def events(self) -> Iterator[tuple[str, str]]:
        event_name = "message"
        data_lines: list[str] = []
        while True:
            raw = self._response.readline()
            if not raw:
                if data_lines:
                    yield event_name, "\n".join(data_lines)
                return
            line = raw.rstrip(b"\r\n")
            if not line:
                if data_lines:
                    yield event_name, "\n".join(data_lines)
                    event_name = "message"
                    data_lines = []
                continue
            if line.startswith(b":"):
                continue
            if line.startswith(b"event:"):
                event_name = line[len(b"event:") :].strip().decode("utf-8", "replace")
            elif line.startswith(b"data:"):
                data_lines.append(
                    line[len(b"data:") :].lstrip(b" ").decode("utf-8", "replace")
                )
            # id:/retry: lines carry no message payload for this adapter.


class _Exchange:
    """Owns one HTTP connection + response pair and guarantees cleanup."""

    def __init__(self, connection: HTTPConnection, response: HTTPResponse):
        self.connection = connection
        self.response = response

    @property
    def status(self) -> int:
        return self.response.status

    def header(self, name: str) -> str | None:
        return self.response.getheader(name)

    def content_type(self) -> str:
        return self.response.getheader("Content-Type", "").lower()

    def sse_events(self) -> Iterator[tuple[str, str]]:
        return _SseParser(self.response).events()

    def read_json_body(self) -> dict[str, Any] | None:
        """Read a bounded JSON body only when its framing is bounded.

        Returns None (without reading) when the response declares no
        Content-Length or Transfer-Encoding, which avoids hanging on a
        keep-alive response with no body.
        """
        content_length = self.response.getheader("Content-Length")
        transfer_encoding = self.response.getheader("Transfer-Encoding")
        bounded = content_length is not None or transfer_encoding is not None
        if not bounded:
            return None
        try:
            if content_length is not None and int(content_length) > 1024 * 1024:
                return None
            raw = self.response.read(1024 * 1024 + 1)
        except TimeoutError as exc:
            raise HttpTransportError(
                "timeout", None, "response read timed out"
            ) from exc
        except (OSError, ValueError, HTTPException):
            return None
        try:
            parsed = json.loads(raw.decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            return None
        return parsed if isinstance(parsed, dict) else None

    def close(self) -> None:
        try:
            self.response.close()
        except OSError:
            pass
        try:
            self.connection.close()
        except OSError:
            pass


class StreamableHttpClient:
    """One logical Streamable HTTP MCP session towards the remote endpoint.

    Concurrent POSTs are safe: session/protocol state is guarded by a lock and
    a session loss (HTTP 404) triggers exactly one fresh initialization.
    """

    def __init__(self, options: AdapterOptions, diag: _Diag):
        self.options = options
        self.diag = diag
        parsed = urllib.parse.urlsplit(options.endpoint_url)
        self._scheme = parsed.scheme
        self._host = parsed.hostname or ""
        self._port = parsed.port or (443 if parsed.scheme == "https" else 80)
        self._path = parsed.path or "/"
        if parsed.query:
            self._path = f"{self._path}?{parsed.query}"
        self._static_headers = {
            key: value
            for key, value in options.headers.items()
            if key.lower() not in _RESERVED_HEADERS
        }
        self._lock = threading.Lock()
        self._reinit_lock = threading.Lock()
        self._session_id: str | None = None
        self._negotiated_version: str | None = None
        self._initialized = False
        self._generation = 0

    # -- connection helpers ------------------------------------------------

    def _open(self, timeout_s: float) -> HTTPConnection:
        if self._scheme == "https":
            from http.client import HTTPSConnection

            return HTTPSConnection(
                self._host,
                self._port,
                timeout=timeout_s,
                context=ssl.create_default_context(),
            )
        return HTTPConnection(self._host, self._port, timeout=timeout_s)

    def _request_headers(
        self, *, accept: str, session_id: str | None, version_value: str | None
    ) -> dict[str, str]:
        headers = dict(self._static_headers)
        headers["Accept"] = accept
        if session_id:
            headers[SESSION_HEADER] = session_id
        if version_value:
            headers[PROTOCOL_HEADER] = version_value
        return headers

    def _post(
        self, message: dict[str, Any], headers: dict[str, str], timeout_s: float
    ) -> _Exchange:
        payload = _json_bytes(message)
        headers["Content-Type"] = CONTENT_TYPE_JSON
        connection = self._open(timeout_s)
        try:
            connection.request("POST", self._path, body=payload, headers=headers)
            response = connection.getresponse()
            return _Exchange(connection, response)
        except (OSError, ssl.SSLError, HTTPException) as exc:
            connection.close()
            raise HttpTransportError(
                "transport", None, f"POST to endpoint failed: {exc}"
            ) from exc

    # -- session management ---------------------------------------------------

    def session_snapshot(self) -> tuple[str | None, int]:
        with self._lock:
            return self._session_id, self._generation

    def initialized(self) -> bool:
        with self._lock:
            return self._initialized

    def initialize(self, message: dict[str, Any]) -> list[dict[str, Any]]:
        """POST one ``initialize``; capture session id and negotiated version.

        Returns the messages to relay to the local client (normally the single
        InitializeResult response, possibly a JSON-RPC error).

        Lock contract: callers must NOT hold ``self._lock`` across this call.
        ``self._lock`` guards only the short state update at the tail; the
        network round trip happens without it so a slow endpoint never blocks
        unrelated readers.  ``reinitialize_session`` serializes whole
        re-initializations with the separate ``self._reinit_lock``.
        """
        headers = self._request_headers(
            accept=ACCEPT_JSON_AND_EVENT, session_id=None, version_value=None
        )
        exchange = self._post(message, headers, self.options.request_timeout_s)
        try:
            if exchange.status != 200:
                raise self._http_error(exchange)
            new_session = exchange.header(SESSION_HEADER)
            messages = self._consume_request_body(message, exchange)
            with self._lock:
                result = messages[-1] if messages else None
                if (
                    isinstance(result, dict)
                    and isinstance(result.get("result"), dict)
                    and isinstance(result["result"].get("protocolVersion"), str)
                ):
                    self._negotiated_version = result["result"]["protocolVersion"]
                    if new_session:
                        self._session_id = new_session
                        self._generation += 1
                    self._initialized = True
                    return messages
                if isinstance(result, dict) and "error" in result:
                    return messages
                raise AdapterError("initialize produced no JSON-RPC response")
        finally:
            exchange.close()

    def reinitialize_session(self) -> bool:
        """Spec-mandated fresh session: ``initialize`` with no session id.

        Exactly-once under concurrency.  Lock order is always
        ``self._reinit_lock`` -> ``self._lock``:

        * the state guard/clear runs under ``self._reinit_lock`` plus a brief
          ``self._lock`` critical section;
        * ``self._lock`` is released *before* the probe ``initialize()`` POST,
          and ``self._reinit_lock`` is held across it so parallel 404
          recoveries serialize into one fresh ``initialize``; later callers
          observe the new live session and no-op.

        Holding ``self._lock`` across ``initialize()`` would deadlock against
        its tail re-acquisition, so that must never happen.
        """
        with self._reinit_lock:
            with self._lock:
                if self._initialized and self._session_id:
                    return True
                self._session_id = None
                self._initialized = False
                self._generation += 1
            # self._lock released; the network probe below holds only
            # self._reinit_lock (never self._lock).
            probe = {
                "jsonrpc": "2.0",
                "id": "adapter-reinitialize",
                "method": "initialize",
                "params": {
                    "protocolVersion": self.options.protocol_version,
                    "capabilities": {},
                    "clientInfo": {"name": PROFILE_NAME, "version": ADAPTER_VERSION},
                },
            }
            try:
                messages = self.initialize(probe)
                return bool(messages) and "error" not in messages[-1]
            except AdapterError as exc:
                self.diag.warn(f"session re-initialization failed ({exc})")
                return False

    def _recover_session_loss(self, failed_session: str | None) -> bool:
        """Return True when the caller may retry on the now-live session.

        A response is correlated with the session id its request actually used:
        a 404 for a *stale* session (one already replaced by a newer live
        session) is benign and never tears the healthy session down; a 404 for
        the current session invalidates it and performs exactly one serialized
        fresh initialization.
        """
        with self._lock:
            current = self._session_id
            initialized = self._initialized
        if initialized and current and current != failed_session:
            return True  # a newer live session already exists
        with self._lock:
            if self._session_id == failed_session or not self._initialized:
                self._session_id = None
                self._initialized = False
                self._generation += 1
        return self.reinitialize_session()

    # -- message POSTing ---------------------------------------------------------

    def post(
        self, message: dict[str, Any], *, retry_session_loss: bool = True
    ) -> list[dict[str, Any]]:
        """POST one JSON-RPC message; returns messages to relay locally.

        Requests get a response (JSON or SSE).  Notifications and responses
        are accepted with HTTP 202 and return an empty list.
        """
        kind = _classify(message)
        with self._lock:
            used_session = self._session_id
            used_version = self._negotiated_version
        headers = self._request_headers(
            accept=ACCEPT_JSON_AND_EVENT,
            session_id=used_session,
            version_value=used_version,
        )
        timeout = (
            self.options.request_timeout_s if kind == "request" else self.options.idle_timeout_s
        )
        exchange = self._post(message, headers, timeout)
        try:
            if kind == "request":
                if exchange.status == 200:
                    return self._consume_request_body(message, exchange)
                error = self._http_error(exchange)
                if retry_session_loss and isinstance(error, SessionLostError):
                    if self._recover_session_loss(used_session):
                        return self.post(message, retry_session_loss=False)
                    raise SessionLostError(
                        "downstream session terminated (HTTP 404) and a fresh "
                        "session could not be established"
                    )
                raise error
            # notification or response: HTTP 202 Accepted with no body.
            if exchange.status in (200, 202, 204):
                return []
            error = self._http_error(exchange)
            self.diag.warn(f"{kind} POST failed ({error.category}, http={error.http_status})")
            if isinstance(error, SessionLostError):
                # A notification/response has no retry semantics of its own, but
                # keep a live session for subsequent requests: recover only when
                # the 404 was for the current session (never a stale one).
                self._recover_session_loss(used_session)
            return []
        finally:
            exchange.close()

    def _consume_request_body(
        self, message: dict[str, Any], exchange: _Exchange
    ) -> list[dict[str, Any]]:
        content_type = exchange.content_type()
        try:
            if CONTENT_TYPE_JSON in content_type:
                body = exchange.read_json_body()
                if body is None:
                    # A JSON content type with no readable object body.
                    raise HttpTransportError(
                        "bad-response", exchange.status, "unreadable JSON response body"
                    )
                return [body]
            if CONTENT_TYPE_EVENT in content_type:
                request_id = message.get("id")
                outbound: list[dict[str, Any]] = []
                for event_name, data in exchange.sse_events():
                    if event_name != "message":
                        continue
                    try:
                        payload = json.loads(data)
                    except ValueError:
                        self.diag.warn("dropping undecodable SSE event payload")
                        continue
                    if not isinstance(payload, dict):
                        continue
                    payload_kind = _classify(payload)
                    if payload_kind == "response" and payload.get("id") == request_id:
                        outbound.append(payload)
                        return outbound
                    if payload_kind == "response":
                        # A response for another request must not arrive on this
                        # stream; never forward it (it would duplicate).
                        self.diag.warn("dropping unrelated response on POST stream")
                        continue
                    outbound.append(payload)
                raise AdapterError(
                    "SSE response stream ended before the request response arrived"
                )
            raise AdapterError(f"unexpected response content type: {content_type}")
        except TimeoutError as exc:
            raise HttpTransportError(
                "timeout", None, "response read timed out"
            ) from exc
        except HTTPException as exc:
            raise HttpTransportError(
                "bad-response", exchange.status, "malformed HTTP response stream"
            ) from exc

    def _http_error(self, exchange: _Exchange) -> HttpTransportError:
        status = exchange.status
        if status == 400:
            # Preserve the remote's precise JSON-RPC error when one is present.
            body = exchange.read_json_body()
            if isinstance(body, dict) and isinstance(body.get("error"), dict):
                error = body["error"]
                if isinstance(error, dict) and isinstance(error.get("code"), int):
                    return HttpTransportError(
                        "remote-error",
                        status,
                        f"remote JSON-RPC error {error.get('code')}: {error.get('message', '')}",
                    )
            return HttpTransportError("bad-request", status, "HTTP 400 Bad Request")
        if status in (401, 403):
            return HttpTransportError(
                "authentication", status, "HTTP authentication failure"
            )
        if status == 404:
            # Session loss is recovered by the caller, which correlates the
            # response with the session id its request actually used (see
            # ``_recover_session_loss``), so a stale 404 never tears down a
            # newer healthy session.
            return SessionLostError("HTTP 404 session not found")
        if status == 405:
            return HttpTransportError("method-not-allowed", status, "HTTP 405")
        if status == 406:
            return HttpTransportError("not-acceptable", status, "HTTP 406")
        if status == 429:
            return HttpTransportError("rate-limited", status, "HTTP 429 Too Many Requests")
        if status >= 500:
            return HttpTransportError("server", status, f"HTTP {status}")
        return HttpTransportError("http", status, f"HTTP {status}")

    # -- SSE server push channel ---------------------------------------------

    def open_server_stream(
        self,
        on_message: Callable[[dict[str, Any]], None],
        should_stop: Callable[[], bool],
    ) -> None:
        """Blocking keep-alive loop for server-initiated messages (daemon)."""
        failures = 0
        while not should_stop():
            session_id, generation = self.session_snapshot()
            if session_id is None:
                # No session yet: wait for initialization or shutdown.
                if self._wait_for_session(generation, should_stop):
                    continue
                time.sleep(0.2)
                continue
            with self._lock:
                session_id = self._session_id
                negotiated_version = self._negotiated_version
            headers = self._request_headers(
                accept=ACCEPT_EVENT,
                session_id=session_id,
                version_value=negotiated_version,
            )
            connection = self._open(self.options.sse_idle_timeout_s)
            try:
                connection.request("GET", self._path, headers=headers)
                response = connection.getresponse()
                exchange = _Exchange(connection, response)
                status = exchange.status
                content_type = exchange.content_type()
                if status == 405:
                    self.diag.info(
                        "remote does not offer a GET SSE stream; "
                        "server-initiated messages are unavailable"
                    )
                    return
                if status == 404:
                    exchange.close()
                    # Same session-loss recovery as POSTs: correlated with the
                    # session id this GET used, serialized with other 404s.
                    self._recover_session_loss(session_id)
                    if self._wait_for_session(generation, should_stop):
                        continue
                    failures += 1
                    if failures >= self.options.sse_max_continuous_failures:
                        return
                    continue
                if status in (401, 403):
                    self.diag.warn(
                        "GET SSE stream authentication failed; push channel closed"
                    )
                    exchange.close()
                    return
                if status != 200 or CONTENT_TYPE_EVENT not in content_type:
                    self.diag.warn(f"GET SSE stream returned HTTP {status}; retrying")
                    exchange.close()
                    failures += 1
                    if failures >= self.options.sse_max_continuous_failures:
                        return
                    time.sleep(
                        min(2**failures, self.options.sse_reconnect_max_delay_s)
                    )
                    continue
                failures = 0
                try:
                    for event_name, data in exchange.sse_events():
                        if should_stop():
                            return
                        if event_name != "message":
                            continue
                        try:
                            payload = json.loads(data)
                        except ValueError:
                            self.diag.warn("dropping undecodable SSE event payload")
                            continue
                        if not isinstance(payload, dict):
                            continue
                        on_message(payload)
                finally:
                    exchange.close()
            except (OSError, ssl.SSLError, HTTPException, AdapterError) as exc:
                if should_stop():
                    return
                self.diag.warn(f"GET SSE stream interrupted ({exc}); reconnecting")
                failures += 1
                if failures >= self.options.sse_max_continuous_failures:
                    self.diag.warn("GET SSE stream gave up after repeated failures")
                    return
                time.sleep(min(2**failures, self.options.sse_reconnect_max_delay_s))
            except Exception:  # pragma: no cover - defensive daemon guard
                if should_stop():
                    return
                failures += 1
                if failures >= self.options.sse_max_continuous_failures:
                    return
                time.sleep(min(2**failures, self.options.sse_reconnect_max_delay_s))

    def _wait_for_session(self, generation: int, should_stop: Callable[[], bool]) -> bool:
        deadline = time.monotonic() + self.options.connect_timeout_s
        while not should_stop() and time.monotonic() < deadline:
            _session_id, current = self.session_snapshot()
            if _session_id is not None and current != generation:
                return True
            time.sleep(0.1)
        return False

    def terminate_session(self) -> None:
        """Best-effort HTTP DELETE to end the remote session (spec SHOULD)."""
        session_id, _generation = self.session_snapshot()
        if not session_id:
            return
        headers = dict(self._static_headers)
        headers[SESSION_HEADER] = session_id
        if self._negotiated_version:
            headers[PROTOCOL_HEADER] = self._negotiated_version
        connection = self._open(self.options.connect_timeout_s)
        try:
            connection.request("DELETE", self._path, headers=headers)
            response = connection.getresponse()
            if response.status not in (200, 202, 204, 405):
                self.diag.warn(f"session DELETE returned HTTP {response.status}")
            response.close()
        except (OSError, ssl.SSLError, HTTPException) as exc:
            self.diag.warn(f"session DELETE failed: {exc}")
        finally:
            connection.close()
        with self._lock:
            self._session_id = None
            self._initialized = False
            self._generation += 1


class StdioStreamableHttpAdapter:
    """Glue: stdio JSON-RPC lines <-> one remote Streamable HTTP session."""

    def __init__(self, options: AdapterOptions, out: TextIO, diag: _Diag):
        self.options = options
        self.writer = _LockedWriter(out)
        self.diag = diag
        self.client = StreamableHttpClient(options, diag)
        self._stop = threading.Event()
        self._pool = ThreadPoolExecutor(
            max_workers=options.max_inflight, thread_name_prefix="adapter-post"
        )
        self._sse_thread: threading.Thread | None = None

    def start(self) -> None:
        self._sse_thread = threading.Thread(
            target=self.client.open_server_stream,
            args=(self._on_server_message, self._stop.is_set),
            name="adapter-sse",
            daemon=True,
        )
        self._sse_thread.start()

    def _on_server_message(self, payload: dict[str, Any]) -> None:
        if _classify(payload) == "response":
            # The spec forbids responses on the GET stream except when resuming
            # a previous POST stream, which this adapter never requests.
            self.diag.warn("dropping unexpected JSON-RPC response on GET SSE stream")
            return
        self.writer.write_message(payload)

    def handle(self, message: dict[str, Any]) -> None:
        kind = _classify(message)
        if kind == "request" and message.get("method") == "initialize":
            # Initialization is ordered: it runs inline on the stdin reader.
            self._handle_initialize(message)
            return
        if kind == "request":
            self._pool.submit(self._post_request, message)
            return
        if kind == "notification":
            self._pool.submit(self._post_notification, message)
            return
        self._pool.submit(self._post_response, message)

    # -- outbound paths --------------------------------------------------------

    def _handle_initialize(self, message: dict[str, Any]) -> None:
        try:
            messages = self.client.initialize(message)
        except AdapterError as exc:
            self.writer.write_message(_error_response(message.get("id"), exc))
            return
        for item in messages:
            self.writer.write_message(item)

    def _post_request(self, message: dict[str, Any]) -> None:
        try:
            messages = self.client.post(message)
        except AdapterError as exc:
            self.writer.write_message(_error_response(message.get("id"), exc))
            return
        for item in messages:
            self.writer.write_message(item)

    def _post_notification(self, message: dict[str, Any]) -> None:
        try:
            self.client.post(message)
        except AdapterError as exc:
            self.diag.warn(
                f"notification {message.get('method')!r} failed "
                f"({getattr(exc, 'category', exc)})"
            )

    def _post_response(self, message: dict[str, Any]) -> None:
        try:
            self.client.post(message)
        except AdapterError as exc:
            self.diag.warn(f"relaying JSON-RPC response failed ({exc})")

    # -- shutdown -----------------------------------------------------------------

    def shutdown(self) -> None:
        self._stop.set()
        self.client.terminate_session()
        self._pool.shutdown(wait=False, cancel_futures=True)


class _ModernTransportError(HttpTransportError):
    """Carries execution uncertainty even without the stdio adapter layer."""

    def __init__(self, failure: HttpTransportError, outcome_unknown: bool):
        super().__init__(failure.category, failure.http_status, str(failure))
        self.outcome_unknown = outcome_unknown

    def to_jsonrpc(self, request_id: Any) -> dict[str, Any]:
        reply = super().to_jsonrpc(request_id)
        reply["error"]["data"] = {
            **(reply["error"]["data"] or {}), "outcomeUnknown": self.outcome_unknown,
        }
        return reply


class _ModernRpcError(AdapterError):
    """Local validation failure with a fixed, non-sensitive diagnostic."""

    def __init__(self, code: int, message: str, data: Any = None):
        self.code, self.message, self.data = code, message, data

    def response(self, request_id: Any) -> dict[str, Any]:
        error = {"code": self.code, "message": self.message,
                 "data": {**(self.data if isinstance(self.data, dict) else {}), "outcomeUnknown": False}}
        return {"jsonrpc": "2.0", "id": request_id, "error": error}


def _modern_request_id(value: Any) -> bool:
    if type(value) not in (str, int, float):
        return False
    try:
        # Invalid Unicode or non-JSON numbers cannot safely be echoed in an
        # error response; JSON-RPC uses null when the id cannot be recovered.
        json.dumps(value, ensure_ascii=False, allow_nan=False).encode("utf-8")
    except (ValueError, UnicodeError):
        return False
    return True


def _modern_json(raw: bytes) -> Any:
    def invalid_constant(_value: str) -> None:
        raise ValueError("non-JSON numeric constant")
    return json.loads(raw.decode("utf-8"), parse_constant=invalid_constant)


def _modern_header(value: str) -> str:
    if (value != value.strip() or any(ord(c) < 32 or ord(c) > 126 for c in value)
            or (value.startswith("=?base64?") and value.endswith("?="))):
        return "=?base64?" + base64.b64encode(value.encode("utf-8")).decode("ascii") + "?="
    return value


def _tool_header_schema(tool: Any) -> list[tuple[tuple[str, ...], str, str]]:
    """Extract primitive annotations reachable solely through properties.

    This is not a general JSON Schema validator. Unsupported/invalid annotation
    placement fails closed; examples/default values are not schemas.
    """
    def invalid() -> _ModernRpcError:
        return _ModernRpcError(-32602, "Unsupported or invalid x-mcp-header tool schema", {
            "category": "unsupported-tool-header-schema", "requiredFeature": "x-mcp-header"})

    if not isinstance(tool, dict) or not isinstance(tool.get("inputSchema"), dict):
        raise invalid()
    stack = [(tool["inputSchema"], (), True)]
    bindings: list[tuple[tuple[str, ...], str, str]] = []
    names: set[str] = set()
    while stack:
        node, path, reachable = stack.pop()
        if isinstance(node, list):
            stack.extend((child, path, False) for child in node)
            continue
        if not isinstance(node, dict):
            continue
        if "x-mcp-header" in node:
            name, kind = node["x-mcp-header"], node.get("type")
            if (not reachable or not path or not isinstance(name, str)
                    or re.fullmatch(r"[!#$%&'*+.^_`|~0-9A-Za-z-]+", name) is None
                    or name.lower() in names or kind not in ("string", "integer", "boolean")):
                raise invalid()
            names.add(name.lower())
            bindings.append((path, "Mcp-Param-" + name, kind))
            if len(bindings) > 64:
                raise invalid()  # bounded profile; never partial mirroring
        for key, value in node.items():
            if key in ("default", "const", "enum", "examples", "x-mcp-header"):
                continue
            if key == "properties" and isinstance(value, dict):
                stack.extend((child, path + (prop,), reachable) for prop, child in value.items())
            elif key in ("patternProperties", "$defs", "definitions", "dependentSchemas") and isinstance(value, dict):
                stack.extend((child, path, False) for child in value.values())
            elif isinstance(value, (dict, list)):
                stack.append((value, path, False))
    return bindings


def _tool_argument_headers(tool: dict[str, Any], arguments: Any) -> dict[str, str]:
    bindings = _tool_header_schema(tool)
    if not isinstance(arguments, dict):
        raise _ModernRpcError(-32602, "Tool arguments must be an object")
    headers = {}
    for path, name, kind in bindings:
        value = arguments
        for key in path:
            if not isinstance(value, dict):
                raise _ModernRpcError(-32602, "Cannot read x-mcp-header property path")
            if key not in value:
                value = None
                break
            value = value[key]
            if value is None:
                break
        if value is None:
            continue
        if kind == "string" and isinstance(value, str):
            text = value
        elif kind == "boolean" and type(value) is bool:
            text = "true" if value else "false"
        elif (kind == "integer" and type(value) in (int, float)
              and -(2 ** 53 - 1) <= value <= 2 ** 53 - 1 and int(value) == value):
            text = str(int(value))
        else:
            raise _ModernRpcError(-32602, "Invalid primitive value for x-mcp-header", {
                "category": "invalid-tool-header-value", "requiredFeature": "x-mcp-header"})
        headers[name] = _modern_header(text)
    return headers


class _ModernPending:
    """Cancellation never closes a buffered reader from a competing thread.

    shutdown(SHUT_RDWR) unblocks headers/body reads; the worker owns close().
    This also works when HTTPConnection detached its socket for a close body.
    """

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.socket: socket.socket | None = None
        self.reason: str | None = None
        self.business_post_started = False

    def begin_business_send(self) -> None:
        with self.lock:
            if self.reason:
                raise HttpTransportError(self.reason, None, self.reason)
            # Mark immediately before the first socket write attempt, after
            # local HTTP header validation. A partial write may execute a call.
            self.business_post_started = True

    def outcome_unknown(self) -> bool:
        with self.lock:
            return self.business_post_started

    def attach(self, sock: socket.socket) -> None:
        with self.lock:
            self.socket = sock
            if self.reason:
                self._interrupt()

    def _interrupt(self) -> None:
        if self.socket is not None:
            try:
                self.socket.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass

    def cancel(self, reason: str = "cancelled") -> None:
        with self.lock:
            if self.reason is None:
                self.reason = reason
            self._interrupt()

    def emit(self, message: dict[str, Any], writer: Callable[[dict[str, Any]], None]) -> None:
        with self.lock:
            if self.reason:
                raise HttpTransportError(self.reason, None, self.reason)
            writer(message)

    def check(self) -> None:
        with self.lock:
            if self.reason:
                raise HttpTransportError(self.reason, None, self.reason)


class _ModernHttpConnection(HTTPConnection):
    def __init__(self, owner: "ModernStreamableHttpClient", pending: _ModernPending,
                 *, business_request: bool = False):
        super().__init__(owner._host, owner._port, timeout=owner.options.connect_timeout_s)
        self.owner, self.pending = owner, pending
        self.business_request = business_request

    def connect(self) -> None:
        self.sock = self.owner.connect_socket(self.pending)

    def send(self, data: Any) -> None:
        if self.business_request and self.sock is not None:
            self.pending.begin_business_send()
        super().send(data)


class ModernStreamableHttpClient:
    """2026-07-28 binding, explicitly selected; no era probing or replay.

    No initialization, session, GET stream or DELETE path exists here. MRTR
    results and all backend identity/instructions are passed through, never
    fulfilled or manufactured here. Each POST owns a cancellable socket.
    """

    def __init__(self, options: AdapterOptions, diag: _Diag):
        self.options, self.diag = options, diag
        parsed = urllib.parse.urlsplit(options.endpoint_url)
        self._scheme, self._host = parsed.scheme, parsed.hostname or ""
        self._port = parsed.port or (443 if self._scheme == "https" else 80)
        self._path = parsed.path or "/"
        if parsed.query:
            self._path += "?" + parsed.query
        self._static_headers = {key: value for key, value in options.headers.items()
                                if key.lower() not in _RESERVED_HEADERS}
        self._resolve_lock = threading.Lock()
        self._resolve_started = False
        self._resolved = threading.Event()
        self._addresses: list[Any] = []

    def _resolve(self) -> None:
        # One bounded resolver worker per adapter, never one per request. libc
        # DNS cannot be interrupted portably; waiting callers remain cancellable
        # and this daemon cannot hold up EOF. Its answer is pinned for this
        # adapter process, just like its explicitly configured endpoint.
        try:
            self._addresses = socket.getaddrinfo(self._host, self._port, type=socket.SOCK_STREAM)[:16]
        except OSError:
            pass
        finally:
            self._resolved.set()

    def connect_socket(self, pending: _ModernPending) -> socket.socket:
        deadline = time.monotonic() + self.options.connect_timeout_s
        with self._resolve_lock:
            if not self._resolve_started:
                self._resolve_started = True
                threading.Thread(target=self._resolve, name="modern-resolver", daemon=True).start()
        while not self._resolved.wait(0.025):
            pending.check()
            if time.monotonic() >= deadline:
                raise TimeoutError()
        pending.check()
        for family, socktype, proto, _canon, address in self._addresses:
            sock = socket.socket(family, socktype, proto)
            try:
                pending.attach(sock)
                pending.check()
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError()
                sock.settimeout(remaining)
                sock.connect(address)
                pending.check()
                if self._scheme == "https":
                    sock = ssl.create_default_context().wrap_socket(
                        sock, server_hostname=self._host, do_handshake_on_connect=False)
                    pending.attach(sock)
                    pending.check()
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise TimeoutError()
                    sock.settimeout(remaining)
                    sock.do_handshake()
                pending.check()
                return sock
            except (OSError, AdapterError):
                sock.close()
                pending.check()
                if time.monotonic() >= deadline:
                    raise TimeoutError() from None
        raise OSError("endpoint connection failed")

    def request_headers(self, message: dict[str, Any]) -> dict[str, str]:
        try:
            encoded = json.dumps(message, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode("utf-8")
        except (ValueError, UnicodeError, RecursionError):
            raise _ModernRpcError(-32600, "Invalid modern JSON-RPC encoding") from None
        if len(encoded) > self.options.max_message_bytes:
            raise _ModernRpcError(-32000, "Modern request-too-large")
        request_id = message.get("id")
        method = message.get("method")
        if (message.get("jsonrpc") != "2.0" or not _modern_request_id(request_id)
                or not isinstance(method, str) or not method
                or any(ord(c) < 33 or ord(c) > 126 for c in method)):
            raise _ModernRpcError(-32600, "Invalid modern JSON-RPC request")
        if method == "initialize":
            raise _ModernRpcError(-32601, "initialize is unavailable in modern mode (2026-07-28)")
        params = message.get("params")
        meta = params.get("_meta") if isinstance(params, dict) else None
        if (not isinstance(meta, dict) or not isinstance(meta.get(MODERN_VERSION_META), str)
                or not isinstance(meta.get(MODERN_CAPABILITIES_META), dict)):
            raise _ModernRpcError(-32602, "Modern requests require protocolVersion and clientCapabilities in params._meta")
        if meta[MODERN_VERSION_META] != MODERN_PROTOCOL_VERSION:
            raise _ModernRpcError(-32022, "Unsupported protocol version", {
                "supported": [MODERN_PROTOCOL_VERSION], "requested": meta[MODERN_VERSION_META],
            })
        headers = {
            key: value for key, value in self._static_headers.items()
            if key.lower() not in ("mcp-method", "mcp-name", "last-event-id", "connection", "transfer-encoding", "host")
            and not key.lower().startswith("mcp-param-")
        }
        headers.update({"Accept": ACCEPT_JSON_AND_EVENT, "Content-Type": CONTENT_TYPE_JSON,
                        PROTOCOL_HEADER: meta[MODERN_VERSION_META], "Mcp-Method": method})
        if method in ("tools/call", "resources/read", "prompts/get"):
            name = params.get("uri" if method == "resources/read" else "name")
            if not isinstance(name, str) or not name:
                raise _ModernRpcError(-32602, "Modern request requires a name or URI")
            headers["Mcp-Name"] = _modern_header(name)
        return headers

    def _validate_response(self, payload: Any, message: dict[str, Any], *, http_error: bool = False,
                           filter_tools: bool = True) -> dict[str, Any]:
        if (not isinstance(payload, dict) or payload.get("jsonrpc") != "2.0"
                or "method" in payload or ("result" in payload) == ("error" in payload)
                or ("id" in payload and (not _modern_request_id(payload["id"])
                                         or isinstance(payload["id"], str) != isinstance(message["id"], str)
                                         or payload["id"] != message["id"]))
                or ("id" not in payload and not http_error)):
            raise HttpTransportError("bad-response", None, "invalid response envelope")
        if "error" in payload:
            error = payload["error"]
            if (not isinstance(error, dict) or type(error.get("code")) is not int
                    or not isinstance(error.get("message"), str)):
                raise HttpTransportError("bad-response", None, "invalid error")
            return dict(payload, id=message["id"])
        result = payload["result"]
        if http_error or not isinstance(result, dict) or not isinstance(result.get("resultType"), str):
            raise HttpTransportError("bad-response", None, "modern resultType required")
        if filter_tools and message["method"] == "tools/list" and result["resultType"] == "complete":
            tools = result.get("tools")
            if not isinstance(tools, list):
                raise HttpTransportError("bad-response", None, "tools/list requires tools array")
            accepted = []
            for tool in tools:
                try:
                    _tool_header_schema(tool)
                except _ModernRpcError:
                    self.diag.warn("excluding tool with unsupported or invalid x-mcp-header schema")
                else:
                    accepted.append(tool)
            return dict(payload, result=dict(result, tools=accepted))
        return payload

    def _notification(self, payload: Any, message: dict[str, Any]) -> bool:
        if not isinstance(payload, dict) or payload.get("jsonrpc") != "2.0" or "id" in payload:
            return False
        method, params = payload.get("method"), payload.get("params")
        if not isinstance(method, str) or not isinstance(params, dict):
            return False
        meta = message["params"]["_meta"]
        if method == "notifications/progress":
            return ("progressToken" in meta and type(params.get("progressToken")) is type(meta["progressToken"])
                    and params.get("progressToken") == meta["progressToken"])
        if method == "notifications/message":
            return "io.modelcontextprotocol/logLevel" in meta
        if message["method"] == "subscriptions/listen":
            notice_meta = params.get("_meta", {})
            return (isinstance(notice_meta, dict)
                    and notice_meta.get("io.modelcontextprotocol/subscriptionId") == message["id"])
        return False

    def _preflight_tool_headers(self, message: dict[str, Any], pending: _ModernPending) -> dict[str, str]:
        """At most four read-only catalog pages; no cache or business retry.

        Use the originating request metadata verbatim, so capabilities and
        identity never come from another request. Preflight messages stay off
        stdio and share the original cancellation token and outer deadline.
        """
        cursor = None
        seen_cursors: set[str] = set()
        total_bytes = 0
        for page in range(4):
            params = {"_meta": message["params"]["_meta"]}
            if cursor is not None:
                params["cursor"] = cursor
            probe = {"jsonrpc": "2.0", "id": f"adapter-schema-{threading.get_ident()}-{page}",
                     "method": "tools/list", "params": params}
            replies = []
            def collect(item: dict[str, Any]) -> None:
                if "result" in item or "error" in item:
                    replies.append(item)
            self.request(probe, pending, collect, filter_tools=False)
            pending.check()
            if len(replies) != 1 or "result" not in replies[0]:
                raise _ModernRpcError(-32000, "Tool schema preflight failed", {"category": "tool-schema-preflight"})
            result = replies[0]["result"]
            total_bytes += len(_json_bytes(result))
            if total_bytes > self.options.max_message_bytes:
                raise _ModernRpcError(-32000, "Tool schema preflight byte limit exceeded")
            tools = result.get("tools")
            if result["resultType"] != "complete" or not isinstance(tools, list):
                raise _ModernRpcError(-32000, "Tool schema preflight requires a complete catalog page")
            matches = [tool for tool in tools if isinstance(tool, dict) and tool.get("name") == message["params"]["name"]]
            if len(matches) > 1:
                raise _ModernRpcError(-32602, "Ambiguous tool schema in preflight")
            if matches:
                return _tool_argument_headers(matches[0], message["params"].get("arguments", {}))
            cursor = result.get("nextCursor")
            if cursor is None:
                raise _ModernRpcError(-32602, "Tool schema unavailable; business call not sent")
            if not isinstance(cursor, str) or not cursor or cursor in seen_cursors:
                raise _ModernRpcError(-32000, "Invalid tool schema pagination cursor")
            seen_cursors.add(cursor)
        raise _ModernRpcError(-32000, "Tool schema preflight page limit exceeded; business call not sent")

    def request(self, message: dict[str, Any], pending: _ModernPending,
                emit: Callable[[dict[str, Any]], None], *, filter_tools: bool = True) -> None:
        try:
            self._request_once(message, pending, emit, filter_tools=filter_tools)
        except HttpTransportError as exc:
            # Snapshot at the transport boundary, not just when writing stdio.
            # Nested tools/list preflights share cancellation but never mark
            # business send; their errors therefore remain known nonexecution.
            raise _ModernTransportError(exc, pending.outcome_unknown()) from None

    def _request_once(self, message: dict[str, Any], pending: _ModernPending,
                      emit: Callable[[dict[str, Any]], None], *, filter_tools: bool = True) -> None:
        headers = self.request_headers(message)
        payload = _json_bytes(message)
        if len(payload) > self.options.max_message_bytes:
            raise HttpTransportError("request-too-large", None, "request exceeds bound")
        pending.check()
        connection = _ModernHttpConnection(self, pending, business_request=message["method"] == "tools/call")
        timer = threading.Timer(self.options.request_timeout_s, pending.cancel, args=("timeout",))
        timer.daemon = True
        response = None
        timer.start()
        try:
            if message["method"] == "tools/call":
                headers.update(self._preflight_tool_headers(message, pending))
                if sum(len(k.encode("ascii")) + len(v.encode("utf-8")) for k, v in headers.items()) > self.options.max_message_bytes:
                    raise _ModernRpcError(-32000, "Generated request headers exceed byte limit")
            pending.check()
            connection.connect()
            pending.attach(connection.sock)
            pending.check()
            connection.sock.settimeout(min(self.options.idle_timeout_s, self.options.request_timeout_s))
            connection.request("POST", self._path, body=payload, headers=headers)
            response = connection.getresponse()
            status = response.status
            content_type = response.getheader("Content-Type", "").split(";", 1)[0].strip().lower()
            if status != 200:
                # Never echo arbitrary HTTP bodies or response headers. Preserve
                # normative modern error codes with bounded, allowlisted data.
                error = None
                if content_type == CONTENT_TYPE_JSON:
                    try:
                        body = self._read_json(response)
                        body = self._validate_response(body, message, http_error=True)
                        error = body.get("error")
                    except HttpTransportError:
                        pass
                if error and error["code"] in (-32020, -32021, -32022, -32601):
                    names = {-32020: "Header mismatch", -32021: "Missing required client capability",
                             -32022: "Unsupported protocol version", -32601: "Method not found"}
                    data: dict[str, Any] = {"httpStatus": status}
                    if error["code"] == -32022:
                        details = error.get("data")
                        if isinstance(details, dict):
                            supported = details.get("supported")
                            if isinstance(supported, list) and all(
                                isinstance(v, str) and len(v) == 10 and v[4] == "-" and v[7] == "-"
                                and v.replace("-", "").isdigit() for v in supported
                            ):
                                data["supported"] = supported
                            data["requested"] = message["params"]["_meta"][MODERN_VERSION_META]
                    emit(_ModernRpcError(error["code"], names[error["code"]], data).response(message["id"]))
                    return
                raise HttpTransportError("http", status, "HTTP request failed")
            if content_type == CONTENT_TYPE_JSON:
                result = self._validate_response(self._read_json(response), message, filter_tools=filter_tools)
                pending.check()
                emit(result)
                return
            if content_type != CONTENT_TYPE_EVENT:
                raise HttpTransportError("bad-response", status, "unexpected response content type")
            # Both each line and the total stream have a hard byte cap. Comments
            # count too, so an endless heartbeat stream cannot bypass bounds.
            remaining = self.options.max_message_bytes
            lines: list[bytes] = []
            event = b"message"
            while True:
                raw = response.readline(remaining + 1)
                remaining -= len(raw)
                pending.check()
                if remaining < 0:
                    raise HttpTransportError("response-too-large", status, "SSE exceeds bound")
                if not raw:
                    raise HttpTransportError("bad-response", status, "SSE ended without response")
                line = raw.rstrip(b"\r\n")
                if not line:
                    if lines and event == b"message":
                        try:
                            item = _modern_json(b"\n".join(lines))
                        except (ValueError, UnicodeError, RecursionError):
                            raise HttpTransportError("bad-response", status, "invalid SSE JSON") from None
                        if isinstance(item, dict) and "method" in item:
                            if not self._notification(item, message):
                                raise HttpTransportError("bad-response", status, "unrelated SSE message")
                            emit(item)
                        else:
                            emit(self._validate_response(item, message, filter_tools=filter_tools))
                            return
                    lines, event = [], b"message"
                elif line.startswith(b"data:"):
                    value = line[5:]
                    lines.append(value[1:] if value.startswith(b" ") else value)
                elif line.startswith(b"event:"):
                    event = line[6:].strip()
        except TimeoutError:
            pending.cancel("timeout")
            raise HttpTransportError("timeout", None, "HTTP exchange timed out") from None
        except (OSError, HTTPException, ValueError, UnicodeError):
            pending.check()
            raise HttpTransportError("transport", None, "HTTP exchange failed") from None
        finally:
            timer.cancel()
            if response is not None:
                response.close()
            connection.close()

    def _read_json(self, response: HTTPResponse) -> Any:
        bound = self.options.max_message_bytes
        length = response.getheader("Content-Length")
        if length is not None:
            try:
                if int(length) < 0 or int(length) > bound:
                    raise HttpTransportError("response-too-large", response.status, "JSON exceeds bound")
            except ValueError:
                raise HttpTransportError("bad-response", response.status, "invalid content length") from None
        raw = response.read(bound + 1)
        if len(raw) > bound:
            raise HttpTransportError("response-too-large", response.status, "JSON exceeds bound")
        try:
            return _modern_json(raw)
        except (ValueError, UnicodeError, RecursionError):
            raise HttpTransportError("bad-response", response.status, "invalid JSON") from None


class ModernStdioStreamableHttpAdapter:
    """Bounded independent modern requests; stdio cancellation stays local."""

    def __init__(self, options: AdapterOptions, out: TextIO, diag: _Diag):
        self.options, self.diag = options, diag
        self.writer = _LockedWriter(out)
        self.client = ModernStreamableHttpClient(options, diag)
        self._lock = threading.Lock()
        self._pending: dict[Any, _ModernPending] = {}
        self._closed = False
        self._pool = ThreadPoolExecutor(max_workers=options.max_inflight, thread_name_prefix="modern-post")

    def start(self) -> None:
        pass  # Modern HTTP has no GET channel.

    def handle(self, message: dict[str, Any]) -> None:
        if "id" not in message:
            if message.get("method") == "notifications/cancelled":
                params = message.get("params")
                request_id = params.get("requestId") if isinstance(params, dict) else None
                if _modern_request_id(request_id):
                    with self._lock:
                        pending = self._pending.get(request_id)
                        if pending:
                            pending.cancel()
            else:
                self.diag.warn("modern mode does not forward client notifications")
            return
        try:
            self.client.request_headers(message)
        except _ModernRpcError as exc:
            request_id = message.get("id")
            self.writer.write_message(exc.response(request_id if _modern_request_id(request_id) else None))
            return
        with self._lock:
            request_id = message["id"]
            if request_id in self._pending:
                self.diag.warn("duplicate active modern request id rejected")
                return  # Do not emit a second terminal response for the active id.
            if self._closed or len(self._pending) >= self.options.max_inflight:
                self.writer.write_message(_ModernRpcError(-32000, "Modern adapter request capacity exceeded").response(request_id))
                return
            pending = _ModernPending()
            self._pending[request_id] = pending
            self._pool.submit(self._request, message, pending)

    def _request(self, message: dict[str, Any], pending: _ModernPending) -> None:
        try:
            self.client.request(message, pending, lambda item: pending.emit(item, self.writer.write_message))
        except _ModernRpcError as exc:
            self.writer.write_message(exc.response(message["id"]))
        except HttpTransportError as exc:
            # A stdio cancellation, like a closed HTTP stream, has no result.
            if pending.reason != "cancelled":
                self.writer.write_message(_error_response(message["id"], exc))
        finally:
            with self._lock:
                self._pending.pop(message["id"], None)

    def shutdown(self) -> None:
        with self._lock:
            self._closed = True
            for pending in self._pending.values():
                pending.cancel()
        self._pool.shutdown(wait=True, cancel_futures=False)


def _error_response(request_id: Any, exc: AdapterError) -> dict[str, Any]:
    if isinstance(exc, HttpTransportError):
        return exc.to_jsonrpc(request_id)
    return {
        "jsonrpc": "2.0",
        "id": request_id,
        "error": {
            "code": ERR_SERVER,
            "message": f"streamable HTTP adapter error: {exc}",
            "data": None,
        },
    }


def _parse_headers(values: list[str] | None) -> dict[str, str]:
    headers: dict[str, str] = {}
    for value in values or []:
        if ":" not in value:
            raise AdapterError(f"invalid --header (expected NAME:VALUE): {value!r}")
        name, _, header_value = value.partition(":")
        name = name.strip()
        header_value = header_value.strip()
        if not name or not header_value:
            raise AdapterError(f"invalid --header (expected NAME:VALUE): {value!r}")
        headers[name] = header_value
    env_raw = os.environ.get(_HEADERS_ENV)
    if env_raw:
        try:
            parsed = json.loads(env_raw)
        except ValueError as exc:
            raise AdapterError(f"{_HEADERS_ENV} is not valid JSON: {exc}") from exc
        if not isinstance(parsed, dict) or not all(
            isinstance(key, str) and isinstance(value, str)
            for key, value in parsed.items()
        ):
            raise AdapterError(f"{_HEADERS_ENV} must be a JSON object of strings")
        for name, header_value in parsed.items():
            headers.setdefault(name, header_value)
    authorization = os.environ.get(_AUTHORIZATION_ENV)
    if authorization:
        headers.setdefault("Authorization", authorization)
    return headers


def run_stdio_adapter(
    options: AdapterOptions, *, stdin: TextIO, out: TextIO, err: TextIO
) -> int:
    """Serve one stdio MCP client until stdin EOF; returns a process exit code."""
    diag = _Diag(err)
    adapter_type = (ModernStdioStreamableHttpAdapter if options.protocol_era == "modern"
                    else StdioStreamableHttpAdapter)
    adapter = adapter_type(options, out=out, diag=diag)
    reader = _JsonLineReader(stdin, options.max_message_bytes)
    adapter.start()
    try:
        for message in reader:
            adapter.handle(message)
        return 0
    except AdapterError as exc:
        diag.warn(f"stdio reader failed ({exc})")
        return 1
    finally:
        adapter.shutdown()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog=PROFILE_NAME,
        description=(
            "Explicit stdio-to-Streamable-HTTP MCP adapter.  Serves one local "
            "stdio MCP client and relays its JSON-RPC to one remote Streamable "
            "HTTP MCP endpoint (MCP 2025-06-18)."
        ),
    )
    parser.add_argument("--url", required=True, help="Remote Streamable HTTP MCP endpoint URL.")
    parser.add_argument(
        "--header",
        action="append",
        default=[],
        metavar="NAME:VALUE",
        help="Static HTTP header (repeatable).  Values are never logged.  "
        "Protocol headers (Accept, Content-Type, Mcp-Session-Id, "
        "MCP-Protocol-Version) cannot be overridden.  The environment "
        f"variables {_HEADERS_ENV} (JSON object) and {_AUTHORIZATION_ENV} "
        "provide the same headers without exposing them in a process list.",
    )
    parser.add_argument("--protocol-era", choices=("legacy", "modern"), default="legacy",
                        help="Explicit transport semantics: legacy (default, 2025-06-18) or modern (2026-07-28). No automatic fallback or replay.")
    parser.add_argument("--protocol-version", default=DEFAULT_PROTOCOL_VERSION,
                        help="Legacy initialization version; modern requires 2026-07-28 metadata on every request.")
    parser.add_argument("--connect-timeout", type=float, default=DEFAULT_CONNECT_TIMEOUT_S)
    parser.add_argument("--idle-timeout", type=float, default=DEFAULT_IDLE_TIMEOUT_S)
    parser.add_argument("--sse-idle-timeout", type=float, default=DEFAULT_SSE_IDLE_TIMEOUT_S)
    parser.add_argument("--request-timeout", type=float, default=DEFAULT_REQUEST_TIMEOUT_S)
    parser.add_argument("--max-message-bytes", type=int, default=DEFAULT_MAX_MESSAGE_BYTES)
    parser.add_argument("--max-inflight", type=int, default=DEFAULT_MAX_INFLIGHT)
    parser.add_argument("--version", action="version", version=f"{PROFILE_NAME} {ADAPTER_VERSION}")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        options = AdapterOptions(
            endpoint_url=args.url,
            headers=_parse_headers(args.header),
            protocol_version=args.protocol_version,
            protocol_era=args.protocol_era,
            connect_timeout_s=args.connect_timeout,
            idle_timeout_s=args.idle_timeout,
            sse_idle_timeout_s=args.sse_idle_timeout,
            request_timeout_s=args.request_timeout,
            max_message_bytes=args.max_message_bytes,
            max_inflight=args.max_inflight,
        )
    except AdapterError as exc:
        detail = "invalid modern adapter configuration" if args.protocol_era == "modern" else str(exc)
        print(f"{PROFILE_NAME}: {detail}", file=sys.stderr)
        return 2
    return run_stdio_adapter(options, stdin=sys.stdin, out=sys.stdout, err=sys.stderr)


if __name__ == "__main__":
    raise SystemExit(main())
