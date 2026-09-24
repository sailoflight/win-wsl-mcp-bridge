"""Offline bounds, fairness and cleanup checks for shared stdio scheduling."""

from __future__ import annotations

import asyncio
import unittest
from types import SimpleNamespace
from unittest import mock

from bridge_runtime import (
    BridgeError,
    MAX_SHARED_INTERNAL_QUEUED_REQUESTS,
    MAX_SHARED_JSONRPC_BYTES,
    RoutedRequest,
    SharedBackend,
    SharedRequestQueue,
    _json_bytes,
)


class SharedRequestQueueTest(unittest.IsolatedAsyncioTestCase):
    def pending(self, owner, sequence, *, initialize=False):
        return RoutedRequest(
            backend_id=f"{owner}:{sequence}",
            client_id=owner,
            original_id=sequence,
            message={"method": "initialize" if initialize else "ping"},
            done=asyncio.get_running_loop().create_future(),
            method="initialize" if initialize else "ping",
            is_initialize=initialize,
        )

    async def test_round_robin_preserves_fifo_for_32_connections(self):
        queue = SharedRequestQueue()
        for owner in range(32):
            for sequence in range(4):
                self.assertIsNone(queue.put_nowait(self.pending(str(owner), sequence), 100))
        actual = [(await queue.get()).backend_id for _index in range(128)]
        self.assertEqual(
            actual,
            [f"{owner}:{sequence}" for sequence in range(4) for owner in range(32)],
        )
        self.assertEqual(queue.qsize(), 0)
        self.assertEqual(queue.queued_bytes, 0)
        self.assertEqual(queue.lane_bytes, {})

    async def test_per_client_and_backend_count_limits(self):
        queue = SharedRequestQueue(max_requests=3, max_client_requests=2)
        for sequence in range(2):
            self.assertIsNone(queue.put_nowait(self.pending("first", sequence), 1))
        self.assertEqual(queue.put_nowait(self.pending("first", 2), 1), "client_request_limit")
        self.assertIsNone(queue.put_nowait(self.pending("second", 0), 1))
        self.assertEqual(queue.put_nowait(self.pending("third", 0), 1), "backend_request_limit")
        await queue.get()
        self.assertIsNone(queue.put_nowait(self.pending("third", 0), 1))

    async def test_per_client_backend_and_single_message_byte_limits(self):
        queue = SharedRequestQueue(max_bytes=200, max_client_bytes=150)
        self.assertIsNone(queue.put_nowait(self.pending("first", 0), 100))
        self.assertEqual(queue.put_nowait(self.pending("first", 1), 51), "client_byte_limit")
        self.assertIsNone(queue.put_nowait(self.pending("second", 0), 100))
        self.assertEqual(queue.put_nowait(self.pending("third", 0), 1), "backend_byte_limit")
        self.assertEqual(
            queue.put_nowait(self.pending("third", 1), MAX_SHARED_JSONRPC_BYTES + 1),
            "request_size_limit",
        )
        self.assertEqual(queue.queued_bytes, 200)
        await queue.get()
        self.assertEqual(queue.queued_bytes, 100)

    async def test_internal_capacity_is_reserved_bounded_and_dispatched_first(self):
        queue = SharedRequestQueue(max_requests=1, max_bytes=100)
        self.assertIsNone(queue.put_nowait(self.pending("busy", 0), 100))
        initialized = self.pending("new", 0, initialize=True)
        self.assertIsNone(queue.put_nowait(initialized, 100))
        for sequence in range(MAX_SHARED_INTERNAL_QUEUED_REQUESTS - 1):
            self.assertIsNone(queue.put_nowait(self.pending(None, sequence), 100))
        self.assertEqual(queue.put_nowait(self.pending(None, 99), 1), "internal_request_limit")
        self.assertIs(await queue.get(), initialized)
        self.assertEqual(
            queue.put_nowait(self.pending(None, 99), MAX_SHARED_JSONRPC_BYTES),
            "internal_byte_limit",
        )

    async def test_discard_releases_storage_even_after_client_identity_is_cleared(self):
        queue = SharedRequestQueue(max_requests=1)
        for sequence in range(1000):
            pending = self.pending("gone", sequence)
            self.assertIsNone(queue.put_nowait(pending, 100))
            pending.client_id = None
            queue.discard(pending)
            queue.discard(pending)
            self.assertEqual(queue.qsize(), 0)
            self.assertEqual(queue.queued_bytes, 0)
            self.assertFalse(queue.ready.is_set())
        self.assertEqual(queue.lanes, {})
        self.assertEqual(queue.lane_bytes, {})

    async def test_waiter_wakes_and_clear_drops_old_generation(self):
        queue = SharedRequestQueue()
        waiter = asyncio.create_task(queue.get())
        await asyncio.sleep(0)
        self.assertFalse(waiter.done())
        pending = self.pending("new", 0)
        queue.put_nowait(pending, 100)
        self.assertIs(await asyncio.wait_for(waiter, 1), pending)
        queue.put_nowait(self.pending("old", 1), 100)
        queue.clear()
        queue.put_nowait(pending, 100)
        self.assertIs(await queue.get(), pending)
        self.assertEqual(queue.qsize(), 0)


class SharedBackendSchedulingTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.backend = SharedBackend(mock.Mock(), "fixture", {"process": {}})
        self.backend.state = "running"
        self.backend.generation = 1
        self.backend._send_client_message = mock.AsyncMock()
        self.backend._write_message = mock.AsyncMock()
        for owner in ("first", "second"):
            self.add_client(owner)

    def add_client(self, owner):
        self.backend.clients[owner] = SimpleNamespace(
            protocol_version="2025-06-18", input_eof=False,
        )

    def message(self, request_id, *, tool=None):
        params = {"_meta": {"progressToken": "same-token"}}
        if tool is not None:
            params.update({"name": tool, "arguments": {}})
        return {
            "jsonrpc": "2.0", "id": request_id,
            "method": "tools/call" if tool else "ping", "params": params,
        }

    async def test_overload_keeps_stream_and_has_no_routing_or_progress_leaks(self):
        self.backend.request_queue = SharedRequestQueue(max_client_requests=1)
        accepted = await self.backend._enqueue_request("first", self.message(1))
        for modern in (False, True):
            rejected = await self.backend._enqueue_request("first", self.message("busy"), modern=modern)
            self.assertIsNone(rejected)
            owner, response = self.backend._send_client_message.call_args.args
            self.assertEqual(owner, "first")
            self.assertEqual(response["id"], "busy")
            self.assertEqual(response["error"]["data"], {
                "reason": "shared_backend_busy", "limit": "client_request_limit",
                "retryable": True, "outcomeUnknown": False,
            })
        self.assertEqual(list(self.backend.pending_by_backend_id.values()), [accepted])
        self.assertEqual(len(self.backend.pending_by_client_id), 1)
        self.assertEqual(len(self.backend.progress_tokens), 1)
        self.assertIn("first", self.backend.clients)
        self.assertIsNotNone(await self.backend._enqueue_request("second", self.message(1)))
        self.backend._write_message.assert_not_called()

    async def test_encoded_byte_accounting_and_active_id_collision(self):
        message = self.message(1)
        message["params"]["text"] = "连接" * 40
        pending = await self.backend._enqueue_request("first", message)
        self.assertEqual(self.backend.request_queue.queued_bytes, len(_json_bytes(pending.message)) + 1)
        with self.assertRaisesRegex(BridgeError, "reused"):
            await self.backend._enqueue_request("first", message)
        self.assertEqual(self.backend.request_queue.qsize(), 1)

    async def test_cancellation_immediately_releases_queue_and_allows_retry(self):
        self.backend.request_queue = SharedRequestQueue(max_client_requests=1)
        pending = await self.backend._enqueue_request("first", self.message(1))
        await self.backend._route_cancellation("first", {
            "params": {"requestId": 1},
        })
        self.assertTrue(pending.cancelled)
        self.assertTrue(pending.done.done())
        self.assertEqual(self.backend.request_queue.qsize(), 0)
        self.assertEqual(self.backend.request_queue.queued_bytes, 0)
        self.assertEqual(self.backend.progress_tokens, {})
        self.assertEqual(self.backend.pending_by_backend_id, {})
        self.assertEqual(self.backend.pending_by_client_id, {})
        retried = await self.backend._enqueue_request("first", self.message(1))
        self.assertIs(await self.backend.request_queue.get(), retried)

    async def test_active_cancellation_bypasses_full_queue(self):
        self.backend.request_queue = SharedRequestQueue(max_requests=1)
        pending = await self.backend._enqueue_request("first", self.message(1))
        self.backend.current_request = await self.backend.request_queue.get()
        await self.backend._enqueue_request("second", self.message(1))
        await self.backend._route_cancellation("first", {
            "jsonrpc": "2.0", "method": "notifications/cancelled",
            "params": {"requestId": 1},
        })
        self.assertEqual(self.backend._write_message.call_args.args[0]["params"]["requestId"],
                         pending.backend_id)
        self.assertEqual(self.backend.request_queue.qsize(), 1)

    async def test_disconnect_removes_only_its_lane_and_keeps_backend_running(self):
        first = await self.backend._enqueue_request("first", self.message(1))
        second = await self.backend._enqueue_request("second", self.message(1))
        await self.backend.detach("first")
        self.assertTrue(first.cancelled)
        self.assertTrue(first.done.done())
        self.assertEqual(self.backend.state, "running")
        self.assertEqual(list(self.backend.clients), ["second"])
        self.assertEqual(list(self.backend.pending_by_backend_id.values()), [second])
        self.assertEqual(len(self.backend.progress_tokens), 1)
        self.assertIs(await self.backend.request_queue.get(), second)

    async def test_recovery_disconnect_releases_progress_and_queued_storage(self):
        pending = await self.backend._enqueue_request("first", self.message(1))
        self.backend.state = "stopping"
        self.assertTrue(await self.backend._cancel_client_requests("first"))
        self.assertTrue(pending.done.done())
        self.assertEqual(self.backend.request_queue.qsize(), 0)
        self.assertEqual(self.backend.progress_tokens, {})

    async def test_failed_generation_discards_queued_business_requests(self):
        pending = await self.backend._enqueue_request("first", self.message(1))
        self.backend._fail_all_pending(BridgeError("generation lost"))
        self.assertIn("bridgeError", pending.done.result())
        self.assertEqual(self.backend.request_queue.qsize(), 0)
        self.assertEqual(self.backend.request_queue.queued_bytes, 0)
        self.assertEqual(self.backend.progress_tokens, {})
        self.assertEqual(self.backend.pending_by_client_id, {})
        self.backend._write_message.assert_not_called()

    async def test_rejected_request_does_not_acquire_lease(self):
        self.backend.process_config["clientLease"] = {
            "toolPatterns": ["owned_*"], "releaseTool": "release",
        }
        self.backend.request_queue = SharedRequestQueue(max_client_requests=1)
        await self.backend._enqueue_request("first", self.message(1))
        self.assertTrue(await self.backend._apply_lease_policy("first", self.message(2, tool="owned_work")))
        self.assertIsNone(self.backend.lease_owner)
        self.assertTrue(await self.backend._apply_lease_policy("second", self.message(2, tool="owned_work")))
        self.assertEqual(self.backend.lease_owner, "second")

    async def test_disconnect_lease_cleanup_has_reserved_priority(self):
        self.backend.process_config["clientLease"] = {
            "releaseTool": "release", "releasedResultPath": ["released"],
        }
        self.backend.lease_owner = "first"
        self.backend.request_queue = SharedRequestQueue(max_requests=1)
        business = await self.backend._enqueue_request("second", self.message(1))
        cleanup_task = asyncio.create_task(self.backend._cleanup_lease("first"))
        try:
            await asyncio.sleep(0)
            cleanup = await self.backend.request_queue.get()
            self.assertTrue(cleanup.is_release)
            self.assertIsNone(cleanup.client_id)
            await self.backend._route_backend_message({
                "jsonrpc": "2.0", "id": cleanup.backend_id,
                "result": {"released": True},
            })
            self.assertTrue(await asyncio.wait_for(cleanup_task, 1))
            self.assertIsNone(self.backend.lease_owner)
            self.assertIs(await self.backend.request_queue.get(), business)
        finally:
            cleanup_task.cancel()
            await asyncio.gather(cleanup_task, return_exceptions=True)

    async def test_worker_routes_48_clients_fairly_with_colliding_ids(self):
        expected = [(str(owner), sequence) for sequence in range(3) for owner in range(48)]
        for owner in range(48):
            self.add_client(str(owner))
            for sequence in range(3):
                await self.backend._route_client_message(str(owner), self.message(sequence))
        finished = asyncio.Event()

        async def respond(message):
            await self.backend._route_backend_message({
                "jsonrpc": "2.0", "id": message["id"], "result": {},
            })
            if self.backend._send_client_message.call_count == len(expected):
                finished.set()

        self.backend._write_message.side_effect = respond
        worker = asyncio.create_task(self.backend._request_worker(1))
        try:
            await asyncio.wait_for(finished.wait(), 3)
        finally:
            worker.cancel()
            await asyncio.gather(worker, return_exceptions=True)
        actual = [(call.args[0], call.args[1]["id"])
                  for call in self.backend._send_client_message.call_args_list]
        self.assertEqual(actual, expected)
        self.assertEqual(self.backend.pending_by_backend_id, {})
        self.assertEqual(self.backend.pending_by_client_id, {})
        self.assertEqual(self.backend.progress_tokens, {})
        self.assertEqual(self.backend.request_queue.qsize(), 0)
        self.assertIsNone(self.backend.current_request)


if __name__ == "__main__":
    unittest.main()
