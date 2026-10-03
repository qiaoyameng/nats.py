# Copyright 2016-2023 The NATS Authors
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import asyncio
import unittest
from typing import List

import nats
from nats.aio.client import (
    Client,
    PING_PROTO,
    PONG_PROTO,
)
from nats.aio.transport import Transport


class MemoryTransport(Transport):
    """
    Loopback transport that never touches a socket.

    Every write()/writelines() payload is appended (in call order) to
    ``writes``; drain() is a no-op coroutine.  Tests feed server bytes via
    ``feed_server_bytes`` which the client's read loop consumes.
    """

    def __init__(self) -> None:
        self.writes: List[bytes] = []
        self._read_queue: asyncio.Queue[bytes] = asyncio.Queue()
        self._buffer = bytearray()
        self._closed = False

    async def connect(self, uri, buffer_size: int, connect_timeout: int):
        return None

    async def connect_tls(self, uri, ssl_context, buffer_size,
                          connect_timeout):
        return None

    def write(self, payload: bytes) -> None:
        self.writes.append(bytes(payload))

    def writelines(self, payload: List[bytes]) -> None:
        for chunk in payload:
            self.writes.append(bytes(chunk))

    async def drain(self) -> None:
        return None

    async def read(self, buffer_size: int) -> bytes:
        while not self._buffer:
            chunk = await self._read_queue.get()
            if chunk == b"":
                return b""
            self._buffer.extend(chunk)
        data = bytes(self._buffer[:buffer_size])
        del self._buffer[:buffer_size]
        return data

    async def readline(self) -> bytes:
        return await self.read(65536)

    def feed_server_bytes(self, data: bytes) -> None:
        self._read_queue.put_nowait(data)

    async def wait_closed(self) -> None:
        self._closed = True

    def close(self) -> None:
        self._closed = True
        self._read_queue.put_nowait(b"")

    def at_eof(self) -> bool:
        return self._closed and not self._buffer and self._read_queue.empty()

    def __bool__(self) -> bool:
        return not self._closed


def make_client(loop: asyncio.AbstractEventLoop) -> Client:
    nc = Client()
    # Minimal options normally populated by connect().
    nc.options = {
        "ping_interval": 10_000,
        "max_outstanding_pings": 2,
    }
    nc._flush_queue = asyncio.Queue(maxsize=1024)
    nc._transport = MemoryTransport()
    nc._status = Client.CONNECTED
    nc._flusher_task = loop.create_task(nc._flusher())
    nc._reading_task = loop.create_task(nc._read_loop())
    return nc


def count_pings(transport: MemoryTransport) -> int:
    return sum(1 for chunk in transport.writes if chunk == PING_PROTO)


async def yield_ticks(count: int = 5) -> None:
    for _ in range(count):
        await asyncio.sleep(0)


class FlushBarrierTest(unittest.TestCase):

    def setUp(self) -> None:
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

    def tearDown(self) -> None:
        self.loop.run_until_complete(self._cleanup())
        self.loop.close()

    async def _cleanup(self) -> None:
        tasks = [
            t for t in asyncio.all_tasks(self.loop)
            if t is not asyncio.current_task()
        ]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    def _run(self, coro):
        return self.loop.run_until_complete(coro)

    async def _send_pongs(self, nc: Client, count: int) -> None:
        transport: MemoryTransport = nc._transport
        for _ in range(count):
            transport.feed_server_bytes(PONG_PROTO)
        # Let the read loop parse all injected PONGs.
        for _ in range(20):
            await asyncio.sleep(0)

    def test_concurrent_flushes_share_single_ping(self):
        async def go():
            nc = make_client(self.loop)
            transport: MemoryTransport = nc._transport
            try:
                await nc.publish("foo", b"m1")
                await nc.publish("foo", b"m2")
                await nc.publish("foo", b"m3")

                f1 = asyncio.ensure_future(nc.flush(1))
                f2 = asyncio.ensure_future(nc.flush(1))
                f3 = asyncio.ensure_future(nc.flush(1))
                await yield_ticks()
                self.assertEqual(count_pings(transport), 1)

                await self._send_pongs(nc, 1)
                results = await asyncio.gather(
                    f1,
                    f2,
                    f3,
                )
                self.assertEqual(results, [None, None, None])
                self.assertEqual(count_pings(transport), 1)
            finally:
                nc._flusher_task.cancel()

        self._run(go())

    def test_flush_ordering_pub_before_ping_and_later_writes_not_shared(self):
        async def go():
            nc = make_client(self.loop)
            transport: MemoryTransport = nc._transport
            try:
                await nc.publish("foo", b"m1")
                f1 = asyncio.ensure_future(nc.flush(1))
                await yield_ticks()
                self.assertEqual(count_pings(transport), 1)

                await nc.publish("foo", b"m2")
                f2 = asyncio.ensure_future(nc.flush(1))
                await yield_ticks()

                # f2 covers newer data, so it cannot share f1's PING.
                self.assertEqual(count_pings(transport), 2)
                self.assertFalse(f2.done())
                self.assertFalse(f1.done())

                # Only the first PONG arrives: f1 completes, f2 cannot
                # borrow it.
                await self._send_pongs(nc, 1)
                await f1
                self.assertFalse(f2.done())

                # The matching second PONG completes f2.
                await self._send_pongs(nc, 1)
                await f2

                kinds = [
                    b"PING" if chunk == PING_PROTO else b"PUB"
                    for chunk in transport.writes
                ]
                self.assertEqual(kinds.count(b"PING"), 2)
                ping_indexes = [
                    i for i, kind in enumerate(kinds) if kind == b"PING"
                ]
                # Every PING follows the publishes it confirms.
                self.assertLess(kinds.index(b"PUB"), ping_indexes[0])
                self.assertEqual(kinds.count(b"PUB"), 2)
                self.assertLess(
                    kinds.index(b"PUB", ping_indexes[0] + 1),
                    ping_indexes[1],
                )
            finally:
                nc._flusher_task.cancel()

        self._run(go())

    def test_cancel_one_waiter_keeps_shared_confirmation(self):
        async def go():
            nc = make_client(self.loop)
            transport: MemoryTransport = nc._transport
            try:
                await nc.publish("foo", b"m1")

                f1 = asyncio.ensure_future(nc.flush(1))
                f2 = asyncio.ensure_future(nc.flush(1))
                f3 = asyncio.ensure_future(nc.flush(1))
                await yield_ticks()

                f1.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await f1

                self.assertFalse(f2.done())
                self.assertFalse(f3.done())
                self.assertEqual(count_pings(transport), 1)

                await self._send_pongs(nc, 1)
                await f2
                await f3

                # A second flush must need its own PING: the old PONG is
                # consumed.
                await nc.publish("foo", b"m2")
                f4 = asyncio.ensure_future(nc.flush(1))
                await yield_ticks()
                self.assertEqual(count_pings(transport), 2)
                await self._send_pongs(nc, 1)
                await f4
            finally:
                nc._flusher_task.cancel()

        self._run(go())

    def test_flush_timeout_does_not_kill_shared_ping(self):
        async def go():
            nc = make_client(self.loop)
            transport: MemoryTransport = nc._transport
            try:
                await nc.publish("foo", b"m1")

                f_fast = asyncio.ensure_future(nc.flush(0.05))
                f_slow = asyncio.ensure_future(nc.flush(5))
                await yield_ticks()

                with self.assertRaises(nats.errors.FlushTimeoutError):
                    await f_fast
                self.assertFalse(f_slow.done())
                self.assertEqual(count_pings(transport), 1)

                # The late PONG still completes the other waiter; the read
                # loop must not have died with InvalidStateError.
                await self._send_pongs(nc, 1)
                await f_slow
            finally:
                nc._flusher_task.cancel()

        self._run(go())

    def test_keepalive_ping_matched_by_its_own_pong(self):
        async def go():
            nc = make_client(self.loop)
            transport: MemoryTransport = nc._transport
            try:
                # Interval keepalive PING has no flush waiters.
                await nc._send_ping()
                self.assertEqual(count_pings(transport), 1)

                transport.feed_server_bytes(PONG_PROTO)
                await yield_ticks()
                self.assertEqual(len(nc._pings), 0)

                # Flush barriers after the keepalive use fresh PINGs and
                # still coalesce.
                await nc.publish("foo", b"m1")
                f1 = asyncio.ensure_future(nc.flush(1))
                f2 = asyncio.ensure_future(nc.flush(1))
                await yield_ticks()
                self.assertEqual(count_pings(transport), 2)

                transport.feed_server_bytes(PONG_PROTO)
                await f1
                await f2
            finally:
                nc._flusher_task.cancel()
                nc._reading_task.cancel()

        self._run(go())

    def test_unexpected_pong_does_not_kill_read_loop(self):
        async def go():
            nc = make_client(self.loop)
            transport: MemoryTransport = nc._transport
            try:
                # A stray PONG with nothing outstanding must be ignored,
                # and the read loop must keep functioning.
                transport.feed_server_bytes(PONG_PROTO)
                await yield_ticks()
                self.assertFalse(nc._reading_task.done())

                # A subsequent real flush still completes normally.
                await nc.publish("foo", b"m1")
                f = asyncio.ensure_future(nc.flush(1))
                await yield_ticks()
                self.assertEqual(count_pings(transport), 1)
                transport.feed_server_bytes(PONG_PROTO)
                await f
            finally:
                nc._flusher_task.cancel()
                nc._reading_task.cancel()

        self._run(go())

    def test_disconnect_settles_old_barriers_and_generation(self):
        async def go():
            nc = make_client(self.loop)
            transport: MemoryTransport = nc._transport
            try:
                await nc.publish("foo", b"m1")
                f_pending = asyncio.ensure_future(nc.flush(0.5))
                await yield_ticks()
                old_generation = nc._ping_generation
                self.assertEqual(count_pings(transport), 1)

                old_flusher = nc._flusher_task
                # Connection lost but the client will reconnect: settle the
                # old generation barriers without launching the real
                # reconnection state machine.
                nc._status = Client.RECONNECTING
                nc._fail_flush_barriers(
                    nats.errors.ConnectionReconnectingError()
                )
                old_flusher.cancel()
                await yield_ticks()

                with self.assertRaises(nats.errors.FlushTimeoutError):
                    await f_pending

                self.assertEqual(nc._ping_generation, old_generation + 1)

                # Simulate a new connection: fresh generation state.
                nc._status = Client.CONNECTED
                nc._ping_generation = old_generation + 1
                nc._pings = type(nc._pings)()
                nc._flushed_bytes = 0
                nc._flusher_task = self.loop.create_task(nc._flusher())

                # A PONG from the old connection must not settle the new
                # generation waiters.
                await nc.publish("foo", b"m2")
                f_new = asyncio.ensure_future(nc.flush(2))
                await yield_ticks()
                self.assertEqual(count_pings(transport), 2)

                # Inject a stale PONG that would answer the old PING; it is
                # ignored because no old-generation record is outstanding.
                transport.feed_server_bytes(PONG_PROTO)
                await yield_ticks()
                self.assertFalse(f_new.done())

                # Proper PONG for the new PING completes the new barrier.
                await self._send_pongs(nc, 1)
                await f_new
            finally:
                nc._flusher_task.cancel()
                nc._reading_task.cancel()

        self._run(go())

    def test_close_fails_pending_flush_with_closed_error(self):
        async def go():
            nc = make_client(self.loop)
            transport: MemoryTransport = nc._transport
            try:
                await nc.publish("foo", b"m1")
                f = asyncio.ensure_future(nc.flush(2))
                await yield_ticks()
                self.assertEqual(count_pings(transport), 1)

                nc.options["allow_reconnect"] = False
                await nc._process_op_err(ConnectionError("boom"))
                await yield_ticks()

                with self.assertRaises(nats.errors.ConnectionClosedError):
                    await f

                # A new flush on a closed client fails immediately.
                async def _closed_flush():
                    await nc.flush(1)

                with self.assertRaises(nats.errors.ConnectionClosedError):
                    await _closed_flush()
            finally:
                nc._flusher_task.cancel()

        self._run(go())


if __name__ == "__main__":
    unittest.main()
