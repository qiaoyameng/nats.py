import asyncio
import logging
import unittest

import nats
import nats.errors
from nats.aio.client import PING_PROTO, PONG_PROTO
from nats.aio.client import Client as NATS
from tests.utils import async_test


class InMemoryTransport:
    """
    In-memory transport that records everything the client flushes to
    the socket (preserving order) and lets the test inject server bytes
    (e.g. PONGs) one chunk at a time.
    """

    def __init__(self):
        # Ordered log of chunks that the client has flushed to the socket.
        self.written = []
        self._buffered = []
        self._inbound = asyncio.Queue()
        self._closed = False

    async def connect(self, uri, buffer_size, connect_timeout):
        pass

    async def connect_tls(self, uri, ssl_context, buffer_size, connect_timeout):
        pass

    def write(self, payload):
        self._buffered.append(payload)

    def writelines(self, payloads):
        self._buffered.extend(payloads)

    async def drain(self):
        self.written.extend(self._buffered)
        self._buffered = []

    async def read(self, buffer_size):
        return await self._inbound.get()

    async def readline(self):
        return await self._inbound.get()

    async def wait_closed(self):
        pass

    def close(self):
        self._closed = True

    def at_eof(self):
        return False

    def __bool__(self):
        return not self._closed

    def feed(self, data):
        """Inject bytes as if they arrived from the server."""
        self._inbound.put_nowait(data)

    def pings_sent(self):
        return sum(chunk == PING_PROTO for chunk in self.written)

    def write_kinds(self):
        kinds = []
        for chunk in self.written:
            if chunk == PING_PROTO:
                kinds.append(b"PING")
            else:
                kinds.append(chunk.split(b" ", 1)[0])
        return kinds


class FlushBarrierTest(unittest.TestCase):

    def setUp(self):
        self.loop = asyncio.new_event_loop()

    def tearDown(self):
        self.loop.close()

    async def _make_client(self):
        """Returns a connected client wired to an InMemoryTransport."""
        transport = InMemoryTransport()
        nc = NATS()
        nc._transport = transport
        nc._status = NATS.CONNECTED
        nc._flush_queue = asyncio.Queue()
        nc._flusher_task = asyncio.create_task(nc._flusher())
        nc._reading_task = asyncio.create_task(nc._read_loop())
        return nc, transport

    @async_test
    async def test_concurrent_flushes_share_single_ping(self):
        nc, transport = await self._make_client()
        try:
            for _ in range(3):
                await nc.publish("foo", b"hello")

            tasks = [asyncio.create_task(nc.flush()) for _ in range(5)]
            await asyncio.sleep(0.1)

            # All five waiters cover the same send watermark, so a
            # single PING is sent instead of five.
            self.assertEqual(1, transport.pings_sent())
            # Everything published was flushed before the PING.
            self.assertEqual(
                [b"PUB", b"PUB", b"PUB", b"PING"], transport.write_kinds()
            )
            for task in tasks:
                self.assertFalse(task.done())

            transport.feed(PONG_PROTO)
            await asyncio.gather(*tasks)
            self.assertEqual(1, nc._pongs_received)
        finally:
            await nc.close()

    @async_test
    async def test_later_writes_not_confirmed_by_earlier_pong(self):
        nc, transport = await self._make_client()
        try:
            await nc.publish("foo", b"a")
            flush1 = asyncio.create_task(nc.flush())
            await asyncio.sleep(0.1)

            # More data enters the send queue after the first PING.
            await nc.publish("foo", b"b")
            flush2 = asyncio.create_task(nc.flush())
            await asyncio.sleep(0.1)

            # The second flush needs its own PING, queued behind the
            # second publish.
            self.assertEqual(2, transport.pings_sent())
            self.assertEqual(
                [b"PUB", b"PING", b"PUB", b"PING"], transport.write_kinds()
            )

            # The first PONG only confirms the first barrier.
            transport.feed(PONG_PROTO)
            await asyncio.wait_for(flush1, 1)
            self.assertFalse(flush2.done())

            transport.feed(PONG_PROTO)
            await asyncio.wait_for(flush2, 1)
        finally:
            await nc.close()

    @async_test
    async def test_waiter_cancellation_keeps_shared_confirmation(self):
        nc, transport = await self._make_client()
        try:
            await nc.publish("foo", b"a")
            flush1 = asyncio.create_task(nc.flush())
            flush2 = asyncio.create_task(nc.flush())
            await asyncio.sleep(0.1)
            self.assertEqual(1, transport.pings_sent())

            # Cancelling one waiter must not cancel the shared barrier.
            flush1.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await flush1
            self.assertFalse(flush2.done())

            # The PONG still confirms the remaining waiter and the read
            # loop survives.
            transport.feed(PONG_PROTO)
            await asyncio.wait_for(flush2, 1)
            self.assertFalse(nc._reading_task.done())
        finally:
            await nc.close()

    @async_test
    async def test_waiter_timeout_keeps_barrier_and_read_loop(self):
        nc, transport = await self._make_client()
        try:
            await nc.publish("foo", b"a")
            with self.assertRaises(nats.errors.FlushTimeoutError):
                await nc.flush(timeout=0.1)
            self.assertEqual(1, transport.pings_sent())

            # The PING is still on the wire; its late PONG must neither
            # raise InvalidStateError nor kill the read loop.
            transport.feed(PONG_PROTO)
            await asyncio.sleep(0.1)
            self.assertFalse(nc._reading_task.done())

            # The connection is still fully usable afterwards.
            await nc.publish("foo", b"b")
            flush = asyncio.create_task(nc.flush())
            await asyncio.sleep(0.1)
            self.assertEqual(2, transport.pings_sent())
            transport.feed(PONG_PROTO)
            await asyncio.wait_for(flush, 1)
            self.assertFalse(nc._reading_task.done())
        finally:
            await nc.close()

    @async_test
    async def test_reconnect_settles_old_barrier(self):
        nc, transport = await self._make_client()
        nc.options["allow_reconnect"] = True

        # Collect client logs to prove no InvalidStateError (or any
        # other error) is hit while settling the old barrier.
        log_records = []

        class _Handler(logging.Handler):

            def emit(self, record):
                log_records.append(record)

        client_logger = logging.getLogger("nats.aio.client")
        handler = _Handler()
        client_logger.addHandler(handler)
        self.addCleanup(client_logger.removeHandler, handler)

        await nc.publish("foo", b"a")
        flush1 = asyncio.create_task(nc.flush())
        await asyncio.sleep(0.1)
        self.assertEqual(1, transport.pings_sent())
        generation = nc._flush_barrier_generation

        # Simulate the connection breaking.
        await nc._process_op_err(OSError("connection reset"))

        # Stop the reconnection task before it attempts to dial a
        # server; the reconnect is simulated manually below.
        nc._reconnection_task.cancel()

        # The old barrier is settled immediately with a reconnect error
        # and a new barrier generation starts.
        with self.assertRaises(nats.errors.ConnectionReconnectingError):
            await flush1
        self.assertEqual(generation + 1, nc._flush_barrier_generation)

        try:
            await nc._reconnection_task
        except asyncio.CancelledError:
            pass

        # A PONG trickling in from the old connection is consumed
        # without raising InvalidStateError, even though its barrier
        # was already settled by the reconnect.
        transport.feed(PONG_PROTO)
        await asyncio.sleep(0.1)
        self.assertEqual([], nc._pongs)
        self.assertEqual(1, nc._pongs_received)
        self.assertEqual(
            [],
            [r for r in log_records if r.levelno >= logging.ERROR],
        )

        # Simulate a successful reconnect on a brand new transport.
        nc._reading_task.cancel()
        nc._flusher_task.cancel()

        new_transport = InMemoryTransport()
        nc._transport = new_transport
        nc._pongs = []
        nc._status = NATS.CONNECTED
        nc._flush_queue = asyncio.Queue()
        nc._flusher_task = asyncio.create_task(nc._flusher())
        nc._reading_task = asyncio.create_task(nc._read_loop())

        # New waiters belong to the new generation and are only
        # confirmed by PONGs from the new connection.
        await nc.publish("foo", b"b")
        flush2 = asyncio.create_task(nc.flush())
        await asyncio.sleep(0.1)
        self.assertEqual(1, new_transport.pings_sent())
        self.assertFalse(flush2.done())

        new_transport.feed(PONG_PROTO)
        await asyncio.wait_for(flush2, 1)
        await nc.close()
        self.assertEqual(
            [],
            [r for r in log_records if r.levelno >= logging.ERROR],
        )

    @async_test
    async def test_close_settles_old_barrier(self):
        nc, transport = await self._make_client()
        await nc.publish("foo", b"a")
        flush = asyncio.create_task(nc.flush())
        await asyncio.sleep(0.1)
        self.assertEqual(1, transport.pings_sent())

        await nc.close()
        with self.assertRaises(nats.errors.ConnectionClosedError):
            await flush
        with self.assertRaises(nats.errors.ConnectionClosedError):
            await nc.flush()


if __name__ == "__main__":
    unittest.main()
