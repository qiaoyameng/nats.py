import asyncio
import unittest

import nats.errors
from nats.aio.client import Client, Subscription
from tests.utils import async_test


def key_of(msg):
    # Messages use payloads like b"a:1"; the part before ':' is the key.
    return msg.data.split(b":")[0]


class KeyedSubscriptionTest(unittest.TestCase):

    def setUp(self):
        self.loop = asyncio.new_event_loop()

    def _make_client(self):
        # An unconnected client is enough: messages are delivered straight
        # through Client._process_msg and network facing methods are stubbed
        # per test when needed.
        nc = Client()
        nc.errors = []

        async def error_cb(e):
            nc.errors.append(e)

        nc._error_cb = error_cb
        return nc

    def _make_sub(self, nc, cb, **kwargs):
        sub = Subscription(nc, 1, "foo", cb=cb, **kwargs)
        nc._subs[1] = sub
        sub._start(nc._error_cb)
        return sub

    async def _deliver(self, nc, *payloads):
        for payload in payloads:
            await nc._process_msg(1, b"foo", b"", payload, None)

    @async_test
    async def test_same_key_processed_in_order(self):
        nc = self._make_client()
        started = []
        finished = []
        first_entered = asyncio.Event()
        release = asyncio.Event()

        async def cb(msg):
            started.append(msg.data)
            if msg.data == b"a:1":
                first_entered.set()
                await release.wait()
            finished.append(msg.data)

        sub = self._make_sub(nc, cb, cb_key=key_of, cb_concurrency=4)
        await self._deliver(nc, b"a:1", b"a:2")

        await asyncio.wait_for(first_entered.wait(), 1)
        await asyncio.sleep(0.1)
        # The second message of the same key must not start while the
        # first one is still blocked.
        self.assertEqual(started, [b"a:1"])

        release.set()
        await asyncio.wait_for(sub._pending_queue.join(), 1)
        self.assertEqual(finished, [b"a:1", b"a:2"])
        sub._stop_processing()

    @async_test
    async def test_different_keys_run_in_parallel(self):
        nc = self._make_client()
        finished = []
        a_entered = asyncio.Event()
        release = asyncio.Event()

        async def cb(msg):
            if msg.data == b"a:1":
                a_entered.set()
                await release.wait()
            finished.append(msg.data)

        sub = self._make_sub(nc, cb, cb_key=key_of, cb_concurrency=4)
        await self._deliver(nc, b"a:1", b"b:1")

        await asyncio.wait_for(a_entered.wait(), 1)
        await asyncio.sleep(0.1)
        # Key "a" is blocked but key "b" made progress meanwhile.
        self.assertEqual(finished, [b"b:1"])

        release.set()
        await asyncio.wait_for(sub._pending_queue.join(), 1)
        self.assertEqual(sorted(finished), [b"a:1", b"b:1"])
        sub._stop_processing()

    @async_test
    async def test_concurrency_limit_holds_messages_as_pending(self):
        nc = self._make_client()
        started = []
        entered = asyncio.Event()
        release = asyncio.Event()

        async def cb(msg):
            started.append(msg.data)
            entered.set()
            await release.wait()

        sub = self._make_sub(nc, cb, cb_key=key_of, cb_concurrency=1)
        await self._deliver(nc, b"a:1", b"b:1")

        await asyncio.wait_for(entered.wait(), 1)
        await asyncio.sleep(0.1)
        # A single callback slot: the second message is held by the
        # dispatcher but must still count as pending.
        self.assertEqual(started, [b"a:1"])
        self.assertEqual(sub.pending_msgs, 2)
        self.assertEqual(sub.pending_bytes, len(b"a:1") + len(b"b:1"))

        release.set()
        await asyncio.wait_for(sub._pending_queue.join(), 1)
        self.assertEqual(started, [b"a:1", b"b:1"])
        self.assertEqual(sub.pending_msgs, 0)
        self.assertEqual(sub.pending_bytes, 0)
        sub._stop_processing()

    @async_test
    async def test_pending_msgs_limit_counts_dispatched_messages(self):
        nc = self._make_client()
        entered = asyncio.Event()
        release = asyncio.Event()

        async def cb(msg):
            entered.set()
            await release.wait()

        sub = self._make_sub(
            nc, cb, cb_key=key_of, cb_concurrency=1, pending_msgs_limit=1
        )
        await self._deliver(nc, b"a:1")

        await asyncio.wait_for(entered.wait(), 1)
        await asyncio.sleep(0.1)
        # a:1 left the main queue and is being processed, but it still
        # occupies the single pending slot.
        self.assertEqual(sub.pending_msgs, 1)

        # The next message overflows the pending limit and is dropped
        # as a slow consumer instead of being buffered on the side.
        await self._deliver(nc, b"b:1")
        self.assertEqual(len(nc.errors), 1)
        self.assertIsInstance(nc.errors[0], nats.errors.SlowConsumerError)
        self.assertEqual(sub.pending_msgs, 1)

        release.set()
        await asyncio.wait_for(sub._pending_queue.join(), 1)
        self.assertEqual(sub.pending_msgs, 0)
        sub._stop_processing()

    @async_test
    async def test_key_function_error_uses_error_cb_and_does_not_stall(self):
        nc = self._make_client()
        finished = []

        def bad_key(msg):
            if msg.data == b"bad":
                raise ValueError("cannot key this message")
            return msg.data

        async def cb(msg):
            finished.append(msg.data)

        sub = self._make_sub(nc, cb, cb_key=bad_key, cb_concurrency=2)
        await self._deliver(nc, b"x", b"bad", b"y")

        await asyncio.wait_for(sub._pending_queue.join(), 1)
        # Only the offending message is dropped, the other keys continue.
        self.assertEqual(finished, [b"x", b"y"])
        self.assertEqual(len(nc.errors), 1)
        self.assertIsInstance(nc.errors[0], ValueError)
        self.assertEqual(sub.pending_msgs, 0)
        sub._stop_processing()

    @async_test
    async def test_callback_error_uses_error_cb_and_key_continues(self):
        nc = self._make_client()
        finished = []

        async def cb(msg):
            if msg.data == b"a:1":
                raise RuntimeError("boom")
            finished.append(msg.data)

        sub = self._make_sub(nc, cb, cb_key=key_of, cb_concurrency=2)
        await self._deliver(nc, b"a:1", b"a:2", b"b:1")

        await asyncio.wait_for(sub._pending_queue.join(), 1)
        # The failing message is reported but its key keeps making
        # progress and other keys are unaffected.
        self.assertEqual(finished, [b"a:2", b"b:1"])
        self.assertEqual(len(nc.errors), 1)
        self.assertIsInstance(nc.errors[0], RuntimeError)
        sub._stop_processing()

    @async_test
    async def test_drain_waits_for_dispatched_messages(self):
        nc = self._make_client()
        sent_unsub = []

        async def _send_unsubscribe(sid, limit=0):
            sent_unsub.append((sid, limit))

        async def flush():
            pass

        nc._send_unsubscribe = _send_unsubscribe
        nc.flush = flush

        entered = asyncio.Event()
        release = asyncio.Event()
        finished = []

        async def cb(msg):
            entered.set()
            await release.wait()
            finished.append(msg.data)

        sub = self._make_sub(nc, cb, cb_key=key_of, cb_concurrency=2)
        await self._deliver(nc, b"a:1")
        await asyncio.wait_for(entered.wait(), 1)

        drain_task = asyncio.create_task(sub.drain())
        await asyncio.sleep(0.1)
        # Drain announced the unsubscribe but waits for the accepted
        # message to finish processing.
        self.assertEqual(sent_unsub, [(1, 0)])
        self.assertFalse(drain_task.done())

        release.set()
        await asyncio.wait_for(drain_task, 1)
        self.assertEqual(finished, [b"a:1"])
        self.assertTrue(sub._closed)
        self.assertNotIn(1, nc._subs)

    @async_test
    async def test_unsubscribe_with_limit_processes_accepted_messages(self):
        nc = self._make_client()
        sent_unsub = []

        async def _send_unsubscribe(sid, limit=0):
            sent_unsub.append((sid, limit))

        nc._send_unsubscribe = _send_unsubscribe

        entered = asyncio.Event()
        release = asyncio.Event()
        finished = []

        async def cb(msg):
            if msg.data == b"a:1":
                entered.set()
                await release.wait()
            finished.append(msg.data)

        sub = self._make_sub(nc, cb, cb_key=key_of, cb_concurrency=2)
        await self._deliver(nc, b"a:1", b"a:2")
        await asyncio.wait_for(entered.wait(), 1)

        # Limit already reached but accepted messages are still pending,
        # so this must not stop processing immediately.
        await sub.unsubscribe(limit=2)
        self.assertEqual(sent_unsub, [(1, 2)])
        self.assertFalse(sub._closed)

        release.set()
        # The dispatcher task finishes once the accepted messages are done.
        await asyncio.wait_for(sub._wait_for_msgs_task, 1)
        self.assertEqual(finished, [b"a:1", b"a:2"])

    @async_test
    async def test_stop_processing_releases_all_dispatch_tasks(self):
        nc = self._make_client()
        entered = asyncio.Event()
        never = asyncio.Event()

        async def cb(msg):
            entered.set()
            await never.wait()

        sub = self._make_sub(nc, cb, cb_key=key_of, cb_concurrency=2)
        await self._deliver(nc, b"a:1", b"a:2", b"b:1")
        await asyncio.wait_for(entered.wait(), 1)
        await asyncio.sleep(0.1)

        workers = list(sub._key_workers.values())
        self.assertEqual(len(workers), 2)

        # What Client._close does for every subscription.
        sub._stop_processing()
        await asyncio.sleep(0.1)

        self.assertTrue(sub._wait_for_msgs_task.done())
        for worker in workers:
            self.assertTrue(worker.done())
        # Cancelled workers release their accounting, so nothing leaks
        # and a drain-style join cannot hang.
        await asyncio.wait_for(sub._pending_queue.join(), 1)
        self.assertEqual(sub.pending_msgs, 0)
        self.assertEqual(sub.pending_bytes, 0)

    @async_test
    async def test_default_callback_dispatch_remains_serial(self):
        nc = self._make_client()
        started = []
        entered = asyncio.Event()
        release = asyncio.Event()

        async def cb(msg):
            started.append(msg.data)
            if msg.data == b"first":
                entered.set()
                await release.wait()

        # No cb_key: a slow callback still blocks the whole subscription.
        sub = self._make_sub(nc, cb)
        await self._deliver(nc, b"first", b"second")

        await asyncio.wait_for(entered.wait(), 1)
        await asyncio.sleep(0.1)
        self.assertEqual(started, [b"first"])

        release.set()
        await asyncio.wait_for(sub._pending_queue.join(), 1)
        self.assertEqual(started, [b"first", b"second"])
        sub._stop_processing()

    @async_test
    async def test_keyed_mode_validation(self):
        nc = self._make_client()

        async def cb(msg):
            pass

        # cb_key requires a callback subscription.
        with self.assertRaises(nats.errors.Error):
            Subscription(nc, 1, "foo", cb_key=key_of)

        # The concurrency limit must be at least one.
        with self.assertRaises(nats.errors.Error):
            Subscription(nc, 1, "foo", cb=cb, cb_key=key_of, cb_concurrency=0)


if __name__ == "__main__":
    unittest.main()
