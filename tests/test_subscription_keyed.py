import asyncio

import pytest

from nats.aio.msg import Msg
from nats.aio.subscription import Subscription


class _FakeConn:

    def __init__(self):
        self.is_closed = False
        self.is_draining = False
        self.is_reconnecting = False
        self.unsubscribed = []
        self.removed = []

    async def _send_unsubscribe(self, sid, limit=0):
        self.unsubscribed.append((sid, limit))

    async def flush(self):
        pass

    def _remove_sub(self, sid, max_msgs=0):
        self.removed.append(sid)


def _run(coro, timeout=10):

    async def main():
        await asyncio.wait_for(coro, timeout)

    asyncio.run(main())


def _make_sub(
    cb,
    key_cb=None,
    concurrency=1,
    pending_msgs_limit=512 * 1024,
    pending_bytes_limit=128 * 1024 * 1024,
):
    conn = _FakeConn()
    sub = Subscription(
        conn,
        id=1,
        subject="foo",
        cb=cb,
        key_cb=key_cb,
        concurrency=concurrency,
        pending_msgs_limit=pending_msgs_limit,
        pending_bytes_limit=pending_bytes_limit,
    )
    errors = []

    async def error_cb(e):
        errors.append(e)

    sub._start(error_cb)
    return conn, sub, errors


def _feed(sub, data):
    msg = Msg(_client=None, subject="foo", reply="", data=data, _sid=1)
    sub._received += 1
    sub._pending_size += len(msg.data)
    sub._enqueue_msg(msg)
    return msg


async def _settle(rounds=5):
    for _ in range(rounds):
        await asyncio.sleep(0.01)


async def _shutdown(sub):
    sub._stop_processing()
    if sub._wait_for_msgs_task:
        await asyncio.gather(sub._wait_for_msgs_task, return_exceptions=True)


def test_default_serial_callback_blocks_next_message():

    async def scenario():
        loop = asyncio.get_running_loop()
        release = loop.create_future()
        started, finished = [], []

        async def cb(msg):
            name = msg.data.decode()
            started.append(name)
            if name == "m1":
                await release
            finished.append(name)

        conn, sub, errors = _make_sub(cb)
        _feed(sub, b"m1")
        _feed(sub, b"m2")
        await _settle()
        # A slow callback blocks the whole subscription by default.
        assert started == ["m1"]
        assert finished == []
        release.set_result(None)
        await _settle()
        assert finished == ["m1", "m2"]
        assert errors == []
        await _shutdown(sub)

    _run(scenario())


def test_keyed_same_key_serial_different_keys_parallel():

    async def scenario():
        loop = asyncio.get_running_loop()
        release_a1 = loop.create_future()
        started, finished = [], []

        async def cb(msg):
            name = msg.data.decode()
            started.append(name)
            if name == "A1":
                await release_a1
            finished.append(name)

        conn, sub, errors = _make_sub(
            cb, key_cb=lambda m: m.data[:1], concurrency=4
        )
        _feed(sub, b"A1")
        _feed(sub, b"B1")
        _feed(sub, b"A2")
        await _settle()
        # A2 is stuck behind A1 (same key, receive order) while B1
        # moves forward in parallel (different key).
        assert started == ["A1", "B1"]
        assert finished == ["B1"]
        release_a1.set_result(None)
        await _settle()
        assert finished == ["B1", "A1", "A2"]
        assert errors == []
        await _shutdown(sub)

    _run(scenario())


def test_keyed_concurrency_limit_serializes_lanes():

    async def scenario():
        loop = asyncio.get_running_loop()
        release = loop.create_future()
        started, finished = [], []

        async def cb(msg):
            name = msg.data.decode()
            started.append(name)
            if name == "A1":
                await release
            finished.append(name)

        conn, sub, errors = _make_sub(
            cb, key_cb=lambda m: m.data[:1], concurrency=1
        )
        _feed(sub, b"A1")
        _feed(sub, b"B1")
        await _settle()
        # Only one lane may run at a time, B1 waits for a free slot.
        assert started == ["A1"]
        # Both messages still count as pending: one in-flight, one
        # queued in B's lane.
        assert sub.pending_msgs == 2
        assert sub.pending_bytes == 4
        release.set_result(None)
        await _settle()
        assert finished == ["A1", "B1"]
        assert sub.pending_msgs == 0
        assert sub.pending_bytes == 0
        await _shutdown(sub)

    _run(scenario())


def test_key_cb_error_uses_error_channel_and_other_keys_continue():

    async def scenario():
        finished = []

        async def cb(msg):
            finished.append(msg.data.decode())

        def key_cb(msg):
            if msg.data == b"bad":
                raise ValueError("cannot key this message")
            return msg.data

        conn, sub, errors = _make_sub(cb, key_cb=key_cb, concurrency=2)
        _feed(sub, b"k1")
        _feed(sub, b"bad")
        _feed(sub, b"k2")
        await _settle()
        assert sorted(finished) == ["k1", "k2"]
        assert len(errors) == 1
        assert isinstance(errors[0], ValueError)
        # The unkeyable message was completed instead of leaking
        # pending accounting.
        assert sub.pending_msgs == 0
        assert sub.pending_bytes == 0
        await _shutdown(sub)

    _run(scenario())


def test_callback_error_does_not_stall_its_key():

    async def scenario():
        finished = []

        async def cb(msg):
            if msg.data == b"k1":
                raise RuntimeError("boom")
            finished.append(msg.data.decode())

        conn, sub, errors = _make_sub(
            cb, key_cb=lambda m: m.data[:1], concurrency=1
        )
        _feed(sub, b"k1")
        _feed(sub, b"k2")
        await _settle()
        assert finished == ["k2"]
        assert len(errors) == 1
        assert isinstance(errors[0], RuntimeError)
        await _shutdown(sub)

    _run(scenario())


def test_pending_msgs_limit_counts_lane_queued_messages():

    async def scenario():
        loop = asyncio.get_running_loop()
        release = loop.create_future()

        async def cb(msg):
            await release

        conn, sub, errors = _make_sub(
            cb,
            key_cb=lambda m: m.data[:1],
            concurrency=2,
            pending_msgs_limit=2,
        )
        _feed(sub, b"A1")
        _feed(sub, b"B1")
        await _settle()
        # Both accepted messages are in-flight and count as pending.
        assert sub.pending_msgs == 2
        # A third message would exceed the pending limit even though
        # the main queue itself is empty.
        with pytest.raises(asyncio.QueueFull):
            _feed(sub, b"C1")
        release.set_result(None)
        await _settle()
        assert sub.pending_msgs == 0
        await _shutdown(sub)

    _run(scenario())


def test_drain_waits_for_accepted_messages():

    async def scenario():
        loop = asyncio.get_running_loop()
        release = loop.create_future()
        finished = []

        async def cb(msg):
            if msg.data == b"A1":
                await release
            finished.append(msg.data.decode())

        conn, sub, errors = _make_sub(
            cb, key_cb=lambda m: m.data[:1], concurrency=2
        )
        _feed(sub, b"A1")
        _feed(sub, b"A2")
        await _settle()
        drain_task = asyncio.create_task(sub.drain())
        await _settle()
        # Drain cannot complete while an accepted message is blocked.
        assert not drain_task.done()
        release.set_result(None)
        await drain_task
        # Both accepted messages were processed before drain finished.
        assert finished == ["A1", "A2"]
        assert sub._closed
        assert conn.removed == [1]
        assert conn.unsubscribed == [(1, 0)]
        await _shutdown(sub)

    _run(scenario())


def test_close_releases_all_dispatch_tasks():

    async def scenario():
        loop = asyncio.get_running_loop()
        never = loop.create_future()
        cancelled = []

        async def cb(msg):
            try:
                await never
            except asyncio.CancelledError:
                cancelled.append(msg.data.decode())
                raise

        conn, sub, errors = _make_sub(
            cb, key_cb=lambda m: m.data[:1], concurrency=1
        )
        _feed(sub, b"A1")
        _feed(sub, b"A2")
        _feed(sub, b"B1")
        await _settle()
        assert len(sub._keyed_tasks) == 1
        # Mimic what Client._close does: cancel the dispatch task.
        sub._wait_for_msgs_task.cancel()
        await asyncio.gather(sub._wait_for_msgs_task, return_exceptions=True)
        # The in-flight callback was cancelled and every lane worker
        # and lane buffer was released.
        assert cancelled == ["A1"]
        assert sub._keyed_tasks == set()
        assert sub._key_lanes == {}
        assert sub._active_keys == set()

    _run(scenario())


def test_unsubscribe_immediate_cancels_processing():

    async def scenario():
        loop = asyncio.get_running_loop()
        never = loop.create_future()

        async def cb(msg):
            await never

        conn, sub, errors = _make_sub(
            cb, key_cb=lambda m: m.data[:1], concurrency=1
        )
        _feed(sub, b"A1")
        _feed(sub, b"A2")
        await _settle()
        await sub.unsubscribe()
        assert sub._closed
        assert conn.removed == [1]
        assert conn.unsubscribed == [(1, 0)]
        await _settle()
        assert sub._wait_for_msgs_task.done()
        assert sub._keyed_tasks == set()

    _run(scenario())


def test_unsubscribe_with_limit_processes_accepted_messages():

    async def scenario():
        finished = []

        async def cb(msg):
            finished.append(msg.data.decode())

        conn, sub, errors = _make_sub(
            cb, key_cb=lambda m: m.data[:1], concurrency=2
        )
        _feed(sub, b"A1")
        _feed(sub, b"A2")
        # Limit already reached but accepted messages are still pending,
        # so the subscription stops only after they complete.
        await sub.unsubscribe(limit=2)
        assert not sub._closed
        assert conn.unsubscribed == [(1, 2)]
        await _settle()
        assert finished == ["A1", "A2"]
        assert sub._wait_for_msgs_task.done()
        assert sub._keyed_tasks == set()
        await _shutdown(sub)

    _run(scenario())
