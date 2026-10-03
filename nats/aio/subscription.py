# Copyright 2016-2021 The NATS Authors
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

from __future__ import annotations

import asyncio
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Dict,
    List,
    Optional,
)
from uuid import uuid4

from nats import errors
# Default Pending Limits of Subscriptions
from nats.aio.msg import Msg

if TYPE_CHECKING:
    from nats.js import JetStreamContext

DEFAULT_SUB_PENDING_MSGS_LIMIT = 512 * 1024
DEFAULT_SUB_PENDING_BYTES_LIMIT = 128 * 1024 * 1024


class Subscription:
    """
    A Subscription represents interest in a particular subject.

    A Subscription should not be constructed directly, rather
    `connection.subscribe()` should be used to get a subscription.

    ::

        nc = await nats.connect()

        # Async Subscription
        async def cb(msg):
          print('Received', msg)
        await nc.subscribe('foo', cb=cb)

        # Sync Subscription
        sub = nc.subscribe('foo')
        msg = await sub.next_msg()
        print('Received', msg)

    """

    def __init__(
        self,
        conn,
        id: int = 0,
        subject: str = "",
        queue: str = "",
        cb: Optional[Callable[[Msg], Awaitable[None]]] = None,
        future: Optional[asyncio.Future] = None,
        max_msgs: int = 0,
        pending_msgs_limit: int = DEFAULT_SUB_PENDING_MSGS_LIMIT,
        pending_bytes_limit: int = DEFAULT_SUB_PENDING_BYTES_LIMIT,
        cb_key: Optional[Callable[[Msg], Any]] = None,
        cb_concurrency: int = 1,
    ) -> None:
        self._conn = conn
        self._id = id
        self._subject = subject
        self._queue = queue
        self._max_msgs = max_msgs
        self._received = 0
        self._cb = cb
        self._future = future
        self._closed = False

        # Per subscription message processor.
        self._pending_msgs_limit = pending_msgs_limit
        self._pending_bytes_limit = pending_bytes_limit
        self._pending_queue: asyncio.Queue[Msg] = asyncio.Queue(
            maxsize=pending_msgs_limit
        )
        # If no callback, then this is a sync subscription which will
        # require tracking the next_msg calls inflight for cancelling.
        if cb is None:
            self._pending_next_msgs_calls = {}
        else:
            self._pending_next_msgs_calls = None
        self._pending_size = 0
        self._wait_for_msgs_task = None
        self._message_iterator = None

        # Optional keyed concurrent callback dispatch. When a `cb_key`
        # function is given, messages are fanned out to per-key workers:
        # messages with different keys may run their callbacks in parallel
        # (up to `cb_concurrency` at a time) while messages with the same
        # key are always processed in the order they were received.
        if cb_key is not None and cb is None:
            raise errors.Error(
                "nats: cb_key requires a subscription callback"
            )
        if cb_concurrency < 1:
            raise errors.Error(
                "nats: cb_concurrency must be at least 1"
            )
        self._cb_key = cb_key
        self._cb_concurrency = cb_concurrency
        self._key_queues: Dict[Any, asyncio.Queue] = {}
        self._key_workers: Dict[Any, asyncio.Task] = {}
        # Messages already taken from the pending queue by the dispatcher
        # but not fully processed yet. They still count as pending so that
        # queued work is not hidden from the pending limits accounting.
        self._pending_inflight = 0

        # For JetStream enabled subscriptions.
        self._jsi: Optional[JetStreamContext._JSI] = None

    @property
    def subject(self) -> str:
        """
        Returns the subject of the `Subscription`.
        """
        return self._subject

    @property
    def queue(self) -> str:
        """
        Returns the queue name of the `Subscription` if part of a queue group.
        """
        return self._queue

    @property
    def messages(self) -> AsyncIterator[Msg]:
        """
        Retrieves an async iterator for the messages from the subscription.

        This is only available if a callback isn't provided when creating a
        subscription.

        ::

            nc = await nats.connect()
            sub = await nc.subscribe('foo')
            # Use `async for` which implicitly awaits messages
            async for msg in sub.messages:
                print('Received', msg)
        """
        if not self._message_iterator:
            raise errors.Error(
                "cannot iterate over messages with a non iteration subscription type"
            )

        return self._message_iterator

    @property
    def pending_msgs(self) -> int:
        """
        Number of delivered messages by the NATS Server that are being buffered
        in the pending queue.

        When keyed concurrent callbacks are enabled, messages that have been
        dispatched to a per-key queue but not processed yet are included as
        well: they are still held by the subscription and keep counting
        against the pending limits.
        """
        return self._pending_queue.qsize() + self._pending_inflight

    @property
    def pending_bytes(self) -> int:
        """
        Size of data sent by the NATS Server that is being buffered
        in the pending queue.
        """
        return self._pending_size

    @property
    def delivered(self) -> int:
        """
        Number of delivered messages to this subscription so far.
        """
        return self._received

    async def next_msg(self, timeout: Optional[float] = 1.0) -> Msg:
        """
        :params timeout: Time in seconds to wait for next message before timing out.
        :raises nats.errors.TimeoutError:

        next_msg can be used to retrieve the next message from a stream of messages using
        await syntax, this only works when not passing a callback on `subscribe`::

            sub = await nc.subscribe('hello')
            msg = await sub.next_msg(timeout=1)

        """

        async def timed_get() -> Msg:
            return await asyncio.wait_for(self._pending_queue.get(), timeout)

        if self._conn.is_closed:
            raise errors.ConnectionClosedError

        if self._cb:
            raise errors.Error(
                "nats: next_msg cannot be used in async subscriptions"
            )

        task_name = str(uuid4())
        try:
            future = asyncio.create_task(timed_get())
            self._pending_next_msgs_calls[task_name] = future
            msg = await future
        except asyncio.TimeoutError:
            if self._conn.is_closed:
                raise errors.ConnectionClosedError
            raise errors.TimeoutError
        except asyncio.CancelledError:
            if self._conn.is_closed:
                raise errors.ConnectionClosedError
            raise
        else:
            self._pending_size -= len(msg.data)
            # For sync subscriptions we will consider a message
            # to be done once it has been consumed by the client
            # regardless of whether it has been processed.
            self._pending_queue.task_done()
            return msg
        finally:
            self._pending_next_msgs_calls.pop(task_name, None)

    def _start(self, error_cb):
        """
        Creates the resources for the subscription to start processing messages.
        """
        if self._cb:
            if not asyncio.iscoroutinefunction(self._cb) and not (
                    hasattr(self._cb, "func")
                    and asyncio.iscoroutinefunction(self._cb.func)):
                raise errors.Error(
                    "nats: must use coroutine for subscriptions"
                )

            if self._cb_key is not None:
                self._wait_for_msgs_task = asyncio.get_running_loop(
                ).create_task(self._wait_for_msgs_keyed(error_cb))
            else:
                self._wait_for_msgs_task = asyncio.get_running_loop(
                ).create_task(self._wait_for_msgs(error_cb))

        elif self._future:
            # Used to handle the single response from a request.
            pass
        else:
            self._message_iterator = _SubscriptionMessageIterator(self)

    async def drain(self):
        """
        Removes interest in a subject, but will process remaining messages.

        Waits until every message that was already accepted into the pending
        queue has been fully processed by the callback. With keyed concurrent
        callbacks this includes messages sitting in per-key queues and the
        ones currently running their callback.
        """
        if self._conn.is_closed:
            raise errors.ConnectionClosedError
        if self._conn.is_draining:
            raise errors.ConnectionDrainingError
        if self._closed:
            raise errors.BadSubscriptionError
        await self._drain()

    async def _drain(self) -> None:
        try:
            # Announce server that no longer want to receive more
            # messages in this sub and just process the ones remaining.
            await self._conn._send_unsubscribe(self._id)

            # Roundtrip to ensure that the server has sent all messages.
            await self._conn.flush()

            if self._pending_queue:
                # Wait until no more messages are left,
                # then cancel the subscription task.
                await self._pending_queue.join()

            # stop waiting for messages
            self._stop_processing()

            # Subscription is done and won't be receiving further
            # messages so can throw it away now.
            self._conn._remove_sub(self._id)
        except asyncio.CancelledError:
            raise
        finally:
            self._closed = True

    async def unsubscribe(self, limit: int = 0):
        """
        :param limit: Max number of messages to receive before unsubscribing.

        Removes interest in a subject, remaining messages will be discarded.

        Without a `limit` this stops processing immediately: the dispatcher
        and any per-key callback workers are cancelled and buffered messages
        are dropped.

        If `limit` is greater than zero, interest is not immediately removed,
        rather, interest will be automatically removed after `limit` messages
        are received. Messages already accepted into the pending queue by
        then are still processed to completion before the subscription
        finishes.
        """
        if self._conn.is_closed:
            raise errors.ConnectionClosedError
        if self._conn.is_draining:
            raise errors.ConnectionDrainingError
        if self._closed:
            raise errors.BadSubscriptionError

        self._max_msgs = limit
        if limit == 0 or (self._received >= limit
                          and self.pending_msgs == 0):
            self._closed = True
            self._stop_processing()
            self._conn._remove_sub(self._id)

        if not self._conn.is_reconnecting:
            await self._conn._send_unsubscribe(self._id, limit=limit)

    def _stop_processing(self) -> None:
        """
        Stops the subscription from processing new messages.
        """
        if self._wait_for_msgs_task and not self._wait_for_msgs_task.done():
            self._wait_for_msgs_task.cancel()
        for worker in self._key_workers.values():
            if not worker.done():
                worker.cancel()
        if self._message_iterator:
            self._message_iterator._cancel()

    async def _wait_for_msgs(self, error_cb) -> None:
        """
        A coroutine to read and process messages if a callback is provided.

        Should be called as a task.
        """
        assert self._cb, "_wait_for_msgs can be called only from _start"
        while True:
            try:
                msg = await self._pending_queue.get()
                self._pending_size -= len(msg.data)

                try:
                    # Invoke depending of type of handler.
                    await self._cb(msg)
                except asyncio.CancelledError:
                    # In case the coroutine handler gets cancelled
                    # then stop task loop and return.
                    break
                except Exception as e:
                    # All errors from calling a handler
                    # are async errors.
                    if error_cb:
                        await error_cb(e)
                finally:
                    # indicate the message finished processing so drain can continue.
                    self._pending_queue.task_done()

                # Apply auto unsubscribe checks after having processed last msg.
                if (self._max_msgs > 0 and self._received >= self._max_msgs
                        and self._pending_queue.empty):
                    self._stop_processing()
            except asyncio.CancelledError:
                break

    async def _wait_for_msgs_keyed(self, error_cb) -> None:
        """
        Dispatcher coroutine used when a `cb_key` function is provided.

        Takes messages from the pending queue and hands them over to a
        per-key worker. Messages with different keys are processed in
        parallel, up to `_cb_concurrency` workers at a time, while messages
        sharing a key are processed one by one in reception order.

        Pending accounting (pending bytes and the `task_done` marker used
        by drain) is only settled once a message has been fully processed,
        so messages waiting in per-key queues keep counting against the
        pending limits instead of disappearing from them.
        """
        assert self._cb and self._cb_key, \
            "_wait_for_msgs_keyed requires both cb and cb_key"
        try:
            while True:
                msg = await self._pending_queue.get()
                self._pending_inflight += 1
                try:
                    try:
                        key = self._cb_key(msg)
                        # Keys are used as dictionary keys by the
                        # dispatcher, so reject unhashable ones early.
                        hash(key)
                    except asyncio.CancelledError:
                        raise
                    except Exception as e:
                        # A failing key function is reported through the
                        # error callback and only drops this message, it
                        # must not stall the other keys.
                        if error_cb:
                            await error_cb(e)
                        self._finish_keyed_msg(msg)
                        msg = None
                        continue

                    worker = self._key_workers.get(key)
                    while worker is None:
                        if len(self._key_workers) < self._cb_concurrency:
                            worker = self._spawn_key_worker(key, error_cb)
                        else:
                            # All callback slots are busy: wait for any key
                            # worker to become idle before dispatching more
                            # messages. The message keeps counting as
                            # pending while it is held here.
                            await asyncio.wait(
                                set(self._key_workers.values()),
                                return_when=asyncio.FIRST_COMPLETED,
                            )
                            worker = self._key_workers.get(key)
                    self._key_queues[key].put_nowait(msg)
                    # The key worker owns the message from now on.
                    msg = None

                    # Apply auto unsubscribe checks once everything received
                    # has been dispatched, then wait for the accepted
                    # messages to be processed below.
                    if (self._max_msgs > 0
                            and self._received >= self._max_msgs
                            and self._pending_queue.empty()):
                        break
                finally:
                    if msg is not None:
                        self._finish_keyed_msg(msg)
        except asyncio.CancelledError:
            # Immediate stop (unsubscribe without limit or connection
            # close): the key workers are cancelled separately via
            # `_stop_processing` and release whatever they still hold.
            return

        # Auto unsubscribe limit reached: no more messages will arrive, so
        # wait until the already accepted ones have been processed.
        if self._key_workers:
            await asyncio.gather(
                *self._key_workers.values(), return_exceptions=True
            )

    def _spawn_key_worker(self, key, error_cb) -> asyncio.Task:
        """
        Creates the queue and worker task dedicated to a key.
        """
        queue: asyncio.Queue[Msg] = asyncio.Queue()
        self._key_queues[key] = queue
        worker = asyncio.get_running_loop().create_task(
            self._key_worker(key, error_cb)
        )
        self._key_workers[key] = worker
        return worker

    async def _key_worker(self, key, error_cb) -> None:
        """
        Processes the messages of a single key one at a time, in order.

        The worker exits once its queue is empty, freeing its concurrency
        slot for other keys. The empty check and the unregistration happen
        without awaiting in between, so the dispatcher can never enqueue a
        message into a queue whose worker has already gone away.
        """
        queue = self._key_queues[key]
        try:
            while True:
                try:
                    msg = queue.get_nowait()
                except asyncio.QueueEmpty:
                    return
                try:
                    await self._cb(msg)
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    # Callback failures are reported through the error
                    # callback; the key keeps making progress with its
                    # next message and other keys are unaffected.
                    if error_cb:
                        await error_cb(e)
                finally:
                    self._finish_keyed_msg(msg)
        finally:
            # On cancellation (unsubscribe or connection close) release
            # whatever is still queued for this key so that the pending
            # accounting does not leak.
            while True:
                try:
                    msg = queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                self._finish_keyed_msg(msg)
            current = asyncio.current_task()
            if self._key_workers.get(key) is current:
                self._key_workers.pop(key, None)
                self._key_queues.pop(key, None)

    def _finish_keyed_msg(self, msg: Msg) -> None:
        """
        Settles the pending accounting of a dispatched message.
        """
        self._pending_inflight -= 1
        self._pending_size -= len(msg.data)
        self._pending_queue.task_done()


class _SubscriptionMessageIterator:

    def __init__(self, sub: Subscription) -> None:
        self._sub: Subscription = sub
        self._queue: asyncio.Queue[Msg] = sub._pending_queue
        self._unsubscribed_future: asyncio.Future[bool] = asyncio.Future()

    def _cancel(self) -> None:
        if not self._unsubscribed_future.done():
            self._unsubscribed_future.set_result(True)

    def __aiter__(self) -> _SubscriptionMessageIterator:
        return self

    async def __anext__(self) -> Msg:
        get_task = asyncio.get_running_loop().create_task(self._queue.get())
        tasks: List[asyncio.Future] = [get_task, self._unsubscribed_future]
        finished, _ = await asyncio.wait(
            tasks, return_when=asyncio.FIRST_COMPLETED
        )
        sub = self._sub

        if get_task in finished:
            self._queue.task_done()
            msg = get_task.result()
            self._sub._pending_size -= len(msg.data)

            # Unblock the iterator in case it has already received enough messages.
            if sub._max_msgs > 0 and sub._received >= sub._max_msgs:
                self._cancel()
            return msg
        elif self._unsubscribed_future.done():
            get_task.cancel()

        raise StopAsyncIteration
