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
from collections import deque
from typing import (
    TYPE_CHECKING,
    AsyncIterator,
    Awaitable,
    Callable,
    Deque,
    Dict,
    Hashable,
    List,
    Optional,
    Set,
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
        key_cb: Optional[Callable[[Msg], Hashable]] = None,
        concurrency: int = 1,
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

        # Keyed concurrent callback dispatch. When `key_cb` is set,
        # messages taken from the main pending queue are routed into
        # per-key lanes. Lanes of different keys are processed in
        # parallel (bounded by `concurrency`) while messages sharing a
        # key are completed one at a time in receive order. Messages
        # sitting in a lane or being processed keep counting towards
        # the pending msgs/bytes limits until their callback finishes.
        self._key_cb = key_cb
        self._concurrency = concurrency
        self._key_lanes: Dict[Hashable, Deque[Msg]] = {}
        self._active_keys: Set[Hashable] = set()
        self._keyed_tasks: Set[asyncio.Task] = set()
        # Messages accepted from the main queue but not yet completed
        # (queued in a lane or running their callback).
        self._keyed_pending = 0

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

        With keyed concurrent callbacks enabled this also includes messages
        already routed into per-key lanes or currently being processed.
        """
        return self._pending_queue.qsize() + self._keyed_pending

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
            if self._key_cb is not None and self._concurrency < 1:
                raise errors.Error(
                    "nats: keyed callback concurrency must be at least 1"
                )

            self._wait_for_msgs_task = asyncio.get_running_loop().create_task(
                self._wait_for_msgs(error_cb)
            )

        elif self._future:
            # Used to handle the single response from a request.
            pass
        else:
            self._message_iterator = _SubscriptionMessageIterator(self)

    async def drain(self):
        """
        Removes interest in a subject, but will process remaining messages.

        With keyed concurrent callbacks enabled, this waits until every
        accepted message, including the ones queued in per-key lanes or
        being processed, has completed its callback.
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

        If `limit` is greater than zero, interest is not immediately removed,
        rather, interest will be automatically removed after `limit` messages
        are received. Messages already accepted by the subscription are still
        processed to completion before the subscription stops.

        With keyed concurrent callbacks enabled, an immediate unsubscribe
        (`limit=0`) cancels in-flight callbacks and discards messages still
        queued in per-key lanes; with `limit` greater than zero, processing
        continues until every accepted message (main queue and lanes) has
        completed.
        """
        if self._conn.is_closed:
            raise errors.ConnectionClosedError
        if self._conn.is_draining:
            raise errors.ConnectionDrainingError
        if self._closed:
            raise errors.BadSubscriptionError

        self._max_msgs = limit
        if limit == 0 or (self._received >= limit and self._pending_empty()):
            self._closed = True
            self._stop_processing()
            self._conn._remove_sub(self._id)

        if not self._conn.is_reconnecting:
            await self._conn._send_unsubscribe(self._id, limit=limit)

    def _pending_empty(self) -> bool:
        """
        Whether there are no accepted messages left to process, including
        messages routed into per-key lanes or being processed.
        """
        return self._pending_queue.empty() and self._keyed_pending == 0

    def _enqueue_msg(self, msg: Msg) -> None:
        """
        Buffers a message into the pending queue, enforcing the pending
        messages limit. Raises `asyncio.QueueFull` when the limit has
        been reached.

        With keyed concurrent callbacks, messages already moved into
        per-key lanes or being processed still count towards the limit
        so that accepting them does not pretend the memory is free.
        """
        if self._key_cb is not None and (
                self._pending_queue.qsize() + self._keyed_pending
                >= self._pending_msgs_limit):
            raise asyncio.QueueFull
        self._pending_queue.put_nowait(msg)

    def _stop_processing(self) -> None:
        """
        Stops the subscription from processing new messages.
        """
        if self._wait_for_msgs_task and not self._wait_for_msgs_task.done():
            self._wait_for_msgs_task.cancel()
        # Cancelling the dispatcher also shuts down the keyed lane
        # workers, but cancel them directly as well in case the
        # dispatcher is already done.
        for task in list(self._keyed_tasks):
            task.cancel()
        if self._message_iterator:
            self._message_iterator._cancel()

    def _check_max_msgs_reached(self) -> None:
        """
        Stops the subscription once the auto unsubscribe limit has been
        received and every accepted message has been processed.
        """
        if (self._max_msgs > 0 and self._received >= self._max_msgs
                and self._pending_empty()):
            self._stop_processing()

    async def _wait_for_msgs(self, error_cb) -> None:
        """
        A coroutine to read and process messages if a callback is provided.

        Should be called as a task.
        """
        assert self._cb, "_wait_for_msgs can be called only from _start"
        if self._key_cb is not None:
            await self._dispatch_keyed_msgs(error_cb)
            return
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
                self._check_max_msgs_reached()
            except asyncio.CancelledError:
                break

    async def _dispatch_keyed_msgs(self, error_cb) -> None:
        """
        Routes messages from the main pending queue into per-key lanes so
        that different keys are processed in parallel (bounded by
        `concurrency`) while each key keeps its receive order.

        Messages stay accounted in the pending msgs/bytes limits until
        their callback completes. Should be called as a task.
        """
        assert self._key_cb, "_dispatch_keyed_msgs requires a key callback"
        try:
            while True:
                msg = await self._pending_queue.get()
                try:
                    key = self._key_cb(msg)
                except Exception as e:
                    # A failing key function cannot stall the other keys:
                    # report it through the error channel and complete the
                    # message so it stops counting as pending.
                    self._pending_size -= len(msg.data)
                    self._pending_queue.task_done()
                    if error_cb:
                        await error_cb(e)
                    continue
                lane = self._key_lanes.get(key)
                if lane is None:
                    lane = self._key_lanes[key] = deque()
                lane.append(msg)
                self._keyed_pending += 1
                self._pump_keyed_lanes(error_cb)
        except asyncio.CancelledError:
            pass
        finally:
            await self._shutdown_keyed_workers()

    def _pump_keyed_lanes(self, error_cb) -> None:
        """
        Starts lane workers for keys that have queued messages, up to the
        configured concurrency limit.
        """
        for key, lane in self._key_lanes.items():
            if len(self._keyed_tasks) >= self._concurrency:
                break
            if not lane or key in self._active_keys:
                continue
            self._active_keys.add(key)
            task = asyncio.get_running_loop().create_task(
                self._process_key_lane(key, error_cb)
            )
            self._keyed_tasks.add(task)
            task.add_done_callback(self._on_keyed_task_done)

    def _on_keyed_task_done(self, task: asyncio.Task) -> None:
        self._keyed_tasks.discard(task)
        if not task.cancelled():
            # Retrieve the exception if any so it does not go unreported
            # as "exception was never retrieved" warnings.
            task.exception()

    async def _process_key_lane(self, key: Hashable, error_cb) -> None:
        """
        Processes the messages of a single key one at a time, in the
        order they were received.
        """
        assert self._cb, "_process_key_lane requires a message callback"
        lane = self._key_lanes[key]
        try:
            while lane:
                msg = lane.popleft()
                try:
                    await self._cb(msg)
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    # A failing callback is reported through the error
                    # channel and the lane keeps moving so neither this
                    # key nor the others stay stuck.
                    if error_cb:
                        await error_cb(e)
                finally:
                    self._keyed_pending -= 1
                    self._pending_size -= len(msg.data)
                    self._pending_queue.task_done()
                self._check_max_msgs_reached()
        finally:
            self._active_keys.discard(key)
            if not lane:
                self._key_lanes.pop(key, None)
        # This worker is done, so free its concurrency slot before
        # checking whether other keys are waiting for one.
        current = asyncio.current_task()
        if current is not None:
            self._keyed_tasks.discard(current)
        self._pump_keyed_lanes(error_cb)

    async def _shutdown_keyed_workers(self) -> None:
        """
        Cancels all lane workers and waits for them to finish, releasing
        every dispatch task. Used when the subscription stops or the
        connection is closed.
        """
        tasks = [task for task in self._keyed_tasks if not task.done()]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._keyed_tasks.clear()
        self._key_lanes.clear()
        self._active_keys.clear()


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
