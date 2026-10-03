import asyncio
import json
import unittest

import nats
import nats.js.api
from nats.js.api import Header, StatusCode
from nats.js.client import JetStreamContext
from nats.js.errors import FetchTimeoutError


class FakeMsg:
    def __init__(self, data=b"", headers=None):
        self.data = data
        self.headers = headers


def business_msg(payload):
    return FakeMsg(data=payload)


def status_msg(code):
    return FakeMsg(headers={Header.STATUS: code.value})


def heartbeat_msg():
    return FakeMsg(
        headers={
            Header.STATUS: StatusCode.CONTROL_MESSAGE.value,
            Header.DESCRIPTION: "Idle Heartbeat",
        }
    )


class FakeSubscription:
    def __init__(self):
        self._pending_queue = asyncio.Queue()
        self._pending_size = 0
        self._received = 0

    async def next_msg(self, timeout=None):
        if timeout is None:
            return await self._pending_queue.get()
        return await asyncio.wait_for(self._pending_queue.get(), timeout)

    def deliver(self, msg):
        self._pending_size += len(msg.data)
        self._pending_queue.put_nowait(msg)


class FakeNATS:
    """Records every published pull request."""

    def __init__(self):
        self.requests = []

    async def publish(self, subject, payload, reply):
        self.requests.append(
            {
                "subject": subject,
                "reply": reply,
                "request": json.loads(payload.decode()),
            }
        )
        await asyncio.sleep(0)


class FakeJSM:
    pass


class FakeJS:
    def __init__(self, nc):
        self._nc = nc
        self._prefix = "$JS.API"
        self._jsm = FakeJSM()


def make_pull_sub():
    nc = FakeNATS()
    js = FakeJS(nc)
    sub = FakeSubscription()
    psub = JetStreamContext.PullSubscription(
        js=js,
        sub=sub,
        stream="mystream",
        consumer="myconsumer",
        deliver=b"deliver.inbox",
    )
    return psub, sub, nc


async def wait_for_requests(nc, count, timeout=1.0):
    """Wait until the client has published at least `count` requests."""
    async def _wait():
        while len(nc.requests) < count:
            await asyncio.sleep(0.001)

    await asyncio.wait_for(_wait(), timeout)


class PullSubscriptionWaitFullTest(unittest.IsolatedAsyncioTestCase):
    async def test_partial_arrivals_heartbeats_and_final_expiry(self):
        psub, sub, nc = make_pull_sub()

        # Two business messages already buffered, plus a stale status message
        # that must be discarded without counting towards the batch.
        sub.deliver(status_msg(StatusCode.NO_MESSAGES))
        sub.deliver(business_msg(b"buffered-1"))
        sub.deliver(business_msg(b"buffered-2"))

        task = asyncio.create_task(
            psub.fetch(5, timeout=0.6, heartbeat=0.05, wait_full=True)
        )

        # First request: remaining 3 messages. Respond with one message, a
        # heartbeat and a 'No Messages' status. Neither may reset the
        # total deadline nor count towards the batch.
        await wait_for_requests(nc, 1)
        req = nc.requests[0]["request"]
        self.assertEqual(req["batch"], 3)
        self.assertIn("expires", req)
        self.assertEqual(req["idle_heartbeat"], 50_000_000)
        first_expires = req["expires"]
        sub.deliver(business_msg(b"pulled-1"))
        sub.deliver(heartbeat_msg())
        sub.deliver(status_msg(StatusCode.NO_MESSAGES))

        # The 'No Messages' response triggers a new request for the
        # remaining 2 messages, with a smaller expires budget.
        await wait_for_requests(nc, 2)
        req = nc.requests[1]["request"]
        self.assertEqual(req["batch"], 2)
        self.assertLess(req["expires"], first_expires)
        sub.deliver(business_msg(b"pulled-2"))
        # Let this request expire on the server side.
        sub.deliver(status_msg(StatusCode.REQUEST_TIMEOUT))

        # A third request is made for the last missing message, then the
        # total deadline expires with no more deliveries.
        await wait_for_requests(nc, 3)
        req = nc.requests[2]["request"]
        self.assertEqual(req["batch"], 1)
        self.assertLess(req["expires"], nc.requests[1]["request"]["expires"])

        msgs = await task
        self.assertEqual(
            [m.data for m in msgs],
            [b"buffered-1", b"buffered-2", b"pulled-1", b"pulled-2"],
        )
        # Never fetched more than the batch.
        self.assertLessEqual(len(msgs), 5)
        # No further requests after the fetch completed.
        await asyncio.sleep(0.05)
        self.assertEqual(len(nc.requests), 3)

    async def test_fully_served_from_buffer_makes_no_request(self):
        psub, sub, nc = make_pull_sub()
        sub.deliver(business_msg(b"buffered-1"))
        sub.deliver(business_msg(b"buffered-2"))
        sub.deliver(business_msg(b"buffered-3"))

        msgs = await psub.fetch(2, timeout=0.5, wait_full=True)
        self.assertEqual([m.data for m in msgs], [b"buffered-1", b"buffered-2"])

        # No request was sent to the server and the unconsumed buffered
        # message is left untouched for the next fetch.
        self.assertEqual(nc.requests, [])
        self.assertEqual(sub._pending_queue.qsize(), 1)
        self.assertEqual(sub._pending_queue.get_nowait().data, b"buffered-3")

    async def test_expiry_without_data_raises_timeout(self):
        psub, sub, nc = make_pull_sub()

        task = asyncio.create_task(psub.fetch(3, timeout=0.2, wait_full=True))
        await wait_for_requests(nc, 1)
        self.assertEqual(nc.requests[0]["request"]["batch"], 3)
        # Only heartbeats arrive: some response was seen, so the fetch
        # times out with FetchTimeoutError once the deadline is reached.
        sub.deliver(heartbeat_msg())
        sub.deliver(status_msg(StatusCode.NO_MESSAGES))
        await wait_for_requests(nc, 2)
        sub.deliver(heartbeat_msg())

        with self.assertRaises(FetchTimeoutError):
            await task

    async def test_expiry_without_any_response_raises_timeout(self):
        psub, sub, nc = make_pull_sub()

        with self.assertRaises(nats.errors.TimeoutError) as ctx:
            await psub.fetch(2, timeout=0.2, wait_full=True)
        self.assertNotIsInstance(ctx.exception, FetchTimeoutError)
        self.assertGreaterEqual(len(nc.requests), 1)

    async def test_cancellation_leaves_no_background_request(self):
        psub, sub, nc = make_pull_sub()

        task = asyncio.create_task(psub.fetch(4, timeout=5, wait_full=True))
        await wait_for_requests(nc, 1)
        self.assertEqual(nc.requests[0]["request"]["batch"], 4)

        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task

        # No further pull requests are issued after the cancellation.
        await asyncio.sleep(0.1)
        self.assertEqual(len(nc.requests), 1)


if __name__ == "__main__":
    unittest.main()
