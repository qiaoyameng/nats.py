"""Unit tests for service statistics period switching (reset semantics).

These tests do not require a running nats-server: endpoint messages are
delivered directly to the service endpoint coroutines using lightweight
fakes. In-flight requests are parked on controlled futures, so the tests
deterministically exercise the boundary of a period without relying on
sleeps.
"""

import asyncio
import unittest

from nats.aio.msg import Msg
from nats.micro.request import ServiceError
from nats.micro.service import (
    Endpoint,
    EndpointConfig,
    Service,
    ServiceConfig,
)
from tests.utils import async_test


class ServerFreeTestCase(unittest.TestCase):
    """Provides the event loop expected by `async_test` without a server."""

    def setUp(self):
        self.loop = asyncio.new_event_loop()

    def tearDown(self):
        self.loop.close()


class _NUID:
    def next(self):
        return b"test-service-id"


class FakeClient:
    def __init__(self):
        self.sent = []
        self.publish_error = None
        self._nuid = _NUID()

    async def publish(self, subject, data, headers=None):
        if self.publish_error is not None:
            raise self.publish_error
        self.sent.append((subject, data, headers))

    async def subscribe(self, **kwargs):
        return None


def make_service(*, stats_handler=None, client=None):
    client = client or FakeClient()
    config = ServiceConfig(
        name="period-service",
        version="0.1.0",
        stats_handler=stats_handler,
    )
    service = Service(client, config)
    service._id = b"test-service-id".decode()
    return service, client


def add_endpoint(service, handler, *, name="default", subject="svc.fn"):
    config = EndpointConfig(
        name=name, subject=subject, handler=handler
    )
    endpoint = Endpoint(service, config)
    service._endpoints.append(endpoint)
    return endpoint


def make_msg(client, *, reply="reply-subject", data=b"msg"):
    return Msg(
        client,
        subject="svc.fn",
        reply=reply,
        data=data,
        headers=None,
        _sid=1,
    )


async def _noop_handler(request):
    pass


class PeriodsTest(ServerFreeTestCase):
    @async_test
    async def test_inflight_request_settles_into_previous_period(self):
        old_gate = asyncio.get_running_loop().create_future()
        started = asyncio.get_running_loop().create_future()

        async def handler(request):
            started.set_result(None)
            await old_gate
            await request.respond(b"old-ok")

        service, client = make_service()
        endpoint = add_endpoint(service, handler)

        # Old request begins in period one and stays in flight.
        old_task = asyncio.ensure_future(
            endpoint._handle_request(make_msg(client))
        )
        await started

        self.assertEqual(endpoint._current.in_flight, 1)
        self.assertEqual(endpoint._current.num_requests, 1)

        # Switch periods while the old request is still running.
        await service.reset()
        self.assertEqual(len(endpoint._closed), 1)
        closed = endpoint._closed[0]
        self.assertEqual(closed.num_requests, 1)
        self.assertEqual(closed.in_flight, 1)
        self.assertEqual(endpoint._current.num_requests, 0)

        new_done = asyncio.get_running_loop().create_future()

        async def new_handler(request):
            await request.respond(b"new-ok")
            new_done.set_result(None)

        endpoint._handler = new_handler
        new_task = asyncio.ensure_future(
            endpoint._handle_request(make_msg(client))
        )
        await new_done

        # Current period already reflects the new request; the old request
        # has not leaked into it.
        current = service.stats().endpoints[0]
        self.assertEqual(current.num_requests, 1)
        self.assertEqual(current.num_errors, 0)
        self.assertGreater(current.processing_time, 0)

        # Taking the previous summary must wait for the old in-flight
        # request to finish.
        pop_task = asyncio.ensure_future(service.pop_previous_stats())
        await asyncio.sleep(0)
        self.assertFalse(pop_task.done())

        old_gate.set_result(None)
        previous = await pop_task
        await old_task
        await new_task

        self.assertIsNotNone(previous)
        endpoint_stats = previous.endpoints[0]
        self.assertEqual(endpoint_stats.num_requests, 1)
        self.assertEqual(endpoint_stats.num_errors, 0)
        self.assertGreater(endpoint_stats.processing_time, 0)
        self.assertEqual(
            endpoint_stats.average_processing_time,
            endpoint_stats.processing_time,
        )

        # Every closed period can be taken at most once.
        self.assertIsNone(await service.pop_previous_stats())

        # Current period is unaffected once everything settles.
        current = service.stats().endpoints[0]
        self.assertEqual(current.num_requests, 1)
        self.assertEqual(current.num_errors, 0)

    @async_test
    async def test_errors_processing_time_and_last_error_are_per_period(self):
        gate = asyncio.get_running_loop().create_future()
        started = asyncio.get_running_loop().create_future()

        async def failing_handler(request):
            started.set_result(None)
            await gate
            raise RuntimeError("boom")

        service, client = make_service()
        endpoint = add_endpoint(service, failing_handler)

        old_task = asyncio.ensure_future(
            endpoint._handle_request(make_msg(client))
        )
        await started

        async def service_error_handler(request):
            raise ServiceError("400", "bad request")

        await service.reset()
        endpoint._handler = service_error_handler

        new_task = asyncio.ensure_future(
            endpoint._handle_request(make_msg(client))
        )
        # Let the new (immediately failing) request settle in period two.
        await asyncio.sleep(0)
        await new_task

        current = service.stats().endpoints[0]
        self.assertEqual(current.num_requests, 1)
        self.assertEqual(current.num_errors, 1)
        self.assertEqual(
            current.last_error, "400:bad request"
        )

        pop_task = asyncio.ensure_future(service.pop_previous_stats())
        await asyncio.sleep(0)
        self.assertFalse(pop_task.done())

        gate.set_result(None)
        previous = await pop_task
        await old_task

        old_stats = previous.endpoints[0]
        self.assertEqual(old_stats.num_requests, 1)
        self.assertEqual(old_stats.num_errors, 1)
        self.assertIn("boom", old_stats.last_error or "")
        self.assertGreater(old_stats.processing_time, 0)
        self.assertEqual(
            old_stats.average_processing_time, old_stats.processing_time
        )

    @async_test
    async def test_failed_error_response_is_handled_explicitly(self):
        client = FakeClient()
        client.publish_error = RuntimeError("connection closed")

        service, _ = make_service(client=client)

        async def failing_handler(request):
            raise RuntimeError("handler boom")

        endpoint = add_endpoint(service, failing_handler)

        # The secondary publish failure propagates to the dispatcher instead
        # of being swallowed or corrupting the counters...
        with self.assertRaises(RuntimeError) as ctx:
            await endpoint._handle_request(make_msg(client))
        self.assertEqual(str(ctx.exception), "connection closed")

        # ...and the request is still settled exactly once in its period.
        stats = service.stats().endpoints[0]
        self.assertEqual(stats.num_requests, 1)
        self.assertEqual(stats.num_errors, 1)
        self.assertIn("handler boom", stats.last_error or "")
        self.assertGreater(stats.processing_time, 0)
        self.assertEqual(endpoint._current.in_flight, 0)

    @async_test
    async def test_cancelled_request_settles_in_its_period(self):
        gate = asyncio.get_running_loop().create_future()
        started = asyncio.get_running_loop().create_future()

        async def handler(request):
            started.set_result(None)
            await gate

        service, client = make_service()
        endpoint = add_endpoint(service, handler)

        task = asyncio.ensure_future(
            endpoint._handle_request(make_msg(client))
        )
        await started
        task.cancel()

        with self.assertRaises(asyncio.CancelledError):
            await task

        # Cancellation is a settled request, not an error.
        stats = service.stats().endpoints[0]
        self.assertEqual(stats.num_requests, 1)
        self.assertEqual(stats.num_errors, 0)
        self.assertIsNone(stats.last_error)
        self.assertEqual(endpoint._current.in_flight, 0)

    @async_test
    async def test_empty_period_resets_and_pops_zeroed_summary(self):
        service, client = make_service()
        add_endpoint(service, _noop_handler)

        self.assertIsNone(await service.pop_previous_stats())

        await service.reset()
        previous = await service.pop_previous_stats()
        self.assertIsNotNone(previous)
        stats = previous.endpoints[0]
        self.assertEqual(stats.num_requests, 0)
        self.assertEqual(stats.num_errors, 0)
        self.assertEqual(stats.processing_time, 0)
        self.assertEqual(stats.average_processing_time, 0)
        self.assertIsNone(stats.last_error)

        current = service.stats()
        self.assertEqual(current.endpoints[0].num_requests, 0)

    @async_test
    async def test_stats_handler_cannot_mutate_internal_counters(self):
        def stats_handler(snapshot):
            # Mutate the snapshot handed to the extension callback.
            snapshot.num_requests = 9999
            snapshot.num_errors = 9999
            snapshot.processing_time = 9999
            snapshot.average_processing_time = 9999
            snapshot.last_error = "polluted"
            return {"handled": True}

        service, client = make_service(stats_handler=stats_handler)

        async def ok_handler(request):
            pass

        endpoint = add_endpoint(service, ok_handler, subject="svc.fn")

        msg = make_msg(client)
        await endpoint._handle_request(msg)

        stats = service.stats().endpoints[0]
        # The handler is allowed to mutate the snapshot it receives.
        self.assertEqual(stats.num_requests, 9999)
        self.assertEqual(stats.last_error, "polluted")
        self.assertEqual(stats.data, {"handled": True})

        # Internal counters of the period must remain untouched.
        internal = endpoint.current_stats()
        self.assertEqual(internal.num_requests, 1)
        self.assertEqual(internal.num_errors, 0)
        self.assertIsNone(internal.last_error)
        self.assertGreater(internal.processing_time, 0)

        # A reset must hand out pristine internal snapshots as well.
        await service.reset()
        previous = await service.pop_previous_stats()
        old_stats = previous.endpoints[0]
        self.assertEqual(old_stats.num_requests, 1)
        self.assertEqual(old_stats.num_errors, 0)
        self.assertIsNone(old_stats.last_error)

    @async_test
    async def test_started_marks_each_period(self):
        service, client = make_service()
        add_endpoint(service, _noop_handler)

        first_started = service.stats().started
        await asyncio.sleep(0.005)
        await service.reset()
        self.assertGreater(service.stats().started, first_started)

        previous = await service.pop_previous_stats()
        self.assertEqual(previous.started, first_started)

    @async_test
    async def test_closed_periods_are_taken_in_order(self):
        service, client = make_service()
        endpoint = add_endpoint(service, _noop_handler)

        # Period one carries one request.
        await endpoint._handle_request(make_msg(client))
        await service.reset()

        # Period two carries two requests.
        await endpoint._handle_request(make_msg(client))
        await endpoint._handle_request(make_msg(client))
        await service.reset()

        first = await service.pop_previous_stats()
        self.assertEqual(first.endpoints[0].num_requests, 1)

        second = await service.pop_previous_stats()
        self.assertEqual(second.endpoints[0].num_requests, 2)

        self.assertIsNone(await service.pop_previous_stats())
        # Only the fresh period is live afterwards.
        self.assertEqual(service.stats().endpoints[0].num_requests, 0)

    @async_test
    async def test_reset_while_pop_waits_keeps_both_periods(self):
        first_gate = asyncio.get_running_loop().create_future()
        first_started = asyncio.get_running_loop().create_future()

        async def slow_handler(request):
            first_started.set_result(None)
            await first_gate

        service, client = make_service()
        endpoint = add_endpoint(service, slow_handler)

        first_task = asyncio.ensure_future(
            endpoint._handle_request(make_msg(client))
        )
        await first_started

        # Reset to period two while period one's request is in flight, and
        # start taking period one immediately.
        await service.reset()
        pop_task = asyncio.ensure_future(service.pop_previous_stats())
        await asyncio.sleep(0)
        self.assertFalse(pop_task.done())

        # Another reset moves on to period three while the take is pending.
        endpoint._handler = _noop_handler
        await service.reset()
        await endpoint._handle_request(make_msg(client))

        # Completing the original request unblocks the first take...
        first_gate.set_result(None)
        first = await pop_task
        await first_task
        self.assertEqual(first.endpoints[0].num_requests, 1)

        # ...and period two (empty) and period three remain available in
        # order.
        second = await service.pop_previous_stats()
        self.assertEqual(second.endpoints[0].num_requests, 0)

        self.assertIsNone(await service.pop_previous_stats())
        # The request made after the second reset belongs to the current
        # period three.
        self.assertEqual(service.stats().endpoints[0].num_requests, 1)

    @async_test
    async def test_cancelled_inflight_request_finalizes_closed_period(self):
        gate = asyncio.get_running_loop().create_future()
        started = asyncio.get_running_loop().create_future()

        async def handler(request):
            started.set_result(None)
            await gate

        service, client = make_service()
        endpoint = add_endpoint(service, handler)

        task = asyncio.ensure_future(
            endpoint._handle_request(make_msg(client))
        )
        await started

        await service.reset()
        closed = endpoint._closed[0]
        self.assertFalse(closed.settled.is_set())

        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task

        # The closed period settles on cancellation and can be taken without
        # hanging.
        previous = await service.pop_previous_stats()
        stats = previous.endpoints[0]
        self.assertEqual(stats.num_requests, 1)
        self.assertEqual(stats.num_errors, 0)
        self.assertGreater(stats.processing_time, 0)
