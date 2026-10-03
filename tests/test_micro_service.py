import asyncio
import random
import unittest

import nats
import nats.micro
from nats.micro import *
from nats.micro.request import *
from nats.micro.service import *
from tests.utils import SingleServerTestCase, async_test


class MicroServiceTest(SingleServerTestCase):

    def test_invalid_service_name(self):
        with self.assertRaises(ValueError) as context:
            ServiceConfig(name="", version="0.1.0")
            self.assertEqual(str(context.exception), "Name cannot be empty.")

        with self.assertRaises(ValueError) as context:
            ServiceConfig(name="test.service@!", version="0.1.0")
            self.assertEqual(
                str(context.exception),
                "Invalid name. It must contain only alphanumeric characters, dashes, and underscores.",
            )

    def test_invalid_service_version(self):
        with self.assertRaises(ValueError) as context:
            ServiceConfig(name="test_service", version="abc")
            self.assertEqual(
                str(context.exception),
                "Invalid version. It must follow semantic versioning (e.g., 1.0.0, 2.1.3-alpha.1).",
            )

    def test_invalid_endpoint_subject(self):

        async def noop_handler(request: Request) -> None:
            pass

        with self.assertRaises(ValueError) as context:
            EndpointConfig(
                name="test_service",
                subject="endpoint subject",
                handler=noop_handler,
            )
        self.assertEqual(
            str(context.exception),
            "Invalid subject. Subject must not contain spaces, and can only have '>' at the end.",
        )

    @async_test
    async def test_service_basics(self):
        nc = await nats.connect()
        svcs = []

        async def add_handler(request: Request):
            if random.random() < 0.1:
                await request.respond_error("500", "Unexpected error!")
                return

            await asyncio.sleep(0.005 + random.random() * 0.005)
            await request.respond(b"42")

        service_config = ServiceConfig(
            name="CoolAddService",
            version="0.1.0",
            description="Add things together",
            metadata={"basic": "metadata"},
        )

        endpoint_config = EndpointConfig(
            name="default", subject="svc.add", handler=add_handler
        )

        for _ in range(5):
            svc = await add_service(nc, service_config)
            await svc.add_endpoint(endpoint_config)
            svcs.append(svc)

        for _ in range(50):
            await nc.request(
                "svc.add",
                json.dumps({
                    "x": 22,
                    "y": 11
                }).encode("utf-8")
            )

        for svc in svcs:
            info = svc.info()
            assert info.name == "CoolAddService"
            assert info.description == "Add things together"
            assert info.version == "0.1.0"
            assert info.metadata == {"basic": "metadata"}

        info_subject = control_subject(ServiceVerb.INFO, "CoolAddService")
        info_response = await nc.request(info_subject)
        info = ServiceInfo.from_dict(json.loads(info_response.data.decode()))

        ping_reply = nc.new_inbox()
        ping_subscription = await nc.subscribe(ping_reply)

        stats_subject = control_subject(ServiceVerb.PING, "CoolAddService")
        await nc.publish(stats_subject, reply=ping_reply)

        ping_responses = []
        while True:
            try:
                ping_responses.append(
                    await ping_subscription.next_msg(timeout=0.25)
                )
            except:
                break

        assert len(ping_responses) == 5

        stats_reply = nc.new_inbox()
        stats_subscription = await nc.subscribe(stats_reply)

        stats_subject = control_subject(ServiceVerb.STATS, "CoolAddService")
        await nc.publish(stats_subject, reply=stats_reply)

        stats_responses = []
        while True:
            try:
                stats_responses.append(
                    await stats_subscription.next_msg(timeout=0.25)
                )
            except:
                break

        assert len(stats_responses) == 5
        stats = [
            ServiceStats.from_dict(json.loads(response.data.decode()))
            for response in stats_responses
        ]
        total_requests = sum([
            stat.endpoints[0].num_requests for stat in stats
        ])
        assert total_requests == 50

    @async_test
    async def test_add_service(self):

        async def noop_handler(request: Request):
            pass

        sub_tests = {
            "no_endpoint": {
                "service_config":
                    ServiceConfig(
                        name="test_service",
                        version="0.1.0",
                        metadata={"basic": "metadata"},
                    ),
                "expected_ping":
                    ServicePing(
                        id="*",
                        type="io.nats.micro.v1.ping_response",
                        name="test_service",
                        version="0.1.0",
                        metadata={"basic": "metadata"},
                    ),
            },
            "with_single_endpoint": {
                "service_config":
                    ServiceConfig(
                        name="test_service",
                        version="0.1.0",
                    ),
                "endpoint_configs": [
                    EndpointConfig(
                        name="test",
                        subject="test",
                        handler=noop_handler,
                        metadata={"basic": "endpoint_metadata"},
                    ),
                ],
                "expected_ping":
                    ServicePing(
                        id="*",
                        name="test_service",
                        version="0.1.0",
                        metadata={},
                    ),
            },
            "with_multiple_endpoints": {
                "service_config":
                    ServiceConfig(
                        name="test_service",
                        version="0.1.0",
                    ),
                "endpoint_configs": [
                    EndpointConfig(
                        name="foo",
                        handler=noop_handler,
                    ),
                    EndpointConfig(
                        name="bar",
                        handler=noop_handler,
                    ),
                    EndpointConfig(
                        name="baz",
                        handler=noop_handler,
                    ),
                ],
                "expected_ping":
                    ServicePing(
                        id="*",
                        name="test_service",
                        version="0.1.0",
                        metadata={},
                    ),
            },
        }

        nc = await nats.connect()
        for name, data in sub_tests.items():
            with self.subTest(name=name):
                service_config = data.get("service_config")
                assert service_config

                svc = await add_service(nc, service_config)

                endpoint_configs = data.get("endpoint_configs", [])
                for endpoint_config in endpoint_configs:
                    await svc.add_endpoint(endpoint_config)

                info = svc.info()
                assert len(info.endpoints) == len(endpoint_configs)

                subject = control_subject(ServiceVerb.PING, info.name, info.id)
                ping_response = await nc.request(subject, timeout=1)
                ping = ServicePing.from_dict(json.loads(ping_response.data))

                expected_ping = data["expected_ping"]
                assert expected_ping

                assert ping.id
                assert ping.name == expected_ping.name
                assert ping.version == expected_ping.version
                assert ping.metadata == expected_ping.metadata

                await svc.stop()
                assert svc.stopped

    @async_test
    async def test_groups(self):
        sub_tests = {
            "no_groups": {
                "name": "no groups",
                "endpoint_name": "foo",
                "expected_endpoint": {
                    "name": "foo",
                    "subject": "foo"
                },
            },
            "single_group": {
                "name": "single group",
                "endpoint_name": "foo",
                "group_names": ["g1"],
                "expected_endpoint": {
                    "name": "foo",
                    "subject": "g1.foo"
                },
            },
            "single_empty_group": {
                "name": "single empty group",
                "endpoint_name": "foo",
                "group_names": [""],
                "expected_endpoint": {
                    "name": "foo",
                    "subject": "foo"
                },
            },
            "empty_groups": {
                "name": "empty groups",
                "endpoint_name": "foo",
                "group_names": ["", "g1", ""],
                "expected_endpoint": {
                    "name": "foo",
                    "subject": "g1.foo"
                },
            },
            "multiple_groups": {
                "endpoint_name": "foo",
                "group_names": ["g1", "g2", "g3"],
                "expected_endpoint": {
                    "name": "foo",
                    "subject": "g1.g2.g3.foo"
                },
            },
        }

        nc = await nats.connect()

        for name, data in sub_tests.items():
            with self.subTest(name=name):

                async def noop_handler(_):
                    pass

            svc = await add_service(
                nc, ServiceConfig(name="test_service", version="0.0.1")
            )

            group = svc
            for group_name in data.get("group_names", []):
                group = group.add_group(name=group_name)

            await group.add_endpoint(
                name=data["endpoint_name"], handler=noop_handler
            )

            info = svc.info()
            assert info.endpoints
            assert len(info.endpoints) == 1
            expected_endpoint = EndpointInfo(
                **data["expected_endpoint"], queue_group="q"
            )
            assert info.endpoints[0].name == expected_endpoint.name
            assert info.endpoints[0].subject == expected_endpoint.subject

            await svc.stop()

    @async_test
    async def test_monitoring_handlers(self):

        async def noop_handler(request: Request):
            pass

        service_config = ServiceConfig(
            name="test_service",
            version="0.1.0",
        )

        endpoint_config = EndpointConfig(
            name="default",
            subject="test.func",
            handler=noop_handler,
            metadata={"basic": "schema"},
        )

        nc = await nats.connect()
        await nc.flush()

        svc = await add_service(nc, service_config)
        await svc.add_endpoint(endpoint_config)

        sub_tests = {
            "ping_all": {
                "subject": "$SRV.PING",
                "expected_response": {
                    "type": "io.nats.micro.v1.ping_response",
                    "name": "test_service",
                    "version": "0.1.0",
                    "id": svc.id,
                    "metadata": {},
                },
            },
            "ping_name": {
                "subject": "$SRV.PING.test_service",
                "expected_response": {
                    "type": "io.nats.micro.v1.ping_response",
                    "name": "test_service",
                    "version": "0.1.0",
                    "id": svc.id,
                    "metadata": {},
                },
            },
            "ping_id": {
                "subject": f"$SRV.PING.test_service.{svc.id}",
                "expected_response": {
                    "type": "io.nats.micro.v1.ping_response",
                    "name": "test_service",
                    "version": "0.1.0",
                    "id": svc.id,
                    "metadata": {},
                },
            },
            "info_all": {
                "subject": "$SRV.INFO",
                "expected_response": {
                    "type": "io.nats.micro.v1.info_response",
                    "name": "test_service",
                    "description": None,
                    "version": "0.1.0",
                    "id": svc.id,
                    "endpoints": [{
                        "name": "default",
                        "subject": "test.func",
                        "queue_group": "q",
                        "metadata": {
                            "basic": "schema"
                        },
                    }],
                    "metadata": {},
                },
            },
            "info_name": {
                "subject": "$SRV.INFO.test_service",
                "expected_response": {
                    "type": "io.nats.micro.v1.info_response",
                    "name": "test_service",
                    "description": None,
                    "version": "0.1.0",
                    "id": svc.id,
                    "endpoints": [{
                        "name": "default",
                        "subject": "test.func",
                        "queue_group": "q",
                        "metadata": {
                            "basic": "schema"
                        },
                    }],
                    "metadata": {},
                },
            },
            "info_id": {
                "subject": f"$SRV.INFO.test_service.{svc.id}",
                "expected_response": {
                    "type": "io.nats.micro.v1.info_response",
                    "name": "test_service",
                    "description": None,
                    "version": "0.1.0",
                    "id": svc.id,
                    "endpoints": [{
                        "name": "default",
                        "subject": "test.func",
                        "queue_group": "q",
                        "metadata": {
                            "basic": "schema"
                        },
                    }],
                    "metadata": {},
                },
            },
        }

        for name, data in sub_tests.items():
            with self.subTest(name=name):
                response = await nc.request(data["subject"], timeout=1)
                response_data = json.loads(response.data)
                expected_response = data["expected_response"]
                assert response_data == expected_response

        await svc.stop()

    @async_test
    async def test_service_stats(self):

        async def handler(request: Request):
            await request.respond(b"ok")

        sub_tests = {
            "stats_handler": {
                "service_config":
                    ServiceConfig(
                        name="test_service",
                        version="0.1.0",
                    ),
                "endpoint_configs": [
                    EndpointConfig(
                        name="default",
                        subject="test.func",
                        handler=handler,
                        metadata={"test": "value"},
                    )
                ],
            },
            "with_stats_handler": {
                "service_config":
                    ServiceConfig(
                        name="test_service",
                        version="0.1.0",
                        stats_handler=lambda endpoint: {"key": "val"},
                    ),
                "endpoint_configs": [
                    EndpointConfig(
                        name="default",
                        subject="test.func",
                        handler=handler,
                        metadata={"test": "value"},
                    )
                ],
                "expected_stats": {
                    "key": "val"
                },
            },
            "with_endpoint": {
                "service_config":
                    ServiceConfig(
                        name="test_service",
                        version="0.1.0",
                    ),
                "endpoint_configs": [
                    EndpointConfig(
                        name="default",
                        subject="test.func",
                        handler=handler,
                        metadata={"test": "value"},
                    )
                ],
            },
        }

        nc = await nats.connect()

        for name, data in sub_tests.items():
            with self.subTest(name=name):
                svc = await add_service(nc, data["service_config"])

                for endpoint_config in data["endpoint_configs"]:
                    await svc.add_endpoint(endpoint_config)

                for _ in range(10):
                    response = await nc.request("test.func", b"msg", timeout=1)
                    assert response.data == b"ok"

                await nc.publish("test.func", b"err")
                await asyncio.sleep(1.0)

                info = svc.info()

                stats_subject = control_subject(
                    ServiceVerb.STATS, "test_service"
                )
                stats_response = await nc.request(
                    stats_subject, b"", timeout=1
                )
                stats = ServiceStats.from_dict(json.loads(stats_response.data))

                assert len(stats.endpoints) == len(info.endpoints)
                assert len(stats.endpoints) == len(data["endpoint_configs"])

                assert stats.endpoints[0].subject == "test.func"
                assert stats.endpoints[0].num_requests == 11
                assert stats.endpoints[0].num_errors == 1
                assert stats.endpoints[0].processing_time > 0
                assert stats.endpoints[0].average_processing_time > 0

                assert stats.endpoints[0].data == data.get("expected_stats")

                await svc.stop()

    @async_test
    async def test_request_respond(self):
        sub_tests = {
            "empty_response": {
                "respond_data": b"",
                "expected_response": b"",
            },
            "byte_response": {
                "respond_data": b"OK",
                "expected_response": b"OK",
            },
            "byte_response_with_headers": {
                "respond_headers": {
                    "key": "value"
                },
                "respond_data": b"OK",
                "expected_response": b"OK",
                "expected_headers": {
                    "key": "value"
                },
            },
        }

        nc = await nats.connect()
        for name, data in sub_tests.items():
            with self.subTest(name=name):

                async def handler(request: Request):
                    await request.respond(
                        data["respond_data"],
                        headers=data.get("respond_headers"),
                    )

                svc = await add_service(
                    nc,
                    ServiceConfig(
                        name="CoolService",
                        version="0.1.0",
                        description="test service",
                    ),
                )
                await svc.add_endpoint(
                    EndpointConfig(
                        name="default", subject="test.func", handler=handler
                    )
                )

                response = await nc.request(
                    "test.func",
                    data["respond_data"],
                    headers=data.get("respond_headers"),
                    timeout=0.5,
                )

                assert response.data == data["expected_response"]
                assert response.headers == data.get("expected_headers")

                await svc.stop()

    def test_control_subject(self):
        sub_tests = {
            "ping_all": {
                "verb": ServiceVerb.PING,
                "expected_subject": "$SRV.PING",
            },
            "ping_name": {
                "name": "PING name",
                "verb": ServiceVerb.PING,
                "name": "test",
                "expected_subject": "$SRV.PING.test",
            },
            "ping_id": {
                "verb": ServiceVerb.PING,
                "name": "test",
                "id": "123",
                "expected_subject": "$SRV.PING.test.123",
            },
        }

        for name, data in sub_tests.items():
            with self.subTest(name=name):
                subject = control_subject(
                    data["verb"], name=data.get("name"), id=data.get("id")
                )
                assert subject == data["expected_subject"]

    @async_test
    async def test_custom_queue_group(self):

        async def noop_handler(request: Request):
            pass

        sub_tests = {
            "default_queue_group": {
                "service_config":
                    ServiceConfig(
                        name="test_service",
                        version="0.0.1",
                    ),
                "endpoint_configs": [
                    EndpointConfig(
                        name="foo",
                        handler=noop_handler,
                    ),
                ],
                "expected_queue_groups": {
                    "foo": "q",
                },
            },
            "custom_queue_group_on_service_config": {
                "service_config":
                    ServiceConfig(
                        name="test_service",
                        version="0.0.1",
                        queue_group="custom",
                    ),
                "endpoint_configs": [
                    EndpointConfig(
                        name="foo",
                        subject="foo",
                        handler=noop_handler,
                    ),
                ],
                "expected_queue_groups": {
                    "foo": "custom",
                },
            },
            "endpoint_config_overriding_queue_groups": {
                "service_config":
                    ServiceConfig(
                        name="test_service",
                        version="0.0.1",
                        queue_group="q-config",
                    ),
                "endpoint_configs": [
                    EndpointConfig(
                        name="foo",
                        queue_group="q-foo",
                        handler=noop_handler,
                    ),
                ],
                "expected_queue_groups": {
                    "foo": "q-foo",
                },
            },
            "empty_queue_group_in_option_inherit_from_parent": {
                "name": "empty queue group in option, inherit from parent",
                "service_config":
                    ServiceConfig(
                        name="test_service",
                        version="0.0.1",
                        queue_group="q-service",
                    ),
                "endpoint_configs": [
                    EndpointConfig(
                        name="foo",
                        queue_group=None,
                        handler=noop_handler,
                    ),
                    EndpointConfig(
                        name="bar",
                        queue_group="",
                        handler=noop_handler,
                    ),
                ],
                "expected_queue_groups": {
                    "foo": "q-service",
                    "bar": "q-service",
                },
            },
        }

        nc = await nats.connect()

        for name, data in sub_tests.items():
            with self.subTest(name=name):
                svc = await add_service(nc, data["service_config"])

                for endpoint_config in data.get("endpoint_configs", []):
                    await svc.add_endpoint(endpoint_config)

                info = svc.info()

                assert len(info.endpoints
                           ) == len(data["expected_queue_groups"])
                for endpoint in info.endpoints:
                    assert (
                        endpoint.queue_group == data["expected_queue_groups"][
                            endpoint.name]
                    )

                await svc.stop()

    @async_test
    async def test_custom_queue_group_multiple_responses(self):
        nc = await nats.connect()
        svcs = []
        for i in range(5):

            async def handler(request: Request):
                await asyncio.sleep(0.01)
                await request.respond(str(i).encode())

            svc = await add_service(
                nc,
                ServiceConfig(
                    name="test_service",
                    version="0.0.1",
                    queue_group=f"q-{i}",
                ),
            )

            await svc.add_endpoint(
                EndpointConfig(
                    name="foo",
                    handler=handler,
                )
            )
            svcs.append(svc)

        reply = nc.new_inbox()
        await nc.publish("foo", b"req", reply=reply)
        sub = await nc.subscribe(reply)

        responses = []
        for _ in range(5):
            msg = await sub.next_msg(timeout=2)
            responses.append(msg)

        assert len(responses) == 5

        await sub.unsubscribe()
        for svc in svcs:
            await svc.stop()


class _FakeNUID:
    def __init__(self):
        self._count = 0

    def next(self):
        self._count += 1
        return f"svc-id-{self._count}".encode()


class _FakeSubscription:
    async def unsubscribe(self):
        pass

    async def drain(self):
        pass


class _FakeStatsClient:
    """Minimal client used to exercise period bookkeeping without a server."""

    def __init__(self, fail_publish=False):
        self._nuid = _FakeNUID()
        self.fail_publish = fail_publish
        self.published = []

    async def subscribe(self, subject=None, cb=None, queue=None, **kwargs):
        return _FakeSubscription()

    async def publish(self, subject, data, headers=None):
        if self.fail_publish:
            raise ConnectionError("publish failed")
        self.published.append((subject, data, headers))

    async def flush(self):
        pass


def _make_msg(client, subject="ep", reply="reply", data=b"data"):
    from nats.aio.msg import Msg
    return Msg(_client=client, subject=subject, reply=reply, data=data)


async def _make_service(client=None, stats_handler=None, handler=None):
    client = client or _FakeStatsClient()
    service = Service(
        client,
        ServiceConfig(
            name="period_service",
            version="0.1.0",
            stats_handler=stats_handler,
        ),
    )
    await service.start()

    endpoint = Endpoint(
        service,
        EndpointConfig(
            name="default",
            subject="ep",
            handler=handler,
        ),
    )
    service._endpoints.append(endpoint)
    return service, endpoint, client


class StatsPeriodTest(unittest.TestCase):

    def setUp(self):
        self.loop = asyncio.new_event_loop()

    def tearDown(self):
        self.loop.close()

    @async_test
    async def test_in_flight_request_settles_into_previous_period(self):
        old_gate = asyncio.Future()
        new_gate = asyncio.Future()
        current_gate = {"gate": old_gate}

        async def gated_handler(request: Request):
            await current_gate["gate"]
            await request.respond(b"ok")

        service, endpoint, client = await _make_service(handler=gated_handler)

        # An old request starts in period 0 and remains in flight.
        old_task = asyncio.ensure_future(
            endpoint._handle_request(_make_msg(client))
        )
        await asyncio.sleep(0)

        self.assertEqual(
            service.stats().endpoints[0].num_requests, 1
        )
        self.assertIsNone(service.take_previous_stats())

        # Rotate while the old request is still running. The current period
        # must start empty and the previous period cannot be taken yet.
        await service.reset()
        self.assertEqual(
            service.stats().endpoints[0].num_requests, 0
        )
        self.assertIsNone(service.take_previous_stats())

        # A new request belongs to the new period.
        current_gate["gate"] = new_gate
        new_task = asyncio.ensure_future(
            endpoint._handle_request(_make_msg(client))
        )
        await asyncio.sleep(0)

        self.assertEqual(
            service.stats().endpoints[0].num_requests, 1
        )
        self.assertIsNone(service.take_previous_stats())

        # Completing the new request first must not settle the old period.
        new_gate.set_result(None)
        await new_task
        self.assertIsNone(service.take_previous_stats())

        current_stats = service.stats().endpoints[0]
        self.assertEqual(current_stats.num_requests, 1)
        self.assertEqual(current_stats.num_errors, 0)
        self.assertGreater(current_stats.processing_time, 0)
        self.assertGreater(current_stats.average_processing_time, 0)

        # The late old request settles into period 0.
        old_gate.set_result(None)
        await old_task

        previous = service.take_previous_stats()
        self.assertIsNotNone(previous)
        previous_endpoint = previous.endpoints[0]
        self.assertEqual(previous_endpoint.num_requests, 1)
        self.assertEqual(previous_endpoint.num_errors, 0)
        self.assertGreater(previous_endpoint.processing_time, 0)
        self.assertGreater(
            previous_endpoint.average_processing_time, 0
        )

        # Taking is a one-shot operation and the current period is untouched.
        self.assertIsNone(service.take_previous_stats())
        self.assertEqual(
            service.stats().endpoints[0].num_requests, 1
        )

    @async_test
    async def test_empty_period_without_requests(self):
        async def handler(request: Request):
            await request.respond(b"ok")

        service, _, _ = await _make_service(handler=handler)

        # A period without any requests settles immediately on reset.
        await service.reset()
        previous = service.take_previous_stats()
        self.assertIsNotNone(previous)
        empty = previous.endpoints[0]
        self.assertEqual(empty.num_requests, 0)
        self.assertEqual(empty.num_errors, 0)
        self.assertEqual(empty.processing_time, 0)
        self.assertEqual(empty.average_processing_time, 0)
        self.assertEqual(empty.last_error, None)
        self.assertIsNone(service.take_previous_stats())

        # Consecutive empty periods are taken in closing order.
        await service.reset()
        await service.reset()
        self.assertIsNotNone(service.take_previous_stats())
        self.assertIsNotNone(service.take_previous_stats())
        self.assertIsNone(service.take_previous_stats())

    @async_test
    async def test_failed_error_response_is_counted(self):
        async def failing_handler(request: Request):
            raise ServiceError("400", "boom")

        client = _FakeStatsClient(fail_publish=True)
        service, endpoint, _ = await _make_service(
            client=client, handler=failing_handler
        )

        await endpoint._handle_request(_make_msg(client))

        stats = service.stats().endpoints[0]
        self.assertEqual(stats.num_requests, 1)
        self.assertEqual(stats.num_errors, 1)
        self.assertIn("publish failed", stats.last_error)

        async def value_error_handler(request: Request):
            raise ValueError("broken")

        endpoint2 = Endpoint(
            service,
            EndpointConfig(
                name="other",
                subject="other",
                handler=value_error_handler,
            ),
        )
        service._endpoints.append(endpoint2)
        client.fail_publish = False
        await endpoint2._handle_request(_make_msg(client, subject="other"))

        stats2 = service.stats().endpoints[1]
        self.assertEqual(stats2.num_requests, 1)
        self.assertEqual(stats2.num_errors, 1)
        self.assertIn("ValueError", stats2.last_error)

    @async_test
    async def test_cancelled_request_settles_in_its_period(self):
        gate = asyncio.Future()

        async def blocked_handler(request: Request):
            await gate

        service, endpoint, client = await _make_service(handler=blocked_handler)

        task = asyncio.ensure_future(
            endpoint._handle_request(_make_msg(client))
        )
        await asyncio.sleep(0)

        await service.reset()
        self.assertIsNone(service.take_previous_stats())

        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task

        previous = service.take_previous_stats()
        self.assertIsNotNone(previous)
        cancelled_stats = previous.endpoints[0]
        self.assertEqual(cancelled_stats.num_requests, 1)
        self.assertEqual(cancelled_stats.num_errors, 1)
        self.assertIn("CancelledError", cancelled_stats.last_error)
        self.assertGreater(cancelled_stats.processing_time, 0)

        self.assertEqual(
            service.stats().endpoints[0].num_requests, 0
        )

    @async_test
    async def test_stats_handler_cannot_mutate_internal_counters(self):
        def mutating_stats_handler(endpoint_stats):
            endpoint_stats.num_requests = 9999
            endpoint_stats.num_errors = 9999
            endpoint_stats.processing_time = 9999
            endpoint_stats.average_processing_time = 9999
            endpoint_stats.last_error = "mutated"
            return {"key": "value"}

        async def handler(request: Request):
            await request.respond(b"ok")

        service, endpoint, client = await _make_service(
            stats_handler=mutating_stats_handler, handler=handler
        )

        await endpoint._handle_request(_make_msg(client))

        first = service.stats().endpoints[0]
        self.assertEqual(first.data, {"key": "value"})
        self.assertEqual(first.num_requests, 1)
        self.assertEqual(first.num_errors, 0)
        self.assertIsNone(first.last_error)

        second = service.stats().endpoints[0]
        self.assertEqual(second.data, {"key": "value"})
        self.assertEqual(second.num_requests, 1)
        self.assertEqual(second.num_errors, 0)
        self.assertIsNone(second.last_error)

    @async_test
    async def test_periods_are_self_consistent(self):
        async def error_handler(request: Request):
            raise RuntimeError("nope")

        service, endpoint, client = await _make_service(handler=error_handler)

        for _ in range(3):
            await endpoint._handle_request(_make_msg(client))

        await service.reset()
        previous = service.take_previous_stats()
        self.assertIsNotNone(previous)
        stats = previous.endpoints[0]
        self.assertEqual(stats.num_requests, 3)
        self.assertEqual(stats.num_errors, 3)
        self.assertEqual(
            stats.average_processing_time,
            int(stats.processing_time / stats.num_requests),
        )
        self.assertIn("RuntimeError", stats.last_error)

        async def ok_handler(request: Request):
            await request.respond(b"ok")

        endpoint._handler = ok_handler
        await endpoint._handle_request(_make_msg(client))
        current = service.stats().endpoints[0]
        self.assertEqual(current.num_requests, 1)
        self.assertEqual(current.num_errors, 0)
        self.assertIsNone(current.last_error)
