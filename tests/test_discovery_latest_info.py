"""
Tests for the optional "latest INFO is the discovery set" policy.

These tests do not use a real NATS cluster: a fake transport stands in
for TCP connections and connection failures, while successive INFO
payloads are fed directly to the client to exercise the lifecycle of
seed, dynamic and currently connected server identities.
"""

import asyncio
import unittest
from unittest import mock

from nats.aio.client import (
    DEFAULT_NATS_PORT,
    Client as NATS,
    _normalize_server_url,
    _parse_discovered_url,
    _server_id,
)
from tests.utils import async_test

INFO = (
    b'INFO {"server_id":"fake","version":"9.9.9","go":"","host":"127.0.0.1",'
    b'"port":4222,"max_payload":1048576,"proto":1}\r\n'
)
PONG = b"PONG\r\n"


class FakeTransport:
    """Minimal Transport double driven by the test harness."""

    # class level registry shared between instances created by the client
    down = set()
    fail_once = set()

    def __init__(self, *args, **kwargs):
        self.connected = False
        self.initialized = False
        self.uri = None
        self.closed = False
        # Connect init expects INFO first, then the PONG to its PING.
        self._lines = [INFO, PONG]

    def _key(self, uri):
        return f"{uri.hostname}:{uri.port}"

    async def connect(self, uri, buffer_size=0, connect_timeout=2):
        key = self._key(uri)
        if key in FakeTransport.down or key in FakeTransport.fail_once:
            FakeTransport.fail_once.discard(key)
            raise ConnectionError(f"cannot connect to {key}")
        self.uri = uri
        self.connected = True
        self.initialized = True
        self._lines = [INFO, PONG]

    async def connect_tls(
        self, hostname=None, ssl_context=None, buffer_size=0,
        connect_timeout=2
    ):
        if isinstance(hostname, str):
            self.connected = True

    def write(self, payload):
        return None

    def writelines(self, payload):
        return None

    async def read(self, buffer_size=0):
        # Block until the reconnect/close logic cancels the read task.
        await asyncio.sleep(3600)
        return b""

    async def readline(self):
        await asyncio.sleep(0)
        if self._lines:
            return self._lines.pop(0)
        # Keep answering application level PINGs with PONG.
        return PONG

    async def drain(self):
        return None

    async def wait_closed(self):
        return None

    def close(self):
        self.closed = True
        self.connected = False
        # A torn down connection cannot be reused; the client builds a
        # fresh transport for its next connect attempt.
        self.initialized = False

    def at_eof(self):
        return self.closed

    def __bool__(self):
        # Truthy only while a usable connection backing it exists.
        return self.initialized


class LatestInfoDiscoveryTest(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.loop = asyncio.new_event_loop()
        FakeTransport.down = set()
        FakeTransport.fail_once = set()

    def tearDown(self):
        self.loop.close()

    def _connect_kwargs(self, **overrides):
        options = {
            "servers": ["nats://127.0.0.1:4222"],
            "dont_randomize": True,
            "allow_reconnect": True,
            "reconnect_time_wait": 0.01,
            "max_reconnect_attempts": 100,
            "connect_timeout": 1,
        }
        options.update(overrides)
        return options

    def _ids(self, nc):
        return {_server_id(s.uri) for s in nc._server_pool}

    @async_test
    async def test_seed_dynamic_and_current_identities_flow(self):
        nc = NATS()
        with mock.patch(
            "nats.aio.client.TcpTransport", FakeTransport
        ):
            await nc.connect(
                **self._connect_kwargs(
                    use_latest_info_discovery=True,
                )
            )

            seed_id = ("nats", "127.0.0.1", 4222)
            dyn_a = ("nats", "127.0.0.1", 4223)
            dyn_b = ("nats", "127.0.0.1", 4224)

            # INFO #1: two dynamic peers join; the current server is 4222.
            await nc._process_info(
                {"connect_urls": ["127.0.0.1:4223", "127.0.0.1:4224"]}
            )
            self.assertEqual(
                self._ids(nc), {seed_id, dyn_a, dyn_b}
            )
            self.assertEqual(len(nc.discovered_servers), 2)
            self.assertEqual(nc.connected_url.port, 4222)

            # Force a reconnect so that the *dynamic* node 4223 becomes
            # the current server (seed 4222 is the other candidate).
            FakeTransport.down.add("127.0.0.1:4222")
            await nc._process_op_err(ConnectionError("boom"))
            for _ in range(50):
                await asyncio.sleep(0.01)
                if nc.connected_url is not None and nc.connected_url.port == 4223:
                    break
            self.assertEqual(
                nc.connected_url.port, 4223,
                f"expected dynamic node, got {nc.connected_url}",
            )
            FakeTransport.down.discard("127.0.0.1:4222")

            # INFO #2: the current dynamic node 4223 disappears from the
            # advertised set (along with 4224). The live connection must
            # not be torn down, but the node is retired for reconnects.
            await nc._process_info({"connect_urls": []})
            self.assertTrue(nc.is_connected)
            self.assertEqual(nc.connected_url.port, 4223)

            current = nc._current_server
            self.assertTrue(current.evicted)
            # Pool keeps the evicted current node, but the set of valid
            # reconnect candidates consists only of the retained seed
            # (the live connection itself still counts while it is up).
            self.assertEqual(
                self._ids(nc),
                {seed_id, dyn_a},
            )
            self.assertEqual(
                nc._candidate_server_ids(), {seed_id, dyn_a}
            )
            non_evicted = {
                _server_id(s.uri)
                for s in nc._server_pool
                if not s.evicted
            }
            self.assertEqual(non_evicted, {seed_id})
            # The evicted current identity is not advertised as a
            # discovered candidate anymore.
            self.assertEqual(nc.discovered_servers, [])

            # The seed survives regardless.
            self.assertEqual(
                [s for s in nc._server_pool if not s.discovered][0].uri.port,
                4222,
            )

            # Now the connection to the evicted node drops. Reconnect
            # must retire it and land back on the seed 4222.
            await nc._process_op_err(ConnectionError("boom"))
            for _ in range(50):
                await asyncio.sleep(0.01)
                if nc.connected_url is not None and nc.connected_url.port == 4222:
                    break
            self.assertEqual(nc.connected_url.port, 4222)
            self.assertFalse(
                any(s.uri.port == 4223 for s in nc._server_pool)
            )
            self.assertEqual(self._ids(nc), {seed_id})
            await nc.close()

    @async_test
    async def test_seed_is_always_retained(self):
        nc = NATS()
        with mock.patch("nats.aio.client.TcpTransport", FakeTransport):
            await nc.connect(
                **self._connect_kwargs(use_latest_info_discovery=True)
            )
            await nc._process_info({"connect_urls": ["127.0.0.1:4233"]})
            self.assertEqual(
                {u.port for u in nc.servers}, {4222, 4233}
            )
            # An empty discovery set removes dynamics but keeps seeds.
            await nc._process_info({"connect_urls": []})
            self.assertEqual([u.port for u in nc.servers], [4222])
            await nc.close()

    @async_test
    async def test_reappearing_node_gets_fresh_state(self):
        nc = NATS()
        with mock.patch("nats.aio.client.TcpTransport", FakeTransport):
            await nc.connect(
                **self._connect_kwargs(use_latest_info_discovery=True)
            )
            await nc._process_info({"connect_urls": ["127.0.0.1:4240"]})

            def find(port):
                for srv in nc._server_pool:
                    if srv.uri.port == port:
                        return srv
                return None

            srv = find(4240)
            srv.reconnects = 5
            srv.last_attempt = 0.0

            # Removed then readvertised: a brand new Srv with cleared
            # failure counters and backoff must take its place.
            await nc._process_info({"connect_urls": []})
            self.assertIsNone(find(4240))
            await nc._process_info({"connect_urls": ["127.0.0.1:4240"]})
            new_srv = find(4240)
            self.assertIsNot(new_srv, srv)
            self.assertEqual(new_srv.reconnects, 0)
            self.assertIsNone(new_srv.last_attempt)
            self.assertFalse(new_srv.evicted)
            await nc.close()

    @async_test
    async def test_callback_only_on_candidate_set_change(self):
        nc = NATS()
        discovered_cb = mock.AsyncMock()
        with mock.patch("nats.aio.client.TcpTransport", FakeTransport):
            await nc.connect(
                **self._connect_kwargs(
                    use_latest_info_discovery=True,
                    discovered_server_cb=discovered_cb,
                )
            )
            urls = ["127.0.0.1:4223", "127.0.0.1:4224"]

            await nc._process_info({"connect_urls": list(urls)})
            self.assertEqual(discovered_cb.call_count, 1)

            # Same set, different order/spelling: no callback.
            await nc._process_info(
                {"connect_urls": ["127.0.0.1:4224", "nats://127.0.0.1:4223"]}
            )
            self.assertEqual(discovered_cb.call_count, 1)

            # Removal is a change too.
            await nc._process_info({"connect_urls": ["127.0.0.1:4223"]})
            self.assertEqual(discovered_cb.call_count, 2)

            # Stable again.
            await nc._process_info({"connect_urls": ["127.0.0.1:4223"]})
            self.assertEqual(discovered_cb.call_count, 2)
            await nc.close()

    @async_test
    async def test_legacy_mode_still_only_adds(self):
        nc = NATS()
        with mock.patch("nats.aio.client.TcpTransport", FakeTransport):
            await nc.connect(**self._connect_kwargs())
            await nc._process_info(
                {"connect_urls": ["127.0.0.1:4223", "127.0.0.1:4224"]}
            )
            await nc._process_info({"connect_urls": ["127.0.0.1:4223"]})
            # Default policy never removes discovered servers.
            self.assertEqual(
                {u.port for u in nc.servers}, {4222, 4223, 4224}
            )
            self.assertFalse(any(s.evicted for s in nc._server_pool))
            await nc.close()

    @async_test
    async def test_tls_scheme_and_server_name_for_dynamics(self):
        nc = NATS()
        with mock.patch("nats.aio.client.TcpTransport", FakeTransport):
            await nc.connect(
                **self._connect_kwargs(
                    servers=["tls://127.0.0.1:4222"],
                    use_latest_info_discovery=True,
                )
            )
            # IP literals over TLS pin the tls_name of the seed hostname.
            await nc._process_info(
                {
                    "connect_urls": ["127.0.0.1:4223"],
                    "tls_required": True,
                }
            )
            dyn = [s for s in nc._server_pool if s.uri.port == 4223][0]
            self.assertEqual(dyn.uri.scheme, "tls")
            self.assertEqual(dyn.tls_name, "127.0.0.1")

            # Reappearing node recomputes its tls name with fresh state.
            await nc._process_info({"connect_urls": [], "tls_required": True})
            self.assertFalse(any(s.uri.port == 4223 for s in nc._server_pool))
            await nc._process_info(
                {"connect_urls": ["127.0.0.1:4223"], "tls_required": True}
            )
            dyn = [s for s in nc._server_pool if s.uri.port == 4223][0]
            self.assertEqual(dyn.uri.scheme, "tls")
            self.assertEqual(dyn.tls_name, "127.0.0.1")
            self.assertEqual(dyn.reconnects, 0)
            await nc.close()

    @async_test
    async def test_current_node_rejoin_restores_candidate(self):
        nc = NATS()
        with mock.patch("nats.aio.client.TcpTransport", FakeTransport):
            await nc.connect(
                **self._connect_kwargs(use_latest_info_discovery=True)
            )
            # Reconnect to a dynamic node so that it is current.
            FakeTransport.down.add("127.0.0.1:4222")
            await nc._process_info({"connect_urls": ["127.0.0.1:4223"]})
            await nc._process_op_err(ConnectionError("boom"))
            for _ in range(50):
                await asyncio.sleep(0.01)
                if nc.connected_url is not None and nc.connected_url.port == 4223:
                    break
            self.assertEqual(nc.connected_url.port, 4223)
            FakeTransport.down.discard("127.0.0.1:4222")

            current = nc._current_server
            # It drops out of the discovery set while still connected.
            await nc._process_info({"connect_urls": []})
            self.assertTrue(current.evicted)
            self.assertTrue(nc.is_connected)

            # It rejoins before the connection drops: same identity is a
            # valid dynamic candidate again, reusing the live Srv.
            await nc._process_info({"connect_urls": ["127.0.0.1:4223"]})
            self.assertFalse(current.evicted)
            self.assertIs(nc._current_server, current)
            self.assertEqual(
                {u.port for u in nc.discovered_servers}, {4223}
            )
            await nc.close()

    @async_test
    async def test_reappearing_node_retried_without_backoff(self):
        nc = NATS()
        with mock.patch("nats.aio.client.TcpTransport", FakeTransport):
            await nc.connect(
                **self._connect_kwargs(use_latest_info_discovery=True)
            )
            await nc._process_info({"connect_urls": ["127.0.0.1:4250"]})
            srv = next(s for s in nc._server_pool if s.uri.port == 4250)
            srv.reconnects = 9
            srv.last_attempt = 0.0

            # Gone, then back: fresh Srv => no accumulated failures and
            # no reconnect_time_wait backoff, so it is retried promptly.
            await nc._process_info({"connect_urls": []})
            await nc._process_info({"connect_urls": ["127.0.0.1:4250"]})
            new_srv = next(
                s for s in nc._server_pool if s.uri.port == 4250
            )
            self.assertEqual(new_srv.reconnects, 0)
            self.assertIsNone(new_srv.last_attempt)
            await nc.close()


class ServerNormalizationTest(unittest.TestCase):
    def test_identity_normalizes_ipv6_default_port_and_auth(self):
        variants = [
            "nats://[2001:db8::7]:4222",
            "nats://2001:db8::7",
            "nats://user:pass@[2001:db8::7]:4222",
            "2001:db8::7",
        ]
        ids = {_server_id(_normalize_server_url(v)) for v in variants}
        self.assertEqual(ids, {("nats", "2001:db8::7", DEFAULT_NATS_PORT)})

    def test_seed_keeps_auth_but_identity_drops_it(self):
        uri = _normalize_server_url("nats://foo:bar@127.0.0.1:4222")
        self.assertEqual(uri.username, "foo")
        self.assertEqual(uri.password, "bar")
        self.assertEqual(_server_id(uri), ("nats", "127.0.0.1", 4222))

    def test_discovered_urls_drop_auth_and_apply_scheme(self):
        uri = _parse_discovered_url("user:pw@10.0.0.1:4333", "tls", True)
        self.assertIsNone(uri.username)
        self.assertEqual(uri.geturl(), "tls://10.0.0.1:4333")

        uri = _parse_discovered_url("[::1]", "nats", False)
        self.assertEqual(_server_id(uri), ("nats", "::1", 4222))

    def test_default_ports(self):
        self.assertEqual(
            _normalize_server_url("demo.nats.io").geturl(),
            "nats://demo.nats.io:4222",
        )
        self.assertEqual(
            _normalize_server_url("wss://example.com").port, 443
        )
        self.assertEqual(
            _normalize_server_url("ws://example.com").port, 80
        )

    def test_invalid_seed_url(self):
        with self.assertRaises(ValueError):
            _normalize_server_url("nats://none:4222")


if __name__ == "__main__":
    unittest.main()
