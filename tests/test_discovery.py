import asyncio
import time
import unittest
from unittest import mock
from urllib.parse import urlparse

from nats.aio.client import Client as NATS, Srv
from nats.errors import NoServersError
from tests.utils import async_test


class FakeTransport:
    """
    Connection stand-in that records connection attempts and fails
    for a configurable set of hosts.
    """

    def __init__(self, fail_hosts=()):
        self.attempts = []
        self.fail_hosts = set(fail_hosts)

    async def connect(self, uri, **kwargs):
        self.attempts.append(uri.hostname)
        if uri.hostname in self.fail_hosts:
            raise OSError("connection refused")

    async def connect_tls(self, *args, **kwargs):
        raise OSError("connection refused")


class PruneDiscoveredServersTest(unittest.TestCase):

    def setUp(self):
        self.loop = asyncio.new_event_loop()

    def tearDown(self):
        self.loop.close()

    def _new_client(self, seeds, prune=True, tls_required=False):
        nc = NATS()
        nc._setup_server_pool(seeds)
        nc.options["dont_randomize"] = True
        nc.options["prune_discovered_servers"] = prune
        nc._server_info = {"tls_required": tls_required}
        nc._current_server = nc._server_pool[0]
        return nc

    def _hosts(self, nc):
        return [srv.uri.hostname for srv in nc._server_pool]

    @async_test
    async def test_seeds_kept_and_discovered_pruned(self):
        nc = self._new_client(["nats://a:4222", "nats://b:4222"])

        await nc._process_info({"connect_urls": ["c:4222", "d:4222"]})
        self.assertEqual(self._hosts(nc), ["a", "b", "c", "d"])
        self.assertEqual(len(nc.discovered_servers), 2)

        # The latest INFO no longer advertises "d".
        await nc._process_info({"connect_urls": ["c:4222"]})
        self.assertEqual(self._hosts(nc), ["a", "b", "c"])
        self.assertEqual([uri.hostname for uri in nc.discovered_servers], ["c"])

        # An empty connect_urls prunes every discovered server.
        await nc._process_info({"connect_urls": []})
        self.assertEqual(self._hosts(nc), ["a", "b"])
        self.assertEqual(nc.discovered_servers, [])

    @async_test
    async def test_pruning_disabled_by_default(self):
        nc = self._new_client(["nats://a:4222"], prune=False)

        await nc._process_info({"connect_urls": ["b:4222", "c:4222"]})
        await nc._process_info({"connect_urls": ["b:4222"]})
        self.assertEqual(self._hosts(nc), ["a", "b", "c"])

    @async_test
    async def test_current_server_pruned_from_pool_but_not_disconnected(self):
        nc = self._new_client(["nats://a:4222"])

        await nc._process_info({"connect_urls": ["b:4222"]})
        current = nc._server_pool[-1]
        self.assertEqual(current.uri.hostname, "b")
        # Simulate being connected to the discovered server.
        nc._current_server = current

        # The latest INFO drops "b" while it is in use.
        await nc._process_info({"connect_urls": ["c:4222"]})

        # The active connection is untouched...
        self.assertIs(nc._current_server, current)
        # ...but "b" is no longer a reconnect candidate.
        self.assertEqual(self._hosts(nc), ["a", "c"])

    @async_test
    async def test_pruned_server_not_attempted_on_reconnect(self):
        nc = self._new_client(["nats://a:4222"], prune=True)
        nc.options.update(
            max_reconnect_attempts=2,
            reconnect_time_wait=0,
            connect_timeout=1,
            ws_connection_headers=None,
        )
        nc._error_cb = mock.AsyncMock()
        transport = FakeTransport(fail_hosts={"a"})
        nc._transport = transport

        await nc._process_info({"connect_urls": ["b:4222"]})

        # The seed fails, the discovered server accepts the connection.
        await nc._select_next_server()
        self.assertEqual(transport.attempts, ["a", "b"])
        self.assertEqual(nc._current_server.uri.hostname, "b")

        # After "b" is pruned it must never be attempted again.
        await nc._process_info({"connect_urls": []})
        for srv in nc._server_pool:
            srv.reconnects = 0
        transport.attempts.clear()
        transport.fail_hosts = {"a", "b"}
        with self.assertRaises(NoServersError):
            await nc._select_next_server()
        self.assertEqual(set(transport.attempts), {"a"})

    @async_test
    async def test_reappearing_server_gets_fresh_state(self):
        nc = self._new_client(["nats://a:4222"])

        await nc._process_info({"connect_urls": ["b:4222"]})
        srv = nc._server_pool[-1]
        # Simulate connection failures recorded against the server.
        srv.reconnects = 5
        srv.last_attempt = time.monotonic()
        srv.tls_name = "stale.example"

        # The server disappears and then reappears in a later INFO.
        await nc._process_info({"connect_urls": []})
        await nc._process_info({"connect_urls": ["b:4222"]})

        readded = nc._server_pool[-1]
        self.assertEqual(readded.uri.hostname, "b")
        self.assertIsNot(readded, srv)
        self.assertEqual(readded.reconnects, 0)
        self.assertIsNone(readded.last_attempt)
        self.assertIsNone(readded.tls_name)

    @async_test
    async def test_tls_name_recomputed_when_server_reappears(self):
        nc = self._new_client(["nats://10.0.0.1:4222"], tls_required=True)

        await nc._process_info({"connect_urls": ["10.0.0.2:4222"]})
        srv = nc._server_pool[-1]
        self.assertEqual(srv.tls_name, "10.0.0.1")

        # The server disappears and the client moves to another node
        # before it reappears: the TLS server name follows the current
        # connection, not stale state.
        await nc._process_info({"connect_urls": []})
        nc._current_server = Srv(urlparse("nats://10.0.0.9:4222"))
        await nc._process_info({"connect_urls": ["10.0.0.2:4222"]})
        readded = nc._server_pool[-1]
        self.assertEqual(readded.tls_name, "10.0.0.9")

    @async_test
    async def test_address_normalization(self):
        nc = self._new_client([
            "nats://user:pass@auth-host:4222",
            "nats://[::1]:4222",
            "nats://default-port-host",
        ])
        nc._current_server = nc._server_pool[0]

        # None of these may produce duplicates: userinfo, IPv6 brackets
        # and default ports are normalized away.
        await nc._process_info(
            {"connect_urls": [
                "auth-host:4222",
                "[::1]:4222",
                "default-port-host:4222",
                "new-host:4222",
            ]}
        )
        self.assertEqual(
            self._hosts(nc),
            ["auth-host", "::1", "default-port-host", "new-host"],
        )

    @async_test
    async def test_tls_scheme_normalization(self):
        nc = self._new_client(["tls://tls-host:4222"])

        # Discovered URLs inherit the scheme of the current connection,
        # so a tls:// seed matches the advertised address.
        await nc._process_info({"connect_urls": ["tls-host:4222"]})
        self.assertEqual(self._hosts(nc), ["tls-host"])
        self.assertEqual(nc.discovered_servers, [])

    @async_test
    async def test_discovered_server_cb_only_fires_on_change(self):
        nc = self._new_client(["nats://a:4222"])
        discovered_server_cb = mock.AsyncMock()
        nc._discovered_server_cb = discovered_server_cb

        await nc._process_info({"connect_urls": ["b:4222"]})
        self.assertEqual(discovered_server_cb.call_count, 1)

        # An identical INFO does not change the candidate set.
        await nc._process_info({"connect_urls": ["b:4222"]})
        self.assertEqual(discovered_server_cb.call_count, 1)

        # An addition fires the callback.
        await nc._process_info({"connect_urls": ["b:4222", "c:4222"]})
        self.assertEqual(discovered_server_cb.call_count, 2)

        # A removal fires the callback as well.
        await nc._process_info({"connect_urls": ["c:4222"]})
        self.assertEqual(discovered_server_cb.call_count, 3)

        # No change, no callback.
        await nc._process_info({"connect_urls": ["c:4222"]})
        self.assertEqual(discovered_server_cb.call_count, 3)

    @async_test
    async def test_discovered_server_cb_not_fired_on_initial_connection(self):
        nc = self._new_client(["nats://a:4222"])
        discovered_server_cb = mock.AsyncMock()
        nc._discovered_server_cb = discovered_server_cb

        await nc._process_info(
            {"connect_urls": ["b:4222"]}, initial_connection=True
        )
        self.assertEqual(discovered_server_cb.call_count, 0)
        self.assertEqual(self._hosts(nc), ["a", "b"])


if __name__ == "__main__":
    unittest.main()
