import asyncio
import unittest

from nats.aio.client import Client
from nats.aio.headers import Headers
from nats.aio.subscription import Subscription
from nats.protocol.command import hpub_cmd
from tests.utils import async_test


class HeaderTest(unittest.TestCase):

    def test_case_insensitive_multi_values_and_order(self):
        headers = Headers()
        headers.add("X-One", "first")
        headers.add("x-one", "second")
        headers["X-Two"] = "value"

        self.assertEqual(headers["X-ONE"], "second")
        self.assertEqual(headers.get_all("x-one"), ["first", "second"])
        self.assertEqual(headers.getlist("X-Two"), ["value"])
        self.assertEqual(
            list(headers.multi_items()),
            [("X-One", "first"), ("x-one", "second"), ("X-Two", "value")],
        )

    def test_status_is_separate_from_ordinary_fields(self):
        raw = (
            b"NATS/1.0 100 Idle Heartbeat\r\n"
            b"Status: ordinary\r\n"
            b"Nats-Consumer-Stalled: _INBOX.x\r\n"
            b"Description: ordinary description\r\n\r\n"
        )
        headers = Headers.from_wire(raw)

        self.assertEqual(headers.status, "100")
        self.assertEqual(headers.description, "Idle Heartbeat")
        self.assertEqual(headers["Status"], "100")
        self.assertEqual(headers["Description"], "Idle Heartbeat")
        self.assertEqual(
            headers.wire_items(),
            [
                ("Status", "100"),
                ("Description", "Idle Heartbeat"),
                ("Status", "ordinary"),
                ("Nats-Consumer-Stalled", "_INBOX.x"),
                ("Description", "ordinary description"),
            ],
        )
        self.assertEqual(headers.get_all("Status"), ["ordinary"])
        self.assertEqual(
            headers.get_all("Description"), ["ordinary description"]
        )


    def test_status_line_serialization(self):
        headers = Headers(status="100", description="Idle Heartbeat")
        headers["Nats-Consumer-Stalled"] = "_INBOX.x"

        self.assertEqual(
            headers.to_wire(),
            b"NATS/1.0 100 Idle Heartbeat\r\n"
            b"Nats-Consumer-Stalled: _INBOX.x\r\n\r\n",
        )

    def test_ordinary_status_does_not_become_control_status(self):
        headers = Headers.from_wire(b"NATS/1.0\r\nStatus: ordinary\r\n\r\n")

        self.assertIsNone(headers.status)
        self.assertIsNone(headers.description)
        self.assertEqual(headers["Status"], "ordinary")
        self.assertEqual(
            headers.to_wire(), b"NATS/1.0\r\nStatus: ordinary\r\n\r\n"
        )

    def test_utf8_hpub_length(self):
        value = "héllo"
        headers = Headers({"X": value})
        payload = "wörld".encode()
        wire = hpub_cmd("foo", "", headers.to_wire(), payload)

        header_block = headers.to_wire()
        self.assertEqual(len(value.encode()), 6)
        self.assertTrue(
            wire.startswith(
                f"HPUB foo  {len(header_block)} "
                f"{len(header_block) + len(payload)}\r\n".encode()
            )
        )
        self.assertEqual(wire[-len(payload) - 2:-2], payload)

    def test_newline_injection_is_rejected(self):
        for name, value in (
            ("X\r\nInjected: yes", "value"),
            ("X", "value\r\nInjected: yes"),
        ):
            with self.assertRaises(ValueError):
                Headers({name: value}).to_wire()


class HeaderParserTest(unittest.TestCase):

    def setUp(self):
        self.loop = asyncio.new_event_loop()

    @async_test
    async def test_split_headers_mixed_messages_and_republish(self):
        nc = Client()
        messages = []
        sent = []
        errors = []

        async def cb(msg):
            messages.append(msg)

        async def send_command(cmd):
            sent.append(cmd)

        async def error_cb(error):
            errors.append(error)

        nc._error_cb = error_cb
        sub = Subscription(
            nc,
            1,
            "foo",
            cb=cb,
            pending_msgs_limit=10,
            pending_bytes_limit=1024 * 1024,
        )
        nc._subs[1] = sub
        sub._start(error_cb)
        nc._send_command = send_command
        nc._status = Client.CONNECTED

        header_block = (
            b"NATS/1.0 100 Idle Heartbeat\r\n"
            b"X-Test: one\r\n"
            b"x-test: two\r\n\r\n"
        )
        bad_header_block = b"NATS/1.0\r\nBad Header\r\n\r\n"
        good_header_block = (
            "NATS/1.0\r\nX-Forward: valeur accentuée\r\n\r\n".encode()
        )

        first = (
            f"HMSG foo 1 reply {len(header_block)} "
            f"{len(header_block) + 7}\r\n"
        ).encode() + header_block + b"payload\r\n"
        bad = (
            f"HMSG foo 1 {len(bad_header_block)} "
            f"{len(bad_header_block) + 4}\r\n"
        ).encode() + bad_header_block + b"bad!\r\n"
        good = (
            f"HMSG foo 1 {len(good_header_block)} "
            f"{len(good_header_block) + 2}\r\n"
        ).encode() + good_header_block + b"ok\r\n"
        plain = b"MSG foo 1 3\r\nabc\r\n"

        chunks = [
            first[:30],
            first[30:45],
            first[45:] + bad[:10],
            bad[10:] + good[:-5],
            good[-5:] + plain,
        ]
        for chunk in chunks:
            await nc._ps.parse(chunk)
        await asyncio.sleep(0)

        self.assertEqual(len(messages), 3)
        self.assertEqual(messages[0].data, b"payload")
        self.assertEqual(messages[0].raw_headers, header_block)
        self.assertEqual(messages[0].headers["X-Test"], "two")
        self.assertEqual(len(messages[0].headers), 3)
        self.assertEqual(
            messages[0].headers.get_all("X-Test"), ["one", "two"]
        )
        self.assertEqual(messages[0].status, "100")
        self.assertEqual(messages[0].description, "Idle Heartbeat")
        self.assertEqual(messages[1].data, b"ok")
        self.assertEqual(
            messages[1].headers["x-forward"], "valeur accentuée"
        )
        self.assertEqual(messages[2].data, b"abc")
        self.assertEqual(len(errors), 1)

        await nc.publish("forward", messages[0].data, headers=messages[0].headers)
        self.assertEqual(
            sent[0],
            b"HPUB forward  "
            + str(len(header_block)).encode()
            + b" "
            + str(len(header_block) + 7).encode()
            + b"\r\n"
            + header_block
            + b"payload\r\n",
        )
