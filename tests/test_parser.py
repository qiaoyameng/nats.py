import asyncio
import unittest

from nats.aio.client import Subscription
from nats.errors import ProtocolError
from nats.protocol.parser import *
from tests.utils import async_test


class MockNatsClient:

    def __init__(self):
        self._subs = {}
        self._pongs = []
        self._pings_outstanding = 0
        self._pongs_received = 0
        self._server_info = {"max_payload": 1048576, "auth_required": False}
        self._server_pool = []

    async def _send_command(self, cmd):
        pass

    async def _process_pong(self):
        pass

    async def _process_ping(self):
        pass

    async def _process_msg(self, sid, subject, reply, payload, headers=None):
        sub = self._subs[sid]
        await sub._cb(sid, subject, reply, payload)

    async def _process_err(self, err=None):
        pass

    async def _process_info(self, info):
        self._server_info = info


class ProtocolParserTest(unittest.TestCase):

    def setUp(self):
        self.loop = asyncio.new_event_loop()

    @async_test
    async def test_parse_ping(self):
        ps = Parser(MockNatsClient())
        data = b"PING\r\n"
        await ps.parse(data)
        self.assertEqual(len(ps.buf), 0)
        self.assertEqual(ps.state, AWAITING_CONTROL_LINE)

    @async_test
    async def test_parse_pong(self):
        ps = Parser(MockNatsClient())
        data = b"PONG\r\n"
        await ps.parse(data)
        self.assertEqual(len(ps.buf), 0)
        self.assertEqual(ps.state, AWAITING_CONTROL_LINE)

    @async_test
    async def test_parse_ok(self):
        ps = Parser()
        data = b"+OK\r\n"
        await ps.parse(data)
        self.assertEqual(len(ps.buf), 0)
        self.assertEqual(ps.state, AWAITING_CONTROL_LINE)

    @async_test
    async def test_parse_msg(self):
        nc = MockNatsClient()
        expected = b"hello world!"

        async def payload_test(sid, subject, reply, payload):
            self.assertEqual(payload, expected)

        params = {
            "subject": "hello",
            "queue": None,
            "cb": payload_test,
            "future": None,
        }
        sub = Subscription(nc, 1, **params)
        nc._subs[1] = sub
        ps = Parser(nc)
        data = b"MSG hello 1 world 12\r\n"

        await ps.parse(data)
        self.assertEqual(len(ps.buf), 0)
        self.assertEqual(len(ps.msg_arg.keys()), 3)
        self.assertEqual(ps.msg_arg["subject"], b"hello")
        self.assertEqual(ps.msg_arg["reply"], b"world")
        self.assertEqual(ps.msg_arg["sid"], 1)
        self.assertEqual(ps.needed, 12)
        self.assertEqual(ps.state, AWAITING_MSG_PAYLOAD)

        await ps.parse(b"hello world!")
        self.assertEqual(len(ps.buf), 12)
        self.assertEqual(ps.state, AWAITING_MSG_PAYLOAD)

        data = b"\r\n"
        await ps.parse(data)
        self.assertEqual(len(ps.buf), 0)
        self.assertEqual(ps.state, AWAITING_CONTROL_LINE)

    @async_test
    async def test_parse_msg_op(self):
        ps = Parser()
        data = b"MSG hello"
        await ps.parse(data)
        self.assertEqual(len(ps.buf), 9)
        self.assertEqual(ps.state, AWAITING_CONTROL_LINE)

    @async_test
    async def test_parse_split_msg_op(self):
        ps = Parser()
        data = b"MSG"
        await ps.parse(data)
        self.assertEqual(len(ps.buf), 3)
        self.assertEqual(ps.state, AWAITING_CONTROL_LINE)

    @async_test
    async def test_parse_split_msg_op_space(self):
        ps = Parser()
        data = b"MSG "
        await ps.parse(data)
        self.assertEqual(len(ps.buf), 4)
        self.assertEqual(ps.state, AWAITING_CONTROL_LINE)

    @async_test
    async def test_parse_split_msg_op_wrong_args(self):
        ps = Parser(MockNatsClient())
        data = b"MSG PONG\r\n"
        with self.assertRaises(ProtocolError):
            await ps.parse(data)

    @async_test
    async def test_parse_err_op(self):
        ps = Parser(MockNatsClient())
        data = b"-ERR 'Slow "
        await ps.parse(data)
        self.assertEqual(len(ps.buf), 11)
        self.assertEqual(ps.state, AWAITING_CONTROL_LINE)

        data = b"Consumer'\r\n"
        await ps.parse(data)
        self.assertEqual(len(ps.buf), 0)
        self.assertEqual(ps.state, AWAITING_CONTROL_LINE)

    @async_test
    async def test_parse_err(self):
        ps = Parser(MockNatsClient())
        data = b"-ERR 'Slow Consumer'\r\n"
        await ps.parse(data)
        self.assertEqual(len(ps.buf), 0)
        self.assertEqual(ps.state, AWAITING_CONTROL_LINE)

    @async_test
    async def test_parse_info(self):
        nc = MockNatsClient()
        ps = Parser(nc)
        server_id = "A" * 2048
        data = f'INFO {{"server_id": "{server_id}", "max_payload": 100, "auth_required": false, "connect_urls":["127.0.0.0.1:4223"]}}\r\n'
        await ps.parse(data.encode())
        self.assertEqual(len(ps.buf), 0)
        self.assertEqual(ps.state, AWAITING_CONTROL_LINE)
        self.assertEqual(len(nc._server_info["server_id"]), 2048)

    @async_test
    async def test_parse_msg_long_subject_reply(self):
        nc = MockNatsClient()

        msgs = 0

        async def payload_test(sid, subject, reply, payload):
            nonlocal msgs
            msgs += 1

        params = {
            "subject": "hello",
            "queue": None,
            "cb": payload_test,
            "future": None,
        }
        sub = Subscription(nc, 1, **params)
        nc._subs[1] = sub

        ps = Parser(nc)
        reply = "A" * 2043
        data = f"PING\r\nMSG hello 1 {reply}"
        await ps.parse(data.encode())
        await ps.parse(b"AAAAA 0\r\n\r\nMSG hello 1 world 0")
        self.assertEqual(msgs, 1)
        self.assertEqual(len(ps.buf), 19)
        self.assertEqual(ps.state, AWAITING_CONTROL_LINE)
        await ps.parse(b"\r\n\r\n")
        self.assertEqual(msgs, 2)

    @async_test
    async def test_parse_unknown_long_protocol_line(self):
        nc = MockNatsClient()

        msgs = 0

        async def payload_test(sid, subject, reply, payload):
            nonlocal msgs
            msgs += 1

        params = {
            "subject": "hello",
            "queue": None,
            "cb": payload_test,
            "future": None,
        }
        sub = Subscription(nc, 1, **params)
        nc._subs[1] = sub

        ps = Parser(nc)
        reply = "A" * 2043 * 4

        # FIXME: Malformed long protocol lines will not be detected
        # by the client in this case so we rely on the ping/pong interval
        # from the client to give up instead.
        data = f"PING\r\nWRONG hello 1 {reply}"
        await ps.parse(data.encode())
        await ps.parse(b"AAAAA 0")
        self.assertEqual(ps.state, AWAITING_CONTROL_LINE)
        await ps.parse(b"\r\n\r\n")
        await ps.parse(b"\r\n\r\n")

        ps = Parser(nc)
        reply = "A" * 2043
        data = f"PING\r\nWRONG hello 1 {reply}"
        with self.assertRaises(ProtocolError):
            await ps.parse(data.encode())
            await ps.parse(b"AAAAA 0")
            self.assertEqual(ps.state, AWAITING_CONTROL_LINE)
            await ps.parse(b"\r\n\r\n")
            await ps.parse(b"\r\n\r\n")


class HeaderParserTest(unittest.TestCase):
    """Multi-value header parsing, split reads and message forwarding."""

    def setUp(self):
        self.loop = asyncio.new_event_loop()

    def _client(self):
        from nats.aio.headers import Headers

        nc = MockNatsClient()
        nc.header_errors = []

        async def on_err(err):
            nc.header_errors.append(err)

        nc._error_cb = on_err
        nc.messages = []

        async def process_msg(sid, subject, reply, payload, headers=None):
            if headers is not None:
                headers = Headers.parse(
                    headers, on_error=nc.header_errors.append
                )
            nc.messages.append((subject, reply, payload, headers))

        nc._process_msg = process_msg
        return nc

    @async_test
    async def test_split_status_line_multi_value_and_plain_mix(self):
        from nats.protocol import command as prot_command

        nc = self._client()
        ps = Parser(nc)

        hdr_block = (
            b"NATS/1.0 100\r\nIdle Heartbeat\r\n"
            b"X: one\r\nx: two\r\n\r\n"
        )
        total = len(hdr_block)
        hmsg = (
            b"HMSG hs 1 hr %d %d\r\n" % (total, total)
            + hdr_block + b"\r\n"
        )
        plain = b"MSG ps 1  5\r\nhello\r\n"
        block = hmsg + plain

        # Split the status line itself across two reads.
        cut = hmsg.index(b"NATS/1.0 1") + len(b"NATS/1.0 1")
        await ps.parse(block[:cut])
        await ps.parse(block[cut:])

        self.assertEqual(len(nc.messages), 2)
        subject, reply, payload, headers = nc.messages[0]
        self.assertEqual(subject, b"hs")
        self.assertEqual(reply, b"hr")
        self.assertEqual(payload, b"")

        # Old mapping reads keep last-value semantics.
        self.assertEqual(headers["X"], "two")
        self.assertEqual(headers["Status"], "100")
        self.assertEqual(headers["Description"], "Idle Heartbeat")
        self.assertEqual(len(headers), 3)

        # New full access exposes all values in original order.
        self.assertEqual(headers.get_all("x"), ["one", "two"])
        self.assertEqual(
            headers.pairs(), [("X", "one"), ("x", "two")]
        )

        # Re-publishing forwards the exact wire headers.
        wire = prot_command.hpub_cmd(
            "forward", "", headers.encode(), payload
        )
        expected_hdr = (
            b"NATS/1.0 100 Idle Heartbeat\r\n"
            b"X: one\r\nx: two\r\n\r\n"
        )
        self.assertEqual(
            wire,
            b"HPUB forward  %d %d\r\n" % (
                len(expected_hdr), len(expected_hdr)
            ) + expected_hdr + b"\r\n",
        )

        # The trailing plain message in the same chunk still arrives.
        subject, reply, payload, headers = nc.messages[1]
        self.assertEqual(subject, b"ps")
        self.assertEqual(payload, b"hello")
        self.assertIsNone(headers)
        self.assertEqual(nc.header_errors, [])

    @async_test
    async def test_bad_header_does_not_swallow_following_messages(self):
        nc = self._client()
        ps = Parser(nc)

        bad_hdr = b"NATS/1.0\r\nGood: 1\r\nBROKEN_LINE\r\n\r\n"
        bad_hmsg = (
            b"HMSG a 1  %d %d\r\n" % (len(bad_hdr), len(bad_hdr))
            + bad_hdr + b"\r\n"
        )
        good = b"MSG b 1  2\r\nhi\r\n"
        await ps.parse(bad_hmsg + good)

        self.assertEqual([m[0] for m in nc.messages], [b"a", b"b"])
        bad_msg = nc.messages[0]
        self.assertEqual(bad_msg[3].pairs(), [("Good", "1")])
        self.assertEqual(nc.messages[1][2], b"hi")
        self.assertEqual(len(nc.header_errors), 1)

    @async_test
    async def test_message_dispatch_error_keeps_parsing_chunk(self):
        nc = MockNatsClient()
        errors = []

        async def on_err(err):
            errors.append(err)

        nc._error_cb = on_err

        calls = []

        async def boom(sid, subject, reply, payload, headers=None):
            calls.append(subject)
            if subject == b"bad":
                raise RuntimeError("kaboom")

        nc._process_msg = boom
        ps = Parser(nc)
        block = (
            b"MSG bad 1  1\r\nx\r\n"
            b"MSG good 1  1\r\ny\r\n"
        )
        await ps.parse(block)
        self.assertEqual(calls, [b"bad", b"good"])
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], RuntimeError)

    @async_test
    async def test_utf8_header_lengths(self):
        nc = self._client()
        ps = Parser(nc)

        hdr_block = "NATS/1.0\r\n\u591a: \u503c\r\n\r\n".encode("utf-8")
        total = len(hdr_block)
        data = (
            b"HMSG u 1  %d %d\r\n" % (total, total)
            + hdr_block + b"\r\n"
        )
        await ps.parse(data)
        headers = nc.messages[0][3]
        self.assertEqual(headers["\u591a"], "\u503c")
        self.assertEqual(headers.encode(), hdr_block)
