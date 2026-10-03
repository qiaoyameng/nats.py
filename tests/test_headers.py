import unittest

from nats.aio.headers import Headers
from nats.protocol import command as prot_command


class HeadersTest(unittest.TestCase):

    def test_old_mapping_keeps_last_value(self):
        h = Headers([("Foo", "1"), ("foo", "2")])
        self.assertEqual(h["foo"], "2")
        self.assertEqual(h.get("FOO"), "2")
        self.assertIn("foo", h)
        self.assertEqual(len(h), 1)
        self.assertEqual(dict(h.items()), {"Foo": "2"})
        # Backwards compatible equality with plain dicts (case-insensitive).
        self.assertEqual(h, {"foo": "2"})
        self.assertEqual(h, {"FOO": "2"})

    def test_multi_values_preserve_order(self):
        h = Headers([("X", "a"), ("Y", "b"), ("x", "c")])
        self.assertEqual(h.get_all("X"), ["a", "c"])
        self.assertEqual(h.get_all("x"), ["a", "c"])
        self.assertEqual(h.get_all("missing", []), [])
        self.assertEqual(
            h.pairs(), [("X", "a"), ("Y", "b"), ("x", "c")]
        )

    def test_build_with_dict_of_lists(self):
        h = Headers({"X": ["1", "2"], "y": "3"})
        self.assertEqual(h.get_all("x"), ["1", "2"])
        self.assertEqual(h["Y"], "3")

    def test_status_line_is_not_a_regular_field(self):
        raw = b"NATS/1.0 404 No Messages\r\nX: 1\r\n\r\n"
        h = Headers.parse(raw)
        self.assertEqual(h.status, "404")
        self.assertEqual(h.description, "No Messages")
        self.assertEqual(h["Status"], "404")
        self.assertEqual(h["Description"], "No Messages")
        # Ordinary fields live separately from the control status.
        self.assertEqual(h.pairs(), [("X", "1")])
        # A regular field named "Status" cannot override the control status.
        h.add("Status", "999")
        self.assertEqual(h.status, "404")
        self.assertEqual(h["Status"], "404")
        # Setting through the mapping updates the real control status.
        h["status"] = "503"
        self.assertEqual(h.status, "503")
        del h["STATUS"]
        self.assertIsNone(h.status)

    def test_status_validation(self):
        for bad in ("abc", "12", "1234", ""):
            with self.assertRaises(ValueError):
                Headers(status=bad)
        h = Headers()
        h.status = 200
        self.assertEqual(h.status, "200")
        h.status = None
        self.assertIsNone(h.status)

    def test_newline_injection_rejected(self):
        for bad_name in ("X\r\n", "X\n", "X\r"):
            with self.assertRaises(ValueError):
                Headers({bad_name: "v"})
        for bad_value in ("a\r\nb: c", "a\nb", "a\rb"):
            with self.assertRaises(ValueError):
                Headers({"X": bad_value})
            h = Headers()
            with self.assertRaises(ValueError):
                h["X"] = bad_value
            with self.assertRaises(ValueError):
                h.add("X", bad_value)
        with self.assertRaises(ValueError):
            Headers(status=200, description="x\r\nEvil: 1")

    def test_parse_plain_headers(self):
        h = Headers.parse(b"NATS/1.0\r\nA: 1\r\na: 2\r\n\r\n")
        self.assertIsNone(h.status)
        self.assertEqual(h.get_all("a"), ["1", "2"])

    def test_parse_status_without_description(self):
        h = Headers.parse(b"NATS/1.0 503\r\n\r\n")
        self.assertEqual(h.status, "503")
        self.assertIsNone(h.description)
        self.assertEqual(h.encode(), b"NATS/1.0 503\r\n\r\n")

    def test_parse_legacy_heartbeat(self):
        h = Headers.parse(
            b"NATS/1.0 100\r\nIdle Heartbeat\r\n"
            b"Nats-Last-Consumer: 1\r\n\r\n"
        )
        self.assertEqual(h.status, "100")
        self.assertEqual(h.description, "Idle Heartbeat")
        self.assertEqual(h["Nats-Last-Consumer"], "1")

    def test_bad_lines_do_not_drop_other_fields(self):
        errors = []
        h = Headers.parse(
            b"NATS/1.0\r\nGood: 1\r\nBROKEN\r\nAlso: 2\r\n\r\n",
            on_error=errors.append,
        )
        self.assertEqual(h.pairs(), [("Good", "1"), ("Also", "2")])
        self.assertEqual(len(errors), 1)

    def test_empty_block(self):
        self.assertIsNone(Headers.parse(b"NATS/1.0\r\n\r\n"))
        errors = []
        self.assertIsNone(
            Headers.parse(b"BOGUS\r\n", on_error=errors.append)
        )
        self.assertEqual(len(errors), 1)

    def test_encode_roundtrip_and_utf8_sizes(self):
        raw = (
            b"NATS/1.0 100 Idle Heartbeat\r\n"
            b"X: 1\r\nx: 2\r\n\r\n"
        )
        h = Headers.parse(raw)
        self.assertEqual(h.encode(), raw)

        hdr = Headers([("多", "值")])
        wire = hdr.encode()
        # HPUB header length and total length are byte counts.
        cmd = prot_command.hpub_cmd("s", "", wire, b"abc")
        expected = (
            b"HPUB s  %d %d\r\n" % (len(wire), len(wire) + 3)
            + wire + b"abc\r\n"
        )
        self.assertEqual(cmd, expected)
        # Sanity: UTF-8 makes the fields larger than the python str length.
        self.assertEqual(len(wire), len(b"NATS/1.0\r\n") + len(
            "\u591a: \u503c\r\n".encode("utf-8")
        ) + 2)


if __name__ == "__main__":
    unittest.main()
