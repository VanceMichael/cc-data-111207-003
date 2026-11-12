import json
import tempfile
import unittest
from pathlib import Path

from sse import EventStreamParser


def parse_all(chunks, last_event_id=""):
    parser = EventStreamParser(last_event_id)
    events = []
    for chunk in chunks:
        events.extend(parser.feed(chunk))
    parser.finish()
    return events, parser


class ParserTests(unittest.TestCase):
    def test_multiline_data_and_comments(self):
        raw = (
            b": keepalive\r\n"
            b"id: 41\r\nevent: state\r\n"
            b'data: {"device":"meter-a",\r\n'
            b'data: "offset":41}\r\n\r\n'
        )
        events, _ = parse_all([raw])
        self.assertEqual(len(events), 1)
        event = events[0]
        self.assertEqual(event.event, "state")
        self.assertEqual(event.id, "41")
        self.assertEqual(json.loads(event.data), {"device": "meter-a", "offset": 41})

    def test_network_chunks_are_not_event_boundaries(self):
        raw = (
            b"id: 42\ndata: {\"offset\":42}\n\n"
            b"id: 43\ndata: {\"offset\":43}\n\n"
        )
        events, _ = parse_all([raw[i:i + 3] for i in range(0, len(raw), 3)])
        self.assertEqual([e.id for e in events], ["42", "43"])

    def test_utf8_spans_chunks(self):
        raw = "id: 1\ndata: {\"state\":\"在线\"}\n\n".encode("utf-8")
        # 在多字节字符内部切开。
        cut = raw.index("在".encode("utf-8")) + 1
        events, _ = parse_all([raw[:cut], raw[cut:]])
        self.assertEqual(json.loads(events[0].data)["state"], "在线")

    def test_unterminated_tail_is_not_dispatched(self):
        events, _ = parse_all([b"id: 99\ndata: {\"offset\":99}"])
        self.assertEqual(events, [])

    def test_blank_id_clears_reconnect_position(self):
        events, parser = parse_all(
            [b"id: 7\ndata: a\n\nid\ndata: b\n\n"]
        )
        self.assertEqual([e.id for e in events], ["7", ""])
        self.assertTrue(events[1].id_changed)
        self.assertEqual(parser.last_event_id, "")

    def test_nul_id_is_ignored(self):
        events, parser = parse_all(
            [b"id: 7\ndata: a\n\nid: 8\x000\ndata: b\n\n"]
        )
        self.assertEqual(events[1].id, "7")
        self.assertFalse(events[1].id_changed)
        self.assertEqual(parser.last_event_id, "7")

    def test_cr_lf_and_bare_cr_line_endings(self):
        # 三种行终止混用；每个事件仍以独立空行终止才分派。
        raw = b"id: 1\rdata: a\r\r\nid: 2\ndata: b\n\n"
        events, _ = parse_all([raw])
        self.assertEqual([e.id for e in events], ["1", "2"])

    def test_line_break_split_across_chunks(self):
        events, _ = parse_all([b"id: 1\ndata: a\n", b"\nid: 2\ndata: b\n\n"])
        self.assertEqual([e.id for e in events], ["1", "2"])
        events, _ = parse_all([b"id: 1\ndata: a\r", b"\nid: 2\ndata: b\n\n"])
        self.assertEqual([e.id for e in events], ["1", "2"])

    def test_retry_and_unknown_fields_ignored(self):
        events, _ = parse_all([b"retry: 5000\nfoo: bar\ndata: x\n\n"])
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].data, "x")

    def test_comment_only_dispatch_is_skipped(self):
        events, _ = parse_all([b": ping\n\n"])
        self.assertEqual(events, [])


if __name__ == "__main__":
    unittest.main()
