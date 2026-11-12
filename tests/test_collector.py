import json
import tempfile
import unittest

from collector import CollectorError, RetryPolicy, run
from lab import StreamSource, serving_stream
from source import Source, byte_chunks
from lab import serving
from store import Store


def event_frame(event_id, payload, event_type=None):
    lines = []
    if event_id is not None:
        lines.append(f"id: {event_id}")
    if event_type:
        lines.append(f"event: {event_type}")
    lines.append(f"data: {json.dumps(payload, ensure_ascii=False)}")
    return ("\n".join(lines) + "\n\n").encode("utf-8")


NO_WAIT = RetryPolicy(base_delay=0, max_delay=0, timeout=2)


class CollectorTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.store = Store(self.tmp.name)

    def test_drain_full_stream_once(self):
        chunks = [
            b": keepalive\n",
            event_frame("41", {"device": "a", "offset": 41, "version": 1, "state": "alarm"}),
            event_frame("42", {"device": "a", "offset": 42, "version": 2, "state": "normal"}),
        ]
        with serving_stream(StreamSource(chunks)) as base:
            report = run(base + "/events", self.store, NO_WAIT)
        self.assertEqual(report.stop_reason, "drained")
        self.assertEqual(report.events_adopted, 2)
        self.assertEqual(self.store.last_id, "42")
        self.assertEqual(self.store.position, 42)
        self.assertEqual(self.store.devices["a"]["state"], "normal")

    def test_mid_frame_disconnect_resumes_without_double_adoption(self):
        frame1 = event_frame("41", {"device": "a", "offset": 41, "state": "alarm"})
        frame2 = event_frame("42", {"device": "a", "offset": 42, "state": "normal"})
        frame3 = event_frame("43", {"device": "b", "offset": 43, "state": "在线"})
        stream = frame1 + frame2 + frame3
        # 第一次连接在 frame2 中间断开（frame1 已完整终止并确认）。
        cut_point = len(frame1) + 7
        first = StreamSource(
            byte_chunks_of(stream[:cut_point]), fail_after=10 ** 6
        )
        second = StreamSource(byte_chunks_of(stream))
        for source in (first, second):
            with serving_stream(source) as base:
                report = run(base + "/events", self.store, NO_WAIT)
        self.assertEqual(self.store.counters["events_adopted"], 3)
        self.assertEqual(self.store.counters.get("duplicates_suppressed"), 1)
        self.assertEqual(self.store.devices["a"]["state"], "normal")
        # 重连带的是已确认的 41，而不是未终止的 42。
        self.assertEqual(second.requests[-1], "41")
        self.assertEqual(self.store.position, 43)

    def test_409_switches_to_snapshot_and_skips_old_events(self):
        fixture = {
            "cut": 40,
            "snapshot": {"meter-a": {"version": 40, "state": "normal"}},
            "chunks": [],
        }
        # 预置一个已过期的重连位置。
        self.store.last_id = "5"
        self.store.commit_cursor("5", True, 5)
        source = Source(fixture)
        with serving(source) as base:
            report = run(base + "/events", self.store, NO_WAIT)
        self.assertEqual(report.snapshots_applied, 1)
        self.assertEqual(self.store.cut, 40)
        self.assertEqual(self.store.devices["meter-a"]["offset"], 40)
        self.assertIn("5", source.seen_ids)

    def test_snapshot_cannot_regress_event_covered_state(self):
        # 本地已有 offset 50 的新状态，快照 cut 40 不得倒退。
        frame = event_frame("50", {"device": "a", "offset": 50, "state": "alarm"})
        self.store.apply_event("50", True, frame.split(b"data: ", 1)[1].strip().decode())
        # 仍持有会被厂商判失效的旧重连 id，触发 409 快照恢复。
        self.store.last_id = "5"
        self.store.commit_cursor("5", True, 5)
        fixture = {"cut": 40, "snapshot": {"a": {"version": 40, "state": "normal"}},
                   "chunks": []}
        with serving(Source(fixture)) as base:
            run(base + "/events", self.store, NO_WAIT)
        self.assertEqual(self.store.devices["a"]["state"], "alarm")
        self.assertEqual(self.store.devices["a"]["offset"], 50)

    def test_stream_spanning_cut_keeps_newer_events(self):
        # 409 -> 快照 cut=40，随后流里同时有 cut 前(39)和 cut 后(41)事件。
        fixture = {
            "cut": 40,
            "snapshot": {"a": {"version": 40, "state": "normal"}},
            "chunks": [
                event_frame("39", {"device": "a", "offset": 39, "state": "old"}).decode(),
                event_frame("41", {"device": "a", "offset": 41, "state": "alarm"}).decode(),
            ],
        }
        self.store.last_id = "5"
        self.store.commit_cursor("5", True, 5)
        with serving(Source(fixture)) as base:
            run(base + "/events", self.store, NO_WAIT)
        self.assertEqual(self.store.devices["a"]["offset"], 41)
        self.assertEqual(self.store.devices["a"]["state"], "alarm")
        self.assertEqual(self.store.counters["events_before_cut_skipped"], 1)

    def test_clean_eof_after_events_drains_without_reconnect(self):
        policy = RetryPolicy(max_attempts=3, max_idle_attempts=2,
                             base_delay=0, max_delay=0)
        chunks = [event_frame("1", {"device": "a", "offset": 1, "state": "s"})]
        with serving_stream(StreamSource(chunks)) as base:
            report = run(base + "/events", self.store, policy)
        self.assertEqual(report.stop_reason, "drained")
        self.assertEqual(report.attempts, 1)

    def test_retry_policy_stops_instead_of_looping_forever(self):
        # 空流（对端提前关闭、零分派）达到空闲上限后必须报错退出。
        policy = RetryPolicy(max_attempts=5, max_idle_attempts=2,
                             base_delay=0, max_delay=0)
        with serving_stream(StreamSource([])) as base:
            with self.assertRaises(CollectorError):
                run(base + "/events", self.store, policy)

    def test_conflicting_id_is_quarantined_end_to_end(self):
        chunks = [
            event_frame("41", {"device": "a", "offset": 41, "state": "alarm"}),
            event_frame("41", {"device": "a", "offset": 41, "state": "normal"}),
        ]
        with serving_stream(StreamSource(chunks)) as base:
            run(base + "/events", self.store, NO_WAIT)
        status = self.store.status()
        self.assertEqual(len(status["quarantine"]), 1)
        self.assertEqual(self.store.devices["a"]["state"], "alarm")
        self.assertEqual(status["confirmed_position"], 41)

    def test_restart_after_write_before_commit_does_not_double_adopt(self):
        # 第一次连接：事件已落盘，但在 commit_cursor 之前进程退出。
        raw = event_frame("70", {"device": "a", "offset": 70, "state": "alarm"})
        from sse import EventStreamParser
        for event in EventStreamParser().feed(raw):
            self.store.apply_event(event.id, event.id_changed, event.data)
            # 故意不调用 commit_cursor，模拟在两步之间崩溃。
        self.assertIsNone(self.store.position)

        # 重开存储并整流重放：同 id 事件被去重，设备状态与事件数不翻倍。
        reopened = Store(self.tmp.name)
        with serving_stream(StreamSource([raw])) as base:
            report = run(base + "/events", reopened, NO_WAIT)
        self.assertEqual(report.duplicates, 1)
        self.assertEqual(report.events_adopted, 0)
        self.assertEqual(reopened.position, 70)
        self.assertEqual(reopened.devices["a"]["state"], "alarm")


def byte_chunks_of(raw, size=7):
    return [raw[i:i + size] for i in range(0, len(raw), size)]


if __name__ == "__main__":
    unittest.main()
