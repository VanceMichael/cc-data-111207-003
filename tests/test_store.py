import json
import os
import tempfile
import unittest
from pathlib import Path

from store import Store


def make_event(event_id, offset, device="meter-a", version=None, state="normal",
               id_changed=True):
    return event_id, id_changed, json.dumps({
        "device": device,
        "offset": offset,
        "version": version if version is not None else offset,
        "state": state,
    })


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.dir = self.tmp.name

    def store(self):
        return Store(self.dir)

    def test_adopt_then_commit_advances_cursor(self):
        store = self.store()
        result = store.apply_event(*make_event("41", 41, state="alarm"))
        self.assertTrue(result.adopted)
        self.assertIsNone(store.position)
        store.commit_cursor("41", True, 41)
        self.assertEqual(store.position, 41)
        self.assertEqual(store.last_id, "41")

        reopened = self.store()
        self.assertEqual(reopened.position, 41)
        self.assertEqual(reopened.last_id, "41")
        self.assertEqual(reopened.devices["meter-a"]["state"], "alarm")

    def test_replay_same_id_same_content_is_deduplicated(self):
        store = self.store()
        store.apply_event(*make_event("41", 41))
        store.commit_cursor("41", True, 41)
        again = store.apply_event(*make_event("41", 41))
        self.assertFalse(again.adopted)
        self.assertEqual(again.reason, "duplicate")
        self.assertEqual(store.counters["events_adopted"], 1)

    def test_same_id_conflicting_content_is_quarantined(self):
        store = self.store()
        store.apply_event(*make_event("41", 41, state="alarm"))
        conflict = store.apply_event(*make_event("41", 41, state="normal"))
        self.assertFalse(conflict.adopted)
        self.assertEqual(conflict.reason, "id_conflict")
        self.assertEqual(store.devices["meter-a"]["state"], "alarm")
        status = store.status()
        self.assertEqual(len(status["quarantine"]), 1)
        self.assertEqual(status["quarantine"][0]["reason"], "id_conflict")

    def test_device_state_never_moves_backwards(self):
        store = self.store()
        store.apply_event(*make_event("a", 50, state="alarm"))
        # 较小 offset 的事件（模拟快照残留/重放）不得覆盖新状态。
        store.apply_event(*make_event("b", 40, state="normal"))
        self.assertEqual(store.devices["meter-a"]["offset"], 50)
        self.assertEqual(store.devices["meter-a"]["state"], "alarm")
        self.assertEqual(store.counters.get("device_regressions_blocked"), 1)

    def test_snapshot_does_not_override_newer_event(self):
        store = self.store()
        store.apply_event(*make_event("a", 50, state="alarm"))
        store.apply_snapshot({"meter-a": {"version": 40, "state": "normal"}}, 40)
        self.assertEqual(store.devices["meter-a"]["state"], "alarm")
        # 快照 cut 之后 <= 40 的事件被直接跳过。
        skipped = store.apply_event(*make_event("b", 40, state="normal"))
        self.assertEqual(skipped.reason, "before_cut")
        # 新设备仍可从快照建立。
        store.apply_snapshot(
            {"meter-new": {"version": 40, "state": "normal"}}, 40
        )
        self.assertEqual(store.devices["meter-new"]["state"], "normal")

    def test_crash_between_event_write_and_cursor_commit_is_safe(self):
        store = self.store()
        store.apply_event(*make_event("41", 41, state="alarm"))
        # 未 commit_cursor 即“崩溃”，重开后重放同 id 事件。
        reopened = self.store()
        self.assertIsNone(reopened.position)
        result = reopened.apply_event(*make_event("41", 41, state="alarm"))
        self.assertFalse(result.adopted)
        self.assertEqual(result.reason, "duplicate")
        reopened.commit_cursor("41", True, 41)
        self.assertEqual(reopened.devices["meter-a"]["state"], "alarm")
        # 事件文件中同 id 只有一份采纳记录。
        lines = [
            json.loads(line)
            for line in Path(self.dir, "events.jsonl").read_text().splitlines()
        ]
        self.assertEqual([line["id"] for line in lines].count("41"), 1)

    def test_blank_id_event_uses_offset_identity(self):
        store = self.store()
        first = store.apply_event("", True, json.dumps({"offset": 60, "device": "x"}))
        self.assertTrue(first.adopted)
        second = store.apply_event("", False, json.dumps({"offset": 60, "device": "x"}))
        self.assertEqual(second.reason, "duplicate_offset")
        store.commit_cursor("", True, 60)
        self.assertEqual(store.last_id, "")

    def test_bad_json_goes_to_quarantine(self):
        store = self.store()
        result = store.apply_event("9", True, "{not json")
        self.assertEqual(result.reason, "bad_json")
        self.assertTrue(Path(self.dir, "quarantine.jsonl").exists())

    def test_status_reports_gaps(self):
        store = self.store()
        for offset in (41, 42, 45):
            store.apply_event(*make_event(str(offset), offset))
        self.assertEqual(store.status()["offset_gaps"], [43, 44])

    def test_opaque_ids_are_not_parsed_as_integers(self):
        store = self.store()
        result = store.apply_event(
            "evt-不透明-①", True,
            json.dumps({"offset": 70, "device": "d", "state": "s"}),
        )
        self.assertTrue(result.adopted)
        store.commit_cursor("evt-不透明-①", True, 70)
        self.assertEqual(self.store().last_id, "evt-不透明-①")


if __name__ == "__main__":
    unittest.main()
