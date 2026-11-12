"""端到端演示：断线接续、去重、冲突隔离与快照切换。

用本地合成来源演示真实采集语义，而不是只打印“连接成功”。所有持久化
数据都写进调用方指定的临时目录，不污染样例文件。

三个场景：
1. 中途故障：连接在事件中间断开，重连后同一告警只采纳一次，设备最终
   状态与事件采纳次数可核对；
2. 冲突隔离：厂商用相同 id 重放不同内容，旧状态保留、冲突进隔离区；
3. 409 快照：过期 Last-Event-ID 收到 409，按快照 cut 切换，cut 前
   事件被跳过、cut 后事件正常推进，状态不倒退。
"""

from __future__ import annotations

import argparse
import json
import tempfile
from pathlib import Path

from collector import RetryPolicy, run
from lab import StreamSource, serving_stream, serving
from source import Source
from store import Store


def frame(event_id, payload, event_type=None):
    lines = []
    if event_id is not None:
        lines.append(f"id: {event_id}")
    if event_type:
        lines.append(f"event: {event_type}")
    lines.append(f"data: {json.dumps(payload, ensure_ascii=False)}")
    return ("\n".join(lines) + "\n\n").encode("utf-8")


def heading(title):
    print("\n" + "=" * 68)
    print(title)
    print("=" * 68)


def show_store(store):
    status = store.status()
    print(f"最后确认位置 position={status['confirmed_position']} "
          f"last_event_id={status['last_event_id']!r} cut={status['snapshot_cut']}")
    print("设备状态：")
    for device, state in sorted(status["devices"].items()):
        print(f"  {device}: {state}")
    print(f"采纳 offset：{status['adopted_offsets']}")
    print(f"offset 缺口：{status['offset_gaps']}")
    counts = status["counters"]
    print("计数：" + ", ".join(f"{k}={v}" for k, v in counts.items()))
    if status["quarantine"]:
        print("隔离事件：")
        for record in status["quarantine"]:
            print(f"  [{record['reason']}] id={record['id']!r} "
                  f"offset={record['offset']} payload={record['payload']}")


def scenario_fault(state_dir):
    heading("场景 1：中途断线后重连，同一告警只采纳一次")
    store = Store(state_dir)
    f1 = frame("41", {"device": "meter-a", "offset": 41, "version": 41, "state": "alarm"})
    f2 = frame("42", {"device": "meter-a", "offset": 42, "version": 42, "state": "normal"})
    f3 = frame("43", {"device": "meter-b", "offset": 43, "version": 1, "state": "在线"})
    full = f1 + f2 + f3
    cut_point = len(f1) + 7  # 落在 f2 中间，f1 已完整终止，f2 尚未终止

    policy = RetryPolicy(base_delay=0, max_delay=0, timeout=2, read_chunk_size=5)
    first = StreamSource([full[:cut_point]])
    second = StreamSource([full[i:i + 5] for i in range(0, len(full), 5)])

    with serving_stream(first) as base:
        report1 = run(base + "/events", store, policy)
    print(f"第一次连接（故障断开）：停止原因={report1.stop_reason}，"
          f"采纳={report1.events_adopted}，确认 id={store.last_id!r}")
    with serving_stream(second) as base:
        report2 = run(base + "/events", store, policy)
    print(f"第二次连接（重放接续）：停止原因={report2.stop_reason}，"
          f"采纳={report2.events_adopted}，去重={report2.duplicates}")
    show_store(store)
    adopted = store.status()["counters"].get("events_adopted", 0)
    assert adopted == 3, f"应只采纳 3 个事件，实际 {adopted}"
    assert store.devices["meter-a"]["state"] == "normal"
    print(">>> 结论：41 号告警虽被重放，采纳计数仍为 3，没有重复入库。")


def scenario_conflict(state_dir):
    heading("场景 2：相同 id 冲突内容被隔离，不静默覆盖")
    store = Store(state_dir)
    chunks = [
        frame("61", {"device": "meter-c", "offset": 61, "version": 1, "state": "alarm"}),
        frame("61", {"device": "meter-c", "offset": 61, "version": 9, "state": "tampered"}),
        frame("", {"device": "meter-c", "offset": 62, "version": 2, "state": "normal"}),
        frame("63\x00x", {"device": "meter-c", "offset": 63, "version": 3, "state": "ok"}),
    ]
    policy = RetryPolicy(base_delay=0, max_delay=0, timeout=2)
    with serving_stream(StreamSource(chunks)) as base:
        report = run(base + "/events", store, policy)
    print(f"采纳={report.events_adopted}，去重={report.duplicates}，隔离={report.quarantined}")
    show_store(store)
    assert store.devices["meter-c"]["state"] == "ok"
    assert any(q["reason"] == "id_conflict" for q in store.status()["quarantine"])
    print(">>> 结论：冲突的 61 号事件进隔离区，原始 alarm 状态未被篡改内容覆盖。")


def scenario_snapshot(state_dir):
    heading("场景 3：过期 Last-Event-ID 触发 409 快照切换")
    store = Store(state_dir)
    store.last_id = "5"
    store.commit_cursor("5", True, 5)
    fixture = {
        "cut": 40,
        "snapshot": {"meter-a": {"version": 40, "state": "normal"}},
        "chunks": [
            frame("39", {"device": "meter-a", "offset": 39, "version": 39, "state": "old"}).decode(),
            frame("41", {"device": "meter-a", "offset": 41, "version": 41, "state": "alarm"}).decode(),
            frame("42", {"device": "meter-a", "offset": 42, "version": 42, "state": "normal"}).decode(),
        ],
    }
    policy = RetryPolicy(base_delay=0, max_delay=0, timeout=2)
    with serving(Source(fixture)) as base:
        report = run(base + "/events", store, policy)
    print(f"应用快照={report.snapshots_applied}，采纳={report.events_adopted}")
    show_store(store)
    assert store.cut == 40
    assert store.devices["meter-a"]["state"] == "normal"
    assert store.devices["meter-a"]["offset"] == 42
    assert store.status()["counters"].get("events_before_cut_skipped") == 1
    print(">>> 结论：cut=40 前的 39 号事件被跳过，41/42 正常推进，状态未倒退。")


def main():
    parser = argparse.ArgumentParser(description="采集器端到端演示")
    parser.add_argument("--state-dir", type=Path, default=None,
                        help="持久化目录；默认使用三个独立临时目录")
    arguments = parser.parse_args()
    if arguments.state_dir:
        root = arguments.state_dir
        root.mkdir(parents=True, exist_ok=True)
        dirs = [str(root / name) for name in ("fault", "conflict", "snapshot")]
    else:
        dirs = [tempfile.mkdtemp(prefix="collector-demo-") for _ in range(3)]
    scenario_fault(dirs[0])
    scenario_conflict(dirs[1])
    scenario_snapshot(dirs[2])
    heading("演示完成")
    print("持久化目录：")
    for name, path in zip(("故障接续", "冲突隔离", "快照切换"), dirs):
        print(f"  {name}: {path}")


if __name__ == "__main__":
    main()
