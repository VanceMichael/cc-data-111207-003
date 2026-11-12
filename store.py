"""采集状态的持久化存储。

所有写入都在单个目录内完成，JSON 文件通过“临时文件 + os.replace”原子
替换，fsync 后再推进下一个文件，保证在“保存业务数据”和“更新重连位置”
之间退出时，重放事件不会被重复采纳：

1. ``events.jsonl``  已采纳的业务事件（每行一条，追加后 fsync）；
2. ``devices.json``  各设备最新状态，仅接受更大的全局 offset，禁止倒退；
3. ``cursor.json``   最后确认位置（重连 id + 全局 position）；
4. ``quarantine.jsonl`` 同 id 冲突、坏数据等被隔离的记录；
5. ``ledger.json``   观察用计数与 id/offset 采纳痕迹。

事件 id 是不透明去重键，不能转整数；全局 offset 才是位置空间的序。
"""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass
class QuarantineRecord:
    reason: str
    event_id: str
    offset: Optional[int]
    payload: Any
    detail: str = ""


@dataclass
class ApplyResult:
    """一次事件写入的结果，供采集层决定是否推进重连位置。"""

    adopted: bool
    offset: Optional[int]
    event_id: str
    reason: str = ""


@dataclass
class Store:
    directory: str
    devices: dict[str, dict[str, Any]] = field(default_factory=dict)
    # 已采纳事件的 id 集合（含空 id 事件按 offset 去重的合成键）。
    _seen_ids: set[str] = field(default_factory=set)
    _seen_offsets: set[int] = field(default_factory=set)
    last_id: str = ""
    position: Optional[int] = None
    # 快照 cut 标记：应用过快照后，<= cut 的业务事件一律跳过。
    cut: Optional[int] = None
    counters: dict[str, int] = field(default_factory=dict)

    def __post_init__(self) -> None:
        os.makedirs(self.directory, exist_ok=True)
        self._events_path = os.path.join(self.directory, "events.jsonl")
        self._devices_path = os.path.join(self.directory, "devices.json")
        self._cursor_path = os.path.join(self.directory, "cursor.json")
        self._quarantine_path = os.path.join(self.directory, "quarantine.jsonl")
        self._ledger_path = os.path.join(self.directory, "ledger.json")
        self._load()

    # ------------------------------------------------------------------ 加载
    def _load(self) -> None:
        cursor = self._read_json(self._cursor_path)
        if cursor:
            self.last_id = cursor.get("last_id", "")
            self.position = cursor.get("position")
            self.cut = cursor.get("cut")
        devices = self._read_json(self._devices_path)
        if devices:
            self.devices = devices.get("devices", {})
        ledger = self._read_json(self._ledger_path)
        if ledger:
            self.counters = ledger.get("counters", {})
            self._seen_ids = set(ledger.get("seen_ids", []))
            self._seen_offsets = set(ledger.get("seen_offsets", []))

    @staticmethod
    def _read_json(path: str) -> dict[str, Any]:
        try:
            with open(path, "r", encoding="utf-8") as handle:
                return json.load(handle)
        except FileNotFoundError:
            return {}

    # -------------------------------------------------------------- 原子写盘
    def _atomic_write_json(self, path: str, payload: Any) -> None:
        directory = os.path.dirname(path)
        handle, tmp_name = tempfile.mkstemp(prefix=".tmp-", dir=directory)
        try:
            with os.fdopen(handle, "w", encoding="utf-8") as stream:
                json.dump(payload, stream, ensure_ascii=False)
                stream.write("\n")
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(tmp_name, path)
            self._fsync_dir(directory)
        except BaseException:
            try:
                os.unlink(tmp_name)
            except FileNotFoundError:
                pass
            raise

    def _append_jsonl(self, path: str, record: Any) -> None:
        with open(path, "a", encoding="utf-8") as stream:
            stream.write(json.dumps(record, ensure_ascii=False))
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())

    @staticmethod
    def _fsync_dir(directory: str) -> None:
        try:
            descriptor = os.open(directory, os.O_RDONLY)
        except OSError:
            return
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    # -------------------------------------------------------------- 事件写入
    def apply_event(
        self,
        event_id: str,
        id_changed: bool,
        raw_data: str,
        event_type: str = "message",
    ) -> ApplyResult:
        """处理一个已终止的 SSE 事件并落盘。

        只有本方法完整成功（事件、设备、账本均落盘）后，采集层才允许
        调用 :meth:`commit_cursor` 推进持久化重连位置。
        """
        self._bump("events_received")
        try:
            payload = json.loads(raw_data)
        except (json.JSONDecodeError, ValueError) as error:
            self._quarantine(
                QuarantineRecord(
                    reason="bad_json",
                    event_id=event_id,
                    offset=None,
                    payload=raw_data,
                    detail=str(error),
                )
            )
            return ApplyResult(False, None, event_id, "bad_json")
        if not isinstance(payload, dict):
            self._quarantine(
                QuarantineRecord(
                    reason="bad_payload",
                    event_id=event_id,
                    offset=None,
                    payload=payload,
                    detail="payload 不是 JSON 对象",
                )
            )
            return ApplyResult(False, None, event_id, "bad_payload")

        offset = payload.get("offset")
        offset_int = offset if isinstance(offset, int) and not isinstance(offset, bool) else None

        # 快照 cut 之前的事件已被快照覆盖，跳过但视为已处理。
        if self.cut is not None and offset_int is not None and offset_int <= self.cut:
            self._bump("events_before_cut_skipped")
            return ApplyResult(False, offset_int, event_id, "before_cut")

        # 同 id 重放：内容一致只去重；内容冲突进隔离区，绝不静默覆盖。
        if event_id and event_id in self._seen_ids:
            if self._payload_matches(event_id, payload):
                self._bump("duplicates_suppressed")
                return ApplyResult(False, offset_int, event_id, "duplicate")
            self._quarantine(
                QuarantineRecord(
                    reason="id_conflict",
                    event_id=event_id,
                    offset=offset_int,
                    payload=payload,
                    detail="同一事件 id 出现不同内容",
                )
            )
            return ApplyResult(False, offset_int, event_id, "id_conflict")

        # 空 id 事件没有重连身份，按全局 offset 去重。
        if not event_id and offset_int is not None and offset_int in self._seen_offsets:
            self._bump("duplicates_suppressed")
            return ApplyResult(False, offset_int, event_id, "duplicate_offset")

        if offset_int is None:
            self._quarantine(
                QuarantineRecord(
                    reason="missing_offset",
                    event_id=event_id,
                    offset=None,
                    payload=payload,
                    detail="业务事件缺少全局 offset",
                )
            )
            return ApplyResult(False, None, event_id, "missing_offset")

        record = {
            "id": event_id,
            "event": event_type,
            "offset": offset_int,
            "payload": payload,
        }
        self._append_jsonl(self._events_path, record)
        self._adopt_device(payload, offset_int)
        if event_id:
            self._seen_ids.add(event_id)
        self._seen_offsets.add(offset_int)
        self._bump("events_adopted")
        self._write_ledger()
        return ApplyResult(True, offset_int, event_id, "adopted")

    def _adopt_device(self, payload: dict[str, Any], offset: int) -> None:
        device = payload.get("device")
        if not isinstance(device, str):
            return
        current = self.devices.get(device)
        # 全局 offset 是同一递增位置空间：只接受更新的状态，禁止倒退。
        if current is not None and current.get("offset", -1) >= offset:
            self._bump("device_regressions_blocked")
            return
        self.devices[device] = {
            "offset": offset,
            "version": payload.get("version"),
            "state": payload.get("state"),
        }
        self._atomic_write_json(
            self._devices_path, {"devices": self.devices}
        )

    def _payload_matches(self, event_id: str, payload: dict[str, Any]) -> bool:
        """与 events.jsonl 中同 id 的已采纳内容比对。"""
        if not os.path.exists(self._events_path):
            return False
        with open(self._events_path, "r", encoding="utf-8") as stream:
            for line in stream:
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if record.get("id") == event_id:
                    return record.get("payload") == payload
        return False

    def _quarantine(self, record: QuarantineRecord) -> None:
        self._bump("quarantined")
        self._append_jsonl(
            self._quarantine_path,
            {
                "reason": record.reason,
                "id": record.event_id,
                "offset": record.offset,
                "payload": record.payload,
                "detail": record.detail,
            },
        )

    def _bump(self, name: str) -> None:
        self.counters[name] = self.counters.get(name, 0) + 1

    def _write_ledger(self) -> None:
        self._atomic_write_json(
            self._ledger_path,
            {
                "counters": self.counters,
                "seen_ids": sorted(self._seen_ids),
                "seen_offsets": sorted(self._seen_offsets),
            },
        )

    # ----------------------------------------------------------- 重连位置提交
    def commit_cursor(self, event_id: str, id_changed: bool, offset: Optional[int]) -> None:
        """业务写入成功后推进持久化重连位置。

        空 id（且本事件确实带过 id 字段）清空重连 id；含 NUL 的 id
        已在解析层拦截，不会走到这里。position 只随全局 offset 前进。
        """
        if id_changed:
            self.last_id = event_id
        if offset is not None and (self.position is None or offset > self.position):
            self.position = offset
        self._atomic_write_json(
            self._cursor_path,
            {
                "last_id": self.last_id,
                "position": self.position,
                "cut": self.cut,
            },
        )

    def apply_snapshot(self, snapshot_payload: dict[str, Any], cut: int) -> None:
        """应用 409 后取回的快照。

        cut 与业务 offset 属同一递增位置空间：先记 cut 拦住旧事件，
        再逐设备按 offset 比较，绝不允许快照把事件已经覆盖过的新状态
        倒退回去。
        """
        if self.cut is None or cut > self.cut:
            self.cut = cut
        changed = False
        for device, state in snapshot_payload.items():
            if not isinstance(state, dict):
                continue
            candidate = {
                "offset": cut,
                "version": state.get("version"),
                "state": state.get("state"),
            }
            current = self.devices.get(device)
            if current is None or current.get("offset", -1) < cut:
                self.devices[device] = candidate
                changed = True
        if changed:
            self._atomic_write_json(self._devices_path, {"devices": self.devices})
        if self.position is None or self.position < cut:
            self.position = cut
        # 旧 Last-Event-ID 已被厂商判定失效（409），快照 cut 接管位置，
        # 清空重连 id；后续流内事件再以新 id 重建接续点。
        self.last_id = ""
        self._atomic_write_json(
            self._cursor_path,
            {"last_id": self.last_id, "position": self.position, "cut": self.cut},
        )
        self._bump("snapshots_applied")
        self._write_ledger()

    # ------------------------------------------------------------- 观察入口
    def status(self) -> dict[str, Any]:
        """返回可核对的采集状态：确认位置、缺口、隔离事件与计数。"""
        adopted_offsets = sorted(self._seen_offsets)
        gaps = []
        if adopted_offsets:
            expected = adopted_offsets[0]
            for value in adopted_offsets[1:]:
                expected += 1
                while expected < value:
                    gaps.append(expected)
                    expected += 1
        quarantine = []
        if os.path.exists(self._quarantine_path):
            with open(self._quarantine_path, "r", encoding="utf-8") as stream:
                for line in stream:
                    try:
                        quarantine.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
        return {
            "last_event_id": self.last_id,
            "confirmed_position": self.position,
            "snapshot_cut": self.cut,
            "offset_gaps": gaps,
            "adopted_offsets": adopted_offsets,
            "devices": self.devices,
            "quarantine": quarantine,
            "counters": dict(sorted(self.counters.items())),
        }
