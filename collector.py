"""可持续运行的事件流采集入口。

职责衔接 :mod:`sse` 的解析与 :mod:`store` 的持久化：

* 每次连接携带持久化的 Last-Event-ID；
* 只有事件终止空行已收到 *且* 业务写入成功，才推进重连位置；
* 409（旧 id 超出历史窗口）时按响应里的地址取快照，先记 cut 再合并，
  cut 与 offset 同属一个递增位置空间，杜绝状态倒退；
* 干净的连接结束（EOF）视为来源耗尽直接停止；读取中断等故障才按
  受控策略退避重连，尝试次数与空转次数耗尽即报错，不无限重连；
* 所有不可恢复异常都转换为 :class:`CollectorError`，交由调用方处理。
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from typing import Callable, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin
from urllib.request import Request, urlopen

from sse import EventStreamParser
from store import Store


class CollectorError(RuntimeError):
    """采集策略放弃继续重连时抛出。"""


@dataclass
class RetryPolicy:
    # 一次完整采集生命周期内的连接尝试上限（含首次）。
    max_attempts: int = 10
    # 连续没有分派任何事件的“故障”连接尝试上限，防止来源静默空转。
    max_idle_attempts: int = 5
    # 退避起点与封顶（秒），实际等待 base * 2 ** (n-1)，无随机抖动以便演示。
    base_delay: float = 0.2
    max_delay: float = 5.0
    read_chunk_size: int = 4096
    # 单次读取的 socket 超时；空闲心跳连接也会在此时长内有字节到达。
    timeout: float = 10.0

    def delay_for(self, attempt: int) -> float:
        return min(self.base_delay * (2 ** max(0, attempt - 1)), self.max_delay)


@dataclass
class RunReport:
    attempts: int = 0
    events_adopted: int = 0
    duplicates: int = 0
    quarantined: int = 0
    snapshots_applied: int = 0
    stop_reason: str = ""
    delays: list[float] = field(default_factory=list)


def _open_stream(url: str, last_event_id: str, timeout: float):
    headers = {"Accept": "text/event-stream"}
    if last_event_id:
        headers["Last-Event-ID"] = last_event_id
    request = Request(url, headers=headers)
    return urlopen(request, timeout=timeout)


def _fetch_snapshot(base_url: str, location: str, timeout: float) -> dict:
    snapshot_url = urljoin(base_url, location)
    with urlopen(Request(snapshot_url), timeout=timeout) as response:
        if response.code != 200:
            raise CollectorError(f"快照地址返回状态 {response.code}")
        return json.loads(response.read().decode("utf-8"))


def run(
    stream_url: str,
    store: Store,
    policy: Optional[RetryPolicy] = None,
    sleeper: Callable[[float], None] = time.sleep,
) -> RunReport:
    """按策略运行采集，直到来源耗尽或策略终止，返回运行报告。

    对端在完整应答后正常关闭连接即“来源耗尽”（演示来源的自然终点）；
    读取中断、连接失败按退避策略重连，尝试次数或空转次数耗尽则抛
    :class:`CollectorError`，避免无休止重连掩盖错误。
    """
    policy = policy or RetryPolicy()
    report = RunReport()
    idle_failures = 0
    # 本轮通过 409 应用过快照：随后的干净空流视为恢复完成而非故障。
    snapshot_pending = False

    while report.attempts < policy.max_attempts:
        report.attempts += 1
        dispatched = 0
        interrupted = False
        try:
            response = _open_stream(stream_url, store.last_id, policy.timeout)
        except HTTPError as error:
            if error.code == 409:
                _handle_conflict(stream_url, error, store, policy, report)
                snapshot_pending = True
                continue
            if not _backoff(policy, report, sleeper):
                raise CollectorError(
                    f"来源返回 HTTP {error.code}，重连策略已耗尽"
                ) from error
            continue
        except (URLError, OSError, TimeoutError) as error:
            if not _backoff(policy, report, sleeper):
                raise CollectorError(f"连接失败且重连策略已耗尽: {error}") from error
            continue

        try:
            with response:
                parser = EventStreamParser(store.last_id)
                while True:
                    try:
                        chunk = response.read(policy.read_chunk_size)
                    except (URLError, OSError, TimeoutError) as error:
                        # 读取中断：未终止尾帧由 parser.finish 丢弃；
                        # 已确认位置此前已逐事件持久化，重连即可接续。
                        parser.finish()
                        interrupted = True
                        report.stop_reason = f"read-interrupted:{error}"
                        break
                    if not chunk:
                        parser.finish()
                        report.stop_reason = "drained"
                        break
                    for event in parser.feed(chunk):
                        dispatched += 1
                        _consume_event(event, store, report)
        except HTTPError as error:
            if error.code == 409:
                _handle_conflict(stream_url, error, store, policy, report)
                snapshot_pending = True
                continue
            raise CollectorError(f"流中返回 HTTP {error.code}") from error

        # 有分派后的干净 EOF：来源已耗尽，正常结束。
        if not interrupted and dispatched:
            return report

        # 409 快照恢复后的干净空流：没有更多历史可读，恢复完成。
        if not interrupted and snapshot_pending:
            report.stop_reason = "snapshot-caught-up"
            return report
        snapshot_pending = False

        # 零分派连接（故障中断或对端提前关闭）：按故障重连，受空转
        # 上限约束，防止来源静默空转或无休止重连掩盖错误。
        idle_failures += 1
        if idle_failures >= policy.max_idle_attempts:
            raise CollectorError(
                f"连续 {idle_failures} 次连接未分派任何事件，停止以免空转"
            )
        if not _backoff(policy, report, sleeper):
            raise CollectorError("连接尝试次数已达上限")

    raise CollectorError("连接尝试次数已达上限")


def _backoff(
    policy: RetryPolicy,
    report: RunReport,
    sleeper: Callable[[float], None],
) -> bool:
    """退避等待；若尝试次数已耗尽返回 False。"""
    if report.attempts >= policy.max_attempts:
        return False
    delay = policy.delay_for(report.attempts)
    report.delays.append(delay)
    sleeper(delay)
    return True


def _handle_conflict(
    stream_url: str,
    error: HTTPError,
    store: Store,
    policy: RetryPolicy,
    report: RunReport,
) -> None:
    try:
        body = json.loads(error.read().decode("utf-8"))
        location = body["snapshot"]
    except (json.JSONDecodeError, KeyError, UnicodeDecodeError) as parse_error:
        raise CollectorError("409 响应缺少可用的 snapshot 地址") from parse_error
    snapshot = _fetch_snapshot(stream_url, location, policy.timeout)
    cut = snapshot.get("cut")
    devices = snapshot.get("devices")
    if not isinstance(cut, int) or not isinstance(devices, dict):
        raise CollectorError("快照内容缺少 cut 或 devices")
    store.apply_snapshot(devices, cut)
    report.snapshots_applied += 1


def _consume_event(event, store: Store, report: RunReport) -> None:
    result = store.apply_event(
        event_id=event.id,
        id_changed=event.id_changed,
        raw_data=event.data,
        event_type=event.event,
    )
    # 关键顺序：业务写入成功后才提交重连位置；
    # 去重/隔离/跳过类结果同样表示该终止帧已处理完，可以推进。
    store.commit_cursor(event.id, event.id_changed, result.offset)
    if result.adopted:
        report.events_adopted += 1
    elif result.reason in {"duplicate", "duplicate_offset"}:
        report.duplicates += 1
    elif result.reason in {"id_conflict", "bad_json", "bad_payload", "missing_offset"}:
        report.quarantined += 1
