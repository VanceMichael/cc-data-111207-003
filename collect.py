"""采集入口命令行。

子命令：
  run     连接事件流并持续采集，重连策略可通过参数约束；
  status  只读观察最后确认位置、offset 缺口、隔离事件和设备状态。

单次读取预览仍由 app.py / reader.py 保留，本模块只做持久化采集。
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from collector import CollectorError, RetryPolicy, run
from store import Store


def _build_policy(arguments: argparse.Namespace) -> RetryPolicy:
    return RetryPolicy(
        max_attempts=arguments.max_attempts,
        max_idle_attempts=arguments.max_idle_attempts,
        base_delay=arguments.base_delay,
        max_delay=arguments.max_delay,
        timeout=arguments.timeout,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="设备事件流持久化采集器")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="连接并采集事件流")
    run_parser.add_argument("--url", required=True, help="text/event-stream 地址")
    run_parser.add_argument(
        "--state-dir",
        type=Path,
        default=Path("state"),
        help="持久化目录（默认 state/，已在 .gitignore 中忽略）",
    )
    run_parser.add_argument("--max-attempts", type=int, default=10)
    run_parser.add_argument("--max-idle-attempts", type=int, default=5)
    run_parser.add_argument("--base-delay", type=float, default=0.2)
    run_parser.add_argument("--max-delay", type=float, default=5.0)
    run_parser.add_argument("--timeout", type=float, default=10.0)

    status_parser = subparsers.add_parser("status", help="观察本地采集状态")
    status_parser.add_argument("--state-dir", type=Path, default=Path("state"))

    arguments = parser.parse_args(argv)

    if arguments.command == "status":
        snapshot = Store(str(arguments.state_dir)).status()
        print(json.dumps(snapshot, ensure_ascii=False, indent=2))
        return 0

    store = Store(str(arguments.state_dir))
    try:
        report = run(arguments.url, store, _build_policy(arguments))
    except CollectorError as error:
        print(json.dumps({"终止": str(error), **store.status()},
                         ensure_ascii=False, indent=2))
        return 2
    print(json.dumps(
        {
            "运行报告": {
                "连接尝试": report.attempts,
                "采纳事件": report.events_adopted,
                "去重事件": report.duplicates,
                "隔离事件": report.quarantined,
                "应用快照": report.snapshots_applied,
                "停止原因": report.stop_reason,
            },
            "确认状态": store.status(),
        },
        ensure_ascii=False,
        indent=2,
    ))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
