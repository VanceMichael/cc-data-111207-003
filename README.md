# 设备事件流的断线接续与缺口恢复

本工具把节能厂商的 `text/event-stream` 事件流落为可核对的设备状态与事件记录，
替代每分钟轮询。样例中的机构、设备、人员和标识均为虚构，来源预览入口保持不变。

## 目录

- `source.py`：可重复的本地来源（409、快照、固定事件片段），不做改动；
- `reader.py` / `app.py`：保留的单次读取预览入口，一次响应读到底；
- `sse.py`：SSE 增量解析器（字节级行边界、跨块 UTF-8、注释/多行/空 id/NUL id）；
- `store.py`：持久化存储（原子写、去重、冲突隔离、设备状态防倒退、缺口观察）；
- `collector.py`：受控重连的采集主循环（409 快照切换、退避与终止条件）；
- `collect.py`：命令行入口，`run` 采集、`status` 只读观察；
- `lab.py`：本地 HTTP 工具，新增分块流式服务用于故障注入演示；
- `demo.py`：断线接续、冲突隔离、409 快照三个端到端场景；
- `fixtures/source.json`：可替换的同结构样例；
- `docs/数据约定.md`：协议与数据含义。

## 关键语义

- 网络分块不等于事件边界：只有收到事件终止空行且业务写入成功，才推进持久化
  重连位置；未终止尾帧不分派，连接结束即丢弃。
- 注释心跳不分派；`data` 多行按换行连接；空 id 清空重连位置；含 U+0000 的 id
  不生效；UTF-8 多字节字符可跨块。
- 事件 id 是不透明的重连/去重键，不能转整数、不能替代 offset。同一 id 重放
  相同内容只去重；相同 id 的冲突内容进 `quarantine.jsonl`，绝不静默覆盖。
- 业务事件 `offset` 与快照 `cut` 属同一递增位置空间。409 后先应用快照并记录
  cut，cut 及之前的事件跳过；设备状态只接受更大 offset，快照无法倒退事件已
  覆盖的新状态。设备 `version` 仅在本设备内有序。
- “业务数据已写、重连位置未提交”之间退出后，重启重放会按 id/offset 去重，
  事件不会被采纳第二次。
- 重连受 `--max-attempts`、`--max-idle-attempts` 和退避封顶约束，耗尽即报错
  退出，不靠无休止重连掩盖错误。

## 持久化文件（默认 `state/`，已在 .gitignore 忽略）

| 文件 | 内容 |
| --- | --- |
| `events.jsonl` | 已采纳业务事件，每行一条，追加并 fsync |
| `devices.json` | 各设备最新状态，按全局 offset 防倒退 |
| `cursor.json` | 最后确认位置：`last_id`、`position`、`cut` |
| `quarantine.jsonl` | 同 id 冲突、坏 JSON、缺 offset 等隔离记录 |
| `ledger.json` | 已见 id/offset 与各项计数，供去重与观察 |

JSON 文件均以“临时文件 + `os.replace` + 目录 fsync”原子替换。

## 运行

使用 Python 3.11 及标准库，无第三方依赖。

```bash
# 原有单次读取预览（保留不变）
python3 app.py

# 端到端演示：故障接续、冲突隔离、409 快照，打印实际采纳次数
python3 demo.py

# 采集（地址替换为真实事件流；本地演示可用 lab.serving_stream 起服务）
python3 collect.py run --url http://127.0.0.1:8000/events --state-dir state

# 只读观察最后确认位置、offset 缺口与隔离事件
python3 collect.py status --state-dir state

# 测试
python3 -m unittest discover -s tests -v
PYTHONPYCACHEPREFIX=/tmp/collector-pycache python3 -m compileall -q .
```

`run` 支持 `--max-attempts`、`--max-idle-attempts`、`--base-delay`、
`--max-delay`、`--timeout` 控制断线等待与终止条件。

## 容器

基础镜像固定摘要，不在启动阶段安装软件或获取远程数据。

```bash
docker build --pull=false -t event-stream:local .
docker run --rm --network none event-stream:local
docker run --rm --network none event-stream:local python3 -m unittest discover -s tests -v
```

## 本机演示

在全新临时目录执行 `python3 demo.py` 可观察三个场景的事件采纳次数与最终设备
状态，随后运行测试验证解析与存储边界。这些步骤只用本地小尺寸材料，数秒结束，
不需要账户、设备或外部站点。持久化数据应放在 `state/` 或调用方明确传入的
目录，不写入样例文件。
