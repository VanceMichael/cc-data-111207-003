"""text/event-stream 的增量解析器。

按 W3C Server-Sent Events 的分派规则实现，并覆盖 docs/数据约定.md 中的
本地约定：

* 网络分块与事件边界无关，feed 接受任意长度的字节块；
* UTF-8 多字节字符允许跨块，不完整序列留在字节缓冲里等待下一块；
* 行终止支持 LF、CR、CRLF，且换行符本身可以跨块；
* 注释行（以冒号开头）不分派，心跳走这里；
* data 多行按换行连接；
* 空 id 清空重连位置；含 U+0000 的 id 不生效（保留上一个有效 id）；
* 只有收到终止空行才分派事件，连接结束时的未终止尾帧被丢弃。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

_LF = 0x0A
_CR = 0x0D


@dataclass(frozen=True)
class Event:
    """一个已终止、可分派的事件。

    event 为事件类型（默认 ``message``），data 为按换行连接后的字符串。
    id 是该事件分派时刻生效的 id 缓冲；id_changed 表示本事件是否通过
    id 字段修改过缓冲（空 id 也算修改，用于清空重连位置）。
    """

    event: str
    data: str
    id: str
    id_changed: bool


class EventStreamParser:
    """逐块消费 SSE 字节流，产出 :class:`Event`。"""

    def __init__(self, last_event_id: str = "") -> None:
        self._line_buffer = bytearray()
        # 上一块以 CR 结尾，等待本块首字节确认是否构成 CRLF。
        self._carriage_pending = False
        self._last_event_id = last_event_id
        self._reset_event()

    @property
    def last_event_id(self) -> str:
        return self._last_event_id

    def _reset_event(self) -> None:
        self._event_type = "message"
        self._data: list[str] = []
        self._id_changed = False

    def feed(self, chunk: bytes) -> list[Event]:
        """喂入一个字节块，返回块内已完整终止的事件列表。"""
        if self._carriage_pending:
            # 挂起 CR 与本块首字节的 LF 配对，CRLF 只算一个终止符。
            if chunk[:1] == b"\n":
                chunk = chunk[1:]
            self._carriage_pending = False
        self._line_buffer.extend(chunk)
        events: list[Event] = []
        for line in self._split_lines():
            event = self._handle_line(line)
            if event is not None:
                events.append(event)
        return events

    def _split_lines(self) -> list[str]:
        """把缓冲按 LF/CR/CRLF 切成完整行，残段保留。

        每行以行终止符界定；末尾无终止符的残段（含跨块 UTF-8 字符与
        块尾 CR）留在缓冲，等下一块继续。
        """
        buffer = self._line_buffer
        lines: list[bytes] = []
        start = 0
        index = 0
        while index < len(buffer):
            byte = buffer[index]
            if byte not in (_LF, _CR):
                index += 1
                continue
            if byte == _CR and index == len(buffer) - 1:
                # CR 落在块尾：是否与下一块 LF 配对尚不确定，
                # 本块只产出它之前已完整的行，CR 留在残段挂起。
                lines.append(bytes(buffer[start:index]))
                start = index
                break
            # 命中确定的终止符，切出 [start, index)，再推进游标。
            lines.append(bytes(buffer[start:index]))
            if byte == _CR and buffer[index + 1] == _LF:
                index += 2  # CRLF 整体作为一个终止符
            else:
                index += 1  # LF 或独立 CR
            start = index
        del buffer[:start]
        # 残段若以 CR 结尾则挂起；注意此时该 CR 尚未被当作终止符消费。
        self._carriage_pending = buffer.endswith(b"\r")
        decoded: list[str] = []
        for line in lines:
            if line.endswith(b"\r"):
                line = line[:-1]
            decoded.append(line.decode("utf-8", errors="strict"))
        return decoded

    def _handle_line(self, line: str) -> Optional[Event]:
        if line == "":
            # 空行终止并分派当前事件；没有任何字段的空事件不分派。
            if self._data:
                event = Event(
                    event=self._event_type,
                    data="\n".join(self._data),
                    id=self._last_event_id,
                    id_changed=self._id_changed,
                )
                self._reset_event()
                return event
            self._reset_event()
            return None
        if line.startswith(":"):
            # 注释 / 心跳，永不分派。
            return None
        if ":" in line:
            field, _, value = line.partition(":")
            if value.startswith(" "):
                value = value[1:]
        else:
            field, value = line, ""
        if field == "event":
            self._event_type = value
        elif field == "data":
            self._data.append(value)
        elif field == "id":
            # 含 U+0000 的 id 整条忽略，id 缓冲保持不变。
            if "\x00" not in value:
                self._last_event_id = value
                self._id_changed = True
        # 其余字段（retry 及未知字段）忽略。
        return None

    def finish(self) -> None:
        """连接结束时调用：未终止的尾帧按约定丢弃。"""
        self._line_buffer.clear()
        self._carriage_pending = False
        self._reset_event()
