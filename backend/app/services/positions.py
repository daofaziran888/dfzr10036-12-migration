"""保存位点领域模型：带来源与格式版本的位点数据，以及 MySQL/PG 两套解释。

旧版检查点只保存一段无类型文本（如 ``binlog.001234:891200``、
``pg_lsn.0/3E82A00``）。这里把可识别的文本升级为结构化的保存位点：

- ``source``            位点来源（mysql / postgresql）
- ``source_format``     位点文本格式（mysql-binlog-file-offset / postgresql-lsn）
- ``format_version``    保存位点数据本身的格式版本
- ``payload``           解析后的结构化位点（MySQL 是文件序号+偏移，PG 是 LSN）
- ``raw``               原始文本，留档和旧写入端契约继续使用

MySQL 的文件偏移与 PostgreSQL 的 LSN 是两套完全不同的坐标系，比较、取下一个
位置、区间运算都按来源分开做，禁止跨来源比较。
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

SAVED_POSITION_FORMAT = "saved-position"
SAVED_POSITION_FORMAT_VERSION = 1

MYSQL = "mysql"
POSTGRESQL = "postgresql"

SOURCE_FORMATS: dict[str, dict[str, Any]] = {
    MYSQL: {
        "source": MYSQL,
        "source_format": "mysql-binlog-file-offset",
        "format_version": 1,
        "unit": "file-offset",
        "description": "MySQL binlog 文件名与事件偏移；偏移按字节推进。",
    },
    POSTGRESQL: {
        "source": POSTGRESQL,
        "source_format": "postgresql-lsn",
        "format_version": 1,
        "unit": "lsn",
        "description": "PostgreSQL WAL LSN（hi/lo 两段十六进制）；LSN 按字节推进。",
    },
}


class PositionError(ValueError):
    """位点无法按声明来源解释，或跨来源参与了比较/区间运算。"""

    def __init__(
        self,
        message: str,
        *,
        raw: str | None = None,
        source: str | None = None,
        reason: str = "invalid-position",
    ) -> None:
        super().__init__(message)
        self.raw = raw
        self.source = source
        self.reason = reason


# MySQL binlog 文件形如 binlog.001234 / mysql-bin.000001，冒号后是字节偏移。
_MYSQL_RE = re.compile(
    r"^(?P<file>[A-Za-z][A-Za-z0-9_.-]*\.(?P<seq>\d{1,12})):(?P<offset>\d{1,20})$"
)
# PostgreSQL 逻辑复制位点显式带 pg_lsn 前缀，斜杠前后各是一段十六进制。
_PG_RE = re.compile(
    r"^pg_lsn\.(?P<hi>[0-9A-Fa-f]{1,8})/(?P<lo>[0-9A-Fa-f]{1,8})$"
)


def detect_source(raw: str) -> str | None:
    """只按文本前缀判断来源，不代表位点可解析。"""
    text = (raw or "").strip()
    if text.startswith("pg_lsn."):
        return POSTGRESQL
    if re.match(r"^[A-Za-z][A-Za-z0-9_.-]*\.\d+:\d+$", text):
        return MYSQL
    return None


def _parse_mysql(raw: str) -> dict[str, Any]:
    match = _MYSQL_RE.match(raw)
    if match is None:
        raise PositionError(
            f"MySQL 位点应为 文件.序号:偏移，实际为 {raw!r}",
            raw=raw, source=MYSQL, reason="unparsable-mysql-binlog",
        )
    seq_text = match.group("seq")
    offset = int(match.group("offset"))
    return {
        "file": match.group("file"),
        "file_index": int(seq_text),
        # 保留文件名里的补零宽度，回写时和旧文本完全一致。
        "file_seq": seq_text,
        "offset": offset,
    }


def _parse_postgresql(raw: str) -> dict[str, Any]:
    match = _PG_RE.match(raw)
    if match is None:
        raise PositionError(
            f"PostgreSQL 位点应为 pg_lsn.<hi>/<lo>，实际为 {raw!r}",
            raw=raw, source=POSTGRESQL, reason="unparsable-postgresql-lsn",
        )
    hi = int(match.group("hi"), 16)
    lo = int(match.group("lo"), 16)
    return {
        "lsn": (hi << 32) | lo,
        "hi": hi,
        "lo": lo,
        "lsn_hex": f"{hi:X}/{lo:X}",
    }


def _render_mysql(payload: dict[str, Any]) -> str:
    return f"{payload['file']}:{payload['offset']}"


def _render_postgresql(payload: dict[str, Any]) -> str:
    lsn = int(payload["lsn"])
    return f"pg_lsn.{lsn >> 32:X}/{lsn & 0xFFFFFFFF:X}"


def _key_mysql(payload: dict[str, Any]) -> list[int]:
    return [int(payload["file_index"]), int(payload["offset"])]


def _key_postgresql(payload: dict[str, Any]) -> int:
    return int(payload["lsn"])


def _next_mysql(payload: dict[str, Any]) -> dict[str, Any]:
    advanced = dict(payload)
    advanced["offset"] = int(payload["offset"]) + 1
    return advanced


def _next_postgresql(payload: dict[str, Any]) -> dict[str, Any]:
    lsn = int(payload["lsn"]) + 1
    return {
        "lsn": lsn,
        "hi": lsn >> 32,
        "lo": lsn & 0xFFFFFFFF,
        "lsn_hex": f"{lsn >> 32:X}/{lsn & 0xFFFFFFFF:X}",
    }


_PARSERS = {MYSQL: _parse_mysql, POSTGRESQL: _parse_postgresql}
_RENDERERS = {MYSQL: _render_mysql, POSTGRESQL: _render_postgresql}
_KEYS = {MYSQL: _key_mysql, POSTGRESQL: _key_postgresql}
_NEXT = {MYSQL: _next_mysql, POSTGRESQL: _next_postgresql}


@dataclass(frozen=True)
class SavedPosition:
    """一条带来源和格式版本的保存位点。"""

    source: str
    payload: dict[str, Any]
    raw: str

    @property
    def source_format(self) -> str:
        return SOURCE_FORMATS[self.source]["source_format"]

    @property
    def source_format_version(self) -> int:
        return int(SOURCE_FORMATS[self.source]["format_version"])

    @property
    def format_version(self) -> int:
        return SAVED_POSITION_FORMAT_VERSION

    @property
    def key(self) -> list[int] | int:
        return _KEYS[self.source](self.payload)

    def render(self) -> str:
        return _RENDERERS[self.source](self.payload)

    def next(self) -> "SavedPosition":
        payload = _NEXT[self.source](self.payload)
        return SavedPosition(
            source=self.source, payload=payload, raw=_RENDERERS[self.source](payload)
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "format": SAVED_POSITION_FORMAT,
            "format_version": self.format_version,
            "source": self.source,
            "source_format": self.source_format,
            "source_format_version": self.source_format_version,
            "payload": dict(self.payload),
            "raw": self.raw or self.render(),
            "unit": SOURCE_FORMATS[self.source]["unit"],
        }


def parse_position(raw: str, *, source: str | None = None) -> SavedPosition:
    """按来源解释位点文本；来源未知时先按前缀推断。"""
    text = (raw or "").strip()
    if not text:
        raise PositionError("位点文本为空", raw=raw, source=source, reason="empty-position")
    resolved = source or detect_source(text)
    if resolved is None:
        raise PositionError(
            f"无法识别的位点文本：{text!r}",
            raw=text, source=None, reason="unrecognized-position",
        )
    if resolved not in _PARSERS:
        raise PositionError(
            f"未知位点来源：{resolved}",
            raw=text, source=resolved, reason="unknown-source",
        )
    payload = _PARSERS[resolved](text)
    return SavedPosition(source=resolved, payload=payload, raw=text)


def recognize(raw: str) -> SavedPosition | None:
    """识别成功返回保存位点，失败返回 None（由调用方原样留档）。"""
    try:
        return parse_position(raw)
    except PositionError:
        return None


def compare(left: SavedPosition, right: SavedPosition) -> int:
    """同来源比较两个位点；跨来源直接拒绝，避免文件偏移与 LSN 混算。"""
    if left.source != right.source:
        raise PositionError(
            f"位点来源不同，不能比较：{left.source} vs {right.source}",
            source=left.source, reason="source-mismatch",
        )
    left_key = left.key
    right_key = right.key
    return (left_key > right_key) - (left_key < right_key)


def _restore_key(source: str, key: list[int] | int) -> str:
    """把持久化里的 key 还原成可读位点文本，仅用于展示。"""
    if source == MYSQL:
        file_index, offset = int(key[0]), int(key[1])
        return f"<mysql file#{file_index}>:{offset}"
    return f"pg_lsn.{(int(key) >> 32):X}/{(int(key) & 0xFFFFFFFF):X}"


def describe_source(source: str) -> dict[str, Any]:
    return dict(SOURCE_FORMATS[source])


def catalog() -> list[dict[str, Any]]:
    return [dict(item) for item in SOURCE_FORMATS.values()]
