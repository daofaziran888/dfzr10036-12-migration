"""保存位点的本地持久化夹具：只追加保存位点记录，支持重启、回滚与回放登记。

存量检查点里的无类型文本在启动时被导入成一条 ``legacy-baseline`` 记录；
之后的旧写入端文本提交、新版结构化提交、确认放行、回放和回滚都以追加记录的
方式落库。采用哪条位点由“已确认且未隔离的最高序号”决定，迁移和两次确认重叠
时不会互相覆盖历史。

存储使用标准库 sqlite3，默认落在 ``backend/.state/checkpoint-positions.sqlite3``，
可用环境变量 ``APP_STATE_DB`` 指向别的文件。进程重启后读同一个文件即可恢复。
"""
from __future__ import annotations

import json
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Iterator

from app.services.positions import (
    MYSQL,
    SAVED_POSITION_FORMAT,
    SAVED_POSITION_FORMAT_VERSION,
    SOURCE_FORMATS,
    PositionError,
    SavedPosition,
    compare,
    detect_source,
    parse_position,
)

DEFAULT_STATE_PATH = (
    Path(__file__).resolve().parents[2] / ".state" / "checkpoint-positions.sqlite3"
)

# 数据流 -> 源库类型。存量文本无法识别时仍要知道按哪套坐标系留档。
STREAM_SOURCES: dict[str, str] = {
    "order_events_cdc": MYSQL,
    "payment_log_cdc": "postgresql",
}

STATE_ACTIVE = "active"
STATE_HELD = "held"
STATE_QUARANTINED = "quarantined"
STATE_SUPERSEDED = "superseded"

MIGRATION_NOT_STARTED = "not-started"
MIGRATION_PAUSED = "paused"
MIGRATION_COMPLETED = "completed"
MIGRATION_ROLLED_BACK = "rolled-back-legacy"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS stream (
    stream_id TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    migration_state TEXT NOT NULL,
    paused_at TEXT,
    adopted_sequence INTEGER,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS position_record (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    stream_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    origin TEXT NOT NULL,
    state TEXT NOT NULL,
    acknowledged INTEGER NOT NULL DEFAULT 0,
    format TEXT,
    format_version INTEGER,
    source TEXT,
    source_format TEXT,
    source_format_version INTEGER,
    payload TEXT,
    raw TEXT NOT NULL,
    note TEXT,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS quarantine (
    qid INTEGER PRIMARY KEY AUTOINCREMENT,
    stream_id TEXT NOT NULL,
    raw TEXT NOT NULL,
    reason TEXT NOT NULL,
    record_sequence INTEGER,
    archived_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS replayed_range (
    range_id INTEGER PRIMARY KEY AUTOINCREMENT,
    stream_id TEXT NOT NULL,
    source TEXT NOT NULL,
    start_raw TEXT NOT NULL,
    end_raw TEXT NOT NULL,
    start_payload TEXT NOT NULL,
    end_payload TEXT NOT NULL,
    record_sequence INTEGER,
    replayed_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS backfill_dispatch (
    dispatch_id INTEGER PRIMARY KEY AUTOINCREMENT,
    stream_id TEXT NOT NULL,
    source TEXT NOT NULL,
    start_raw TEXT NOT NULL,
    end_raw TEXT NOT NULL,
    status TEXT NOT NULL,
    dispatched_at TEXT NOT NULL,
    completed_at TEXT
);
CREATE TABLE IF NOT EXISTS event_log (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    stream_id TEXT,
    event_type TEXT NOT NULL,
    detail TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_position_stream
    ON position_record(stream_id, sequence);
CREATE INDEX IF NOT EXISTS idx_replayed_stream
    ON replayed_range(stream_id);
"""


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


class PositionStore:
    """保存位点数据库；每次操作独立连接，避免线程间共享游标。"""

    def __init__(self, path: str | Path | None = None) -> None:
        raw_path = path or os.getenv("APP_STATE_DB") or DEFAULT_STATE_PATH
        self.path = Path(raw_path)
        self.ready = False

    # ---- 基础 ----

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.path, timeout=10)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def ensure_ready(self) -> None:
        if self.ready:
            return
        with self._connect() as conn:
            conn.executescript(_SCHEMA)
            conn.execute("PRAGMA journal_mode=WAL")
        self.ready = True

    def _log(self, conn: sqlite3.Connection, stream_id: str | None,
             event_type: str, detail: dict[str, Any]) -> None:
        conn.execute(
            "INSERT INTO event_log(stream_id, event_type, detail, created_at) "
            "VALUES (?, ?, ?, ?)",
            (stream_id, event_type, json.dumps(detail, ensure_ascii=False), _now()),
        )

    # ---- 流与导入 ----

    def stream_source(self, stream_id: str) -> str | None:
        self.ensure_ready()
        with self._connect() as conn:
            row = conn.execute(
                "SELECT source FROM stream WHERE stream_id = ?", (stream_id,)
            ).fetchone()
        return row["source"] if row else None

    @staticmethod
    def known_source(stream_id: str) -> str | None:
        """不触发建库，仅查静态映射；用于登记前的字段补齐。"""
        return STREAM_SOURCES.get(stream_id)

    def ensure_stream(self, stream_id: str, source: str) -> None:
        self.ensure_ready()
        if source not in SOURCE_FORMATS:
            raise PositionError(f"未知位点来源：{source}", source=source,
                                reason="unknown-source")
        with self._connect() as conn:
            existing = conn.execute(
                "SELECT 1 FROM stream WHERE stream_id = ?", (stream_id,)
            ).fetchone()
            if existing:
                return
            conn.execute(
                "INSERT INTO stream(stream_id, source, migration_state, updated_at) "
                "VALUES (?, ?, ?, ?)",
                (stream_id, source, MIGRATION_NOT_STARTED, _now()),
            )
            self._log(conn, stream_id, "stream-registered", {"source": source})

    def import_legacy(
        self, stream_id: str, legacy_text: str, *, source: str | None = None,
        checkpoint_id: int | None = None,
    ) -> dict[str, Any]:
        """把存量检查点文本导入为基线记录；无法识别的文本原样留档。"""
        self.ensure_ready()
        source = source or STREAM_SOURCES.get(stream_id) or detect_source(legacy_text)
        if source is None:
            raise PositionError(
                f"数据流 {stream_id} 未配置位点来源，无法归档 {legacy_text!r}",
                raw=legacy_text, reason="unknown-source",
            )
        self.ensure_stream(stream_id, source)
        with self._connect() as conn:
            exists = conn.execute(
                "SELECT 1 FROM position_record WHERE stream_id = ? AND kind = ?",
                (stream_id, "legacy-baseline"),
            ).fetchone()
            if exists:
                return {"imported": False, "reason": "baseline-exists"}
            record = self._append_position(
                conn, stream_id,
                kind="legacy-baseline", origin="legacy-text",
                raw=legacy_text, declared_source=source,
                acknowledged=True, state=STATE_ACTIVE,
                note="存量检查点位点导入",
            )
            self._log(conn, stream_id, "legacy-imported", {
                "raw": legacy_text,
                "sequence": record["sequence"],
                "quarantined": record["state"] == STATE_QUARANTINED,
                "checkpoint_id": checkpoint_id,
            })
            return {"imported": True, "record": record}

    # ---- 写入 ----

    def _append_position(
        self,
        conn: sqlite3.Connection,
        stream_id: str,
        *,
        kind: str,
        origin: str,
        raw: str,
        declared_source: str,
        acknowledged: bool,
        state: str,
        note: str,
        format_version: int = SAVED_POSITION_FORMAT_VERSION,
    ) -> dict[str, Any]:
        """追加一条保存位点；解析失败时转为隔离记录，原文不丢。"""
        parsed: SavedPosition | None = None
        error: PositionError | None = None
        try:
            parsed = parse_position(raw, source=declared_source)
        except PositionError as exc:
            error = exc

        if parsed is None:
            stored_source = declared_source
            source_format = None
            source_format_version = None
            payload_json = None
            state = STATE_QUARANTINED
            acknowledged = False
        else:
            stored_source = parsed.source
            source_format = parsed.source_format
            source_format_version = parsed.source_format_version
            payload_json = json.dumps(parsed.payload, ensure_ascii=False)
            state = state
        record_format = SAVED_POSITION_FORMAT if parsed is not None else None
        record_format_version = int(format_version) if parsed is not None else None

        cursor = conn.execute(
            "INSERT INTO position_record("
            "stream_id, kind, origin, state, acknowledged, format, format_version,"
            " source, source_format, source_format_version, payload, raw, note, created_at"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                stream_id, kind, origin, state, int(acknowledged),
                record_format, record_format_version,
                stored_source, source_format, source_format_version,
                payload_json, raw, note, _now(),
            ),
        )
        sequence = int(cursor.lastrowid or 0)
        if error is not None:
            conn.execute(
                "INSERT INTO quarantine(stream_id, raw, reason, record_sequence, archived_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (stream_id, raw, error.reason, sequence, _now()),
            )
        self._reconcile_adoption(conn, stream_id)
        return self.get_record_by_sequence(sequence, conn=conn) or {}

    def submit_legacy(
        self, stream_id: str, raw: str, *, acknowledged: bool = True,
        hold: bool = False, note: str = "旧写入端文本提交",
    ) -> dict[str, Any]:
        """旧写入端沿用原文本契约提交进度；可暂缓确认。"""
        self.ensure_ready()
        source = self._require_source(stream_id)
        with self._connect() as conn:
            record = self._append_position(
                conn, stream_id,
                kind="legacy-save", origin="legacy-text", raw=raw,
                declared_source=source,
                acknowledged=acknowledged and not hold,
                state=STATE_HELD if hold else STATE_ACTIVE,
                note=note,
            )
            self._log(conn, stream_id, "legacy-progress", {
                "sequence": record["sequence"], "raw": raw,
                "hold": hold, "state": record["state"],
            })
        return record

    def commit_progress(
        self,
        stream_id: str,
        raw: str,
        *,
        source: str | None = None,
        format_version: int | None = None,
        source_format: str | None = None,
        replay: dict[str, str] | None = None,
        note: str = "新版保存位点提交",
    ) -> dict[str, Any]:
        """新版进度提交：落库带来源和格式版本的位点，并回报实际采用的格式。"""
        self.ensure_ready()
        declared = source or self._require_source(stream_id)
        stream_source = self._require_source(stream_id)
        if declared != stream_source:
            raise PositionError(
                f"提交来源 {declared} 与数据流来源 {stream_source} 不一致",
                source=declared, reason="source-mismatch",
            )
        if source_format is not None and \
                source_format != SOURCE_FORMATS[declared]["source_format"]:
            raise PositionError(
                f"位点格式 {source_format} 与来源 {declared} 不匹配",
                source=declared, reason="format-mismatch",
            )
        if format_version is not None and int(format_version) != SAVED_POSITION_FORMAT_VERSION:
            raise PositionError(
                f"不支持的保存位点格式版本：{format_version}",
                source=declared, reason="unsupported-format-version",
            )
        parsed = parse_position(raw, source=declared)

        with self._connect() as conn:
            replay_info: dict[str, Any] | None = None
            if replay:
                replay_info = self._register_replay(conn, stream_id, replay)
            record = self._append_position(
                conn, stream_id,
                kind="saved-position-save", origin="saved-position", raw=raw,
                declared_source=declared, acknowledged=True, state=STATE_ACTIVE,
                note=note, format_version=SAVED_POSITION_FORMAT_VERSION,
            )
            if replay_info is not None:
                conn.execute(
                    "UPDATE replayed_range SET record_sequence = ? WHERE range_id = ?",
                    (record["sequence"], replay_info["range_id"]),
                )
            self._log(conn, stream_id, "progress-committed", {
                "sequence": record["sequence"], "raw": raw,
                "replay": replay_info,
                "format": record["format"],
                "format_version": record["format_version"],
                "source_format": record["source_format"],
                "source_format_version": record["source_format_version"],
            })
        adopted = self.adopted_record(stream_id)
        return {
            "record": record,
            "applied_format": {
                "format": record["format"],
                "format_version": record["format_version"],
                "source": record["source"],
                "source_format": record["source_format"],
                "source_format_version": record["source_format_version"],
            },
            "applied_position": parsed.render(),
            "adopted_sequence": adopted["sequence"] if adopted else None,
            "adopted_position": adopted["position"] if adopted else None,
            "replay": replay_info,
        }

    def archive_unrecognized(self, stream_id: str, raw: str) -> dict[str, Any]:
        """无法识别的历史文本只留档，不参与采用与起点计算。"""
        self.ensure_ready()
        source = self._require_source(stream_id)
        with self._connect() as conn:
            record = self._append_position(
                conn, stream_id,
                kind="unrecognized-archive", origin="legacy-text", raw=raw,
                declared_source=source, acknowledged=False,
                state=STATE_QUARANTINED, note="无法识别的历史文本原样留档",
            )
            self._log(conn, stream_id, "unrecognized-archived", {
                "sequence": record["sequence"], "raw": raw,
            })
        return record

    def hold_record(self, stream_id: str, sequence: int) -> dict[str, Any]:
        return self._set_acknowledgement(stream_id, sequence, hold=True)

    def release_record(self, stream_id: str, sequence: int) -> dict[str, Any]:
        return self._set_acknowledgement(stream_id, sequence, hold=False)

    def _set_acknowledgement(
        self, stream_id: str, sequence: int, *, hold: bool,
    ) -> dict[str, Any]:
        self.ensure_ready()
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM position_record WHERE stream_id = ? AND sequence = ?",
                (stream_id, sequence),
            ).fetchone()
            if row is None:
                raise PositionError(
                    f"数据流 {stream_id} 不存在保存位点记录 {sequence}",
                    reason="record-not-found",
                )
            if row["state"] == STATE_QUARANTINED:
                raise PositionError(
                    "隔离记录不能确认放行，原文仅作留档",
                    raw=row["raw"], reason="record-quarantined",
                )
            conn.execute(
                "UPDATE position_record SET state = ?, acknowledged = ? WHERE sequence = ?",
                (STATE_HELD if hold else STATE_ACTIVE, int(not hold), sequence),
            )
            self._reconcile_adoption(conn, stream_id)
            self._log(conn, stream_id, "ack-hold" if hold else "ack-release", {
                "sequence": sequence,
            })
            return self.get_record_by_sequence(sequence, conn=conn) or {}

    # ---- 采用与回放 ----

    def _reconcile_adoption(self, conn: sqlite3.Connection, stream_id: str) -> None:
        """已确认且未隔离的最高序号即采用位点；其余已确认记录标记为被取代。"""
        rows = conn.execute(
            "SELECT sequence FROM position_record "
            "WHERE stream_id = ? AND acknowledged = 1 AND state != ? "
            "ORDER BY sequence DESC",
            (stream_id, STATE_QUARANTINED),
        ).fetchall()
        if not rows:
            conn.execute(
                "UPDATE stream SET adopted_sequence = NULL, updated_at = ? "
                "WHERE stream_id = ?",
                (_now(), stream_id),
            )
            return
        adopted = int(rows[0]["sequence"])
        conn.execute(
            "UPDATE position_record SET state = ? "
            "WHERE stream_id = ? AND sequence != ? AND acknowledged = 1 "
            "AND state != ?",
            (STATE_SUPERSEDED, stream_id, adopted, STATE_QUARANTINED),
        )
        conn.execute(
            "UPDATE position_record SET state = ? WHERE sequence = ?",
            (STATE_ACTIVE, adopted),
        )
        conn.execute(
            "UPDATE stream SET adopted_sequence = ?, updated_at = ? WHERE stream_id = ?",
            (adopted, _now(), stream_id),
        )

    def adopted_record(self, stream_id: str) -> dict[str, Any] | None:
        self.ensure_ready()
        with self._connect() as conn:
            stream = conn.execute(
                "SELECT adopted_sequence FROM stream WHERE stream_id = ?", (stream_id,)
            ).fetchone()
            if stream is None or stream["adopted_sequence"] is None:
                return None
            return self.get_record_by_sequence(int(stream["adopted_sequence"]), conn=conn)

    def adopted_position(self, stream_id: str) -> SavedPosition | None:
        record = self.adopted_record(stream_id)
        if record is None or record["payload"] is None:
            return None
        return SavedPosition(
            source=record["source"],
            payload=record["payload"],
            raw=record["raw"],
        )

    def _register_replay(
        self, conn: sqlite3.Connection, stream_id: str, replay: dict[str, str],
    ) -> dict[str, Any]:
        """回放成功后登记区间；同一区间重复登记不产生新记录。"""
        from_raw = str(replay.get("from", "")).strip()
        to_raw = str(replay.get("to", "")).strip()
        source = self._require_source(stream_id)
        start = parse_position(from_raw, source=source)
        end = parse_position(to_raw, source=source)
        if compare(start, end) > 0:
            raise PositionError(
                f"回放区间起点晚于终点：{from_raw} > {to_raw}",
                raw=from_raw, source=source, reason="invalid-replay-range",
            )
        row = conn.execute(
            "SELECT * FROM replayed_range WHERE stream_id = ? AND source = ? "
            "AND start_raw = ? AND end_raw = ? ORDER BY range_id DESC LIMIT 1",
            (stream_id, source, start.render(), end.render()),
        ).fetchone()
        if row is not None:
            return {
                "range_id": int(row["range_id"]),
                "from": row["start_raw"],
                "to": row["end_raw"],
                "source": row["source"],
                "duplicate": True,
            }
        cursor = conn.execute(
            "INSERT INTO replayed_range("
            "stream_id, source, start_raw, end_raw, start_payload, end_payload,"
            " replayed_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                stream_id, source, start.render(), end.render(),
                json.dumps(start.payload, ensure_ascii=False),
                json.dumps(end.payload, ensure_ascii=False), _now(),
            ),
        )
        conn.execute(
            "UPDATE backfill_dispatch SET status = ?, completed_at = ? "
            "WHERE stream_id = ? AND source = ? AND status = ? "
            "AND start_raw = ? AND end_raw = ?",
            ("completed", _now(), stream_id, source, "dispatched",
             start.render(), end.render()),
        )
        return {
            "range_id": int(cursor.lastrowid or 0),
            "from": start.render(),
            "to": end.render(),
            "source": source,
            "duplicate": False,
        }

    def request_backfill(
        self, stream_id: str, *, to_raw: str, from_raw: str | None = None,
    ) -> dict[str, Any]:
        """从采用的已确认位点之后申请回放；已成功回放的同一范围不再进入回填。

        回放“成功”只由回放后的进度提交登记；这里仅派发尚未回放的剩余部分，
        不把派发当作成功回放，避免一次没跑完的回放被误认为已完成。
        """
        self.ensure_ready()
        source = self._require_source(stream_id)
        adopted = self.adopted_position(stream_id)
        if adopted is None:
            raise PositionError(
                f"数据流 {stream_id} 没有已确认位点，无法确定回放起点",
                source=source, reason="no-adopted-position",
            )
        start = (parse_position(from_raw, source=source) if from_raw
                 else adopted.next())
        if compare(start, adopted) <= 0:
            raise PositionError(
                f"回放起点 {start.render()} 必须晚于已确认位点 {adopted.render()}",
                raw=start.raw, source=source, reason="start-not-after-adopted",
            )
        end = parse_position(to_raw, source=source)
        if compare(start, end) > 0:
            raise PositionError(
                f"回放起点晚于终点：{start.render()} > {end.render()}",
                raw=to_raw, source=source, reason="invalid-backfill-range",
            )
        requested_from = start.render()
        requested_to = end.render()
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM replayed_range WHERE stream_id = ? AND source = ?",
                (stream_id, source),
            ).fetchall()
        covering: dict[str, Any] | None = None
        furthest_end: SavedPosition | None = None
        for row in rows:
            prior_start = SavedPosition(
                source=row["source"],
                payload=json.loads(row["start_payload"]), raw=row["start_raw"],
            )
            prior_end = SavedPosition(
                source=row["source"],
                payload=json.loads(row["end_payload"]), raw=row["end_raw"],
            )
            if compare(prior_start, start) <= 0 and compare(prior_end, end) >= 0:
                covering = {
                    "from": prior_start.render(),
                    "to": prior_end.render(),
                    "range_id": int(row["range_id"]),
                }
                break
            # 已成功回放的区间与请求区间相接或重叠时，跳过该段。
            if compare(prior_start, end) <= 0 and compare(prior_end, start) >= 0:
                if furthest_end is None or compare(prior_end, furthest_end) > 0:
                    furthest_end = prior_end
        if covering is not None:
            return {
                "dispatched": False,
                "reentered": False,
                "reason": "range-already-replayed",
                "requested": {"from": requested_from, "to": requested_to},
                "covered_by": covering,
            }
        skipped: dict[str, Any] | None = None
        if furthest_end is not None:
            skipped = {"from": requested_from, "to": furthest_end.render()}
            start = furthest_end.next()
            if compare(start, end) > 0:
                return {
                    "dispatched": False,
                    "reentered": False,
                    "reason": "range-already-replayed",
                    "requested": {"from": requested_from, "to": requested_to},
                    "covered_by": skipped,
                }
        with self._connect() as conn:
            prior = conn.execute(
                "SELECT dispatch_id FROM backfill_dispatch WHERE stream_id = ? "
                "AND source = ? AND start_raw = ? AND end_raw = ? AND status = ? "
                "ORDER BY dispatch_id DESC LIMIT 1",
                (stream_id, source, start.render(), end.render(), "dispatched"),
            ).fetchone()
            resumed = prior is not None
            if prior is None:
                conn.execute(
                    "INSERT INTO backfill_dispatch("
                    "stream_id, source, start_raw, end_raw, status, dispatched_at"
                    ") VALUES (?, ?, ?, ?, ?, ?)",
                    (stream_id, source, start.render(), end.render(),
                     "dispatched", _now()),
                )
            self._log(conn, stream_id, "backfill-dispatched", {
                "from": start.render(), "to": end.render(),
                "requested": {"from": requested_from, "to": requested_to},
                "resumed": resumed,
            })
        return {
            "dispatched": True,
            "reentered": False,
            "resumed": resumed,
            "reason": "resumed-after-interruption" if resumed else "new-range",
            "from": start.render(),
            "to": end.render(),
            "requested": {"from": requested_from, "to": requested_to},
            "skipped_replayed": skipped,
        }

    # ---- 迁移 / 恢复 / 回滚 ----

    def start_migration(self, stream_id: str) -> dict[str, Any]:
        """读取存量旧值后暂停迁移，等待旧写入端的确认与新入口的恢复请求。"""
        self.ensure_ready()
        with self._connect() as conn:
            stream = conn.execute(
                "SELECT * FROM stream WHERE stream_id = ?", (stream_id,)
            ).fetchone()
            if stream is None:
                raise PositionError(
                    f"数据流 {stream_id} 未登记", reason="stream-not-found"
                )
            baseline = conn.execute(
                "SELECT * FROM position_record WHERE stream_id = ? AND kind = ? "
                "ORDER BY sequence ASC LIMIT 1",
                (stream_id, "legacy-baseline"),
            ).fetchone()
            if baseline is None:
                raise PositionError(
                    f"数据流 {stream_id} 没有可迁移的存量位点",
                    reason="baseline-missing",
                )
            conn.execute(
                "UPDATE stream SET migration_state = ?, paused_at = ?, updated_at = ? "
                "WHERE stream_id = ?",
                (MIGRATION_PAUSED, _now(), _now(), stream_id),
            )
            self._log(conn, stream_id, "migration-paused", {
                "legacy_raw": baseline["raw"],
                "baseline_sequence": int(baseline["sequence"]),
                "adopted_sequence": stream["adopted_sequence"],
            })
            return {
                "stream": stream_id,
                "source": stream["source"],
                "migration_state": MIGRATION_PAUSED,
                "legacy_value": baseline["raw"],
                "baseline": self.get_record_by_sequence(
                    int(baseline["sequence"]), conn=conn),
            }

    def rollback_to_legacy(self, stream_id: str) -> dict[str, Any]:
        """按旧写入端契约回滚：把当前采用的位点用原文本重新提交。"""
        self.ensure_ready()
        adopted = self.adopted_record(stream_id)
        if adopted is None:
            raise PositionError(
                f"数据流 {stream_id} 没有可回滚的已确认位点",
                reason="no-adopted-position",
            )
        with self._connect() as conn:
            record = self._append_position(
                conn, stream_id,
                kind="legacy-rollback", origin="legacy-text",
                raw=adopted["raw"], declared_source=self._require_source(stream_id),
                acknowledged=True, state=STATE_ACTIVE,
                note="按旧写入端契约回滚",
            )
            conn.execute(
                "UPDATE stream SET migration_state = ?, updated_at = ? WHERE stream_id = ?",
                (MIGRATION_ROLLED_BACK, _now(), stream_id),
            )
            self._log(conn, stream_id, "rollback-legacy", {
                "sequence": record["sequence"], "raw": record["raw"],
                "adopted_before": adopted["sequence"],
            })
        return {
            "stream": stream_id,
            "migration_state": MIGRATION_ROLLED_BACK,
            "legacy_text": record["raw"],
            "record": record,
        }

    def mark_migration_completed(self, stream_id: str) -> dict[str, Any]:
        """确认后的恢复请求把迁移从暂停推进到完成。"""
        self.ensure_ready()
        with self._connect() as conn:
            changed = conn.execute(
                "UPDATE stream SET migration_state = ?, updated_at = ? WHERE stream_id = ?",
                (MIGRATION_COMPLETED, _now(), stream_id),
            ).rowcount
            if not changed:
                raise PositionError(
                    f"数据流 {stream_id} 未登记", reason="stream-not-found"
                )
            self._log(conn, stream_id, "migration-completed", {})
        return {"stream": stream_id, "migration_state": MIGRATION_COMPLETED}

    # ---- 查询 ----

    def _require_source(self, stream_id: str) -> str:
        source = self.stream_source(stream_id)
        if source is None:
            raise PositionError(
                f"数据流 {stream_id} 未登记", reason="stream-not-found"
            )
        return source

    def get_record_by_sequence(
        self, sequence: int, *, conn: sqlite3.Connection | None = None,
    ) -> dict[str, Any] | None:
        self.ensure_ready()
        if conn is None:
            with self._connect() as own:
                row = own.execute(
                    "SELECT * FROM position_record WHERE sequence = ?", (sequence,)
                ).fetchone()
        else:
            row = conn.execute(
                "SELECT * FROM position_record WHERE sequence = ?", (sequence,)
            ).fetchone()
        return self._record_to_dict(row)

    def _record_to_dict(self, row: sqlite3.Row | None) -> dict[str, Any] | None:
        if row is None:
            return None
        payload = json.loads(row["payload"]) if row["payload"] else None
        position = None
        if payload is not None and row["source"] in SOURCE_FORMATS:
            position = SavedPosition(
                source=row["source"], payload=payload, raw=row["raw"]
            ).render()
        return {
            "sequence": int(row["sequence"]),
            "stream": row["stream_id"],
            "kind": row["kind"],
            "origin": row["origin"],
            "state": row["state"],
            "acknowledged": bool(row["acknowledged"]),
            "format": row["format"],
            "format_version": row["format_version"],
            "source": row["source"],
            "source_format": row["source_format"],
            "source_format_version": row["source_format_version"],
            "payload": payload,
            "raw": row["raw"],
            "position": position or row["raw"],
            "note": row["note"],
            "created_at": row["created_at"],
        }

    def list_records(self, stream_id: str) -> list[dict[str, Any]]:
        self.ensure_ready()
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM position_record WHERE stream_id = ? ORDER BY sequence ASC",
                (stream_id,),
            ).fetchall()
        return [self._record_to_dict(row) for row in rows if row is not None]

    def list_quarantine(self, stream_id: str) -> list[dict[str, Any]]:
        self.ensure_ready()
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM quarantine WHERE stream_id = ? ORDER BY qid ASC",
                (stream_id,),
            ).fetchall()
        return [
            {
                "qid": int(row["qid"]),
                "raw": row["raw"],
                "reason": row["reason"],
                "record_sequence": row["record_sequence"],
                "archived_at": row["archived_at"],
            }
            for row in rows
        ]

    def list_replayed_ranges(self, stream_id: str) -> list[dict[str, Any]]:
        self.ensure_ready()
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM replayed_range WHERE stream_id = ? ORDER BY range_id ASC",
                (stream_id,),
            ).fetchall()
        return [
            {
                "range_id": int(row["range_id"]),
                "from": row["start_raw"],
                "to": row["end_raw"],
                "source": row["source"],
                "record_sequence": row["record_sequence"],
                "replayed_at": row["replayed_at"],
            }
            for row in rows
        ]

    def stream_state(self, stream_id: str) -> dict[str, Any] | None:
        self.ensure_ready()
        with self._connect() as conn:
            stream = conn.execute(
                "SELECT * FROM stream WHERE stream_id = ?", (stream_id,)
            ).fetchone()
        if stream is None:
            return None
        source = stream["source"]
        adopted = self.adopted_record(stream_id)
        records = self.list_records(stream_id)
        quarantined = self.list_quarantine(stream_id)
        ranges = self.list_replayed_ranges(stream_id)
        return {
            "stream": stream_id,
            "source": source,
            "source_format": SOURCE_FORMATS[source]["source_format"],
            "source_format_version": SOURCE_FORMATS[source]["format_version"],
            "format_version": SAVED_POSITION_FORMAT_VERSION,
            "migration_state": stream["migration_state"],
            "paused_at": stream["paused_at"],
            "adopted": adopted,
            "adopted_position": adopted["position"] if adopted else None,
            "records": records,
            "held": [r for r in records if r["state"] == STATE_HELD],
            "quarantined": quarantined,
            "replayed_ranges": ranges,
        }

    def list_streams(self) -> list[dict[str, Any]]:
        self.ensure_ready()
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT stream_id FROM stream ORDER BY stream_id"
            ).fetchall()
        result = []
        for row in rows:
            state = self.stream_state(row["stream_id"])
            if state:
                result.append({
                    "stream": state["stream"],
                    "source": state["source"],
                    "migration_state": state["migration_state"],
                    "adopted_position": state["adopted_position"],
                    "record_count": len(state["records"]),
                    "held_count": len(state["held"]),
                    "quarantined_count": len(state["quarantined"]),
                    "replayed_range_count": len(state["replayed_ranges"]),
                })
        return result


position_store = PositionStore()
