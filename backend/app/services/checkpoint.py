"""检查点服务：断点恢复时触发水位重置和回填任务；保存位点带来源与格式版本。"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.services.checkpoint_positions import (
    MIGRATION_PAUSED,
    PositionStore,
    position_store,
)
from app.services.positions import (
    MYSQL,
    PositionError,
    SavedPosition,
    catalog,
    compare,
    parse_position,
)
from app.store import store

# 恢复后的读取窗口终点（示例里用固定上限，真实实现由源端 latest 决定）。
RESTORE_WINDOW_END: dict[str, str] = {
    "mysql": "binlog.001234:892000",
    "postgresql": "pg_lsn.0/3E83000",
}
DEFAULT_WINDOW_END = {
    "mysql": "binlog.001234:999999",
    "postgresql": "pg_lsn.FFFF/FFFFFFFF",
}


class CheckpointService(BaseModuleService):
    MODULE = "checkpoint"
    ENTITY = "检查点"
    OPERATION_TERM = "校验"
    LIST_FIELDS = [
        "检查点编号", "数据流", "保存位点", "位点来源", "位点格式", "格式版本",
        "保存时间", "恢复状态", "丢失行数", "回放代价", "恢复耗时",
    ]
    REQUIRED_FIELDS = ["检查点编号", "数据流", "保存位点"]
    STATUS_ORDER = ["已保存", "恢复中", "已恢复", "恢复失败"]
    ACTION_RULES = {"触发恢复": "恢复中", "确认恢复": "已恢复", "标记失败": "恢复失败"}
    NEGATIVE_ACTIONS = []

    def __init__(self, positions: PositionStore | None = None) -> None:
        self.positions = positions or position_store

    # ---- 位点字段补齐 ----

    def _annotate(self, row: dict[str, Any]) -> dict[str, Any]:
        """为老链路新建的行补齐来源/格式版本，无法识别的标注为留档。"""
        if row.get("位点来源"):
            return row
        stream = str(row.get("数据流", ""))
        raw = str(row.get("保存位点", ""))
        source = self.positions.stream_source(stream) or self.positions.known_source(stream)
        try:
            parsed = parse_position(raw, source=source)
        except PositionError:
            row["位点来源"] = source
            row["位点格式"] = None
            row["格式版本"] = None
            row["位点迁移"] = "已留档-不可识别"
            return row
        row["位点来源"] = parsed.source
        row["位点格式"] = parsed.source_format
        row["格式版本"] = parsed.format_version
        row["位点迁移"] = "已迁移"
        return row

    def list_entries(self, **kwargs: Any):
        items, total = super().list_entries(**kwargs)
        return [self._annotate(row) for row in items], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        entry = super().get_entry(entry_id)
        return self._annotate(entry) if entry is not None else None

    def create_entry(self, values: dict[str, Any]):
        """登记检查点时同步落一条保存位点，无法识别的文本原样留档。"""
        values = dict(values)
        stream = str(values.get("数据流", "")).strip()
        raw = str(values.get("保存位点", "")).strip()
        entry, missing = super().create_entry(values)
        if entry is None:
            return entry, missing
        self._annotate(entry)
        source = self.positions.stream_source(stream) or self.positions.known_source(stream)
        if stream and raw and source is not None:
            self.positions.ensure_stream(stream, source)
            try:
                if entry["位点迁移"] == "已迁移":
                    self.positions.commit_progress(stream, raw)
                else:
                    self.positions.archive_unrecognized(stream, raw)
            except PositionError:
                entry["位点迁移"] = "已留档-不可识别"
        return entry, missing

    # ---- 保存位点查询 ----

    def list_saved_positions(self, stream: str | None = None) -> dict[str, Any]:
        """读取已落库的保存位点；按数据流逐条给出采用位点和留档。"""
        if stream:
            state = self.positions.stream_state(stream)
            if state is None:
                return {"ok": False, "message": f"数据流 {stream} 尚无保存位点",
                        "streams": []}
            return {"ok": True, "streams": [state], "catalog": catalog()}
        return {
            "ok": True,
            "streams": self.positions.list_streams(),
            "catalog": catalog(),
        }

    # ---- 存量迁移 ----

    def migrate_legacy_positions(self, *, dry_run: bool = False) -> dict[str, Any]:
        """把存量检查点的无类型文本导入为带来源和格式版本的保存位点。"""
        streams: list[dict[str, Any]] = []
        for row in store.rows(self.MODULE):
            stream = str(row.get("数据流", "")).strip()
            legacy = str(row.get("保存位点", ""))
            if not stream:
                continue
            known_source = (
                self.positions.stream_source(stream)
                or self.positions.known_source(stream)
            )
            entry: dict[str, Any] = {
                "stream": stream,
                "checkpoint_id": row.get("id"),
                "legacy_value": legacy,
            }
            try:
                parsed = parse_position(legacy, source=known_source)
            except PositionError:
                parsed = None
            if parsed is not None:
                entry["recognized"] = True
                entry["source"] = parsed.source
                entry["source_format"] = parsed.source_format
                entry["format_version"] = parsed.format_version
                entry["payload"] = parsed.payload
                if not dry_run:
                    # 检查点条目补齐来源/格式版本；原始文本保留供旧端契约回读。
                    row["原始保存位点"] = legacy
                    row["位点来源"] = parsed.source
                    row["位点格式"] = parsed.source_format
                    row["格式版本"] = parsed.format_version
                    row["保存位点"] = parsed.render()
                    row["位点迁移"] = "已迁移"
            else:
                entry["recognized"] = False
                entry["archive_only"] = True
                entry["reason"] = "位点文本无法识别，原样留档，不参与起点计算"
                if not dry_run:
                    row["原始保存位点"] = legacy
                    row["位点来源"] = known_source
                    row["位点格式"] = None
                    row["格式版本"] = None
                    row["位点迁移"] = "已留档-不可识别"
            if not dry_run:
                try:
                    imported = self.positions.import_legacy(
                        stream, legacy,
                        source=known_source,
                        checkpoint_id=int(row.get("id", 0)),
                    )
                except PositionError as exc:
                    entry["recognized"] = False
                    entry["archive_only"] = True
                    entry["reason"] = str(exc)
                    imported = None
                if imported is not None:
                    entry["imported"] = imported.get("imported", False)
                    if imported.get("record"):
                        entry["baseline_sequence"] = imported["record"]["sequence"]
            streams.append(entry)
        return {
            "ok": True,
            "dry_run": dry_run,
            "streams": streams,
            "catalog": catalog(),
        }

    # ---- 旧写入端进度提交 ----

    def submit_legacy_progress(
        self, stream: str, raw: str, *, acknowledged: bool = True,
        hold: bool = False,
    ) -> dict[str, Any]:
        """模拟继续运行的旧端：沿用原文本契约提交进度，可暂缓确认。"""
        try:
            record = self.positions.submit_legacy(
                stream, raw, acknowledged=acknowledged, hold=hold,
            )
        except PositionError as exc:
            return self._position_error(exc)
        return {
            "ok": True,
            "stream": stream,
            "record": record,
            "adopted_position": self.positions.stream_state(stream)["adopted_position"],
        }

    def hold_position(self, stream: str, sequence: int) -> dict[str, Any]:
        return self._ack(stream, sequence, hold=True)

    def release_position(self, stream: str, sequence: int) -> dict[str, Any]:
        return self._ack(stream, sequence, hold=False)

    def _ack(self, stream: str, sequence: int, *, hold: bool) -> dict[str, Any]:
        try:
            if hold:
                record = self.positions.hold_record(stream, sequence)
            else:
                record = self.positions.release_record(stream, sequence)
        except PositionError as exc:
            return self._position_error(exc)
        adopted = self.positions.adopted_record(stream)
        if adopted is not None:
            self._sync_checkpoint_row(
                stream,
                position=adopted["position"],
                source=adopted["source"],
                source_format=adopted["source_format"],
                format_version=adopted["format_version"],
                migration="已迁移-确认更新" if not hold else "已迁移-待确认",
            )
        return {
            "ok": True,
            "stream": stream,
            "record": record,
            "adopted": adopted,
            "adopted_position": adopted["position"] if adopted else None,
        }

    # ---- 新版进度提交 ----

    def commit_progress(
        self, stream: str, raw: str, *, source: str | None = None,
        source_format: str | None = None, format_version: int | None = None,
        replay_from: str | None = None, replay_to: str | None = None,
    ) -> dict[str, Any]:
        """新版进度提交入口；若带成功回放的区间，会登记并阻止同一范围再次回填。"""
        replay = None
        if replay_from is not None or replay_to is not None:
            if not (replay_from and replay_to):
                return {"ok": False, "message": "回放区间需要同时给出起点和终点"}
            replay = {"from": replay_from, "to": replay_to}
        try:
            result = self.positions.commit_progress(
                stream, raw, source=source, source_format=source_format,
                format_version=format_version, replay=replay,
            )
        except PositionError as exc:
            return self._position_error(exc)
        self._sync_checkpoint_row(
            stream,
            position=result["applied_position"],
            source=result["record"]["source"],
            source_format=result["record"]["source_format"],
            format_version=result["record"]["format_version"],
            migration="已迁移",
        )
        return {"ok": True, "stream": stream, **result}

    # ---- 恢复入口 ----

    def restore_stream(
        self, stream: str, *, replay_to: str | None = None,
        replay_from: str | None = None, dispatch_backfill: bool = True,
    ) -> dict[str, Any]:
        """断点恢复：按采用的已确认位点选实际读取范围，并派发回放。"""
        try:
            state = self.positions.stream_state(stream)
        except PositionError as exc:
            return self._position_error(exc)
        if state is None:
            return {"ok": False, "stream": stream, "message": f"数据流 {stream} 未登记"}
        if state["quarantined"] and state["adopted"] is None:
            return {
                "ok": False, "stream": stream,
                "message": "仅有无法识别的历史文本已留档，无法计算读取起点",
                "quarantine": state["quarantined"],
                "migration_state": state["migration_state"],
            }
        adopted = self.positions.adopted_position(stream)
        if adopted is None:
            return {
                "ok": False, "stream": stream,
                "message": "没有已确认位点，恢复停止",
                "migration_state": state["migration_state"],
            }
        first_read = adopted.next()
        window_end = parse_position(
            replay_to or RESTORE_WINDOW_END.get(state["source"],
                                                 DEFAULT_WINDOW_END[state["source"]]),
            source=state["source"],
        )
        from_source = parse_position(
            replay_from, source=state["source"],
        ) if replay_from else first_read
        nothing_to_read = compare(from_source, window_end) > 0
        response: dict[str, Any] = {
            "ok": True,
            "stream": stream,
            "source": state["source"],
            "source_format": state["source_format"],
            "source_format_version": state["source_format_version"],
            "format_version": state["format_version"],
            "migration_state": state["migration_state"],
            "adopted": state["adopted"],
            "adopted_position": adopted.render(),
            "adopted_commit_state": (
                "已确认" if state["adopted"] and state["adopted"]["acknowledged"]
                else "未确认"
            ),
            "first_read_position": from_source.render()
            if not nothing_to_read else None,
            "commit_state": "已确认",
            "read_range": {
                "from": None if nothing_to_read else from_source.render(),
                "to": window_end.render(),
                "includes_adopted": False,
                "empty": nothing_to_read,
            },
            "pending_confirmation": state["held"],
            "quarantined": state["quarantined"],
            "replayed_ranges": state["replayed_ranges"],
            "progress_commit": {
                "endpoint": "/api/checkpoint-state/progress",
                "replay_from": None if nothing_to_read else from_source.render(),
                "replay_to": window_end.render(),
                "next_commit_position": window_end.render(),
            },
            "backfill": None,
        }
        if nothing_to_read:
            response["backfill"] = {
                "dispatched": False,
                "reentered": False,
                "reason": "nothing-to-read",
                "message": "采用的已确认位点已到或越过本次读取窗口终点",
            }
            response["reentered"] = False
            return response
        if dispatch_backfill:
            try:
                backfill = self.positions.request_backfill(
                    stream,
                    to_raw=window_end.render(),
                    from_raw=replay_from,
                )
            except PositionError as exc:
                backfill = self._position_error(exc)
            response["backfill"] = backfill
            response["reentered"] = bool(
                isinstance(backfill, dict) and backfill.get("reentered"))
        return response

    # ---- 回滚 ----

    @staticmethod
    def _sync_checkpoint_row(stream: str, *, position: str | None,
                             source: str | None, source_format: str | None,
                             format_version: int | None, migration: str) -> None:
        for row in store.rows(CheckpointService.MODULE):
            if str(row.get("数据流", "")) != stream:
                continue
            row.setdefault("原始保存位点", row.get("保存位点"))
            if position:
                row["保存位点"] = position
            row["位点来源"] = source
            row["位点格式"] = source_format
            row["格式版本"] = format_version
            row["位点迁移"] = migration
            break

    def rollback_to_legacy(self, stream: str) -> dict[str, Any]:
        """回滚到旧端契约：用原始文本重新提交已确认位点。"""
        try:
            result = self.positions.rollback_to_legacy(stream)
            state = self.positions.stream_state(stream)
        except PositionError as exc:
            return self._position_error(exc)
        self._sync_checkpoint_row(
            stream,
            position=None,
            source=result["record"]["source"],
            source_format=result["record"]["source_format"],
            format_version=result["record"]["format_version"],
            migration="已回滚-旧端契约",
        )
        return {
            "ok": True,
            "stream": stream,
            "migration_state": result["migration_state"],
            "legacy_text": result["legacy_text"],
            "record": result["record"],
            "adopted_position": state["adopted_position"] if state else None,
            "adopted": state["adopted"] if state else None,
        }

    def resume_migration(self, stream: str) -> dict[str, Any]:
        """继续迁移：恢复请求完成确认后把迁移标记完成。"""
        try:
            state = self.positions.stream_state(stream)
            if state is None:
                return {"ok": False, "message": f"数据流 {stream} 未登记"}
            if state["adopted"] is None:
                return {"ok": False, "message": "没有采用的已确认位点，无法完成迁移"}
            self.positions.mark_migration_completed(stream)
        except PositionError as exc:
            return self._position_error(exc)
        updated = self.positions.stream_state(stream)
        return {
            "ok": True,
            "stream": stream,
            "migration_state": updated["migration_state"] if updated else None,
            "adopted_position": updated["adopted_position"] if updated else None,
        }

    def migrate(self, stream: str) -> dict[str, Any]:
        """开始迁移（读取旧值后暂停）。"""
        try:
            result = self.positions.start_migration(stream)
        except PositionError as exc:
            return self._position_error(exc)
        return {"ok": True, **result, "commit_state": "已暂停"}

    def migration_paused(self, stream: str) -> bool:
        state = self.positions.stream_state(stream)
        return bool(state and state["migration_state"] == MIGRATION_PAUSED)

    # ---- 旧链路：恢复后的水位重置与回填 ----

    def trigger_watermark_reset_and_backfill(self, entry_id: int) -> dict[str, Any]:
        """断点恢复后触发水位重置和回填任务。

        数据流链路: checkpoint -> watermark_mgr + backfill_run
        保存位点按来源和格式版本解释后再写回水位，避免文本被原样搬运。
        """
        entry = self.get_entry(entry_id)
        if entry is None:
            return None

        tag = str(entry.get("数据流", ""))
        results: dict[str, Any] = {"watermark": None, "backfill": None}

        if entry.get("status") == "已恢复":
            position = self._position_for_entry(entry)
            if position is None:
                results["skipped"] = "位点无法识别，保留原文，不参与起点计算"
                return results

            for wm in store.rows("watermark_mgr"):
                if str(wm.get("数据流", "")) == tag:
                    wm["当前位点"] = position.render()
                    wm["status"] = "推进中"
                    wm["pending"] = True
                    wm["abnormal"] = False
                    results["watermark"] = wm
                    break

            loss = int(entry.get("丢失行数", 0))
            if loss > 0:
                bf_rows = store.rows("backfill_run")
                new_bf_id = max((int(r.get("id", 0)) for r in bf_rows), default=0) + 1
                backfill = {
                    "id": new_bf_id,
                    "任务编号": f"BACK-{new_bf_id:04d}",
                    "目标表": tag,
                    "起始位点": position.render(),
                    "截止位点": "latest_binlog",
                    "回填模式": "批量并行回填",
                    "并行粒度": "按分区",
                    "当前进度": "0%",
                    "回填速率": "0 eps",
                    "status": "回填中",
                    "pending": True,
                    "abnormal": False,
                }
                bf_rows.append(backfill)
                results["backfill"] = backfill

        return results

    def _position_for_entry(self, entry: dict[str, Any]) -> SavedPosition | None:
        """优先使用落库的采用位点，其次按数据流来源解释文本。"""
        stream = str(entry.get("数据流", ""))
        adopted = self.positions.adopted_position(stream)
        if adopted is not None:
            return adopted
        raw = str(entry.get("保存位点", ""))
        source = (
            self.positions.stream_source(stream)
            or self.positions.known_source(stream)
            or MYSQL
        )
        try:
            return parse_position(raw, source=source)
        except PositionError:
            return None

    @staticmethod
    def _position_error(exc: PositionError) -> dict[str, Any]:
        return {
            "ok": False,
            "message": str(exc),
            "reason": exc.reason,
            "raw": exc.raw,
            "source": exc.source,
        }
