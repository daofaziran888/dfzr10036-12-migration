"""保存位点接口：迁移、旧端进度、确认放行、恢复、回滚。

路由统一挂在 ``/api/checkpoint-state`` 下，避免和通用资源路由
``/api/{module}/{entry_id}`` 的动态段相互抢先匹配。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Query
from pydantic import BaseModel, Field

from app.services.checkpoint import CheckpointService
from app.services.checkpoint_positions import position_store

router = APIRouter(prefix="/api/checkpoint-state", tags=["保存位点"])

checkpoint = CheckpointService(position_store)


class MigratePayload(BaseModel):
    dry_run: bool = False


class StreamMigrationPayload(BaseModel):
    stream: str


class LegacyProgressPayload(BaseModel):
    stream: str
    raw: str
    acknowledged: bool = True
    hold: bool = False


class ProgressPayload(BaseModel):
    stream: str
    raw: str
    source: str | None = None
    source_format: str | None = None
    format_version: int | None = None
    replay_from: str | None = None
    replay_to: str | None = None


class RestorePayload(BaseModel):
    stream: str
    replay_from: str | None = None
    replay_to: str | None = None
    dispatch_backfill: bool = True


class RollbackPayload(BaseModel):
    stream: str


@router.get("/positions")
def list_saved_positions(stream: str | None = Query(default=None)) -> dict[str, Any]:
    """查看已落库的保存位点、采用结果、隔离原文和已回放区间。"""
    return checkpoint.list_saved_positions(stream)


@router.post("/migrate")
def migrate_legacy_positions(payload: MigratePayload | None = None) -> dict[str, Any]:
    """存量迁移：读取旧文本，能解释的转成保存位点，不能识别的原样留档。"""
    return checkpoint.migrate_legacy_positions(dry_run=bool(payload and payload.dry_run))


@router.post("/migrate/start")
def start_migration(payload: StreamMigrationPayload) -> dict[str, Any]:
    """开始迁移：读取旧值后暂停，等待确认与恢复请求。"""
    return checkpoint.migrate(payload.stream)


@router.post("/migrate/complete")
def complete_migration(payload: StreamMigrationPayload) -> dict[str, Any]:
    """恢复请求确认后完成迁移。"""
    return checkpoint.resume_migration(payload.stream)


@router.post("/legacy-progress")
def submit_legacy_progress(payload: LegacyProgressPayload) -> dict[str, Any]:
    """旧写入端沿用原文本契约提交进度（可暂缓确认）。"""
    return checkpoint.submit_legacy_progress(
        payload.stream, payload.raw,
        acknowledged=payload.acknowledged, hold=payload.hold,
    )


@router.post("/records/{sequence}/hold")
def hold_position(sequence: int, payload: StreamMigrationPayload) -> dict[str, Any]:
    """暂缓一条待确认位点。"""
    return checkpoint.hold_position(payload.stream, sequence)


@router.post("/records/{sequence}/release")
def release_position(sequence: int, payload: StreamMigrationPayload) -> dict[str, Any]:
    """单独放行一条待确认位点。"""
    return checkpoint.release_position(payload.stream, sequence)


@router.post("/progress")
def commit_progress(payload: ProgressPayload) -> dict[str, Any]:
    """新版进度提交：保存带来源和格式版本的位点，并回报实际采用的格式。"""
    return checkpoint.commit_progress(
        payload.stream, payload.raw,
        source=payload.source, source_format=payload.source_format,
        format_version=payload.format_version,
        replay_from=payload.replay_from, replay_to=payload.replay_to,
    )


@router.post("/restore")
def restore_stream(payload: RestorePayload) -> dict[str, Any]:
    """恢复入口：按采用的已确认位点给出首个读取位置并派发回填。"""
    return checkpoint.restore_stream(
        payload.stream,
        replay_from=payload.replay_from,
        replay_to=payload.replay_to,
        dispatch_backfill=payload.dispatch_backfill,
    )


@router.post("/rollback")
def rollback_to_legacy(payload: RollbackPayload) -> dict[str, Any]:
    """按旧写入端契约回滚，重新读取已确认的旧版/新版位点。"""
    return checkpoint.rollback_to_legacy(payload.stream)
