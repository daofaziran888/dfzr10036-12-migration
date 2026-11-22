"""保存位点迁移的本地可重启夹具：建库、导入种子旧值、按诉求回放场景。

用法：
    python -m app.scripts.checkpoint_fixture init            # 建库并导入种子旧值
    python -m app.scripts.checkpoint_fixture reset           # 删库后重建
    python -m app.scripts.checkpoint_fixture state [stream]  # 查看已落库位点
    python -m app.scripts.checkpoint_fixture scenario        # 跑一遍订单流+PG流场景
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

from app.services.checkpoint import CheckpointService
from app.services.checkpoint_positions import DEFAULT_STATE_PATH, PositionStore


def _print(title: str, payload: Any) -> None:
    print(f"\n== {title} ==")
    print(json.dumps(payload, ensure_ascii=False, indent=2))


def _store(path: str | None) -> PositionStore:
    return PositionStore(path or os.getenv("APP_STATE_DB") or DEFAULT_STATE_PATH)


def cmd_init(path: str | None) -> int:
    positions = _store(path)
    service = CheckpointService(positions)
    result = service.migrate_legacy_positions()
    _print("存量检查点迁移（读取旧值）", result)
    return 0 if result["ok"] else 1


def cmd_reset(path: str | None) -> int:
    target = Path(path or os.getenv("APP_STATE_DB") or DEFAULT_STATE_PATH)
    for suffix in ("", "-wal", "-shm"):
        candidate = Path(str(target) + suffix)
        if candidate.exists():
            candidate.unlink()
    return cmd_init(path)


def cmd_state(path: str | None, stream: str | None) -> int:
    positions = _store(path)
    service = CheckpointService(positions)
    _print("已落库保存位点", service.list_saved_positions(stream))
    return 0


def cmd_scenario(path: str | None) -> int:
    positions = _store(path)
    service = CheckpointService(positions)
    service.migrate_legacy_positions()

    # ---- 订单流：迁移读取旧值后暂停 ----
    _print("订单流开始迁移（读取旧值后暂停）",
           service.migrate("order_events_cdc"))
    _print("暂停期间恢复请求 #1（确认前）",
           service.restore_stream("order_events_cdc"))

    # 新版进度先记录实际采用的 891200；旧写入端随后提交 891250 并暂缓确认。
    service.commit_progress("order_events_cdc", "binlog.001234:891200")
    held = service.submit_legacy_progress(
        "order_events_cdc", "binlog.001234:891250", hold=True)
    _print("旧端提交 891250（暂缓确认）", {
        "sequence": held["record"]["sequence"],
        "raw": held["record"]["raw"],
        "adopted_position": held["adopted_position"],
    })
    _print("暂缓期间的恢复请求",
           service.restore_stream("order_events_cdc"))

    # 单独放行 891250，再做一次恢复请求。
    _print("单独放行 891250",
           service.release_position("order_events_cdc", held["record"]["sequence"]))
    restore = service.restore_stream("order_events_cdc")
    _print("确认后恢复请求 #2", {
        "adopted_position": restore["adopted_position"],
        "first_read_position": restore["first_read_position"],
        "migration_state": restore["migration_state"],
        "backfill": restore["backfill"],
    })

    # 回放成功后再次提交进度：同一范围不应再次进入回填。
    committed = service.commit_progress(
        "order_events_cdc", "binlog.001234:892000",
        replay_from="binlog.001234:891251", replay_to="binlog.001234:892000",
    )
    _print("回放后的再次提交", {
        "applied_format": committed["applied_format"],
        "applied_position": committed["applied_position"],
        "replay": committed["replay"],
    })
    again = service.restore_stream("order_events_cdc")
    _print("再次恢复（不应重复回填）", {
        "adopted_position": again["adopted_position"],
        "first_read_position": again["first_read_position"],
        "backfill": again["backfill"],
    })

    # ---- PG 流：坏串留档 + 回放后再次提交 ----
    _print("PG 流坏串提交",
           service.submit_legacy_progress("payment_log_cdc", "0/3E82A00-typo"))
    pg = service.restore_stream("payment_log_cdc")
    _print("PG 流恢复", {
        "adopted_position": pg["adopted_position"],
        "first_read_position": pg["first_read_position"],
        "quarantined": pg["quarantined"],
    })
    pg_commit = service.commit_progress(
        "payment_log_cdc", "pg_lsn.0/3F00000",
        replay_from="pg_lsn.0/3E82A01", replay_to="pg_lsn.0/3F00000",
    )
    _print("PG 流回放后的再次提交", {
        "applied_format": pg_commit["applied_format"],
        "applied_position": pg_commit["applied_position"],
        "replay": pg_commit["replay"],
    })

    # ---- 回滚到旧端契约 ----
    _print("订单流回滚", service.rollback_to_legacy("order_events_cdc"))
    _print("回滚后重新读取", {
        "streams": positions.stream_state("order_events_cdc"),
    })
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default=None, help="覆盖 APP_STATE_DB / 默认状态库路径")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("init")
    sub.add_parser("reset")
    state = sub.add_parser("state")
    state.add_argument("stream", nargs="?")
    sub.add_parser("scenario")
    args = parser.parse_args(argv)
    if args.command == "init":
        return cmd_init(args.db)
    if args.command == "reset":
        return cmd_reset(args.db)
    if args.command == "state":
        return cmd_state(args.db, args.stream)
    if args.command == "scenario":
        return cmd_scenario(args.db)
    return 2


if __name__ == "__main__":
    sys.exit(main())
