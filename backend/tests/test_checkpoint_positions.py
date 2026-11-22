"""保存位点迁移的回归测试：用临时库覆盖迁移/确认重叠/回放/回滚。

运行：python -m unittest discover -s tests -v
"""
from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

from app.services.checkpoint import CheckpointService
from app.services.checkpoint_positions import PositionStore
from app.services.positions import (
    PositionError,
    compare,
    parse_position,
)

ORDER = "order_events_cdc"
PAY = "payment_log_cdc"


class PositionParsingTest(unittest.TestCase):
    def test_mysql_offset_advances_by_one_byte(self) -> None:
        position = parse_position("binlog.001234:891200")
        self.assertEqual(position.source, "mysql")
        self.assertEqual(position.source_format, "mysql-binlog-file-offset")
        self.assertEqual(position.payload["file_index"], 1234)
        self.assertEqual(position.next().render(), "binlog.001234:891201")

    def test_postgres_lsn_renders_and_advances(self) -> None:
        position = parse_position("pg_lsn.0/3E82A00")
        self.assertEqual(position.source, "postgresql")
        self.assertEqual(position.render(), "pg_lsn.0/3E82A00")
        self.assertEqual(position.next().render(), "pg_lsn.0/3E82A01")

    def test_cross_source_comparison_rejected(self) -> None:
        with self.assertRaises(PositionError):
            compare(parse_position("binlog.001234:891200"),
                    parse_position("pg_lsn.0/3E82A00"))

    def test_unrecognized_text_raises_with_reason(self) -> None:
        with self.assertRaises(PositionError) as caught:
            parse_position("checkpoint_after_overflow_3")
        self.assertEqual(caught.exception.reason, "unrecognized-position")


class SavedPositionStoreTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Path(self.tmp.name) / "positions.sqlite3"
        self.store = PositionStore(self.db)
        self.service = CheckpointService(self.store)
        # 迁移读的是模块级 seed，测试里先复位，避免受其它用例登记影响。
        from app.seed import SEED_ROWS
        from app.store import store
        store._tables["checkpoint"] = [dict(row) for row in SEED_ROWS["checkpoint"]]
        self.service.migrate_legacy_positions()
        self.service.migrate(ORDER)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_restore_before_and_after_confirmation(self) -> None:
        before = self.service.restore_stream(ORDER)
        self.assertEqual(before["adopted_position"], "binlog.001234:891200")
        self.assertEqual(before["first_read_position"], "binlog.001234:891201")
        self.assertEqual(before["migration_state"], "paused")

        held = self.service.submit_legacy_progress(
            ORDER, "binlog.001234:891250", hold=True)
        self.assertEqual(held["adopted_position"], "binlog.001234:891200")

        while_held = self.service.restore_stream(ORDER)
        self.assertEqual(while_held["adopted_position"], "binlog.001234:891200")

        self.service.release_position(ORDER, held["record"]["sequence"])
        after = self.service.restore_stream(ORDER)
        self.assertEqual(after["adopted_position"], "binlog.001234:891250")
        self.assertEqual(after["first_read_position"], "binlog.001234:891251")

    def test_successful_replay_is_not_reentered(self) -> None:
        self.service.commit_progress(
            ORDER, "binlog.001234:892000",
            replay_from="binlog.001234:891201",
            replay_to="binlog.001234:892000",
        )
        again = self.service.restore_stream(ORDER)
        self.assertFalse(again["backfill"]["dispatched"])
        self.assertFalse(again["reentered"])
        self.assertFalse(again["read_range"]["includes_adopted"])
        self.assertEqual(again["replayed_ranges"][0]["from"], "binlog.001234:891201")

    def test_progress_commit_reports_applied_format(self) -> None:
        result = self.service.commit_progress(ORDER, "binlog.001234:891300")
        self.assertEqual(result["applied_format"]["format"], "saved-position")
        self.assertEqual(result["applied_format"]["format_version"], 1)
        self.assertEqual(result["applied_format"]["source"], "mysql")
        self.assertEqual(
            result["applied_format"]["source_format"], "mysql-binlog-file-offset")
        self.assertEqual(result["applied_position"], "binlog.001234:891300")

    def test_bad_format_version_rejected(self) -> None:
        result = self.service.commit_progress(
            ORDER, "binlog.001234:891300", format_version=9)
        self.assertFalse(result["ok"])
        self.assertEqual(result["reason"], "unsupported-format-version")

    def test_bad_postgres_text_archived_not_used(self) -> None:
        self.service.submit_legacy_progress(PAY, "0/3E82A00-typo")
        state = self.store.stream_state(PAY)
        self.assertEqual(state["adopted_position"], "pg_lsn.0/3E82A00")
        self.assertEqual(state["quarantined"][0]["raw"], "0/3E82A00-typo")
        restore = self.service.restore_stream(PAY)
        self.assertEqual(restore["first_read_position"], "pg_lsn.0/3E82A01")

    def test_restart_rereads_confirmed_positions(self) -> None:
        held = self.service.submit_legacy_progress(
            ORDER, "binlog.001234:891250", hold=True)
        self.service.release_position(ORDER, held["record"]["sequence"])

        restarted = CheckpointService(PositionStore(self.db))
        state = restarted.positions.stream_state(ORDER)
        self.assertEqual(state["adopted_position"], "binlog.001234:891250")
        positions = {r["position"] for r in state["records"] if r["acknowledged"]}
        self.assertEqual(positions, {"binlog.001234:891200", "binlog.001234:891250"})

    def test_rollback_uses_legacy_text_and_rereads(self) -> None:
        rollback = self.service.rollback_to_legacy(ORDER)
        self.assertEqual(rollback["migration_state"], "rolled-back-legacy")
        self.assertEqual(rollback["legacy_text"], "binlog.001234:891200")
        self.assertEqual(rollback["record"]["origin"], "legacy-text")
        restarted = CheckpointService(PositionStore(self.db))
        state = restarted.positions.stream_state(ORDER)
        self.assertEqual(state["adopted_position"], "binlog.001234:891200")
        self.assertTrue(all(r["acknowledged"] for r in state["records"]))


if __name__ == "__main__":
    unittest.main()
