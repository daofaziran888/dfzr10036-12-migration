"""水位管理服务：水位推进后触发checkpoint保存和lag_monitor延迟计算。"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class WatermarkMgrService(BaseModuleService):
    MODULE = "watermark_mgr"
    ENTITY = "水位线"
    OPERATION_TERM = "水位"
    LIST_FIELDS = ["水位编号", "数据流", "当前位点", "提交位点", "延迟行数", "水位类型", "最后推进", "追赶进度"]
    REQUIRED_FIELDS = ["水位编号", "数据流", "当前位点"]
    STATUS_ORDER = ["推进中", "已追上", "水位延迟", "已停滞"]
    ACTION_RULES = {"提交水位": "已追上", "标记延迟": "水位延迟", "标记停滞": "已停滞"}
    NEGATIVE_ACTIONS = []

    def trigger_checkpoint(self, entry_id: int) -> dict[str, Any] | None:
        """水位提交后，保存checkpoint供断点恢复。

        数据流链路: watermark_mgr -> checkpoint
        """
        entry = self.get_entry(entry_id)
        if entry is None or entry.get("status") != "已追上":
            return None

        cp_rows = store.rows("checkpoint")
        tag = str(entry.get("数据流", ""))
        existing = [r for r in cp_rows if str(r.get("数据流", "")) == tag]
        if existing:
            existing[0]["保存位点"] = str(entry.get("提交位点", ""))
            existing[0].pop("位点来源", None)
            existing[0].pop("位点格式", None)
            existing[0].pop("格式版本", None)
            existing[0].pop("位点迁移", None)
            existing[0]["保存时间"] = "2026-11-18 10:35:00"
            existing[0]["status"] = "已保存"
            existing[0]["pending"] = True
            existing[0]["abnormal"] = False
            return existing[0]

        new_id = max((int(r.get("id", 0)) for r in cp_rows), default=0) + 1
        cp = {
            "id": new_id,
            "检查点编号": f"CHEC-{new_id:04d}",
            "数据流": tag,
            "保存位点": str(entry.get("提交位点", "")),
            "保存时间": "2026-11-18 10:35:00",
            "恢复状态": "就绪",
            "丢失行数": 0,
            "回放代价": "预估3.2s",
            "恢复耗时": "未触发",
            "status": "已保存",
            "pending": True,
            "abnormal": False,
        }
        cp_rows.append(cp)
        return cp

    def trigger_lag_update(self, entry_id: int) -> str:
        """水位延迟时，更新lag_monitor中的延迟指标。

        数据流链路: watermark_mgr(水位延迟) -> lag_monitor
        """
        entry = self.get_entry(entry_id)
        if entry is None or entry.get("status") != "水位延迟":
            return "skip: 非延迟状态"

        tag = str(entry.get("数据流", ""))
        for lm in store.rows("lag_monitor"):
            if str(lm.get("数据流", "")) == tag:
                lm["status"] = "严重滞后"
                lm["延迟秒数"] = str(entry.get("延迟行数", 1200))
                lm["处理位点"] = str(entry.get("当前位点", ""))
                lm["最近采样"] = "2026-11-18 10:36:00"
                return f"updated lag_monitor for {tag} -> 严重滞后"
        return f"no lag_monitor found for {tag}"