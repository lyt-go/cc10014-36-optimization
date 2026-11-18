"""分区策略业务规则：管理表的分区策略，支持冷热分区判定。"""
from __future__ import annotations

from typing import Any

from app.services.partition_boundary import build_partition_analysis
from app.store import store

MODULE = "partition_mgr"
LIST_FIELDS = ["策略编号", "目标表名", "分区字段", "分区粒度", "保留天数", "冷热阈值", "预建数量", "目录规则"]
REQUIRED_FIELDS = ["策略编号", "目标表名"]
STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]
ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "停用策略": "已停用"}
NEGATIVE_ACTIONS = ["停用策略"]


class PartitionMgrService:
    def list_entries(
        self,
        *,
        filters: dict[str, str] | None = None,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = store.rows(MODULE)
        for field, expected in (filters or {}).items():
            rows = [row for row in rows if expected.casefold() in str(row.get(field, "")).casefold()]
        if keyword:
            rows = [row for row in rows if keyword.casefold() in str(row.get("策略编号", "")).casefold()]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = (page - 1) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update({field: values.get(field) for field in LIST_FIELDS})
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"分区策略 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于分区治理可执行范围"
        target = ACTION_RULES[action]
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        store.save_version(MODULE, entry_id, entry)
        return entry, f"分区策略已{action}"

    def get_cold_partitions(self, entry_id: int) -> list[dict[str, Any]]:
        """返回该策略下应归档的冷分区列表；缺日期分区不参与判定。"""
        return self.analyze_partitions(entry_id)["待归档分区"]

    def analyze_partitions(self, entry_id: int) -> dict[str, Any]:
        """按分区边界分析表数据：冷/热、归档边界、保留期边界与缺日期清单。

        缺日期分区只进入空日期清单，不参与“最新分区”和保留期计算，避免保留策略误判。
        """
        entry = self.get_entry(entry_id)
        if not entry:
            return {
                "状态": "异常",
                "说明": f"分区策略 {entry_id} 不存在或已归档",
                "分区清单": [], "待归档分区": [], "边界分区": None,
                "保留期边界分区": None, "最新分区": None, "空日期清单": [],
                "空日期行数": 0, "空日期数量": 0, "空日期参与保留期计算": False,
            }
        threshold = _as_int(entry.get("冷热阈值"), 30)
        retention = _as_int(entry.get("保留天数"), 365)
        table = str(entry.get("目标表名", ""))
        analysis = build_partition_analysis(
            store.table_rows(table),
            cold_threshold_days=threshold,
            retention_days=retention,
        )
        analysis["策略编号"] = entry.get("策略编号", "")
        analysis["目标表名"] = table
        return analysis


def _as_int(value: Any, default: int) -> int:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return default
