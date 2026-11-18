"""分区边界分析：统一解析分区日期、识别缺日期分区与冷分区边界。

缺日期分区（dt 为空、只有空白、或无法解析成日期）不参与“最新分区”与保留期计算，
否则保留策略会把空日期当成最新分区，导致合法冷分区被提前归档。
"""
from __future__ import annotations

from datetime import date, datetime
from typing import Any

EMPTY_DATE = "缺日期"


def parse_partition_date(value: Any) -> date | None:
    """把分区值解析成日期；空值、空白与无法识别的文本都返回 None。"""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _empty_partition_label(value: Any) -> str:
    if value is None:
        return "（空）"
    text = str(value)
    return text.strip() or "（空）"


def build_partition_analysis(
    rows: list[dict[str, Any]],
    *,
    cold_threshold_days: int = 30,
    retention_days: int = 365,
    as_of: date | None = None,
) -> dict[str, Any]:
    """按分区聚合行数并判定冷/热、归档边界与缺日期清单。

    边界分区取冷分区中距冷热阈值最近的一个（最靠近热区的冷分区），
    它也是批次里需要单独登记归档结果的分区；缺日期分区只进入清单，不进入边界计算。
    """
    as_of = as_of or date.today()
    cold_threshold_days = max(0, int(cold_threshold_days))
    retention_days = max(0, int(retention_days))
    # 整张表没有 dt 字段时是未分区表，按整表处理；dt 字段存在但为空才是缺日期分区。
    unpartitioned = bool(rows) and not any("dt" in row for row in rows)

    valid: dict[str, dict[str, Any]] = {}
    empty: dict[str, dict[str, Any]] = {}
    empty_rows = 0
    for row in rows:
        parsed = parse_partition_date(row.get("dt"))
        if parsed is None:
            label = _empty_partition_label(row.get("dt"))
            item = empty.setdefault(label, {"partition": label, "row_count": 0, "reason": EMPTY_DATE})
            item["row_count"] += 1
            empty_rows += 1
            continue
        key = parsed.isoformat()
        item = valid.setdefault(key, {"partition": key, "date": parsed, "row_count": 0})
        item["row_count"] += 1

    partitions: list[dict[str, Any]] = []
    for key, item in valid.items():
        age_days = (as_of - item["date"]).days
        partitions.append({
            "partition": key,
            "row_count": item["row_count"],
            "age_days": age_days,
            "is_cold": age_days >= cold_threshold_days,
            "within_retention": age_days <= retention_days,
            "at_retention_boundary": age_days == retention_days,
        })
    partitions.sort(key=lambda item: item["partition"], reverse=True)

    empty_list = [
        {
            "partition": item["partition"],
            "row_count": item["row_count"],
            "reason": EMPTY_DATE,
            "参与保留期计算": False,
        }
        for item in sorted(empty.values(), key=lambda item: item["partition"])
    ]

    cold_partitions = [item for item in partitions if item["is_cold"]]
    latest_partition = partitions[0]["partition"] if partitions else None

    boundary: dict[str, Any] | None = None
    if cold_partitions:
        # 冷分区里年龄最小（最靠近热区）的分区就是冷热边界。
        boundary = dict(min(cold_partitions, key=lambda item: item["age_days"]))
        boundary["boundary_kind"] = "冷热阈值边界"
        boundary["threshold_days"] = cold_threshold_days

    retention_boundary: dict[str, Any] | None = None
    if partitions:
        retention_boundary = dict(min(
            partitions, key=lambda item: abs(item["age_days"] - retention_days)))
        retention_boundary["boundary_kind"] = "保留期边界"
        retention_boundary["retention_days"] = retention_days

    if not rows:
        state = "暂无"
        message = "暂无分区数据"
    elif unpartitioned:
        state = "未分区"
        message = "目标表没有分区字段，按整表归档而不是按冷分区分批"
    elif not partitions and empty_list:
        state = "空态"
        message = "只有缺日期分区（空态），补日期前不参与保留期与归档计算"
    elif not cold_partitions:
        state = "没有"
        message = "没有待归档冷分区，合法分区均在冷热阈值内"
    else:
        state = "可归档"
        message = f"待归档冷分区 {len(cold_partitions)} 个"

    return {
        "状态": state,
        "说明": message,
        "as_of": as_of.isoformat(),
        "未分区": unpartitioned,
        "冷热阈值天数": cold_threshold_days,
        "保留天数": retention_days,
        "分区清单": [
            {key: value for key, value in item.items() if key != "date"} for item in partitions
        ],
        "待归档分区": [
            {key: value for key, value in item.items() if key != "date"}
            for item in sorted(cold_partitions, key=lambda item: item["partition"])
        ],
        "边界分区": boundary,
        "保留期边界分区": retention_boundary,
        "最新分区": latest_partition,
        "空日期清单": empty_list,
        "空日期行数": empty_rows,
        "空日期数量": len(empty_list),
        "空日期参与保留期计算": False,
    }
