"""冷热归档业务规则：消费快照和分区策略，执行冷数据分批归档。

归档以“冷分区归档批次表”为落库单元：批次边界、归档路径、可恢复标记、归档结果与
归档记录必须原子写入；缺日期分区只登记清单，不参与最新分区与保留期计算。
"""
from __future__ import annotations

import threading
from datetime import date, datetime
from typing import Any

from app.services.partition_boundary import build_partition_analysis
from app.store import ArchiveCommitError, store

MODULE = "cold_archive"
LIST_FIELDS = ["策略编号", "目标表名", "冷数据阈值", "归档路径", "压缩算法", "归档格式", "保留天数", "恢复超时"]
REQUIRED_FIELDS = ["策略编号", "目标表名"]
STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]
ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "停用策略": "已停用"}
NEGATIVE_ACTIONS = ["停用策略"]

# 同一策略的分区归档串行执行，保证同一冷分区被并发提交时只归档一次。
_ARCHIVE_LOCK = threading.RLock()


class ColdArchiveService:
    def __init__(self) -> None:
        # 扫描缓存挂在服务实例上：(表名, 表数据版本, 冷热阈值, 保留天数) -> 分析结果。
        # 归档失败后用同一个服务重试会直接复用上次扫描，不再重复扫描整表。
        self._scan_cache: dict[tuple[str, int, int, int], dict[str, Any]] = {}

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
            return None, f"归档策略 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于冷热归档可执行范围"
        target = ACTION_RULES[action]
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        store.save_version(MODULE, entry_id, entry)
        return entry, f"归档策略已{action}"

    # ---- 分区扫描与批次规划 ----

    def analyze_partitions(self, entry: dict[str, Any], *,
                           rows: list[dict[str, Any]] | None = None) -> dict[str, Any]:
        """分析目标表的冷分区边界与空日期清单。

        扫描结果按表数据版本缓存：归档失败后重试不会重复扫描整表。
        """
        table = str(entry.get("目标表名", ""))
        cold_threshold = _as_int(entry.get("冷数据阈值"), _as_int(entry.get("冷热阈值"), 30))
        retention_days = _as_int(entry.get("保留天数"), 365)
        # 缓存键带上阈值：同一张表的不同策略不会复用彼此的分析结果。
        cache_key = (table, store.table_version(table), cold_threshold, retention_days)
        scanned = rows is None
        if scanned:
            if cache_key in self._scan_cache:
                return dict(self._scan_cache[cache_key])
            rows = store.table_rows(table)
        analysis = build_partition_analysis(
            rows,
            cold_threshold_days=cold_threshold,
            retention_days=retention_days,
        )
        # 只缓存整表扫描结果；调用方传入的行子集不写回缓存，避免污染下次重试。
        if scanned:
            self._scan_cache[cache_key] = analysis
        return dict(analysis)

    def plan_batches(
        self,
        entry: dict[str, Any],
        *,
        batch_size: int = 5,
        boundary_mode: str = "冷热阈值",
        rows: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """按冷分区边界规划归档批次；已归档分区不会进入新计划，重试可安全重入。"""
        analysis = self.analyze_partitions(entry, rows=rows)
        table = str(entry.get("目标表名", ""))
        strategy_no = str(entry.get("策略编号", ""))
        base_path = str(entry.get("归档路径") or f"/archive/{table}").rstrip("/")
        pending, archived = self._split_archived(strategy_no, table, analysis["待归档分区"], base_path)

        conflict = self._detect_conflict(entry, analysis, pending)
        batches = self._chunk(pending, max(1, int(batch_size)), boundary_mode)

        if analysis["状态"] == "暂无":
            state, message = "暂无", "暂无分区数据，未生成归档批次"
        elif analysis["状态"] == "异常":
            state, message = "异常", analysis["说明"]
        elif analysis["未分区"]:
            state, message = "未分区", "目标表没有分区字段，按整表归档而不是按冷分区分批"
        elif not analysis["分区清单"] and analysis["空日期清单"]:
            state, message = "空态", "只有缺日期分区（空态），补日期前不参与保留期与归档计算"
        elif not analysis["待归档分区"]:
            state, message = "没有", "没有待归档冷分区，合法分区均在冷热阈值内"
        elif pending:
            state, message = "可归档", f"待归档冷分区 {len(pending)} 个，分 {len(batches)} 批"
        else:
            state, message = "已归档", "待归档冷分区均已有归档批次，不会重复归档"

        return {
            "状态": state,
            "说明": message,
            "策略编号": strategy_no,
            "目标表名": table,
            "边界类型": boundary_mode,
            "修改日期": analysis["as_of"],
            "冷热阈值天数": analysis["冷热阈值天数"],
            "保留天数": analysis["保留天数"],
            "待归档分区": analysis["待归档分区"],
            "未归档分区": [dict(item) for item in pending],
            "已归档分区": archived,
            "批次": batches,
            "边界分区": analysis["边界分区"],
            "保留期边界分区": analysis["保留期边界分区"],
            "最新分区": analysis["最新分区"],
            "空日期清单": analysis["空日期清单"],
            "空日期行数": analysis["空日期行数"],
            "空日期参与保留期计算": analysis["空日期参与保留期计算"],
            "冲突": conflict,
            "冲突优先级": "待确认",
        }

    def _split_archived(self, strategy_no: str, table: str,
                        cold_partitions: list[dict[str, Any]],
                        base_path: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """把冷分区拆成待归档与已归档两份；重试时已归档分区不会再进入批次。"""
        pending: list[dict[str, Any]] = []
        archived: list[dict[str, Any]] = []
        for item in sorted(cold_partitions, key=lambda p: p["partition"]):
            existing = store.find_archive_batch_by_partition(strategy_no, table, item["partition"])
            if existing:
                archived.append({
                    "partition": item["partition"],
                    "批次编号": existing.get("批次编号"),
                    "归档路径": existing.get("归档路径"),
                    "可恢复标记": existing.get("可恢复标记"),
                    "归档结果": existing.get("归档结果"),
                })
                continue
            pending.append(dict(item, 归档路径=f"{base_path}/{item['partition']}"))
        return pending, archived

    def _chunk(self, partitions: list[dict[str, Any]], size: int,
               boundary_mode: str) -> list[dict[str, Any]]:
        chunks: list[list[dict[str, Any]]] = [partitions[i:i + size] for i in range(0, len(partitions), size)]
        batches = []
        for index, chunk in enumerate(chunks, start=1):
            batches.append({
                "批次序号": index,
                "分区边界": self._boundary_of(chunk, boundary_mode),
                "分区清单": [
                    {"partition": item["partition"], "row_count": item["row_count"],
                     "归档路径": item["归档路径"], "可恢复标记": "可恢复"}
                    for item in chunk
                ],
                "行数": sum(int(item.get("row_count", 0)) for item in chunk),
                "归档路径": chunk[0]["归档路径"] if len(chunk) == 1 else f"{_parent_path(chunk[0]['归档路径'])}",
                "可恢复标记": "可恢复",
            })
        return batches

    def _boundary_of(self, chunk: list[dict[str, Any]], boundary_mode: str) -> dict[str, Any]:
        ordered = sorted(chunk, key=lambda item: item["partition"])
        first, last = ordered[0], ordered[-1]
        boundary = {
            "类型": boundary_mode,
            "起始分区": first["partition"],
            "结束分区": last["partition"],
            "分区数量": len(ordered),
            "行数": sum(int(item.get("row_count", 0)) for item in ordered),
            # partition/partitions 供批次表跨模块按分区查找恢复路径。
            "partition": first["partition"],
            "partitions": [item["partition"] for item in ordered],
            "boundary_kind": "冷热阈值边界" if boundary_mode == "冷热阈值" else "分区大小边界",
        }
        if boundary_mode == "分区大小":
            boundary["最大分区"] = max(ordered, key=lambda item: item["row_count"])["partition"]
            boundary["最小分区"] = min(ordered, key=lambda item: item["row_count"])["partition"]
        return boundary

    def _detect_conflict(self, entry: dict[str, Any], analysis: dict[str, Any],
                         pending: list[dict[str, Any]]) -> dict[str, Any] | None:
        """缺日期分区与保留期边界分区同时出现时登记冲突，两种边界给出各自的结果视图。"""
        empty_list = analysis.get("空日期清单") or []
        retention_boundary = analysis.get("保留期边界分区")
        if not empty_list:
            return None
        if not retention_boundary and not pending:
            return None
        boundary_result = {
            "边界类型": "冷热阈值边界",
            "边界分区": (analysis.get("边界分区") or {}).get("partition"),
            "保留期边界分区": (retention_boundary or {}).get("partition"),
            "归档结果": "待归档" if pending else "已归档",
            "归档分区数": len(pending),
        }
        size_result = {
            "边界类型": "分区大小边界",
            "大小分区归档结果": sorted(
                ({"partition": item["partition"], "row_count": item["row_count"]}
                 for item in pending),
                key=lambda item: item["row_count"], reverse=True),
            "归档结果": "待归档" if pending else "已归档",
        }
        return {
            "类型": "缺日期分区与保留期边界分区冲突",
            "缺日期分区": [item["partition"] for item in empty_list],
            "保留期边界分区": (retention_boundary or {}).get("partition"),
            "边界分区归档结果": boundary_result,
            "大小分区归档结果": size_result,
            "待修复集合": {
                "空日期清单": empty_list,
                "说明": "缺日期分区需补齐 dt 后才纳入保留期与归档计算",
            },
            "优先级": "待确认",
            "处理顺序": "待确认",
        }

    # ---- 执行归档 ----

    def archive_cold_partitions(
        self,
        entry_id: int,
        *,
        batch_size: int = 5,
        boundary_mode: str = "冷热阈值",
        max_batches: int | None = None,
        snapshot_id: str | None = None,
    ) -> dict[str, Any]:
        """分批归档冷分区并写入批次表；覆盖空态/暂无/没有/失败/异常/中断并可安全重试。"""
        entry = self.get_entry(entry_id)
        if not entry:
            return {
                "ok": False, "状态": "异常", "archived": 0, "batches": [],
                "message": f"归档策略 {entry_id} 不存在或已归档",
                "retryable": False,
            }

        try:
            with _ARCHIVE_LOCK:
                return self._archive_locked(
                    entry, batch_size=batch_size, boundary_mode=boundary_mode,
                    max_batches=max_batches, snapshot_id=snapshot_id)
        except ArchiveCommitError as exc:
            # 落库校验失败：该批次与归档记录都未写入，边界与空态保持原样。
            return {
                "ok": False, "状态": "失败", "archived": 0, "batches": [],
                "message": f"归档批次落库失败，已回滚：{exc}；边界与空态未改变，可直接重试",
                "retryable": True,
            }
        except Exception as exc:  # pragma: no cover - 防御：任何异常都转成可读说明
            return {
                "ok": False, "状态": "异常", "archived": 0, "batches": [],
                "message": f"归档执行异常：{exc}；未写入的批次可通过重试补齐",
                "retryable": True,
            }

    def _archive_locked(self, entry: dict[str, Any], *, batch_size: int,
                        boundary_mode: str, max_batches: int | None,
                        snapshot_id: str | None) -> dict[str, Any]:
        plan = self.plan_batches(entry, batch_size=batch_size, boundary_mode=boundary_mode)
        state = plan["状态"]
        if state in ("暂无", "没有", "空态", "未分区", "异常"):
            return {
                "ok": state != "异常", "状态": state, "archived": 0, "batches": [],
                "message": plan["说明"], "retryable": False,
                "边界分区": plan["边界分区"],
                "空日期清单": plan["空日期清单"],
                "空日期参与保留期计算": plan["空日期参与保留期计算"],
                "冲突": plan["冲突"],
            }
        if not plan["批次"]:
            return {
                "ok": True, "状态": "已归档", "archived": 0, "batches": [],
                "message": "待归档冷分区均已有归档批次，本次未重复归档",
                "retryable": False, "边界分区": plan["边界分区"],
                "空日期清单": plan["空日期清单"], "冲突": plan["冲突"],
            }

        planned = plan["批次"]
        interrupted = max_batches is not None and len(planned) > max_batches
        if interrupted:
            planned = planned[:max(0, max_batches)]

        # 本批全部提交完成后冲突结果才算“已归档”；中断时保持“待归档”等待重试。
        conflict = plan["冲突"]
        final_conflict = (self._mark_conflict_archived(conflict)
                          if conflict is not None and not interrupted else conflict)

        committed: list[dict[str, Any]] = []
        rows = store.table_rows(str(entry.get("目标表名", "")))
        for batch in planned:
            record = self._build_record(entry, batch, rows, snapshot_id)
            first_partition = batch["分区清单"][0]["partition"]
            # 锁内二次确认：并发提交已写入同一分区时不再重复归档。
            if store.find_archive_batch_by_partition(
                    str(entry.get("策略编号", "")), str(entry.get("目标表名", "")), first_partition):
                continue
            stored = store.commit_archive_batch(
                {
                    "批次编号": self._next_batch_no(entry),
                    "策略编号": entry.get("策略编号", ""),
                    "目标表名": entry.get("目标表名", ""),
                    "分区边界": batch["分区边界"],
                    "归档路径": batch["归档路径"],
                    "可恢复标记": batch["可恢复标记"],
                    "归档结果": "已归档",
                    "分区清单": batch["分区清单"],
                    "行数": batch["行数"],
                    "归档记录编号": record["archive_id"],
                    "归档时间": record["archived_at"],
                    "边界类型": boundary_mode,
                    "冲突": final_conflict,
                },
                record,
            )
            committed.append(stored)

        if conflict is not None and not interrupted:
            # 中断后重试补齐最后一批时，前面批次的冲突结果也要跟着更新。
            for batch in store.list_archive_batches():
                if (batch.get("目标表名") == entry.get("目标表名")
                        and batch.get("策略编号") == entry.get("策略编号")
                        and batch.get("冲突")):
                    store.update_archive_batch(batch["批次编号"], 冲突=final_conflict)

        remaining = self.plan_batches(entry, batch_size=batch_size,
                                      boundary_mode=boundary_mode)["未归档分区"]
        archived = len(committed)
        if interrupted:
            return {
                "ok": False, "状态": "中断", "archived": archived, "batches": committed,
                "message": (f"归档在 {archived} 批后中断，剩余 {len(remaining)} 个冷分区未归档；"
                            f"重试会跳过已归档分区继续"),
                "retryable": True,
                "边界分区": plan["边界分区"], "空日期清单": plan["空日期清单"],
                "冲突": conflict, "待重试分区": remaining,
            }
        return {
            "ok": True, "状态": "已归档", "archived": archived, "batches": committed,
            "message": (f"已分批归档 {archived} 批、{sum(b['行数'] for b in committed)} 行；"
                        f"空日期分区未进入保留期计算"),
            "retryable": False,
            "边界分区": plan["边界分区"], "空日期清单": plan["空日期清单"],
            "空日期参与保留期计算": plan["空日期参与保留期计算"],
            "冲突": final_conflict, "待重试分区": remaining,
        }

    @staticmethod
    def _mark_conflict_archived(conflict: dict[str, Any]) -> dict[str, Any]:
        resolved = dict(conflict)
        boundary_result = dict(conflict.get("边界分区归档结果") or {})
        boundary_result["归档结果"] = "已归档"
        size_result = dict(conflict.get("大小分区归档结果") or {})
        size_result["归档结果"] = "已归档"
        resolved["边界分区归档结果"] = boundary_result
        resolved["大小分区归档结果"] = size_result
        return resolved

    def _build_record(self, entry: dict[str, Any], batch: dict[str, Any],
                      rows: list[dict[str, Any]], snapshot_id: str | None) -> dict[str, Any]:
        partitions = {item["partition"] for item in batch["分区清单"]}
        if snapshot_id:
            snap = store.get_snapshot_data(snapshot_id) or {}
            selected = [row for row in snap.get("rows", []) if str(row.get("dt", "")).strip() in partitions]
            row_count = len(selected) or batch["行数"]
        else:
            row_count = batch["行数"] or sum(
                1 for row in rows if str(row.get("dt", "")).strip() in partitions)
        return {
            "archive_id": f"ARC-{entry.get('id')}-{datetime.now().strftime('%Y%m%d%H%M%S%f')}",
            "table": entry.get("目标表名", ""),
            "row_count": row_count,
            "path": batch["归档路径"],
            "format": entry.get("归档格式", "PARQUET"),
            "compressed_by": entry.get("压缩算法", "ZSTD"),
            "archived_at": datetime.now().isoformat(),
            "partitions": sorted(partitions),
            "partition_boundary": batch["分区边界"],
            "restorable": True,
            "snapshot_id": snapshot_id or "",
        }

    def _next_batch_no(self, entry: dict[str, Any]) -> str:
        prefix = f"BAT-{entry.get('策略编号') or entry.get('id')}-{date.today().strftime('%Y%m%d')}"
        existing = [b for b in store.list_archive_batches() if str(b.get("批次编号", "")).startswith(prefix)]
        return f"{prefix}-{len(existing) + 1:03d}"

    # ---- 跨模块读取与恢复 ----

    def list_archive_batches(self, *, strategy_no: str | None = None,
                             table: str | None = None) -> list[dict[str, Any]]:
        """冷分区归档批次表：随归档一起落库的边界、路径与可恢复标记。"""
        return store.list_archive_batches(strategy_no=strategy_no, table=table)

    def find_recovery_path(self, table: str, partition: str) -> dict[str, Any] | None:
        """按分区查找归档恢复路径；缺日期分区不会出现在批次表里。"""
        for batch in store.list_archive_batches(table=table):
            for item in batch.get("分区清单", []):
                if item.get("partition") == partition:
                    return {
                        "批次编号": batch.get("批次编号"),
                        "分区边界": batch.get("分区边界"),
                        "归档路径": item.get("归档路径") or batch.get("归档路径"),
                        "可恢复标记": item.get("可恢复标记") or batch.get("可恢复标记"),
                        "归档结果": batch.get("归档结果"),
                    }
        return None

    def restore_batch(self, batch_no: str) -> dict[str, Any]:
        batch = store.restore_archive_batch(batch_no)
        if batch is None:
            return {"ok": False, "message": f"归档批次 {batch_no} 不存在或不可恢复"}
        return {"ok": True, "message": f"归档批次 {batch_no} 已按批次表恢复", "batch": batch}

    def execute_archive(self, entry_id: int, snapshot_id: str | None = None) -> dict[str, Any]:
        """执行整表归档（兼容入口）：边界、路径与归档记录原子落库，可恢复标记随批次保存。"""
        entry = self.get_entry(entry_id)
        if not entry:
            return {"ok": False, "message": "归档策略不存在"}
        table = str(entry.get("目标表名", ""))
        rows: list[dict[str, Any]] = []
        if snapshot_id:
            snap = store.get_snapshot_data(snapshot_id)
            if snap:
                rows = snap.get("rows", [])
        if not rows:
            rows = store.table_rows(table)
        if not rows:
            return {"ok": False, "message": f"目标表 {table} 无数据可归档"}

        analysis = self.analyze_partitions(entry, rows=rows)
        if not analysis["未分区"] and _archive_row_count(rows, analysis) == 0:
            return {
                "ok": True, "状态": analysis["状态"], "archived": 0, "batches": [],
                "message": (f"目标表 {table} 只有缺日期分区，空态不生成归档批次；"
                            f"补齐 dt 后重试"),
                "retryable": True, "空日期清单": analysis["空日期清单"],
                "空日期参与保留期计算": False,
            }
        base_path = str(entry.get("归档路径") or f"/archive/{table}").rstrip("/")
        valid_partitions = [item["partition"] for item in analysis["分区清单"]]
        if analysis["边界分区"]:
            boundary = analysis["边界分区"]
        elif analysis["未分区"]:
            # 未分区表没有分区边界，整表归档并登记整表边界。
            boundary = {
                "类型": "整表边界", "partition": "", "partitions": [],
                "起始分区": "", "结束分区": "", "分区数量": 0, "行数": len(rows),
                "boundary_kind": "整表边界",
            }
        else:
            # 只有缺日期分区时没有可归档边界：空态进入批次表但不伪造分区。
            boundary = {
                "类型": "空态边界", "partition": "", "partitions": [],
                "起始分区": "", "结束分区": "", "分区数量": 0, "行数": 0,
                "boundary_kind": "空态边界",
            }
        archive_rows = _archive_row_count(rows, analysis)
        record = {
            "archive_id": f"ARC-{entry_id}-{datetime.now().strftime('%Y%m%d%H%M%S%f')}",
            "table": table,
            # 缺日期的行数不计入已归档行数，避免空日期分区进入归档与保留期口径。
            "row_count": archive_rows,
            "path": base_path,
            "format": entry.get("归档格式", "PARQUET"),
            "compressed_by": entry.get("压缩算法", "ZSTD"),
            "archived_at": datetime.now().isoformat(),
            "partitions": valid_partitions,
            "partition_boundary": boundary,
            "restorable": True,
            "snapshot_id": snapshot_id or "",
        }
        try:
            store.commit_archive_batch({
                "批次编号": self._next_batch_no(entry),
                "策略编号": entry.get("策略编号", ""),
                "目标表名": table,
                "分区边界": boundary,
                "归档路径": base_path,
                "可恢复标记": "可恢复",
                "归档结果": "已归档",
                "分区清单": [
                    {"partition": p, "归档路径": f"{base_path}/{p}",
                     "可恢复标记": "可恢复"} for p in valid_partitions
                ],
                "行数": archive_rows,
                "归档记录编号": record["archive_id"],
                "归档时间": record["archived_at"],
                "边界类型": "整表边界",
                "空日期清单": analysis["空日期清单"],
                "冲突": None,
            }, record)
        except ArchiveCommitError as exc:
            return {
                "ok": False,
                "message": f"归档批次落库失败，已回滚：{exc}；边界与空态未改变，可直接重试",
                "retryable": True,
            }
        return {
            "ok": True,
            "message": f"已归档{archive_rows}行；缺日期分区未进入归档与保留期计算",
            "record": record,
            "空日期清单": analysis["空日期清单"],
            "空日期参与保留期计算": analysis["空日期参与保留期计算"],
        }


def _as_int(value: Any, default: int) -> int:
    try:
        text = str(value).strip()
        return int(text) if text else default
    except (TypeError, ValueError):
        return default


def _parent_path(path: str) -> str:
    return path.rsplit("/", 1)[0] if "/" in path else path


def _archive_row_count(rows: list[dict[str, Any]], analysis: dict[str, Any]) -> int:
    """合法分区行数；缺日期行不计入归档口径，未分区表按整表计入。"""
    if analysis.get("未分区"):
        return len(rows)
    empty_rows = int(analysis.get("空日期行数", 0) or 0)
    return max(0, len(rows) - empty_rows)
