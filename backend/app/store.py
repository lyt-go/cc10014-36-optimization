"""内存数据仓库：支持模块间关联、版本管理、模拟表数据、血缘记录与状态流转。

真实项目里这里会换成数据库访问层；当前实现只依赖标准库，保证克隆下来就能起。
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.seed import SEED_ROWS, seed_table_data


class ArchiveCommitError(Exception):
    """归档批次落库失败；调用方需要保持事务外的边界与空态不变。"""


class Store:
    # 冷分区归档批次表必须和归档记录一起落库的字段。
    ARCHIVE_BATCH_REQUIRED_FIELDS = [
        "批次编号", "策略编号", "目标表名", "分区边界", "归档路径", "可恢复标记", "归档结果", "分区清单",
    ]

    def __init__(self) -> None:
        self._tables: dict[str, list[dict[str, Any]]] = {
            name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
        }
        # 模拟真实表数据（分层存储的实际行数据）
        self._table_data: dict[str, list[dict[str, Any]]] = {
            name: [dict(row) for row in rows] for name, rows in seed_table_data().items()
        }
        # 表数据版本：每次整表写入递增，归档扫描据此判断能否复用缓存，避免失败后重复扫描整表
        self._table_versions: dict[str, int] = {}
        # 版本历史: "module:id" -> [{version, timestamp, data}, ...]
        self._versions: dict[str, list[dict[str, Any]]] = {}
        # 血缘边记录
        self._lineage_edges: list[dict[str, Any]] = []
        # 质量稽核结果
        self._quality_results: dict[str, list[dict[str, Any]]] = {}
        # 快照冻结数据: "snapshot_id" -> {table_name, rows, frozen_at}
        self._snapshot_data: dict[str, dict[str, Any]] = {}
        # 归档记录
        self._archive_records: list[dict[str, Any]] = []
        # 冷分区归档批次表：批次编号 -> 批次（边界/路径/可恢复标记/结果）
        self._archive_batches: list[dict[str, Any]] = []
        # 补数区间记录
        self._backfill_partitions: dict[str, list[dict[str, Any]]] = {}

    def module_names(self) -> list[str]:
        return sorted(self._tables)

    def rows(self, module: str) -> list[dict[str, Any]]:
        return self._tables.setdefault(module, [])

    def find(self, module: str, entry_id: int) -> dict[str, Any] | None:
        for row in self.rows(module):
            if int(row.get("id", 0)) == entry_id:
                return row
        return None

    def find_by_field(self, module: str, field: str, value: Any) -> dict[str, Any] | None:
        for row in self.rows(module):
            if row.get(field) == value:
                return row
        return None

    def find_all_by_field(self, module: str, field: str, value: Any) -> list[dict[str, Any]]:
        return [row for row in self.rows(module) if row.get(field) == value]

    def overview(self) -> dict[str, object]:
        modules: list[dict[str, object]] = []
        for name in self.module_names():
            rows = self.rows(name)
            modules.append({
                "name": name,
                "created": len(rows),
                "pending": sum(1 for row in rows if row.get("pending")),
                "abnormal": sum(1 for row in rows if row.get("abnormal")),
            })
        cards = [
            {"label": "业务模块", "value": len(modules)},
            {"label": "今日新增", "value": sum(int(item["created"]) for item in modules)},
            {"label": "待处理", "value": sum(int(item["pending"]) for item in modules)},
            {"label": "异常量", "value": sum(int(item["abnormal"]) for item in modules)},
        ]
        return {"cards": cards, "modules": modules}

    # ---- 模拟真实表数据 ----

    def table_rows(self, table_name: str) -> list[dict[str, Any]]:
        return self._table_data.setdefault(table_name, [])

    def table_version(self, table_name: str) -> int:
        return self._table_versions.get(table_name, 0)

    def set_table_rows(self, table_name: str, rows: list[dict[str, Any]]) -> None:
        self._table_data[table_name] = rows
        self._table_versions[table_name] = self.table_version(table_name) + 1

    def append_table_rows(self, table_name: str, rows: list[dict[str, Any]]) -> None:
        self._table_data.setdefault(table_name, []).extend(rows)
        self._table_versions[table_name] = self.table_version(table_name) + 1

    # ---- 版本管理 ----

    def _version_key(self, module: str, entry_id: int) -> str:
        return f"{module}:{entry_id}"

    def save_version(self, module: str, entry_id: int, data: dict[str, Any]) -> dict[str, Any]:
        key = self._version_key(module, entry_id)
        versions = self._versions.setdefault(key, [])
        version = {
            "version": len(versions) + 1,
            "timestamp": datetime.now().isoformat(),
            "data": dict(data),
        }
        versions.append(version)
        return version

    def get_versions(self, module: str, entry_id: int) -> list[dict[str, Any]]:
        key = self._version_key(module, entry_id)
        return list(self._versions.get(key, []))

    def get_latest_version(self, module: str, entry_id: int) -> dict[str, Any] | None:
        versions = self.get_versions(module, entry_id)
        return versions[-1] if versions else None

    # ---- 血缘 ----

    def add_lineage(self, upstream: str, downstream: str, field_map: str,
                    transform_logic: str = "", batch_id: str = "") -> dict[str, Any]:
        edge = {
            "id": len(self._lineage_edges) + 1,
            "上游表名": upstream,
            "下游表名": downstream,
            "来源字段": field_map,
            "目标字段": field_map,
            "转换逻辑": transform_logic,
            "采集批次": batch_id,
            "链路版本": f"v{len(self._lineage_edges) + 1}",
            "status": "已生效",
            "pending": False,
            "abnormal": False,
        }
        self._lineage_edges.append(edge)
        # 同时写入 lineage_track 模块表供查询
        self.rows("lineage_track").append(edge)
        return edge

    def get_lineage_edges(self) -> list[dict[str, Any]]:
        return list(self._lineage_edges)

    # ---- 质量稽核结果 ----

    def save_quality_result(self, rule_id: str, result: dict[str, Any]) -> None:
        self._quality_results.setdefault(rule_id, []).append(result)

    def get_quality_results(self, rule_id: str) -> list[dict[str, Any]]:
        return list(self._quality_results.get(rule_id, []))

    # ---- 快照 ----

    def save_snapshot_data(self, snapshot_id: str, table_name: str, rows: list[dict[str, Any]]) -> None:
        self._snapshot_data[snapshot_id] = {
            "table_name": table_name,
            "rows": [dict(r) for r in rows],
            "frozen_at": datetime.now().isoformat(),
            "row_count": len(rows),
        }

    def get_snapshot_data(self, snapshot_id: str) -> dict[str, Any] | None:
        return self._snapshot_data.get(snapshot_id)

    # ---- 归档 ----

    def add_archive_record(self, record: dict[str, Any]) -> None:
        self._archive_records.append(record)

    def get_archive_records(self) -> list[dict[str, Any]]:
        return list(self._archive_records)

    # ---- 冷分区归档批次表 ----

    def _validate_archive_batch(self, batch: dict[str, Any]) -> list[str]:
        missing = []
        for field in self.ARCHIVE_BATCH_REQUIRED_FIELDS:
            value = batch.get(field)
            if value is None or (isinstance(value, str) and not value.strip()):
                missing.append(field)
        if batch.get("可恢复标记") not in ("可恢复", "不可恢复"):
            missing.append("可恢复标记")
        boundary = batch.get("分区边界")
        if boundary is not None and not isinstance(boundary, dict):
            missing.append("分区边界")
        return missing

    def commit_archive_batch(self, batch: dict[str, Any],
                             record: dict[str, Any] | None = None) -> dict[str, Any]:
        """原子落库：批次表字段与归档记录必须同时写入，任一校验失败整体回滚。

        校验发生在任何写入之前，失败时批次表、归档记录和调用方持有的边界/空态都不变。
        """
        missing = self._validate_archive_batch(batch)
        if missing:
            raise ArchiveCommitError(f"冷分区归档批次缺少字段：{'、'.join(missing)}")
        if record is not None:
            record_boundary = record.get("partition_boundary")
            if record_boundary is not None and record_boundary != batch.get("分区边界"):
                raise ArchiveCommitError("分区边界与归档记录不一致")
            if record.get("path") and batch.get("归档路径") and record["path"] != batch["归档路径"]:
                raise ArchiveCommitError("归档路径与归档记录不一致")
        self._archive_batches.append(dict(batch))
        if record is not None:
            self._archive_records.append(dict(record))
        return batch

    def find_archive_batch_by_partition(self, strategy_no: str, table: str,
                                        partition: str) -> dict[str, Any] | None:
        for batch in self._archive_batches:
            boundary = batch.get("分区边界") or {}
            covered = set(boundary.get("partitions") or [])
            covered.update(item.get("partition", "") for item in batch.get("分区清单", []))
            if (batch.get("策略编号") == strategy_no and batch.get("目标表名") == table
                    and partition in covered):
                return batch
        return None

    def list_archive_batches(self, *, strategy_no: str | None = None,
                             table: str | None = None) -> list[dict[str, Any]]:
        batches = self._archive_batches
        if strategy_no:
            batches = [b for b in batches if b.get("策略编号") == strategy_no]
        if table:
            batches = [b for b in batches if b.get("目标表名") == table]
        return list(batches)

    def update_archive_batch(self, batch_no: str, **fields: Any) -> dict[str, Any] | None:
        """更新已落库批次的结果字段（如归档完成后回填冲突结果）。"""
        for batch in self._archive_batches:
            if batch.get("批次编号") == batch_no:
                batch.update(fields)
                return batch
        return None

    def restore_archive_batch(self, batch_no: str) -> dict[str, Any] | None:
        """按批次表登记的边界与归档路径恢复；不可恢复或缺失批次返回 None。"""
        for batch in self._archive_batches:
            if batch.get("批次编号") == batch_no:
                if batch.get("可恢复标记") != "可恢复":
                    return None
                batch["归档结果"] = "已恢复"
                return batch
        return None

    # ---- 补数分区 ----

    def save_backfill_partition(self, job_id: str, partition: dict[str, Any]) -> None:
        self._backfill_partitions.setdefault(job_id, []).append(partition)

    def get_backfill_partitions(self, job_id: str) -> list[dict[str, Any]]:
        return list(self._backfill_partitions.get(job_id, []))


store = Store()
