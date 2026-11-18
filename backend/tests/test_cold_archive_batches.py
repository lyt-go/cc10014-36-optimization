"""冷分区归档批次：边界、空态、事务回滚、幂等与中断续跑。"""
from __future__ import annotations

import threading
from datetime import date, timedelta

import pytest

from app.services.cold_archive import ColdArchiveService
from app.services.partition_boundary import build_partition_analysis
from app.store import ArchiveCommitError, store


def reset_store():
    from app.seed import SEED_ROWS
    store._tables = {name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()}
    store._table_data.clear()
    store._table_versions.clear()
    store._versions.clear()
    store._lineage_edges.clear()
    store._quality_results.clear()
    store._snapshot_data.clear()
    store._archive_records.clear()
    store._archive_batches.clear()
    store._backfill_partitions.clear()


def rows_for(partitions: dict[str, int], *, empty_rows: int = 0) -> list[dict]:
    rows: list[dict] = []
    for partition, count in partitions.items():
        rows.extend({"order_id": f"O{partition}-{i}", "dt": partition} for i in range(count))
    rows.extend({"order_id": f"E{i}", "dt": ""} for i in range(empty_rows))
    return rows


def day(offset: int) -> str:
    return (date.today() - timedelta(days=offset)).isoformat()


class TestPartitionBoundary:
    def setup_method(self):
        reset_store()

    def test_empty_date_does_not_become_latest_partition(self):
        analysis = build_partition_analysis(rows_for({day(5): 2, day(40): 3}, empty_rows=2))
        assert analysis["最新分区"] == day(5)
        assert analysis["空日期清单"][0]["reason"] == "缺日期"
        assert analysis["空日期清单"][0]["参与保留期计算"] is False
        assert analysis["空日期数量"] == 1
        assert analysis["空日期行数"] == 2

    def test_cold_boundary_is_nearest_cold_partition(self):
        analysis = build_partition_analysis(
            rows_for({day(10): 1, day(35): 2, day(50): 3, day(400): 1}),
            cold_threshold_days=30,
        )
        assert analysis["边界分区"]["partition"] == day(35)
        assert analysis["边界分区"]["boundary_kind"] == "冷热阈值边界"
        assert analysis["保留期边界分区"]["partition"] == day(400)
        assert [item["partition"] for item in analysis["待归档分区"]] == sorted([day(35), day(50), day(400)])

    def test_states_cover_empty_and_none(self):
        assert build_partition_analysis([])["状态"] == "暂无"
        assert build_partition_analysis(rows_for({}, empty_rows=3))["状态"] == "空态"
        assert build_partition_analysis(rows_for({day(1): 2}))["状态"] == "没有"

    def test_empty_state_does_not_enter_retention_or_batch(self):
        store.set_table_rows("ods_orders", rows_for({}, empty_rows=2))
        svc = ColdArchiveService()
        result = svc.archive_cold_partitions(1)
        assert result["状态"] == "空态"
        assert result["archived"] == 0
        assert result["空日期参与保留期计算"] is False
        assert store.list_archive_batches(table="ods_orders") == []

    def test_whole_table_archive_skips_empty_only_table(self):
        store.set_table_rows("ods_orders", rows_for({}, empty_rows=2))
        result = ColdArchiveService().execute_archive(1)
        assert result["ok"] is True
        assert result["archived"] == 0
        assert store.list_archive_batches(table="ods_orders") == []


class TestArchiveBatches:
    def setup_method(self):
        reset_store()

    def test_batches_keep_boundary_path_and_restorable_flag(self):
        store.set_table_rows("ods_orders", rows_for({day(40): 2, day(45): 1, day(60): 3}))
        svc = ColdArchiveService()
        result = svc.archive_cold_partitions(1, batch_size=2)
        assert result["ok"] is True
        assert result["archived"] == 2
        assert len(store.list_archive_batches(table="ods_orders")) == 2
        for batch in store.list_archive_batches(table="ods_orders"):
            assert batch["分区边界"]["起始分区"] <= batch["分区边界"]["结束分区"]
            assert batch["归档路径"].startswith("/archive/ods_orders")
            assert batch["可恢复标记"] == "可恢复"
            for item in batch["分区清单"]:
                assert item["归档路径"].startswith("/archive/ods_orders/")
                assert item["可恢复标记"] == "可恢复"
        records = store.get_archive_records()
        assert len(records) == len(store.list_archive_batches(table="ods_orders"))

    def test_every_valid_partition_recoverable_and_empty_not_in_batches(self):
        store.set_table_rows("ods_orders", rows_for({day(40): 1, day(70): 1}, empty_rows=2))
        svc = ColdArchiveService()
        svc.archive_cold_partitions(1)
        for partition in (day(40), day(70)):
            path = svc.find_recovery_path("ods_orders", partition)
            assert path is not None
            assert path["可恢复标记"] == "可恢复"
            assert path["归档路径"].endswith(partition)
        assert svc.find_recovery_path("ods_orders", "") is None
        batches = store.list_archive_batches(table="ods_orders")
        assert all(item["partition"] != "" for b in batches for item in b["分区清单"])

    def test_same_partition_concurrent_only_archived_once(self):
        store.set_table_rows("ods_orders", rows_for({day(40): 1, day(41): 1}))
        svc = ColdArchiveService()
        results: list[dict] = []

        def worker() -> None:
            results.append(svc.archive_cold_partitions(1))

        threads = [threading.Thread(target=worker) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        archived_partitions = [
            item["partition"]
            for batch in store.list_archive_batches(table="ods_orders")
            for item in batch["分区清单"]
        ]
        assert sorted(archived_partitions) == sorted([day(40), day(41)]), f"同一冷分区被重复归档: {archived_partitions}"
        assert sum(1 for r in results if r["archived"]) == 1

    def test_retry_skips_archived_and_is_explainable(self):
        store.set_table_rows("ods_orders", rows_for({day(40): 1, day(50): 1, day(60): 1}))
        svc = ColdArchiveService()
        first = svc.archive_cold_partitions(1, batch_size=1, max_batches=1)
        assert first["状态"] == "中断"
        assert first["retryable"] is True
        assert len(first["待重试分区"]) == 2
        second = svc.archive_cold_partitions(1, batch_size=1)
        assert second["ok"] is True
        assert second["archived"] == 2
        assert len(store.list_archive_batches(table="ods_orders")) == 3
        third = svc.archive_cold_partitions(1)
        assert third["状态"] == "已归档"
        assert third["archived"] == 0
        assert "未重复归档" in third["message"]

    def test_commit_failure_rolls_back_boundary_and_empty_state(self):
        store.set_table_rows("ods_orders", rows_for({day(40): 1}, empty_rows=1))
        svc = ColdArchiveService()
        before_batches = len(store.list_archive_batches())
        before_records = len(store.get_archive_records())

        def broken_commit(batch, record=None):
            raise ArchiveCommitError("写入批次表中断")

        original = store.commit_archive_batch
        store.commit_archive_batch = broken_commit
        try:
            result = svc.archive_cold_partitions(1)
        finally:
            store.commit_archive_batch = original
        assert result["ok"] is False
        assert result["状态"] == "失败"
        assert result["retryable"] is True
        assert len(store.list_archive_batches()) == before_batches
        assert len(store.get_archive_records()) == before_records
        plan = svc.plan_batches(svc.get_entry(1))
        assert plan["边界分区"]["partition"] == day(40)
        assert plan["空日期清单"][0]["partition"] == "（空）"

    def test_missing_batch_fields_rejected_before_write(self):
        before = len(store.list_archive_batches())
        with pytest.raises(ArchiveCommitError):
            store.commit_archive_batch({"批次编号": "BAT-X", "分区边界": {"分区": "x"}})
        assert len(store.list_archive_batches()) == before

    def test_boundary_must_match_archive_record(self):
        before_batches = len(store.list_archive_batches())
        before_records = len(store.get_archive_records())
        batch = {
            "批次编号": "BAT-Y", "策略编号": "ARC-ORD-001", "目标表名": "ods_orders",
            "分区边界": {"partition": day(40), "partitions": [day(40)]},
            "归档路径": "/archive/ods_orders", "可恢复标记": "可恢复",
            "归档结果": "已归档", "分区清单": [{"partition": day(40)}],
        }
        with pytest.raises(ArchiveCommitError):
            store.commit_archive_batch(
                batch, {"partition_boundary": {"partition": day(41)}, "path": "/archive/ods_orders"})
        assert len(store.list_archive_batches()) == before_batches
        assert len(store.get_archive_records()) == before_records

    def test_conflict_reports_two_boundary_views(self):
        store.set_table_rows("ods_orders", rows_for({day(35): 2, day(400): 1}, empty_rows=1))
        svc = ColdArchiveService()
        plan = svc.plan_batches(svc.get_entry(1), boundary_mode="分区大小")
        conflict = plan["冲突"]
        assert conflict is not None
        assert conflict["缺日期分区"] == ["（空）"]
        assert conflict["边界分区归档结果"]["归档分区数"] >= 1
        assert conflict["大小分区归档结果"]["大小分区归档结果"][0]["row_count"] == 2
        assert conflict["待修复集合"]["空日期清单"][0]["reason"] == "缺日期"
        assert conflict["优先级"] == "待确认"
        assert conflict["处理顺序"] == "待确认"

    def test_conflict_persisted_into_batch_table(self):
        store.set_table_rows("ods_orders", rows_for({day(400): 1}, empty_rows=1))
        svc = ColdArchiveService()
        result = svc.archive_cold_partitions(1)
        assert result["ok"] is True
        batch = store.list_archive_batches(table="ods_orders")[0]
        assert batch["冲突"]["缺日期分区"] == ["（空）"]
        assert batch["冲突"]["边界分区归档结果"]["归档结果"] == "已归档"
        assert batch["冲突"]["大小分区归档结果"]["归档结果"] == "已归档"
        assert batch["冲突"]["处理顺序"] == "待确认"

    def test_snapshot_reads_batch_table_after_workbench_commit(self):
        store.set_table_rows("ods_orders", [
            {"order_id": "O1", "dt": day(40)}, {"order_id": "O2", "dt": day(40)},
        ])
        svc = ColdArchiveService()
        result = svc.archive_cold_partitions(1)
        assert result["ok"] is True
        items = svc.list_archive_batches(table="ods_orders")
        assert len(items) == 1
        assert items[0]["分区清单"][0]["可恢复标记"] == "可恢复"
        assert items[0]["分区边界"]["结束分区"] == day(40)

    def test_restore_uses_registered_path(self):
        store.set_table_rows("ods_orders", rows_for({day(40): 1}))
        svc = ColdArchiveService()
        svc.archive_cold_partitions(1)
        batch_no = store.list_archive_batches(table="ods_orders")[0]["批次编号"]
        restored = svc.restore_batch(batch_no)
        assert restored["ok"] is True
        assert restored["batch"]["归档结果"] == "已恢复"
        assert svc.restore_batch("BAT-UNKNOWN")["ok"] is False

    def test_interrupted_conflict_backfilled_after_retry(self):
        store.set_table_rows("ods_orders", rows_for({day(400): 1, day(500): 1}, empty_rows=1))
        svc = ColdArchiveService()
        first = svc.archive_cold_partitions(1, batch_size=1, max_batches=1)
        assert first["冲突"]["边界分区归档结果"]["归档结果"] == "待归档"
        second = svc.archive_cold_partitions(1, batch_size=1)
        assert second["ok"] is True
        for batch in store.list_archive_batches(table="ods_orders"):
            assert batch["冲突"]["边界分区归档结果"]["归档结果"] == "已归档"
            assert batch["冲突"]["处理顺序"] == "待确认"

    def test_snapshot_archives_expired_and_reads_restorable_flag(self):
        from app.services.snapshot_mgr import SnapshotMgrService
        store.set_table_rows("ods_orders", rows_for({day(40): 1}))
        expired = (date.today() - timedelta(days=60)).isoformat()
        snap_row = store.rows("snapshot_mgr")[0]
        snap_row["快照时间"] = expired
        snap_row["保留天数"] = "30"
        result = SnapshotMgrService().archive_expired_snapshots()
        assert result["ok"] is True
        assert result["archived"] == 1
        assert result["items"][0]["可恢复标记"] == "可恢复"
        batches = ColdArchiveService().list_archive_batches(table="ods_orders")
        assert len(batches) == 1
        # 再次归档不会重复写入批次表。
        again = SnapshotMgrService().archive_expired_snapshots()
        assert len(ColdArchiveService().list_archive_batches(table="ods_orders")) == 1
        assert again["archived"] == 0
