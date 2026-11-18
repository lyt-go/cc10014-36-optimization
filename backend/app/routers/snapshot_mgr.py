"""快照管理 时间线接口：返回作业时间序列结构。"""
from __future__ import annotations

import csv
import io
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from app.schemas import ActionPayload, ActionResult, EntryPayload
from app.services.snapshot_mgr import SnapshotMgrService

router = APIRouter(prefix="/api/snapshot_mgr", tags=["快照管理"])
service = SnapshotMgrService()
LIST_FIELDS = ["快照编号", "目标数据集", "快照时间", "数据行数", "存储路径", "压缩算法", "保留天数", "快照版本"]
STATUSES = ["草稿", "创建中", "已完成", "已过期"]


def csv_cell(value: Any) -> str:
    text = str(value) if value is not None else ""
    if text and text[0] in "=+-@":
        text = "'" + text
    return '"' + text.replace('"', '""') + '"'


@router.get("")
def list_entries() -> dict[str, Any]:
    items = service.get_timeline()
    return {"items": items}


@router.get("/export")
def export_entries() -> StreamingResponse:
    items = service.get_timeline()
    buf = io.StringIO(newline="")
    writer = csv.writer(buf)
    all_keys = ["id"]
    if items:
        all_keys = list(items[0].keys())
    writer.writerow(all_keys)
    for item in items:
        writer.writerow([csv_cell(item.get(k, "")) for k in all_keys])
    buf.seek(0)
    return StreamingResponse(iter([buf.getvalue()]), media_type="text/csv; charset=utf-8",
                             headers={"Content-Disposition": "attachment; filename=snapshot_mgr.csv"})


@router.get("/archive-batches")
def list_archive_batches(
    table: str | None = Query(default=None, description="按目标表名过滤"),
) -> dict[str, Any]:
    """跨模块读取冷分区归档批次表：批次边界、归档路径与可恢复标记。"""
    from app.services.cold_archive import ColdArchiveService
    items = ColdArchiveService().list_archive_batches(table=table)
    return {"items": items, "total": len(items)}


@router.post("/expired/archive")
def archive_expired_snapshots() -> dict[str, Any]:
    """归档过期快照，并回读批次表的可恢复路径；重试会跳过已归档快照。"""
    return service.archive_expired_snapshots()


@router.get("/{entry_id}")
def get_entry(entry_id: int) -> dict:
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"数据快照 {entry_id} 不存在")
    return entry


@router.post("")
def create_entry(payload: EntryPayload) -> ActionResult:
    entry_or_none, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="数据快照已登记", entry=entry_or_none)


@router.post("/{entry_id}/actions")
def run_action(entry_id: int, payload: ActionPayload) -> ActionResult:
    entry_or_none, message = service.run_action(entry_id, payload.action)
    if entry_or_none is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry_or_none)


@router.get("/{entry_id}/frozen")
def get_frozen_data(entry_id: int) -> dict[str, Any]:
    """返回快照冻结的数据内容。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"数据快照 {entry_id} 不存在")
    snapshot_id = entry.get("快照编号", "")
    from app.store import store
    data = store.get_snapshot_data(snapshot_id)
    return data or {"rows": [], "frozen_at": ""}
