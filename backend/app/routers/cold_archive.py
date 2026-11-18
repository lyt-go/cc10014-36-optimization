"""冷热归档接口：维护归档策略，覆盖提交校验、标记生效、停用策略等动作。"""
from __future__ import annotations

import csv
import io
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from app.schemas import ActionPayload, ActionResult, EntryPayload, PageResult
from app.services.cold_archive import ColdArchiveService

router = APIRouter(prefix="/api/cold_archive", tags=["冷热归档"])

service = ColdArchiveService()

LIST_FIELDS = ["策略编号", "目标表名", "冷数据阈值", "归档路径", "压缩算法", "归档格式", "保留天数", "恢复超时"]
STATUSES = ["草稿", "待校验", "已生效", "已停用"]


def query_filters(request: Request) -> dict[str, str]:
    """只接受当前模块声明过的字段，忽略分页等保留参数。"""
    return {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in LIST_FIELDS and value.strip()
    }


@router.get("", response_model=PageResult[dict])
def list_entries(
    request: Request,
    keyword: str | None = Query(default=None, description="按策略编号检索"),
    status: str | None = Query(default=None, description="草稿、待校验、已生效、已停用"),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=20, ge=1, le=200),
) -> PageResult[dict]:
    """按页面字段、策略编号关键字与状态过滤冷热归档列表。"""
    items, total = service.list_entries(
        filters=query_filters(request), keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/export")
def export_entries(
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
) -> StreamingResponse:
    """下载当前筛选条件下的 UTF-8 CSV；必须声明在 /{entry_id} 之前。"""
    items, _ = service.list_entries(
        filters=query_filters(request), keyword=keyword, status=status, page=1, size=1000000)
    output = io.StringIO()
    output.write("\ufeff")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(["id", *LIST_FIELDS, "status"])
    for item in items:
        row = []
        for value in [item.get("id"), *(item.get(field, "") for field in LIST_FIELDS), item.get("status", "")]:
            text = "" if value is None else str(value)
            row.append("'" + text if text.startswith(("=", "+", "-", "@")) else text)
        writer.writerow(row)
    return StreamingResponse(
        iter([output.getvalue()]), media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="cold_archive.csv"'})


@router.get("/batches")
def list_archive_batches(
    strategy_no: str | None = Query(default=None),
    table: str | None = Query(default=None),
) -> dict[str, Any]:
    """冷分区归档批次表；必须声明在 /{entry_id} 之前。"""
    items = service.list_archive_batches(strategy_no=strategy_no, table=table)
    return {"items": items, "total": len(items)}


@router.post("/batches/{batch_no}/restore")
def restore_batch(batch_no: str) -> dict[str, Any]:
    """按批次表登记的边界与归档路径恢复冷分区。"""
    return service.restore_batch(batch_no)


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条归档策略明细；不存在时给出可读的错误说明。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"归档策略 {entry_id} 不存在或已归档")
    return entry


@router.post("", response_model=ActionResult)
def create_entry(payload: dict[str, object]) -> ActionResult:
    """登记一条归档策略，缺字段时说明原因而不是静默丢弃。"""
    entry, missing = service.create_entry(payload)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="归档策略已登记", entry=entry)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: ActionPayload) -> ActionResult:
    """对单条归档策略执行提交校验、标记生效、停用策略；不允许的动作会被拦下并说明原因。"""
    entry, message = service.run_action(entry_id, payload.action.strip())
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)


@router.post("/{entry_id}/execute")
def execute_archive(entry_id: int, snapshot_id: str | None = None) -> dict[str, Any]:
    """执行归档策略：消费快照或目标表数据，写入归档记录。"""
    result = service.execute_archive(entry_id, snapshot_id)
    return result


@router.get("/{entry_id}/partitions")
def analyze_partitions(entry_id: int) -> dict[str, Any]:
    """冷分区边界分析：待归档分区、边界分区、保留期边界与缺日期清单。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"归档策略 {entry_id} 不存在或已归档")
    return service.analyze_partitions(entry)


@router.get("/{entry_id}/plan")
def plan_batches(
    entry_id: int,
    batch_size: int = Query(default=5, ge=1, le=100),
    boundary_mode: str = Query(default="冷热阈值", description="冷热阈值或分区大小"),
) -> dict[str, Any]:
    """生成分批归档计划：批次边界、归档路径、可恢复标记与冲突说明。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"归档策略 {entry_id} 不存在或已归档")
    return service.plan_batches(entry, batch_size=batch_size, boundary_mode=boundary_mode)


@router.post("/{entry_id}/archive")
def archive_cold_partitions(
    entry_id: int,
    batch_size: int = Query(default=5, ge=1, le=100),
    boundary_mode: str = Query(default="冷热阈值", description="冷热阈值或分区大小"),
    max_batches: int | None = Query(default=None, ge=0, description="只提交前 N 批，用于中断后续跑"),
) -> dict[str, Any]:
    """分批归档待归档冷分区，批次表与归档记录原子落库；重复提交不会重复归档。"""
    return service.archive_cold_partitions(
        entry_id, batch_size=batch_size, boundary_mode=boundary_mode, max_batches=max_batches)
