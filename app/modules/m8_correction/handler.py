"""M8 入口：行级补正提交、追溯、正式快照修订版与核对页面。

只读原批次证据，所有写操作落在 corrections / snapshot_revisions /
effective_snapshot_rows 三张新表；原批次 API 与导出不受影响。
"""
from __future__ import annotations

from fastapi import APIRouter, Depends, Request, Response

from ...web import Ctx, get_ctx, render
from ..m7_export import csv_out
from . import service

router = APIRouter(tags=["M8 行级补正"])

CSV_MEDIA = "text/csv; charset=utf-8"

CORRECTION_COLUMNS = ("补正单号", "批次", "目标行号", "状态", "原因码",
                      "表", "主键", "版本", "修订版", "取代", "操作人",
                      "事由", "载荷摘要", "提交时间", "明细")
EFFECTIVE_COLUMNS = ("表", "主键", "版本", "状态", "金额(元)", "数量",
                     "事件时间(UTC)", "行号引用", "来源", "补正单号", "目标行号",
                     "迟到", "待定")


@router.post("/api/corrections")
def api_submit(payload: dict | None = None, ctx: Ctx = Depends(get_ctx)) -> dict:
    body = payload or {}
    return service.submit(
        store=ctx.store, event=body.get("event") or {},
        target_ref=str(body.get("target_ref") or ""),
        batch_id=body.get("batch_id"),
        operator=str(body.get("operator") or ""),
        reason=str(body.get("reason") or ""))


@router.get("/api/corrections")
def api_list(status: str | None = None, ctx: Ctx = Depends(get_ctx)) -> dict:
    batch = ctx.current_batch()
    if batch is None:
        return {"batch_id": None, "count": 0, "corrections": []}
    return service.list_corrections(ctx.store, batch["batch_id"], status)


@router.get("/api/corrections/{correction_id}")
def api_trace_one(correction_id: str, ctx: Ctx = Depends(get_ctx)) -> dict:
    return service.trace_correction(ctx.store, correction_id)


@router.get("/api/trace/row/{row_ref:path}")
def api_trace_row(row_ref: str, ctx: Ctx = Depends(get_ctx)) -> dict:
    return service.trace_target(ctx.store, row_ref)


@router.get("/api/effective-snapshot")
def api_effective(revision: int | None = None, ctx: Ctx = Depends(get_ctx)) -> dict:
    batch = ctx.current_batch()
    if batch is None:
        return {"batch_id": None, "revision_no": 0, "rows": []}
    return service.effective_snapshot(ctx.store, batch["batch_id"], revision)


@router.get("/api/export/corrections.csv")
def export_corrections(ctx: Ctx = Depends(get_ctx)) -> Response:
    batch = ctx.current_batch()
    if batch is None:
        return Response(content=csv_out.encode_csv(
            csv_out.to_csv(CORRECTION_COLUMNS, [])), media_type=CSV_MEDIA,
            headers={"Content-Disposition": 'attachment; filename="corrections.csv"'})
    data = service.list_corrections(ctx.store, batch["batch_id"])
    rows = []
    for item in data["corrections"]:
        event = item["event"]
        rows.append([
            item["correction_id"], item["batch_id"], item["target_ref"],
            item["status"], item.get("reason_code") or "",
            event.get("table", ""), event.get("pk", ""), event.get("version", ""),
            item.get("revision_no") or "", item.get("supersedes_id") or "",
            item.get("operator") or "", item.get("reason") or "",
            (item.get("payload_digest") or "")[:16],
            item.get("created_at") or "", item.get("detail") or "",
        ])
    text = csv_out.to_csv(CORRECTION_COLUMNS, rows)
    filename = f"{batch['batch_id']}-corrections.csv"
    return Response(content=csv_out.encode_csv(text), media_type=CSV_MEDIA,
                    headers={"Content-Disposition":
                             f'attachment; filename="{filename}"'})


@router.get("/api/export/effective-snapshot.csv")
def export_effective(ctx: Ctx = Depends(get_ctx)) -> Response:
    batch = ctx.current_batch()
    if batch is None:
        return Response(content=csv_out.encode_csv(
            csv_out.to_csv(EFFECTIVE_COLUMNS, [])), media_type=CSV_MEDIA,
            headers={"Content-Disposition":
                     'attachment; filename="effective-snapshot.csv"'})
    doc = service.effective_snapshot(ctx.store, batch["batch_id"])
    rows = []
    for row in doc["rows"]:
        rows.append([
            row["table"], row["pk"], row["version"],
            "墓碑" if row["op"] == "delete" else "活跃",
            row.get("amount") or "", row.get("qty") if row.get("qty") is not None else "",
            row["event_time_utc"], row["row_ref"],
            "补正" if row["source_kind"] == "correction" else "原批次",
            row.get("correction_id") or "", row.get("target_ref") or "",
            "是" if row["late"] else "否", "是" if row["pending"] else "否",
        ])
    text = csv_out.to_csv(EFFECTIVE_COLUMNS, rows)
    filename = f"{batch['batch_id']}-effective-r{doc['revision_no']}.csv"
    return Response(content=csv_out.encode_csv(text), media_type=CSV_MEDIA,
                    headers={"Content-Disposition":
                             f'attachment; filename="{filename}"'})


@router.get("/corrections")
def page_corrections(request: Request, ctx: Ctx = Depends(get_ctx)):
    batch = ctx.current_batch()
    payload = service.page_payload(ctx.store, batch["batch_id"]) if batch else {
        "corrections": [], "revisions": [],
        "effective": {"rows": [], "revision_no": 0, "revisions": []},
        "deltas": [], "reconcile": [], "quarantine": []}
    payload.pop("batch", None)
    return render(request, "corrections.html", batch=batch, page="corrections",
                  page_title="补正核对", **payload)
