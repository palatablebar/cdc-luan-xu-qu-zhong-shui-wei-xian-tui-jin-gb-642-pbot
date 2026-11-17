"""行级补正编排：提交补正单 → 同一套契约校验 → 事务内发布正式快照修订版。

四条硬约束（对应业务/审计诉求）：

1. 原批次证据只读：不写 events / snapshots / manifests，坏行留在隔离区；
2. 不整批重跑：已采信行不重新计数，补正只注入目标 row_ref 的一条补正事件；
3. 正式快照不沿用旧值：补正成功即在同事务发布 revision_no+1 的不可变修订版；
4. 失败留旧版、重试幂等：发布失败整体回滚（上一 active 版可用），
   correction_key 命中既有补正单，不新增、不重复。
"""
from __future__ import annotations

import datetime as dt
import json
from decimal import Decimal

from ... import config, errors, reasons
from ..m1_events.reader import RawRow
from ..m2_schema import service as m2_service
from ..m2_schema.coerce import parse_iso
from ..m5_snapshot import apply as apply_mod
from . import revision as revision_mod
from . import rules


def _now_iso() -> str:
    return dt.datetime.now(config.UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _load_batch(store, batch_id: str | None) -> dict:
    batch = store.get_batch(batch_id) if batch_id else store.latest_batch()
    if batch is None:
        raise errors.NotFound("还没有批次，先采信一次", code="E_NO_BATCH")
    if batch["status"] != "done":
        raise errors.BadRequest(f"批次 {batch['batch_id']} 未完成，不能补正",
                                code="E_BAD_BATCH")
    return batch


def _as_of(store, batch: dict):
    return parse_iso(batch.get("watermark_utc"))


def _normalize_correction(event: dict) -> m2_service.Norm:
    raw = RawRow(file="correction", lineno=1,
                 text=json.dumps(event, ensure_ascii=False), raw=event)
    return m2_service.normalize_row(raw)


def _norm_from_stored(row: dict) -> m2_service.Norm:
    """已落库的 applied 事件行 → Norm（补齐 event_time，签名才与首跑一致）。"""
    return m2_service.Norm(
        row_ref=row["row_ref"], file="", lineno=0, seq=row["seq"],
        table=row["table_name"], pk=row["pk"], version=row["version"],
        op=row["op"], event_time=parse_iso(row["event_time_utc"]),
        event_time_raw=row.get("event_time_raw") or "",
        schema=row.get("schema_version") or config.DEFAULT_SCHEMA,
        payload=row.get("payload") or {}, amount=row["amount"],
        qty=row.get("qty"), row_class="applied")


def _record(*, batch_id: str, no: int, key: str, target_ref: str, status: str,
            event: dict, digest: str, operator: str, reason: str,
            code: str | None, detail: str, revision_no: int | None,
            supersedes_id: str | None, created_at: str,
            finished_at: str | None) -> dict:
    return {
        "correction_id": rules.correction_id_for(no),
        "correction_key": key, "batch_id": batch_id, "correction_no": no,
        "target_ref": target_ref, "status": status, "operator": operator,
        "reason": reason, "event": event, "payload_digest": digest,
        "reason_code": code, "detail": detail, "revision_no": revision_no,
        "supersedes_id": supersedes_id, "created_at": created_at,
        "finished_at": finished_at,
    }


def submit(*, store, event: dict, target_ref: str, batch_id: str | None = None,
           operator: str = "", reason: str = "") -> dict:
    if not isinstance(event, dict) or not event:
        raise errors.BadRequest("缺少补正事件 event（完整事件对象）",
                                code="E_BAD_REQUEST")
    target_ref = (target_ref or "").strip()
    if not target_ref:
        raise errors.BadRequest("缺少补正目标 row_ref", code="E_BAD_REQUEST")
    batch = _load_batch(store, batch_id)
    bid = batch["batch_id"]
    digest = rules.event_digest(event)
    key = rules.correction_key(batch_id=bid, target_ref=target_ref, event=event,
                               operator=operator, reason=reason)

    # ---- 幂等：同键重发命中既有补正单，不新增 ----
    existing = store.get_correction_by_key(bid, key)
    if existing is not None:
        return _idempotent_payload(store, existing)

    no = store.next_correction_no(bid)
    created_at = _now_iso()

    # ---- 目标锚定校验 ----
    target = _find_target(store, bid, target_ref)
    if target is None:
        return _reject(store, batch, no, key, target_ref, event, digest,
                       operator, reason, created_at, "R_TARGET_NOT_FOUND",
                       f"批次 {bid} 的证据里找不到 {target_ref}")
    if not rules.is_quarantine_target(target):
        return _reject(store, batch, no, key, target_ref, event, digest,
                       operator, reason, created_at, "R_TARGET_NOT_QUARANTINE",
                       f"{target_ref} 分类为 {target.get('row_class')}，"
                       "补正只面向隔离区坏行")

    # ---- 同一套契约校验 ----
    norm = _normalize_correction(event)
    if norm.errors:
        return _reject(store, batch, no, key, target_ref, event, digest,
                       operator, reason, created_at, "R_CONTRACT_FAILED",
                       f"契约不通过：{'；'.join(norm.errors)} · {norm.detail}")

    # ---- 身份一致性 ----
    ok, mismatch = rules.identity_matches(norm, target)
    if not ok:
        return _reject(store, batch, no, key, target_ref, event, digest,
                       operator, reason, created_at, "R_IDENTITY_MISMATCH", mismatch)

    # ---- 与已采信事件撞签名（同键同版本同内容无需补正）----
    applied_norms = [_norm_from_stored(r) for r in store.load_events(bid, "applied")]
    collide = rules.collides_with_applied(
        norm, rules.applied_signature_index(applied_norms))
    if collide:
        return _reject(store, batch, no, key, target_ref, event, digest,
                       operator, reason, created_at, collide,
                       reasons.label(collide))

    # ---- 通过：单事务落补正单 + 发布修订版（失败整体回滚，旧版不动）----
    try:
        return _publish(store=store, batch=batch, norm=norm, no=no, key=key,
                        target_ref=target_ref, event=event, digest=digest,
                        operator=operator, reason=reason, created_at=created_at)
    except errors.CorrectionRejected:
        raise
    except Exception as exc:  # noqa: BLE001 - 发布异常转失败留痕，正式快照不动
        _reject(store, batch, no, key, target_ref, event, digest,
                operator, reason, created_at, "R_REVISION_FAILED",
                f"修订版发布失败已回滚：{exc}")
        raise errors.CorrectionRejected(
            reasons.label("R_REVISION_FAILED"), code="R_REVISION_FAILED",
            detail={"target_ref": target_ref},
            correction_id=rules.correction_id_for(no))


def _publish(*, store, batch, norm, no, key, target_ref, event, digest,
             operator, reason, created_at) -> dict:
    bid = batch["batch_id"]
    as_of = _as_of(store, batch)
    prior_active = store.active_corrections(bid)
    supersedes = next((r["correction_id"] for r in reversed(prior_active)
                       if r["target_ref"] == target_ref), None)
    revision_before = store.latest_revision(bid)
    revision_no = (revision_before["revision_no"] + 1) if revision_before else 1
    correction_id = rules.correction_id_for(no)

    correction_events = _correction_events(store, bid, prior_active, as_of)
    seq_base = max((r["seq"] for r in store.load_events(bid)), default=0)
    applied = apply_mod.Applied.from_norm(norm)
    applied.seq = seq_base + no
    applied.row_ref = f"{target_ref}#correction:{correction_id}"
    applied.late = rules.late_flag(norm.event_time, as_of)
    correction_events.append(revision_mod.CorrectionEvent(
        correction_id=correction_id, target_ref=target_ref, applied=applied))

    original_applied = store.load_events(bid, "applied")
    revision = revision_mod.build_revision(
        revision_no=revision_no, original_applied=original_applied,
        correction_events=correction_events, as_of=as_of)

    finished_at = _now_iso()
    record = _record(batch_id=bid, no=no, key=key, target_ref=target_ref,
                     status="applied", event=event, digest=digest,
                     operator=operator, reason=reason, code="R_APPLIED",
                     detail=reasons.label("R_APPLIED"),
                     revision_no=revision_no, supersedes_id=supersedes,
                     created_at=created_at, finished_at=finished_at)
    rev_row = {
        "revision_id": rules.revision_id_for(bid, revision_no),
        "batch_id": bid, "revision_no": revision_no, "status": "active",
        "base_of_correction": correction_id,
        "as_of_utc": revision_mod.as_of_text(as_of),
        "row_count": len(revision.rows),
        "amount_snapshot": revision.amount_snapshot_text,
        "created_at": finished_at,
        "note": f"补正 {correction_id} 生效"
                + (f"，取代 {supersedes}" if supersedes else ""),
    }
    # 单事务：补正单落库 + 修订版整版发布，任一失败整体回滚。
    store.publish_correction_revision(record=record, revision=rev_row,
                                      rows=revision.rows)

    base_counters, base_amounts = _baseline(store, bid, revision_before)
    reconcile = revision_mod.reconcile_revision(
        revision, base_counters, base_amounts, len(correction_events))
    return {
        "status": "applied", "idempotent_retry": False,
        "correction": _public_correction(record),
        "batch_id": bid,
        "revision": {"revision_no": revision_no,
                     "revision_id": rev_row["revision_id"],
                     "rows": len(revision.rows),
                     "as_of_utc": rev_row["as_of_utc"],
                     "amount_snapshot": revision.amount_snapshot_text,
                     "supersedes_id": supersedes},
        "reconciliation": _jsonable(reconcile),
        "all_balanced": all(r["ok"] for r in reconcile),
        "counters": revision.counters,
    }


def _reject(store, batch, no, key, target_ref, event, digest, operator, reason,
            created_at, code, detail) -> dict:
    """失败留痕：独立事务写 failed 补正单，不发布修订版，正式快照不动。"""
    record = _record(batch_id=batch["batch_id"], no=no, key=key,
                     target_ref=target_ref, status="failed", event=event,
                     digest=digest, operator=operator, reason=reason, code=code,
                     detail=detail, revision_no=None, supersedes_id=None,
                     created_at=created_at, finished_at=_now_iso())
    store.insert_correction(record)
    raise errors.CorrectionRejected(
        detail, code=code, detail={"target_ref": target_ref, "reason": code},
        correction_id=record["correction_id"])


def _idempotent_payload(store, existing: dict) -> dict:
    payload = {
        "status": existing["status"], "idempotent_retry": True,
        "correction": _public_correction({**existing,
                                          "event": _safe_json(existing)}),
        "batch_id": existing["batch_id"],
        "note": reasons.label("R_IDEMPOTENT_RETRY"),
    }
    if existing["status"] == "applied":
        rev = (store.get_revision(rules.revision_id_for(
            existing["batch_id"], existing["revision_no"]))
            if existing.get("revision_no") is not None else None)
        payload["revision"] = ({"revision_no": rev["revision_no"],
                                "revision_id": rev["revision_id"],
                                "rows": rev["row_count"],
                                "as_of_utc": rev["as_of_utc"],
                                "amount_snapshot": rev["amount_snapshot"],
                                "supersedes_id": existing.get("supersedes_id")}
                               if rev else None)
    return payload


def _safe_json(row: dict) -> dict:
    return json.loads(row.get("event_json") or "{}")


def _find_target(store, batch_id: str, target_ref: str) -> dict | None:
    for row in store.load_events(batch_id):
        if row["row_ref"] == target_ref:
            return row
    return None


def _correction_events(store, batch_id: str, active: list[dict], as_of):
    events = []
    original_seq = max((r["seq"] for r in store.load_events(batch_id)), default=0)
    for record in active:
        norm = _normalize_correction(_safe_json(record))
        applied = apply_mod.Applied.from_norm(norm)
        applied.seq = original_seq + record["correction_no"]
        applied.row_ref = (f"{record['target_ref']}#correction:"
                           f"{record['correction_id']}")
        applied.late = rules.late_flag(norm.event_time, as_of)
        events.append(revision_mod.CorrectionEvent(
            correction_id=record["correction_id"],
            target_ref=record["target_ref"], applied=applied))
    return events


def _baseline(store, batch_id: str, revision_before: dict | None) -> tuple[dict, dict]:
    if revision_before is not None:
        rows = store.load_effective_rows(revision_before["revision_id"])
    else:
        rows = store.load_snapshot(batch_id)
    counters = {"snapshot_keys": len(rows),
                "active_keys": sum(1 for r in rows if r["op"] != "delete"),
                "tombstone_keys": sum(1 for r in rows if r["op"] == "delete")}
    amount = Decimal("0.00")
    for row in rows:
        if row["op"] != "delete" and row.get("amount") is not None:
            amount += Decimal(str(row["amount"]))
    return counters, {"snapshot": amount}


def _public_correction(record: dict) -> dict:
    event = record.get("event")
    if event is None:
        event = _safe_json(record)
    return {
        "correction_id": record["correction_id"],
        "correction_key": record["correction_key"],
        "batch_id": record["batch_id"],
        "correction_no": record["correction_no"],
        "target_ref": record["target_ref"], "status": record["status"],
        "operator": record.get("operator", ""), "reason": record.get("reason", ""),
        "event": event, "payload_digest": record.get("payload_digest"),
        "reason_code": record.get("reason_code"), "detail": record.get("detail"),
        "revision_no": record.get("revision_no"),
        "supersedes_id": record.get("supersedes_id"),
        "created_at": record.get("created_at"),
        "finished_at": record.get("finished_at"),
    }


def list_corrections(store, batch_id: str, status: str | None = None) -> dict:
    rows = store.list_corrections(batch_id)
    if status:
        rows = [r for r in rows if r["status"] == status]
    return {"batch_id": batch_id, "count": len(rows),
            "corrections": [_public_correction(r) for r in rows]}


def trace_target(store, target_ref: str, batch_id: str | None = None) -> dict:
    """正向追溯：原行证据 → 补正单链 → 各修订版里的正式值。"""
    batch = _load_batch(store, batch_id)
    bid = batch["batch_id"]
    target = _find_target(store, bid, target_ref)
    corrections = [r for r in store.list_corrections(bid, limit=1000)
                   if r["target_ref"] == target_ref]
    chain = []
    for record in corrections:
        item = {"correction": _public_correction(record)}
        if record["status"] == "applied" and record.get("revision_no") is not None:
            rev_id = rules.revision_id_for(bid, record["revision_no"])
            item["revision"] = store.get_revision(rev_id)
            item["effective_row"] = store.find_effective_row_by_correction(
                bid, record["correction_id"])
        chain.append(item)
    return {"batch_id": bid, "target_ref": target_ref,
            "original_evidence": target, "chain": chain,
            "chain_count": len(chain)}


def trace_correction(store, correction_id: str) -> dict:
    """反向追溯：补正单 → 目标原行证据 + 所在修订版正式值。"""
    record = store.get_correction(correction_id)
    if record is None:
        raise errors.NotFound(f"补正单 {correction_id} 不存在", code="E_NOT_FOUND")
    bid = record["batch_id"]
    target = _find_target(store, bid, record["target_ref"])
    out = {"batch_id": bid, "correction": _public_correction(
        {**record, "event": _safe_json(record)}),
        "original_evidence": target}
    if record["status"] == "applied" and record.get("revision_no") is not None:
        rev_id = rules.revision_id_for(bid, record["revision_no"])
        out["revision"] = store.get_revision(rev_id)
        out["effective_row"] = store.find_effective_row_by_correction(
            bid, correction_id)
    return out


def effective_snapshot(store, batch_id: str | None = None,
                       revision_no: int | None = None) -> dict:
    """正式快照：默认读最新有效修订版；无修订版时回退原批次快照（基线）。"""
    batch = _load_batch(store, batch_id)
    bid = batch["batch_id"]
    revision = None
    if revision_no is not None:
        revision = store.get_revision(rules.revision_id_for(bid, revision_no))
        if revision is None:
            raise errors.NotFound(f"修订版 {revision_no} 不存在",
                                  code="E_NOT_FOUND")
    if revision is None:
        revision = store.latest_revision(bid)
    if revision is None:
        rows = store.load_snapshot(bid)
        return {"batch_id": bid, "revision_no": 0, "revision_id": None,
                "baseline": True, "as_of_utc": batch.get("watermark_utc"),
                "rows": [_eff_row(r) for r in rows],
                "revisions": store.list_revisions(bid), "corrections_applied": 0}
    rows = store.load_effective_rows(revision["revision_id"])
    return {"batch_id": bid, "revision_no": revision["revision_no"],
            "revision_id": revision["revision_id"], "baseline": False,
            "as_of_utc": revision["as_of_utc"], "note": revision.get("note"),
            "rows": [_eff_row(r) for r in rows],
            "revisions": store.list_revisions(bid),
            "corrections_applied": len(store.active_corrections(bid)),
            "corrected_keys": sum(1 for r in rows
                                  if r.get("source_kind") == "correction")}


def _eff_row(row: dict) -> dict:
    return {
        "table": row.get("table_name") or row.get("table"),
        "pk": row["pk"], "version": row["version"], "op": row["op"],
        "amount": None if row.get("amount") is None else str(row["amount"]),
        "qty": row.get("qty"),
        "event_time_utc": row.get("event_time_utc"),
        "row_ref": row["row_ref"], "late": bool(row.get("late")),
        "pending": bool(row.get("pending")),
        "source_kind": row.get("source_kind", "original"),
        "correction_id": row.get("correction_id"),
        "target_ref": row.get("target_ref"),
        "payload": row.get("payload", {}),
    }


def revision_deltas(store, batch_id: str) -> list[dict]:
    """最新修订版相对基线（原批次快照）的逐键金额差。"""
    latest = store.latest_revision(batch_id)
    if latest is None:
        return []
    after = store.load_effective_rows(latest["revision_id"])
    before = store.load_snapshot(batch_id)
    return revision_mod.key_deltas(before, after)


def page_payload(store, batch_id: str | None = None) -> dict:
    batch = _load_batch(store, batch_id)
    bid = batch["batch_id"]
    corrections = store.list_corrections(bid, limit=500)
    effective = effective_snapshot(store, bid)
    latest = store.latest_revision(bid)
    reconcile = []
    if latest is not None:
        rows = store.load_effective_rows(latest["revision_id"])
        rev = revision_mod.Revision(
            revision_no=latest["revision_no"], as_of=parse_iso(latest["as_of_utc"]),
            rows=rows,
            counters={"snapshot_keys": latest["row_count"],
                      "active_keys": sum(1 for r in rows if r["op"] != "delete"),
                      "tombstone_keys": sum(1 for r in rows if r["op"] == "delete")},
            amounts={"snapshot": Decimal(str(latest["amount_snapshot"]))},
            folded_corrections=[c["correction_id"]
                                for c in store.active_corrections(bid)])
        base_counters, base_amounts = _baseline(store, bid, None)
        reconcile = revision_mod.reconcile_revision(
            rev, base_counters, base_amounts,
            len(store.active_corrections(bid)))
    return {
        "batch": batch,
        "corrections": [_public_correction(r) for r in corrections],
        "revisions": store.list_revisions(bid),
        "effective": effective,
        "deltas": revision_deltas(store, bid),
        "reconcile": _jsonable(reconcile),
        "quarantine": [{"row_ref": r["row_ref"], "table": r["table_name"],
                        "pk": r["pk"], "version": r["version"],
                        "reason_code": r["reason_code"], "detail": r["detail"]}
                       for r in store.load_events(bid)
                       if r["row_class"] == "quarantine"],
    }


def _jsonable(rows: list[dict]) -> list[dict]:
    out = []
    for row in rows:
        item = {}
        for key, value in row.items():
            item[key] = str(value) if isinstance(value, Decimal) else value
        out.append(item)
    return out
