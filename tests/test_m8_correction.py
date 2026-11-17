"""M8 行级补正：独立补正单、原批次冻结、版本化正式快照、追溯、失败留旧版、幂等。

裁决（对应业务/审计诉求）：

- 补正独立成单、只追加，原批次 events/snapshots/maniferts 一行不改；
- 不整批重跑：已采信行不重新计数，只注入目标 row_ref 的一条补正事件；
- 补正成功在同事务发布不可变修订版，正式快照默认读最新有效修订版；
- 发布失败整体回滚（上一版可用），同补正键重发命中同一单，不新增、不重复。
"""
from __future__ import annotations

from decimal import Decimal

import pytest
from fastapi.testclient import TestClient

from app import errors
from app.config import Settings
from app.main import create_app
from app.modules.m6_batches import service as batch_service
from app.modules.m8_correction import rules, service as m8
from fixtures import expected as X

TARGET_REF = "events_main.jsonl:23"   # products/6001，E_TYPE_MISMATCH 隔离行


def _event(amount: str = "88.00", *, table="products", pk="6001", version=1,
           op="upsert", when="2026-11-15T08:31:00Z", status="在售") -> dict:
    return {"table": table, "pk": pk, "version": version, "op": op,
            "event_time": when, "schema": "v2",
            "payload": {"status": status, "amount": amount}}


@pytest.fixture()
def seeded(settings, store) -> dict:
    run = batch_service.run(settings=settings, store=store, source="all")
    return {"batch_id": run["batch"]["batch_id"], "store": store}


# ---------- 裁决一：补正独立成单，原批次证据冻结 ----------

def test_correction_creates_revision_without_touching_original(seeded):
    store, bid = seeded["store"], seeded["batch_id"]
    before = store.count_rows(bid)
    before_manifest = store.load_manifest(bid)
    before_quarantine = [r for r in store.load_events(bid)
                         if r["row_class"] == "quarantine"]

    result = m8.submit(store=store, event=_event(), target_ref=TARGET_REF,
                       operator="auditor", reason="金额修正")

    assert result["status"] == "applied"
    assert result["revision"]["revision_no"] == 1
    # 原批次六张表行数一字未变
    assert store.count_rows(bid) == before
    assert store.load_manifest(bid) == before_manifest
    after_quarantine = [r for r in store.load_events(bid)
                        if r["row_class"] == "quarantine"]
    assert len(after_quarantine) == len(before_quarantine)
    target = next(r for r in after_quarantine if r["row_ref"] == TARGET_REF)
    assert target["reason_code"] == "E_TYPE_MISMATCH"
    # 原批次基线快照仍不含该键（坏行原本未应用）
    assert not [r for r in store.load_snapshot(bid)
                if r["table_name"] == "products" and r["pk"] == "6001"]


def test_no_full_rerun_recounting_applied_rows(seeded):
    store, bid = seeded["store"], seeded["batch_id"]
    m8.submit(store=store, event=_event(), target_ref=TARGET_REF)
    totals = store.load_totals(bid)
    # 批次行账保持 42 = 27+2+3+10，已采信 27 行未被再次计数
    assert int(totals["raw_lines"]) == X.RAW_LINES
    assert int(totals["applied"]) == X.APPLIED
    assert int(totals["quarantine"]) == X.QUARANTINE


# ---------- 裁决二：正式快照用修订版承载修正值，as_of 不变 ----------

def test_effective_snapshot_carries_corrected_value(seeded):
    store, bid = seeded["store"], seeded["batch_id"]
    result = m8.submit(store=store, event=_event("88.00"), target_ref=TARGET_REF)
    eff = m8.effective_snapshot(store, bid)
    assert eff["revision_no"] == 1
    assert eff["baseline"] is False
    assert eff["as_of_utc"] == X.WATERMARK      # 补正不推进水位
    row = next(r for r in eff["rows"] if r["table"] == "products"
               and r["pk"] == "6001")
    assert row["amount"] == "88.00"
    assert row["source_kind"] == "correction"
    assert row["correction_id"] == result["correction"]["correction_id"]
    assert row["target_ref"] == TARGET_REF


def test_effective_snapshot_falls_back_to_baseline(seeded):
    store, bid = seeded["store"], seeded["batch_id"]
    eff = m8.effective_snapshot(store, bid)
    assert eff["baseline"] is True
    assert eff["revision_no"] == 0
    assert len(eff["rows"]) == X.SNAPSHOT_KEYS


def test_revision_is_immutable_history(seeded):
    store, bid = seeded["store"], seeded["batch_id"]
    m8.submit(store=store, event=_event("88.00"), target_ref=TARGET_REF)
    m8.submit(store=store, event=_event("99.50"), target_ref=TARGET_REF)
    r1 = m8.effective_snapshot(store, bid, revision_no=1)
    r2 = m8.effective_snapshot(store, bid, revision_no=2)
    assert next(r for r in r1["rows"] if r["pk"] == "6001")["amount"] == "88.00"
    assert next(r for r in r2["rows"] if r["pk"] == "6001")["amount"] == "99.50"
    revisions = store.list_revisions(bid)
    assert [v["revision_no"] for v in revisions] == [1, 2]
    assert all(v["status"] == "active" for v in revisions)


def test_second_correction_supersedes_first_same_target(seeded):
    store, bid = seeded["store"], seeded["batch_id"]
    first = m8.submit(store=store, event=_event("88.00"), target_ref=TARGET_REF)
    second = m8.submit(store=store, event=_event("99.50"),
                       target_ref=TARGET_REF)
    assert second["revision"]["supersedes_id"] == \
        first["correction"]["correction_id"]
    assert len(store.active_corrections(bid)) == 1
    latest = m8.effective_snapshot(store, bid)
    row = next(r for r in latest["rows"] if r["pk"] == "6001")
    assert row["amount"] == "99.50"
    assert row["correction_id"] == second["correction"]["correction_id"]


# ---------- 裁决三：核对页面、正式快照、原行证据相互追溯 ----------

def test_trace_target_and_correction_roundtrip(seeded):
    store = seeded["store"]
    result = m8.submit(store=store, event=_event(), target_ref=TARGET_REF)
    cid = result["correction"]["correction_id"]

    forward = m8.trace_target(store, TARGET_REF)
    assert forward["original_evidence"]["row_ref"] == TARGET_REF
    assert forward["original_evidence"]["reason_code"] == "E_TYPE_MISMATCH"
    assert forward["chain_count"] == 1
    assert forward["chain"][0]["correction"]["correction_id"] == cid
    assert forward["chain"][0]["effective_row"]["amount"] == Decimal("88.00")
    assert forward["chain"][0]["revision"]["revision_id"] == \
        result["revision"]["revision_id"]

    backward = m8.trace_correction(store, cid)
    assert backward["original_evidence"]["row_ref"] == TARGET_REF
    assert backward["effective_row"]["correction_id"] == cid


def test_reconciliation_balanced_and_deltas(seeded):
    store, bid = seeded["store"], seeded["batch_id"]
    result = m8.submit(store=store, event=_event("88.00"), target_ref=TARGET_REF)
    assert result["all_balanced"] is True
    names = [r["name"] for r in result["reconciliation"]]
    assert names == ["补正行账", "快照键账", "补正金额账"]
    deltas = m8.revision_deltas(store, bid)
    assert deltas == [{"key": "products/6001", "amount_before": None,
                       "amount_after": "88.00", "delta": "88.00"}]


# ---------- 裁决四：失败保留旧版，同单重试不新增、不重复 ----------

def test_contract_failure_keeps_old_revision_and_leaves_trace(seeded):
    store, bid = seeded["store"], seeded["batch_id"]
    m8.submit(store=store, event=_event("88.00"), target_ref=TARGET_REF)
    bad = _event("not-a-number")
    with pytest.raises(errors.CorrectionRejected) as info:
        m8.submit(store=store, event=bad, target_ref=TARGET_REF)
    assert info.value.code == "R_CONTRACT_FAILED"
    assert info.value.status_code == 409
    # 失败留痕，但修订版仍是 r1
    failed = [c for c in store.list_corrections(bid)
              if c["status"] == "failed"]
    assert len(failed) == 1
    assert store.latest_revision(bid)["revision_no"] == 1
    assert m8.effective_snapshot(store, bid)["revision_no"] == 1


def test_retry_same_payload_is_idempotent_no_new_row(seeded):
    store, bid = seeded["store"], seeded["batch_id"]
    first = m8.submit(store=store, event=_event(), target_ref=TARGET_REF,
                      operator="a", reason="r")
    again = m8.submit(store=store, event=_event(), target_ref=TARGET_REF,
                      operator="a", reason="r")
    assert again["idempotent_retry"] is True
    assert again["correction"]["correction_id"] == \
        first["correction"]["correction_id"]
    assert m8.list_corrections(store, bid)["count"] == 1
    assert len(store.list_revisions(bid)) == 1


def test_failed_retry_same_payload_is_idempotent(seeded):
    store, bid = seeded["store"], seeded["batch_id"]
    bad = _event("not-a-number")
    with pytest.raises(errors.CorrectionRejected):
        m8.submit(store=store, event=bad, target_ref=TARGET_REF)
    again = m8.submit(store=store, event=bad, target_ref=TARGET_REF)
    assert again["idempotent_retry"] is True
    assert again["status"] == "failed"
    assert m8.list_corrections(store, bid)["count"] == 1
    assert store.latest_revision(bid) is None


def test_changed_payload_is_new_correction(seeded):
    store, bid = seeded["store"], seeded["batch_id"]
    m8.submit(store=store, event=_event("88.00"), target_ref=TARGET_REF)
    m8.submit(store=store, event=_event("88.01"), target_ref=TARGET_REF)
    assert len(store.list_corrections(bid)) == 2
    assert len(store.list_revisions(bid)) == 2


def test_pending_window_and_late_corrections_keep_as_of(seeded):
    """补正不推进水位：晚于 as_of 的补正进待定窗口，早于 as_of 的补正标迟到。"""
    store, bid = seeded["store"], seeded["batch_id"]
    target = "events_main.jsonl:16"      # payments/3001，E_BAD_TIME
    pending_event = _event("18.00", table="payments", pk="3001",
                           when="2026-11-15T09:30:00Z")
    first = m8.submit(store=store, event=pending_event, target_ref=target)
    eff = m8.effective_snapshot(store, bid)
    assert eff["as_of_utc"] == X.WATERMARK
    assert not [r for r in eff["rows"] if r["pk"] == "3001"]
    assert first["all_balanced"] is True

    late_event = _event("19.00", table="payments", pk="3001",
                        when="2026-11-15T08:00:00Z")
    m8.submit(store=store, event=late_event, target_ref=target)
    eff2 = m8.effective_snapshot(store, bid)
    row = next(r for r in eff2["rows"] if r["pk"] == "3001")
    assert row["late"] is True and row["pending"] is False
    assert row["amount"] == "19.00" and row["source_kind"] == "correction"


# ---------- 准入规则 ----------

def test_target_must_exist(seeded):
    store = seeded["store"]
    with pytest.raises(errors.CorrectionRejected) as info:
        m8.submit(store=store, event=_event(), target_ref="missing.jsonl:9")
    assert info.value.code == "R_TARGET_NOT_FOUND"


def test_target_must_be_quarantine(seeded):
    store, bid = seeded["store"], seeded["batch_id"]
    applied_ref = next(r["row_ref"] for r in store.load_events(bid)
                       if r["row_class"] == "applied")
    with pytest.raises(errors.CorrectionRejected) as info:
        m8.submit(store=store, event=_event(), target_ref=applied_ref)
    assert info.value.code == "R_TARGET_NOT_QUARANTINE"


def test_identity_mismatch_rejected(seeded):
    store = seeded["store"]
    with pytest.raises(errors.CorrectionRejected) as info:
        m8.submit(store=store, event=_event(pk="9999"), target_ref=TARGET_REF)
    assert info.value.code == "R_IDENTITY_MISMATCH"


def test_duplicate_business_key_rejected():
    """与已采信事件同键同版本同签名：该值已在正式结果里，无需补正。"""
    from app.modules.m2_schema import service as m2
    from app.modules.m1_events.reader import RawRow
    norms = [m2.normalize_row(RawRow(file="f", lineno=1, text="", raw=_event()))]
    index = rules.applied_signature_index(norms)
    dup = m2.normalize_row(RawRow(file="f", lineno=2, text="", raw=_event()))
    assert rules.collides_with_applied(dup, index) == "R_DUPLICATE_BUSINESS_KEY"


def test_correction_key_stable_and_identity_rules():
    event = _event()
    k1 = rules.correction_key(batch_id="B-1", target_ref=TARGET_REF, event=event,
                              operator="a", reason="r")
    k2 = rules.correction_key(batch_id="B-1", target_ref=TARGET_REF, event=event,
                              operator="a", reason="r")
    k3 = rules.correction_key(batch_id="B-1", target_ref=TARGET_REF,
                              event=_event("99.99"), operator="a", reason="r")
    k4 = rules.correction_key(batch_id="B-2", target_ref=TARGET_REF, event=event,
                              operator="a", reason="r")
    assert k1 == k2 and len(k1) == 12
    assert len({k1, k3, k4}) == 3


# ---------- HTTP / 页面 / 导出 ----------

def test_http_submit_idempotent_and_pages_render(settings):
    app = create_app(settings, seed=True)
    with TestClient(app) as client:
        payload = {"target_ref": TARGET_REF, "event": _event("88.00"),
                   "operator": "http", "reason": "线上补正"}
        first = client.post("/api/corrections", json=payload)
        assert first.status_code == 200
        assert first.json()["status"] == "applied"
        second = client.post("/api/corrections", json=payload)
        assert second.json()["idempotent_retry"] is True

        listing = client.get("/api/corrections").json()
        assert listing["count"] == 1
        eff = client.get("/api/effective-snapshot").json()
        assert eff["revision_no"] == 1
        trace = client.get(f"/api/trace/row/{TARGET_REF}").json()
        assert trace["chain_count"] == 1
        cid = first.json()["correction"]["correction_id"]
        one = client.get(f"/api/corrections/{cid}").json()
        assert one["original_evidence"]["row_ref"] == TARGET_REF

        for path in ("/", "/events", "/schema", "/conflicts", "/snapshot",
                     "/batches", "/corrections"):
            assert client.get(path).status_code == 200, path
        page = client.get("/corrections").text
        assert "补正核对" in page and cid in page and TARGET_REF in page

        assert client.get("/api/export/corrections.csv").status_code == 200
        eff_csv = client.get("/api/export/effective-snapshot.csv")
        assert eff_csv.status_code == 200 and "6001" in eff_csv.text
        # 原批次导出仍是冻结基线
        quarantine_csv = client.get("/api/export/quarantine.csv").text
        assert TARGET_REF in quarantine_csv


def test_http_rejections_are_409_with_correction_id(settings):
    app = create_app(settings, seed=True)
    with TestClient(app) as client:
        resp = client.post("/api/corrections",
                           json={"target_ref": TARGET_REF, "event": _event("bad")})
        assert resp.status_code == 409
        body = resp.json()
        assert body["error"] == "R_CONTRACT_FAILED"
        assert body["detail"]["correction_id"].startswith("R-")


def test_batch_replay_keeps_corrections_intact(seeded):
    store, bid = seeded["store"], seeded["batch_id"]
    m8.submit(store=store, event=_event(), target_ref=TARGET_REF)
    # 同一 store 重跑：命中幂等批次，补正单与修订版不受影响
    settings = Settings(db_path=store.path,
                        sample_dir=str(Settings().sample_dir))
    replay = batch_service.run(settings=settings, store=store, source="all")
    assert replay["idempotent_replay"] is True
    assert m8.list_corrections(store, bid)["count"] == 1
    assert m8.effective_snapshot(store, bid)["revision_no"] == 1
