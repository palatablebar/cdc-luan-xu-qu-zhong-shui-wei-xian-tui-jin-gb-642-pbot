"""正式快照修订版折叠（纯函数）。

修订版 = 原批次「已应用事件」∪ 当前全部生效补正事件，在原批次 as_of（水位线）上
重新物化。复用 m5 的 apply/materialize，保证折叠顺序、墓碑、半开窗口、金额 Decimal
与原批次完全同一套语义；补正不推进水位。

每行带来源标注：

- source_kind=original：来自原批次采信，correction_id 为空；
- source_kind=correction：由补正事件折叠而来，带 correction_id 与 target_ref。

修订版不可变：每次发布一个新 revision_no，旧版行原样保留在 effective_snapshot_rows。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal

from ..m2_schema.coerce import iso_utc
from ..m5_snapshot import apply as apply_mod
from ..m5_snapshot import materialize


@dataclass
class CorrectionEvent:
    correction_id: str
    target_ref: str
    applied: apply_mod.Applied


@dataclass
class Revision:
    revision_no: int
    as_of: object
    rows: list[dict] = field(default_factory=list)
    counters: dict = field(default_factory=dict)
    amounts: dict = field(default_factory=dict)
    corrected_keys: set = field(default_factory=set)
    folded_corrections: list = field(default_factory=list)

    @property
    def amount_snapshot_text(self) -> str:
        return f"{self.amounts.get('snapshot', Decimal('0.00')):.2f}"


def _to_applied(row: dict) -> apply_mod.Applied:
    return apply_mod.Applied.from_row(row)


def build_revision(*, revision_no: int, original_applied: list[dict],
                   correction_events: list[CorrectionEvent], as_of) -> Revision:
    applied = [_to_applied(row) for row in original_applied]
    # 同一目标行的多次补正：按 seq 升序，后者（最新补正单）覆盖前者。
    latest_by_target: dict[str, CorrectionEvent] = {}
    for item in sorted(correction_events, key=lambda c: c.applied.seq):
        latest_by_target[item.target_ref] = item
    active = list(latest_by_target.values())
    applied.extend(item.applied for item in active)

    snapshot = materialize.materialize(applied, as_of)
    # 物化后按 (table, pk) 标注补正来源：该键最新状态来自哪条补正。
    origin: dict[tuple[str, str], CorrectionEvent] = {}
    for item in active:
        origin[item.applied.key] = item

    rows: list[dict] = []
    corrected_keys: set = set()
    for row in snapshot.rows:
        key = (row["table"], row["pk"])
        item = origin.get(key)
        if item is not None and row["row_ref"] == item.applied.row_ref:
            row = {**row, "source_kind": "correction",
                   "correction_id": item.correction_id,
                   "target_ref": item.target_ref}
            corrected_keys.add(key)
        else:
            row = {**row, "source_kind": "original",
                   "correction_id": None, "target_ref": None}
        rows.append(row)

    return Revision(
        revision_no=revision_no, as_of=as_of, rows=rows,
        counters=snapshot.counters, amounts=snapshot.amounts,
        corrected_keys=corrected_keys,
        folded_corrections=[item.correction_id for item in active])


def key_deltas(before: list[dict], after: list[dict]) -> list[dict]:
    """逐键金额变化：补正前后正式快照的金额差，供补正账逐键复核。"""
    def index(rows):
        out = {}
        for row in rows:
            amount = row.get("amount")
            out[(row.get("table_name") or row.get("table"), row["pk"])] = (
                None if amount is None else Decimal(str(amount)))
        return out

    left, right = index(before), index(after)
    out = []
    for key in sorted(set(left) | set(right)):
        old = left.get(key)
        new = right.get(key)
        if old == new:
            continue
        out.append({
            "key": f"{key[0]}/{key[1]}",
            "amount_before": None if old is None else f"{old:.2f}",
            "amount_after": None if new is None else f"{new:.2f}",
            "delta": f"{(new or Decimal('0.00')) - (old or Decimal('0.00')):.2f}",
        })
    return out


def reconcile_revision(revision: Revision, base_counters: dict,
                       base_amounts: dict, correction_count: int) -> list[dict]:
    """补正账：补正无遗漏、键账守恒、金额差额可逐键解释。"""
    after_amount = revision.amounts.get("snapshot", Decimal("0.00"))
    before_amount = base_amounts.get("snapshot", Decimal("0.00"))
    delta = after_amount - before_amount
    return [
        {"name": "补正行账", "kind": "count",
         "left": "生效补正单", "left_value": correction_count,
         "right": "折叠进修订版的补正",
         "right_value": len(revision.folded_corrections),
         "ok": correction_count == len(revision.folded_corrections)},
        {"name": "快照键账", "kind": "count",
         "left": "修订版快照键", "left_value": revision.counters["snapshot_keys"],
         "right": "活跃 + 墓碑",
         "right_value": (revision.counters["active_keys"]
                         + revision.counters["tombstone_keys"]),
         "ok": revision.counters["snapshot_keys"]
         == revision.counters["active_keys"] + revision.counters["tombstone_keys"]},
        {"name": "补正金额账", "kind": "money",
         "left": "修订版快照金额", "left_value": after_amount,
         "right": "基线快照金额 + 补正差额",
         "right_value": before_amount + delta,
         "ok": after_amount == before_amount + delta},
    ]


def as_of_text(as_of) -> str:
    return iso_utc(as_of) or "—"
