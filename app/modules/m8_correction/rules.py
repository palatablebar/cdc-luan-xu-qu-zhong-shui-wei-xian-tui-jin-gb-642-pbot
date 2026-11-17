"""补正键与身份规则（纯函数，无 IO）。

补正键是「同一补正重试不新增、不重复」的根据：

    correction_key = sha256(batch_id | target_ref | 规范化补正事件 | 操作人 | 事由)

同键重发命中同一条 corrections 记录；改了内容就换键、换单。
身份规则：补正只能修内容，不能换 (table, pk, version) 身份，也不允许跳版本，
避免借补正通道绕过 M3 的版本序与墓碑规则。
"""
from __future__ import annotations

import hashlib
import json

from ..m2_schema.service import Norm


def event_digest(event: dict) -> str:
    """补正事件载荷摘要：键排序后哈希，重发同内容得到同一摘要。"""
    body = json.dumps(event or {}, sort_keys=True, ensure_ascii=False,
                      separators=(",", ":"), default=str)
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def correction_key(*, batch_id: str, target_ref: str, event: dict,
                   operator: str = "", reason: str = "") -> str:
    parts = [batch_id, target_ref, event_digest(event),
             (operator or "").strip(), (reason or "").strip()]
    h = hashlib.sha256()
    for part in parts:
        h.update(part.encode("utf-8"))
        h.update(b"\x00")
    return h.hexdigest()[:12]


def correction_id_for(no: int) -> str:
    return f"R-{no:04d}"


def revision_id_for(batch_id: str, revision_no: int) -> str:
    return f"RV-{batch_id}-{revision_no:03d}"


def identity_of(norm: Norm) -> tuple[str, str, int | None]:
    return (norm.table, norm.pk, norm.version)


def identity_matches(norm: Norm, target: dict) -> tuple[bool, str]:
    """补正事件身份必须与目标坏行一致；目标坏行身份缺失时按不通过处理。"""
    target_table = (target.get("table_name") or "").strip()
    target_pk = (target.get("pk") or "").strip()
    target_version = target.get("version")
    if not target_table or not target_pk or target_version is None:
        return False, "目标坏行缺少可用的表/主键/版本身份，无法锚定补正"
    if norm.table != target_table:
        return False, f"表不一致：补正 {norm.table} ≠ 目标 {target_table}"
    if norm.pk != target_pk:
        return False, f"主键不一致：补正 {norm.pk} ≠ 目标 {target_pk}"
    if int(norm.version or 0) != int(target_version):
        return False, f"版本不一致：补正 {norm.version} ≠ 目标 {target_version}"
    return True, ""


def is_quarantine_target(target: dict) -> bool:
    return target.get("row_class") == "quarantine"


def collides_with_applied(norm: Norm, applied_sigs: dict[tuple, set[str]]) -> str:
    """与已采信事件同键同版本且同签名 → 该值已在正式结果里，无需补正。

    同键同版本但签名不同属于补正替换（隔离行原本未应用，不构成版本冲突），放行。
    返回非空字符串即拒绝原因码。
    """
    key = (norm.table, norm.pk, int(norm.version or 0))
    if key in applied_sigs:
        from ..m3_dedup.dedup import signature
        if signature(norm) in applied_sigs[key]:
            return "R_DUPLICATE_BUSINESS_KEY"
    return ""


def applied_signature_index(applied_norms: list[Norm]) -> dict[tuple, set[str]]:
    from ..m3_dedup.dedup import signature
    index: dict[tuple, set[str]] = {}
    for norm in applied_norms:
        if norm.version is None:
            continue
        index.setdefault((norm.table, norm.pk, int(norm.version)), set()).add(
            signature(norm))
    return index


def late_flag(event_time, as_of) -> bool:
    """补正事件迟到标记沿用批次水位半开区间：严格小于 as_of 才迟到。"""
    if event_time is None or as_of is None:
        return False
    return event_time < as_of
