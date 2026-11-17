# 深度登记 cdcwater（CDC 乱序去重与水位线推进）

- 规模：中型（8 个功能模块 / 8 个页面 / 99 条测试）
- 核心规则一：**版本序折叠 + 墓碑**——每个 `(table, pk)` 按 `version` 升序应用，`delete` 写墓碑；
  同键同版本出现两种内容整组不应用（`C_VERSION_CONFLICT`），删除之后到达的低版本 upsert
  不复活（`C_TOMBSTONE_RESURRECT`），删除之前到达的低版本照常应用后被覆盖。
- 核心规则二：**水位线与半开窗口**——水位线 = 已应用事件最大 `event_time` − 10 分钟，单调
  不回退；`event_time < 水位线` 记迟到但**仍然应用**；快照取 `event_time ≤ as_of`，
  待定窗口 `(as_of, +inf)` 半开，两窗口不重叠也不留缝。
- 使用者与工作流：订单库 CDC 管道负责人，每天收 3 个导出文件（主表、旧列名补数、迟到补丁），
  原先靠 grep 日志判断「这条到底应用了没有」；现在一次采信即可得到水位、迟到清单、冲突与
  隔离明细，以及可导出复核的快照与运行清单。

## 功能模块

| 模块 | 使用者动作 | 入口 | 核心文件 | 测试 |
|---|---|---|---|---|
| M1 事件采信 | 选来源跑一次采信，看逐文件诊断与行号 | POST /api/events/import、页面 /events | `app/modules/m1_events/reader.py` `app/modules/m1_events/decode.py` `app/modules/m1_events/format.py` `app/modules/m1_events/service.py` | `tests/test_m1_events.py` |
| M2 契约兼容 | 看字段契约与别名，试归一一条事件 | GET /api/schema、POST /api/schema/preview、页面 /schema | `app/modules/m2_schema/contract.py` `app/modules/m2_schema/coerce.py` `app/modules/m2_schema/alias.py` `app/modules/m2_schema/service.py` | `tests/test_m2_schema.py` |
| M3 去重冲突 | 看重复、冲突、隔离与优先级 | GET /api/conflicts、页面 /conflicts | `app/modules/m3_dedup/dedup.py` `app/modules/m3_dedup/conflict.py` `app/modules/m3_dedup/service.py` | `tests/test_m3_dedup.py` |
| M4 水位迟到 | 看水位时间轴与迟到明细 | GET /api/watermark、页面 / | `app/modules/m4_watermark/watermark.py` `app/modules/m4_watermark/late.py` `app/modules/m4_watermark/service.py` | `tests/test_m4_watermark.py` |
| M5 快照物化 | 按水位物化快照，看待定键与三条对账 | POST /api/snapshot、页面 /snapshot | `app/modules/m5_snapshot/apply.py` `app/modules/m5_snapshot/materialize.py` `app/modules/m5_snapshot/service.py` | `tests/test_m5_snapshot.py` |
| M6 批次续跑 | 查历史、看运行清单、续跑与重跑 | GET /api/batches、POST /api/batches/{id}/resume、页面 /batches | `app/modules/m6_batches/state.py` `app/modules/m6_batches/resume.py` `app/modules/m6_batches/history.py` `app/modules/m6_batches/service.py` | `tests/test_m6_batches.py` |
| M7 审计导出 | 下载证据行、隔离、冲突、快照与清单 | GET /api/export/* | `app/modules/m7_export/csv_out.py` `app/modules/m7_export/json_out.py` `app/modules/m7_export/manifest_out.py` `app/modules/m7_export/service.py` | `tests/test_m7_export.py` |
| M8 行级补正 | 隔离坏行补正、修订版链、双向追溯、失败留旧版与幂等重试 | POST /api/corrections、GET /api/effective-snapshot、页面 /corrections | `app/modules/m8_correction/rules.py` `app/modules/m8_correction/revision.py` `app/modules/m8_correction/service.py` `app/modules/m8_correction/handler.py` | `tests/test_m8_correction.py` |

端到端：`tests/test_e2e.py` 覆盖「样例 → 采信 → 归一 → 去重冲突 → 水位 → 快照 → 导出 → 页面」，
含 4xx 失败输入与幂等重跑；`tests/test_invariants.py` 覆盖行账、键账、金额、幂等、迟到互补。

## 陷阱登记

| TRAP | 现象 | 朴素实现会怎样 | 本项目处理 | 处理位置 | 测试 |
|---|---|---|---|---|---|
| TRAP-01 | 旧列名导出文件声明 `utf-8`，字节其实是 GB18030 | 按声明解码就崩，或整列乱码 | 声明只作首个候选，失败即回退 gb18030，并记 `E_CHARSET_MISMATCH` | `app/modules/m1_events/decode.py` | `tests/test_m1_events.py::test_gb18030_fallback_and_mismatch_code` |
| TRAP-05 | CSV 行尾多一个分隔符（1102 行多出 EXTRA） | 静默丢弃多余列或错位 | 严格列数校验，整行隔离并保留原值 | `app/modules/m1_events/reader.py` | `tests/test_m1_events.py::test_csv_column_count_mismatch_quarantined` |
| TRAP-09 | 主键 `9007199254740993` 超过 2^53，另有前导零 `00123` | `int()` 化后丢精度或丢零 | 主键全程字符串；JSON 导出超过 2^53 的整数也转字符串 | `app/modules/m7_export/json_out.py`、`app/modules/m2_schema/service.py` | `tests/test_m7_export.py::test_csv_keeps_bigint_and_leading_zero`、`tests/test_m2_schema.py::test_bigint_pk_kept_as_text` |
| TRAP-11 | 同一时刻写成 `08:10Z` 与 `16:10+08:00`，另有无偏移时间 | 落进两个时刻、去重失效 | 先归一 UTC；无偏移按 +08:00 解释并记 `W_TIME_NO_OFFSET` | `app/modules/m2_schema/coerce.py` | `tests/test_m2_schema.py::test_timezone_offsets_collapse_to_same_utc`、`tests/test_m3_dedup.py::test_timezone_written_duplicate_merged` |
| TRAP-13 | 补数文件整批比当前水位早（主文件 08:40 那条迟到 12 分钟） | 水位一推进就丢迟到事件 | 迟到窗口 10 分钟，`event_time < 水位线` 记迟到但仍应用，快照标 `late` | `app/modules/m4_watermark/watermark.py` | `tests/test_m4_watermark.py::test_late_event_still_applied`、`tests/test_m4_watermark.py::test_late_by_exactly_twelve_minutes` |
| TRAP-14 | 同键同版本两种内容（1005 的 PAID / CANCELLED），删除后飘来低版本（1012 的 v2） | 后写覆盖，或整批丢弃，看不出保留了哪条 | 两条都进冲突表、都不应用，并给出证据行与差异字段 | `app/modules/m3_dedup/conflict.py` | `tests/test_m3_dedup.py::test_same_version_two_payloads_both_conflict`、`tests/test_m3_dedup.py::test_tombstone_after_delete_conflicts` |
| TRAP-16 | 主文件声明 34 行实际 33 行，末行只剩半个 JSON | 半行被当有效事件，凭空多一条 | 声明条数校验 + 末行解析失败 → `E_TAIL_TRUNCATED` 隔离 | `app/modules/m1_events/reader.py` | `tests/test_m1_events.py::test_declared_rows_mismatch_marks_truncation`、`tests/test_m1_events.py::test_tail_unparsable_line_isolated_as_truncated` |
| TRAP-17 | v1 改列名（`ref_no`/`amt`/`st`）后新旧混发 | `KeyError` 或整行丢弃 | 别名映射到规范列名；新旧同时出现且取值不同判 `E_ALIAS_AMBIGUOUS` | `app/modules/m2_schema/alias.py` | `tests/test_m2_schema.py::test_v1_alias_maps_to_canonical`、`tests/test_m2_schema.py::test_alias_ambiguous_quarantined` |
| TRAP-19 | 一条事件同时像「重复」又像「冲突」，坏行还互相重复 | 按代码书写顺序覆盖，结果随遍历顺序变 | 优先级 `quarantine > conflict > duplicate > apply`，各桶由集合运算得出 | `app/modules/m3_dedup/service.py` | `tests/test_m3_dedup.py::test_conflict_beats_duplicate`、`tests/test_m3_dedup.py::test_priority_independent_of_input_order` |
| TRAP-23 | 事件 `event_time` 恰好等于水位线 / 恰好等于 `as_of` | 两端都算，重复计入或漏计 | 迟到用半开 `[水位线, +inf)`；快照含 `as_of`、待定窗口 `(as_of, +inf)` 半开 | `app/modules/m4_watermark/watermark.py`、`app/modules/m5_snapshot/materialize.py` | `tests/test_m4_watermark.py::test_boundary_equal_watermark_is_not_late`、`tests/test_m5_snapshot.py::test_boundary_event_at_as_of_included_once` |
| TRAP-26 | 拿上一轮旧导出再跑一次（最大事件 06:00 < 已持久化水位 08:52） | 无条件覆盖，水位回退、快照变旧 | 陈旧批次直接 409 `C_STALE_BATCH`，已落库结果不动 | `app/modules/m6_batches/resume.py`、`app/modules/m6_batches/state.py` | `tests/test_m6_batches.py::test_stale_input_rejected` |
| TRAP-27 | 隔离坏行当天要用上修正值，但审计要求原批次证据原样；整批重跑又让已采信行再次计数 | 直接 UPDATE 原行（证据被污染）或整批重跑（重复计数），或只改补正行导致正式快照沿用旧值 | 补正独立成单只追加：原批次六张表只读，补正事件与不可变正式快照修订版落新表；基线快照冻结，业务读最新 active 修订版 | `app/modules/m8_correction/service.py`、`app/modules/m8_correction/revision.py` | `tests/test_m8_correction.py::test_correction_creates_revision_without_touching_original`、`::test_no_full_rerun_recounting_applied_rows` |
| TRAP-28 | 补正发布到一半失败（写修订版报错） | 补正单落了但快照没换，页面读到半套状态；或旧版被覆盖无法回退 | 补正单 + 修订版整版在同一事务提交，异常整体回滚并写 failed 留痕，上一 active 修订版继续可用 | `app/store.py::publish_correction_revision`、`app/modules/m8_correction/service.py` | `tests/test_m8_correction.py::test_contract_failure_keeps_old_revision_and_leaves_trace` |
| TRAP-29 | 同一张补正单网络重试、或对同一坏行反复点提交 | 每次重试新增一张单、修订版递增、金额被重复累计 | `correction_key = 批次+目标行+规范化事件+操作人+事由` 摘要，唯一约束 + 先查后写：同键返回原单（成功返修订版、失败保持留痕），不新增行 | `app/modules/m8_correction/rules.py`、`app/modules/m8_correction/service.py` | `tests/test_m8_correction.py::test_retry_same_payload_is_idempotent_no_new_row`、`::test_failed_retry_same_payload_is_idempotent` |
| TRAP-30 | 借补正通道换主键/跳版本，绕过 M3 版本序与墓碑规则 | 补正一条高版本事件把墓碑复活或顶替别的键 | 补正事件身份 `(table,pk,version)` 必须与目标隔离行一致，不一致 409 `R_IDENTITY_MISMATCH`；补正不推进水位，窗口/迟到沿用原批次 as_of | `app/modules/m8_correction/rules.py` | `tests/test_m8_correction.py::test_identity_mismatch_rejected`、`::test_pending_window_and_late_corrections_keep_as_of` |

## 对账规则

- **行账**：原始数据行 = 已应用 + 重复 + 冲突 + 隔离（空行与注释行单独计数）；
- **键账**：已应用键 = 快照键 + 只有待定事件的键；快照键 = 活跃键 + 墓碑键；
- **金额账**：快照金额 + 待定金额 + 变化金额 = 已应用金额（Decimal，单位元，2 位）；
- **迟到账**：迟到 + 准时 = 已应用（迟到只打标记，不改变应用结果）；
- 四条账在首页、`/snapshot` 与 `manifest.json` 的 `reconcile` 中同时出现，任一条不平会显式为「不平」。
- **补正账（M8）**：生效补正单数 = 折叠进修订版的补正数；修订版快照键 = 活跃 + 墓碑；
  修订版快照金额 = 基线快照金额 + 逐键差额。原批次四账在补正前后保持不变（行不重数）。

样例数据的四账：行 42 = 27 + 2 + 3 + 10；键 19 = 18 + 1；快照 18 = 16 活跃 + 2 墓碑；
金额 1552.51 + 200.00 + 0.00 = 1752.51。

## 反例与不变量

- 反例（朴素实现会挂）：
  - `tests/test_m1_events.py::test_naive_utf8_decode_would_raise`（按声明编码直接解码抛
    `UnicodeDecodeError`，回退链不抛）；
  - `tests/test_m3_dedup.py::test_conflict_beats_duplicate`（顺序覆盖会把它判成 1 冲突 + 1 重复）；
  - `tests/test_m3_dedup.py::test_low_version_before_delete_still_applies`（把「删除前的低版本」
    一律当复活，会丢掉 1001 的 v1/v2，快照金额少 120.50）；
  - `tests/test_m6_batches.py::test_stale_input_rejected`（后写覆盖会让水位从 08:52 退回 06:00）。
- 不变量：
  - `tests/test_invariants.py::test_row_accounting_identity`（行账守恒）；
  - `tests/test_invariants.py::test_key_accounting_identity`（键账守恒）；
  - `tests/test_invariants.py::test_amount_conservation_is_exact`（金额守恒，Decimal 精确）；
  - `tests/test_invariants.py::test_idempotent_rerun_keeps_rows_and_totals`（幂等重跑）；
  - `tests/test_m4_watermark.py::test_watermark_monotonic`（水位单调）；
  - `tests/test_m5_snapshot.py::test_rebuild_is_idempotent`（物化可重复）。

## 已知不覆盖

- 只处理文件型导出，不接 Kafka / Debezium 在线流；水位线是「批内推进」，不是全局流进度。
- 不做跨批次键级合并：每个批次独立折叠，快照只反映当前批次；跨批次的复活检测需要把历史
  状态一起纳入，属于下一期。
- 事务边界未建模：同一事务的多条变更不保证原子可见。
- 中文数字、货币符号（`￥`）不在金额归一范围内，遇到直接按类型错误隔离。
- 补正只受理隔离区坏行：冲突行（同版本两值/墓碑复活）须先回源定版，再以正常采信进入；
  已采信行的内容修正须以更高版本事件到达，补正通道不改写已采信记录。
