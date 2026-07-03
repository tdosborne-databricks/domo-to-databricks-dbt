# Known Patterns

Persistent log of every build failure this pipeline has hit, across all flows and customers. Read
this before diagnosing a new failure — it may already be answered here. Every entry records the
decision (promoted to the converter vs. patched locally) and the reasoning, so a pattern that looked
like a one-off the first time gets recognized immediately the second time.

Entries are append-only. Don't delete a pattern once fixed — future flows can still hit the
pre-fix converter version if they're on an older plugin release, and the reasoning is worth keeping.

---

## PROMOTED — `NEEDS REVIEW` line comment swallows the trailing `AS` alias

- **Signature**: `UNRESOLVED_COLUMN.WITH_SUGGESTION` for a column that the source model's GroupBy
  or Window tile was *supposed* to produce, but doesn't appear in its output schema.
- **First seen**: Advisor_Services_ETL flow, `int_append_rows_1` (missing `hs_deal_ids`), traced to
  `int_group_by_8`.
- **Root cause**: `domo_to_dbt/tiles.py`'s `m_group_by`/`m_window` mappers emitted unmapped
  aggregations as `NULL AS \`col\`  -- NEEDS REVIEW: ...` on a single comma-joined line. SQL `--`
  comments run to end-of-line, so the comment silently swallowed everything after it — including
  the `AS \`col\`` alias for every column emitted after the flagged one on that line.
- **Decision: PROMOTED.** This is a converter bug, not a data issue — any Domo flow with an
  unmapped aggregation type hits it identically. Fixed in `tiles.py`: switched the annotation from
  a trailing `-- line comment` to an inline `/* block comment */` immediately after the alias, so
  it can't consume anything past itself.
- **Status**: Fixed 2026-07-02.

---

## PROMOTED — CTE-chain depth uncapped on wide reshaping tiles

- **Signature**: one dbt model taking 25+ minutes to build (or hanging) with no data-volume
  explanation; the compiled SQL is dozens of nested nested `select *`-heavy CTEs in one model.
- **First seen**: Advisor_Services_ETL flow, a 30-tile linear join/groupby chain collapsed into one
  `int_alter_columns_12` model.
- **Root cause**: `project.py`'s `_boundary_layer()` only made a tile its own model boundary for
  sources, sinks, and out-degree ≥2 fan-out points. A long linear chain of out-degree-1 tiles
  (joins, group-bys, window functions) had no depth cap, so it all inlined into one Catalyst
  analysis unit — cost grows superlinearly with chain depth, independent of row count.
- **Decision: PROMOTED.** Fixed in `project.py`: added `_WIDE_TILE_TYPES` (`MergeJoin`, `GroupBy`,
  `WindowAction`, `Normalizer`, `UnionAll`) as an additional model-boundary rule, capping CTE-chain
  depth per model regardless of out-degree.
- **Status**: Fixed 2026-07-02.

---

## PROMOTED — intermediate view default + fan-out table promotion

- **Signature**: all intermediates materialized as Delta tables (storage sprawl, slow iteration) or
  deep view chains re-analyzing upstream plans on every read.
- **Decision: PROMOTED (updated 2026-07).** Intermediates default to **`view`** in `dbt_project.yml`
  and the converter. `apply_materialization.py` (Phase A) promotes fan-out ≥ 2 intermediates to
  `table` (+ Delta column mapping). Marts always `table` in `{build_schema}_marts` via `+schema:
  marts`.
- **Status**: Current behavior. Supersedes the earlier "default intermediate to table" entry below
  for new migrations.

---

## PROMOTED — transitive view-chain re-analysis on Spark/Databricks (historical)

- **Signature**: chained intermediate views each re-resolved full upstream logical plans.
- **First fix (2026-07-02):** defaulted all intermediates to `table` — worked but caused storage
  sprawl.
- **Current fix:** view default + fan-out table promotion via `apply_materialization.py` (see above).

---

## PROMOTED — Delta column mapping needed once intermediate/marts became physical tables

- **Signature**: `DELTA_INVALID_CHARACTERS_IN_COLUMN_NAMES` errors that did NOT exist before the
  view→table fix above — Domo columns routinely contain spaces/special characters that Spark SQL
  views tolerate freely but Delta's default physical format rejects.
- **Decision: PROMOTED** (a direct consequence of persisting intermediates/marts as tables).
  Delta column mapping on `table` and `marts` layers in `dbt_project.yml` / per-model config.
- **Status**: Fixed 2026-07-02; marts land in `{build_schema}_marts` via `+schema: marts`.

---

## AD-HOC — `coms_tickets` unsanitized identifier + `EffectiveClosedTime` collision

- **Signature**: a raw-SQL tile referencing a table name with an embedded space
  (`` `coms tickets` ``) that the converter passed through literally instead of sanitizing, plus a
  `CASE ... AS EffectiveClosedTime` that collided with a pre-existing column of the same name from
  `t.*`.
- **Decision: AD-HOC, not promoted.** This is a raw-SQL tile — the converter deliberately passes
  raw-SQL tile bodies through with minimal rewriting (see `tile-translation/references/paradigm.md`)
  because rewriting arbitrary customer SQL is out of scope and risks worse semantic drift than
  leaving it alone. The specific identifier and the specific collision are unique to this flow's
  source data, not a general converter defect. Patched locally in
  `models/intermediate/int_join_advisors_table_1.sql` (`` `coms tickets` `` → `` `coms_tickets` ``
  and `SELECT t.* EXCEPT (EffectiveClosedTime), CASE ... AS EffectiveClosedTime`). Re-apply after
  every regeneration of this specific flow.
- **Reconsider promotion if**: a second, independent flow hits a raw-SQL tile with a spaced table
  identifier — at that point it's a pattern (sanitize spaced identifiers in raw-SQL tile bodies
  generically) rather than a coincidence, and belongs in the converter.

---

## AD-HOC — schemaModification2 rename collision on a re-joined column

- **Signature**: a `LEFT OUTER JOIN` re-introducing columns from an earlier point in the same
  lineage (a "join back to an ancestor" pattern), where Domo's own `schemaModification2` config
  renamed the re-joined columns to `new.<col>` to disambiguate, but the converter's generic
  `select l.*, r.* except (...)` didn't apply that rename.
- **Decision: AD-HOC, not promoted.** Domo's rename-on-rejoin metadata isn't consistently present
  or structured across flows; hand-verifying the rename mapping per occurrence is safer than
  guessing a generic rule. Patched locally in `models/intermediate/int_join_data_26.sql`.
- **Reconsider promotion if**: this shows up on a second flow with the same `schemaModification2`
  shape — at that point it's worth teaching `project.py`'s join-tile mapper to read and apply that
  rename metadata directly instead of hand-patching.

---

## AD-HOC — stale view schema after `ALTER TABLE ... ADD COLUMNS`

- **Signature**: `UNRESOLVED_COLUMN` for a column that genuinely exists on the base table (per
  `information_schema.columns`) but isn't visible through a view built on top of it.
- **Root cause**: Spark freezes a view's `select *` expansion into an explicit column list at
  `CREATE VIEW` time. Adding columns to the base table afterward doesn't propagate until the view
  is recreated.
- **Decision: AD-HOC, not a converter bug** — this is an operational/environment quirk (dummy
  tables built incrementally during testing), not something the generated SQL causes. Not
  patchable in the converter at all. **Remediation**: `dbt build --select <view_model>+` to force
  the view (and everything downstream) to rebuild and re-expand against the current base-table
  schema.
