---
name: tile-translation
description: >-
  Use to transpile a normalized Domo flow graph into Spark SQL for dbt on Databricks. Walks the
  tile DAG, emits each tile chain as ordered CTEs inside one model per flow (NOT one model per
  tile), rewrites Domo Beast Mode / MySQL dialect to Spark SQL, and flags anything it can't
  translate deterministically as a `-- TODO` block for agent judgment. Triggers on "transpile Domo
  tiles", "Domo tile to SQL", "Beast Mode to Spark", "convert Domo flow to dbt SQL", "Magic ETL to
  Spark SQL", "tile mapping", "semantic gotchas", "Domo dialect". Run AFTER domo-ingestion.
---

# Tile Translation (Domo tiles → Spark SQL CTEs)

This is a **decompilation** problem, not a syntax conversion: Magic ETL logic lives in serialized
GUI state (tile config JSON). We recover intent and re-express it as idiomatic Spark SQL. Read
`references/paradigm.md` first — it is the conceptual foundation.

<HARD-GATE>
Step 2 of the fixed pipeline (domo-ingestion → **tile-translation** → org-dbt-conventions →
databricks-materialization-policy → dbt-error-triage → migration-validation).
`flows/<flow_id>.json` + `inventory.csv` from domo-ingestion — do not hand-author a flow graph.
Do not skip straight to databricks-materialization-policy on your own judgment; org-dbt-conventions
must scaffold the project first.
</HARD-GATE>

## The granularity rule (the central judgment call)

**Tile ≠ model. Flow ≠ always one model.**

- **Tile chains → CTEs** inside a single model, named after the source tiles (traceability).
- **A flow → usually one model.**
- **Split into multiple models only** at reuse boundaries, materialization points, or where a
  flow-to-flow handoff existed in Domo.
- **Merge flows** that were only split apart to work around Domo engine/dataset-size limits.

The engine under `scripts/converter/` implements this: a tile becomes its own model **only** if
it's a boundary — a source (`LoadFromVault` → staging view), a sink (`PublishToVault`/terminal →
marts table), or a reuse point (out-degree ≥ 2 → intermediate view). Every other tile has
out-degree 1 and collapses into exactly one boundary's model as a named CTE. On the AppDirect flow
this turns **272 tiles into 70 models** (29 staging + 22 intermediate + 19 marts), ~200 tiles
inlined as CTEs. Each model carries a traceability header listing the Domo flow + tiles it came from.

## Workflow

1. **Transpile** each tile's config → a SQL fragment (deterministic; `scripts/converter`).
2. **Graph-walk**: topologically sort tiles and emit them as ordered CTEs (target) / dependency-
   resolved models (current).
3. **Flag** untranslatable tiles as `-- TODO` blocks with the raw config attached — the agent
   resolves these using `references/semantic-gotchas.md` and judgment. **Never silently drop or
   guess a flagged tile.**
4. Hand off to `org-dbt-conventions` (structure/tests), then
   `databricks-materialization-policy` (apply view/table defaults) before the first `dbt build`.

```bash
python3 <skill_dir>/scripts/converter/convert_dataflow_to_dbt.py <extract_dir> <out_dir> [overrides.json]
# → dbt models + conversion_report.json (needs_review = the manual worklist)
```

## Deterministic scripts do the translation; the agent's judgment is reserved for edge cases

The transpiler auto-rewrites Beast Mode / MySQL → Spark SQL: comment styles, `IFNULL`→`coalesce`,
`CURDATE`/`NOW`, `DATE_ADD(x, INTERVAL n DAY)`, `CONVERT_TZ`→`from_utc_timestamp`,
`DATE_FORMAT` codes, `REGEXP_LIKE` flags, `DATETIME()` cast, `DATE_WORKING_DIFF` (business-day
formula), and more. Full list + how to add a rule test-first: `references/semantic-gotchas.md`.

Flagged (needs the agent / user): raw SQL tiles, positional UNION (Spark has no `UNION BY NAME`),
non-UTC `CONVERT_TZ`, and any unrecognized dialect. **Semantic drift is the silent failure mode** —
a literal translation that "looks right" can return different rows (null handling, type coercion,
regex dialect, date/timezone defaults). `semantic-gotchas.md` doubles as the validation triage guide.

## References

- `references/paradigm.md` — why this isn't a 1:1 translation (read first).
- `references/tile-mapping.md` — every Magic ETL tile type → Spark SQL, with worked examples.
- `references/semantic-gotchas.md` — Domo vs. Spark SQL behavioral differences + dialect rules
  (also the Tier-3 mismatch triage guide).

## Extending the transpiler (test-first)

`scripts/converter/domo_to_dbt/` is a pure-stdlib package: `tiles.py` (one `m_<type>()` per tile),
`common.py` (`transpile_expr()` dialect engine), `dag.py` (topological sort), `lineage.py` (column
tracking), `sources.py` (source resolution), `project.py` (project assembly). Add a rule with a
failing test in `scripts/converter/tests/` first, then `python3 -m pytest` from `scripts/converter/`.
