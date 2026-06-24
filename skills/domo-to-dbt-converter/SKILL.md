---
name: domo-to-dbt-converter
description: Use when migrating a Domo Magic ETL dataflow to dbt on Databricks — converting a Domo flow export (dataflows.json) into a runnable dbt-databricks project, replacing Domo Beast Mode / Magic ETL pipelines, or standing up dbt models from a Domo dataflow extract. Triggers on "Domo to dbt", "convert Domo dataflow", "migrate Domo ETL", "Domo Magic ETL", "Beast Mode to Spark".
---

# Domo → dbt Converter

Converts a Domo Magic ETL dataflow into a dbt-databricks project — **one dbt model per Domo tile**, layered into staging / intermediate / marts. Domo Beast Mode / MySQL ("Magic") dialect inside formula, filter, group-by, SQL, and DateCalculator tiles is **transpiled to Spark SQL automatically** at conversion time; anything that can't be translated deterministically is flagged for manual review.

**Core idea:** the Domo flow is a DAG of typed tiles. Each tile maps to one dbt model; the converter resolves dependencies, rewrites dialect, and emits a project you build with `dbt-databricks`. Sources are wired to your **real Unity Catalog tables** via an `overrides.json` mapping (`catalog.schema.table`), so the generated models read your actual data.

## When to use

- You have a Domo dataflow export (`dataflows.json` + `dataset_mapping.json`) and want dbt models on Databricks.
- You are replacing Domo Beast Mode / Magic ETL with dbt + Spark SQL.
- You want to validate a Domo migration end-to-end before customers have moved their source data.

## Inputs you need

The converter reads a **Domo extract directory** containing:
- `dataflows.json` — the flow definition (the tile DAG). One or more flows.
- `dataset_mapping.json` — maps Domo dataSourceId → dataset name.

(These come from the Domo API / a Step-1 extraction. See `references/workflow.md` for what each file must contain.)

## What a session looks like (end-to-end)

A user installs the plugin, points the agent at a Domo extract, and says whether they have real source data yet. The agent then drives a loop: **convert → (wire sources) → build → triage → surface gaps to the user → fix → repeat.** Most of this is automatic; the agent pauses for the user only on semantic choices (e.g. business-day vs calendar days), source-table mapping, and approvals. The agent reports build PASS/ERROR counts and a prioritized gap list each round, and stops when marts build (or the user calls it).

## Workflow

1. **Convert.** Run the bundled CLI to generate the dbt project and a conversion report. Conversion needs only **Python 3.9+ (standard library)** — no install. Run it with the script's path (it works from any working directory; the script adds its own directory to the import path):
   ```bash
   python3 <skill_dir>/converter/convert_dataflow_to_dbt.py <extract_dir> <out_dir> [overrides.json]
   ```
   where `<skill_dir>` is this skill's directory (`.../skills/domo-to-dbt-converter`). `<extract_dir>` only needs `dataflows.json` + `dataset_mapping.json` (other files in a Domo export are ignored). Output: a dbt project under `<out_dir>` (models/staging, models/intermediate, models/marts, sources.yml, dbt_project.yml) and `conversion_report.json`.

2. **Wire sources to real tables.** Provide an `overrides.json` mapping each Domo source → its Unity Catalog table (`catalog.schema.table`), passed as the 3rd CLI arg; `sources.yml` then resolves `{{ source('domo', name) }}` to the real table. Sources still missing a mapping are listed in `conversion_report.json → sources_needing_table` so you can see what's left to wire. See `references/real-data-overrides.md`. **A source that isn't a UC table yet (an Excel/CSV export) must be landed in Unity Catalog first, then mapped** — see `references/file-sources.md` (Excel can't be read by Spark directly).

3. **Build.** The recommended way to run the build on Databricks is a **Workflows dbt task** (`project_directory` + `commands: ["dbt build"]` + `warehouse_id` + `catalog`/`schema`) — it authenticates as the job's run-as identity, so there's **no token to manage**. To run dbt yourself instead, add a `profiles.yml` using **OAuth** (service principal), then `dbt build --project-dir <out_dir> --profiles-dir <out_dir>`. **Do not rely on a PAT in an env var — serverless compute doesn't expose one.** See `references/authentication.md`.

4. **Surface & correct (the loop).** Collect every gap from BOTH discovery points and **present them to the user** — grouped, prioritized, each with a proposed fix. **Never silently leave a mart failing or drop a flagged tile.** Then apply the correction guide below, regenerate, rebuild, and re-report until clean or the user stops. (See "Surfacing gaps & the correction loop".)

Marts sit at the end of deep chains; because intermediates inline as CTEs, a mart fails at the *first* error in its chain — fix it and the *next* is exposed, so iterate.

## Surfacing gaps & the correction loop

**The agent must surface every gap to the user — not work around it silently.** Gaps appear at two points; check both on every run:

1. **At conversion** — `conversion_report.json → needs_review` (and `-- NEEDS REVIEW:` banners in the generated models). Covers known-hard tiles: raw SQL, positional UNION, and a denylist of common MySQL-only functions (`STR_TO_DATE`, `GROUP_CONCAT`, `TIMESTAMPDIFF`, …). When a new flow uses a function not yet covered, add it to `_UNSUPPORTED_FUNCS` in `tiles.py` (and a `transpile_expr` rule if it has a clean Spark equivalent).
2. **At build** — parse `<out_dir>/target/run_results.json`. This is the comprehensive safety net: an `UNRESOLVED_ROUTINE` is almost always an **uncovered Beast Mode/MySQL function** that slipped through — name it for the user.

### Correction guide

| Gap (where seen) | Agent action |
|---|---|
| Uncovered dialect function — flagged at conversion, or `UNRESOLVED_ROUTINE` / `PARSE_SYNTAX_ERROR` at build | Identify the function. **Clear Spark equivalent** → add a `transpile_expr` rule **test-first** (`references/dialect-rules.md`), regenerate, rebuild. **Ambiguous semantics** (business-day, timezone, rounding) → **ask the user** before choosing. |
| Raw `SQL` tile (always flagged) | Show the SQL; rewrite to Spark with the user, or confirm it already runs. |
| Positional `UNION` (flagged) | Verify leg column order/count with the user; rebuild with explicit columns if they differ. |
| `UNRESOLVED_COLUMN` | A referenced column isn't in the wired source table — confirm the `overrides.json` mapping points at the right table and that the column exists there. |
| `COLUMN_ALREADY_EXISTS` (join) | Disambiguate the colliding columns in the join. |
| Anything with ambiguous intent | **Stop and ask the user — never guess.** |

### Automatic vs needs-the-user

- **Automatic:** running convert/build, triaging results, and applying deterministic dialect rules (a function with one obvious Spark equivalent).
- **Needs the user:** semantic choices (e.g. business-day vs calendar days, timezone assumptions), which Domo sources map to which real UC tables, and accepting any approximation.

## What is auto-translated vs flagged

The converter rewrites Beast Mode / MySQL → Spark SQL automatically: comment styles (`#`, `--`, `/* */`), `IFNULL`, `CURDATE`/`NOW`, `DATE_ADD(x, INTERVAL n DAY)`, `CONVERT_TZ`, `DATE_FORMAT('%Y-%m')`, `REGEXP_LIKE(...,'i')`, `DATETIME()` cast, `CAST(... AS CHAR)`, and `DATE_WORKING_DIFF` (→ exact Mon–Fri business-day formula). Full list and how to add rules: `references/dialect-rules.md`.

Flagged (manual): raw SQL tiles (arbitrary MySQL), positional UNIONs (Domo aligns by column name; Spark SQL has no `UNION BY NAME`), non-UTC `CONVERT_TZ`, and any unrecognized dialect.

## Tile coverage

All 14 Domo Magic ETL tile types are mapped (LoadFromVault, Filter, GroupBy, ExpressionEvaluator, MergeJoin, SelectValues, Metadata, Unique, UnionAll, WindowAction, Normalizer, DateCalculator, SQL, PublishToVault). Per-tile mapping details: `references/tile-types.md`.

## Architecture (for extending the converter)

`converter/domo_to_dbt/` is a small Python package:
- `tiles.py` — one `m_<type>()` mapper per tile → SQL.
- `common.py` — `transpile_expr()` (the dialect engine) + filter/where helpers.
- `dag.py` — topological sort + dependency resolution.
- `lineage.py` — `produced_columns()` column tracking through the DAG.
- `sources.py` — LoadFromVault → UC table resolution (overrides) + referenced-column inference.
- `project.py` — assembles models, writes the dbt project.

Add a dialect rule or tile mapper **test-first** (`converter/tests/`, 83 tests). Run `python3 -m pytest` from `converter/`.

## Examples

### Example: convert against the customer's real tables
User says: "Convert this Domo flow to dbt and run it on Databricks."
→ Map each Domo source to its UC table in `overrides.json`, run the CLI with it, add a `profiles.yml`, `dbt build`. Report the build PASS/ERROR count, the `needs_review` tiles, and any `sources_needing_table` still unmapped. See `references/real-data-overrides.md`.

### Example: a mart fails with UNRESOLVED_ROUTINE / PARSE_SYNTAX
→ It's almost always Beast Mode dialect. Check `references/dialect-rules.md`; add a `transpile_expr` rule test-first, or fix the flagged tile by hand.
