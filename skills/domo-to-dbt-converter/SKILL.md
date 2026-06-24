---
name: domo-to-dbt-converter
description: Use when migrating a Domo Magic ETL dataflow to dbt on Databricks — converting a Domo flow export (dataflows.json) into a runnable dbt-databricks project, replacing Domo Beast Mode / Magic ETL pipelines, or standing up dbt models from a Domo dataflow extract. Triggers on "Domo to dbt", "convert Domo dataflow", "migrate Domo ETL", "Domo Magic ETL", "Beast Mode to Spark".
---

# Domo → dbt Converter

Converts a Domo Magic ETL dataflow into a dbt-databricks project — **one dbt model per Domo tile**, layered into staging / intermediate / marts. Domo Beast Mode / MySQL ("Magic") dialect inside formula, filter, group-by, SQL, and DateCalculator tiles is **transpiled to Spark SQL automatically** at conversion time; anything that can't be translated deterministically is flagged for manual review.

**Core idea:** the Domo flow is a DAG of typed tiles. Each tile maps to one dbt model; the converter resolves dependencies, rewrites dialect, and emits a project you build with `dbt-databricks`. Sources are wired either to **synthetic** stand-in tables (for a demo with no real data) or to **real Unity Catalog tables** via an `overrides.json` — the same generated project runs against either, no code change.

## When to use

- You have a Domo dataflow export (`dataflows.json` + `dataset_mapping.json`) and want dbt models on Databricks.
- You are replacing Domo Beast Mode / Magic ETL with dbt + Spark SQL.
- You want to validate a Domo migration end-to-end before customers have moved their source data.

## Inputs you need

The converter reads a **Domo extract directory** containing:
- `dataflows.json` — the flow definition (the tile DAG). One or more flows.
- `dataset_mapping.json` — maps Domo dataSourceId → dataset name.

(These come from the Domo API / a Step-1 extraction. See `references/workflow.md` for what each file must contain.)

## Workflow

1. **Convert.** Run the bundled CLI to generate the dbt project and a conversion report. Conversion needs only **Python 3.9+ (standard library)** — no install. Run it with the script's path (it works from any working directory; the script adds its own directory to the import path):
   ```bash
   python3 <skill_dir>/converter/convert_dataflow_to_dbt.py <extract_dir> <out_dir> [overrides.json]
   ```
   where `<skill_dir>` is this skill's directory (`.../skills/domo-to-dbt-converter`). `<extract_dir>` only needs `dataflows.json` + `dataset_mapping.json` (other files in a Domo export are ignored). Output: a dbt project under `<out_dir>` (models/staging, models/intermediate, models/marts, sources.yml, dbt_project.yml) and `conversion_report.json`.

2. **Pick a source mode.**
   - **Synthetic (demo / no real data):** sources without an override are listed in `conversion_report.json → sources_needing_synthetic`. Create stand-in Delta tables with `converter/dbt_validation/gen_synthetic_sources.py` (string-typed columns, inferred from tile references). See `references/workflow.md`.
   - **Real data:** pass an `overrides.json` mapping each Domo source → a real UC table (`catalog.schema.table`). See `references/real-data-overrides.md`. This is the production path.

3. **Add a dbt profile** (`profiles.yml`) pointing at a Databricks SQL warehouse, then build:
   ```bash
   dbt build --project-dir <out_dir> --profiles-dir <out_dir>
   ```

4. **Review flagged tiles.** `conversion_report.json → needs_review` lists tiles the converter could not fully auto-translate (raw SQL tiles, positional UNIONs, un-transpilable dialect). Each generated model also carries a `-- NEEDS REVIEW:` banner. Fix these by hand or extend the converter. See `references/dialect-rules.md`.

5. **Iterate.** Re-run convert → build → review. Marts sit at the end of deep chains; a mart only builds once every blocker in its chain clears.

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
- `sources.py` — LoadFromVault → UC table / synthetic column inference.
- `project.py` — assembles models, writes the dbt project.

Add a dialect rule or tile mapper **test-first** (`converter/tests/`, 83 tests). Run `python3 -m pytest` from `converter/`.

## Examples

### Example: convert and run a demo on synthetic data
User says: "Convert this Domo flow to dbt and prove it runs on Databricks."
→ Run the CLI, generate synthetic sources from `sources_needing_synthetic`, add a `profiles.yml`, `dbt build`. Report the build PASS/ERROR count and the `needs_review` tiles.

### Example: convert against the customer's real tables
User says: "We have the source tables in Unity Catalog now — wire them up."
→ Build an `overrides.json` (Domo source → `catalog.schema.table`), re-run the CLI with it, `dbt build`. See `references/real-data-overrides.md`.

### Example: a mart fails with UNRESOLVED_ROUTINE / PARSE_SYNTAX
→ It's almost always Beast Mode dialect. Check `references/dialect-rules.md`; add a `transpile_expr` rule test-first, or fix the flagged tile by hand.
