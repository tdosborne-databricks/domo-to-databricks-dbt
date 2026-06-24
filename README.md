# domo-to-dbt

A Claude Code plugin that converts **Domo Magic ETL dataflows** into a runnable
**dbt-databricks** project — one dbt model per Domo tile, with Domo Beast Mode / MySQL
dialect transpiled to Spark SQL automatically.

## What it does

Given a Domo flow export (`dataflows.json` + `dataset_mapping.json`), it generates a
layered dbt project (staging / intermediate / marts), rewrites Beast Mode expressions to
Spark SQL, and flags anything it can't translate deterministically. Sources wire to your
real Unity Catalog tables via an `overrides.json` mapping (`catalog.schema.table`).

## Install

Add this directory as a plugin in Claude Code (e.g. via your plugin marketplace, or point
Claude Code at this repo). Once installed, the `domo-to-dbt-converter` skill activates when
you ask to convert or migrate a Domo dataflow to dbt.

## Use

Ask Claude: *"Convert this Domo dataflow to dbt and run it on Databricks"* (point it at
your extract directory). Or run the bundled converter directly:

```bash
python3 skills/domo-to-dbt-converter/converter/convert_dataflow_to_dbt.py \
    <extract_dir> <out_dir> [overrides.json]
```

Then add a `profiles.yml` and `dbt build`. Full workflow:
`skills/domo-to-dbt-converter/references/workflow.md`.

## What's inside

- `skills/domo-to-dbt-converter/SKILL.md` — the skill (workflow + when to use).
- `skills/domo-to-dbt-converter/converter/` — the Python converter (no third-party deps) + 83 unit tests.
- `skills/domo-to-dbt-converter/references/` — workflow, dialect rules, tile-type mapping, real-data overrides.

## Requirements

- Python 3.9+ (converter uses only the standard library).
- `dbt-databricks` to build the generated project.
- A Databricks workspace + SQL warehouse.

## Status

v1 ships the converter with all 14 Domo tile types and automatic dialect transpilation.
Validated end-to-end on a real 272-tile flow (Databricks serverless warehouse). Known
follow-up: an option to materialize high-fan-out shared intermediates (compute-once reuse)
instead of inlining them as CTEs.
