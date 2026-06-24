# End-to-end workflow

Detailed steps for converting and building a Domo dataflow as dbt on Databricks.

## 0. Prerequisites

- Python 3.9+ (standard library only — the converter has no third-party deps).
- `dbt-databricks` for building: `pip install dbt-databricks`.
- A Databricks workspace + SQL warehouse, and auth (OAuth/CLI or a PAT).

## 1. Get the Domo extract

The converter needs a directory with:

- **`dataflows.json`** — the flow definition. Either a single flow object or a list of flows. Each flow has `name` and `actions` (the tiles). Each action has `id`, `type`, and type-specific fields (e.g. `expressions`, `filterList`, `keys1/keys2`, `calculations`). Dependencies are read from `dependsOn` / `inputs` / `input`.
- **`dataset_mapping.json`** — `{ "<dataSourceId>": "<dataset name>", ... }`, used to name `LoadFromVault` sources.

These are produced by a Domo API extraction (the "Step 1" inventory). If you only have one flow, that's fine.

> Note: the Domo extract does **not** contain real source column schemas (Domo stores those on the dataset, not in the flow). The converter infers source columns from how tiles reference them. For row-level fidelity, use real data via `overrides.json` (step 4b).

## 2. Convert

```bash
python3 <skill_dir>/converter/convert_dataflow_to_dbt.py <extract_dir> <out_dir> [overrides.json]
```

`<skill_dir>` is `.../skills/domo-to-dbt-converter`. Run it via the script's path from any
working directory — the script puts its own directory on `sys.path`, so the `domo_to_dbt`
package imports resolve regardless of cwd. (If you prefer relative paths, `cd <skill_dir>`
first and use `converter/convert_dataflow_to_dbt.py`.)

Prints model count, needs-review tile count, and the count of sources still needing a real-table mapping. Writes:

- `<out_dir>/dbt_project.yml`, `<out_dir>/models/sources.yml`
- `<out_dir>/models/{staging,intermediate,marts}/*.sql`
- `<out_dir>/conversion_report.json`

Materialization: staging → **view**, intermediate → **ephemeral** (inlined as CTEs, not separate objects), marts → **table**. So N tiles produce far fewer objects than N (only staging views + mart tables are real relations).

## 3. Read the conversion report

`conversion_report.json` keys:
- `flows` — name + model count per flow.
- `needs_review` — tiles needing manual attention (`model`, `type`, `note`).
- `sources_needing_table` — sources with no real UC table mapping yet (each has `name` + inferred `inferred_columns`, the columns the flow references from that source).

## 4. Wire sources to real tables (overrides.json)

See `real-data-overrides.md`. Build an `overrides.json` mapping each Domo source to its real UC table, pass it as the 3rd CLI arg, and `sources.yml` wires `{{ source('domo', name) }}` to the real `catalog.schema.table`. Anything left in `sources_needing_table` still needs a mapping before its downstream models can build.

## 5. dbt profile

Create `<out_dir>/profiles.yml`. The profile name must match `dbt_project.yml`'s `profile:`,
which the converter derives from your flow name and **prints** ("dbt project/profile name: …").
Use that exact name as the top key (shown here as `<project_name>`):

```yaml
<project_name>:            # e.g. the sanitized flow name the CLI printed
  target: dev
  outputs:
    dev:
      type: databricks
      host: <workspace-host>            # no https://
      http_path: /sql/1.0/warehouses/<warehouse-id>
      auth_type: oauth                  # or token: <pat>
      catalog: main
      schema: domo_migration_dbt
      threads: 8
```

## 6. Build

```bash
dbt debug --project-dir <out_dir> --profiles-dir <out_dir>   # verify connection
dbt build --project-dir <out_dir> --profiles-dir <out_dir>
```

Marts are Delta tables; the converter sets `delta.columnMapping.mode=name` so Domo column names with spaces/`#` are allowed.

## 7. Triage failures

Categorize `dbt build` errors (read `<out_dir>/target/run_results.json`):

| Error | Cause | Fix |
|---|---|---|
| `UNRESOLVED_ROUTINE`, `PARSE_SYNTAX_ERROR` | Beast Mode / MySQL dialect | Add a `transpile_expr` rule (see `dialect-rules.md`) or fix the flagged tile. |
| `UNRESOLVED_COLUMN` | a referenced column isn't in the wired source table | Confirm the `overrides.json` mapping points at the right table and the column exists there. |
| `AMBIGUOUS_REFERENCE` | a tile re-creates a column that already exists (Domo replaces, Spark duplicates) | Column lineage `EXCEPT`s known cases; if it persists, the source table carries a column the flow also computes — confirm the mapped table's schema. |
| `COLUMN_ALREADY_EXISTS` | join brings same-named columns from both sides | Disambiguate the join (flagged in the model). |

Marts sit at the end of deep chains. Because intermediate tiles inline as CTEs, a mart fails at the **first** error in its chain — fix it and the **next** is exposed. A mart only turns green when its whole chain is clean.

## 8. Validate the dialect engine itself

```bash
cd converter && python3 -m pytest -q     # 83 tests
```

Add any new rule test-first.
