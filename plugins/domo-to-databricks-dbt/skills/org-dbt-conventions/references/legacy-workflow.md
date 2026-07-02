# End-to-end workflow

Detailed steps for converting and building a Domo dataflow as dbt on Databricks.

## 0. Prerequisites

- Python 3.9+ (standard library only — the converter has no third-party deps). The
  conversion step needs nothing else and no credentials.
- For the build: `dbt-databricks` (`pip install dbt-databricks`) if running dbt yourself, or
  just a Databricks Workflows **dbt task** (no local install). A Databricks workspace + SQL
  warehouse. Auth via the dbt task's job identity or OAuth — see `authentication.md`.

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

`<skill_dir>` is `.../skills/domo-to-databricks-dbt-converter`. Run it via the script's path from any
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

## 5. dbt profile / authentication

> **On Databricks (esp. serverless), prefer a Workflows dbt task** — it builds the project
> authenticated as the job's run-as identity, with no token and no `profiles.yml` to manage.
> If you run dbt yourself, use **OAuth** (service principal), not a PAT in an env var —
> serverless compute doesn't expose one. Full guidance + the dbt-task config: `authentication.md`.


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
      catalog: main
      schema: domo_migration_dbt
      threads: 8
      auth_type: oauth                  # OAuth M2M (service principal):
      client_id: "{{ env_var('DATABRICKS_CLIENT_ID') }}"
      client_secret: "{{ env_var('DATABRICKS_CLIENT_SECRET') }}"
```

(SP secret belongs in a Databricks secret scope / your env, not the file. PAT `token:` works
locally but fails on serverless — see `authentication.md`.)

## 6. Build

Running dbt yourself:
```bash
dbt debug --project-dir <out_dir> --profiles-dir <out_dir>   # verify connection
dbt build --project-dir <out_dir> --profiles-dir <out_dir>
```
Or, on Databricks, a Workflows **dbt task** (no token; see `authentication.md`):
```yaml
- task_key: build_dbt
  dbt_task:
    project_directory: <out_dir>
    commands: ["dbt build"]
    warehouse_id: "<sql_warehouse_id>"
    catalog: main
    schema: domo_migration_dbt
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
