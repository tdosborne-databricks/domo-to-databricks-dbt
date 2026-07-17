---
name: domo-source-resolution
description: >-
  Use AFTER domo-ingestion and BEFORE tile-translation on a Domo→dbt migration. Extracts
  connector metadata from streams.json for the target flow only, searches Unity Catalog with
  the user to find or plan bronze sources, and produces overrides.json mapping each Domo
  LoadFromVault input to catalog.schema.table. Triggers on "resolve Domo sources", "streams.json",
  "overrides.json", "Unity Catalog source mapping", "Lakeflow Connect", "Lakehouse Federation",
  "bronze sources", "wire Domo inputs to UC".
---

# Domo Source Resolution (streams → Unity Catalog → overrides.json)

Domo Magic ETL inputs must resolve to **queryable Unity Catalog relations** before dbt models
can build. This skill closes the gap between the Domo extract (`streams.json`) and the plugin's
`overrides.json` contract (see `org-dbt-conventions/references/real-data-overrides.md`).

**`overrides.json` is plugin-custom** — it is not from official dbt skills. The converter reads
it and generates standard dbt `sources.yml` with real `database` / `schema` / `identifier`.

<HARD-GATE>
Step 2 of the fixed pipeline:

  domo-ingestion → **domo-source-resolution** → tile-translation → org-dbt-conventions → …

Requires `dataflows.json` + `dataset_mapping.json` in the export; `streams.json` strongly
recommended. Do not run tile-translation until every non-upstream input is either mapped in
`overrides.json` or explicitly deferred by the user. Hand off `overrides.json` to
tile-translation and org-dbt-conventions.
</HARD-GATE>

## What this skill does

1. **Extract** — flow-scoped join: `LoadFromVault` → `streams.json` (this flow only).
2. **Classify** — database / file / SaaS / Domo-native / upstream DataFlow.
3. **Discover** — search UC with the user (interactive); use official Databricks skills.
4. **Plan gaps** — Lakeflow Connect, Lakehouse Federation, or file landing when bronze missing.
5. **Emit** — validated `overrides.json` pointing at bronze (native Delta or foreign catalog).

Domo stream SQL tells you **what tables/views Domo read** — use it to search UC. dbt staging
runs Spark SQL **on bronze**, not on a pre-materialized copy of Domo's exact query output.

Upstream Magic ETL inputs (`type: DataFlow`, no stream) → `ref()` another dbt model, **not**
`source()` — exclude from `overrides.json`.

## Required Databricks skill overlays

Load these from marketplace `databricks-agent-skills` (plugin dependency). **Do not** maintain
parallel ingestion docs in this skill — delegate to the official skills.

| When | Load skill |
|---|---|
| Auth, profile, UC search | **`databricks-core`** — `discover-schema`, `query` aitools |
| Replicate DB/SaaS into UC | **`databricks-lakeflow-connect`** |
| Query-in-place (foreign catalog) | **`databricks-dbsql`** (+ federation refs in serverless-migration) |
| Unfamiliar connector / API | **`databricks-docs`** — `https://docs.databricks.com/llms.txt` |
| Land files to UC volumes | **`databricks-unity-catalog`** + `domo-ingestion/references/file-sources.md` |

Ask the user to confirm the Databricks **profile** before any UC search (`databricks-core` rule:
never auto-select a profile).

## Ingestion decision tree (summary)

Full detail: `databricks-lakeflow-connect/references/4-ingestion-decision-tree.md`.

```
For each Domo input (database / SaaS / file):

Need a governed bronze copy in UC for dbt builds?
├─ YES — operational DB or SaaS with a Lakeflow Connect connector
│         → load databricks-lakeflow-connect (SQL Server GA, Postgres/MySQL PuPr, …)
├─ YES — files (Google Sheets, CSV, xlsx)
│         → land to UC (volume + ingest job); see file-sources.md
├─ NO — low query volume, data stays in source, acceptable federation latency
│         → load databricks-dbsql (CREATE CONNECTION + CREATE FOREIGN CATALOG)
└─ Already in UC?
          → map existing catalog.schema.table in overrides.json

Then: databricks-core → search UC for table/view names from streams.json SQL metadata
Then: ask user to confirm match → write overrides.json
```

**Common mistake:** both Connect and Federation use a UC `CONNECTION`. Connect **materializes**
to Delta; Federation **queries through** to the source.

## Workflow

### 1. Extract (deterministic)

```bash
python3 <skill_dir>/scripts/extract_flow_sources.py <export_dir> <out_dir> \
  --flow-id <id>
# or: --flow-name "Advisor_Services_ETL"
```

Review `source_inventory.md` and `source_inventory.json`.

### 2. UC discovery (interactive — agent + user)

For each input with `resolution.status: pending`:

1. Show connector type, table names / SQL excerpt / sheet URL from inventory.
2. **Ask the user:** does this already exist in UC? Which catalog/schema?
3. If unknown, run UC search via `databricks-core`:
   ```bash
   databricks experimental aitools tools discover-schema <catalog>.<schema>.<table> --profile <PROFILE>
   databricks experimental aitools tools query "SHOW TABLES IN <catalog>.<schema> LIKE '*quotes*'" --profile <PROFILE>
   ```
4. Present candidates; user confirms the bronze path.

### 3. Gap resolution (interactive)

If no UC match:

1. Present Connect vs Federation using the decision tree above.
2. Load the appropriate Databricks skill and draft setup (pipeline JSON, `CREATE FOREIGN CATALOG`, etc.).
3. Agree on **target bronze path** (e.g. `main.project_dbt_src.quotes_report` or `foreign_aso.dbo.uvw_0200_DomoQuotesReport`).
4. User executes or approves ingestion; re-run UC discovery to confirm.

### 4. Write overrides.json

Copy resolved `catalog.schema.table` paths into `overrides.json` (see `output-contract.md`).
Update `source_resolution_status.json` — `pending` should be empty.

### 5. Hand off

Pass `overrides.json` to `tile-translation` / `convert_dataflow_to_dbt.py` and
`org-dbt-conventions` / `scaffold.py`.

## References (this skill only)

- `references/streams-schema.md` — fields read from `streams.json`
- `references/output-contract.md` — artifacts and `overrides.json` format
- `org-dbt-conventions/references/real-data-overrides.md` — how overrides wire `sources.yml`
- `domo-ingestion/references/file-sources.md` — Excel/CSV landing

## What NOT to do

- Do not parse all streams in the extract — **only the target flow's inputs**.
- Do not generate per-run connector pattern docs — use Databricks skills + official docs.
- Do not skip user confirmation on UC matches — wrong overrides cause `UNRESOLVED_COLUMN` in dbt.
- Do not put upstream DataFlow outputs in `overrides.json`.
