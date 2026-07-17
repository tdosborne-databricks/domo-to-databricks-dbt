# domo-source-resolution output contract

All artifacts land in `<workspace>/source_resolution/` (or a path the user chooses).

## Script output (`extract_flow_sources.py`)

| File | Purpose |
|---|---|
| `source_inventory.json` | Machine-readable inventory for the target flow |
| `source_inventory.md` | Human-readable report (SQL, sheet URLs, status) |
| `upstream_dataflows.json` | Inputs that are upstream Magic ETL outputs (`ref()`, not `source()`) |
| `overrides.template.json` | Keys pre-filled; values `null` until UC resolution completes |
| `source_resolution_status.json` | `pending` / `resolved` / `upstream_dataflows` id lists |

## Final deliverable (after interactive UC work)

| File | Purpose |
|---|---|
| `overrides.json` | **Ready for converter** — each Domo source → `catalog.schema.table` |

Pass `overrides.json` to `convert_dataflow_to_dbt.py` and `scaffold.py` (see
`org-dbt-conventions/references/real-data-overrides.md`).

### overrides.json format (plugin custom — not official dbt)

```json
{
  "fc92d485-b6fb-4894-905f-e55abe56ec3d": "main.project_dbt_src.quotes_report",
  "Quotes Report": "main.project_dbt_src.quotes_report"
}
```

Keys: Domo `dataSourceId`, raw dataset name, or sanitized source name (id wins).
Values: fully qualified Unity Catalog table — native Delta **or** foreign federated catalog.

Land replicated sources in `{project}_dbt_src` (bronze). dbt staging runs SQL on that bronze.

## source_inventory.json input object

```json
{
  "tile_name": "...",
  "dataset_name": "...",
  "data_source_id": "uuid",
  "connector_key": "ms-sql-server",
  "source_kind": "database",
  "sql": {
    "query": "...",
    "table_name": "dbo.uvw_...",
    "referenced_tables": ["McPartner", "AbpUsers"]
  },
  "file": {
    "spreadsheet_url": "https://docs.google.com/...",
    "sheet_name": "Sheet1"
  },
  "resolution": {
    "status": "pending",
    "uc_table": null,
    "ingestion_approach": null,
    "notes": null
  }
}
```

After UC discovery, update `resolution.status` to `resolved`, set `uc_table` and
`ingestion_approach` (`lakeflow_connect`, `lakehouse_federation`, `existing_native`,
`file_landing`, `domo_native`), then copy resolved mappings into `overrides.json`.

## Hard gate

`source_resolution_status.json` → `pending` must be **empty** before tile-translation,
unless the user explicitly defers specific sources and accepts downstream `dbt build` gaps.
