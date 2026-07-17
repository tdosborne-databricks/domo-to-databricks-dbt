# streams.json schema (Domo Step-1 extract)

`streams.json` is a JSON **array** of stream objects. Each stream is the ingestion
configuration for one Domo DataSet (how data enters Domo from an external connector).

This reference documents the fields the source-resolution script reads. Domo has many
connectors; only the fields below are stable enough to parse generically.

## Join key

```
LoadFromVault.dataSourceId  ==  stream.dataSource.id
```

Also resolve the human name via `dataset_mapping.json[dataSourceId]`.

## Top-level stream fields

| Field | Use |
|---|---|
| `id` | Numeric stream id |
| `dataProvider.key` | **Connector type** (e.g. `ms-sql-server`, `google-sheets`) |
| `transport.type` | `CONNECTOR`, `GLOBAL`, `CLOUD`, `API`, `VIEW`, … |
| `updateMethod` | `REPLACE`, `UPSERT`, `APPEND` |
| `scheduleState` | `ACTIVE`, `MANUAL`, `INACTIVE` |
| `dataSource.id` | Dataset UUID (join key) |
| `configuration` | Array of connector-specific settings |

## configuration[] items

Each item:

```json
{
  "streamId": 2051,
  "category": "METADATA",
  "name": "query",
  "type": "string",
  "value": "..."
}
```

### Fields commonly used for source resolution

| `name` | Connectors | Meaning |
|---|---|---|
| `query` | SQL databases | Custom SQL (often T-SQL) |
| `generatedQuery` | SQL databases | Query-builder SQL when `query` is empty |
| `tableName` | SQL databases | Source table or view |
| `queryType` | SQL databases | `customQuery`, `queryBuilder`, … |
| `spreadsheetIDFileName` | Google Sheets | URL or spreadsheet id |
| `fileName` | Google Sheets / files | Spreadsheet id or path |
| `searchedFileName` | Google Sheets (search mode) | Spreadsheet id |
| `spreadsheetIDSheetName`, `sheetName`, `searchedSheetName` | Google Sheets | Tab name |
| `fileSelection` | Google Sheets | `spreadsheetID`, `discovery`, `search` |
| `_description_` | Most | Human description |

## Inputs with no matching stream

When `datasets.json` shows `type: DataFlow`, the input is an **upstream Magic ETL output**.
It has no connector stream. Map it with `ref()` to another dbt model, not `source()`.

## Scope

Only parse streams for **inputs of the flow being migrated**. The extract may contain
thousands of instance-wide streams; filter by the flow's `LoadFromVault` dataset ids.
