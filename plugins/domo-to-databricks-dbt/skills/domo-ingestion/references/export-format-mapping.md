# Customer export format mapping

Inspected against the real AppDirect delivery; the Mode A parser (`scripts/ingest_export.py`)
implements the mapping below. Re-verify against each new delivery.

## Directory layout (AppDirect, Step-1 format)

Flat directory (no per-flow subfolders), UTF-8 JSON:

| File | Contents | Used by Mode A |
|---|---|---|
| `dataflows.json` | list of full flow definitions (`{id, name, actions:[...]}`) | **yes** (required) |
| `dataset_mapping.json` | `{dataset_id: dataset_name}` | yes (input/output names) |
| `datasets.json` | dataset metadata — **no column schema** | schema inference only |
| `complexity_report.json`, `streams.json`, `beast_modes.json`, `_manifest.json` | inventory / governance extras | not required |

## Field-by-field mapping (dataflows.json → normalized graph)

- `flow.id` → `flow_id` (stringified); `flow.name` → `name`.
- `flow.actions[]` → `tiles[]`: `action.id`→`id`, `action.type`→`type`, `action.name`→`name`,
  the whole raw action → `config` (tile-translation transpiles from this), and
  `dependsOn` \| `inputs` \| `input` → `depends_on`.
- `inputs[]` ← `LoadFromVault` tiles: dataset id from `dataSourceId` (also handles `datasetId`/
  `dataSetId`), name resolved via `dataset_mapping`.
- `outputs[]` ← `PublishToVault` tiles (in this export they carry **no** `dataSourceId`, so the
  output name comes from the tile name).
- `schedule` ← `triggerSettings`/`schedule`/`runSettings` — **absent in this export** → `unknown`.

## Completeness of the AppDirect export

- **Present:** flow graph, inputs (29), outputs (19), dataset id↔name mapping.
- **Missing:** schedules (no `triggerSettings`); input/output column schemas (`datasets.json`
  has no columns) → downstream infers source columns from tile field-refs.

## Format quirks the parser handles

- Dependency edges appear under any of `dependsOn` / `inputs` / `input` (string or list).
- Dataset id spelled `dataSourceId` here (other Domo versions: `datasetId`, `dataSetId`).
- `PublishToVault` outputs may lack a dataset id.
- Column names contain spaces / special chars / parens (e.g. `Account Manager`, `(CBOT)`) →
  sanitized for model/source names; marts enable Delta column mapping downstream.
