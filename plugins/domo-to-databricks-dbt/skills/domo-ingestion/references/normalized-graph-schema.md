# Normalized flow graph schema (the internal contract)

Both ingestion modes produce this. Downstream skills read only this — never the raw Domo export.

## `flows/<flow_id>.json`

```jsonc
{
  "flow_id": "67",
  "name": "Advisor_Services_ETL",
  "schedule": { "type": "cron|dataset-update|unknown", "expr": "0 6 * * *", "source": "export|default" },
  "inputs":  [ { "dataset_id": "...", "name": "...", "schema": [ {"name":"col","type":"STRING"} ] | null } ],
  "outputs": [ { "dataset_id": "...", "name": "...", "tile_id": "..." } ],
  "tiles": [
    {
      "id": "t_12",
      "type": "MergeJoin",            // Domo action type
      "name": "Join orders + customers",
      "config": { /* raw tile config, verbatim — tile-translation transpiles this */ },
      "depends_on": [ "t_9", "t_11" ] // reconstructed edges (topological input)
    }
  ]
}
```

Rules:
- `depends_on` is the **reconstructed** edge set (from Domo `dependsOn`/`inputs`/`input`).
- `schema: null` on an input means the export didn't declare column types → flagged by the
  completeness check; `tile-translation` falls back to referenced-column inference.
- `schedule.source: "default"` means we applied a default because the export lacked one →
  flagged for customer confirmation at handoff.

## `inventory.csv`

`flow_id, flow_name, tile_count, tile_types (semicolon list), input_count, output_count, schedule_known (bool), complexity_score`

Complexity score = tile count weighted by hard tile types (SQL, DateCalculator, Normalizer). Drives
coverage targets and the dependency order the batch runs in.

## `completeness_report.json`

Per-flow list of missing fields (`schema`, `schedule`, `row_counts`, ...) so gaps are known before
transpiling — request a supplemental export before the scaled batch.
