---
name: domo-ingestion
description: >-
  ALWAYS use this FIRST when starting a Domo→dbt migration, before any transpiling. Ingests a
  Domo Magic ETL export (customer-provided API export files OR a live Domo API pull), normalizes
  every dataflow into the standard internal graph schema, runs a completeness check, and produces
  the flow inventory. Triggers on "ingest Domo export", "Domo extract", "normalize Domo dataflow",
  "Domo flow inventory", "Magic ETL export", "dataflows.json", "start a Domo migration",
  "completeness check", "what Domo flows do we have".
---

# Domo Ingestion (dual-mode; extraction optional)

Turns whatever Domo export the customer delivered into a **normalized flow graph** the rest of
the pipeline consumes. Downstream skills (`tile-translation`, `org-dbt-conventions`,
`databricks-materialization-policy`, `migration-validation`) never care which mode produced the
graph — they only read `flows/<flow_id>.json` + `inventory.csv`.

**This is Step 1 of every migration batch. Run it before transpiling anything.**

## Two modes (same output)

- **Mode A — Provided export (this engagement's default).** The customer already ran their own
  Domo API scripts and handed us the export files. We parse them **as-is**, infer their structure,
  and normalize. We do NOT re-extract. See `references/export-format-mapping.md` (filled in once
  the real files are inspected — Build Sequence Step 2).
- **Mode B — Live API (optional, only when Domo credentials exist).** `scripts/domo_api_client.py`
  pulls dataflow definitions + datasets/Beast Modes/streams over read-only GETs and writes the
  **same export file set as Mode A** to a local folder — so `ingest_export.py` consumes it unchanged.
  Runs **locally** (no Databricks/`dbutils`/UC Volume); token via `--token` or `$DOMO_DEV_TOKEN`.
  Verified endpoints are recorded in `references/domo-api-endpoints.md`.

## Workflow

1. **Locate the export.** Point the ingester at the customer's export directory.
2. **Normalize** each dataflow into the internal graph schema (`references/normalized-graph-schema.md`):
   nodes = tiles (type + config), edges = tile dependencies, plus flow-level inputs/outputs/schedule.
3. **Completeness check.** Report missing fields — schedules, dataset schemas, row counts — so gaps
   are known **up front**, not discovered mid-migration. This directly feeds the "incomplete export"
   risk mitigation: surface gaps, request a supplemental export before the scaled batch.
4. **Inventory.** Emit `inventory.csv`: flow name, tile count, tile-type distribution (complexity
   score), inputs, outputs, schedule-if-known. This inventory drives coverage targets and the
   dependency ordering the batch runs in.

```bash
# Mode A — normalize a provided export (default):
python3 <skill_dir>/scripts/ingest_export.py <export_dir> <out_dir>
# → <out_dir>/flows/<flow_id>.json  +  <out_dir>/inventory.csv  +  <out_dir>/completeness_report.json

# Mode B — pull a fresh export live from Domo first (local, read-only), then normalize it:
export DOMO_DEV_TOKEN=xxxxxxxx   # Domo → Admin → Authentication → Access tokens
python3 <skill_dir>/scripts/domo_api_client.py --instance <subdomain> --flow-name "<filter>" --out ./domo_extract
python3 <skill_dir>/scripts/ingest_export.py ./domo_extract/extract_<ts> <out_dir>
```

## Output contract (both modes identical)

- `flows/<flow_id>.json` — one normalized graph per flow.
- `inventory.csv` — the migration backlog, complexity-scored.
- `completeness_report.json` — per-flow missing-field flags.

## References

- `references/normalized-graph-schema.md` — the internal graph schema (the contract).
- `references/magic-etl-json-schema.md` — Domo Magic ETL tile/edge encoding.
- `references/export-format-mapping.md` — the customer's actual export layout (mapped from the real AppDirect export).
- `references/domo-api-endpoints.md` — verified Domo API endpoints for Mode B.
- `references/file-sources.md` — Excel/CSV source files can't be read by Spark directly; land them
  in Unity Catalog first.
