# Wiring real source data with overrides.json

By default every Domo `LoadFromVault` source resolves to a **synthetic** table in the
build schema. To run against **real** Unity Catalog tables (the production path), supply
an `overrides.json` as the 3rd argument to the converter.

## Format

Map a Domo source → a fully-qualified UC table (`catalog.schema.table`). Keys may be the
Domo `dataSourceId`, the raw dataset name, or the sanitized source name — id wins if both
are present.

```json
{
  "061e91e3-18ee-4051-a503-9f4390015496": "main.appdirect_raw.asm_portal_slas",
  "Monthly Advisor Log": "main.appdirect_raw.monthly_advisor_log",
  "advisor_orders": "main.appdirect_raw.advisor_orders"
}
```

## Run

```bash
python3 <skill_dir>/converter/convert_dataflow_to_dbt.py <extract_dir> <out_dir> overrides.json
```

For each overridden source, `models/sources.yml` emits `database:` / `schema:` /
`identifier:` so dbt resolves `{{ source('domo', name) }}` to the real table. Sources
without an override remain in `sources_needing_synthetic` (create synthetic tables for
those, or add overrides as the customer's tables come online).

## Why this matters

- **Portability:** the *same generated project* runs against synthetic tables (demo) or
  real tables (production) — only `overrides.json` changes, no SQL edits.
- **Clears most `UNRESOLVED_COLUMN` failures:** synthetic sources only contain columns the
  converter could infer from tile references; real tables carry every column, so deep
  passthroughs resolve.
- **Removes synthetic `AMBIGUOUS_REFERENCE` artifacts:** synthetic inference can fabricate a
  *computed* column onto a source (because the name appears downstream), which collides when
  a tile re-creates it. Real sources don't carry computed columns, so the collision vanishes.

## Discovering candidate tables

To find which Domo sources already exist as UC tables, list the source names from
`conversion_report.json → sources_needing_synthetic` and match them against your catalog
(by name or a known mapping). Start with the highest-fan-out sources (feed the most marts).
