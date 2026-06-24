# Wiring real source data with overrides.json

Each Domo `LoadFromVault` source must be wired to a **real Unity Catalog table**. Supply an
`overrides.json` as the 3rd argument to the converter mapping each source → its UC table.

> Some Domo sources are file-based (an Excel/CSV export) and aren't in Unity Catalog yet.
> Land each as a UC table first (Spark can't read `.xlsx` directly), then map it here exactly
> like a Delta source — see `file-sources.md`.

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

For each mapped source, `models/sources.yml` emits `database:` / `schema:` / `identifier:`
so dbt resolves `{{ source('domo', name) }}` to the real table. Any source still missing a
mapping is listed in `conversion_report.json → sources_needing_table` — its downstream
models can't build until you add it.

## Why a correct mapping matters

- **`UNRESOLVED_COLUMN`** usually means the mapped table is missing a column the flow reads —
  point the override at the right table (or confirm the column exists there).
- **`AMBIGUOUS_REFERENCE`** can mean the mapped table already carries a column the flow also
  computes downstream (Domo replaces; Spark duplicates). Column lineage `EXCEPT`s the cases it
  can prove; if it persists, check the table's schema for the colliding column.

## Discovering candidate tables

List the source names from `conversion_report.json → sources_needing_table` and match them
against your catalog (by name or a known mapping). Start with the highest-fan-out sources
(they feed the most marts), so the most models unblock per mapping you add.
