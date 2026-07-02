# Customer export format mapping

> **FILL IN AT BUILD SEQUENCE STEP 2** ("Inspect the export"). This is the critical unknown —
> everything downstream shapes itself to what's actually in the customer's files.

Document here, once the real files are inspected:

- The customer's actual directory/file layout (filenames, one-flow-per-file vs. combined, encoding).
- Where each normalized-graph field comes from in their format (field-by-field mapping).
- Which completeness fields are present vs. missing in their export (schedules? dataset schemas?
  row counts?).
- Any format quirks the Mode A parser must handle.

## Known so far (AppDirect engagement)

- The customer runs their own Domo API scripts (Step-1 format) and delivers the extract directory.
- Prior real extract: `~/Downloads/domo_extract/` — one Complex flow `Advisor_Services_ETL` (id 67),
  272 tiles, 59 joins, 29 inputs, 19 outputs, 14 tile types. `datasets.json` had **no column
  schema** → source columns are inferred from tile field-refs downstream.
- Sources are mixed: some already UC tables, many are Excel exports not yet in Databricks.

Re-inspect against the current delivery and update this doc before building the Mode A parser.
