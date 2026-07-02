# Sources that aren't UC tables yet (Excel, CSV, files)

The converter translates the Domo **transformation** DAG; it does not ingest data. Every
Domo source must resolve to a **queryable Unity Catalog relation** (a table or a view) so
that `{{ source('domo', name) }}` works. Some Domo sources are real Delta/UC tables already
— those just go straight into `overrides.json`. Others are **file-based** (an Excel export, a
CSV) and aren't in Unity Catalog yet. Those need a one-time **land-then-map** step.

Spark **cannot read `.xlsx` natively** (no built-in Excel reader), so Excel always needs a
conversion step. CSV is easier (Spark reads it directly).

## Pattern: land the file as a UC table, then map it

1. **Land the file once** into a managed UC table (pick the approach that fits your setup):

   **Excel (.xlsx) — notebook with pandas (simplest, any cluster with pandas+openpyxl):**
   ```python
   import pandas as pd
   pdf = pd.read_excel("/Volumes/main/landing/files/monthly_advisor_log.xlsx",
                       sheet_name="Sheet1", dtype=str)   # dtype=str keeps it lossless
   (spark.createDataFrame(pdf)
         .write.mode("overwrite")
         .saveAsTable("main.appdirect_raw.monthly_advisor_log"))
   ```
   Put the file in a **UC Volume** first (`/Volumes/<catalog>/<schema>/<volume>/...`) so the
   path is governed and re-runnable. Multiple sheets → loop `sheet_name` and write one table each.

   **CSV — no conversion needed:**
   ```sql
   CREATE TABLE main.appdirect_raw.advisor_orders AS
   SELECT * FROM read_files('/Volumes/main/landing/files/advisor_orders.csv',
                            format => 'csv', header => true);
   ```
   (Or the Databricks UI: **Add data → Upload file** → creates a UC table; or Auto Loader /
   `COPY INTO` for ongoing CSV drops.)

2. **Map it in `overrides.json`** like any other source — the converter doesn't care whether
   the table came from Delta, Excel, or CSV:
   ```json
   {
     "advisor_orders":      "main.appdirect_raw.advisor_orders",
     "Monthly Advisor Log": "main.appdirect_raw.monthly_advisor_log"
   }
   ```

3. **Re-run convert + build.** The source now resolves; its downstream models build.

## Mixed workspace (some Delta, some Excel) — the normal case

Map the existing Delta tables immediately; for the file-based ones, land each as above and add
it to the same `overrides.json`. `conversion_report.json → sources_needing_table` is your
checklist — work it down until it's empty. Start with the highest-fan-out sources (they unblock
the most marts per table you land).

## Keeping it repeatable

Ingestion is a **separate, upstream concern** from the dbt conversion — keep the landing
notebook/job in your own pipeline (e.g. a Databricks Workflow task that refreshes the Excel-
derived tables on a schedule), then let the converted dbt project read them as governed UC
tables. This keeps the converter focused on transformations and your ingestion auditable.

> Tip: type fidelity. `pd.read_excel(..., dtype=str)` lands everything as strings, which is
> safe for a first build (Domo Beast Mode is forgiving about types). Cast in the dbt models or
> tighten the landed table's schema once the pipeline is green.
