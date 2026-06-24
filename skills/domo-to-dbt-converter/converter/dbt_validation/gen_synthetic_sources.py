# Databricks notebook source
# Generates synthetic Delta tables for Domo sources that have no real UC table yet
# (the Excel-export gap). Columns are inferred from tile field-references and stored
# in conversion_report.json -> sources_needing_synthetic.

N_ROWS = 1000


def _dedupe_ci(cols):
    """Drop columns that collide case-insensitively (Delta column names are
    case-insensitive for uniqueness), keeping first occurrence."""
    seen, out = set(), []
    for c in cols:
        key = c.lower()
        if key not in seen:
            seen.add(key)
            out.append(c)
    return out


def synthetic_table_sql(catalog, schema, source, n_rows=N_ROWS):
    """Build a CREATE OR REPLACE TABLE AS SELECT that fabricates n_rows of data
    with every inferred column (string-typed; safe for 'does it run' validation).

    Enables Delta column mapping so Domo column names with spaces/special chars
    (e.g. 'Account Manager', '#HoursToClose') are allowed, and dedupes columns
    that collide case-insensitively.
    """
    fq = f"{catalog}.{schema}.{source['name']}"
    cols = _dedupe_ci(source.get("synthetic_columns") or ["col1"])
    def _col_expr(c):
        prefix = c.replace("'", "''")  # escape single quotes for the SQL string literal
        return f"cast(concat('{prefix}_', cast(id as string)) as string) as `{c}`"
    select_cols = ",\n    ".join(_col_expr(c) for c in cols)
    return (f"CREATE OR REPLACE TABLE {fq}\n"
            f"TBLPROPERTIES ('delta.columnMapping.mode' = 'name')\n"
            f"AS\nSELECT\n    {select_cols}\n"
            f"FROM (SELECT explode(sequence(1, {n_rows})) AS id)")


# COMMAND ----------
# Notebook execution (skipped during unit tests; runs only on Databricks)
if __name__ == "__main__" and "dbutils" in dir():  # pragma: no cover
    import json
    dbutils.widgets.text("report_path", "/Volumes/main/default/domo_migration/conversion_report.json")
    dbutils.widgets.text("catalog", "main")
    dbutils.widgets.text("schema", "domo_migration_dbt")
    catalog = dbutils.widgets.get("catalog")
    schema = dbutils.widgets.get("schema")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    with open(dbutils.widgets.get("report_path")) as fh:
        report = json.load(fh)
    for src in report["sources_needing_synthetic"]:
        stmt = synthetic_table_sql(catalog, schema, src)
        print(f"Creating {catalog}.{schema}.{src['name']} ({len(src['synthetic_columns'])} cols)")
        spark.sql(stmt)
    print(f"Done: {len(report['sources_needing_synthetic'])} synthetic tables")
