import importlib.util, os
spec = importlib.util.spec_from_file_location(
    "gss", os.path.join(os.path.dirname(__file__), "..", "dbt_validation", "gen_synthetic_sources.py"))
gss = importlib.util.module_from_spec(spec); spec.loader.exec_module(gss)

def test_synthetic_sql_lists_all_columns():
    src = {"name": "advisor_orders", "synthetic_columns": ["Status", "Region", "Amount"]}
    sql = gss.synthetic_table_sql("main", "domo_migration_dbt", src)
    assert "main.domo_migration_dbt.advisor_orders" in sql
    assert "`Status`" in sql and "`Region`" in sql and "`Amount`" in sql
    assert sql.lower().startswith("create or replace table")

def test_synthetic_sql_empty_columns():
    src = {"name": "empty_src", "synthetic_columns": []}
    sql = gss.synthetic_table_sql("main", "s", src)
    assert "`col1`" in sql

def test_synthetic_sql_special_char_columns():
    src = {"name": "advisor_orders",
           "synthetic_columns": ["Account Manager", "#HoursToClose", "O'Brien"]}
    sql = gss.synthetic_table_sql("main", "s", src)
    assert "`Account Manager`" in sql
    assert "`#HoursToClose`" in sql
    assert "`O'Brien`" in sql
    # the apostrophe in the concat string literal must be escaped (doubled)
    assert "concat('O''Brien_'" in sql

def test_synthetic_sql_enables_column_mapping():
    # Delta rejects special-char column names unless column mapping is on
    src = {"name": "s1", "synthetic_columns": ["Account Manager"]}
    sql = gss.synthetic_table_sql("main", "s", src)
    assert "'delta.columnMapping.mode' = 'name'" in sql

def test_synthetic_sql_dedupes_case_insensitively():
    # Delta column names collide case-insensitively (APID vs apid)
    src = {"name": "s2", "synthetic_columns": ["APID", "apid", "Other"]}
    sql = gss.synthetic_table_sql("main", "s", src)
    assert sql.count(" as `") == 2  # one of APID/apid kept, plus Other
    assert "`APID`" in sql and "`apid`" not in sql
