"""Unit tests for transpile_expr — Domo Beast Mode / MySQL -> Spark SQL rewrites.

Expressions below are taken verbatim from real flagged tiles in the AppDirect
advisor_services flow (see build/.../conversion_report.json), so the rules are
grounded in patterns that actually appear, not invented ones.
"""
from domo_to_dbt.common import transpile_expr


# --- Batch 1: the approved easy rules -------------------------------------

def test_ifnull_becomes_coalesce():
    assert transpile_expr("IFNULL(`end_date_time`,CURRENT_TIMESTAMP())") == \
        "coalesce(`end_date_time`,CURRENT_TIMESTAMP())"


def test_ifnull_is_case_insensitive():
    assert transpile_expr("ifnull(`x`,0)") == "coalesce(`x`,0)"


def test_curdate_becomes_current_date():
    assert transpile_expr("YEAR(CURDATE())-1") == "YEAR(current_date())-1"


def test_now_becomes_current_timestamp():
    assert transpile_expr("NOW()") == "current_timestamp()"


def test_hash_strips_inline_comment_to_end_of_line():
    src = "`DatePriceComplete` IS NOT NULL #AND YEAR(`DatePriceComplete`)\nTHEN 1"
    assert transpile_expr(src) == "`DatePriceComplete` IS NOT NULL \nTHEN 1"


def test_hash_strips_full_comment_line():
    src = "WHEN a\n    #WHEN b THEN '00-Last Year'\nELSE NULL"
    assert transpile_expr(src) == "WHEN a\n    \nELSE NULL"


def test_strips_double_dash_line_comment():
    # `--` comments are unsafe once an expression is wrapped/inlined (they eat a
    # following `)`), so transpile strips them like `#`.
    assert transpile_expr("`a` >= 1 -- note here\nAND `b`") == "`a` >= 1 \nAND `b`"


def test_hash_preserves_backtick_quoted_column_with_hash():
    # `#HourstoComplete` is a COLUMN NAME, not a comment — must survive intact.
    src = "WHEN `#HourstoComplete` = '0' THEN '1' ELSE `#HourstoComplete`*8"
    assert transpile_expr(src) == src


# --- Batch 2: mechanical MySQL -> Spark rewrites --------------------------

def test_date_add_interval_day_becomes_date_add_days():
    # MySQL INTERVAL with an *expression* amount is invalid in Spark; rewrite to
    # date_add(start, numDays). Real tile: add_completion_status___month.
    src = "date_add(`Date`,interval -DAY(`Date`)+1 DAY)"
    assert transpile_expr(src) == "date_add(`Date`, -DAY(`Date`)+1)"


def test_convert_tz_from_utc_becomes_from_utc_timestamp():
    src = "CONVERT_TZ(`start_date_time`,'UTC','US/Pacific')"
    assert transpile_expr(src) == "from_utc_timestamp(`start_date_time`,'US/Pacific')"


def test_date_format_translates_mysql_codes():
    src = "DATE_FORMAT(`start_date_time`,'%Y-%m')"
    assert transpile_expr(src) == "date_format(`start_date_time`,'yyyy-MM')"


def test_regexp_like_with_i_flag_injects_inline_flag():
    src = "REGEXP_LIKE(`order_status`,'^Canceled ','i')"
    assert transpile_expr(src) == "regexp_like(`order_status`,'(?i)^Canceled ')"


def test_regexp_like_without_flag_is_left_alone():
    # Two-arg regexp_like is valid Spark; leave it (only normalize the name).
    src = "regexp_like(`x`,'foo')"
    assert transpile_expr(src) == "regexp_like(`x`,'foo')"


# --- DATE_WORKING_DIFF: exact business days (Mon-Fri, no holidays) ---------

def test_date_working_diff_expands_to_weekday_serial_formula():
    src = "DATE_WORKING_DIFF(CURRENT_DATE(),`Order Received Date`)"
    out = transpile_expr(src)
    # No raw Domo function should survive.
    assert "DATE_WORKING_DIFF" not in out.upper()
    # Weekday-serial expansion: count weekdays from a Monday epoch for each arg.
    assert "datediff(CURRENT_DATE(), DATE'1900-01-01')" in out
    assert "datediff(`Order Received Date`, DATE'1900-01-01')" in out
    assert "div 7" in out and "least(" in out and "% 7, 5)" in out


def test_datetime_cast_function_becomes_cast_timestamp():
    # MySQL DATETIME(x) is a cast; Spark has no such routine. Nested parens in
    # the arg must be handled (real tile: DATETIME(LAST_DAY(CONCAT(...)))).
    src = "DATETIME(LAST_DAY(CONCAT('2023','-','01')))"
    assert transpile_expr(src) == "CAST(LAST_DAY(CONCAT('2023','-','01')) AS TIMESTAMP)"


def test_datetime_type_keyword_after_AS_is_untouched():
    # `AS DATETIME` is a type position, not a function call -> leave it alone.
    src = "CAST(`x` AS DATETIME)"
    assert transpile_expr(src) == "CAST(`x` AS DATETIME)"


def test_cast_as_char_becomes_string():
    # MySQL CAST(x AS CHAR) means "to string"; Spark CHAR needs a length.
    src = "CAST(date_format(`d`,'yyyy-MM') AS CHAR)"
    assert transpile_expr(src) == "CAST(date_format(`d`,'yyyy-MM') AS STRING)"


def test_cast_as_char_with_length_is_left_alone():
    # An explicit CHAR(n) is valid Spark -> don't touch it.
    src = "CAST(`x` AS CHAR(10))"
    assert transpile_expr(src) == "CAST(`x` AS CHAR(10))"


def test_date_working_diff_handles_nested_commas_in_args():
    # First arg has its own comma inside a function call — must split on the
    # top-level comma only, not the one inside coalesce(...).
    src = "DATE_WORKING_DIFF(coalesce(`a`,`b`),`c`)"
    out = transpile_expr(src)
    assert "DATE_WORKING_DIFF" not in out.upper()
    assert "datediff(coalesce(`a`,`b`), DATE'1900-01-01')" in out
    assert "datediff(`c`, DATE'1900-01-01')" in out
