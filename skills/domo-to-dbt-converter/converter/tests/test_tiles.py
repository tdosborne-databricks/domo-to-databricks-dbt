from domo_to_dbt.tiles import m_load, m_filter, m_groupby, m_formula, m_publish, m_join, m_select, m_metadata, m_unique, m_union, m_window, m_normalizer, m_datecalc, m_sql, render_tile

def _ctx(up, source_for=None):
    return {"up": up, "dataset_mapping": {"7": "advisor_orders"},
            "source_for": source_for or (lambda i: "{{ source('domo', 'advisor_orders') }}")}

def test_load_emits_source_ref():
    a = {"id": "a", "type": "LoadFromVault", "name": "Load Orders", "dataSourceId": "7"}
    r = m_load(a, _ctx([]))
    assert "{{ source('domo', 'advisor_orders') }}" in r.sql
    assert r.layer == "staging"

def test_filter_builds_where():
    a = {"type": "Filter", "name": "f",
         "filterList": [{"leftField": "Status", "operator": "EQ",
                         "rightValue": {"value": "Open", "type": "STRING"}}]}
    r = m_filter(a, _ctx(["stg_orders"]))
    assert "where `Status` = 'Open'" in r.sql
    assert r.layer == "intermediate"

def test_filter_uses_expression_form_and_transpiles():
    # Domo Filter tiles can carry a Beast Mode `expression` with all structured
    # fields null -> must use (and transpile) the expression, not emit `None`.
    a = {"type": "Filter", "name": "f", "filterList": [
        {"leftField": None, "operator": None, "rightValue": None,
         "expression": "YEAR(`d`) >= 2010 AND IFNULL(`x`,0) > 0"}]}
    r = m_filter(a, _ctx(["up"]))
    assert "`None`" not in r.sql
    assert "coalesce(`x`,0)" in r.sql
    assert "YEAR(`d`) >= 2010" in r.sql

def test_filter_skips_empty_predicate():
    a = {"type": "Filter", "name": "f", "filterList": [
        {"leftField": None, "operator": None, "rightValue": None, "expression": None}]}
    r = m_filter(a, _ctx(["up"]))
    assert "`None`" not in r.sql
    assert "where 1=1" in r.sql

def test_groupby_builds_group_and_aggs():
    a = {"type": "GroupBy", "name": "g", "groups": [{"name": "Region"}],
         "fields": [{"name": "total", "expression": "SUM(`Amount`)"}]}
    r = m_groupby(a, _ctx(["filtered"]))
    assert "group by `Region`" in r.sql
    assert "SUM(`Amount`) AS `total`" in r.sql

def test_formula_appends_columns():
    a = {"type": "ExpressionEvaluator", "name": "af",
         "expressions": [{"expression": "`a` + `b`", "fieldName": "c"}]}
    r = m_formula(a, _ctx(["g"]))
    assert "`a` + `b` AS `c`" in r.sql

def test_publish_is_marts_passthrough():
    a = {"type": "PublishToVault", "name": "out", "dataSource": {"name": "Advisor Output"}}
    r = m_publish(a, _ctx(["final"]))
    assert r.layer == "marts"
    assert "from final" in r.sql.lower() or "from {{ ref" in r.sql.lower()

def test_join_zips_keys():
    a = {"type": "MergeJoin", "name": "j", "joinType": "INNER",
         "keys1": ["OrderId"], "keys2": ["Id"], "dependsOn": ["L", "R"]}
    r = m_join(a, _ctx(["left_v", "right_v"]))
    assert "inner join" in r.sql.lower() and "right_v" in r.sql.lower()
    assert "l.`OrderId` = r.`Id`" in r.sql
    # right-side join key dropped to avoid duplicate columns
    assert "r.* except (`Id`)" in r.sql

def test_select_drops_renames_casts():
    a = {"type": "SelectValues", "name": "s", "fields": [
        {"name": "Id", "rename": None, "type": None, "remove": False},
        {"name": "Amt", "rename": None, "type": "DOUBLE", "remove": False},
        {"name": "Old", "rename": "New", "type": None, "remove": False},
        {"name": "Drop", "rename": None, "type": None, "remove": True},
    ]}
    r = m_select(a, _ctx(["up"]))
    assert "`Id`" in r.sql
    assert "CAST(`Amt` AS DOUBLE) AS `Amt`" in r.sql
    assert "`Old` AS `New`" in r.sql
    assert "Drop" not in r.sql

def test_metadata_casts_column():
    a = {"type": "Metadata", "name": "m", "fields": [
        {"name": "PartnerId", "rename": None, "type": "DOUBLE", "remove": False}]}
    r = m_metadata(a, _ctx(["up"]))
    assert "CAST(`PartnerId` AS DOUBLE) AS `PartnerId`" in r.sql

def test_unique_dedupes_on_fields():
    a = {"type": "Unique", "name": "u",
         "fields": [{"name": "OrderLocationDetailId"}]}
    r = m_unique(a, _ctx(["up"]))
    assert ("row_number() over (partition by `OrderLocationDetailId` "
            "order by `OrderLocationDetailId`) = 1") in r.sql

def test_union_all_concatenates_inputs():
    a = {"type": "UnionAll", "name": "u", "unionType": "INCLUDE_ALL",
         "inputs": ["x", "y"]}
    r = m_union(a, _ctx(["va", "vb"]))
    # Databricks SQL has no UNION BY NAME -> positional union, flagged for review
    assert "union all" in r.sql and "by name" not in r.sql
    assert r.needs_review is True
    assert "{{ ref('va') }}" in r.sql and "{{ ref('vb') }}" in r.sql

def test_union_non_include_all_is_distinct_union():
    a = {"type": "UnionAll", "name": "u", "unionType": "DISTINCT",
         "inputs": ["x", "y"]}
    r = m_union(a, _ctx(["va", "vb"]))
    assert "\nunion\n" in r.sql and "union all" not in r.sql
    assert r.needs_review is True

def test_window_emits_rank_over_partition():
    a = {"type": "WindowAction", "name": "w",
         "groupRules": [{"column": "QuoteNumber"}],
         "orderRules": [{"column": "DatePriceComplete", "ascending": False}],
         "additions": [{"name": "Rnk", "operation": {"operationType": "RANK"}}]}
    r = m_window(a, _ctx(["up"]))
    s = r.sql
    assert "RANK() OVER (PARTITION BY `QuoteNumber` ORDER BY `DatePriceComplete` DESC) AS `Rnk`" in s

def test_normalizer_unpivots_with_stack():
    a = {"type": "Normalizer", "name": "n", "typefield": "Date_Type", "fields": [
        {"sourceField": "DateOfRequest", "typefieldValue": "Quotes Created", "destField": "Date"},
        {"sourceField": "DatePriceComplete", "typefieldValue": "Quotes Completed", "destField": "Date"}]}
    r = m_normalizer(a, _ctx(["up"]))
    s = r.sql
    assert "stack(2" in s
    assert "'Quotes Created', `DateOfRequest`" in s
    assert "as (`Date_Type`, `Date`)" in s.replace("AS", "as")

def test_datecalc_working_diff_exact_and_unflagged():
    a = {"type": "DateCalculator", "name": "d", "calculations": [
        {"fieldName": "CompleteProcessingTime", "calcType": "DATE_WORKING_DIFF",
         "fieldA": "DatePriceComplete", "fieldB": "Quote Ticket CreationTime"}]}
    r = m_datecalc(a, _ctx(["up"]))
    # Exact business-day formula -> faithful translation, no manual review needed.
    assert r.needs_review is False
    assert "`CompleteProcessingTime`" in r.sql
    assert "div 7" in r.sql and "datediff(`DatePriceComplete`," in r.sql

def test_datecalc_unhandled_calctype_flagged():
    a = {"type": "DateCalculator", "name": "d", "calculations": [
        {"fieldName": "X", "calcType": "SOME_OTHER", "fieldA": "a", "fieldB": "b"}]}
    r = m_datecalc(a, _ctx(["up"]))
    assert r.needs_review is True
    assert "unhandled calcType 'SOME_OTHER'" in r.note

def test_datecalc_empty_calculations_runnable():
    a = {"type": "DateCalculator", "name": "d", "calculations": []}
    r = m_datecalc(a, _ctx(["up"]))
    assert r.needs_review is True
    assert "{{ ref('up') }}" in r.sql
    assert "select *, " not in r.sql

def test_sql_tile_strips_trailing_semicolon():
    a = {"type": "SQL", "name": "sql", "inputs": [],
         "statements": ["select * from foo;"]}
    r = m_sql(a, {"up": [], "dataset_mapping": {}, "source_for": None})
    assert not r.sql.rstrip().endswith(";")

def test_sql_tile_flagged_and_rewrites_refs():
    a = {"type": "SQL", "name": "sql", "inputs": ["t1"],
         "statements": ["SELECT * FROM month_table WHERE MonthEnd <= CURDATE()"]}
    r = m_sql(a, {"up": ["month_table"], "dataset_mapping": {}, "source_for": None})
    assert r.needs_review is True
    assert "current_date()" in r.sql.lower()
    assert "{{ ref('month_table') }}" in r.sql

def test_render_tile_dispatches_and_flags_unknown():
    r = render_tile({"type": "Totally Unknown", "name": "x"}, _ctx(["up"]))
    assert r.needs_review is True
    assert "select *" in r.sql.lower()

def test_metadata_maps_domo_types_to_spark():
    a = {"type": "Metadata", "name": "m", "fields": [
        {"name": "ts", "rename": None, "type": "DATETIME", "remove": False},
        {"name": "big", "rename": None, "type": "LONG", "remove": False}]}
    r = m_metadata(a, _ctx(["up"]))
    assert "CAST(`ts` AS TIMESTAMP)" in r.sql
    assert "CAST(`big` AS BIGINT)" in r.sql
    assert "DATETIME" not in r.sql

def test_formula_transpiles_date_working_diff_and_unflags():
    a = {"type": "ExpressionEvaluator", "name": "af", "expressions": [
        {"expression": "DATE_WORKING_DIFF(`a`,`b`)", "fieldName": "d"}]}
    r = m_formula(a, _ctx(["g"]))
    assert r.needs_review is False
    assert "DATE_WORKING_DIFF" not in r.sql.upper()
    assert "div 7" in r.sql

def test_formula_flags_genuinely_unhandled_dialect():
    a = {"type": "ExpressionEvaluator", "name": "af", "expressions": [
        {"expression": "WORKING_DAYS(`a`,`b`)", "fieldName": "d"}]}
    r = m_formula(a, _ctx(["g"]))
    assert r.needs_review is True and "WORKING_DAYS" in r.note

def test_formula_plain_expr_not_flagged():
    a = {"type": "ExpressionEvaluator", "name": "af",
         "expressions": [{"expression": "`a` + `b`", "fieldName": "c"}]}
    r = m_formula(a, _ctx(["g"]))
    assert r.needs_review is False

def test_normalizer_keeps_passthrough_columns():
    a = {"type": "Normalizer", "name": "n", "typefield": "Date_Type", "fields": [
        {"sourceField": "DateOfRequest", "typefieldValue": "Created", "destField": "Date"},
        {"sourceField": "DatePriceComplete", "typefieldValue": "Completed", "destField": "Date"}]}
    r = m_normalizer(a, _ctx(["up"]))
    assert "select * except (`DateOfRequest`, `DatePriceComplete`)" in r.sql

def test_formula_transpiles_ifnull_and_unflags():
    a = {"type": "ExpressionEvaluator", "name": "af", "expressions": [
        {"expression": "IFNULL(`x`,0)", "fieldName": "y"}]}
    r = m_formula(a, _ctx(["g"]))
    assert "coalesce(`x`,0) AS `y`" in r.sql
    assert r.needs_review is False

def test_formula_ignores_dialect_inside_block_comment():
    a = {"type": "ExpressionEvaluator", "name": "af", "expressions": [
        {"expression": "`x` /* DATE_SUB(CURRENT_DATE, INTERVAL 3 DAY) */", "fieldName": "y"}]}
    r = m_formula(a, _ctx(["g"]))
    assert r.needs_review is False

def test_groupby_transpiles_expressions():
    a = {"type": "GroupBy", "name": "g", "groups": [{"name": "R"}],
         "fields": [{"name": "t", "expression": "SUM(IFNULL(`Amount`,0))"}]}
    r = m_groupby(a, _ctx(["f"]))
    assert "SUM(coalesce(`Amount`,0)) AS `t`" in r.sql

def test_formula_replaces_self_referential_column():
    # Domo replaces a column when the output name matches an input it reads;
    # Spark would duplicate it via `select *, ...` -> use `* except`.
    a = {"type": "ExpressionEvaluator", "name": "af", "expressions": [
        {"expression": "`Hrs`*8", "fieldName": "Hrs"}]}
    r = m_formula(a, _ctx(["g"]))
    assert "select * except (`Hrs`)," in r.sql

def test_formula_does_not_except_new_or_sibling_columns():
    # `total` is a brand-new output; `bucket` references the sibling `total`,
    # not itself -> neither should be excepted (excepting them would error).
    a = {"type": "ExpressionEvaluator", "name": "af", "expressions": [
        {"expression": "`a`+`b`", "fieldName": "total"},
        {"expression": "`total`+1", "fieldName": "bucket"}]}
    r = m_formula(a, _ctx(["g"]))
    assert "select *," in r.sql and "except" not in r.sql

def test_formula_excepts_output_already_in_input_columns():
    # `Region` already exists upstream (in ctx in_cols); the formula re-creates it
    # without self-referencing -> must EXCEPT it to avoid a duplicate column.
    a = {"type": "ExpressionEvaluator", "name": "af", "expressions": [
        {"expression": "upper(`x`)", "fieldName": "Region"}]}
    ctx = _ctx(["g"]); ctx["in_cols"] = ["Region", "x"]
    r = m_formula(a, ctx)
    assert "select * except (`Region`)," in r.sql

def test_datecalc_excepts_output_already_in_input_columns():
    a = {"type": "DateCalculator", "name": "d", "calculations": [
        {"fieldName": "Hrs", "calcType": "DATE_WORKING_DIFF",
         "fieldA": "End", "fieldB": "Start"}]}
    ctx = _ctx(["up"]); ctx["in_cols"] = ["Hrs", "End", "Start"]
    r = m_datecalc(a, ctx)
    assert "select * except (`Hrs`)," in r.sql

def test_datecalc_excepts_self_referential_output():
    a = {"type": "DateCalculator", "name": "d", "calculations": [
        {"fieldName": "Hrs", "calcType": "DATE_WORKING_DIFF",
         "fieldA": "Hrs", "fieldB": "Start"}]}
    r = m_datecalc(a, _ctx(["up"]))
    assert "select * except (`Hrs`)," in r.sql

def test_formula_ignores_dialect_inside_line_comment():
    # Real tile add_formula_7: author commented out columns with `--`; the
    # INTERVAL inside is a no-op in Spark and must not trigger a false flag.
    a = {"type": "ExpressionEvaluator", "name": "af", "expressions": [
        {"expression": "LAST_DAY(`x`)\n-- DATE_SUB(`x`,INTERVAL 1 MONTH) AS `m`",
         "fieldName": "y"}]}
    r = m_formula(a, _ctx(["g"]))
    assert r.needs_review is False
