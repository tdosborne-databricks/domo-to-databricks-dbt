"""Tests for column lineage — the produced-column set per tile type."""
from domo_to_dbt.lineage import produced_columns


def test_load_produces_nothing_tracked():
    # Source columns are not known at convert time -> untracked (empty).
    assert produced_columns({"type": "LoadFromVault"}, []) == []


def test_filter_passes_through():
    assert produced_columns({"type": "Filter"}, ["a", "b"]) == ["a", "b"]


def test_formula_adds_new_outputs_to_input():
    a = {"type": "ExpressionEvaluator",
         "expressions": [{"expression": "1", "fieldName": "c"}]}
    assert produced_columns(a, ["a", "b"]) == ["a", "b", "c"]


def test_formula_replacing_existing_keeps_single_occurrence():
    a = {"type": "ExpressionEvaluator",
         "expressions": [{"expression": "`a`*2", "fieldName": "a"}]}
    assert produced_columns(a, ["a", "b"]) == ["a", "b"]


def test_groupby_is_a_projection():
    a = {"type": "GroupBy", "groups": [{"name": "Region"}],
         "fields": [{"name": "total", "expression": "sum(`x`)"}]}
    assert produced_columns(a, ["x", "Region", "other"]) == ["Region", "total"]


def test_select_projects_renames_and_drops():
    a = {"type": "SelectValues", "fields": [
        {"name": "Id", "remove": False},
        {"name": "Old", "rename": "New", "remove": False},
        {"name": "Gone", "remove": True}]}
    assert produced_columns(a, ["Id", "Old", "Gone", "z"]) == ["Id", "New"]


def test_datecalc_adds_calc_outputs():
    a = {"type": "DateCalculator", "calculations": [
        {"fieldName": "diff", "calcType": "DATE_WORKING_DIFF",
         "fieldA": "x", "fieldB": "y"}]}
    assert produced_columns(a, ["x", "y"]) == ["x", "y", "diff"]


def test_join_unions_sides_dropping_right_keys():
    # in_cols arrives already unioned across both sides; right join keys dropped.
    a = {"type": "MergeJoin", "keys1": ["OrderId"], "keys2": ["Id"]}
    assert produced_columns(a, ["OrderId", "Id", "amt"]) == ["OrderId", "amt"]


def test_window_adds_addition_names():
    a = {"type": "WindowAction", "additions": [{"name": "rnk"}]}
    assert produced_columns(a, ["a"]) == ["a", "rnk"]
