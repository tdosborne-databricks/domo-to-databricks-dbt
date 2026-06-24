from domo_to_dbt.sources import resolve_sources, infer_source_columns, source_ref

FLOW = {"actions": [
    {"id": "L", "type": "LoadFromVault", "name": "Load Orders", "dataSourceId": "7"},
    {"id": "F", "type": "Filter", "name": "f", "dependsOn": ["L"],
     "filterList": [{"leftField": "Status", "operator": "EQ",
                     "rightValue": {"value": "Open", "type": "STRING"}}]},
    {"id": "G", "type": "GroupBy", "name": "g", "dependsOn": ["F"],
     "groups": [{"name": "Region"}],
     "fields": [{"name": "total", "expression": "SUM(`Amount`)"}]},
]}
MAPPING = {"7": "advisor_orders"}

def test_source_ref_sanitizes():
    assert source_ref("Advisor Orders") == "{{ source('domo', 'advisor_orders') }}"

def test_resolve_uses_override_by_name():
    out = resolve_sources(FLOW, MAPPING, {"advisor_orders": "main.raw.advisor_orders"})
    s = out["sources"][0]
    assert s["name"] == "advisor_orders"
    assert s["catalog_table"] == "main.raw.advisor_orders"

def test_resolve_marks_missing_when_no_override():
    out = resolve_sources(FLOW, MAPPING, {})
    s = out["sources"][0]
    assert s["catalog_table"] is None
    assert "Status" in s["inferred_columns"] and "Region" in s["inferred_columns"]

def test_infer_collects_downstream_fields():
    cols = infer_source_columns(FLOW)["L"]
    assert "Status" in cols and "Region" in cols

def test_resolve_uses_override_by_id():
    out = resolve_sources(FLOW, MAPPING, {"7": "main.raw.advisor_orders"})
    s = out["sources"][0]
    assert s["dataset_id"] == "7"
    assert s["catalog_table"] == "main.raw.advisor_orders"

def test_resolve_id_override_beats_name_override():
    out = resolve_sources(FLOW, MAPPING, {"7": "main.by_id.tbl",
                                          "advisor_orders": "main.by_name.tbl"})
    assert out["sources"][0]["catalog_table"] == "main.by_id.tbl"
