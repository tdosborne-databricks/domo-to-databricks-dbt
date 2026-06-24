# tests/test_project.py
import os
from domo_to_dbt.project import convert_flow_to_dbt, write_dbt_project

FLOW = {"name": "Advisor_Services_ETL", "id": 67, "actions": [
    {"id": "L", "type": "LoadFromVault", "name": "Load Orders", "dataSourceId": "7"},
    {"id": "F", "type": "Filter", "name": "Open Only", "dependsOn": ["L"],
     "filterList": [{"leftField": "Status", "operator": "EQ",
                     "rightValue": {"value": "Open", "type": "STRING"}}]},
    {"id": "P", "type": "PublishToVault", "name": "Output", "dependsOn": ["F"],
     "dataSource": {"name": "Advisor Output"}},
]}
MAPPING = {"7": "advisor_orders"}

def test_convert_assigns_layers_and_source_ref():
    res = convert_flow_to_dbt(FLOW, MAPPING, {})
    by_name = {m["name"]: m for m in res["models"]}
    load = next(m for m in res["models"] if m["layer"] == "staging")
    assert "{{ source('domo', 'advisor_orders') }}" in load["sql"]
    assert any(m["layer"] == "marts" for m in res["models"])
    # the filter model refs the staging model by name
    filt = next(m for m in res["models"] if m["layer"] == "intermediate")
    assert "{{ ref('" in filt["sql"]

def test_sources_yml_wires_override_and_marks_synthetic(tmp_path):
    res = convert_flow_to_dbt(FLOW, MAPPING, {"advisor_orders": "main.raw.advisor_orders"})
    write_dbt_project(res, str(tmp_path))
    sources = (tmp_path / "models" / "sources.yml").read_text()
    # 3-part override wired so dbt resolves to the real table, not the build schema
    assert "database: main" in sources
    assert "schema: raw" in sources
    assert "identifier: advisor_orders" in sources

def test_marts_enable_delta_column_mapping(tmp_path):
    res = convert_flow_to_dbt(FLOW, MAPPING, {})
    write_dbt_project(res, str(tmp_path))
    marts = list((tmp_path / "models" / "marts").glob("*.sql"))
    assert marts
    txt = marts[0].read_text()
    assert "materialized='table'" in txt
    assert "delta.columnMapping.mode" in txt

def test_sources_yml_no_override_has_no_identifier(tmp_path):
    res = convert_flow_to_dbt(FLOW, MAPPING, {})
    write_dbt_project(res, str(tmp_path))
    sources = (tmp_path / "models" / "sources.yml").read_text()
    assert "identifier:" not in sources

def test_write_creates_project_files(tmp_path):
    res = convert_flow_to_dbt(FLOW, MAPPING, {})
    write_dbt_project(res, str(tmp_path))
    assert os.path.exists(tmp_path / "dbt_project.yml")
    assert os.path.exists(tmp_path / "models" / "sources.yml")
    # at least one model file per layer present
    staged = list((tmp_path / "models" / "staging").glob("*.sql"))
    marts = list((tmp_path / "models" / "marts").glob("*.sql"))
    assert staged and marts
    # staging model carries a config materialized line
    assert "materialized='view'" in staged[0].read_text()
