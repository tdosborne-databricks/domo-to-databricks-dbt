import json, os
from convert_dataflow_to_dbt import main

def _write_extract(tmp_path):
    flow = {"name": "Mini", "id": 1, "actions": [
        {"id": "L", "type": "LoadFromVault", "name": "Load A", "dataSourceId": "7"},
        {"id": "P", "type": "PublishToVault", "name": "Out", "dependsOn": ["L"],
         "dataSource": {"name": "Out A"}}]}
    (tmp_path / "dataflows.json").write_text(json.dumps([flow]))
    (tmp_path / "dataset_mapping.json").write_text(json.dumps({"7": "advisor_orders"}))

def test_main_writes_project_and_report(tmp_path):
    ext = tmp_path / "extract"; ext.mkdir(); _write_extract(ext)
    out = tmp_path / "proj"
    report = main(str(ext), str(out))
    assert os.path.exists(out / "dbt_project.yml")
    assert os.path.exists(out / "conversion_report.json")
    assert "flows" in report
    assert "needs_review" in report
    assert "sources_needing_synthetic" in report

def test_project_name_derived_from_flow_name(tmp_path):
    # Generalizable: the generated dbt project/profile name must come from the
    # flow, not a hardcoded constant, so different flows get distinct names.
    flow = {"name": "Sales ETL Flow", "id": 9, "actions": [
        {"id": "L", "type": "LoadFromVault", "name": "Load A", "dataSourceId": "7"},
        {"id": "P", "type": "PublishToVault", "name": "Out", "dependsOn": ["L"],
         "dataSource": {"name": "Out A"}}]}
    ext = tmp_path / "extract"; ext.mkdir()
    (ext / "dataflows.json").write_text(json.dumps([flow]))
    (ext / "dataset_mapping.json").write_text(json.dumps({"7": "a"}))
    out = tmp_path / "proj"
    main(str(ext), str(out))
    proj = (out / "dbt_project.yml").read_text()
    assert "name: 'sales_etl_flow'" in proj
    assert "profile: 'sales_etl_flow'" in proj
