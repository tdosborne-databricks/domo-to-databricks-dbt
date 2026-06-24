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
