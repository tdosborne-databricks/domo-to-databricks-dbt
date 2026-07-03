"""End-to-end regression for the migration-agent skill scripts.

Runs a tiny synthetic Domo export through the full pipeline:
    domo_api_client (Mode B, offline)  ->  ingest_export (Mode A)  ->  scaffold
    ->  static_validator  ->  materialization_policy  ->  gen_dbt_tests  ->  diff kit
so a break in any script fails fast, independent of a live Domo/Databricks connection.
"""
import importlib.util
import json
import os
import subprocess
import sys

import pytest

_SKILLS = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "skills"))


def _load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _script(skill, fname):
    return os.path.join(_SKILLS, skill, "scripts", fname)


ingest = _load(_script("domo-ingestion", "ingest_export.py"), "ingest_export")
scaffold = _load(_script("org-dbt-conventions", "scaffold.py"), "scaffold")
matpol = _load(_script("databricks-materialization-policy", "materialization_policy.py"), "matpol")
applymat = _load(_script("databricks-materialization-policy", "apply_materialization.py"), "applymat")
statval = _load(_script("migration-validation", "static_validator.py"), "static_validator")
gentests = _load(_script("migration-validation", "gen_dbt_tests.py"), "gen_dbt_tests")
diffkit = _load(_script("migration-validation", "build_customer_diff_kit.py"), "diffkit")
domo_api = _load(_script("domo-ingestion", "domo_api_client.py"), "domo_api")


SYNTH_FLOW = {
    "id": 1, "name": "Orders Flow",
    "actions": [
        {"id": "l1", "type": "LoadFromVault", "name": "Orders", "dataSourceId": "ds1"},
        {"id": "l2", "type": "LoadFromVault", "name": "Customers", "dataSourceId": "ds2"},
        {"id": "j1", "type": "MergeJoin", "name": "join", "dependsOn": ["l1", "l2"],
         "joinType": "LEFT", "keys1": ["customer_id"], "keys2": ["customer_id"]},
        {"id": "g1", "type": "GroupBy", "name": "byc", "dependsOn": ["j1"],
         "groups": [{"name": "customer_id"}]},
        {"id": "u1", "type": "Unique", "name": "dedupe", "dependsOn": ["g1"],
         "fields": [{"name": "customer_id"}]},
        {"id": "p1", "type": "PublishToVault", "name": "Orders Summary", "dependsOn": ["u1"]},
    ],
}


@pytest.fixture
def pipeline(tmp_path):
    export = tmp_path / "export"
    export.mkdir()
    (export / "dataflows.json").write_text(json.dumps([SYNTH_FLOW]))
    (export / "dataset_mapping.json").write_text(json.dumps({"ds1": "Orders", "ds2": "Customers"}))

    norm = tmp_path / "norm"
    ingest.main(str(export), str(norm))

    proj = tmp_path / "proj"
    scaffold.scaffold(str(norm), str(proj))
    return {"export": export, "norm": norm, "proj": proj, "tmp": tmp_path}


def test_ingest_inputs_outputs(pipeline):
    flow = json.load(open(pipeline["norm"] / "flows" / "1.json"))
    assert len(flow["inputs"]) == 2
    assert len(flow["outputs"]) == 1
    assert len(flow["tiles"]) == 6
    names = {i["name"] for i in flow["inputs"]}
    assert names == {"Orders", "Customers"}


def test_scaffold_layers_and_org_files(pipeline):
    proj = pipeline["proj"]
    staging = os.listdir(proj / "models" / "staging")
    marts = os.listdir(proj / "models" / "marts")
    assert sum(f.endswith(".sql") for f in staging) == 2       # two LoadFromVault views
    assert sum(f.endswith(".sql") for f in marts) == 1         # one PublishToVault table
    for f in ("packages.yml", "profiles.yml", ".sqlfluff", "README.md", "dbt_project.yml"):
        assert (proj / f).exists(), f


def test_static_validator_passes(pipeline):
    report = statval.validate(str(pipeline["proj"]), str(pipeline["norm"]))
    assert report["summary"]["passed"], report["errors"]
    assert report["summary"]["errors"] == 0


def test_materialization_marks_output_table(pipeline):
    flow = json.load(open(pipeline["norm"] / "flows" / "1.json"))
    proposal = matpol.propose_for_flow(flow, "main", None, 1_000_000, schedule_known=False)
    summary = matpol._sanitize("Orders Summary")
    assert proposal[summary]["materialized"] == "table"
    assert matpol._sanitize("Orders")  # staging views present
    assert proposal[matpol._sanitize("Orders")]["materialized"] == "view"


def test_apply_materialization_fanout(pipeline):
    proj = pipeline["proj"]
    # Bump int_join fan-out to 2 so apply promotes it to table.
    (proj / "models" / "marts" / "orders_alt.sql").write_text(
        "{{ config(materialized='table') }}\nselect * from {{ ref('int_join') }}\n"
    )
    result = applymat.apply(str(proj))
    yml = (proj / "dbt_project.yml").read_text()
    assert "intermediate: {+materialized: view}" in yml
    join_sql = (proj / "models" / "intermediate" / "int_join.sql").read_text()
    assert "materialized='table'" in join_sql
    assert "delta.columnMapping.mode" in join_sql
    assert result["promoted_to_table"] == ["int_join"]


def test_gen_dbt_tests_grain(pipeline):
    flow = json.load(open(pipeline["norm"] / "flows" / "1.json"))
    tests = gentests.infer_tests(flow)
    summary = gentests._sanitize("Orders Summary")
    assert summary in tests
    assert tests[summary]["columns"]["customer_id"] == ["unique", "not_null"]
    # and the schema.yml is written + valid
    written = gentests.write_schema_yml(str(pipeline["proj"]), tests)
    assert written["marts"]["with_tests"] >= 1


def test_diff_kit_match_and_mismatch(pipeline, tmp_path):
    kit = tmp_path / "kit"
    diffkit.build(str(pipeline["proj"]), str(kit))
    assert (kit / "diff.py").exists()
    assert (kit / "references" / "tolerance-rules.md").exists()

    (tmp_path / "a.csv").write_text("id,amount\n1,10.0\n2,20.0\n")
    (tmp_path / "ok.csv").write_text("id,amount\n1,10.0\n2,20.0\n")
    (tmp_path / "bad.csv").write_text("id,amount\n1,10.0\n2,25.0\n")
    mp = tmp_path / "map.json"
    mp.write_text(json.dumps([
        {"name": "ok", "domo_csv": str(tmp_path / "a.csv"),
         "databricks_csv": str(tmp_path / "ok.csv"), "key": ["id"]},
        {"name": "bad", "domo_csv": str(tmp_path / "a.csv"),
         "databricks_csv": str(tmp_path / "bad.csv"), "key": ["id"]},
    ]))
    r = subprocess.run([sys.executable, str(kit / "diff.py"), str(mp)],
                       capture_output=True, text=True)
    assert r.returncode == 1                                    # a mismatch present
    report = json.loads(r.stdout)
    byname = {x["name"]: x for x in report["results"]}
    assert byname["ok"]["match"] is True
    assert byname["bad"]["match"] is False


def test_mode_b_offline_extract():
    r = domo_api.analyze_dataflow_complexity(SYNTH_FLOW)
    assert r["total_tiles"] == 6 and r["join_count"] == 1

    class Fake:
        def list_dataflows(self): return [{"id": 1, "name": "Orders Flow"}]
        def get_dataflow(self, fid): return SYNTH_FLOW
        def list_datasources(self): return [{"id": "ds1", "name": "Orders"}]
        def list_streams(self): return []
        def list_beast_modes(self): raise RuntimeError("404")

    art = domo_api.extract(Fake(), flow_name_filter="orders", log=lambda *a: None)
    assert len(art["dataflows"]) == 1
    assert art["dataset_mapping"] == {"ds1": "Orders"}
