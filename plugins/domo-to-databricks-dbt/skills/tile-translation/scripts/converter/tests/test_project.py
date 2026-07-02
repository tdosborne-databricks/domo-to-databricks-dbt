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

def test_flow_collapses_into_cte_models():
    # L -> F -> P: the source is its own staging model; the Filter tile is NOT its own
    # model — it has out-degree 1, so it collapses into the mart as a CTE.
    res = convert_flow_to_dbt(FLOW, MAPPING, {})
    assert sorted(m["layer"] for m in res["models"]) == ["marts", "staging"]
    staging = next(m for m in res["models"] if m["layer"] == "staging")
    assert "{{ source('domo', 'advisor_orders') }}" in staging["sql"]
    mart = next(m for m in res["models"] if m["layer"] == "marts")
    assert "with" in mart["sql"]                          # CTE chain, not a ref-to-a-filter-model
    assert "where" in mart["sql"].lower()                 # the Filter tile inlined as a CTE
    assert "{{ ref('stg_load_orders') }}" in mart["sql"]  # references the staging boundary
    assert mart["tile_count"] == 2                        # F + P collapsed into one model


def test_reuse_point_becomes_its_own_intermediate_model():
    # A tile consumed by >1 downstream (out-degree >= 2) is a reuse boundary and earns its
    # own intermediate model, ref'd by each consumer instead of duplicated as a CTE.
    flow = {"name": "reuse", "id": 1, "actions": [
        {"id": "L", "type": "LoadFromVault", "name": "Load", "dataSourceId": "7"},
        {"id": "C", "type": "SelectValues", "name": "Clean", "dependsOn": ["L"]},
        {"id": "P1", "type": "PublishToVault", "name": "Out One", "dependsOn": ["C"]},
        {"id": "P2", "type": "PublishToVault", "name": "Out Two", "dependsOn": ["C"]},
    ]}
    res = convert_flow_to_dbt(flow, MAPPING, {})
    inter = [m for m in res["models"] if m["layer"] == "intermediate"]
    assert len(inter) == 1 and inter[0]["name"] == "int_clean"
    marts = [m for m in res["models"] if m["layer"] == "marts"]
    assert len(marts) == 2
    for mt in marts:
        assert "{{ ref('int_clean') }}" in mt["sql"]      # consumers ref the shared model

def test_sources_yml_wires_real_table_override(tmp_path):
    res = convert_flow_to_dbt(FLOW, MAPPING, {"advisor_orders": "main.raw.advisor_orders"})
    write_dbt_project(res, str(tmp_path))
    sources = (tmp_path / "models" / "sources.yml").read_text()
    # 3-part override wired so dbt resolves to the real table, not the build schema
    assert "database: main" in sources
    assert "schema: raw" in sources
    assert "identifier: advisor_orders" in sources


def test_sources_yml_db_schema_at_source_level_not_table_level(tmp_path):
    # dbt honors database/schema ONLY at the source level; a table-level schema is silently
    # ignored (dbt falls back to the source name 'domo' as the schema -> relation-not-found).
    # Regression for a live e2e failure: staging views resolved to main.domo.* not main.raw.*.
    res = convert_flow_to_dbt(FLOW, MAPPING, {"advisor_orders": "main.raw.advisor_orders"})
    write_dbt_project(res, str(tmp_path))
    lines = (tmp_path / "models" / "sources.yml").read_text().splitlines()
    # database:/schema: must sit at source-level indent (4 spaces) and BEFORE `tables:`;
    # they must never appear at table-level indent (8 spaces).
    tables_idx = next(i for i, l in enumerate(lines) if l.strip() == "tables:")
    assert any(l == "    database: main" for l in lines[:tables_idx])
    assert any(l == "    schema: raw" for l in lines[:tables_idx])
    assert not any(l.startswith("        schema:") for l in lines)
    assert not any(l.startswith("        database:") for l in lines)
    # tables still carry identifier at table level
    assert any(l.strip() == "identifier: advisor_orders" for l in lines[tables_idx:])

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
