"""Assemble tile mappers into a dbt-databricks project (one model per tile)."""
import os

from .common import _sanitize, unique_name
from .dag import topo_sort, upstream_views, _deps
from .lineage import produced_columns
from .tiles import render_tile
from .sources import resolve_sources, source_ref

_MATERIALIZE = {"staging": "view", "intermediate": "ephemeral", "marts": "table"}


def convert_flow_to_dbt(flow, dataset_mapping, overrides=None):
    actions = flow["actions"]
    ordered = topo_sort(actions)
    id_to_view, used = {}, set()
    src_resolution = resolve_sources(flow, dataset_mapping, overrides)
    ds_by_id = {str(k): v for k, v in dataset_mapping.items()}

    # pre-assign model names so refs resolve regardless of order
    for a in ordered:
        id_to_view[a["id"]] = unique_name(_sanitize(a.get("name") or a["id"]), used)

    models, report = [], {"flow": flow.get("name"), "needs_review": []}
    cols_by_id = {}  # action id -> ordered list of known output columns
    for a in ordered:
        in_cols = []
        for uid in _deps(a):
            for c in cols_by_id.get(uid, []):
                if c not in in_cols:
                    in_cols.append(c)
        ctx = {
            "up": upstream_views(a, id_to_view),
            "in_cols": in_cols,
            "dataset_mapping": ds_by_id,
            "source_for": lambda dsid: source_ref(ds_by_id.get(str(dsid), f"source_{dsid}")),
        }
        res = render_tile(a, ctx)
        cols_by_id[a["id"]] = produced_columns(a, in_cols)
        name = id_to_view[a["id"]]
        models.append({"name": name, "layer": res.layer, "sql": res.sql,
                       "needs_review": res.needs_review, "note": res.note})
        if res.needs_review:
            report["needs_review"].append({"model": name, "type": a["type"], "note": res.note})
    return {"models": models, "sources": src_resolution["sources"], "report": report}


def _dbt_project_yml(project_name):
    return (f"name: '{project_name}'\n"
            "version: '1.0.0'\n"
            "config-version: 2\n"
            f"profile: '{project_name}'\n"
            "model-paths: ['models']\n"
            "models:\n"
            f"  {project_name}:\n"
            "    staging: {+materialized: view}\n"
            "    intermediate: {+materialized: ephemeral}\n"
            "    marts: {+materialized: table}\n")


def _sources_yml(sources):
    lines = ["version: 2", "sources:", "  - name: domo", "    tables:"]
    for s in sources:
        lines.append(f"      - name: {s['name']}")
        ct = s["catalog_table"]
        parts = ct.split(".") if ct else []
        if len(parts) == 3:
            # real UC table override -> wire catalog/schema/table so dbt resolves
            # {{ source('domo', name) }} to the actual table, not the build schema.
            catalog, schema, table = parts
            lines.append(f"        database: {catalog}")
            lines.append(f"        schema: {schema}")
            lines.append(f"        identifier: {table}")
        elif ct:
            lines.append(f"        # unresolved override (expected catalog.schema.table): {ct}")
        else:
            lines.append("        # no real UC table yet — resolved as synthetic in the build schema")
    return "\n".join(lines) + "\n"


def _schema_yml(models):
    lines = ["version: 2", "models:"]
    for m in models:
        if m["layer"] == "marts":
            lines.append(f"  - name: {m['name']}")
    return "\n".join(lines) + "\n"


def write_dbt_project(result, out_dir, project_name="domo_advisor_services"):
    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, "dbt_project.yml"), "w") as fh:
        fh.write(_dbt_project_yml(project_name))
    models_dir = os.path.join(out_dir, "models")
    for layer in ("staging", "intermediate", "marts"):
        os.makedirs(os.path.join(models_dir, layer), exist_ok=True)
    with open(os.path.join(models_dir, "sources.yml"), "w") as fh:
        fh.write(_sources_yml(result["sources"]))
    for m in result["models"]:
        materialized = _MATERIALIZE[m["layer"]]
        # marts are Delta tables; enable column mapping so Domo column names with
        # spaces/special chars (e.g. `Account Manager`, `#HoursToClose`) are allowed.
        if m["layer"] == "marts":
            header = ("{{ config(materialized='table', "
                      "tblproperties={'delta.columnMapping.mode': 'name'}) }}\n")
        else:
            header = f"{{{{ config(materialized='{materialized}') }}}}\n"
        if m["needs_review"]:
            header += f"-- NEEDS REVIEW: {m['note']}\n"
        path = os.path.join(models_dir, m["layer"], f"{m['name']}.sql")
        with open(path, "w") as fh:
            fh.write(header + m["sql"] + "\n")
    with open(os.path.join(models_dir, "marts", "schema.yml"), "w") as fh:
        fh.write(_schema_yml(result["models"]))
