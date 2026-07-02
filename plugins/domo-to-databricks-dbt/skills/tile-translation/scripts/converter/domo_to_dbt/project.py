"""Assemble tile mappers into a dbt-databricks project.

Granularity (the plan's central judgment call): **tile chains -> CTEs, a flow -> a few models**,
NOT one model per tile. A tile becomes the root of its own model iff it is a *boundary*:
  - a source (LoadFromVault)        -> staging model (view)
  - a sink (PublishToVault, or a terminal tile with out-degree 0) -> marts model (table)
  - a reuse point (out-degree >= 2) -> intermediate model (view); consumed by >1 downstream,
    so it earns its own model instead of being duplicated as a CTE in each consumer.

Every other tile has out-degree exactly 1, so it funnels into a unique boundary and collapses
into that boundary's model as a CTE. This turns e.g. a 272-tile flow (29 sources, 19 sinks,
22 reuse points) into ~70 models with ~200 tiles inlined as CTEs, instead of 272 models.
"""
import os
import re

from .common import _sanitize, unique_name
from .dag import topo_sort, upstream_views, _deps
from .lineage import produced_columns
from .tiles import render_tile
from .sources import resolve_sources, source_ref

_MATERIALIZE = {"staging": "view", "intermediate": "view", "marts": "table"}
_LAYER_PREFIX = {"staging": "stg_", "intermediate": "int_", "marts": ""}
_REF_RE = re.compile(r"\{\{\s*ref\('([^']+)'\)\s*\}\}")


def _out_degree(actions):
    outdeg = {}
    for a in actions:
        for d in _deps(a):
            outdeg[d] = outdeg.get(d, 0) + 1
    return outdeg


def _boundary_layer(action, outdeg):
    """Return the layer if this tile is a model boundary, else None (it's an inlined CTE)."""
    t = action["type"]
    if t == "LoadFromVault":
        return "staging"
    if t == "PublishToVault" or outdeg.get(action["id"], 0) == 0:
        return "marts"
    if outdeg.get(action["id"], 0) >= 2:      # reuse point: consumed by >1 downstream
        return "intermediate"
    return None                                # out-degree 1 -> collapses into its consumer


def _indent(sql, n=2):
    pad = " " * n
    return "\n".join(pad + line if line else line for line in sql.splitlines())


def convert_flow_to_dbt(flow, dataset_mapping, overrides=None):
    actions = flow["actions"]
    ordered = topo_sort(actions)
    by_id = {a["id"]: a for a in actions}
    outdeg = _out_degree(actions)
    children = {}
    for a in actions:
        for d in _deps(a):
            children.setdefault(d, []).append(a["id"])

    id_to_view, used = {}, set()
    for a in ordered:                          # unique CTE/identifier name per tile
        id_to_view[a["id"]] = unique_name(_sanitize(a.get("name") or a["id"]), used)

    src_resolution = resolve_sources(flow, dataset_mapping, overrides)
    ds_by_id = {str(k): v for k, v in dataset_mapping.items()}

    # 1) Render every tile to a SQL fragment (refs to upstream tiles are `{{ ref('<uid>') }}`).
    rendered, cols_by_id = {}, {}
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
        rendered[a["id"]] = render_tile(a, ctx)
        cols_by_id[a["id"]] = produced_columns(a, in_cols)

    # 2) Classify boundaries and assign each boundary a layer + model name.
    boundary_layer = {aid: _boundary_layer(by_id[aid], outdeg) for aid in by_id}
    boundary_layer = {aid: lyr for aid, lyr in boundary_layer.items() if lyr}
    model_name = {aid: _LAYER_PREFIX[lyr] + id_to_view[aid]
                  for aid, lyr in boundary_layer.items()}
    # map by CTE uid too, for resolving `ref('<uid>')` tokens to the boundary's model
    model_name_by_uid = {id_to_view[aid]: mn for aid, mn in model_name.items()}

    # 3) Assign every tile to the region (boundary) it collapses into. A boundary owns
    #    itself; an internal tile follows its single successor to the first boundary.
    owner_cache = {}

    def owner(tid):
        if tid in owner_cache:
            return owner_cache[tid]
        if tid in boundary_layer:
            owner_cache[tid] = tid
            return tid
        ch = children.get(tid, [])
        owner_cache[tid] = owner(ch[0]) if ch else tid
        return owner_cache[tid]

    regions = {aid: [] for aid in boundary_layer}
    for a in ordered:                          # topo order -> region members are topo-ordered
        regions.setdefault(owner(a["id"]), []).append(a["id"])

    # 4) Assemble one model per boundary: region tiles -> CTEs, refs resolved.
    def resolve_refs(sql, region_uids):
        def repl(m):
            uid = m.group(1)
            if uid in region_uids:
                return f"`{uid}`"              # sibling tile in this model -> a CTE
            return "{{ ref('%s') }}" % model_name_by_uid.get(uid, uid)  # a boundary -> its model
        return _REF_RE.sub(repl, sql)

    models, report = [], {"flow": flow.get("name"), "needs_review": []}
    for bid, layer in boundary_layer.items():
        region = regions.get(bid, [bid])
        region_uids = {id_to_view[t] for t in region}
        mname = model_name[bid]
        cte_defs, trace, needs, notes = [], [], False, []
        for tid in region:
            r = rendered[tid]
            cte_defs.append((id_to_view[tid], resolve_refs(r.sql, region_uids)))
            trace.append(by_id[tid].get("name") or tid)
            if r.needs_review:
                needs = True
                notes.append(r.note)
                report["needs_review"].append(
                    {"model": mname, "type": by_id[tid]["type"], "note": r.note})

        buid = id_to_view[bid]
        if len(cte_defs) == 1:
            model_sql = cte_defs[0][1]          # trivial region (e.g. a staging source)
        else:
            ctes = ",\n".join(f"`{c}` as (\n{_indent(s)}\n)" for c, s in cte_defs)
            model_sql = f"with\n{ctes}\nselect * from `{buid}`"

        models.append({"name": mname, "layer": layer, "sql": model_sql,
                       "needs_review": needs, "note": "; ".join(n for n in notes if n),
                       "trace": trace, "tile_count": len(region)})

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
            "    intermediate: {+materialized: view}\n"
            "    marts: {+materialized: table}\n")


def _sources_yml(sources):
    # dbt honors `database`/`schema` only at the SOURCE level; at the table level only
    # `identifier` is honored (a table-level `schema`/`database` is silently ignored and dbt
    # falls back to the source name as the schema). All models here reference a single source
    # named 'domo', so we hoist the common catalog/schema to the source level and leave only
    # `identifier` per table. When overrides disagree on catalog/schema, the dominant pair wins
    # and each outlier gets a warning comment (dbt cannot target a different schema per-table
    # under one source name -- land those tables in the common schema or split the source).
    from collections import Counter
    resolved = {}
    counts = Counter()
    for s in sources:
        parts = (s["catalog_table"] or "").split(".")
        if len(parts) == 3:
            resolved[s["name"]] = tuple(parts)         # (catalog, schema, table)
            counts[(parts[0], parts[1])] += 1

    lines = ["version: 2", "sources:", "  - name: domo"]
    dom_catalog = dom_schema = None
    if counts:
        (dom_catalog, dom_schema), _ = counts.most_common(1)[0]
        lines.append(f"    database: {dom_catalog}")
        lines.append(f"    schema: {dom_schema}")
    lines.append("    tables:")

    for s in sources:
        lines.append(f"      - name: {s['name']}")
        ct = s["catalog_table"]
        if s["name"] in resolved:
            catalog, schema, table = resolved[s["name"]]
            lines.append(f"        identifier: {table}")
            if (catalog, schema) != (dom_catalog, dom_schema):
                lines.append(f"        # WARNING: override targets {catalog}.{schema}, but dbt sources "
                             f"take one schema at the source level ({dom_catalog}.{dom_schema}); "
                             f"land this table in {dom_schema} or give it its own source block.")
        elif ct:
            lines.append(f"        # unresolved override (expected catalog.schema.table): {ct}")
        else:
            lines.append("        # no override: wire this source to a real UC table in overrides.json (catalog.schema.table)")
    return "\n".join(lines) + "\n"


def _schema_yml(models):
    lines = ["version: 2", "models:"]
    for m in models:
        if m["layer"] == "marts":
            lines.append(f"  - name: {m['name']}")
    return "\n".join(lines) + "\n"


def write_dbt_project(result, out_dir, project_name="domo_dbt_project"):
    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, "dbt_project.yml"), "w") as fh:
        fh.write(_dbt_project_yml(project_name))
    models_dir = os.path.join(out_dir, "models")
    for layer in ("staging", "intermediate", "marts"):
        os.makedirs(os.path.join(models_dir, layer), exist_ok=True)
    with open(os.path.join(models_dir, "sources.yml"), "w") as fh:
        fh.write(_sources_yml(result["sources"]))
    flow_name = result["report"].get("flow") or "unknown"
    for m in result["models"]:
        materialized = _MATERIALIZE[m["layer"]]
        # marts are Delta tables; enable column mapping so Domo column names with
        # spaces/special chars (e.g. `Account Manager`, `#HoursToClose`) are allowed.
        if m["layer"] == "marts":
            header = ("{{ config(materialized='table', "
                      "tblproperties={'delta.columnMapping.mode': 'name'}) }}\n")
        else:
            header = f"{{{{ config(materialized='{materialized}') }}}}\n"
        # Traceability: every model records the Domo flow + tiles it was built from.
        header += f"-- Migrated from Domo flow '{flow_name}' | tiles: {', '.join(m.get('trace', []))}\n"
        if m["needs_review"]:
            header += f"-- NEEDS REVIEW: {m['note']}\n"
        path = os.path.join(models_dir, m["layer"], f"{m['name']}.sql")
        with open(path, "w") as fh:
            fh.write(header + m["sql"] + "\n")
    with open(os.path.join(models_dir, "marts", "schema.yml"), "w") as fh:
        fh.write(_schema_yml(result["models"]))
