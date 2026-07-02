#!/usr/bin/env python3
"""Scaffolding generator for the migrated dbt project.

Usage:
    python3 scaffold.py <flows_dir> <dbt_project_dir> [overrides.json]

<flows_dir>   directory of normalized flows from domo-ingestion (contains flows/*.json),
              or a single normalized flow .json file.
<overrides>   optional {dataset_id|name: "catalog.schema.table"} to wire LoadFromVault
              tiles onto real Unity Catalog tables (see references/conventions.md).

Reuses the validated emitters in tile-translation/scripts/converter/domo_to_dbt
(sources.py -> sources.yml + UC-table resolution; project.py -> model files,
dbt_project.yml, layering, ref()-rewiring) rather than reimplementing them, then adds the
org-standard project files (packages.yml, profiles template, .sqlfluff, README).

One flow -> one project at <dbt_project_dir>. Multiple flows -> one subproject per flow
(<dbt_project_dir>/<flow>) to avoid the shared-root overwrite the raw converter warns about.
"""
import json
import os
import sys

# Reuse the tile-translation converter engine (locate it relative to this file).
_HERE = os.path.dirname(os.path.abspath(__file__))
_CONVERTER = os.path.normpath(
    os.path.join(_HERE, "..", "..", "tile-translation", "scripts", "converter")
)
if _CONVERTER not in sys.path:
    sys.path.insert(0, _CONVERTER)
try:
    from domo_to_dbt.common import _sanitize
    from domo_to_dbt.project import convert_flow_to_dbt, write_dbt_project
except ImportError as e:  # pragma: no cover
    sys.exit(f"cannot import the tile-translation converter from {_CONVERTER}: {e}")


def _raw_flow(norm):
    """Reconstruct the raw flow shape the converter expects from a normalized flow."""
    if "actions" in norm:                       # already raw
        return norm
    return {
        "id": norm.get("flow_id") or norm.get("id"),
        "name": norm.get("name"),
        "actions": [t.get("config", t) for t in norm.get("tiles", [])],
    }


def _dataset_mapping(norm):
    """dataset_id -> name, from the normalized flow's inputs (and outputs)."""
    m = {}
    for group in ("inputs", "outputs"):
        for d in norm.get(group, []) or []:
            if d.get("dataset_id"):
                m[str(d["dataset_id"])] = d.get("name")
    return m


def _load_flows(path):
    if os.path.isfile(path):
        return [json.load(open(path))]
    flows_dir = os.path.join(path, "flows") if os.path.isdir(os.path.join(path, "flows")) else path
    out = []
    for fn in sorted(os.listdir(flows_dir)):
        if fn.endswith(".json"):
            out.append(json.load(open(os.path.join(flows_dir, fn))))
    if not out:
        sys.exit(f"no flow .json files under {flows_dir}")
    return out


_PACKAGES_YML = (
    "packages:\n"
    "  - package: dbt-labs/dbt_utils\n"
    "    version: [\">=1.1.0\", \"<2.0.0\"]\n"
)

_SQLFLUFF = (
    "[sqlfluff]\n"
    "dialect = databricks\n"
    "templater = dbt\n"
    "max_line_length = 120\n"
)


def _profiles_yml(project_name):
    return (
        f"{project_name}:\n"
        "  target: dev\n"
        "  outputs:\n"
        "    dev:\n"
        "      type: databricks\n"
        "      catalog: main            # UC catalog for the migrated marts\n"
        "      schema: domo_migration   # build schema\n"
        "      host: <workspace-host>.cloud.databricks.com\n"
        "      http_path: /sql/1.0/warehouses/<warehouse_id>\n"
        "      auth_type: oauth         # PATs are disabled on many workspaces; OAuth reuses the CLI session\n"
        "      threads: 4\n"
    )


def _project_readme(project_name, flow_name, n_models, n_sources):
    return (
        f"# {project_name}\n\n"
        f"dbt project migrated from the Domo Magic ETL flow **{flow_name}**.\n\n"
        f"- {n_models} models (staging=view / intermediate=ephemeral / marts=table)\n"
        f"- {n_sources} sources (wire real UC tables in `models/sources.yml` via overrides)\n\n"
        "## Run\n"
        "```bash\n"
        "dbt deps        # install packages.yml\n"
        "dbt parse       # compile + validate the DAG\n"
        "dbt build       # run models + tests\n"
        "```\n"
        "Set connection details in `profiles.yml`. See the org-dbt-conventions skill for the\n"
        "layering / naming / required-tests rules this project follows.\n"
    )


def _write_org_files(out_dir, project_name, result, flow_name):
    with open(os.path.join(out_dir, "packages.yml"), "w") as fh:
        fh.write(_PACKAGES_YML)
    with open(os.path.join(out_dir, "profiles.yml"), "w") as fh:
        fh.write(_profiles_yml(project_name))
    with open(os.path.join(out_dir, ".sqlfluff"), "w") as fh:
        fh.write(_SQLFLUFF)
    with open(os.path.join(out_dir, "README.md"), "w") as fh:
        fh.write(_project_readme(project_name, flow_name,
                                 len(result["models"]), len(result["sources"])))


def scaffold(flows_dir, dbt_project_dir, overrides=None):
    flows = _load_flows(flows_dir)
    multi = len(flows) > 1
    written = []
    for norm in flows:
        raw = _raw_flow(norm)
        mapping = _dataset_mapping(norm)
        result = convert_flow_to_dbt(raw, mapping, overrides)
        project_name = _sanitize(raw.get("name") or raw.get("id") or "domo_dbt_project")
        out_dir = os.path.join(dbt_project_dir, project_name) if multi else dbt_project_dir
        write_dbt_project(result, out_dir, project_name=project_name)
        _write_org_files(out_dir, project_name, result, raw.get("name"))
        review = [m for m in result["models"] if m.get("needs_review")]
        print(f"  {project_name}: {len(result['models'])} models, "
              f"{len(result['sources'])} sources, {len(review)} need review -> {out_dir}")
        written.append(out_dir)
    return written


def main():
    if len(sys.argv) < 3:
        sys.exit(__doc__)
    overrides = json.load(open(sys.argv[3])) if len(sys.argv) > 3 else None
    scaffold(sys.argv[1], sys.argv[2], overrides)


if __name__ == "__main__":
    main()
