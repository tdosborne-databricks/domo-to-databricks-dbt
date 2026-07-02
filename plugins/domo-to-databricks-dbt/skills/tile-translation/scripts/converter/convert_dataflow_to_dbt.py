"""CLI: convert a Domo Step-1 extract into a dbt-databricks project.

Usage:
    python3 convert_dataflow_to_dbt.py <extract_dir> <out_dir> [overrides.json]
"""
import json
import os
import sys

from domo_to_dbt import convert_flow_to_dbt, write_dbt_project
from domo_to_dbt.common import _sanitize


def main(extract_dir, out_dir, overrides_path=None):
    with open(os.path.join(extract_dir, "dataflows.json")) as fh:
        dataflows = json.load(fh)
    if isinstance(dataflows, dict):
        dataflows = [dataflows]
    # write_dbt_project writes a shared dbt_project.yml/sources.yml per call, so multiple flows
    # sharing one out_dir would overwrite each other's root files. When >1 flow, give each its
    # own subproject dir (mirrors org-dbt-conventions/scaffold.py); single flow uses out_dir as-is.
    multi = len(dataflows) > 1
    with open(os.path.join(extract_dir, "dataset_mapping.json")) as fh:
        dataset_mapping = json.load(fh)
    overrides = {}
    if overrides_path:
        with open(overrides_path) as fh:
            overrides = json.load(fh)

    merged = {"flows": [], "needs_review": [], "sources_needing_table": []}
    for flow in dataflows:
        res = convert_flow_to_dbt(flow, dataset_mapping, overrides)
        # Derive the dbt project/profile name from the flow so different flows
        # get distinct, meaningful names (this is the name to put in profiles.yml).
        project_name = _sanitize(flow.get("name") or "") or "domo_dbt_project"
        flow_out = os.path.join(out_dir, project_name) if multi else out_dir
        write_dbt_project(res, flow_out, project_name=project_name)
        print(f"  dbt project/profile name: {project_name} -> {flow_out}")
        merged["flows"].append({"name": flow.get("name"), "models": len(res["models"])})
        merged["needs_review"].extend(res["report"]["needs_review"])
        merged["sources_needing_table"].extend(
            [s for s in res["sources"] if not s["catalog_table"]])

    with open(os.path.join(out_dir, "conversion_report.json"), "w") as fh:
        json.dump(merged, fh, indent=2)
    print(f"Wrote dbt project to {out_dir}")
    print(f"  models: {sum(f['models'] for f in merged['flows'])}")
    print(f"  needs-review tiles: {len(merged['needs_review'])}")
    print(f"  sources without a real-table override (wire via overrides.json): {len(merged['sources_needing_table'])}")
    return merged


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print(__doc__, file=sys.stderr)
        sys.exit(1)
    main(sys.argv[1], sys.argv[2], sys.argv[3] if len(sys.argv) > 3 else None)
