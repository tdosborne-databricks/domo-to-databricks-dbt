#!/usr/bin/env python3
"""Generate snapshot SQL (row count + per-column null count + per-column checksum) for a set of
dbt models, and assemble query results back into a comparable snapshot JSON.

Used as the before/after baseline for `dbt-project-optimization`'s self-referential diff — see
`references/validation-without-domo-access.md` for why this replaces a Domo-side comparison.

Column lists come from `target/catalog.json`, which `dbt docs generate` populates with each
model's actual warehouse columns — that's how this generates a real, complete query per model
instead of a placeholder that still needs hand-filling in.

Usage:
    dbt docs generate   # populates target/catalog.json from the live warehouse
    python3 snapshot_outputs.py emit-sql <dbt_project_dir> <model_name> [<model_name> ...]
      -> prints one complete SELECT per model to run against the warehouse

    python3 snapshot_outputs.py assemble <results_dir> <model_name> [<model_name> ...]
      -> <results_dir>/<model_name>.json must each hold the single-row query result
         ({"row_count": N, "col_checksums": {...}, "col_null_counts": {...}}); combines them
         into one snapshot JSON on stdout, ready for diff_snapshots.py
"""
import json
import sys
from pathlib import Path


def load_catalog(project_dir):
    catalog_path = Path(project_dir) / "target" / "catalog.json"
    if not catalog_path.exists():
        raise FileNotFoundError(
            f"{catalog_path} not found — run `dbt docs generate` first so column lists reflect "
            "the live warehouse, not just the SQL source."
        )
    return json.loads(catalog_path.read_text())


def find_columns(catalog, model_name):
    for node_id, node in catalog.get("nodes", {}).items():
        if node_id.endswith(f".{model_name}") or node.get("metadata", {}).get("name") == model_name:
            return list(node["columns"].keys())
    raise KeyError(f"model '{model_name}' not found in catalog.json — check the name and that it built")


def snapshot_sql(model_name, columns):
    checksum_exprs = ",\n    ".join(
        f"SUM(HASH(`{c}`)) AS `checksum__{c}`, "
        f"SUM(CASE WHEN `{c}` IS NULL THEN 1 ELSE 0 END) AS `nulls__{c}`"
        for c in columns
    )
    return (
        f"-- snapshot: {model_name}\n"
        f"SELECT\n"
        f"    COUNT(*) AS row_count,\n"
        f"    {checksum_exprs}\n"
        f"FROM {{{{ ref('{model_name}') }}}}\n"
    )


def emit_sql(project_dir, model_names):
    catalog = load_catalog(project_dir)
    for name in model_names:
        columns = find_columns(catalog, name)
        print(snapshot_sql(name, columns))
        print()


def assemble(results_dir, model_names):
    snapshot = {}
    for name in model_names:
        result_file = Path(results_dir) / f"{name}.json"
        if not result_file.exists():
            snapshot[name] = {"error": f"missing result file {result_file}"}
            continue
        snapshot[name] = json.loads(result_file.read_text())
    print(json.dumps(snapshot, indent=2))


def main():
    args = sys.argv[1:]
    if len(args) < 3 or args[0] not in ("emit-sql", "assemble"):
        print(__doc__, file=sys.stderr)
        sys.exit(1)

    mode, target_dir, *model_names = args
    if mode == "emit-sql":
        emit_sql(target_dir, model_names)
    else:
        assemble(target_dir, model_names)


if __name__ == "__main__":
    main()
