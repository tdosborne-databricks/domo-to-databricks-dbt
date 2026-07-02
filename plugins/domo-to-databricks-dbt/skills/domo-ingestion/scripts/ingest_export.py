#!/usr/bin/env python3
"""Mode A ingester: customer-provided Domo export -> normalized flow graph + inventory.

Usage:
    python3 ingest_export.py <export_dir> <out_dir>

Reads whatever the customer delivered (default: dataflows.json + dataset_mapping.json), normalizes
each dataflow into the internal graph schema (see references/normalized-graph-schema.md), runs the
completeness check, and writes:
    <out_dir>/flows/<flow_id>.json
    <out_dir>/inventory.csv
    <out_dir>/completeness_report.json

STATUS: scaffold. The DAG/edge normalization can reuse the validated reader in
tile-translation/scripts/converter/domo_to_dbt/dag.py. Fill the parser against the REAL export
format at Build Sequence Step 2 (see references/export-format-mapping.md).
"""
import csv
import json
import os
import sys

# 14 known Domo Magic ETL tile types; hard types weight the complexity score.
HARD_TILE_TYPES = {"SQL", "DateCalculator", "Normalizer"}


def _edges(tile):
    """Normalize Domo's dependsOn / inputs / input into a single depends_on list."""
    dep = tile.get("dependsOn") or tile.get("inputs") or tile.get("input") or []
    return [dep] if isinstance(dep, str) else list(dep)


def normalize_flow(flow):
    tiles = flow.get("actions") or flow.get("tiles") or []
    norm_tiles = [
        {
            "id": t.get("id"),
            "type": t.get("type"),
            "name": t.get("name"),
            "config": t,               # raw config, verbatim — tile-translation transpiles this
            "depends_on": _edges(t),
        }
        for t in tiles
    ]
    return {
        "flow_id": str(flow.get("id", "")),
        "name": flow.get("name"),
        "schedule": {"type": "unknown", "expr": None, "source": "export"},  # TODO: parse real schedule
        "inputs": [],   # TODO: from LoadFromVault tiles + dataset_mapping + schema files if present
        "outputs": [],  # TODO: from PublishToVault tiles
        "tiles": norm_tiles,
    }


def completeness(flow_norm):
    missing = []
    if flow_norm["schedule"]["expr"] is None:
        missing.append("schedule")
    if not any(i.get("schema") for i in flow_norm["inputs"]):
        missing.append("input_schemas")
    return {"flow_id": flow_norm["flow_id"], "missing": missing}


def main(export_dir, out_dir):
    with open(os.path.join(export_dir, "dataflows.json")) as fh:
        flows = json.load(fh)
    if isinstance(flows, dict):
        flows = [flows]

    os.makedirs(os.path.join(out_dir, "flows"), exist_ok=True)
    inventory, comp = [], []
    for flow in flows:
        n = normalize_flow(flow)
        with open(os.path.join(out_dir, "flows", f"{n['flow_id']}.json"), "w") as fh:
            json.dump(n, fh, indent=2)
        types = [t["type"] for t in n["tiles"]]
        inventory.append({
            "flow_id": n["flow_id"], "flow_name": n["name"], "tile_count": len(types),
            "tile_types": ";".join(sorted(set(types))),
            "input_count": len(n["inputs"]), "output_count": len(n["outputs"]),
            "schedule_known": n["schedule"]["expr"] is not None,
            "complexity_score": len(types) + 3 * sum(t in HARD_TILE_TYPES for t in types),
        })
        comp.append(completeness(n))

    with open(os.path.join(out_dir, "inventory.csv"), "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(inventory[0].keys()) if inventory else [])
        w.writeheader()
        w.writerows(inventory)
    with open(os.path.join(out_dir, "completeness_report.json"), "w") as fh:
        json.dump(comp, fh, indent=2)
    print(f"Ingested {len(flows)} flow(s) → {out_dir}/flows, inventory.csv, completeness_report.json")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit(__doc__)
    main(sys.argv[1], sys.argv[2])
