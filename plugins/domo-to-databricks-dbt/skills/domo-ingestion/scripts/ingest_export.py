#!/usr/bin/env python3
"""Mode A ingester: customer-provided Domo export -> normalized flow graph + inventory.

Usage:
    python3 ingest_export.py <export_dir> <out_dir>

Reads whatever the customer delivered (dataflows.json + optional dataset_mapping.json /
datasets.json), normalizes each dataflow into the internal graph schema (see
references/normalized-graph-schema.md), runs the completeness check, and writes:
    <out_dir>/flows/<flow_id>.json
    <out_dir>/inventory.csv
    <out_dir>/completeness_report.json

The output is mode-agnostic: a Mode B live extract (domo_api_client.py) produces the same
export files, so this ingester normalizes either one.
"""
import csv
import json
import os
import sys

# 14 known Domo Magic ETL tile types; hard types weight the complexity score.
HARD_TILE_TYPES = {"SQL", "DateCalculator", "Normalizer"}
LOAD_TILES = {"LoadFromVault", "InputDataSet"}
PUBLISH_TILES = {"PublishToVault", "OutputDataSet"}

# Domo spells the referenced dataset id / name a few ways depending on tile & version.
_DATASET_ID_KEYS = ("dataSourceId", "datasetId", "dataSetId", "dataset_id", "sourceId")
_DATASET_NAME_KEYS = ("dataSourceName", "datasetName", "dataSetName")


def _edges(tile):
    """Normalize Domo's dependsOn / inputs / input into a single depends_on list."""
    dep = tile.get("dependsOn") or tile.get("inputs") or tile.get("input") or []
    return [dep] if isinstance(dep, str) else list(dep)


def _dataset_id(tile):
    for k in _DATASET_ID_KEYS:
        if tile.get(k):
            return str(tile[k])
    return None


def _dataset_name(tile, ds_id, dataset_mapping):
    for k in _DATASET_NAME_KEYS:
        if tile.get(k):
            return tile[k]
    if ds_id and ds_id in dataset_mapping:
        return dataset_mapping[ds_id]
    return tile.get("name")


def _schedule(flow):
    """Best-effort schedule parse. Domo stores this under a few keys; unknown if absent."""
    ts = flow.get("triggerSettings") or flow.get("schedule") or flow.get("runSettings") or {}
    if isinstance(ts, str):  # some exports store a raw cron / string
        return {"type": "schedule", "expr": ts, "source": "export"}
    if isinstance(ts, dict):
        expr = (ts.get("cron") or ts.get("cronExpression") or ts.get("scheduleExpression")
                or ts.get("expression"))
        ttype = ts.get("type") or ts.get("triggerType")
        # A DATA trigger (run when an input updates) has no cron but IS a known schedule.
        if ttype and str(ttype).upper() in ("DATA", "DATASET", "ONDATA"):
            return {"type": "data", "expr": f"on_update:{ttype}", "source": "export"}
        if expr:
            return {"type": (ttype or "schedule"), "expr": expr, "source": "export"}
    return {"type": "unknown", "expr": None, "source": "export"}


def _inputs(tiles, dataset_mapping, dataset_schema):
    out, seen = [], set()
    for t in tiles:
        if t.get("type") not in LOAD_TILES:
            continue
        ds_id = _dataset_id(t)
        key = ds_id or t.get("id")
        if key in seen:
            continue
        seen.add(key)
        out.append({
            "name": _dataset_name(t, ds_id, dataset_mapping),
            "dataset_id": ds_id,
            "schema": dataset_schema.get(ds_id) if ds_id else None,
        })
    return out


def _outputs(tiles, dataset_mapping):
    out = []
    for t in tiles:
        if t.get("type") not in PUBLISH_TILES:
            continue
        ds_id = _dataset_id(t)
        out.append({
            "name": _dataset_name(t, ds_id, dataset_mapping) or t.get("name"),
            "dataset_id": ds_id,
        })
    return out


def normalize_flow(flow, dataset_mapping=None, dataset_schema=None):
    dataset_mapping = dataset_mapping or {}
    dataset_schema = dataset_schema or {}
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
        "schedule": _schedule(flow),
        "inputs": _inputs(tiles, dataset_mapping, dataset_schema),
        "outputs": _outputs(tiles, dataset_mapping),
        "tiles": norm_tiles,
    }


def completeness(flow_norm):
    missing = []
    if flow_norm["schedule"]["expr"] is None:
        missing.append("schedule")
    if not flow_norm["inputs"]:
        missing.append("inputs")
    elif not any(i.get("schema") for i in flow_norm["inputs"]):
        missing.append("input_schemas")
    if not flow_norm["outputs"]:
        missing.append("outputs")
    return {"flow_id": flow_norm["flow_id"], "missing": missing}


def _load_json(path):
    return json.load(open(path)) if os.path.exists(path) else None


def _build_dataset_schema(datasets):
    """Map dataset_id -> [{name,type}] when the export carries column schemas (often absent)."""
    schema = {}
    for d in datasets or []:
        cols = d.get("columns") or d.get("schema")
        if isinstance(cols, dict):
            cols = cols.get("columns")
        if cols:
            schema[str(d.get("id"))] = [
                {"name": c.get("name") or c.get("id"), "type": c.get("type")}
                for c in cols if isinstance(c, dict)
            ]
    return schema


def main(export_dir, out_dir):
    flows = _load_json(os.path.join(export_dir, "dataflows.json"))
    if flows is None:
        sys.exit(f"no dataflows.json in {export_dir}")
    if isinstance(flows, dict):
        flows = [flows]

    dataset_mapping = _load_json(os.path.join(export_dir, "dataset_mapping.json")) or {}
    dataset_mapping = {str(k): v for k, v in dataset_mapping.items()}
    dataset_schema = _build_dataset_schema(_load_json(os.path.join(export_dir, "datasets.json")))

    os.makedirs(os.path.join(out_dir, "flows"), exist_ok=True)
    inventory, comp = [], []
    for flow in flows:
        n = normalize_flow(flow, dataset_mapping, dataset_schema)
        with open(os.path.join(out_dir, "flows", f"{n['flow_id']}.json"), "w") as fh:
            json.dump(n, fh, indent=2)
        types = [t["type"] for t in n["tiles"]]
        inventory.append({
            "flow_id": n["flow_id"], "flow_name": n["name"], "tile_count": len(types),
            "tile_types": ";".join(sorted({t for t in types if t})),
            "input_count": len(n["inputs"]), "output_count": len(n["outputs"]),
            "schedule_known": n["schedule"]["expr"] is not None,
            "complexity_score": len(types) + 3 * sum(t in HARD_TILE_TYPES for t in types),
        })
        comp.append(completeness(n))

    inventory.sort(key=lambda r: r["complexity_score"], reverse=True)
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
