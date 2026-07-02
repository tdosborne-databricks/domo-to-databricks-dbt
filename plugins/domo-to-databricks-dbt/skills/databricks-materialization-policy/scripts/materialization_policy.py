#!/usr/bin/env python3
"""Propose per-model materialization config from the ingestion inventory + flow graphs.

Usage:
    python3 materialization_policy.py <inventory.csv> <flows_dir> \
        [--catalog NAME] [--schema NAME] [--big-rows N] > materialization.json

Applies the heuristics in references/materialization-rules.md. This is a STARTING PROPOSAL —
surface it for review; do not silently commit storage decisions.

Model set matches the tile-translation converter's boundary granularity (a tile becomes a
model when it is an input, an output, or fanned out to >=2 consumers; everything else is
inlined as a CTE). Names are sanitized the same way the converter names models, so the
proposal maps 1:1 onto the scaffolded project.
"""
import argparse
import csv
import json
import os
import re
import sys

# Reuse the converter's name sanitizer so proposal keys match scaffolded model names.
_HERE = os.path.dirname(os.path.abspath(__file__))
_CONVERTER = os.path.normpath(
    os.path.join(_HERE, "..", "..", "tile-translation", "scripts", "converter")
)
if _CONVERTER not in sys.path:
    sys.path.insert(0, _CONVERTER)
try:
    from domo_to_dbt.common import _sanitize
except ImportError:  # pragma: no cover — fall back to a local sanitizer
    def _sanitize(s):
        return re.sub(r"[^0-9a-zA-Z]+", "_", (s or "").strip().lower()).strip("_") or "unnamed"

LOAD_TILES = {"LoadFromVault", "InputDataSet"}
PUBLISH_TILES = {"PublishToVault", "OutputDataSet"}
# Tiles that materially reshape data -> favor a durable table over a view.
HEAVY_TILES = {"MergeJoin", "GroupBy", "WindowAction", "Normalizer", "SQL", "DateCalculator"}
# Append/union upstream is the Domo signal for an incrementally-grown fact.
APPEND_TILES = {"UnionAll", "Append"}

# Column-bearing config keys, by concern (used to recover clustering candidates).
_JOIN_KEYS = ("keys1", "keys2", "leftKeys", "rightKeys")
_FILTER_KEYS = ("filters", "filterList", "conditions")
_GROUP_KEYS = ("groupByColumns", "groupKeys", "groupBy", "fields")
_COL_FIELDS = ("column", "fieldName", "name", "field")


def _edges(tile):
    dep = tile.get("depends_on") or tile.get("dependsOn") or tile.get("inputs") or tile.get("input") or []
    return [dep] if isinstance(dep, str) else list(dep)


def _cfg(tile):
    return tile.get("config", tile)


def _predicate_cols(tile):
    """Recover join/filter/groupby column names from one tile's config, order-preserving."""
    cfg = _cfg(tile)
    cols = []

    def _add(v):
        if isinstance(v, str) and v and v not in cols:
            cols.append(v)

    for k in _JOIN_KEYS + _GROUP_KEYS:
        v = cfg.get(k)
        if isinstance(v, list):
            for it in v:
                _add(it if isinstance(it, str) else _first_col(it))
        elif isinstance(v, str):
            _add(v)
    for k in _FILTER_KEYS:
        v = cfg.get(k)
        if isinstance(v, list):
            for it in v:
                _add(_first_col(it))
    return cols


def _first_col(obj):
    if isinstance(obj, dict):
        for f in _COL_FIELDS:
            if isinstance(obj.get(f), str):
                return obj[f]
    return None


def _cluster_candidates(tile_id, by_id, out_degree, cap=4):
    """Walk upstream through inlined (non-boundary) tiles collecting predicate columns."""
    cols, seen, stack = [], set(), [tile_id]
    first = True
    while stack and len(cols) < cap:
        tid = stack.pop()
        if tid in seen or tid not in by_id:
            continue
        seen.add(tid)
        t = by_id[tid]
        # include the model tile itself + any inlined (out_degree<2, non-output) upstream tile
        inlined = first or (out_degree.get(tid, 0) < 2 and t.get("type") not in PUBLISH_TILES)
        if inlined:
            for c in _predicate_cols(t):
                if c not in cols:
                    cols.append(c)
            stack.extend(_edges(t))
        first = False
    return cols[:cap]


def _is_boundary(tid, tile, out_degree):
    return (tile.get("type") in LOAD_TILES or tile.get("type") in PUBLISH_TILES
            or out_degree.get(tid, 0) == 0 or out_degree.get(tid, 0) >= 2)


def _flow_has_append(by_id):
    return any(t.get("type") in APPEND_TILES for t in by_id.values())


def propose_for_flow(flow, catalog, schema, big_rows, schedule_known):
    tiles = flow.get("tiles") or [
        {"id": a.get("id"), "type": a.get("type"), "name": a.get("name"), "config": a,
         "depends_on": _edges(a)} for a in flow.get("actions", [])
    ]
    by_id = {t.get("id"): t for t in tiles}
    out_degree = {}
    for t in tiles:
        for dep in _edges(t):
            out_degree[dep] = out_degree.get(dep, 0) + 1

    domain = schema or _sanitize(flow.get("name") or flow.get("flow_id") or "domo")
    append_flow = _flow_has_append(by_id)
    out = {}
    for t in tiles:
        tid, ttype = t.get("id"), t.get("type")
        if not _is_boundary(tid, t, out_degree):
            continue                                   # inlined as a CTE — not a model
        name = _sanitize(t.get("name") or tid)
        reused = out_degree.get(tid, 0) >= 2
        is_output = ttype in PUBLISH_TILES

        if ttype in LOAD_TILES:
            materialized, reason = "view", "staging passthrough"
        elif is_output and schedule_known and append_flow:
            materialized, reason = "incremental", "scheduled append-style fact"
        elif is_output or ttype in HEAVY_TILES or reused:
            materialized = "table"
            reason = ("terminal output" if is_output else
                      "reused by >=2 models" if reused else "heavy transform")
        else:
            materialized, reason = "view", "light transform, single consumer"

        entry = {
            "materialized": materialized,
            "reason": reason,
            "unity_catalog_name": f"{catalog}.{domain}.{name}",
        }
        if materialized in ("table", "incremental"):
            cb = _cluster_candidates(tid, by_id, out_degree)
            # Clustering only pays off above ~big_rows; row counts aren't in the Domo export,
            # so emit candidates + a review note rather than guessing sizes.
            entry["cluster_by"] = cb
            entry["cluster_note"] = (
                f"apply liquid clustering only if >~{big_rows:,} rows; verify against real data"
                if cb else f"no join/filter/groupby cols recovered; skip clustering"
            )
        if materialized == "incremental":
            keys = _cluster_candidates(tid, by_id, out_degree, cap=2)
            entry["incremental_strategy"] = "merge"
            entry["unique_key"] = keys or ["<set_merge_key>"]
        out[name] = entry
    return out


def _inventory_schedule(inventory_path):
    """flow_id -> schedule_known (bool), from inventory.csv."""
    sched = {}
    if not os.path.exists(inventory_path):
        return sched
    with open(inventory_path) as fh:
        for row in csv.DictReader(fh):
            sched[str(row.get("flow_id"))] = str(row.get("schedule_known", "")).lower() == "true"
    return sched


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("inventory")
    ap.add_argument("flows_dir")
    ap.add_argument("--catalog", default="domo_migration")
    ap.add_argument("--schema", default=None, help="override target schema (else derived from flow name)")
    ap.add_argument("--big-rows", type=int, default=1_000_000)
    args = ap.parse_args()

    sched = _inventory_schedule(args.inventory)
    flows_dir = os.path.join(args.flows_dir, "flows") if os.path.isdir(
        os.path.join(args.flows_dir, "flows")) else args.flows_dir

    proposal = {}
    for fn in sorted(os.listdir(flows_dir)):
        if not fn.endswith(".json"):
            continue
        flow = json.load(open(os.path.join(flows_dir, fn)))
        fid = str(flow.get("flow_id") or flow.get("id") or "")
        proposal.update(propose_for_flow(
            flow, args.catalog, args.schema, args.big_rows, sched.get(fid, False)))
    print(json.dumps(proposal, indent=2))


if __name__ == "__main__":
    main()
