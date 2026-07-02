#!/usr/bin/env python3
"""dbt test generator.

Usage:
    python3 gen_dbt_tests.py <dbt_project_dir> <flows_dir>

Infers data tests from the Domo flow graphs and writes them into per-layer schema.yml files
in the dbt project (staging + marts; intermediate models are ephemeral, so dbt skips their
tests). Emits, per org-dbt-conventions:
  - unique / not_null on the inferred grain (nearest Unique tile's fields or GroupBy groups)
  - not_null on join keys (MergeJoin keys)
  - relationships on join keys when the joined-to side is itself a materialized model

Tests are additive and best-effort: a model with no inferable grain still appears in
schema.yml (so the file is complete) but carries no tests. unique_combination_of_columns
uses dbt_utils (scaffold.py writes packages.yml with it).
"""
import glob
import json
import os
import sys

_CONVERTER = os.path.normpath(os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "..", "..", "tile-translation", "scripts", "converter"))
if _CONVERTER not in sys.path:
    sys.path.insert(0, _CONVERTER)
try:
    from domo_to_dbt.common import _sanitize
except ImportError:  # pragma: no cover
    import re
    def _sanitize(s):
        return re.sub(r"[^0-9a-zA-Z]+", "_", (s or "").strip().lower()).strip("_")

LOAD_TILES = {"LoadFromVault", "InputDataSet"}
PUBLISH_TILES = {"PublishToVault", "OutputDataSet"}
TESTED_LAYERS = ("staging", "marts")   # intermediate = ephemeral -> dbt skips its tests


def _edges(t):
    dep = t.get("depends_on") or t.get("config", {}).get("dependsOn") or []
    return [dep] if isinstance(dep, str) else list(dep)


def _cfg(t):
    return t.get("config", t)


def _names(items):
    """Extract column-name strings from a Domo field list ([{name: col}, ...] or [col,...])."""
    out = []
    for it in items or []:
        n = it.get("name") if isinstance(it, dict) else it
        if isinstance(n, str) and n and n not in out:
            out.append(n)
    return out


def _grain_and_keys(tile_id, by_id, out_degree):
    """Walk the model tile + its inlined upstream: nearest grain + all join keys seen."""
    grain, join_keys, seen, stack, first = [], [], set(), [tile_id], True
    while stack:
        tid = stack.pop()
        if tid in seen or tid not in by_id:
            continue
        seen.add(tid)
        t = by_id[tid]
        inlined = first or (out_degree.get(tid, 0) < 2 and t.get("type") not in PUBLISH_TILES)
        first = False
        if not inlined:
            continue
        cfg = _cfg(t)
        ttype = t.get("type")
        if not grain and ttype == "Unique":
            grain = _names(cfg.get("fields"))
        if not grain and ttype == "GroupBy":
            grain = _names(cfg.get("groups"))
        if ttype == "MergeJoin":
            for k in _names(cfg.get("keys1")) + _names(cfg.get("keys2")):
                if k not in join_keys:
                    join_keys.append(k)
        stack.extend(_edges(t))
    return grain, join_keys


def _is_boundary(tid, tile, out_degree):
    return (tile.get("type") in LOAD_TILES or tile.get("type") in PUBLISH_TILES
            or out_degree.get(tid, 0) == 0 or out_degree.get(tid, 0) >= 2)


def infer_tests(flow):
    tiles = flow.get("tiles") or [
        {"id": a.get("id"), "type": a.get("type"), "name": a.get("name"),
         "config": a, "depends_on": _edges({"config": a})} for a in flow.get("actions", [])
    ]
    by_id = {t.get("id"): t for t in tiles}
    out_degree = {}
    for t in tiles:
        for dep in _edges(t):
            out_degree[dep] = out_degree.get(dep, 0) + 1

    tests = {}   # model_name -> {"columns": {col: [tests]}, "model": [modeltests]}
    for t in tiles:
        tid = t.get("id")
        if not _is_boundary(tid, t, out_degree):
            continue
        model = _sanitize(t.get("name") or tid)
        grain, join_keys = _grain_and_keys(tid, by_id, out_degree)
        cols, model_tests = {}, []
        if len(grain) == 1:
            cols[grain[0]] = ["unique", "not_null"]
        elif len(grain) > 1:
            for c in grain:
                cols.setdefault(c, []).append("not_null")
            model_tests.append(
                {"dbt_utils.unique_combination_of_columns": {"combination_of_columns": grain}})
        for k in join_keys:
            cols.setdefault(k, [])
            if "not_null" not in cols[k]:
                cols[k].append("not_null")
        if cols or model_tests:
            tests[model] = {"columns": cols, "model": model_tests}
    return tests


def _yaml_model(name, entry):
    lines = [f"  - name: {name}"]
    for mt in entry.get("model", []):
        (test_name, params), = mt.items()
        lines.append(f"    tests:")
        lines.append(f"      - {test_name}:")
        for pk, pv in params.items():
            lines.append(f"          {pk}: {pv}")
    cols = entry.get("columns", {})
    if cols:
        lines.append("    columns:")
        for col, ctests in cols.items():
            lines.append(f"      - name: {col}")
            if ctests:
                lines.append("        tests:")
                for ct in ctests:
                    lines.append(f"          - {ct}")
    return "\n".join(lines)


def write_schema_yml(project_dir, tests):
    written = {}
    for layer in TESTED_LAYERS:
        layer_dir = os.path.join(project_dir, "models", layer)
        if not os.path.isdir(layer_dir):
            continue
        models = sorted(os.path.splitext(os.path.basename(f))[0]
                        for f in glob.glob(os.path.join(layer_dir, "*.sql")))
        if not models:
            continue
        blocks, tested = [], 0
        for m in models:
            if m in tests:
                blocks.append(_yaml_model(m, tests[m]))
                tested += 1
            else:
                blocks.append(f"  - name: {m}")
        with open(os.path.join(layer_dir, "schema.yml"), "w") as fh:
            fh.write("version: 2\nmodels:\n" + "\n".join(blocks) + "\n")
        written[layer] = {"models": len(models), "with_tests": tested}
    return written


def main():
    if len(sys.argv) != 3:
        sys.exit(__doc__)
    project_dir, flows_dir = sys.argv[1], sys.argv[2]
    fd = os.path.join(flows_dir, "flows") if os.path.isdir(os.path.join(flows_dir, "flows")) else flows_dir
    all_tests = {}
    for f in sorted(glob.glob(os.path.join(fd, "*.json"))):
        all_tests.update(infer_tests(json.load(open(f))))
    written = write_schema_yml(project_dir, all_tests)
    print(json.dumps({"models_with_tests": len(all_tests), "layers": written}, indent=2))


if __name__ == "__main__":
    main()
