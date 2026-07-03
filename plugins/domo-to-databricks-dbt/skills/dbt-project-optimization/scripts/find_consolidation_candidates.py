#!/usr/bin/env python3
"""Flag dbt models that are safe-looking candidates for consolidation or cleanup.

Static analysis only — no warehouse access. Flags:
  - trivial_intermediate: exactly one other model refs() it, and its own body has no
    join/aggregation/window/case logic (pure select/select-except passthrough).
  - trivial_staging: a staging-layer model that is a pure `select * from {{ source(...) }}`
    with no renaming or casting.
  - unquoted_raw_columns: models whose column list still carries raw Domo names (spaces,
    mixed case) that could be normalized to snake_case with no collision.

This is a heuristic triage tool for `dbt-project-optimization`, not an auto-refactor script —
every flagged candidate still needs a human/agent decision and a post-change diff.

Usage:
    python3 find_consolidation_candidates.py <dbt_project_dir>
"""
import json
import re
import sys
from pathlib import Path

REF_RE = re.compile(r"\{\{\s*ref\(\s*['\"]([^'\"]+)['\"]\s*\)\s*\}\}")
SOURCE_RE = re.compile(r"\{\{\s*source\(\s*['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)\s*\}\}")
COMPLEX_RE = re.compile(
    r"\bJOIN\b|\bGROUP BY\b|\bOVER\s*\(|\bCASE\b|\bWINDOW\b|\bUNION\b",
    re.IGNORECASE,
)
RAW_COLUMN_RE = re.compile(r"`([^`]*[ ][^`]*)`")


def find_models(project_dir):
    models = {}
    for layer_dir in ("staging", "intermediate", "marts"):
        layer_path = project_dir / "models" / layer_dir
        if not layer_path.exists():
            continue
        for sql_file in layer_path.glob("*.sql"):
            models[sql_file.stem] = {
                "layer": layer_dir,
                "path": str(sql_file),
                "body": sql_file.read_text(),
            }
    return models


def build_ref_graph(models):
    refs_out = {name: set() for name in models}
    refs_in = {name: set() for name in models}
    for name, m in models.items():
        for target in REF_RE.findall(m["body"]):
            refs_out[name].add(target)
            refs_in.setdefault(target, set()).add(name)
    return refs_in, refs_out


def strip_comments(body):
    body = re.sub(r"/\*.*?\*/", "", body, flags=re.DOTALL)
    return re.sub(r"--[^\n]*", "", body)


def is_trivial_body(body):
    stripped = strip_comments(body)
    stripped = re.sub(r"\{\{.*?\}\}", "", stripped, flags=re.DOTALL)
    return not COMPLEX_RE.search(stripped)


def main():
    if len(sys.argv) < 2:
        print("usage: find_consolidation_candidates.py <dbt_project_dir>", file=sys.stderr)
        sys.exit(1)

    project_dir = Path(sys.argv[1])
    models = find_models(project_dir)
    refs_in, refs_out = build_ref_graph(models)

    trivial_intermediate = []
    trivial_staging = []
    unquoted_raw_columns = []

    for name, m in models.items():
        consumers = refs_in.get(name, set())

        if m["layer"] == "intermediate" and len(consumers) == 1 and is_trivial_body(m["body"]):
            trivial_intermediate.append({
                "model": name,
                "path": m["path"],
                "sole_consumer": next(iter(consumers)),
            })

        if m["layer"] == "staging":
            has_source = SOURCE_RE.search(m["body"])
            body_no_comments = strip_comments(m["body"])
            body_no_config = re.sub(r"\{\{\s*config\(.*?\)\s*\}\}", "", body_no_comments, flags=re.DOTALL)
            body_no_source = SOURCE_RE.sub("__REF__", body_no_config)
            is_pure_passthrough = bool(
                has_source and re.fullmatch(
                    r"\s*select\s*\*\s*from\s*__REF__\s*", body_no_source, re.IGNORECASE
                )
            )
            if is_pure_passthrough:
                trivial_staging.append({"model": name, "path": m["path"]})

        raw_cols = sorted(set(RAW_COLUMN_RE.findall(m["body"])))
        if raw_cols:
            unquoted_raw_columns.append({
                "model": name,
                "path": m["path"],
                "raw_columns": raw_cols[:10],
                "raw_column_count": len(raw_cols),
            })

    result = {
        "total_models": len(models),
        "trivial_intermediate_count": len(trivial_intermediate),
        "trivial_intermediate": trivial_intermediate,
        "trivial_staging_count": len(trivial_staging),
        "trivial_staging": trivial_staging,
        "models_with_raw_column_names": len(unquoted_raw_columns),
        "raw_column_samples": unquoted_raw_columns[:20],
    }
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
