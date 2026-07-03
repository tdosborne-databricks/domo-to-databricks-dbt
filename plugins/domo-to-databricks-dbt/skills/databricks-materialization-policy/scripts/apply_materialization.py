#!/usr/bin/env python3
"""Apply auto materialization defaults to a scaffolded dbt project.

After org-dbt-conventions scaffolds models, this script:
  - sets the intermediate layer default to `view` in dbt_project.yml
  - promotes intermediate models with fan-out >= 2 to Delta `table` (+ column mapping)
  - leaves single-consumer intermediates as `view`

Run **before the first `dbt build`** (dbt-error-triage). After applying, run:
  dbt build --profiles-dir <dir>
Use `--full-refresh` when flipping an existing project from all-table to view/table mix.

Usage:
    python3 apply_materialization.py <dbt_project_dir> [--dry-run]
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

REF_RE = re.compile(r"\{\{\s*ref\(\s*['\"]([^'\"]+)['\"]\s*\)\s*\}\}")
CONFIG_RE = re.compile(
    r"\{\{\s*config\([^)]*\)\s*\}\}\s*\n",
    re.DOTALL,
)
TABLE_CONFIG = (
    "{{ config(materialized='table', "
    "tblproperties={'delta.columnMapping.mode': 'name', "
    "'delta.minReaderVersion': '2', 'delta.minWriterVersion': '5'}) }}\n"
)
VIEW_CONFIG = "{{ config(materialized='view') }}\n"

INTERMEDIATE_YML_VIEW = (
    "    intermediate: {+materialized: view}\n"
)
INTERMEDIATE_YML_TABLE = re.compile(
    r"    intermediate: \{.*?\+materialized: table.*?\}\n",
    re.DOTALL,
)


def intermediate_fanout(project_dir: Path) -> dict[str, int]:
    """Count downstream refs to each intermediate model (all layers)."""
    models_root = project_dir / "models"
    intermediates = {p.stem for p in (models_root / "intermediate").glob("*.sql")}
    refs_in: dict[str, set[str]] = {name: set() for name in intermediates}
    for layer in ("staging", "intermediate", "marts"):
        layer_dir = models_root / layer
        if not layer_dir.exists():
            continue
        for sql_file in layer_dir.glob("*.sql"):
            consumer = sql_file.stem
            for target in REF_RE.findall(sql_file.read_text()):
                if target in refs_in:
                    refs_in[target].add(consumer)
    return {name: len(refs_in[name]) for name in intermediates}


def _patch_dbt_project_yml(path: Path, dry_run: bool) -> bool:
    text = path.read_text()
    if INTERMEDIATE_YML_TABLE.search(text):
        new_text = INTERMEDIATE_YML_TABLE.sub(INTERMEDIATE_YML_VIEW, text, count=1)
    elif "intermediate:" not in text:
        return False
    else:
        new_text = text
    if new_text == text:
        return False
    if not dry_run:
        path.write_text(new_text)
    return True


def _patch_model_config(path: Path, materialized: str, dry_run: bool) -> bool:
    text = path.read_text()
    header = TABLE_CONFIG if materialized == "table" else VIEW_CONFIG
    if CONFIG_RE.match(text):
        new_text = CONFIG_RE.sub(header, text, count=1)
    else:
        new_text = header + text
    if new_text == text:
        return False
    if not dry_run:
        path.write_text(new_text)
    return True


def apply(project_dir: str | Path, dry_run: bool = False) -> dict:
    root = Path(project_dir)
    inter_dir = root / "models" / "intermediate"
    if not inter_dir.is_dir():
        raise FileNotFoundError(f"no intermediate models under {inter_dir}")

    fanout = intermediate_fanout(root)
    keep_table = sorted(n for n, d in fanout.items() if d >= 2)
    to_view = sorted(n for n, d in fanout.items() if d < 2)

    yml_changed = _patch_dbt_project_yml(root / "dbt_project.yml", dry_run)
    table_changed = []
    view_changed = []
    for name in keep_table:
        if _patch_model_config(inter_dir / f"{name}.sql", "table", dry_run):
            table_changed.append(name)
    for name in to_view:
        if _patch_model_config(inter_dir / f"{name}.sql", "view", dry_run):
            view_changed.append(name)

    return {
        "intermediate_total": len(fanout),
        "promoted_to_table": keep_table,
        "kept_as_view": to_view,
        "dbt_project_yml_updated": yml_changed,
        "models_patched_to_table": table_changed,
        "models_patched_to_view": view_changed,
        "dry_run": dry_run,
        "rebuild_required": not dry_run and (yml_changed or table_changed or view_changed),
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("dbt_project_dir")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    import json

    result = apply(args.dbt_project_dir, dry_run=args.dry_run)
    print(json.dumps(result, indent=2))
    if result.get("rebuild_required"):
        print(
            "\nNext: dbt build --profiles-dir <dir>  "
            "(add --full-refresh if relation types changed on an existing warehouse)",
            file=sys.stderr,
        )


if __name__ == "__main__":
    main()
