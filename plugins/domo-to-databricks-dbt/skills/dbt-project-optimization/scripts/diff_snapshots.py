#!/usr/bin/env python3
"""Diff two `snapshot_outputs.py` snapshots (before/after an optimization change).

Flags any model whose row_count changed, whose per-column checksum changed, or whose per-column
null count changed. A refactor is only safe if this reports zero mismatches for every model it
touched (directly or downstream).

Usage:
    python3 diff_snapshots.py <baseline_snapshot.json> <after_snapshot.json>
"""
import json
import sys


def diff_model(name, before, after):
    mismatches = []
    if "error" in before or "error" in after:
        mismatches.append({
            "field": "_snapshot",
            "before": before.get("error", "ok"),
            "after": after.get("error", "ok"),
        })
        return mismatches

    all_keys = set(before) | set(after)
    for key in sorted(all_keys):
        if before.get(key) != after.get(key):
            mismatches.append({"field": key, "before": before.get(key), "after": after.get(key)})
    return mismatches


def main():
    if len(sys.argv) != 3:
        print("usage: diff_snapshots.py <baseline_snapshot.json> <after_snapshot.json>", file=sys.stderr)
        sys.exit(1)

    baseline = json.loads(open(sys.argv[1]).read())
    after = json.loads(open(sys.argv[2]).read())

    all_models = sorted(set(baseline) | set(after))
    report = {"models_compared": len(all_models), "mismatches": {}}

    for name in all_models:
        before_row = baseline.get(name, {"error": "missing from baseline"})
        after_row = after.get(name, {"error": "missing from after"})
        mismatches = diff_model(name, before_row, after_row)
        if mismatches:
            report["mismatches"][name] = mismatches

    report["clean"] = not report["mismatches"]
    print(json.dumps(report, indent=2))
    sys.exit(0 if report["clean"] else 1)


if __name__ == "__main__":
    main()
