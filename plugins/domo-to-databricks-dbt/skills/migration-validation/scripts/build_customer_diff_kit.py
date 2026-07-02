#!/usr/bin/env python3
"""Package the Tier 3 customer-runnable data-diff kit.

Usage:
    python3 build_customer_diff_kit.py <dbt_project_dir> <out_dir>

Produces a standalone kit the customer runs with THEIR Domo access (we have none):
  - diff.py: stdlib-only harness — row counts, column-set, null rates, numeric aggregate
    distributions (Domo output CSV vs. the migrated Databricks table CSV), tolerance-aware
  - references/tolerance-rules.md  (float/timestamp/timezone tolerances)
  - references/mismatch-triage.md  (mismatch pattern -> gotcha, so results map straight to fixes)
  - mapping.json (seeded from the project's marts) + README with run instructions

Deliver EARLY so customer validation runs in parallel, not at the end. Cutover is gated on Tier 3.
The harness emits results as JSON that feeds back to us for triage.
"""
import glob
import json
import os
import shutil
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
_REFS = os.path.normpath(os.path.join(_HERE, "..", "references"))

# ---- the self-contained diff harness written into the kit (stdlib only) ----
_DIFF_PY = r'''#!/usr/bin/env python3
"""Domo-vs-Databricks data diff (stdlib only). Run wherever you have both CSV exports.

    python3 diff.py mapping.json > diff_report.json

mapping.json: [{"name": "...", "domo_csv": "a.csv", "databricks_csv": "b.csv",
                "key": ["id"]  (optional, enables row-level key checks)}]

Export each side to CSV first (Domo: dataset export; Databricks: `SELECT * FROM <table>`
downloaded as CSV, or `dbt` + a COPY INTO). Tolerances follow references/tolerance-rules.md.
Exits non-zero if any pair mismatches beyond tolerance.
"""
import csv, json, math, sys

ABS_EPS = 1e-9
REL_EPS = 1e-6   # money/float relative tolerance


def _read(path):
    with open(path, newline="") as fh:
        r = csv.DictReader(fh)
        rows = list(r)
        return r.fieldnames or [], rows


def _num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _close(a, b):
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    return abs(a - b) <= max(ABS_EPS, REL_EPS * max(abs(a), abs(b)))


def _col_stats(rows, col):
    nums, nulls = [], 0
    for row in rows:
        v = row.get(col)
        if v is None or v == "":
            nulls += 1
            continue
        n = _num(v)
        if n is not None:
            nums.append(n)
    stat = {"null_rate": round(nulls / len(rows), 6) if rows else 0.0, "numeric": bool(nums)}
    if nums:
        stat.update(sum=math.fsum(nums), min=min(nums), max=max(nums),
                    mean=math.fsum(nums) / len(nums))
    return stat


def diff_pair(spec):
    dcols, drows = _read(spec["domo_csv"])
    bcols, brows = _read(spec["databricks_csv"])
    issues = []

    if len(drows) != len(brows):
        issues.append(f"row count: domo={len(drows)} databricks={len(brows)}")

    dset, bset = set(dcols), set(bcols)
    if dset - bset:
        issues.append(f"columns only in domo: {sorted(dset - bset)}")
    if bset - dset:
        issues.append(f"columns only in databricks: {sorted(bset - dset)}")

    for col in sorted(dset & bset):
        ds, bs = _col_stats(drows, col), _col_stats(brows, col)
        if abs(ds["null_rate"] - bs["null_rate"]) > 0.01:
            issues.append(f"{col}: null_rate domo={ds['null_rate']} databricks={bs['null_rate']}")
        if ds["numeric"] and bs["numeric"]:
            for agg in ("sum", "min", "max", "mean"):
                if not _close(ds.get(agg), bs.get(agg)):
                    issues.append(f"{col}.{agg}: domo={ds.get(agg)} databricks={bs.get(agg)}")

    key = spec.get("key")
    if key and set(key) <= (dset & bset):
        dk = {tuple(r[k] for k in key) for r in drows}
        bk = {tuple(r[k] for k in key) for r in brows}
        if dk - bk:
            issues.append(f"{len(dk - bk)} key(s) in domo missing from databricks (e.g. {list(dk - bk)[:3]})")
        if bk - dk:
            issues.append(f"{len(bk - dk)} key(s) in databricks missing from domo (e.g. {list(bk - dk)[:3]})")

    return {"name": spec.get("name"), "match": not issues, "issues": issues,
            "rows": {"domo": len(drows), "databricks": len(brows)}}


def main():
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    specs = json.load(open(sys.argv[1]))
    results = [diff_pair(s) for s in specs]
    report = {"pairs": len(results), "matched": sum(r["match"] for r in results),
              "results": results}
    print(json.dumps(report, indent=2))
    sys.exit(0 if all(r["match"] for r in results) else 1)


if __name__ == "__main__":
    main()
'''

_README = """# Domo → Databricks data-diff kit

Validates that each migrated Databricks table matches its source Domo dataset, within the
tolerances in `references/tolerance-rules.md`. Run this with YOUR Domo access — it needs no
Databricks credentials, only CSV exports from both sides.

## Steps
1. Export each Domo output dataset to CSV.
2. Export each migrated Databricks table to CSV (`SELECT * FROM <catalog.schema.table>` →
   download as CSV, or use `dbt` + `COPY INTO`).
3. Fill in `mapping.json` (seeded below with the project's mart tables) with the CSV paths.
4. Run:
   ```bash
   python3 diff.py mapping.json > diff_report.json
   ```
5. Send `diff_report.json` back. Map each issue to a fix with `references/mismatch-triage.md`.

`diff.py` exits non-zero if any pair mismatches, so it can gate cutover in CI.
"""


def _mart_names(project_dir):
    marts = glob.glob(os.path.join(project_dir, "models", "marts", "*.sql"))
    return sorted(os.path.splitext(os.path.basename(m))[0] for m in marts)


def build(project_dir, out_dir):
    os.makedirs(os.path.join(out_dir, "references"), exist_ok=True)

    with open(os.path.join(out_dir, "diff.py"), "w") as fh:
        fh.write(_DIFF_PY)
    os.chmod(os.path.join(out_dir, "diff.py"), 0o755)

    for ref in ("tolerance-rules.md", "mismatch-triage.md"):
        src = os.path.join(_REFS, ref)
        if os.path.exists(src):
            shutil.copy(src, os.path.join(out_dir, "references", ref))
        else:  # pragma: no cover
            print(f"  ! reference missing, skipped: {ref}", file=sys.stderr)

    mapping = [
        {"name": m, "domo_csv": f"domo/{m}.csv",
         "databricks_csv": f"databricks/{m}.csv", "key": []}
        for m in _mart_names(project_dir)
    ]
    with open(os.path.join(out_dir, "mapping.json"), "w") as fh:
        json.dump(mapping, fh, indent=2)

    with open(os.path.join(out_dir, "README.md"), "w") as fh:
        fh.write(_README)

    print(f"Built diff kit -> {out_dir} ({len(mapping)} mart pair(s) seeded in mapping.json)")


def main():
    if len(sys.argv) != 3:
        sys.exit(__doc__)
    build(sys.argv[1], sys.argv[2])


if __name__ == "__main__":
    main()
