#!/usr/bin/env python3
"""Package the Tier 3 customer-runnable data-diff kit — STUB.

Usage:
    python3 build_customer_diff_kit.py <dbt_project_dir> <out_dir>

Produces a standalone kit the customer runs with THEIR Domo access (we have none):
  - diff harness: row counts, per-column checksums, null rates, aggregate distributions
    (Domo output dataset  vs.  the migrated Databricks table)
  - references/tolerance-rules.md  (float/timestamp/timezone tolerances)
  - references/mismatch-triage.md  (mismatch pattern -> gotcha, so results map straight to fixes)
  - run instructions; emits results as JSON that feeds back to us for triage

Deliver EARLY so customer validation runs in parallel, not at the end. Cutover is gated on Tier 3.

STATUS: scaffold. Bundle a self-contained diff script + the two reference docs + a README into
<out_dir> as a zip-able kit.
"""
import sys


def main():
    sys.exit(
        "build_customer_diff_kit.py is a stub. Bundle the diff harness + tolerance-rules.md + "
        "mismatch-triage.md + run instructions into a standalone kit."
    )


if __name__ == "__main__":
    main()
