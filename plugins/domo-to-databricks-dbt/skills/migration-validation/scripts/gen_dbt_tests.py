#!/usr/bin/env python3
"""dbt test generator — STUB.

Usage:
    python3 gen_dbt_tests.py <dbt_project_dir> <flows_dir>

Adds generated tests to schema.yml per org-dbt-conventions:
  - not_null / unique on inferred primary keys
  - accepted_values where the Domo export reveals a domain
  - relationships on join keys (from MergeJoin tile config)

STATUS: scaffold. Infer keys from Unique/GroupBy tiles; infer join relationships from MergeJoin
tiles in <flows_dir>. Defer to the official `dbt` skill for test YAML structure.
"""
import sys


def main():
    sys.exit(
        "gen_dbt_tests.py is a stub. Infer keys (Unique/GroupBy) + join relationships (MergeJoin) "
        "from the flow graphs and emit schema.yml tests. See references + official dbt skill."
    )


if __name__ == "__main__":
    main()
