#!/usr/bin/env python3
"""Tier 1 (static) validator — STUB.

Usage:
    python3 static_validator.py <dbt_project_dir> <flows_dir> > tier1_report.json

Tier 1 checks (no warehouse needed):
  - transpiled SQL parses in the Spark SQL dialect
  - output schema (column names/types) matches the schema declared in the flow export
  - every input resolves to a source() or ref()  (no dangling refs)
  - no orphaned CTEs
  - the dbt project's lineage graph matches the Domo flow graph

STATUS: scaffold. Parse via `dbt parse` / sqlglot(dialect="spark"); build the dbt lineage from
`target/manifest.json` (run `dbt parse` first, no warehouse required) and diff it against the Domo
flow graph from <flows_dir>.
"""
import sys


def main():
    sys.exit(
        "static_validator.py is a stub. Implement: dbt parse -> manifest.json lineage vs. Domo "
        "flow graph; sqlglot spark-dialect parse; source/ref resolution; orphaned-CTE scan."
    )


if __name__ == "__main__":
    main()
