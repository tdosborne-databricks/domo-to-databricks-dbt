---
name: databricks-materialization-policy
description: >-
  Thin overlay on the official `databricks` skills. Use when deciding how each migrated dbt model
  should be materialized on Databricks — view vs. table vs. incremental (merge), liquid clustering /
  partitioning thresholds, and Unity Catalog catalog.schema.table naming mapped from Domo domains.
  Reads Domo row counts + update schedules from the ingestion inventory and proposes per-model
  materialization config. Triggers on "materialization", "view or table", "incremental model",
  "liquid clustering", "partition", "Unity Catalog naming", "how to materialize", "merge strategy".
---

# Databricks Materialization Policy (overlay on the official databricks skills)

Materialization is **a decision Domo never asked** — every Magic ETL output was just a stored
dataset. dbt on Databricks chooses per model. Blindly materializing everything as tables recreates
Domo's storage sprawl; making everything views can wreck downstream query performance. This skill
encodes the heuristics; it defers to `databricks-dbsql` / `databricks-unity-catalog` for the
underlying Spark SQL and UC mechanics.

## Decision rules

| Signal (from ingestion inventory) | Materialization |
|---|---|
| Light staging, small/cheap, read rarely | **view** |
| Heavy transform, joins, reused downstream | **table** |
| Large + append-style + has an update schedule | **incremental (`merge`)** |
| Terminal marts (`PublishToVault`) | **table** (the customer-facing output) |

- **Liquid clustering / partitioning** above a row-count threshold; cluster on the columns Domo
  filtered/joined on most (recoverable from tile config).
- **Unity Catalog naming**: `catalog.schema.table` mapped from Domo domains — see
  `references/materialization-rules.md`.

## Workflow

```bash
python3 <skill_dir>/scripts/materialization_policy.py <inventory.csv> <flows_dir> > materialization.json
# proposes {model: {materialized, cluster_by, unity_catalog_name}} for the org-dbt-conventions
# scaffolder / dbt_project.yml config to apply.
```

The proposal is a **starting point** — surface it for review; the agent doesn't silently commit
storage decisions.

## References

- `references/materialization-rules.md` — full decision rules + UC naming map + clustering
  thresholds.
