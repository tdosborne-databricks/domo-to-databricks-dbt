---
name: databricks-materialization-policy
description: >-
  Thin overlay on the official `databricks` skills. Use when deciding how each migrated dbt model
  should be materialized on Databricks — view vs. table vs. incremental (merge), liquid clustering /
  partitioning thresholds, and Unity Catalog catalog.schema.table naming mapped from Domo domains.
  Reads Domo row counts + update schedules from the ingestion inventory, applies safe defaults to
  the scaffolded project, and proposes advanced storage config for review. Triggers on
  "materialization", "view or table", "incremental model", "liquid clustering", "partition",
  "Unity Catalog naming", "how to materialize", "merge strategy".
---

# Databricks Materialization Policy (overlay on the official databricks skills)

Materialization is **a decision Domo never asked** — every Magic ETL output was just a stored
dataset. dbt on Databricks chooses per model. Blindly materializing everything as tables recreates
Domo's storage sprawl; making everything views can wreck downstream query performance. This skill
encodes the heuristics; it defers to `databricks-dbsql` / `databricks-unity-catalog` for the
underlying Spark SQL and UC mechanics.

<HARD-GATE>
Step 4 of the fixed pipeline (domo-ingestion → tile-translation → org-dbt-conventions →
**databricks-materialization-policy** → dbt-error-triage → migration-validation). Requires
`org-dbt-conventions` to have scaffolded the dbt project (models + `dbt_project.yml`) — **not** a
green `dbt build`. Tile boundaries must exist before materialization decisions mean anything; SQL
correctness is `dbt-error-triage`'s job on the **first** build, which runs **after** this step.

**Apply auto defaults here, then rebuild in triage.** Advanced policy (clustering, incremental,
UC renames) is **proposal-only** until Tier 2 is green — see "Two phases" below.
</HARD-GATE>

## Two phases

### Phase A — auto-apply (before first `dbt build`)

Safe defaults applied to every migration:

| Layer / signal | Materialization |
|---|---|
| Staging (`source()` passthrough) | `view` |
| Intermediate, single downstream consumer | `view` |
| Intermediate, fan-out ≥ 2 | `table` (+ Delta column mapping) |
| Marts (`PublishToVault`) | `table` (+ Delta column mapping) |

```bash
# 1. Apply fan-out view/table split + layer defaults
python3 <skill_dir>/scripts/apply_materialization.py <dbt_project_dir>

# 2. Optional: full proposal JSON for audit / advanced review
python3 <skill_dir>/scripts/materialization_policy.py <inventory.csv> <flows_dir> \
  --catalog <catalog> --schema <schema> > materialization.json
```

**Rebuild required after Phase A** — relation types and `dbt_project.yml` changed:

```bash
dbt build --profiles-dir <dir>
# add --full-refresh when upgrading an existing warehouse from all-intermediate-table
```

Hand off to `dbt-error-triage` (first build + failure loop).

### Phase B — review (after Tier 2 green, optional before cutover)

From `materialization.json` — **do not auto-apply** without human/agent review:

- Liquid clustering / partitioning (row-count threshold)
- Incremental `merge` + unique keys (append-style Domo schedules)
- Unity Catalog catalog/schema renames

Re-run `dbt build` (often `--full-refresh` on clustered tables) after applying Phase B changes.

## Decision rules (reference)

| Signal (from ingestion inventory) | Materialization |
|---|---|
| Light staging, small/cheap, read rarely | **view** |
| Heavy transform, joins, reused downstream | **table** |
| Large + append-style + has an update schedule | **incremental (`merge`)** |
| Terminal mart (`PublishToVault`) | **table** (customer-facing output) |

Consult the official `databricks-dbsql` skill's `references/best-practices.md` for medallion
layering and Liquid Clustering thresholds — this skill only encodes Domo-specific signals on top.

## Staging is not optional decoration

Staging views read `source()` (landed UC / external tables), **not** marts. They are the stable
dbt contract to raw data — keep them even when the body is `select *` today; add casts/renames
there after real sources are wired.

Hand off to `dbt-error-triage` after Phase A apply + documenting `materialization.json`.

## References

- `references/materialization-rules.md` — full decision rules + UC naming map + clustering
  thresholds + rebuild notes.
