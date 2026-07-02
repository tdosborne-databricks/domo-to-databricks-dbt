# Materialization decision rules (Databricks)

Overlay on `databricks-dbsql` / `databricks-unity-catalog`. Inputs come from the ingestion
inventory (row counts, update schedules) + tile config (filter/join columns).

## Per-model choice

| Condition | Materialization | Rationale |
|---|---|---|
| Staging, small, read rarely, cheap to recompute | `view` | No storage, always fresh |
| Heavy transform / joins / reused by ≥2 models | `table` | Compute once, read many |
| Large + append-only + has an update schedule | `incremental` (`merge` on key) | Avoid full rebuild |
| Terminal mart (`PublishToVault`) | `table` | Customer-facing output |

Do NOT default everything to `table` (recreates Domo storage sprawl) or to `view` (kills downstream
query performance).

## Clustering / partitioning

- Below ~1M rows: no clustering.
- Above the threshold: **liquid clustering** on the columns most frequently filtered/joined in the
  source tiles (recover from tile config predicates + join keys). Prefer liquid clustering over
  static partitioning unless a clear low-cardinality date partition dominates.

## Unity Catalog naming

`catalog.schema.table`:
- `catalog` — the migration target catalog (one per engagement, e.g. `domo_migration`).
- `schema` — mapped from the Domo **domain** / business area the flow belonged to.
- `table` — the mart/model name from `org-dbt-conventions`.

Emit the mapping to `materialization.json` for the scaffolder to apply via `dbt_project.yml`
(`+materialized`, `+cluster_by`, `+schema`) or per-model config blocks.

## TODO (fill during calibration)

- Confirm the row-count threshold against the customer's actual dataset sizes (from inventory).
- Confirm incremental `merge` keys are stable (Domo append flows sometimes lack a natural key).
