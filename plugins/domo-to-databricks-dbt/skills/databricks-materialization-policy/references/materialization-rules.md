# Materialization decision rules (Databricks)

Overlay on `databricks-dbsql` / `databricks-unity-catalog`. Inputs come from the ingestion
inventory (row counts, update schedules) + tile config (filter/join columns).

## Pipeline placement

Materialization policy runs **after** `org-dbt-conventions` and **before** `dbt-error-triage`:

1. Models exist (tile-translation + scaffold).
2. **Phase A** (`apply_materialization.py`) sets view/table defaults.
3. **First `dbt build`** runs in triage on the applied config.

Policy does **not** replace triage — it prevents materializing every intermediate as a Delta table
before the first build.

## Per-model choice

| Condition | Materialization | Rationale |
|---|---|---|
| Staging, 1:1 with `source()` | `view` | Cheap indirection; isolate raw table renames |
| Intermediate, fan-out < 2 | `view` | Single consumer — no reuse benefit from storage |
| Intermediate, fan-out ≥ 2 | `table` | Compute once, read many (+ column mapping for Domo names) |
| Large + append-only + has an update schedule | `incremental` (`merge` on key) | Avoid full rebuild — **Phase B review** |
| Terminal mart (`PublishToVault`) | `table` | Customer-facing output |

Do NOT default everything to `table` (recreates Domo storage sprawl) or to `view` (kills downstream
query performance on hot reuse points).

## Auto-apply vs review

| Auto-apply (Phase A) | Review only (Phase B, post–Tier 2) |
|---|---|
| Layer defaults in `dbt_project.yml` | Liquid clustering keys |
| Fan-out view/table split on intermediates | Incremental strategy + merge keys |
| Delta column mapping on persisted tables | UC catalog/schema renames |

## Rebuild after apply

**Phase A always requires a build** — you changed configs, not just wrote a JSON file.

```bash
python3 apply_materialization.py <dbt_project_dir>
dbt build --profiles-dir <dir>
```

Use `dbt build --full-refresh` when an existing warehouse already has intermediate **tables** and
you are demoting most of them to **views** (relation type change).

Phase B changes (clustering, incremental) also require rebuild; clustering often needs full refresh.

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

Emit the mapping to `materialization.json` for review; do not silently rename production relations.

## Per-engagement calibration

- Confirm the row-count threshold against the customer's actual dataset sizes (from `inventory.csv`).
- Confirm incremental `merge` keys are stable (Domo append flows sometimes lack a natural key).
