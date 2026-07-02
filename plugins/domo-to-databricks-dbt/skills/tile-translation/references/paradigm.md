# Why Domo Magic ETL → dbt isn't a 1:1 translation

The conceptual foundation for the whole migration. The tools differ not just in syntax but in
**paradigm**; an optimal migration exploits those differences rather than mechanically porting tiles.

## Paradigm differences

- **Visual dataflow vs. SQL-as-code.** Magic ETL is a drag-and-drop DAG of tiles executed by Domo's
  proprietary engine; logic lives in tile configs (GUI state serialized to JSON). dbt is SQL under
  version control, compiled and executed by Databricks/Spark. → The migration is a **decompilation**
  problem: recover intent from serialized GUI state, re-express as idiomatic SQL.
- **ETL vs. ELT.** Magic ETL pulls data through Domo's engine and writes result datasets back to
  Domo. dbt transforms in place — no data movement, compute scales with Databricks. → Flows split
  apart to dodge Domo engine/size limits can be recombined; Domo's cheap "output dataset per tile
  chain" habit shouldn't become a materialized table per chain.
- **Implicit vs. explicit dependencies.** Domo wires flows through named datasets (flow outputs X,
  another consumes X — invisible unless you inspect both). dbt makes every dependency explicit via
  `ref()`/`source()`, producing a compiler-verified DAG. → **Reconstruct the implicit Domo
  dependency graph before generating any models**, or `ref()` targets won't exist.
- **No native testing/docs vs. first-class.** Magic ETL has no assertions; data-quality issues
  surface downstream in cards. → An optimal migration **adds** the testing layer Domo never had.
  Net-new value, not overhead.
- **Per-flow scheduling vs. DAG-aware orchestration.** Each Domo flow has its own trigger; chained
  flows run in loosely coordinated, race-prone cascades. In dbt on Databricks one job runs
  `dbt build` over a selector and the DAG guarantees ordering. → **Collapse flows into fewer
  DAG-aware jobs** instead of migrating schedules 1:1.

## The nuance that separates optimal from literal

1. **Granularity mapping is the central judgment call.** Tile ≠ model; flow ≠ always one model.
   Tile chains → CTEs; a flow → usually one model; split only at reuse boundaries, materialization
   points, or former flow-to-flow handoffs; merge flows split only due to Domo limits.
2. **Duplication was rational in Domo, wrong in dbt.** Detect repeated input+cleanup patterns and
   consolidate into shared staging models; don't port the duplication.
3. **Semantic drift is the silent failure mode.** Domo's engine and Spark SQL disagree in small
   ways (null handling in joins/filters/grouping, implicit type coercion, Replace Text regex
   dialect, date/timezone defaults). A literal translation that "looks right" can return different
   rows — hence `semantic-gotchas.md` and tiered validation.
4. **Materialization is a new decision Domo never asked.** view / table / incremental + clustering,
   per model. See `databricks-materialization-policy`.
5. **Layering imposes an architecture Domo lacked.** staging → intermediate → marts, imposed during
   migration. See `org-dbt-conventions`.
6. **Preserve traceability through the transformation.** Every model, CTE, and job traces back to
   its source flow and tiles (naming, comments, migration logs) so the customer can verify and debug.
