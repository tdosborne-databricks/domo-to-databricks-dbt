# Domo Magic ETL JSON schema

How Domo serializes a Magic ETL dataflow. The ingester parses this into the normalized graph
(`normalized-graph-schema.md`). Detailed per-tile config encoding lives in
`tile-translation/references/tile-mapping.md`.

## Top level (dataflow definition)

- `id`, `name` — flow identity.
- `actions` (or `tiles`) — the array of tiles. Each has an `id`, a `type` (the Domo action, e.g.
  `LoadFromVault`, `MergeJoin`, `ExpressionEvaluator`, `PublishToVault`), a `name`, and a
  type-specific config object.
- Edges: encoded per-tile via `dependsOn` (list of upstream tile ids), `inputs` (list), or
  `input` (single id). The ingester normalizes all three into `depends_on`.
- `LoadFromVault` tiles reference a `dataSourceId` → resolved to a dataset name via
  `dataset_mapping.json`.
- `PublishToVault` tiles are the flow's output datasets (become marts).

## Companion files in an export

- `dataflows.json` — one or more flow definitions (the tile DAGs).
- `dataset_mapping.json` — `dataSourceId → dataset name`.
- (optional) dataset schema / row-count / schedule files — presence varies by customer export;
  the completeness check reports what's missing.

## 14 known tile types

`LoadFromVault, Filter, GroupBy, ExpressionEvaluator, MergeJoin, SelectValues, Metadata, Unique,
UnionAll, WindowAction, Normalizer, DateCalculator, SQL, PublishToVault`. Unrecognized types →
passthrough + flag.
