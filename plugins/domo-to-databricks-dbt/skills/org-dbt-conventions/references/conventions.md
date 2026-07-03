# Org dbt conventions (the deltas over the official dbt skill)

Defer to the official `dbt` skill for anything not listed here.

## Layering

| Layer | Contents | Default materialization | Naming |
|---|---|---|---|
| `staging` | 1:1 with a source; light cleanup/renames/casts | view | `stg_<domain>__<entity>` |
| `intermediate` | business logic, joins, the bulk of former tile chains | ephemeral/table (see policy) | `int_<domain>__<verb>` |
| `marts` | the Domo `PublishToVault` outputs | table | `<entity>` or `fct_/dim_` |

## When to split a flow vs. keep CTEs

- **Keep as CTEs** within one model: linear tile chains with no reuse.
- **Split into a new model** only at: (a) a reuse boundary (output consumed by >1 downstream),
  (b) a materialization point (needs to be a table for performance), or (c) a former flow-to-flow
  handoff (was a Domo output dataset another flow consumed).
- **Merge** flows that were only split to dodge Domo engine/size limits.

## Deduplicating repeated cleanup

If the same input+cleanup pattern appears across ≥2 flows, extract it into ONE shared staging model
and `ref()` it. This is the primary defense against dbt project sprawl. Detection: compare staging
CTEs by (source dataset, projection, filter) fingerprint.

## Required tests per layer

- **staging**: `not_null` + `unique` on the inferred primary key; `not_null` on required columns.
- **intermediate/marts**: `relationships` on join keys; `accepted_values` where the Domo export
  reveals a domain; `unique` on the grain.

## schema.yml docs

Column descriptions sourced from Domo dataset metadata where the export includes it; otherwise leave
a `TODO` placeholder rather than inventing docs.

## Traceability

Every generated model header comment records: source `flow_id`, source tile ids collapsed into it,
and (if split/merged) why. Every job records the flows it covers.

## Unity Catalog schemas (three-way split)

Keep raw landed tables separate from dbt-built relations so the catalog UI stays scannable:

| Role | UC schema | Wired by |
|------|-----------|----------|
| Raw sources | `{project}_dbt_src` | `overrides.json` → `sources.yml` |
| Staging + intermediate | `{project}_dbt` | `profiles.yml` `schema` |
| Marts (Domo outputs) | `{project}_dbt_marts` | `dbt_project.yml` `marts: +schema: marts` |

Land ingested Domo/SQL Server tables in `*_src`. Point `overrides.json` at
`main.<project>_dbt_src.<table>`. dbt builds `stg_*` / `int_*` views and tables under
`<project>_dbt` and terminal marts under `<project>_dbt_marts`.
