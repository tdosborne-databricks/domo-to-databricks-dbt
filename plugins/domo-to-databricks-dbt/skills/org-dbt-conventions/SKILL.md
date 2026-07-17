---
name: org-dbt-conventions
description: >-
  Thin overlay on the official `dbt` skill. Use when structuring the generated dbt project from a
  Domo migration — model naming, staging/intermediate/marts layering, when to split a flow into
  multiple models vs. keep CTEs, deduplicating repeated Domo cleanup logic into shared staging
  models, required tests per layer, and schema.yml docs sourced from Domo metadata. Also scaffolds
  sources.yml, model stubs, schema.yml, and rewires flow-to-flow dependencies to ref(). Triggers on
  "dbt project structure", "staging intermediate marts", "dbt naming conventions", "shared staging
  model", "dedupe Domo cleanup", "sources.yml", "schema.yml", "ref rewiring", "layer the models".
---

# Org dbt Conventions (overlay on the official dbt skill)

**After transpiling, follow the official `dbt` skill for model structure and testing; apply the
conventions here where they differ.** This skill is only the deltas — our layering, naming, split
criteria, and Domo-specific scaffolding. It explicitly defers to `dbt` (and `dbt-migration`) for
general analytics-engineering best practice.

<HARD-GATE>
Step 4 of the fixed pipeline (domo-ingestion → domo-source-resolution → tile-translation →
**org-dbt-conventions** → databricks-materialization-policy → dbt-error-triage →
migration-validation). Requires `tile-translation`'s generated models (and its
`conversion_report.json` needs-review list resolved or explicitly deferred) before scaffolding. Do not run `dbt build` here — that belongs to
`dbt-error-triage`, after `databricks-materialization-policy` applies storage defaults.
</HARD-GATE>

## What Domo lacked that we impose during migration

Magic ETL projects grow organically with no enforced structure. Imposing dbt convention **during**
migration is far cheaper than refactoring afterward — it's what makes the result maintainable
rather than a tile graph transcribed into SQL.

1. **Layering.** `staging` (1:1 with sources, light cleanup, usually views) → `intermediate`
   (business logic, joins) → `marts` (the Domo `PublishToVault` outputs, materialized tables).
2. **Granularity / splitting.** Keep tile chains as CTEs; split a flow into multiple models only at
   reuse boundaries, materialization points, or former flow-to-flow handoffs. (Mirror of the rule in
   `tile-translation`.)
3. **Deduplication.** Duplication was rational in Domo (copy-paste flows), wrong in dbt. `ref()`
   makes shared staging nearly free. **Detect repeated input+cleanup patterns across flows and
   consolidate them into shared staging models** instead of porting the duplication. This is the
   mitigation for "dbt project sprawl."
4. **Tests per layer.** not-null/unique on inferred keys; relationship tests on joins; accepted-
   values where the export reveals a domain. Net-new value Domo never had.
5. **Docs.** `schema.yml` column docs sourced from Domo metadata where available.
6. **Traceability.** Every model/CTE carries a comment back to its source flow + tiles.
7. Hand off to `databricks-materialization-policy` (apply view/table defaults before first build).

## Scaffolding scripts

```bash
python3 <skill_dir>/scripts/scaffold.py <flows_dir> <dbt_project_dir>
# generates: sources.yml (from Domo connector datasets), model stubs, schema.yml (column docs),
# and ref-rewiring (flow-to-flow dependencies → ref()).
```

Wire sources to **real Unity Catalog tables** via an `overrides.json` (`catalog.schema.table`) —
see `references/real-data-overrides.md`.

**Model files, `dbt_project.yml`, `sources.yml`, and `schema.yml` always regenerate** — that's
deterministic migration output, not a convention choice, and it's supposed to stay in sync with the
Domo flow graph on every run. `packages.yml`, `profiles.yml`, `.sqlfluff`, and `README.md` are
different: they encode team/org conventions, not migration output, so `scaffold.py` only writes one
if it doesn't already exist at `<dbt_project_dir>` — if the customer already has their own package
pins, lint config, or environment list, that's their preference and this skill doesn't overwrite
it. Pass `--overwrite-org-files` only when the customer has explicitly said to use ours instead.

## References

- `references/conventions.md` — naming, layering, split/dedupe criteria, required tests per layer.
- `references/real-data-overrides.md` — mapping Domo sources → real UC tables.
- `references/legacy-workflow.md` — the v1 single-skill end-to-end loop (historical; being split
  across the 8-skill pipeline).
