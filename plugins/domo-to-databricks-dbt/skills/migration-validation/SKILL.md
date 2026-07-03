---
name: migration-validation
description: >-
  Use to validate a migrated Domo→dbt project in TIERS and write the per-flow audit log. Tier 1
  (static, always): SQL parses, output schema matches the flow export, every input resolves to a
  source/ref, no orphaned CTEs, dbt lineage matches the Domo flow graph. Tier 2 (build, when a
  Databricks workspace is available): dbt build + generated dbt tests pass. Tier 3 (customer-run):
  a packaged data-diff kit the customer runs against their own Domo data. Triggers on "validate
  migration", "dbt build check", "static validation", "generate dbt tests", "data diff", "migration
  log", "validation tier", "customer validation kit", "parity check", "did the migration work".
---

# Migration Validation (tiered; data-diff optional)

We have **no direct access to Domo data**, so validation is tiered. Each flow's migration log
records the **highest tier achieved** — that log is the audit trail deliverable.

<HARD-GATE>
Step 6 of the fixed pipeline (domo-ingestion → tile-translation → org-dbt-conventions →
dbt-error-triage → databricks-materialization-policy → **migration-validation**). If Tier 2 fails
here, that means `dbt-error-triage` didn't actually reach green; go back to it rather than
re-diagnosing from scratch.

This is the last **required** step for a migration that's just being cut over. If the project will
be maintained long-term (not just left as-is post-cutover), hand off to `dbt-project-optimization`
once Tier 2 is green — that step is optional and on request, never mandatory, but it's what turns
the deliberately-faithful 1:1 migration output into something a team can actually own afterward.
</HARD-GATE>

## The three tiers

- **Tier 1 — Static (always).** Transpiled SQL parses in the Spark SQL dialect; output schema
  (column names/types) matches the schema declared in the flow export; every input resolves to a
  `source()` or `ref()`; no orphaned CTEs; the dbt project's lineage graph matches the Domo flow
  graph. Runs with no warehouse.
- **Tier 2 — Build (always, if a Databricks workspace is available).** `dbt build` succeeds and the
  generated dbt tests pass (not-null/unique on inferred keys, accepted-values where the export
  reveals domains, relationship tests on joins). Getting to green is `dbt-error-triage`'s job
  (upstream of this skill, capped at 5 fix iterations before escalating) — by the time a flow
  reaches Tier 2 here, the build should already be clean; this step just re-confirms it and runs
  the tests.
- **Tier 3 — Data diff (customer-run).** A standalone kit (scripts + instructions) the customer
  runs with **their** Domo access — row counts, per-column checksums, null rates, aggregate
  distributions comparing Domo outputs to the Databricks tables. Results come back as JSON; we
  triage via the mismatch → gotcha map. **Cutover sign-off is gated on Tier 3.**

## Workflow

```bash
# Tier 1
python3 <skill_dir>/scripts/static_validator.py <dbt_project_dir> <flows_dir> > tier1_report.json
# generate dbt tests into schema.yml
python3 <skill_dir>/scripts/gen_dbt_tests.py <dbt_project_dir> <flows_dir>
# Tier 2 (defer to official databricks-jobs / dbt skills to run the build)
#   dbt build  (or a Databricks Workflows dbt task — see references/authentication.md)
# write the per-flow audit log (records tier achieved)
python3 <skill_dir>/scripts/migration_log.py <flow_id> <tier_reports...> >> migration_log.jsonl
# package the customer kit
python3 <skill_dir>/scripts/build_customer_diff_kit.py <dbt_project_dir> <out_dir>
```

## Deliver the Tier-3 kit EARLY

Because semantic drift is undetectable on our side, deliver the diff kit early so the customer's
validation runs **in parallel**, not at the end. The kit ships the mismatch → gotcha triage map so
their results map straight to fixes in `tile-translation/references/semantic-gotchas.md`.

## References

- `references/tolerance-rules.md` — float precision, timestamp/timezone normalization tolerances.
- `references/mismatch-triage.md` — mismatch-pattern → gotcha map (shipped inside the customer kit).
- `references/authentication.md` — running `dbt build` on Databricks (Workflows dbt task / OAuth,
  not a PAT env var).
