# Validating optimization without Domo access

## What the Domo extract actually contains

`domo-ingestion` normalizes whatever export the customer provided (Mode A) or pulls live (Mode B)
into `flows/<flow_id>.json` + `inventory.csv`. Checked against
`domo-ingestion/references/normalized-graph-schema.md` and `references/domo-api-endpoints.md`:

- Each flow's `outputs` array is `{dataset_id, name, tile_id}` — **no column schema, no row count,
  no sample data**.
- `inputs` may carry a declared schema, but it's optional and frequently missing —
  `completeness_report.json` exists specifically to flag this gap up front.
- Mode B's verified endpoints (`GET /api/dataprocessing/v2/dataflows/{id}?validationType=PREVIEW`,
  `GET /api/data/v3/datasources`) return the tile DAG and a dataset list by name/id. Neither
  returns row-level data or a data-preview payload. DomoStats/governance datasets can supply row
  counts and schedules when wired in, but that's still not the same as the shape or values of a
  specific output dataset.

**Conclusion: there is no recorded ground truth for "what a Domo flow's output actually contained"
anywhere in the ingestion artifacts.** `tile-translation` doesn't read an expected output schema
from the export — it *infers* the output schema by tracing the tile transform chain forward. If
that inference were wrong, nothing in the extract would catch it; that's exactly why
`migration-validation`'s Tier 3 requires the **customer** to run the diff kit against their own
live Domo access. We were never validating against the extract itself, even during the original
migration.

## Why that's fine for this skill anyway

This skill doesn't need Domo-level ground truth, because it isn't re-validating the migration —
`migration-validation` already did that (Tier 1 static checks, Tier 2 build + tests, optionally
Tier 3 customer data-diff). This skill's only claim is narrower: **"this refactor didn't change
behavior."** That claim only requires comparing the project's output to *itself*, before and after
the change:

1. Snapshot output tables (row counts, per-column checksums, null rates) before refactoring.
2. Apply one class of structural change.
3. Rebuild and snapshot again.
4. Diff the two snapshots using the same tolerance thresholds `migration-validation` already
   defined in `references/tolerance-rules.md` (float precision, timestamp/timezone normalization,
   null-handling) — reused here, not reinvented, because "did this value change" tolerances don't
   depend on which system produced the baseline.

If Tier 2 was genuinely green before this skill started, the pre-refactor snapshot is a trustworthy
baseline regardless of whether it's also been proven correct against Domo yet. A refactor that
preserves that snapshot preserves whatever correctness Tier 2 (and, if run, Tier 3) already
established — it doesn't need to re-derive it from Domo.

## When this reasoning breaks down

- If Tier 2 hasn't actually passed (silently broken tests, or tests not generated at all), the
  snapshot baseline is baselining a bug, not correctness. That's what the `<HARD-GATE>` in
  `SKILL.md` protects against.
- If a "trivial" model turns out to have a side effect the heuristic missed (e.g. a `select *`
  that's silently deduping via an upstream `DISTINCT`), the diff step is the catch — this is why
  changes are applied one class at a time and diffed individually, not batched.
- If the customer later runs Tier 3 for the first time *after* this skill's refactor, and it turns
  up a mismatch, triage it the same way `migration-validation` normally would — the refactor itself
  is not a suspect unless the mismatch is in a column this skill touched (renamed, or its source
  model inlined).
