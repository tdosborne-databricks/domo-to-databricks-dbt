---
name: dbt-project-optimization
description: >-
  Use once a migrated Domo→dbt project is going to be MAINTAINED long-term (not just cut over and
  left alone) — consolidates trivial pass-through models, normalizes raw Domo column names, and
  reconsiders staging models that add no transformation, then proves the refactor changed nothing
  by diffing against a pre-refactor snapshot of the project's own output. Optional, on request, and
  never run before the project is green and tested. Triggers on "optimize this dbt project",
  "clean up the migrated models", "too many intermediate models", "consolidate models", "is this
  good dbt architecture", "refactor the migration output".
---

# dbt Project Optimization (post-migration, snapshot-verified)

`tile-translation`'s converter is deliberately faithful, not idiomatic: one Domo tile becomes one
dbt model, raw Domo column names (spaces and all) pass through unchanged, and staging models exist
even when they do nothing but `select * from source()`. That's the right default for a migration —
it keeps the dbt project traceable 1:1 against the Domo flow, which is what makes Tier 1/2/3
validation in `migration-validation` meaningful. It is **not** the right end state for a project
someone is going to read, extend, and debug for years.

This skill is where the "idiomatic dbt" pass happens — deliberately **after** correctness is
established, never before.

<HARD-GATE>
Runs only after `migration-validation` has reached at least **Tier 2** (green build + generated
dbt tests passing) for the flow being optimized. Refactoring a project that doesn't build yet, or
whose test coverage is unknown, means you can't tell a real regression from an existing bug. If the
customer hasn't run Tier 3 (data-diff against real Domo data) yet, that's fine — this skill's
validation is self-referential (this project's output vs. this project's own pre-refactor output),
not a substitute for Tier 3, and doesn't require Domo access at all (see
`references/validation-without-domo-access.md` for why the Domo export alone can't be used as a
ground truth here).

Before touching a model, snapshot it (Step 1 below). Every refactor step must end with a diff
against that snapshot passing before moving to the next one. Don't batch multiple structural
changes before validating — if the diff fails, you want to know which change broke it.
</HARD-GATE>

## Why this can't be validated against the Domo export or the original Magic ETL

You will often be doing this work without access to the original Domo Magic ETL environment, and
sometimes without live Domo API credentials either — just the `flows/<flow_id>.json` +
`inventory.csv` that `domo-ingestion` produced. Per `domo-ingestion/references/normalized-graph-schema.md`,
that export records **inputs and tile configs**, not verified output data: a flow's `outputs` entry
is `{dataset_id, name, tile_id}` only, with no column schema, no row counts, no sample rows. Even
`inputs` schema is frequently absent (`completeness_report.json` flags it as a common gap). Mode B's
verified endpoints (`references/domo-api-endpoints.md`) list dataflows and datasets by name/id, not
a data-preview or row-sample endpoint. **There is no recorded ground truth for "what the final
output actually looked like" anywhere in the extract** — `tile-translation` *infers* output schema
by tracing the tile transform chain forward; it doesn't read it from anywhere.

That means you cannot validate a post-migration refactor against the Domo export, because the
export was never a source of truth for output data in the first place — it wasn't Tier 3's job
either, which is why Tier 3 requires the customer to run the diff kit with **their own** Domo
access.

The fix is to not need Domo at all for this step: validate the refactor against **the project's
own already-validated output**, captured before you touch anything. If Tier 2 passed, that
pre-refactor state is your ground truth for "does the refactor preserve behavior." Domo-level
correctness was already established upstream by `migration-validation`; this skill's only job is to
prove it *stayed* established.

## Workflow

1. **Snapshot the baseline.** Before any structural change, capture the current build's output:
   ```bash
   python3 <skill_dir>/scripts/snapshot_outputs.py <dbt_project_dir> <target_models...> > baseline_snapshot.json
   ```
   Row counts, per-column checksums, and null rates per model — the same shape `migration-validation`
   uses for Tier 3, reusing `migration-validation/references/tolerance-rules.md` for comparison
   tolerances, just pointed at itself instead of at Domo.

2. **Find consolidation candidates.** Don't guess by eye — the model count here can be in the
   hundreds:
   ```bash
   python3 <skill_dir>/scripts/find_consolidation_candidates.py <dbt_project_dir> > candidates.json
   ```
   Flags: (a) intermediate models with exactly one downstream `ref()` and a trivial body (pure
   `select * [except(...)]` with no join/aggregation/case logic) — safe to inline into their single
   consumer; (b) staging models that are pure `select * from source()` with no renaming — decide
   per-project whether to keep as an indirection point or drop; (c) columns whose raw Domo name
   (spaces, mixed case) has no `` `backtick-quoted` `` collision risk if normalized to snake_case.

   **Note:** fan-out view/table split on intermediates is handled in
   `databricks-materialization-policy` (`apply_materialization.py`) before the first build — do not
   repeat that work here unless inlining models changes the fan-out graph.

3. **Apply one class of change at a time.** Inline flagged intermediate models, or normalize a
   batch of column names, or resolve one staging model's fate — not all three at once.

4. **Rebuild and diff against the baseline.**
   ```bash
   dbt build --select <affected_models>+
   python3 <skill_dir>/scripts/snapshot_outputs.py <dbt_project_dir> <target_models...> > after_snapshot.json
   python3 <skill_dir>/scripts/diff_snapshots.py baseline_snapshot.json after_snapshot.json
   ```
   Any diff beyond `tolerance-rules.md`'s thresholds means the "trivial" model wasn't actually
   trivial (a hidden cast, a dedup you didn't notice) — revert that specific change, don't push
   through it.

5. **Log the decision.** Append to `references/optimization-log.md`: what was consolidated/renamed,
   the before/after model count, and the diff result that proved it safe. This is a different log
   from `dbt-error-triage/references/known-patterns.md` — that one tracks converter bugs across
   flows; this one tracks per-flow architectural decisions that don't generalize back into the
   converter (the converter should stay faithful; this skill's changes are intentionally *not*
   pushed upstream into `tile-translation`).

6. Re-run `migration-validation` Tier 2 once more on the fully optimized project as the final
   sign-off before calling the flow done.

## What NOT to do here

- Don't touch anything until Tier 2 is green — see the hard gate.
- Don't push consolidation/renaming logic into `tile-translation`'s converter. That converter's
  value is being faithful and traceable; "readable" and "faithful" are different goals for
  different audiences (this skill serves the team that inherits the project, not the migration).
- Don't skip the per-change diff to save time. A batch of 10 "obviously safe" inlines with one
  diff at the end tells you *that* something broke, not *which* change broke it.

## References

- `references/validation-without-domo-access.md` — full reasoning on why the Domo export carries
  no output ground truth, and why self-referential snapshot diffing is the correct substitute.
- `references/optimization-log.md` — append-only log of per-flow architectural decisions.
