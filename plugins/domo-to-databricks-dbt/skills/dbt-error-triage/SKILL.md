---
name: dbt-error-triage
description: >-
  Use after org-dbt-conventions has scaffolded a migrated Domo→dbt project, to drive `dbt build`
  to green. Runs the build, clusters failures by normalized error signature, and for each cluster
  decides whether it's a converter bug (promote a fix into tile-translation's project.py/tiles.py
  and regenerate) or a flow-specific data quirk (patch the generated model file locally, log it,
  move on). Defers to the official `using-dbt-for-analytics-engineering` skill (whose
  `references/debugging-dbt-errors.md` covers the diagnosis mechanics) and `troubleshooting-dbt-job-errors`
  for Databricks Job runs. Triggers on "dbt build failed", "unresolved column",
  "materialization error", "triage dbt errors", "converter bug vs ad-hoc patch". Run AFTER
  org-dbt-conventions, BEFORE databricks-materialization-policy.
---

# dbt Error Triage (deterministic build loop; the converter learns from what it patches)

`tile-translation`'s converter is deterministic, but no converter is bug-free and no dummy dataset
exercises every code path. This skill is the loop that closes that gap: run the build, cluster the
failures, and route each cluster to the right fix — architectural (converter) or local (this file).
The core discipline is **never patch the same root cause twice** — if a pattern recurs, it belongs
in the converter, not in another hand-edited `.sql` file.

<HARD-GATE>
Step 4 of the fixed pipeline (domo-ingestion → tile-translation → org-dbt-conventions →
**dbt-error-triage** → databricks-materialization-policy → migration-validation). Do not hand off
to `databricks-materialization-policy` until `dbt build` exits clean (0 errors) for the full
project. Do not silently patch generated `.sql` files without first checking whether the failure
clusters with a prior pattern in `references/known-patterns.md` — a second occurrence of the same
signature is a converter bug by definition, not a coincidence.
</HARD-GATE>

## Workflow

1. **Build.** `dbt build --profiles-dir <dir>` against the target warehouse.
2. **Cluster.** On any failure, run:
   ```bash
   python3 <skill_dir>/scripts/cluster_errors.py <target_dir>/run_results.json <skill_dir>/references/known-patterns.md
   ```
   This groups failed models by a normalized error signature (identifiers/literals/numbers
   stripped) and flags clusters whose signature substring-matches an existing entry in
   `known-patterns.md`.
3. **Diagnose each cluster.** Use the official `using-dbt-for-analytics-engineering` skill (its
   `references/debugging-dbt-errors.md` covers root-cause mechanics — compiled SQL inspection,
   `information_schema` checks, stale-view rebuilds via `dbt build --select <model>+` to force a
   view to re-expand `select *` after an upstream `ALTER TABLE`), and `troubleshooting-dbt-job-errors`
   if this is a Databricks Job run, not local. Don't reinvent that diagnostic process here.
4. **Decide: promote or patch.** For each cluster:
   - **Promote to the converter** (edit `tile-translation`'s `domo_to_dbt/project.py` or `tiles.py`,
     then regenerate the whole project) when: the same normalized signature hits ≥2 models in this
     run, OR it already has an entry in `known-patterns.md` from a prior flow, OR the compiled SQL
     shows the converter emitted structurally wrong output (not a data/schema-drift issue) —
     e.g. a comment swallowing a trailing clause, a materialization default that doesn't scale,
     a dialect rewrite that's simply wrong.
   - **Patch locally** (edit the generated `.sql` file directly, scoped to that model) when the
     cause is flow-specific or data-specific and no reasonable converter rule generalizes it — a
     raw-SQL tile with a hardcoded quirky identifier, a naming collision unique to this flow's
     schema. Regenerating the project will wipe this patch; that's expected and cheap to reapply,
     which is exactly why it *shouldn't* become a converter change.
   - When genuinely unsure, default to logging it as ad-hoc for now — promote on the **second**
     occurrence, not the first guess. Converter changes have blast radius across every future flow.
5. **Log every decision** to `references/known-patterns.md` (promoted or not, with the reasoning) —
   this file is the persistent memory this skill exists to build. Update it even for one-off patches
   so a *future* recurrence gets recognized as a pattern.
6. **Loop.** Re-run `dbt build`. Cap at 5 iterations of promote/patch-and-rebuild; if still red,
   escalate to the human queue rather than continuing to guess.
7. Hand off to `databricks-materialization-policy`.

## References

- `references/known-patterns.md` — the persistent log of every converter bug and ad-hoc patch
  found across all migrations. Read it before diagnosing anything; it may already have the answer.
