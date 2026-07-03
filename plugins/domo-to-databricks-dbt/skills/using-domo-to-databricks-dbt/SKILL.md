---
name: using-domo-to-databricks-dbt
description: >-
  Use when starting work on ANY Domo Magic ETL → dbt-on-Databricks migration, before invoking any
  other skill in this plugin. Establishes the fixed pipeline order and the entry point for each
  new flow or batch. Triggers on "migrate a Domo flow", "Domo to dbt", "Magic ETL migration",
  "convert Domo dataflow", or any request that touches `domo-ingestion`, `tile-translation`,
  `org-dbt-conventions`, `dbt-error-triage`, `databricks-materialization-policy`,
  `migration-validation`, or `dbt-project-optimization`.
---

# Using domo-to-databricks-dbt

<EXTREMELY-IMPORTANT>
This plugin is a FIXED, ORDERED pipeline, not a menu of independent skills:

  domo-ingestion → tile-translation → org-dbt-conventions → dbt-error-triage
  → databricks-materialization-policy → migration-validation → (optional) dbt-project-optimization

Each skill's own SKILL.md carries a `<HARD-GATE>` stating what must be true before it runs and
which skill it hands off to next. Do not jump ahead (e.g. proposing materialization before the
build is green) and do not skip a step because a flow "looks simple." Simple flows still hit
converter bugs — that's the whole point of `dbt-error-triage` existing.

The first six steps are about **migrating correctly**: faithful, traceable, provably equivalent to
the Domo output. `dbt-project-optimization` is a separate, later concern — **making the result good
to maintain** — and only makes sense once correctness is no longer in question. Never collapse
these two goals into one pass: a converter optimized for readability from the start would be harder
to validate against the Domo flow graph, and a "faithful" project left unoptimized forever
accumulates hundreds of raw-passthrough models nobody wants to own.
</EXTREMELY-IMPORTANT>

## Where to start

- **New flow, never ingested**: start at `domo-ingestion`.
- **Already have `flows/<id>.json` + `inventory.csv`**: start at `tile-translation`.
- **Already have generated dbt models, not yet scaffolded**: start at `org-dbt-conventions`.
- **Scaffolded project, `dbt build` not yet green**: start at `dbt-error-triage`.
- **Build is green, materialization not yet reviewed**: start at `databricks-materialization-policy`.
- **Ready for sign-off / audit log**: start at `migration-validation`.
- **Migrated, validated, and being kept long-term — too many models, raw column names, pointless
  staging passthroughs**: start at `dbt-project-optimization` (optional, only after Tier 2).

## The core concept: deterministic converter + adaptive learning loop

`tile-translation`'s converter (`domo_to_dbt/project.py`, `tiles.py`) is deterministic — same input
graph, same output SQL, every time. That determinism is the point: it's what makes 150+ model
migrations tractable. But no converter anticipates every tile config a customer's flow will throw
at it. `dbt-error-triage` is where the system **learns**: every build failure gets clustered, every
fix gets logged to `dbt-error-triage/references/known-patterns.md` with an explicit promoted vs.
ad-hoc decision, and a pattern that recurs across flows gets promoted into the converter itself so
the *next* migration never hits it. Read `known-patterns.md` before diagnosing anything new — check
whether the answer is already there before spending time rediscovering it.

This is why the pipeline is a fixed sequence and not a grab-bag: the learning loop only compounds if
every flow goes through the same triage step in the same place, logging to the same file.

## Official-skill dependencies

This plugin is deliberately thin where the official skills already cover the ground — don't
reinvent what they do:

- `using-dbt-for-analytics-engineering` (marketplace `dbt-agent-marketplace`) — general dbt
  modeling discipline; its `references/debugging-dbt-errors.md` and `references/writing-data-tests.md`
  are what `dbt-error-triage` and `org-dbt-conventions` defer to rather than duplicating. Also
  `troubleshooting-dbt-job-errors` for Databricks Job-specific failures, and
  `migrating-dbt-project-across-platforms` (skill-pack `dbt-migration`) for cross-platform gotchas.
- `databricks-core` + `databricks-dbsql` (marketplace `databricks-agent-skills`) — CLI/profile
  handling, DBSQL best practices (medallion layering, Liquid Clustering/Z-ORDER thresholds).
  `databricks-materialization-policy` overlays Domo-specific signals on top of these, it doesn't
  replace them.

If a Databricks-related prompt fires the `databricks-core` router hook mid-migration, load the
matching product skill it names — don't treat this plugin's skills as a substitute for it.
