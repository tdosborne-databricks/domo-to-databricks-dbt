---
name: using-domo-to-databricks-dbt
description: >-
  Use when starting work on ANY Domo Magic ETL → dbt-on-Databricks migration, before invoking any
  other skill in this plugin. Establishes the fixed pipeline order and the entry point for each
  new flow or batch. Triggers on "migrate a Domo flow", "Domo to dbt", "Magic ETL migration",
  "convert Domo dataflow", or any request that touches `domo-ingestion`, `tile-translation`,
  `org-dbt-conventions`, `databricks-materialization-policy`, `dbt-error-triage`,
  `domo-source-resolution`, `migration-validation`, or `dbt-project-optimization`.
---

# Using domo-to-databricks-dbt

<EXTREMELY-IMPORTANT>
This plugin is a FIXED, ORDERED pipeline, not a menu of independent skills:

  domo-ingestion → domo-source-resolution → tile-translation → org-dbt-conventions
  → databricks-materialization-policy → dbt-error-triage → migration-validation
  → (optional) dbt-project-optimization

Each skill's own SKILL.md carries a `<HARD-GATE>` stating what must be true before it runs and
which skill it hands off to next. Do not jump ahead (e.g. running triage before materialization
defaults are applied, or proposing clustering before the build is green). Simple flows still hit
converter bugs — that's the whole point of `dbt-error-triage` existing.

The first six steps are about **migrating correctly**: faithful, traceable, provably equivalent to
the Domo output. `dbt-project-optimization` is a separate, later concern — **making the result good
to maintain** — and only makes sense once correctness is no longer in question. Never collapse
these two goals into one pass: a converter optimized for readability from the start would be harder
to validate against the Domo flow graph, and a "faithful" project left unoptimized forever
accumulates hundreds of raw-passthrough models nobody wants to own.
</EXTREMELY-IMPORTANT>

## Before you start: ask once, then stop asking

At the start of a fresh session, before touching `domo-ingestion`, ask the user one question if it
isn't already obvious from context:

**Is `dbt build` going to run locally, or on Databricks (warehouse / Databricks Job)?**

This isn't a style preference — it changes concrete behavior downstream and is expensive to
discover mid-pipeline instead of up front:
- `dbt-error-triage` needs to know what `dbt build` even means here (local DuckDB/Spark vs. a
  Databricks SQL warehouse or a `dbt_task` Databricks Job) before it can diagnose a failure.
- `databricks-materialization-policy`'s Unity Catalog naming and Liquid Clustering guidance only
  apply on Databricks — running locally, skip straight to the view/table/incremental decision and
  drop the UC-naming step.
- `migration-validation`'s Tier 2 and its `references/authentication.md` (Databricks Workflows dbt
  task / OAuth) assume a Databricks target; running locally, Tier 2 still applies but that
  reference doesn't.

Do not ask this per-flow or per-skill — ask it once per session/engagement and carry the answer
through every downstream skill's hand-off. If the user already stated it (e.g. "run this against
e2-demo-field-eng"), don't ask again, just proceed.

**Do not** separately ask whether to dispatch each pipeline step as a subagent or run it inline —
that's not a per-session judgment call, it's a fixed default (see "Dispatch model" below).

## Set up a workspace before domo-ingestion runs — but ask first, don't assume

Before creating anything, ask: **does the customer already have a dbt project/repo they want this
migration added into, with their own conventions — or should we scaffold a fresh one?** Don't
default to scaffolding from scratch just because that's this plugin's default path; a customer with
an existing repo, package pins, lint config, and environment list has already made those decisions,
and building a parallel structure next to (or worse, on top of) theirs creates exactly the kind of
mess this section exists to prevent.

**No existing project — scaffold fresh (this plugin's default):**

Every migration still needs its own clean directory (or workspace path) — never reuse a scratch
directory left over from a previous flow or session, and never scaffold into a directory that
already has unrelated files in it.

- **Local target**:
  ```bash
  python3 <skill_dir>/scripts/init_migration_workspace.py <root_dir> <flow_name> --target local
  ```
  Refuses to run if `<root_dir>` already exists and is non-empty (pass `--force` only when
  deliberately resuming a known-good workspace from an earlier session on this same flow). Creates
  `<root_dir>/ingestion/` (point `domo-ingestion` at this) and `<root_dir>/dbt/` (point
  `org-dbt-conventions` at this), git-inits the root, and writes `MIGRATION.md` recording the flow
  name and target so a later session doesn't have to re-ask.

- **Databricks target**: there's no local directory to create — isolation instead comes from a
  unique DAB `bundle.name` per flow plus a schema scoped to the engagement (not a shared default
  like `main.domo_migration` reused across customers). See
  `references/isolated-workspace-setup.md` for the concrete bundle-naming and Unity-Catalog-schema
  conventions and why `mode: development` isolation isn't enough on its own for the data side.

**Existing project — integrate, don't scaffold:**

Skip `init_migration_workspace.py` entirely; it exists to build a fresh structure, which isn't what
this branch needs. Point `org-dbt-conventions`'s `<dbt_project_dir>` straight at the customer's
existing dbt project root. `scaffold.py` already only writes `packages.yml`/`profiles.yml`/
`.sqlfluff`/`README.md` if one doesn't already exist there (never `--overwrite-org-files` here
unless the customer explicitly asks for it) — model files, `dbt_project.yml`'s layer config,
`sources.yml`, and `schema.yml` still regenerate normally, since those are migration output, not a
convention. `domo-ingestion`'s raw output (`flows/<id>.json`, `inventory.csv`) doesn't need to live
inside the customer's repo at all — a throwaway local directory is fine, since nothing downstream
reads it after `org-dbt-conventions` runs.

## Dispatch model: subagents by default

Every step in this pipeline (`domo-ingestion` through `dbt-project-optimization`) runs as its own
subagent dispatch by default, not inline in the main conversation. This is a fixed policy, not a
question to ask the user each time: a single flow migration can touch 150+ models across 7-9
sequential stages, and running each stage inline burns the main context on intermediate JSON/SQL
that the next stage doesn't need — only each stage's hand-off summary does. Dispatch keeps the main
thread free to track pipeline state (which `<HARD-GATE>` was satisfied, what the next step is) while
each subagent does the actual reading/generation/diffing for its one step.

Only fall back to running a step inline when it's a small, single-shot check with no real work to
delegate (e.g. re-reading one file to confirm a hand-off's prerequisite) — not as a default mode.

## Where to start

- **New flow, never ingested**: start at `domo-ingestion`.
- **Already have `flows/<id>.json` + `inventory.csv`, no `overrides.json`**: start at `domo-source-resolution`.
- **Already have `overrides.json` for the flow**: start at `tile-translation`.
- **Already have generated dbt models, not yet scaffolded**: start at `org-dbt-conventions`.
- **Scaffolded, materialization not yet applied**: start at `databricks-materialization-policy`.
- **Materialization applied, `dbt build` not yet green**: start at `dbt-error-triage`.
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

This pipeline delegates to official **dbt** and **Databricks** skills. Install them before
running migrations:

| Agent | Install companions |
|---|---|
| **Claude Code** | Automatic via `.claude-plugin/plugin.json` `dependencies`. If resolution fails: `claude plugin marketplace add dbt-labs/dbt-agent-skills` and `databricks/databricks-agent-skills`, then reinstall. |
| **Cursor** | No dependency field — install manually: `/add-plugin dbt` and `/add-plugin databricks` (official Cursor Marketplace), then enable both in Customize → Plugins. |
| **Other** | `npx skills add dbt-labs/dbt-agent-skills --global` and `databricks aitools install` (or vendor skills into the project). |

If companions are missing, install them before proceeding — do not reinvent what they cover.

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
