# domo-to-databricks-dbt — agent guide

Portable **agent skills** that migrate **Domo Magic ETL dataflows → dbt on Databricks**. This repo
is a *marketplace* containing one plugin (`plugins/domo-to-databricks-dbt`) whose skills are plain
`SKILL.md` + `references/` + `scripts/` — the format Claude Code, Cursor, Codex/CLI agents, and
Databricks agents all read.

## The five skills (run in this order)

1. **`domo-ingestion`** — ingest the Domo export → normalized flow graph + inventory + completeness
   check. **Always first.**
2. **`tile-translation`** — transpile the tile DAG to Spark SQL, collapsing tile chains into CTEs
   (a flow → a few models, not one model per tile). Deterministic transpiler + dialect engine.
3. **`org-dbt-conventions`** — layer/name/test/scaffold the dbt project (overlay on the official
   `dbt` skill).
4. **`databricks-materialization-policy`** — apply view/table defaults before first build (Phase A);
   propose clustering/incremental after Tier 2 (Phase B). Overlay on official `databricks` skills.
5. **`dbt-error-triage`** — drive `dbt build` to green; converter learning loop.
6. **`migration-validation`** — tiered validation (static → build → customer data-diff) + audit log.

Each skill's `SKILL.md` lists its trigger phrases, workflow, and references. Start at
`plugins/domo-to-databricks-dbt/skills/tile-translation/references/paradigm.md` for the conceptual foundation.

## Companion official skills (recommended)

The overlays defer to the official dbt + Databricks agent skills — install them alongside:

```bash
claude plugin marketplace add dbt-labs/dbt-agent-skills
claude plugin marketplace add databricks/databricks-agent-skills
claude plugin install dbt@dbt-agent-marketplace
claude plugin install databricks@databricks-agent-skills
```

## Install this plugin

```bash
# Claude Code (or any tool reading .claude-plugin/marketplace.json)
claude plugin marketplace add <this-repo>
claude plugin install domo-to-databricks-dbt@domo-to-databricks-dbt-marketplace
```

Cursor / Codex / GitHub Copilot read the mirrored manifests in `.cursor-plugin/`, `.agents/plugins/`,
and `.github/plugin/` respectively. Tools without a plugin system can read the `SKILL.md` files
directly — they are self-contained.

## Operational note

Agents under-trigger skills in headless/batch runs, so batch prompts should **name the skills to
use explicitly**. Skill descriptions are written with trigger phrases for this reason.

## Developing

The transpiler is a pure-stdlib Python package at
`plugins/domo-to-databricks-dbt/skills/tile-translation/scripts/converter/`. Add tile mappers or dialect rules
**test-first**:

```bash
cd plugins/domo-to-databricks-dbt/skills/tile-translation/scripts/converter && python3 -m pytest
```
