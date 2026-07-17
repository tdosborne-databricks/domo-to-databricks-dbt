# domo-to-databricks-dbt — agent guide

Portable **agent skills** that migrate **Domo Magic ETL dataflows → dbt on Databricks**. This repo
is a *marketplace* containing one plugin (`plugins/domo-to-databricks-dbt`) whose skills are plain
`SKILL.md` + `references/` + `scripts/`.

## Entry point

Start with **`using-domo-to-databricks-dbt`** — target selection, workspace isolation, subagent
dispatch. Then run the fixed pipeline below.

## The nine skills (in order)

1. **`using-domo-to-databricks-dbt`** — entry point; read before any other skill.
2. **`domo-ingestion`** — Domo export or API → normalized flows + inventory.
3. **`domo-source-resolution`** — streams.json → UC discovery → `overrides.json`.
4. **`tile-translation`** — transpile tile DAG → Spark SQL CTEs (deterministic converter).
5. **`org-dbt-conventions`** — scaffold dbt project; UC `*_src` / `*_dbt` / `*_marts` layout.
6. **`databricks-materialization-policy`** — `apply_materialization.py` before first build; Phase B
   proposals after Tier 2.
7. **`dbt-error-triage`** — `dbt build` to green; learning loop via `known-patterns.md`.
8. **`migration-validation`** — Tier 1 static → Tier 2 build → Tier 3 diff kit.
9. **`dbt-project-optimization`** — optional post-migration cleanup (after correctness proven).

Each skill's `SKILL.md` has a `<HARD-GATE>` with prerequisites and hand-off. Conceptual foundation:
`plugins/domo-to-databricks-dbt/skills/tile-translation/references/paradigm.md`.

## Companion official skills

`plugin.json` declares **`dbt`**, **`dbt-migration`**, and **`databricks`** as dependencies —
Claude Code installs them with this plugin. Non-Claude agents should load the equivalent skills
from `dbt-labs/dbt-agent-skills` and `databricks/databricks-agent-skills` separately.

If dependency resolution fails in Claude Code, register the upstream marketplaces once:

```bash
claude plugin marketplace add dbt-labs/dbt-agent-skills
claude plugin marketplace add databricks/databricks-agent-skills
```

## Install this plugin

```bash
claude plugin marketplace add https://github.com/tdosborne-databricks/domo-to-databricks-dbt
claude plugin install domo-to-databricks-dbt@domo-to-databricks-dbt-marketplace
```

Cursor / Codex / Copilot: mirrored manifests in `.cursor-plugin/`, `.agents/plugins/`,
`.github/plugin/`. Or read `SKILL.md` files directly.

## Developing

```bash
cd plugins/domo-to-databricks-dbt
python3 -m pytest tests/ skills/tile-translation/scripts/converter/tests/
```

Transpiler package:
`plugins/domo-to-databricks-dbt/skills/tile-translation/scripts/converter/`. Add tile mappers and
dialect rules **test-first**.

## Operational note

Name skills explicitly in batch/headless prompts. Skill descriptions include trigger phrases for
this reason.
