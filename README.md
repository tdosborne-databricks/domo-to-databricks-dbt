# domo-to-databricks-dbt

A cross-tool **agent-skills marketplace** that migrates **Domo Magic ETL dataflows** to **dbt on
Databricks** — ingest a Domo export, resolve connector sources to Unity Catalog, transpile the
tile DAG to Spark SQL, scaffold and test a dbt project, apply materialization policy, validate in
tiers, and deploy as a Workflows **dbt task**.

This is a **decompilation** problem, not a syntax conversion: Magic ETL logic lives in serialized
GUI state. A good migration exploits the dbt/Databricks paradigm rather than mechanically porting
tiles. Start at
`plugins/domo-to-databricks-dbt/skills/tile-translation/references/paradigm.md`.

## Install

This repo is a plugin marketplace containing one plugin (`domo-to-databricks-dbt`). Skills are plain
`SKILL.md` files and work across agent tools. Clone this repository locally, then:

```bash
# Claude Code
claude plugin marketplace add https://github.com/tdosborne-databricks/domo-to-databricks-dbt
claude plugin install domo-to-databricks-dbt@domo-to-databricks-dbt-marketplace
```

Cursor, Codex/CLI agents, and GitHub Copilot read the mirrored manifests in `.cursor-plugin/`,
`.agents/plugins/`, and `.github/plugin/`. Tools without a plugin system can read
`plugins/domo-to-databricks-dbt/skills/` directly. See `AGENTS.md`.

### Repo layout

```
.claude-plugin/marketplace.json     # + .cursor-plugin/, .agents/plugins/, .github/plugin/
AGENTS.md                           # cross-tool entrypoint
plugins/domo-to-databricks-dbt/
  .claude-plugin/plugin.json
  skills/                           # 9 skills (SKILL.md + references/ + scripts/)
  tests/                            # pipeline integration tests
```

## Fixed pipeline (run in this order)

Start every migration with **`using-domo-to-databricks-dbt`** — it sets target (local vs Databricks),
workspace isolation, and subagent dispatch.

```
domo-ingestion → domo-source-resolution → tile-translation → org-dbt-conventions
  → databricks-materialization-policy (Phase A: apply)
  → dbt-error-triage (first dbt build)
  → migration-validation (Tier 1 → Tier 2 → Tier 3)
  → (optional) dbt-project-optimization
```

| Skill | Role |
|---|---|
| `using-domo-to-databricks-dbt` | Entry point: target, isolation, dispatch model. **Always first.** |
| `domo-ingestion` | Export or live API → normalized flow graph + inventory. |
| `domo-source-resolution` | Flow-scoped `streams.json` extract → UC discovery with user → `overrides.json`. |
| `tile-translation` | Tile DAG → Spark SQL CTEs; Beast Mode/MySQL dialect; `-- TODO` flags. |
| `org-dbt-conventions` | Layering, naming, scaffold (`sources.yml`, models, tests). |
| `databricks-materialization-policy` | **Apply** view/table defaults (`apply_materialization.py`); propose clustering/incremental (Phase B). |
| `dbt-error-triage` | Drive `dbt build` to green; promote fixes to converter (`known-patterns.md`). |
| `migration-validation` | Tier 1 static → Tier 2 build → Tier 3 customer diff kit. |
| `dbt-project-optimization` | Post-migration cleanup (inline, rename) after correctness is proven. |

**Official skill overlays** (declared in `plugin.json`): `dbt`, `dbt-migration`, `databricks`.
Install marketplaces once if needed:

```bash
claude plugin marketplace add dbt-labs/dbt-agent-skills
claude plugin marketplace add databricks/databricks-agent-skills
```

Prefer skills + the `dbt` CLI over the dbt MCP server for batch runs.

## Unity Catalog layout

Generated projects use **three schemas** so raw sources don't mix with marts in the catalog UI:

| Role | UC schema | Wired by |
|------|-----------|----------|
| Raw sources | `{project}_dbt_src` | `overrides.json` → `sources.yml` |
| Staging + intermediate | `{project}_dbt` | `profiles.yml` / dbt task `schema` |
| Marts (Domo outputs) | `{project}_dbt_marts` | `dbt_project.yml` `marts: +schema: marts` |

Land ingested tables in `*_src`. Point overrides at `main.<project>_dbt_src.<table>` (native Delta
tables or foreign federated catalogs).

## End-to-end workflow

1. **Ingest** (`domo-ingestion`) → flows, inventory, completeness report.
2. **Resolve sources** (`domo-source-resolution`) → extract `streams.json` for the target flow,
   search Unity Catalog with the user, produce `overrides.json`.
3. **Per flow**: transpile (`tile-translation`); resolve `-- TODO` tiles; scaffold
   (`org-dbt-conventions`); **apply** materialization Phase A (`apply_materialization.py` +
   `dbt build`); triage to green (`dbt-error-triage`); Tier 1 + Tier 2 (`migration-validation`).
4. **Deploy** as a Databricks Asset Bundle with a Workflows **dbt task** (recommended for
   serverless/headless — auth is the job identity, no PAT). See
   `migration-validation/references/authentication.md`.
5. **Tier 3** customer data-diff (`customer_diff_kit/`) → cutover sign-off.
6. **(Optional)** `dbt-project-optimization` after Tier 2/3 pass.

> Batch prompts must **name skills explicitly** — agents under-trigger in headless runs.

## Requirements

- Python 3.9+ (skill scripts use the standard library; converter tests use pytest).
- Official dbt + databricks plugins (see above).
- Databricks workspace + SQL warehouse (or Workflows dbt task) for Tier 2 builds.
- Domo Step-1 extract including `dataflows.json`, `dataset_mapping.json`, and `streams.json`.

## Tests

```bash
cd plugins/domo-to-databricks-dbt
python3 -m pytest tests/ skills/tile-translation/scripts/converter/tests/
```
