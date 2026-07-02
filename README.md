# domo-to-databricks-dbt

A cross-tool **agent-skills marketplace** that migrates **Domo Magic ETL dataflows** to **dbt on
Databricks** — an agent that ingests a Domo export, transpiles the tile DAG to Spark SQL, structures
and tests a dbt project, chooses materializations, validates in tiers, and deploys the models as
scheduled Databricks Jobs.

This is a **decompilation** problem, not a syntax conversion: Magic ETL logic lives in serialized
GUI state, and an optimal migration exploits the dbt/Databricks paradigm rather than mechanically
porting tiles. See `plugins/domo-to-databricks-dbt/skills/tile-translation/references/paradigm.md`.

## Install

This repo is a plugin marketplace containing one plugin (`domo-to-databricks-dbt`). The skills are plain
`SKILL.md` files, so they work across agent tools:

```bash
# Claude Code
claude plugin marketplace add <this-repo>          # reads .claude-plugin/marketplace.json
claude plugin install domo-to-databricks-dbt@domo-to-databricks-dbt-marketplace
```

Cursor, Codex/CLI agents, and GitHub Copilot read the mirrored manifests in `.cursor-plugin/`,
`.agents/plugins/`, and `.github/plugin/`. Any tool without a plugin system can read the
`SKILL.md` files under `plugins/domo-to-databricks-dbt/skills/` directly — they are self-contained. See
`AGENTS.md`.

### Repo layout

```
.claude-plugin/marketplace.json     # + .cursor-plugin/, .agents/plugins/, .github/plugin/ (mirrors)
AGENTS.md                           # cross-tool entrypoint
plugins/domo-to-databricks-dbt/
  .claude-plugin/plugin.json
  skills/                           # the 5 skills (SKILL.md + references/ + scripts/)
```

## Architecture: two skill families

**Custom skills (this plugin)** — everything Domo-specific:

| Skill | Role |
|---|---|
| `domo-ingestion` | Ingest the customer export (or live Domo API) → normalized flow graph + inventory + completeness check. **Run first.** |
| `tile-translation` | Transpile the tile DAG → Spark SQL CTEs; rewrite Beast Mode/MySQL dialect; flag untranslatable tiles. |
| `org-dbt-conventions` | Overlay on the official `dbt` skill: layering, naming, split/dedupe rules, tests, scaffolding. |
| `databricks-materialization-policy` | Overlay on the official `databricks` skills: view/table/incremental, clustering, UC naming. |
| `migration-validation` | Tiered validation (static → build → customer data-diff) + per-flow audit log. |

**Official skills (install separately)** — the target platform's best practices:

```bash
claude plugin marketplace add dbt-labs/dbt-agent-skills
claude plugin marketplace add databricks/databricks-agent-skills
claude plugin install dbt@dbt-agent-marketplace
claude plugin install dbt-migration@dbt-agent-marketplace
claude plugin install databricks@databricks-agent-skills
```

The custom overlays contain only the **deltas** (our conventions, our tolerances) and defer to the
official skills for general dbt/Databricks work — e.g. `databricks-dabs`/`databricks-jobs` govern
the Asset Bundle + dbt-task jobs, and `dbt` governs model structure and testing.

**Execution feedback:** prefer skills + the `dbt` CLI over the dbt MCP server for batch runs (MCP
tool schemas inflate token cost ~30x at scale). MCP is optional for interactive debugging only.

## End-to-end workflow (per migration batch)

1. **Ingest** the customer export (`domo-ingestion`) → normalize, inventory, flag incomplete flows.
2. **Order** flows topologically so upstream models exist before `ref()` targets.
3. **Per flow**: transpile → CTEs (`tile-translation`); agent resolves `-- TODO` tiles; scaffold
   models/sources/schema.yml (`org-dbt-conventions`, official `dbt`); apply materialization
   (`databricks-materialization-policy`); validate Tier 1, then Tier 2 `dbt build` if a workspace
   is available (max 3 auto-fix iterations, else escalate); write the migration log.
4. **Deploy** as a Databricks Asset Bundle of dbt-task Jobs grouped by DAG layer/domain (official
   `databricks-dabs`/`databricks-jobs`), schedules mapped from Domo where known.
5. **Human review gate**, then **customer Tier 3 validation** (diff kit) → cutover sign-off.

> **Operational note:** Claude under-triggers skills in headless/batch runs — batch prompts must
> **explicitly name the skills to use** rather than relying on auto-triggering. Skill descriptions
> here are written "pushy" (trigger phrases listed) for this reason.

## Requirements

- Python 3.9+ (custom scripts use only the standard library).
- The official dbt + databricks plugins installed (above).
- `dbt-databricks` + a Databricks workspace / SQL warehouse to build and deploy.

## Status

**v2.0** restructures the original single-skill converter into the 5-skill migration-agent
architecture and installs the official dbt/Databricks skills as overlays. The transpiler
(all 14 tile types, dialect engine, 82 tests) is the engine under
`plugins/domo-to-databricks-dbt/skills/tile-translation/scripts/converter/`.

**v2 granularity rewrite (done):** the transpiler now collapses tile chains into CTEs — a tile
becomes its own model only if it's a boundary (source / sink / reuse point); every other tile
inlines as a named CTE. On the real AppDirect flow this produces **70 models from 272 tiles**
(29 staging + 22 intermediate + 19 marts) with full flow→model→tile traceability headers.

**Still stubbed (need the real export / a workspace):** the ingestion Mode A parser, and the
static-validator / dbt-test-generator / customer-diff-kit scripts. See each SKILL.md's STATUS notes.
