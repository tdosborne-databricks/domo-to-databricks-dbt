# Isolated workspace setup, per target

`scripts/init_migration_workspace.py` handles the local-disk case (fresh directory, refuses to
reuse a non-empty one without `--force`, git-inits it). The Databricks-workspace case needs a
different mechanism, since there's no local directory to `mkdir` — isolation instead comes from
Unity Catalog naming and the Databricks Asset Bundle's own deploy-path scoping.

## Local target

```bash
python3 <skill_dir>/scripts/init_migration_workspace.py <root_dir> <flow_name> --target local
```

Everything downstream (`domo-ingestion`'s `<out_dir>`, `org-dbt-conventions`'s
`<dbt_project_dir>`) should point inside `<root_dir>/ingestion/` and `<root_dir>/dbt/`
respectively — never at a shared or reused scratch directory from a prior flow or session.

## Databricks target

Two separate isolation concerns, both need to hold:

### 1. Bundle deploy path (workspace files)

Don't manually `databricks workspace mkdirs` a path and hope nothing collides — DAB already
isolates deploy paths automatically, if you use its mechanism instead of working around it:

- Give each flow's bundle a **unique `bundle.name`** in `databricks.yml` (e.g.
  `domo_migration_<flow_name>`, not a generic name reused across flows/customers) — this is the
  key DAB scopes the deploy path by.
- Use `mode: development` on the working target (as this plugin's generated `databricks.yml`
  already does). Development mode prefixes deployed resource names with `[dev <user>]` and deploys
  under the current user's own path automatically — two engineers (or two flows) targeting the same
  workspace with `mode: development` don't collide even without picking distinct paths by hand.
- Run `databricks bundle validate -t <target>` **before** `databricks bundle deploy` — it will
  surface a name collision against an existing bundle deployment rather than silently overwriting
  one.

### 2. Unity Catalog data isolation

Workspace-file isolation says nothing about where the dbt models actually land — that's controlled
by `catalog`/`schema` in the `dbt_task` config (or `profiles.yml` target locally). Reusing the same
`catalog.schema` across two different flows or customers (e.g. always defaulting to
`main.domo_migration`, as this session did) means a second migration's tables can silently
overwrite or intermix with the first's. Pick a schema scoped to the engagement —
`<catalog>.domo_migration_<flow_name>` or `<catalog>.domo_migration_<customer>` — not a fixed
default, and confirm it doesn't already exist with unrelated tables in it before the first
`dbt build`.

## Either way

Whichever target is chosen, record it in `MIGRATION.md` (written by `init_migration_workspace.py`
for the local case; for Databricks-only work, write the equivalent — bundle name, target catalog/
schema — into the workspace path's `MIGRATION.md` or the bundle's own README) so a fresh session
picking this back up reads the answer instead of re-asking the local-vs-Databricks question from
`using-domo-to-databricks-dbt/SKILL.md`.
