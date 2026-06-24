# Authentication for the build step (incl. serverless / inside Databricks)

**Conversion needs no credentials.** `convert_dataflow_to_dbt.py` is pure Python that reads
the extract JSON and writes `.sql` files — no token, no network. Run it anywhere (local, CI,
a Databricks serverless notebook). The **only** thing that needs Databricks auth is
`dbt build`, where `dbt-databricks` connects to a SQL warehouse. That auth is configured
*outside* this skill (a `profiles.yml`, or the Databricks dbt task) — nothing in the skill
reads a token from the environment.

So a "no token env var on serverless" error is a `dbt build` connection-config problem, not a
converter problem — and a PAT/env-var token is **not** the right approach on Databricks.

## Recommended: run `dbt build` as a Databricks Workflows dbt task (no token)

The native, no-secrets way to run dbt on Databricks. Databricks runs the dbt commands against
the warehouse and **authenticates as the job's run-as identity** (a user or service principal)
— it injects the connection, so you don't manage a token or even a `profiles.yml`.

```yaml
# Databricks Asset Bundle (resources/jobs.yml)
tasks:
  - task_key: build_dbt
    dbt_task:
      project_directory: ../out/<project_name>   # the converter's output dir
      commands: ["dbt build"]
      warehouse_id: "<sql_warehouse_id>"
      catalog: "main"
      schema: "domo_migration_dbt"
      # profiles_directory is optional — omit it and Databricks generates the profile
```

Python SDK equivalent: `DbtTask(project_directory=..., commands=["dbt build"], warehouse_id=..., catalog=..., schema=...)`. This is the production path and the cleanest fix for the serverless case.

## If you run dbt yourself (notebook / script): use OAuth, not a PAT env var

`profiles.yml` (top key = the project/profile name the converter printed):

```yaml
<project_name>:
  target: dev
  outputs:
    dev:
      type: databricks
      host: <workspace-host>            # no https://
      http_path: /sql/1.0/warehouses/<warehouse-id>
      catalog: main
      schema: domo_migration_dbt
      threads: 8
      # --- pick ONE auth method ---
      auth_type: oauth                              # OAuth M2M (service principal):
      client_id: "{{ env_var('DATABRICKS_CLIENT_ID') }}"
      client_secret: "{{ env_var('DATABRICKS_CLIENT_SECRET') }}"
```

- **OAuth M2M (service principal)** — recommended for automation/headless. Put the SP's
  `client_id`/`client_secret` in a Databricks **secret scope** (or your env), never in the file.
- **OAuth U2M** — `auth_type: oauth` with no client creds → interactive browser login. Fine for
  local dev; not for serverless/headless.
- **PAT** (`token: <pat>`) — simplest locally, but **this is what failed you**: serverless
  compute exposes no ambient `DATABRICKS_TOKEN`, and the legacy notebook-token API is restricted
  there, so a profile expecting `token` (or `env_var('DATABRICKS_TOKEN')`) finds nothing. If you
  must use a PAT, pass it explicitly from a secret — don't rely on an auto-present env var.

## Why the serverless error happened

`dbt-databricks` requires explicit connection credentials. On serverless there is no
auto-present `DATABRICKS_TOKEN` and `dbutils...apiToken()` is restricted, so a token-based
profile has nothing to read. Switch to the dbt task (no token) or OAuth M2M.

## Split the work to match environments

- **Convert** in a serverless notebook or locally — no auth, produces the dbt project.
- **Build** via a Workflows dbt task (auth handled for you) or with OAuth M2M.

A clean end state for the customer: a Databricks Workflow with a **notebook/Python task** that
runs the conversion, followed by a **dbt task** that builds the generated project — all under
the job's identity, zero tokens.

## References

- Jobs dbt task: https://docs.databricks.com/en/jobs/dbt.html
- dbt-databricks auth: https://docs.databricks.com/en/partners/prep/dbt.html
- OAuth M2M (service principals): https://docs.databricks.com/en/dev-tools/auth/oauth-m2m.html
