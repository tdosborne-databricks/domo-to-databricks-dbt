# Domo Magic ETL tile → dbt model mapping

Each Domo tile (`action`) becomes one dbt model. Mappers live in
`converter/domo_to_dbt/tiles.py` (`m_<type>()`); dispatch is in `render_tile()`.

| Domo tile `type` | dbt layer | Maps to | Notes |
|---|---|---|---|
| `LoadFromVault` | staging | `select * from {{ source('domo', <name>) }}` | Source resolved via `dataset_mapping` + `overrides.json`. |
| `Filter` | intermediate | `... where <predicate>` | Structured (`leftField`/`operator`/`rightValue`) **or** Beast Mode `expression` (transpiled). |
| `GroupBy` | intermediate | `select <groups>, <aggs> ... group by <groups>` | Agg expressions transpiled. |
| `ExpressionEvaluator` | intermediate | `select *, <expr> AS <field> ...` | Formula tile. Expressions transpiled; self-referential/replaced columns `EXCEPT`-ed. |
| `MergeJoin` | intermediate | `select l.*, r.* except (<right keys>) ... <join>` | Right-side join keys dropped to avoid dup columns; non-key collisions flagged. |
| `SelectValues` | intermediate | projected `select` | Drops removed cols, casts typed cols, aliases renames. |
| `Metadata` | intermediate | projected `select` | Domo type names mapped to Spark (DATETIME→TIMESTAMP, LONG→BIGINT, TEXT→STRING, NUMBER→DOUBLE). |
| `Unique` | intermediate | `qualify row_number() over (partition by <keys>) = 1` | Dedupe. |
| `UnionAll` | intermediate | positional `UNION` / `UNION ALL` | **Flagged**: Domo aligns by name; Spark SQL has no `UNION BY NAME`. |
| `WindowAction` | intermediate | `<fn>() over (partition by ... order by ...)` | RANK/DENSE_RANK/ROW_NUMBER/PERCENT_RANK; unknown ops flagged. |
| `Normalizer` | intermediate | `stack(n, ...)` unpivot | Keeps passthrough cols (`* except (<unpivoted>)`). |
| `DateCalculator` | intermediate | per-calc expression | `DATE_WORKING_DIFF` → business-day formula; other calcTypes flagged. |
| `SQL` | intermediate | raw SQL body | **Flagged**: known funcs/refs rewritten, trailing `;` stripped, but verify. |
| `PublishToVault` | marts | `select * from {{ ref(...) }}` | Terminal output → a Delta table. |

Unrecognized tile types fall back to a passthrough `select *` and are flagged.

## Dependency resolution

`dag.py` reads upstream ids from `dependsOn` (list), `inputs` (list), or `input` (str),
topologically sorts, and assigns each tile a unique model name (sanitized from the tile
name). Refs between models use `{{ ref('<name>') }}`.

## Column lineage

`lineage.py` (`produced_columns`) tracks each tile's output columns through the DAG so a
tile re-creating an existing column drops the duplicate. Source columns are untracked
until a projection names them, and **SQL tiles are opaque** (their output columns can't be
inferred), which limits the column-replace fix in SQL-heavy chains.
