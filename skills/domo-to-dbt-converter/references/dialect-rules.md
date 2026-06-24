# Beast Mode / MySQL → Spark SQL dialect rules

Domo "Magic" expressions (Beast Mode) use a MySQL-flavored dialect. The converter's
`transpile_expr()` in `converter/domo_to_dbt/common.py` rewrites them to Spark SQL.
It runs on every formula, group-by, filter, SQL-tile, and DateCalculator expression.

## Auto-translated (deterministic)

| Domo / MySQL | Spark SQL | Notes |
|---|---|---|
| `# ...`, `-- ...`, `/* ... */` | stripped | `#` preserved inside `` `backtick` `` identifiers (e.g. `` `#HoursToClose` `` is a column, not a comment). `--`/`#` stripped because they break when expressions are wrapped/inlined. |
| `IFNULL(a, b)` | `coalesce(a, b)` | |
| `CURDATE()` / `NOW()` | `current_date()` / `current_timestamp()` | |
| `DATE_ADD(x, INTERVAL <expr> DAY)` | `date_add(x, <expr>)` | Spark INTERVAL literals require a constant; Domo passes expressions. |
| `CONVERT_TZ(x, 'UTC', tz)` | `from_utc_timestamp(x, tz)` | Only UTC source is auto-translated; non-UTC is flagged. |
| `DATE_FORMAT(x, '%Y-%m')` | `date_format(x, 'yyyy-MM')` | MySQL format codes (`%Y %m %d %H %i %s ...`) → Spark pattern letters. |
| `REGEXP_LIKE(x, 'pat', 'i')` | `regexp_like(x, '(?i)pat')` | Spark has no flag arg; the `i` flag folds into the pattern. |
| `DATETIME(x)` | `CAST(x AS TIMESTAMP)` | MySQL cast function (nested args handled). |
| `CAST(x AS CHAR)` | `CAST(x AS STRING)` | MySQL CHAR (no length) → STRING; `CHAR(n)` left alone. |
| `DATE_WORKING_DIFF(a, b)` | weekday-serial formula | Exact Mon–Fri business-day count, no holiday calendar. Verified vs brute force over 13k date pairs and on Spark. |

## Domo column-replace semantics

Domo's `select *, expr AS name` **replaces** an existing column when `name` already
exists; Spark **duplicates** it (→ `AMBIGUOUS_REFERENCE`). The converter emits
`select * except(name), expr AS name` when a tile either self-references its own output
or (via column lineage) re-creates a column known to exist upstream.

## Flagged for manual review (not auto-translated)

- **Raw SQL tiles** — arbitrary MySQL; the converter rewrites known functions and
  bare table refs but cannot guarantee correctness. Always flagged.
- **Positional UNION** — Domo "Append Rows" aligns legs by **column name**; Databricks
  SQL has **no `UNION BY NAME`**. Emitted as positional `UNION`/`UNION ALL` and flagged;
  verify leg column order/count, or rebuild with explicit projected columns.
- **Non-UTC `CONVERT_TZ`**, and any unrecognized dialect token.

## DATE_WORKING_DIFF formula

Business days in `(b, a]`, Mon–Fri, no holidays. With epoch = a Monday (1900-01-01)
and `n = datediff(d, epoch) + 1`, weekdays up to `d` = `(n div 7)*5 + least(n % 7, 5)`;
the diff of the two endpoint counts is the business-day count. To add holidays, subtract
a join against a holidays calendar table (future enhancement).

## Adding a new rule (test-first)

1. Add a failing test in `converter/tests/test_transpile.py` (or `test_tiles.py`) using a
   real expression from a flagged tile.
2. Add the rule:
   - simple function-name swap → `_FUNC_FIXUPS` in `common.py`
   - structural rewrite with captures → `_REGEX_FIXUPS`
   - balanced-paren / nested-arg rewrite → `_rewrite_func(...)` (see `_expand_datetime`)
3. Run `python3 -m pytest` from `converter/`, then regenerate + rebuild and re-measure.

Find the precise manual worklist for any flow in `conversion_report.json → needs_review`.
