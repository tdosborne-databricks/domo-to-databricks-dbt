"""Domo tile -> dbt model SELECT-body mappers. Each returns a TileResult."""
from collections import namedtuple
import re

from .common import (_sanitize, _lit, _where_from_filterlist,  # noqa: F401
                     transpile_expr, _working_day_diff)

TileResult = namedtuple("TileResult", "sql layer needs_review note")


def _first_up(ctx):
    up = ctx["up"]
    return up[0] if up else "unknown"


def _star_prefix(except_names):
    """`select *` (or `select * except (...)`) for a compute tile. Domo replaces
    an existing column when a tile's output name matches a column it reads;
    Spark's `select *, expr AS name` would duplicate it. We only except names a
    tile *self-references* (output read in its own expression) — a column read
    in its own formula must pre-exist, so excepting it is safe. Sibling/new
    outputs are never excepted (excepting a non-existent column errors)."""
    seen, dups = set(), []
    for n in except_names:
        if n not in seen:
            seen.add(n)
            dups.append(n)
    if dups:
        return "select * except (" + ", ".join(f"`{n}`" for n in dups) + "),"
    return "select *,"


def m_load(action, ctx):
    src = ctx["source_for"](action.get("dataSourceId", ""))
    sql = f"select * from {src}"
    return TileResult(sql, "staging", False, "")


def m_filter(action, ctx):
    where_clause = _where_from_filterlist(action.get('filterList'))
    sql = (f"select * from {{{{ ref('{_first_up(ctx)}') }}}}\n"
           f"where {where_clause}")
    return TileResult(sql, "intermediate", False, "")


# Domo GroupBy aggregation `type` -> Spark aggregate over the field's `source` column.
# Domo encodes aggregations two ways: a `type`+`source` pair (no expression) OR a free-form
# `expression` (a Beast Mode formula). This table covers the former.
_GROUPBY_AGG = {
    "SUM": lambda s: f"SUM(`{s}`)",
    "AVG": lambda s: f"AVG(`{s}`)", "AVERAGE": lambda s: f"AVG(`{s}`)",
    "MIN": lambda s: f"MIN(`{s}`)", "MAX": lambda s: f"MAX(`{s}`)",
    "COUNT": lambda s: f"COUNT(`{s}`)",
    "COUNT_ALL": lambda s: "COUNT(*)",
    "COUNT_DISTINCT": lambda s: f"COUNT(DISTINCT `{s}`)",
    "DISTINCT_COUNT": lambda s: f"COUNT(DISTINCT `{s}`)",
    "STDDEV": lambda s: f"STDDEV(`{s}`)", "VARIANCE": lambda s: f"VARIANCE(`{s}`)",
    "FIRST": lambda s: f"FIRST(`{s}`)", "LAST": lambda s: f"LAST(`{s}`)",
}


def _agg_expr(fld):
    """Aggregate SQL for one GroupBy field, or None if the aggregation is unhandled."""
    if fld.get("expression"):
        return transpile_expr(fld["expression"])
    op = (fld.get("type") or "").upper()
    fn = _GROUPBY_AGG.get(op)
    if fn:
        return fn(fld.get("source") or fld.get("valuefield"))
    return None


def m_groupby(action, ctx):
    gc = ", ".join(f"`{g['name']}`" for g in action.get("groups", []))
    fields = action.get("fields", [])
    exprs, notes, needs = [], [], False
    unmapped = set()
    for fld in fields:
        e = _agg_expr(fld)
        if e is None:                        # unmapped aggregation type -> emit NULL + flag
            needs = True
            unmapped.add(fld.get("name"))
            notes.append(f"{fld.get('name')}: unhandled GroupBy aggregation '{fld.get('type')}'")
            e = "NULL"
        exprs.append(e)
    aggs = ", ".join(
        f"{expr} AS `{fld['name']}`" + (" /* NEEDS REVIEW: unhandled aggregation */" if fld.get("name") in unmapped else "")
        for expr, fld in zip(exprs, fields)
    )
    select_list = ", ".join(x for x in [gc, aggs] if x) or "*"
    sql = (f"select {select_list}\nfrom {{{{ ref('{_first_up(ctx)}') }}}}\n"
           f"group by {gc}" if gc else
           f"select {select_list}\nfrom {{{{ ref('{_first_up(ctx)}') }}}}")
    dn = _dialect_note(exprs)
    note = "; ".join(notes + ([dn] if dn else []))
    return TileResult(sql, "intermediate", needs or bool(dn), note)


# MySQL / Beast Mode functions with no safe automatic Spark translation (or that
# transpile_expr deliberately leaves alone, e.g. non-UTC CONVERT_TZ). Matched as
# function CALLS (`NAME(`) so an identifier merely containing the word isn't
# flagged. Notes are computed AFTER transpile_expr, so functions it handles
# (IFNULL, CURDATE, ...) only remain here when transpilation didn't apply.
# Extend this list as new flows surface uncovered functions (then add a
# transpile_expr rule for the ones with a clean Spark equivalent).
_UNSUPPORTED_FUNCS = (
    "DATE_WORKING_DIFF", "WORKING_DAYS", "CONVERT_TZ", "CURDATE", "IFNULL",
    # common MySQL-only functions Spark lacks — surfaced at conversion time:
    "STR_TO_DATE", "GROUP_CONCAT", "TIMESTAMPDIFF", "TIMESTAMPADD",
    "PERIOD_DIFF", "PERIOD_ADD", "MAKEDATE", "SEC_TO_TIME", "TIME_TO_SEC",
)
# Non-call dialect tokens (substring match).
_DIALECT_SUBSTR = ("INTERVAL ",)


def _dialect_note(texts):
    """Return a review note if any (already-transpiled) expression still carries
    dialect with no safe auto-translation. Line (`--`) and block (`/* */`)
    comments are ignored so commented-out dialect doesn't false-flag; functions
    are matched as calls so identifiers containing a keyword aren't flagged."""
    found = []
    for t in texts:
        scan = re.sub(r"/\*.*?\*/", "", (t or ""), flags=re.S)
        scan = re.sub(r"(?m)--[^\n]*", "", scan).upper()
        for m in _DIALECT_SUBSTR:
            if m in scan and m not in found:
                found.append(m)
        for fn in _UNSUPPORTED_FUNCS:
            if fn not in found and re.search(rf"\b{fn}\s*\(", scan):
                found.append(fn)
    return ("Domo dialect needs manual transpile: " + ", ".join(sorted(found))) if found else ""


def _ci_in(name, cols):
    """Case-insensitive membership: SQL/Spark column identifiers are case-insensitive."""
    n = (name or "").lower()
    return any((c or "").lower() == n for c in cols)


def m_formula(action, ctx):
    exprs = action.get("expressions", [])
    rendered = [(transpile_expr(e["expression"]), e["fieldName"]) for e in exprs]
    in_cols = ctx.get("in_cols") or []
    note = _dialect_note([expr for expr, _ in rendered])
    base = f"{{{{ ref('{_first_up(ctx)}') }}}}"

    # Domo evaluates a formula tile's expressions in order, and a later formula may read a
    # column an earlier one in the SAME tile just produced. A single `select expr1 AS a,
    # expr2 AS b` can't express that -- every reference in a SELECT resolves to the *input*
    # columns, not sibling aliases -- so `b`'s reference to `a` would read the upstream `a`
    # (wrong value, and often the wrong TYPE -> DATATYPE_MISMATCH). Detect that case and emit
    # one nested projection per expression so each sees the prior ones' recomputed columns.
    fns = {fn for _, fn in rendered}
    chained = any(_ci_in(other, [expr]) or f"`{other}`" in expr
                  for expr, fn in rendered for other in fns if other != fn)

    if not chained:
        cols = ", ".join(f"{expr} AS `{fn}`" for expr, fn in rendered)
        # EXCEPT a recomputed column if it already exists upstream (SQL identifiers are
        # case-insensitive, so compare case-insensitively) or the expression references itself.
        replace = [fn for expr, fn in rendered if f"`{fn}`" in expr or _ci_in(fn, in_cols)]
        sql = f"{_star_prefix(replace)} {cols}\nfrom {base}"
        return TileResult(sql, "intermediate", bool(note), note)

    # Nested chain: innermost reads the upstream ref; each layer recomputes one column,
    # EXCEPTing it when it already exists (upstream or produced by an earlier layer).
    seen = {c.lower() for c in in_cols}
    current_from, sql = base, ""
    for expr, fn in rendered:
        replace = [fn] if (fn.lower() in seen or f"`{fn}`" in expr) else []
        sql = f"{_star_prefix(replace)} {expr} AS `{fn}`\nfrom {current_from}"
        current_from = f"(\n{sql}\n)"
        seen.add(fn.lower())
    return TileResult(sql, "intermediate", bool(note), note)


def m_publish(action, ctx):
    sql = f"select * from {{{{ ref('{_first_up(ctx)}') }}}}"
    return TileResult(sql, "marts", False, "")


def m_join(action, ctx):
    up = ctx["up"]
    left = up[0] if len(up) > 0 else "unknown"
    right = up[1] if len(up) > 1 else "unknown"
    jt = (action.get("joinType") or "INNER").upper()
    k1, k2 = action.get("keys1", []), action.get("keys2", [])
    cond = " AND ".join(f"l.`{a}` = r.`{b}`" for a, b in zip(k1, k2)) or "1=1"
    # Drop from the right side: (1) the join-key columns (shared with the left), and
    # (2) any NON-key column also present on the left -- a duplicate name would otherwise
    # trigger AMBIGUOUS_REFERENCE when referenced downstream. (2) needs both sides' column
    # sets: when column lineage is seeded (source_columns), we drop them deterministically
    # and the model is clean; when columns are untracked we can only drop the keys and must
    # flag the model for manual review.
    dep_cols = ctx.get("dep_cols") or []
    left_cols = dep_cols[0] if len(dep_cols) > 0 else []
    right_cols = dep_cols[1] if len(dep_cols) > 1 else []
    known = bool(left_cols) and bool(right_cols)
    drop = list(k2)
    if known:
        lset = {c.lower() for c in left_cols}
        dset = {c.lower() for c in drop}
        for c in right_cols:
            if c.lower() in lset and c.lower() not in dset:
                drop.append(c)
                dset.add(c.lower())
    right_sel = ("r.* except (" + ", ".join(f"`{c}`" for c in drop) + ")") if drop else "r.*"
    sql = (f"select l.*, {right_sel}\nfrom {{{{ ref('{left}') }}}} l\n"
           f"{jt} join {{{{ ref('{right}') }}}} r on {cond}")
    if known:
        return TileResult(sql, "intermediate", False, "")
    # Columns untracked: only join keys dropped; non-key collisions may remain -> review.
    note = ("join may carry non-key columns present on both sides (duplicate names -> "
            "AMBIGUOUS_REFERENCE downstream); wire source_columns or verify upstream schemas")
    return TileResult("-- " + note + "\n" + sql, "intermediate", True, note)


# Domo Magic ETL column types -> Spark/Databricks SQL types. Domo emits a few
# names Spark doesn't have (DATETIME, LONG, TEXT, NUMBER); others pass through.
_SPARK_TYPE = {
    "DATETIME": "TIMESTAMP", "DATE_TIME": "TIMESTAMP",
    "LONG": "BIGINT", "INTEGER": "INT", "TEXT": "STRING", "NUMBER": "DOUBLE",
}


def _spark_type(typ):
    return _SPARK_TYPE.get((typ or "").upper(), typ)


def _project_fields(fields):
    """Render SelectValues/Metadata fields[] to a SELECT column list.
    Drops removed cols, casts typed cols (Domo->Spark type names), aliases renamed cols."""
    cols = []
    for f in fields or []:
        if f.get("remove"):
            continue
        name, rename, typ = f.get("name"), f.get("rename"), f.get("type")
        out_name = rename or name
        expr = f"CAST(`{name}` AS {_spark_type(typ)})" if typ else f"`{name}`"
        cols.append(f"{expr} AS `{out_name}`" if (typ or rename) else f"`{name}`")
    return ", ".join(cols) if cols else "*"


def m_select(action, ctx):
    sql = (f"select {_project_fields(action.get('fields'))}\n"
           f"from {{{{ ref('{_first_up(ctx)}') }}}}")
    return TileResult(sql, "intermediate", False, "")


def _alter_columns(fields, in_cols=()):
    """Render an Alter Columns (Metadata) tile: modify LISTED columns, pass through the rest.

    Unlike Select Columns (an explicit projection), Domo's Alter Columns changes the type/name
    of the listed columns and keeps every unlisted column. So emit `* except(<changed/removed>)`
    plus the transformed expressions, instead of a bare projection that would drop pass-throughs.

    When upstream columns are known (in_cols), skip fields whose column isn't present (a stale
    Domo config referencing a dropped column would otherwise break `except`/CAST), and when a
    rename targets a name that already exists upstream, drop that too (Domo replaces it).
    """
    known = {c.lower() for c in in_cols}        # SQL identifiers are case-insensitive
    tracked = bool(known)
    except_cols, exprs = [], []
    for f in fields or []:
        name = f.get("name")
        if not name:
            continue
        if tracked and name.lower() not in known:   # stale config: column not present upstream
            continue
        if f.get("remove"):
            except_cols.append(name)
            continue
        rename, typ = f.get("rename"), f.get("type")
        if typ or rename:                       # changed -> except original, re-add transformed
            except_cols.append(name)
            out_name = rename or name
            _exc = {c.lower() for c in except_cols}
            if rename and out_name.lower() != name.lower() and out_name.lower() in known \
                    and out_name.lower() not in _exc:
                except_cols.append(out_name)    # rename onto an existing column -> replace it
            expr = f"CAST(`{name}` AS {_spark_type(typ)})" if typ else f"`{name}`"
            exprs.append(f"{expr} AS `{out_name}`")
        # else: listed but unchanged -> passes through via `*`
    if not except_cols and not exprs:
        return "*"
    star = "*" + (f" except ({', '.join(f'`{c}`' for c in except_cols)})" if except_cols else "")
    return star + (", " + ", ".join(exprs) if exprs else "")


def m_metadata(action, ctx):
    sql = (f"select {_alter_columns(action.get('fields'), ctx.get('in_cols', ()))}\n"
           f"from {{{{ ref('{_first_up(ctx)}') }}}}")
    return TileResult(sql, "intermediate", False, "")


def m_unique(action, ctx):
    keys = [f"`{f['name']}`" for f in action.get("fields", [])]
    part = ", ".join(keys) if keys else "1"
    sql = (f"select * from {{{{ ref('{_first_up(ctx)}') }}}}\n"
           f"qualify row_number() over (partition by {part} order by {part}) = 1")
    return TileResult(sql, "intermediate", False, "")


_WINDOW_FN = {"RANK": "RANK", "DENSE_RANK": "DENSE_RANK", "ROW_NUMBER": "ROW_NUMBER",
              "ROWNUMBER": "ROW_NUMBER", "PERCENT_RANK": "PERCENT_RANK"}


def m_union(action, ctx):
    ups = ctx["up"]
    dep_cols = ctx.get("dep_cols") or []
    joiner = ("\nunion all\n" if (action.get("unionType") == "INCLUDE_ALL")
              else "\nunion\n")
    # Databricks SQL has NO `UNION ... BY NAME` (DataFrame-only). Domo Append-Rows aligns legs
    # by column NAME and pads missing columns with NULL; a positional SQL UNION aligns by
    # position and requires identical column counts (-> NUM_COLUMNS_MISMATCH when legs differ).
    # When every leg's columns are known (lineage seeded), emulate UNION BY NAME: project each
    # leg to a canonical column order (first appearance across legs), filling absent columns
    # with NULL. Otherwise fall back to a positional UNION and flag it for manual review.
    if dep_cols and len(dep_cols) == len(ups) and all(dep_cols):
        canon, seen = [], set()
        for cols in dep_cols:
            for c in cols:
                if (c or "").lower() not in seen:
                    canon.append(c)
                    seen.add((c or "").lower())
        legs = []
        for v, cols in zip(ups, dep_cols):
            have = {(c or "").lower() for c in cols}
            sel = ", ".join(f"`{c}`" if (c or "").lower() in have else f"NULL AS `{c}`"
                            for c in canon)
            legs.append(f"select {sel}\nfrom {{{{ ref('{v}') }}}}")
        return TileResult(joiner.join(legs) or "select 1", "intermediate", False, "")

    legs = [f"select * from {{{{ ref('{v}') }}}}" for v in ups]
    sql = ("-- NEEDS REVIEW: positional UNION; Domo aligns by column name. Verify "
           "leg column order/count match (Databricks SQL has no UNION BY NAME).\n"
           + (joiner.join(legs) or "select 1"))
    note = "positional UNION — verify leg column order/count (no UNION BY NAME in Databricks SQL)"
    return TileResult(sql, "intermediate", True, note)


def m_window(action, ctx):
    part = ", ".join(f"`{g['column']}`" for g in action.get("groupRules", []))
    order = ", ".join(
        f"`{o['column']}` {'ASC' if o.get('ascending', True) else 'DESC'}"
        for o in action.get("orderRules", []))
    over = f"PARTITION BY {part} ORDER BY {order}" if part else f"ORDER BY {order}"
    in_cols = ctx.get("in_cols") or []
    cols, replace = [], []
    note, needs = "", False
    for add in action.get("additions", []):
        name = add["name"]
        # A window output whose name already exists upstream (e.g. a rank column carried over from a
        # prior Domo run) must EXCEPT the original, else `select *, ... AS name` -> COLUMN_ALREADY_EXISTS.
        if _ci_in(name, in_cols):
            replace.append(name)
        op = (add.get("operation") or {}).get("operationType", "")
        fn = _WINDOW_FN.get(op.upper())
        if fn:
            cols.append(f"{fn}() OVER ({over}) AS `{name}`")
        else:
            needs = True
            note = f"unsupported window op '{op}'"
            cols.append(f"NULL AS `{name}` /* NEEDS REVIEW: {note} */")
    sql = f"{_star_prefix(replace)} {', '.join(cols)}\nfrom {{{{ ref('{_first_up(ctx)}') }}}}"
    return TileResult(sql, "intermediate", needs, note)


def m_normalizer(action, ctx):
    fields = action.get("fields", [])
    tf = action.get("typefield", "type")
    dest = fields[0]["destField"] if fields else "value"
    pairs = ", ".join(f"'{f['typefieldValue']}', `{f['sourceField']}`" for f in fields)
    in_cols = ctx.get("in_cols") or []
    # Domo unpivot keeps the non-unpivoted columns; drop the source columns being melted so
    # downstream tiles still see passthrough columns. Also drop any passthrough column that
    # collides with the stack's OUTPUT names (typefield / destField) -- otherwise the stack
    # emits a second `dest`/`typefield` alongside the existing one -> AMBIGUOUS_REFERENCE.
    drop = {f["sourceField"] for f in fields}
    for out_name in (tf, dest):
        if _ci_in(out_name, in_cols):
            drop.add(out_name)
    srcs = sorted(drop)
    except_clause = (" except (" + ", ".join(f"`{s}`" for s in srcs) + ")") if srcs else ""
    sql = (f"select *{except_clause}, stack({len(fields)}, {pairs}) as (`{tf}`, `{dest}`)\n"
           f"from {{{{ ref('{_first_up(ctx)}') }}}}")
    return TileResult(sql, "intermediate", False, "")


def m_datecalc(action, ctx):
    # DATE_WORKING_DIFF -> exact Mon-Fri business-day count via the weekday-serial
    # formula (no holiday calendar). Other calcTypes are unhandled and flagged.
    calcs = action.get("calculations", [])
    if not calcs:
        return TileResult(f"select * from {{{{ ref('{_first_up(ctx)}') }}}}", "intermediate", True, "DateCalculator with no calculations")

    in_cols = ctx.get("in_cols") or []
    cols, notes, needs, replace = [], [], False, []
    for c in calcs:
        ct = (c.get("calcType") or "").upper()
        name = c["fieldName"]
        if name in (c.get("fieldA"), c.get("fieldB")) or _ci_in(name, in_cols):
            replace.append(name)
        if ct == "DATE_WORKING_DIFF":
            expr = _working_day_diff(f"`{c['fieldA']}`", f"`{c['fieldB']}`")
            cols.append(f"{expr} AS `{name}`")
        else:
            needs = True
            cols.append(f"NULL AS `{name}`")
            notes.append(f"{name}: unhandled calcType '{ct}'")
    sql = f"{_star_prefix(replace)} {', '.join(cols)}\nfrom {{{{ ref('{_first_up(ctx)}') }}}}"
    return TileResult(sql, "intermediate", needs, "; ".join(notes))


def m_sql(action, ctx):
    body = "\n".join(action.get("statements", [])) or "select 1"
    body = transpile_expr(body)
    # dbt models are a single statement — a trailing `;` is a parse error.
    body = body.rstrip().rstrip(";").rstrip()
    # Rewrite bare references to known input view names -> {{ ref() }}.
    for v in ctx["up"]:
        body = re.sub(rf"(?<![\w`]){re.escape(v)}(?![\w`])",
                      f"{{{{ ref('{v}') }}}}", body)
    note = "raw Domo MAGIC SQL — verify MySQL->Spark transpile (functions, backticks, joins); ref-rewrite may also match a view name inside a string literal"
    return TileResult(body, "intermediate", True, note)


MAPPERS = {
    "LoadFromVault": m_load, "Filter": m_filter, "GroupBy": m_groupby,
    "ExpressionEvaluator": m_formula, "PublishToVault": m_publish, "MergeJoin": m_join,
    "SelectValues": m_select, "Metadata": m_metadata, "Unique": m_unique,
    "UnionAll": m_union, "WindowAction": m_window, "Normalizer": m_normalizer,
    "DateCalculator": m_datecalc, "SQL": m_sql,
}


def render_tile(action, ctx):
    mapper = MAPPERS.get(action["type"])
    if mapper:
        return mapper(action, ctx)
    sql = f"select * from {{{{ ref('{_first_up(ctx)}') }}}}"
    return TileResult(sql, "intermediate", True,
                      f"unhandled tile type '{action['type']}'")
