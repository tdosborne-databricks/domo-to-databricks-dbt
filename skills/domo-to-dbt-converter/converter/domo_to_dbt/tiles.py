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


def m_groupby(action, ctx):
    gc = ", ".join(f"`{g['name']}`" for g in action.get("groups", []))
    exprs = [transpile_expr(fld["expression"]) for fld in action.get("fields", [])]
    aggs = ", ".join(f"{expr} AS `{fld['name']}`"
                     for expr, fld in zip(exprs, action.get("fields", [])))
    select_list = ", ".join(x for x in [gc, aggs] if x) or "*"
    sql = (f"select {select_list}\nfrom {{{{ ref('{_first_up(ctx)}') }}}}\n"
           f"group by {gc}" if gc else
           f"select {select_list}\nfrom {{{{ ref('{_first_up(ctx)}') }}}}")
    note = _dialect_note(exprs)
    return TileResult(sql, "intermediate", bool(note), note)


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


def m_formula(action, ctx):
    exprs = action.get("expressions", [])
    rendered = [(transpile_expr(e["expression"]), e["fieldName"]) for e in exprs]
    cols = ", ".join(f"{expr} AS `{fn}`" for expr, fn in rendered)
    in_cols = ctx.get("in_cols") or []
    replace = [fn for expr, fn in rendered if f"`{fn}`" in expr or fn in in_cols]
    sql = f"{_star_prefix(replace)} {cols}\nfrom {{{{ ref('{_first_up(ctx)}') }}}}"
    note = _dialect_note([expr for expr, _ in rendered])
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
    # Drop the right side's join-key columns to avoid duplicate column names
    # (l and r share the join keys). Non-key same-named columns across sides
    # may still collide and need manual disambiguation.
    if k2:
        right_cols = "r.* except (" + ", ".join(f"`{b}`" for b in k2) + ")"
    else:
        right_cols = "r.*"
    sql = ("-- non-key column-name collisions across sides may still need manual disambiguation\n"
           f"select l.*, {right_cols}\nfrom {{{{ ref('{left}') }}}} l\n"
           f"{jt} join {{{{ ref('{right}') }}}} r on {cond}")
    return TileResult(sql, "intermediate", False, "")


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


def m_metadata(action, ctx):
    sql = (f"select {_project_fields(action.get('fields'))}\n"
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
    legs = [f"select * from {{{{ ref('{v}') }}}}" for v in ctx["up"]]
    # Databricks SQL has NO `UNION ... BY NAME` (DataFrame-only). Domo Append-Rows
    # aligns legs by column NAME, but positional SQL UNION aligns by position, so
    # flag for review: legs must have matching column order/count, else align
    # columns manually or rebuild this model with explicit projected columns.
    joiner = ("\nunion all\n" if (action.get("unionType") == "INCLUDE_ALL")
              else "\nunion\n")
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
    cols = []
    note, needs = "", False
    for add in action.get("additions", []):
        op = (add.get("operation") or {}).get("operationType", "")
        fn = _WINDOW_FN.get(op.upper())
        if fn:
            cols.append(f"{fn}() OVER ({over}) AS `{add['name']}`")
        else:
            needs = True
            note = f"unsupported window op '{op}'"
            cols.append(f"NULL AS `{add['name']}`  -- NEEDS REVIEW: {note}")
    sql = f"select *, {', '.join(cols)}\nfrom {{{{ ref('{_first_up(ctx)}') }}}}"
    return TileResult(sql, "intermediate", needs, note)


def m_normalizer(action, ctx):
    fields = action.get("fields", [])
    tf = action.get("typefield", "type")
    dest = fields[0]["destField"] if fields else "value"
    pairs = ", ".join(f"'{f['typefieldValue']}', `{f['sourceField']}`" for f in fields)
    # Domo unpivot keeps the non-unpivoted columns; drop only the source columns
    # being melted so downstream tiles still see passthrough columns.
    srcs = sorted({f["sourceField"] for f in fields})
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
        if name in (c.get("fieldA"), c.get("fieldB")) or name in in_cols:
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
