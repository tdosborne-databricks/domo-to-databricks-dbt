"""Shared SQL-rendering helpers (ported from convert_dataflow_to_sdp_sql.py)."""
import re

_OPS = {"EQ": "=", "NE": "<>", "NEQ": "<>", "GT": ">", "GTE": ">=", "GE": ">=",
        "LT": "<", "LTE": "<=", "LE": "<=", "LIKE": "LIKE", "IN": "IN",
        "IS_NULL": "IS NULL", "IS_NOT_NULL": "IS NOT NULL", "NOT_NULL": "IS NOT NULL"}


def _sanitize(s):
    s = (s or "").replace(" ", "_").lower()
    return "".join(c if c.isalnum() or c == "_" else "_" for c in s)


def unique_name(base, used):
    """Return base, or base_<n> if already used. Records the result in `used`."""
    name, i = base, 1
    while name in used:
        i += 1
        name = f"{base}_{i}"
    used.add(name)
    return name


def _lit(rv):
    if not rv:
        return "NULL"
    v, t = rv.get("value"), (rv.get("type") or "").upper()
    if v is None:
        return "NULL"
    if t in ("STRING", "TEXT", "DATE", "DATETIME"):
        return "'" + str(v).replace("'", "''") + "'"
    return str(v)


def _strip_line_comments(s):
    """Remove MySQL `#` and SQL `--` line comments. `#` is preserved inside a
    backtick-quoted identifier (e.g. `#HourstoComplete`, a real column) by only
    treating it as a comment at line start or after whitespace — column-name
    hashes are always preceded by a backtick. `--` is stripped too: it carries
    no semantics and is unsafe once an expression is wrapped/inlined (a trailing
    `-- ...` would comment out a following `)`)."""
    s = re.sub(r"(?m)^#[^\n]*", "", s)
    s = re.sub(r"(?m)(?<=\s)#[^\n]*", "", s)
    s = re.sub(r"(?m)^--[^\n]*", "", s)
    s = re.sub(r"(?m)(?<=\s)--[^\n]*", "", s)
    return s


# MySQL date-format codes -> Spark datetime pattern letters. Longest-first so
# e.g. %% (literal percent) and multi-char codes don't clobber each other.
_MYSQL_DATEFMT = [
    ("%Y", "yyyy"), ("%y", "yy"), ("%m", "MM"), ("%d", "dd"),
    ("%H", "HH"), ("%h", "hh"), ("%i", "mm"), ("%s", "ss"), ("%p", "a"),
]


def _mysql_fmt_to_spark(fmt):
    for src, dst in _MYSQL_DATEFMT:
        fmt = fmt.replace(src, dst)
    return fmt


# Pattern-based MySQL -> Spark rewrites that need structure (captures), applied
# before the simple name swaps below.
_REGEX_FIXUPS = [
    # DATE_ADD(x, INTERVAL <expr> DAY): Spark INTERVAL literals require a
    # constant, but Domo passes an expression -> rewrite to date_add(x, <expr>).
    (re.compile(r"(?i),\s*interval\s+(.+?)\s+day(?=\s*\))"), r", \1"),
    # CONVERT_TZ(x,'UTC',tz) -> from_utc_timestamp(x, tz). All real uses convert
    # from UTC; non-UTC sources are left for _dialect_note to flag.
    (re.compile(r"(?i)\bconvert_tz\s*\(\s*(.+?)\s*,\s*'utc'\s*,\s*('[^']*')\s*\)"),
     r"from_utc_timestamp(\1,\2)"),
    # REGEXP_LIKE(x,'pat','i'): Spark regexp_like has no flag arg -> fold the
    # case-insensitive flag into the pattern as an inline (?i).
    (re.compile(r"(?i)\bregexp_like\s*\(\s*(.+?)\s*,\s*'([^']*)'\s*,\s*'i'\s*\)"),
     r"regexp_like(\1,'(?i)\2')"),
    # MySQL CAST(x AS CHAR) (no length) -> Spark STRING; CHAR(n) is left alone.
    (re.compile(r"(?i)\bAS\s+CHAR\b(?!\s*\()"), "AS STRING"),
]

# DateFormat needs a callback to translate the literal format string.
_DATE_FORMAT_RE = re.compile(r"(?i)\bdate_format\s*\(\s*(.+?)\s*,\s*'([^']*)'\s*\)")

# Simple function name swaps (no structural change).
_FUNC_FIXUPS = [
    (re.compile(r"\bIFNULL\s*\(", re.I), "coalesce("),
    (re.compile(r"\bCURDATE\s*\(\s*\)", re.I), "current_date()"),
    (re.compile(r"\bNOW\s*\(\s*\)", re.I), "current_timestamp()"),
]


def _split_top_args(s):
    """Split a function-argument string on top-level commas, ignoring commas
    inside parens, backtick identifiers, or single-quoted string literals."""
    args, cur, depth = [], [], 0
    in_bt = in_sq = False
    for ch in s:
        if in_bt:
            cur.append(ch)
            in_bt = ch != "`"
        elif in_sq:
            cur.append(ch)
            in_sq = ch != "'"
        elif ch == "`":
            in_bt = True
            cur.append(ch)
        elif ch == "'":
            in_sq = True
            cur.append(ch)
        elif ch == "(":
            depth += 1
            cur.append(ch)
        elif ch == ")":
            depth -= 1
            cur.append(ch)
        elif ch == "," and depth == 0:
            args.append("".join(cur))
            cur = []
        else:
            cur.append(ch)
    args.append("".join(cur))
    return [a.strip() for a in args]


# Monday epoch: 1900-01-01 was a Monday. With n = datediff(d, epoch) + 1, the
# count of weekdays in (epoch, d] is full_weeks*5 + min(remainder, 5). The
# difference of two such counts is the exact number of Mon-Fri days in (b, a],
# matching datediff's half-open convention (verified vs brute force over 13k
# date pairs, both directions). No holiday calendar.
_WD_EPOCH = "DATE'1900-01-01'"


def _working_day_diff(a, b):
    def wd(d):
        n = f"(datediff({d}, {_WD_EPOCH}) + 1)"
        return f"(({n} div 7) * 5 + least({n} % 7, 5))"
    return f"({wd(a)} - {wd(b)})"


def _rewrite_func(s, name, render):
    """Find calls to `name(...)` (word-boundaried, case-insensitive) and replace
    each with render(args), where args is the top-level argument list. Balanced-
    paren scanning handles nested calls. If render returns None the call is left
    untouched. A bare `name` not followed by `(` (e.g. a type keyword) is skipped."""
    token = name.lower()
    low = s.lower()
    out, i = [], 0
    while True:
        j = low.find(token, i)
        if j == -1:
            out.append(s[i:])
            return "".join(out)
        if j > 0 and (s[j - 1].isalnum() or s[j - 1] == "_"):
            out.append(s[i:j + len(token)])
            i = j + len(token)
            continue
        k = j + len(token)
        while k < len(s) and s[k] in " \t":
            k += 1
        if k >= len(s) or s[k] != "(":
            out.append(s[i:k])
            i = k
            continue
        depth, m, in_bt, in_sq = 0, k, False, False
        while m < len(s):
            ch = s[m]
            if in_bt:
                in_bt = ch != "`"
            elif in_sq:
                in_sq = ch != "'"
            elif ch == "`":
                in_bt = True
            elif ch == "'":
                in_sq = True
            elif ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
                if depth == 0:
                    break
            m += 1
        args = _split_top_args(s[k + 1:m])
        rep = render(args)
        out.append(s[i:j])
        out.append(rep if rep is not None else s[j:m + 1])
        i = m + 1


def _expand_working_day_diff(s):
    """DATE_WORKING_DIFF(a, b) -> exact business-day formula."""
    return _rewrite_func(s, "date_working_diff",
                         lambda a: _working_day_diff(a[0], a[1]) if len(a) == 2 else None)


def _expand_datetime(s):
    """MySQL DATETIME(x) cast -> Spark CAST(x AS TIMESTAMP)."""
    return _rewrite_func(s, "datetime",
                         lambda a: f"CAST({a[0]} AS TIMESTAMP)" if len(a) == 1 else None)


def transpile_expr(expr):
    """Rewrite Domo Beast Mode / MySQL dialect in a scalar expression (or SQL
    body) into Spark SQL. Conservative: only deterministic, well-understood
    rewrites. Patterns with no safe automatic translation are left untouched
    for _dialect_note to flag."""
    if not expr:
        return expr
    out = _strip_line_comments(expr)
    out = _expand_working_day_diff(out)
    out = _expand_datetime(out)
    for pat, repl in _REGEX_FIXUPS:
        out = pat.sub(repl, out)
    out = _DATE_FORMAT_RE.sub(
        lambda m: f"date_format({m.group(1)},'{_mysql_fmt_to_spark(m.group(2))}')", out)
    for pat, repl in _FUNC_FIXUPS:
        out = pat.sub(repl, out)
    return out


def _where_from_filterlist(filter_list):
    parts = []
    for f in filter_list or []:
        # Domo Filter tiles come in two shapes: a structured leftField/operator/
        # rightValue, or a Beast Mode boolean `expression` (then the structured
        # fields are all null). Prefer the expression and transpile its dialect.
        expr = f.get("expression")
        if expr:
            parts.append(f"({transpile_expr(expr)})")
            continue
        left = f.get("leftField")
        if not left:
            continue  # no predicate to emit (avoids `None` = NULL)
        op = _OPS.get((f.get("operator") or "").upper(), "=")
        if op in ("IS NULL", "IS NOT NULL"):
            parts.append(f"`{left}` {op}")
        elif f.get("rightField"):
            parts.append(f"`{left}` {op} `{f['rightField']}`")
        else:
            parts.append(f"`{left}` {op} {_lit(f.get('rightValue'))}")
    return " AND ".join(parts) if parts else "1=1"
