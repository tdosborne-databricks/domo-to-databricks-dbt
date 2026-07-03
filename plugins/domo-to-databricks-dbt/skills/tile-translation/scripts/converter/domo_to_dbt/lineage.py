"""Column lineage for the tile DAG.

`produced_columns(action, in_cols)` returns the ordered list of columns a tile
outputs, given the columns available from its upstream(s). Tracking only what we
can name with certainty: source columns are untracked until a projection names
them, which is safe — the lineage is used to (a) decide when a compute tile
re-creates an existing column (so it can EXCEPT it instead of duplicating), and
(b) attribute referenced-but-unknown columns to sources. Both only need *known*
columns; an untracked source column simply isn't excepted (the self-reference
heuristic in tiles.py still covers the direct source-replace case).
"""


def _append_unique(base, names):
    out = list(base)
    for n in names:
        if n not in out:
            out.append(n)
    return out


def produced_columns(action, in_cols, dep_cols=None):
    t = action.get("type")
    incols = list(in_cols)

    if t == "ExpressionEvaluator":
        return _append_unique(incols, [e["fieldName"] for e in action.get("expressions", [])])
    if t == "DateCalculator":
        return _append_unique(incols, [c["fieldName"] for c in action.get("calculations", [])])
    if t == "WindowAction":
        return _append_unique(incols, [w["name"] for w in action.get("additions", [])])
    if t == "GroupBy":
        names = ([g["name"] for g in action.get("groups", [])]
                 + [f["name"] for f in action.get("fields", [])])
        return names
    if t == "SelectValues":
        # Select Columns is an explicit projection: it DROPS every column not listed.
        names = [(f.get("rename") or f.get("name"))
                 for f in action.get("fields", []) if not f.get("remove")]
        return names if names else incols
    if t == "Metadata":
        # Alter Columns modifies LISTED columns and PASSES THROUGH the rest (mirror
        # tiles._alter_columns). Returning only the listed columns would make downstream joins
        # undercount their inputs and skip the AMBIGUOUS_REFERENCE dedup. Match case-insensitively
        # (SQL identifiers are case-insensitive). When upstream is untracked (incols empty) this
        # degrades to the old behavior: the listed field names.
        fields = action.get("fields", [])
        removed = {f["name"].lower() for f in fields if f.get("remove") and f.get("name")}
        renames = {f["name"].lower(): f.get("rename")
                   for f in fields if f.get("rename") and f.get("name")}
        out, seen = [], set()
        for c in incols:
            cl = (c or "").lower()
            if cl in removed:
                continue
            nm = renames.get(cl, c)
            if nm.lower() not in seen:
                out.append(nm)
                seen.add(nm.lower())
        for f in fields:                       # columns named only in config (added/computed)
            if f.get("remove"):
                continue
            nm = f.get("rename") or f.get("name")
            if nm and nm.lower() not in seen:
                out.append(nm)
                seen.add(nm.lower())
        return out if out else incols
    if t == "Normalizer":
        fields = action.get("fields", [])
        srcs = {f["sourceField"] for f in fields}
        tf = action.get("typefield", "type")
        dest = fields[0]["destField"] if fields else "value"
        kept = [c for c in incols if c not in srcs]
        return _append_unique(kept, [tf, dest])
    if t == "MergeJoin":
        # Mirror the emitted SQL exactly (tiles.m_join): keep ALL left columns, then append
        # right columns except the join keys (keys2) and any name already on the left. Doing
        # this on a name-deduped *union* is wrong -- a name may live on the left, the right,
        # or both, and the union can't tell them apart, so we'd drop a surviving left column
        # (COLUMN_ALREADY_EXISTS downstream) or keep a dropped right one. When both branches'
        # columns are tracked, use them; otherwise fall back to the union heuristic.
        if dep_cols and len(dep_cols) >= 2 and dep_cols[0] and dep_cols[1]:
            left, right = list(dep_cols[0]), list(dep_cols[1])
            k2 = {c.lower() for c in action.get("keys2", [])}
            lset = {(c or "").lower() for c in left}
            out = list(left)
            for c in right:
                cl = (c or "").lower()
                if cl not in k2 and cl not in lset:
                    out.append(c)
            return out
        k1 = {c.lower() for c in action.get("keys1", [])}
        drop = {c.lower() for c in action.get("keys2", []) if c.lower() not in k1}
        return [c for c in incols if (c or "").lower() not in drop]
    if t == "LoadFromVault":
        return []
    # Filter, Unique, UnionAll, PublishToVault, SQL (opaque), unknown -> passthrough
    return incols
