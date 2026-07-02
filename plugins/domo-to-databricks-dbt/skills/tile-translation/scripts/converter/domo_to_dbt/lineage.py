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


def produced_columns(action, in_cols):
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
    if t in ("SelectValues", "Metadata"):
        names = [(f.get("rename") or f.get("name"))
                 for f in action.get("fields", []) if not f.get("remove")]
        return names if names else incols
    if t == "Normalizer":
        fields = action.get("fields", [])
        srcs = {f["sourceField"] for f in fields}
        tf = action.get("typefield", "type")
        dest = fields[0]["destField"] if fields else "value"
        kept = [c for c in incols if c not in srcs]
        return _append_unique(kept, [tf, dest])
    if t == "MergeJoin":
        # in_cols arrives already unioned across both sides; Domo drops the right
        # side's join-key columns (kept on the left).
        k2 = set(action.get("keys2", []))
        return [c for c in incols if c not in k2]
    if t == "LoadFromVault":
        return []
    # Filter, Unique, UnionAll, PublishToVault, SQL (opaque), unknown -> passthrough
    return incols
