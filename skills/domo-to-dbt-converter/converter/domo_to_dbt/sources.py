"""Resolve LoadFromVault inputs to UC tables, infer needed columns from tile refs."""
from .common import _sanitize
from .dag import _deps

# tile keys whose list items carry column-name fields
_FIELD_KEYS = ("filterList", "fields", "groups", "expressions", "calculations",
               "additions", "groupRules", "orderRules")
_FIELD_NAMES = ("leftField", "rightField", "name", "fieldName", "column",
                "fieldA", "fieldB", "sourceField", "destField")


def source_ref(dataset_name):
    return f"{{{{ source('domo', '{_sanitize(dataset_name)}') }}}}"


def _fields_in_action(a):
    out = set()
    for k in _FIELD_KEYS:
        for it in a.get(k, []) or []:
            if isinstance(it, dict):
                for fk in _FIELD_NAMES:
                    if it.get(fk):
                        out.add(it[fk])
            elif isinstance(it, str):
                out.add(it)
    for k in ("keys1", "keys2"):
        out.update(a.get(k, []) or [])
    return out


def _downstream_ids(start_id, by_id, children):
    seen, stack = set(), [start_id]
    while stack:
        cur = stack.pop()
        for c in children.get(cur, []):
            if c not in seen:
                seen.add(c)
                stack.append(c)
    return seen


def infer_source_columns(flow):
    actions = flow["actions"]
    by_id = {a["id"]: a for a in actions}
    children = {}
    for a in actions:
        for dep in _deps(a):
            children.setdefault(dep, []).append(a["id"])
    result = {}
    for a in actions:
        if a["type"] != "LoadFromVault":
            continue
        cols = set()
        for cid in _downstream_ids(a["id"], by_id, children):
            cols |= _fields_in_action(by_id[cid])
        result[a["id"]] = sorted(cols)
    return result


def resolve_sources(flow, dataset_mapping, overrides):
    overrides = overrides or {}
    cols_by_load = infer_source_columns(flow)
    sources = []
    for a in flow["actions"]:
        if a["type"] != "LoadFromVault":
            continue
        ds_id = str(a.get("dataSourceId", ""))
        name = _sanitize(dataset_mapping.get(ds_id, f"source_{ds_id}"))
        raw_name = dataset_mapping.get(ds_id, name)
        catalog_table = overrides.get(ds_id) or overrides.get(raw_name) or overrides.get(name)
        sources.append({
            "name": name,
            "dataset_id": ds_id,
            "catalog_table": catalog_table,
            "inferred_columns": cols_by_load.get(a["id"], []),
        })
    return {"sources": sources}
