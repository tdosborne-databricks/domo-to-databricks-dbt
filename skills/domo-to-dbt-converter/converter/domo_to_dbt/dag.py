"""Dependency ordering for a Domo DataFlow tile graph."""


def _deps(action):
    """Return the list of upstream action ids for a tile, normalizing the
    three shapes Domo emits: dependsOn (list), inputs (list), input (str)."""
    if action.get("dependsOn"):
        return list(action["dependsOn"])
    if action.get("inputs"):
        return list(action["inputs"])
    if action.get("input"):
        return [action["input"]]
    return []


def topo_sort(actions):
    """Return actions in dependency order (deps before dependents)."""
    by_id = {a["id"]: a for a in actions}
    visited, ordered = set(), []

    def visit(aid):
        if aid in visited or aid not in by_id:
            return
        visited.add(aid)
        for dep in _deps(by_id[aid]):
            visit(dep)
        ordered.append(by_id[aid])

    for a in actions:
        visit(a["id"])
    return ordered


def upstream_views(action, id_to_view):
    """Resolve an action's input ids to their assigned view names, in order."""
    return [id_to_view.get(dep, "unknown") for dep in _deps(action)]
