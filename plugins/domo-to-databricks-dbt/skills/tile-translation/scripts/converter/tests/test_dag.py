from domo_to_dbt.dag import topo_sort, upstream_views

def test_topo_sort_orders_deps_first():
    actions = [
        {"id": "c", "type": "Filter", "dependsOn": ["b"]},
        {"id": "a", "type": "LoadFromVault"},
        {"id": "b", "type": "ExpressionEvaluator", "dependsOn": ["a"]},
    ]
    order = [a["id"] for a in topo_sort(actions)]
    assert order.index("a") < order.index("b") < order.index("c")

def test_topo_sort_handles_inputs_list_and_input_str():
    actions = [
        {"id": "j", "type": "UnionAll", "inputs": ["a", "b"]},
        {"id": "a", "type": "LoadFromVault"},
        {"id": "b", "type": "LoadFromVault"},
        {"id": "k", "type": "SelectValues", "input": "j"},
    ]
    order = [a["id"] for a in topo_sort(actions)]
    assert order.index("a") < order.index("j")
    assert order.index("b") < order.index("j")
    assert order.index("j") < order.index("k")

def test_upstream_views_resolves_in_order():
    action = {"id": "j", "dependsOn": ["a", "b"]}
    assert upstream_views(action, {"a": "src_a", "b": "src_b"}) == ["src_a", "src_b"]
