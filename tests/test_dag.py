from __future__ import annotations

import pytest

from pw.dag import Graph, load_model, parse_config


def test_every_shipped_model_parses():
    g = Graph.load()
    assert len(g.models) >= 16
    assert set(g.models) >= {"bronze_orders", "silver_orders", "dim_customer", "fct_orders"}


def test_config_header_parsing():
    cfg = parse_config("-- config: materialized=incremental, unique_key=id, tags=silver\nselect 1")
    assert cfg == {"materialized": "incremental", "unique_key": "id", "tags": "silver"}


def test_refs_come_from_the_query_not_a_list():
    g = Graph.load()
    assert g.models["silver_orders"].refs == ["bronze_orders"]
    assert "dim_customer" in g.models["fct_orders"].refs


def test_topological_order_puts_parents_first():
    g = Graph.load()
    pos = {n: i for i, n in enumerate(g.order)}
    for name, m in g.models.items():
        for r in m.refs:
            assert pos[r] < pos[name], f"{r} must build before {name}"


def test_layers_are_disjoint_and_cover_everything():
    g = Graph.load()
    layers = g.layers()
    flat = [n for layer in layers for n in layer]
    assert sorted(flat) == sorted(g.order)
    for i, layer in enumerate(layers):
        for n in layer:
            for r in g.models[n].refs:
                assert any(r in earlier for earlier in layers[:i])


def test_cycle_is_rejected(tmp_path):
    (tmp_path / "a.sql").write_text("-- config: materialized=view\nselect * from {{ ref('b') }}")
    (tmp_path / "b.sql").write_text("-- config: materialized=view\nselect * from {{ ref('a') }}")
    with pytest.raises(ValueError, match="cycle"):
        Graph.load(tmp_path)


def test_ref_to_a_model_that_does_not_exist_is_rejected(tmp_path):
    (tmp_path / "a.sql").write_text("-- config: materialized=view\nselect * from {{ ref('nope') }}")
    with pytest.raises(ValueError, match="do not exist"):
        Graph.load(tmp_path)


def test_unknown_materialization_is_rejected(tmp_path):
    f = tmp_path / "a.sql"
    f.write_text("-- config: materialized=cube\nselect 1")
    with pytest.raises(ValueError, match="unknown materialization"):
        load_model(f)


def test_incremental_without_a_key_is_rejected(tmp_path):
    f = tmp_path / "a.sql"
    f.write_text("-- config: materialized=incremental\nselect 1")
    with pytest.raises(ValueError, match="needs a unique_key"):
        load_model(f)


def test_scd2_without_check_cols_is_rejected(tmp_path):
    f = tmp_path / "a.sql"
    f.write_text("-- config: materialized=scd2, unique_key=id\nselect 1")
    with pytest.raises(ValueError, match="needs check_cols"):
        load_model(f)


@pytest.mark.parametrize("selector,expected", [
    ("fct_orders", {"fct_orders"}),
    ("tag:gold", {"fct_orders", "agg_customer_ltv", "agg_daily_city_revenue"}),
])
def test_selection(selector, expected):
    assert set(Graph.load().select(selector)) == expected


def test_upstream_selection_includes_the_whole_chain():
    picked = set(Graph.load().select("+fct_orders"))
    assert {"bronze_orders", "silver_orders", "dim_customer", "fct_orders"} <= picked
    assert "agg_customer_ltv" not in picked


def test_downstream_selection_stops_at_the_leaves():
    picked = set(Graph.load().select("silver_payments+"))
    assert "fct_orders" in picked and "agg_daily_city_revenue" in picked
    assert "bronze_payments" not in picked


def test_unknown_model_in_a_selector_raises():
    with pytest.raises(KeyError):
        Graph.load().select("not_a_model")


def test_graph_renders_every_node_in_both_formats():
    g = Graph.load()
    dot, mermaid = g.to_dot(), g.to_mermaid()
    for n in g.models:
        assert n in dot and n in mermaid
