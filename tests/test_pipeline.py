"""End to end. These are the tests that would catch a wrong number in a report."""
from __future__ import annotations

import json
import os

import duckdb
import pytest

from pw.config import paths
from pw.metadata import Metadata
from pw.quality import load_expectations, run_expectations
from pw.runner import backfill, run_pipeline

from .conftest import DAYS

BUSINESS_TABLES = ["silver_orders", "silver_order_items", "silver_payments", "fct_orders",
                   "agg_daily_city_revenue", "agg_customer_ltv"]


def con_for(home, read_only=True):
    return duckdb.connect(str(paths(home).warehouse), read_only=read_only)


def fingerprint(con, table, skip=("ingest_date",)):
    cols = [d[0] for d in con.execute(f"SELECT * FROM {table} LIMIT 0").description]
    keep = [c for c in cols if c not in skip]
    sel = ", ".join(f'"{c}"' for c in keep)
    rows = con.execute(f"SELECT {sel} FROM {table} ORDER BY ALL").fetchall()
    return len(rows), hash(tuple(map(str, rows)))


def test_every_model_got_built(built):
    con = con_for(built)
    from pw.dag import Graph
    for name in Graph.load().models:
        assert con.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0] >= 0
    con.close()


def test_every_expectation_passes_on_the_finished_warehouse(built):
    con = con_for(built)
    meta = Metadata(paths(built).metadata)
    checks = run_expectations(con, load_expectations(), None,
                              f"2026-10-{DAYS:02d}", meta)
    failed = [(c.model, c.name, c.observed) for c in checks
              if not c.passed and c.severity == "error"]
    assert failed == []
    con.close()
    meta.close()


def test_the_fact_has_one_row_per_order_and_loses_none_of_them(built):
    con = con_for(built)
    orders, fact = con.execute(
        "SELECT (SELECT COUNT(*) FROM silver_orders), (SELECT COUNT(*) FROM fct_orders)"
    ).fetchone()
    assert orders == fact
    assert con.execute("SELECT COUNT(*) FROM (SELECT order_id FROM fct_orders "
                       "GROUP BY 1 HAVING COUNT(*) > 1)").fetchone()[0] == 0
    con.close()


def test_the_duplicate_payments_are_collapsed_to_one_row_each(built):
    con = con_for(built)
    injected = json.loads((paths(built).injected).read_text())
    dupes = con.execute(
        "SELECT COUNT(*) FROM (SELECT payment_id FROM bronze_payments GROUP BY 1 "
        "HAVING COUNT(*) > 1)").fetchone()[0]
    assert dupes > 0, "the fixture should contain duplicate payment rows"
    assert con.execute("SELECT COUNT(*) FROM (SELECT payment_id FROM silver_payments "
                       "GROUP BY 1 HAVING COUNT(*) > 1)").fetchone()[0] == 0
    assert injected["duplicate_payment_rows"] > 0
    con.close()


def test_the_quarantine_accounts_for_the_rows_silver_dropped(built):
    con = con_for(built)
    injected = json.loads((paths(built).injected).read_text())
    by_reason = dict(con.execute(
        "SELECT reason, COUNT(*) FROM quarantine GROUP BY 1").fetchall())
    assert by_reason["non_positive_qty"] == injected["zero_qty_items"]
    assert by_reason["negative_amount"] == injected["negative_payment_amounts"]
    assert by_reason["unparseable_email"] == injected["bad_email_customers"]
    con.close()


def test_the_late_arriving_orders_are_the_ones_that_were_injected(built):
    con = con_for(built)
    injected = json.loads((paths(built).injected).read_text())
    flagged = con.execute(
        "SELECT COUNT(*) FROM fct_orders WHERE is_late_arriving").fetchone()[0]
    assert flagged == injected["late_arriving_orders"]
    con.close()


def test_attribution_uses_the_customer_as_they_were_at_order_time(built):
    con = con_for(built)
    moved = con.execute("""
        SELECT COUNT(*) FROM fct_orders f
        JOIN dim_customer d ON d.customer_id = f.customer_id AND d.is_current
        WHERE f.customer_city_at_order <> d.city
    """).fetchone()[0]
    assert moved > 0, "no customer moved city in the fixture, so this proves nothing"
    wrong = con.execute("""
        SELECT COUNT(*) FROM fct_orders f
        JOIN dim_customer d ON d.customer_id = f.customer_id
        WHERE NOT f.attribution_fell_back
          AND f.customer_sk = d.customer_sk
          AND (f.order_ts < d.valid_from OR f.order_ts >= d.valid_to)
    """).fetchone()[0]
    assert wrong == 0
    con.close()


def test_the_dimension_history_is_well_formed(built):
    con = con_for(built)
    assert con.execute("SELECT COUNT(*) FROM dim_customer WHERE version > 1").fetchone()[0] > 0
    bad = con.execute("""
        SELECT COUNT(*) FROM (
          SELECT customer_id, valid_to,
                 LEAD(valid_from) OVER (PARTITION BY customer_id ORDER BY valid_from) nxt
          FROM dim_customer)
        WHERE nxt IS NOT NULL AND valid_to <> nxt
    """).fetchone()[0]
    assert bad == 0
    con.close()


def test_running_the_same_batch_twice_changes_nothing(built_copy):
    before = {}
    con = con_for(built_copy)
    for t in BUSINESS_TABLES:
        before[t] = fingerprint(con, t)
    con.close()
    rep = run_pipeline(built_copy, batch_date=f"2026-10-{DAYS:02d}",
                       as_of=f"2026-10-{DAYS:02d}")
    assert rep.status == "success", rep.failures
    con = con_for(built_copy)
    for t in BUSINESS_TABLES:
        assert fingerprint(con, t) == before[t], f"{t} changed on a repeat run"
    con.close()


def test_a_backfill_over_days_already_processed_changes_nothing(built_copy):
    con = con_for(built_copy)
    before = {t: fingerprint(con, t) for t in BUSINESS_TABLES}
    con.close()
    reps = backfill(built_copy, start="2026-10-02", end=f"2026-10-{DAYS:02d}", quiet=True)
    assert all(r.status == "success" for r in reps), [r.failures for r in reps]
    con = con_for(built_copy)
    for t in BUSINESS_TABLES:
        assert fingerprint(con, t) == before[t], f"{t} changed after a backfill"
    con.close()


def test_a_full_rebuild_lands_on_the_same_numbers_as_the_daily_runs(built_copy):
    """The one that matters. If the incremental path drifts from the full rebuild, every
    number in every report is quietly wrong and nothing else in this suite would say so."""
    con = con_for(built_copy)
    before = {t: fingerprint(con, t, skip=("ingest_date", "first_seen_date"))
              for t in ["silver_payments", "fct_orders", "agg_daily_city_revenue",
                        "agg_customer_ltv"]}
    dim_before = con.execute(
        "SELECT customer_id, city, tier, version, valid_from, valid_to, is_current "
        "FROM dim_customer ORDER BY ALL").fetchall()
    con.close()

    rep = run_pipeline(built_copy, full_refresh=True, skip_extract=True,
                       as_of=f"2026-10-{DAYS:02d}")
    assert rep.status == "success", rep.failures

    con = con_for(built_copy)
    for t, fp in before.items():
        assert fingerprint(con, t, skip=("ingest_date", "first_seen_date")) == fp, \
            f"{t} differs between the incremental build and the full rebuild"
    dim_after = con.execute(
        "SELECT customer_id, city, tier, version, valid_from, valid_to, is_current "
        "FROM dim_customer ORDER BY ALL").fetchall()
    assert dim_after == dim_before
    con.close()


def test_a_transient_failure_is_retried(built_copy):
    os.environ["PW_FAIL_ONCE"] = "silver_orders"
    try:
        rep = run_pipeline(built_copy, batch_date=f"2026-10-{DAYS:02d}", retries=1,
                           as_of=f"2026-10-{DAYS:02d}")
    finally:
        del os.environ["PW_FAIL_ONCE"]
    assert rep.status == "success", rep.failures
    meta = Metadata(paths(built_copy).metadata)
    attempts = meta.con.execute(
        "SELECT attempts FROM tasks WHERE node = 'silver_orders' "
        "ORDER BY finished_at DESC LIMIT 1").fetchone()[0]
    meta.close()
    assert attempts == 2


def test_a_failed_model_stops_everything_downstream_of_it(built_copy):
    os.environ["PW_FAIL_ALWAYS"] = "silver_orders"
    try:
        rep = run_pipeline(built_copy, batch_date=f"2026-10-{DAYS:02d}", retries=0,
                           as_of=f"2026-10-{DAYS:02d}")
    finally:
        del os.environ["PW_FAIL_ALWAYS"]
    assert rep.status == "failed"
    assert ("silver_orders", "injected failure in silver_orders") in [
        (n, e) for n, e in rep.failures]
    assert "fct_orders" in rep.skipped
    assert "agg_daily_city_revenue" in rep.skipped
    assert "silver_payments" not in rep.skipped
    meta = Metadata(paths(built_copy).metadata)
    assert set(meta.last_failed_nodes()) >= {"silver_orders", "fct_orders"}
    meta.close()


def test_a_quality_failure_opens_the_circuit_too(built_copy, monkeypatch):
    """Corrupt silver_payments behind the pipeline's back, then prove gold does not move."""
    con = con_for(built_copy, read_only=False)
    row = con.execute("SELECT * FROM silver_payments LIMIT 1").fetchall()[0]
    con.execute("INSERT INTO silver_payments VALUES " +
                "(" + ", ".join("?" * len(row)) + ")", list(row))
    before = fingerprint(con, "agg_daily_city_revenue")
    con.close()

    rep = run_pipeline(built_copy, batch_date=f"2026-10-{DAYS:02d}",
                       selector="silver_payments+", skip_extract=True,
                       as_of=f"2026-10-{DAYS:02d}")
    assert rep.status == "failed"
    assert any("one row per payment id" in e for _, e in rep.failures)
    assert "fct_orders" in rep.skipped
    con = con_for(built_copy)
    assert fingerprint(con, "agg_daily_city_revenue") == before
    con.close()


def test_selecting_one_model_leaves_the_rest_alone(built_copy):
    con = con_for(built_copy)
    before = fingerprint(con, "fct_orders")
    con.close()
    rep = run_pipeline(built_copy, selector="agg_daily_city_revenue", skip_extract=True,
                       as_of=f"2026-10-{DAYS:02d}")
    assert [b.model for b in rep.built] == ["agg_daily_city_revenue"]
    con = con_for(built_copy)
    assert fingerprint(con, "fct_orders") == before
    con.close()


def test_revenue_only_counts_orders_that_reached_a_paying_state(built):
    con = con_for(built)
    leak = con.execute("SELECT COUNT(*) FROM fct_orders WHERE is_revenue "
                       "AND status IN ('placed', 'cancelled', 'refunded')").fetchone()[0]
    assert leak == 0
    agg, fact = con.execute("""
        SELECT (SELECT COALESCE(SUM(gross_paise), 0) FROM agg_daily_city_revenue),
               (SELECT COALESCE(SUM(gross_paise), 0) FROM fct_orders WHERE is_revenue)
    """).fetchone()
    assert agg == fact
    con.close()


@pytest.mark.parametrize("table", BUSINESS_TABLES)
def test_no_business_table_came_out_empty(built, table):
    con = con_for(built)
    assert con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] > 0
    con.close()
