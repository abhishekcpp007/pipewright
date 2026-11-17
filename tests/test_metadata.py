from __future__ import annotations

from pw.metadata import Metadata, new_run_id
from pw.warehouse import stable_sk


def test_run_ids_do_not_collide():
    assert len({new_run_id() for _ in range(200)}) == 200


def test_watermarks_round_trip_and_accumulate(tmp_path):
    m = Metadata(tmp_path / "m.duckdb")
    assert m.watermark("orders") is None
    m.set_watermark("orders", "2026-10-01 00:00:01", 10)
    m.set_watermark("orders", "2026-10-02 00:00:01", 5)
    assert m.watermark("orders") == "2026-10-02 00:00:01"
    assert m.con.execute("SELECT rows_seen FROM watermarks WHERE table_name = 'orders'"
                         ).fetchone()[0] == 15
    m.reset_watermark("orders")
    assert m.watermark("orders") is None
    m.close()


def test_the_manifest_recognises_a_batch_it_already_took(tmp_path):
    m = Metadata(tmp_path / "m.duckdb")
    assert not m.manifest_has("orders", "abc")
    m.add_manifest("orders", "2026-10-01", "r1", "/tmp/x.parquet", 10, 100, "abc")
    assert m.manifest_has("orders", "abc")
    assert not m.manifest_has("payments", "abc")
    m.close()


def test_unfinished_nodes_are_what_a_resume_would_pick_up(tmp_path):
    m = Metadata(tmp_path / "m.duckdb")
    r1 = m.start_run("run")
    m.record_task(r1, "silver_orders", "success", 1, 0.1, 10)
    m.record_task(r1, "fct_orders", "failed", 2, 0.2, None, "boom")
    m.record_task(r1, "agg_customer_ltv", "upstream_failed", 0, 0.0)
    m.finish_run(r1, "failed")
    assert sorted(m.last_failed_nodes()) == ["agg_customer_ltv", "fct_orders"]
    r2 = m.start_run("run")
    m.record_task(r2, "fct_orders", "success", 1, 0.1, 10)
    m.finish_run(r2, "success")
    assert m.last_failed_nodes() == []
    m.close()


def test_dq_results_and_model_stats_are_kept_for_history(tmp_path):
    m = Metadata(tmp_path / "m.duckdb")
    r = m.start_run("run")
    m.record_dq(r, "silver_orders", "one row per order", "error", True, 0, 0, "")
    m.record_model(r, "silver_orders", "incremental", 100, 20, 0.5)
    assert m.con.execute("SELECT COUNT(*) FROM dq_results").fetchone()[0] == 1
    assert m.con.execute("SELECT rows_changed FROM model_stats").fetchone()[0] == 20
    m.close()


def test_surrogate_keys_are_reproducible_and_do_not_collide():
    assert stable_sk(7, "2026-10-01 00:00:00") == stable_sk(7, "2026-10-01 00:00:00")
    assert stable_sk(7, "2026-10-01 00:00:00") != stable_sk(7, "2026-10-01 00:00:01")
    assert stable_sk(7, "2026-10-01") != stable_sk(8, "2026-10-01")
    keys = {stable_sk(i, f"2026-10-{d:02d}") for i in range(500) for d in range(1, 15)}
    assert len(keys) == 500 * 14
    assert all(k > 0 for k in keys)
