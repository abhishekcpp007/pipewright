from __future__ import annotations

import sqlite3

import duckdb

from pw.config import paths
from pw.extract import SOURCE_TABLES, day_bounds, extract_all, extract_table
from pw.lake import content_checksum, partitions
from pw.metadata import Metadata


def run_one_day(home, day, mode="cdc"):
    p = paths(home).prepare()
    meta = Metadata(p.metadata)
    run_id = meta.start_run("test")
    res = extract_all(p.source_db, meta, run_id, p.lake, day, day_bounds(day)[1], mode=mode)
    meta.finish_run(run_id, "success")
    return p, meta, res


def test_the_first_extract_writes_one_partition_per_table(fresh):
    p, meta, res = run_one_day(fresh, "2026-10-01")
    assert {r.table for r in res} == {s.name for s in SOURCE_TABLES}
    assert all(r.rows > 0 for r in res)
    for src in SOURCE_TABLES:
        assert partitions(p.lake, src.name) == ["2026-10-01"]
    assert len(meta.manifest_rows()) == len(SOURCE_TABLES)
    meta.close()


def test_the_watermark_moves_and_the_next_day_only_takes_new_rows(fresh):
    p, meta, _ = run_one_day(fresh, "2026-10-01")
    wm = meta.watermark("orders")
    assert wm.startswith("2026-10-01")
    meta.close()
    p, meta, second = run_one_day(fresh, "2026-10-02")
    assert meta.watermark("orders") > wm
    rows = {r.table: r.rows for r in second}
    con = duckdb.connect()
    landed = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{p.lake}/bronze/orders/**/*.parquet', "
        f"union_by_name = true, hive_partitioning = 1) WHERE ingest_date = DATE '2026-10-02'"
    ).fetchone()[0]
    assert landed == rows["orders"]
    meta.close()


def test_no_temp_files_survive_a_write(fresh):
    p, meta, _ = run_one_day(fresh, "2026-10-01")
    assert list(p.lake.rglob("*.tmp")) == []
    meta.close()


def test_the_window_upper_bound_is_respected(fresh):
    p, meta, _ = run_one_day(fresh, "2026-10-01")
    con = duckdb.connect()
    newest = con.execute(
        f"SELECT MAX(updated_at) FROM read_parquet('{p.lake}/bronze/orders/**/*.parquet', "
        f"hive_partitioning = 1)").fetchone()[0]
    assert newest < "2026-10-02"
    meta.close()


def test_replaying_a_window_already_committed_is_refused_by_the_checksum(fresh):
    p, meta, _ = run_one_day(fresh, "2026-10-01")
    files_before = len(list(p.lake.rglob("*.parquet")))
    # Rewind the watermark the way a crash before the watermark commit would have left it.
    meta.reset_watermark()
    con = sqlite3.connect(f"file:{p.source_db}?mode=ro", uri=True)
    run_id = meta.start_run("replay")
    again = [extract_table(con, meta, src, run_id, p.lake, "2026-10-01",
                           day_bounds("2026-10-01")[1]) for src in SOURCE_TABLES]
    con.close()
    assert all(r.skipped_duplicate for r in again), [(r.table, r.skipped_duplicate) for r in again]
    assert len(list(p.lake.rglob("*.parquet"))) == files_before
    meta.close()


def test_the_checksum_is_over_the_data_not_the_file(fresh):
    import pyarrow as pa
    t1 = pa.table({"a": [1, 2, 3]})
    t2 = pa.table({"a": [1, 2, 3]})
    t3 = pa.table({"a": [1, 2, 4]})
    assert content_checksum(t1) == content_checksum(t2)
    assert content_checksum(t1) != content_checksum(t3)


def test_the_polling_mode_reads_the_live_table_instead_of_the_feed(fresh):
    p, meta, cdc = run_one_day(fresh, "2026-10-01")
    meta.close()
    import shutil
    other = fresh.parent / "poll"
    other.mkdir()
    (other / "source").mkdir()
    shutil.copy(p.source_db, paths(other).source_db)
    _, meta2, poll = run_one_day(other, "2026-10-01", mode="poll")
    by_table = {r.table: r.rows for r in cdc}
    poll_rows = {r.table: r.rows for r in poll}
    assert poll_rows["orders"] < by_table["orders"]
    meta2.close()


def test_a_column_added_later_unions_with_the_files_written_before_it(fresh):
    p = paths(fresh)
    from pw.source import generate
    generate(p.source_db, days=9)
    for i in range(1, 10):
        day = f"2026-10-{i:02d}"
        _, meta, _ = run_one_day(fresh, day)
        meta.close()
    con = duckdb.connect()
    cols = [d[0] for d in con.execute(
        f"SELECT * FROM read_parquet('{p.lake}/bronze/orders/**/*.parquet', "
        f"union_by_name = true, hive_partitioning = 1) LIMIT 0").description]
    assert "promo_code" in cols
    early_nulls = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{p.lake}/bronze/orders/**/*.parquet', "
        f"union_by_name = true, hive_partitioning = 1) "
        f"WHERE ingest_date < DATE '2026-10-09' AND promo_code IS NOT NULL").fetchone()[0]
    assert early_nulls == 0
