from __future__ import annotations

import pytest

from pw.dag import Model, load_model
from pw.warehouse import Warehouse, sk_name, strip_config


def model(tmp_path, name, sql) -> Model:
    f = tmp_path / f"{name}.sql"
    f.write_text(sql)
    return load_model(f)


@pytest.fixture
def wh(tmp_path) -> Warehouse:
    w = Warehouse(tmp_path / "w.duckdb", tmp_path / "lake")
    w.con.execute("CREATE TABLE seed (id INTEGER, val VARCHAR, updated_at TIMESTAMP)")
    return w


def seed(wh, rows):
    wh.con.execute("DELETE FROM seed")
    wh.con.executemany("INSERT INTO seed VALUES (?, ?, ?)", rows)


def test_a_view_is_not_materialised(wh, tmp_path):
    m = model(tmp_path, "v", "-- config: materialized=view\nselect * from seed")
    seed(wh, [(1, "a", "2026-01-01")])
    wh.build(m)
    seed(wh, [(1, "a", "2026-01-01"), (2, "b", "2026-01-02")])
    assert wh.count("v") == 2


def test_a_table_is_a_snapshot(wh, tmp_path):
    m = model(tmp_path, "t", "-- config: materialized=table\nselect * from seed")
    seed(wh, [(1, "a", "2026-01-01")])
    wh.build(m)
    seed(wh, [(1, "a", "2026-01-01"), (2, "b", "2026-01-02")])
    assert wh.count("t") == 1
    wh.build(m)
    assert wh.count("t") == 2


def test_the_first_incremental_build_creates_the_table(wh, tmp_path):
    m = model(tmp_path, "inc",
              "-- config: materialized=incremental, unique_key=id\nselect * from seed")
    seed(wh, [(1, "a", "2026-01-01"), (2, "b", "2026-01-01")])
    r = wh.build(m)
    assert r.incremental is False
    assert wh.count("inc") == 2


def test_a_merge_replaces_the_row_it_matches(wh, tmp_path):
    m = model(tmp_path, "inc",
              "-- config: materialized=incremental, unique_key=id\nselect * from seed")
    seed(wh, [(1, "a", "2026-01-01")])
    wh.build(m)
    seed(wh, [(1, "a2", "2026-01-02"), (2, "b", "2026-01-02")])
    r = wh.build(m)
    assert r.incremental is True
    assert wh.con.execute("SELECT val FROM inc WHERE id = 1").fetchone()[0] == "a2"
    assert wh.count("inc") == 2


def test_the_watermark_guard_refuses_a_stale_row(wh, tmp_path):
    m = model(tmp_path, "inc", "-- config: materialized=incremental, unique_key=id, "
                               "watermark=updated_at\nselect * from seed")
    seed(wh, [(1, "new", "2026-01-05")])
    wh.build(m)
    seed(wh, [(1, "old", "2026-01-02")])
    r = wh.build(m)
    assert wh.con.execute("SELECT val FROM inc WHERE id = 1").fetchone()[0] == "new"
    assert r.notes["rows_rejected_as_stale"] == 1


def test_without_a_watermark_the_last_write_wins_even_if_it_is_older(wh, tmp_path):
    m = model(tmp_path, "inc",
              "-- config: materialized=incremental, unique_key=id\nselect * from seed")
    seed(wh, [(1, "new", "2026-01-05")])
    wh.build(m)
    seed(wh, [(1, "old", "2026-01-02")])
    wh.build(m)
    assert wh.con.execute("SELECT val FROM inc WHERE id = 1").fetchone()[0] == "old"


def test_full_refresh_throws_the_incremental_table_away(wh, tmp_path):
    m = model(tmp_path, "inc",
              "-- config: materialized=incremental, unique_key=id\nselect * from seed")
    seed(wh, [(1, "a", "2026-01-01"), (2, "b", "2026-01-01")])
    wh.build(m)
    seed(wh, [(3, "c", "2026-01-02")])
    wh.build(m, full_refresh=True)
    assert [r[0] for r in wh.con.execute("SELECT id FROM inc").fetchall()] == [3]


# ---- slowly changing dimension

SCD2 = ("-- config: materialized=scd2, unique_key=id, check_cols=val, watermark=updated_at\n"
        "select * from seed")


def dim(wh):
    return wh.con.execute(
        "SELECT id, val, valid_from, valid_to, is_current, version FROM d "
        "ORDER BY id, valid_from").fetchall()


def test_a_new_key_opens_version_one(wh, tmp_path):
    m = model(tmp_path, "d", SCD2)
    seed(wh, [(1, "a", "2026-01-01")])
    r = wh.build(m)
    rows = dim(wh)
    assert len(rows) == 1
    assert rows[0][1] == "a" and rows[0][4] is True and rows[0][5] == 1
    assert str(rows[0][3]).startswith("9999")
    assert r.notes["new_keys"] == 1


def test_a_changed_attribute_closes_the_old_version(wh, tmp_path):
    m = model(tmp_path, "d", SCD2)
    seed(wh, [(1, "a", "2026-01-01")])
    wh.build(m)
    seed(wh, [(1, "b", "2026-01-05")])
    r = wh.build(m)
    rows = dim(wh)
    assert len(rows) == 2
    assert rows[0][4] is False and str(rows[0][3]) == "2026-01-05 00:00:00"
    assert rows[1][1] == "b" and rows[1][4] is True and rows[1][5] == 2
    assert r.notes["new_versions"] == 1


def test_an_unchanged_attribute_does_not_make_a_version(wh, tmp_path):
    m = model(tmp_path, "d", SCD2)
    seed(wh, [(1, "a", "2026-01-01")])
    wh.build(m)
    seed(wh, [(1, "a", "2026-01-05")])
    r = wh.build(m)
    assert len(dim(wh)) == 1
    assert r.notes["unchanged"] == 1


def test_a_version_older_than_the_open_one_is_dropped(wh, tmp_path):
    m = model(tmp_path, "d", SCD2)
    seed(wh, [(1, "a", "2026-01-10")])
    wh.build(m)
    seed(wh, [(1, "stale", "2026-01-02")])
    r = wh.build(m)
    assert [row[1] for row in dim(wh)] == ["a"]
    assert r.notes["late_versions_dropped"] == 1


def test_two_changes_in_one_batch_still_leave_one_current_row(wh, tmp_path):
    m = model(tmp_path, "d", SCD2)
    seed(wh, [(1, "a", "2026-01-01"), (1, "b", "2026-01-02"), (1, "c", "2026-01-03")])
    wh.build(m)
    rows = dim(wh)
    assert [r[1] for r in rows] == ["a", "b", "c"]
    assert [r[4] for r in rows] == [False, False, True]
    assert [r[5] for r in rows] == [1, 2, 3]
    assert str(rows[0][3]) == "2026-01-02 00:00:00"


def test_validity_windows_are_contiguous_and_never_overlap(wh, tmp_path):
    m = model(tmp_path, "d", SCD2)
    seed(wh, [(1, "a", "2026-01-01")])
    wh.build(m)
    for day, val in (("2026-01-04", "b"), ("2026-01-09", "c")):
        seed(wh, [(1, val, day)])
        wh.build(m)
    overlaps = wh.con.execute(
        "SELECT COUNT(*) FROM (SELECT valid_to, LEAD(valid_from) OVER (ORDER BY valid_from) nxt "
        "FROM d) WHERE nxt IS NOT NULL AND valid_to <> nxt").fetchone()[0]
    assert overlaps == 0


def test_surrogate_keys_are_unique_across_versions(wh, tmp_path):
    m = model(tmp_path, "d", SCD2)
    for i, (val, day) in enumerate([("a", "2026-01-01"), ("b", "2026-01-02"),
                                    ("c", "2026-01-03")]):
        seed(wh, [(1, val, day), (2, val, day)])
        wh.build(m)
    n, distinct = wh.con.execute("SELECT COUNT(*), COUNT(DISTINCT id_sk) FROM d").fetchone()
    assert n == distinct == 6


def test_scd2_needs_its_columns_in_the_select(wh, tmp_path):
    m = model(tmp_path, "d", "-- config: materialized=scd2, unique_key=id, check_cols=missing\n"
                             "select * from seed")
    seed(wh, [(1, "a", "2026-01-01")])
    with pytest.raises(ValueError, match="needs column"):
        wh.build(m)


# ---- rendering

def test_the_incremental_block_is_dropped_on_a_first_build(wh, tmp_path):
    m = model(tmp_path, "inc", "-- config: materialized=incremental, unique_key=id\n"
                               "select * from seed\n{% if is_incremental() %}"
                               "where updated_at = '{{ batch_date }}'{% endif %}")
    seed(wh, [(1, "a", "2026-01-01"), (2, "b", "2026-01-02")])
    assert "where" not in wh.render(m, "2026-01-02", incremental=False)
    assert "where" in wh.render(m, "2026-01-02", incremental=True)
    wh.build(m, batch_date="2026-01-02")
    assert wh.count("inc") == 2
    seed(wh, [(3, "c", "2026-01-03")])
    wh.build(m, batch_date="2026-01-02")
    assert wh.count("inc") == 2


def test_safe_col_fills_a_missing_column_with_a_typed_null(wh, tmp_path):
    m = model(tmp_path, "s", "-- config: materialized=view\n"
                             "select id, {{ safe_col('seed', 'nope', 'varchar') }} as nope,"
                             " {{ safe_col('seed', 'val', 'varchar') }} as val from seed")
    seed(wh, [(1, "a", "2026-01-01")])
    wh.build(m)
    assert wh.con.execute("SELECT nope, val FROM s").fetchone() == (None, "a")


def test_an_unresolved_template_is_an_error_not_a_silent_pass(wh, tmp_path):
    m = model(tmp_path, "s", "-- config: materialized=view\nselect {{ mystery }} from seed")
    with pytest.raises(ValueError, match="unresolved template"):
        wh.render(m, None, False)


def test_strip_config_leaves_the_query_alone():
    body = strip_config("-- config: materialized=view\n-- a note\n\nselect 1\n-- trailing")
    assert body.startswith("select 1")
    assert "-- trailing" in body


@pytest.mark.parametrize("key,expected", [("customer_id", "customer_sk"), ("sku", "sku_sk")])
def test_surrogate_key_naming(key, expected):
    assert sk_name(key) == expected
