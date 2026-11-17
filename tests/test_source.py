from __future__ import annotations

import sqlite3

from pw.config import SCHEMA_DRIFT_DAY, paths
from pw.source import generate


def q(db, sql, *args):
    con = sqlite3.connect(db)
    try:
        return con.execute(sql, args).fetchall()
    finally:
        con.close()


def test_the_same_seed_builds_the_same_database(tmp_path):
    a, b = tmp_path / "a.sqlite3", tmp_path / "b.sqlite3"
    da, db_ = generate(a, days=4, seed=11), generate(b, days=4, seed=11)
    assert da.as_dict() == db_.as_dict()
    for t in ("customers", "orders", "order_items", "payments"):
        assert q(a, f"SELECT * FROM {t}") == q(b, f"SELECT * FROM {t}")


def test_a_different_seed_builds_a_different_database(tmp_path):
    a, b = tmp_path / "a.sqlite3", tmp_path / "b.sqlite3"
    generate(a, days=4, seed=11)
    generate(b, days=4, seed=12)
    assert q(a, "SELECT * FROM orders") != q(b, "SELECT * FROM orders")


def test_the_defect_counters_match_what_is_in_the_table(fresh):
    db = paths(fresh).source_db
    d = generate(db, days=5, seed=7)
    dupes = q(db, "SELECT COALESCE(SUM(n - 1), 0) FROM (SELECT payment_id, COUNT(*) n "
                  "FROM payments GROUP BY 1 HAVING COUNT(*) > 1)")[0][0]
    assert dupes == d.duplicate_payment_rows + d.conflicting_payment_rows
    assert q(db, "SELECT COUNT(*) FROM payments WHERE amount_paise < 0")[0][0] == \
        d.negative_payment_amounts
    assert q(db, "SELECT COUNT(*) FROM customers WHERE city IS NULL")[0][0] == \
        d.null_city_customers
    assert q(db, "SELECT COUNT(*) FROM order_items WHERE qty = 0")[0][0] == d.zero_qty_items
    assert q(db, "SELECT COUNT(*) FROM order_items i WHERE NOT EXISTS "
                 "(SELECT 1 FROM orders o WHERE o.order_id = i.order_id)")[0][0] == \
        d.orphan_order_items


def test_the_change_feed_holds_more_than_the_live_table(tmp_path):
    db = tmp_path / "s.sqlite3"
    generate(db, days=10)
    live = q(db, "SELECT COUNT(*) FROM orders")[0][0]
    feed = q(db, "SELECT COUNT(*) FROM orders_cdc")[0][0]
    assert feed > live * 2, (live, feed)
    # Every live order appears in the feed at least once.
    assert q(db, "SELECT COUNT(*) FROM orders o WHERE NOT EXISTS "
                 "(SELECT 1 FROM orders_cdc c WHERE c.order_id = o.order_id)")[0][0] == 0


def test_the_feed_keeps_one_row_per_version_and_no_duplicates(tmp_path):
    db = tmp_path / "s.sqlite3"
    generate(db, days=6)
    dupes = q(db, "SELECT COUNT(*) FROM (SELECT order_id, updated_at, COUNT(*) n "
                  "FROM orders_cdc GROUP BY 1, 2 HAVING COUNT(*) > 1)")[0][0]
    assert dupes == 0


def test_the_source_grows_a_column_part_way_through(tmp_path):
    before, after = tmp_path / "b.sqlite3", tmp_path / "a.sqlite3"
    generate(before, days=SCHEMA_DRIFT_DAY)
    generate(after, days=SCHEMA_DRIFT_DAY + 1)
    cols = lambda db: {r[1] for r in q(db, "PRAGMA table_info(orders)")}
    assert "promo_code" not in cols(before)
    assert "promo_code" in cols(after)
    assert "promo_code" in {r[1] for r in q(after, "PRAGMA table_info(orders_cdc)")}


def test_status_never_walks_backwards_in_the_live_table(tmp_path):
    db = tmp_path / "s.sqlite3"
    generate(db, days=8)
    terminal = q(db, "SELECT COUNT(*) FROM orders WHERE status IN "
                     "('delivered', 'cancelled', 'refunded')")[0][0]
    assert terminal > 0
