"""Three analytical queries, written the way they usually arrive and then rewritten.

Every pair returns the same rows, which the script checks before it reports a time. A faster
query that answers a different question is not a rewrite.
"""
from __future__ import annotations

import json
import statistics
import sys
import time
from pathlib import Path

import duckdb

from pw.config import paths

PAIRS = {
    "hours since the customer's previous order": (
        """
        SELECT f.order_id, f.customer_id, f.order_ts,
               date_diff('hour', (SELECT MAX(p.order_ts) FROM fct_orders p
                                  WHERE p.customer_id = f.customer_id
                                    AND (p.order_ts, p.order_id) < (f.order_ts, f.order_id)),
                         f.order_ts) AS gap_hours
        FROM fct_orders f
        ORDER BY f.order_id
        """,
        """
        SELECT order_id, customer_id, order_ts,
               date_diff('hour', LAG(order_ts) OVER (PARTITION BY customer_id
                                                     ORDER BY order_ts, order_id),
                         order_ts) AS gap_hours
        FROM fct_orders
        ORDER BY order_id
        """,
    ),
    "each customer's largest order": (
        """
        SELECT f.customer_id, f.order_id, f.gross_paise
        FROM fct_orders f
        WHERE f.gross_paise = (SELECT MAX(g.gross_paise) FROM fct_orders g
                               WHERE g.customer_id = f.customer_id)
        ORDER BY f.customer_id, f.order_id
        """,
        """
        SELECT customer_id, order_id, gross_paise
        FROM (SELECT customer_id, order_id, gross_paise,
                     MAX(gross_paise) OVER (PARTITION BY customer_id) AS mx
              FROM fct_orders)
        WHERE gross_paise = mx
        ORDER BY customer_id, order_id
        """,
    ),
    "running revenue by city": (
        """
        SELECT a.order_date, a.city,
               (SELECT SUM(b.gross_paise) FROM agg_daily_city_revenue b
                WHERE b.city = a.city AND b.order_date <= a.order_date) AS running_paise
        FROM agg_daily_city_revenue a
        ORDER BY a.city, a.order_date
        """,
        """
        SELECT order_date, city,
               SUM(gross_paise) OVER (PARTITION BY city ORDER BY order_date
                                      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
                 AS running_paise
        FROM agg_daily_city_revenue
        ORDER BY city, order_date
        """,
    ),
}


def time_query(con, sql: str, repeats: int = 5) -> tuple[float, list]:
    rows = con.execute(sql).fetchall()
    times = []
    for _ in range(repeats):
        t = time.perf_counter()
        con.execute(sql).fetchall()
        times.append(time.perf_counter() - t)
    return statistics.median(times), rows


ROWSTORE_SLOW = """
SELECT f.order_id, (SELECT MAX(p.order_ts) FROM fct_orders p
                    WHERE p.customer_id = f.customer_id
                      AND (p.order_ts, p.order_id) < (f.order_ts, f.order_id)) AS prev_ts
FROM fct_orders f ORDER BY f.order_id
"""
ROWSTORE_FAST = """
SELECT order_id, LAG(order_ts) OVER (PARTITION BY customer_id ORDER BY order_ts, order_id)
         AS prev_ts
FROM fct_orders ORDER BY order_id
"""


def rowstore_pass(warehouse: Path) -> dict:
    """The same pair on SQLite, which is a row store and does not decorrelate.

    This is the part that changed my mind about the rewrite. On DuckDB the optimizer flattens
    the correlated subquery and the rewrite is worth a couple of times. On a row store with no
    index it is worth three orders of magnitude, and with the right index it is worth nothing.
    """
    import sqlite3
    import tempfile

    con = duckdb.connect(str(warehouse), read_only=True)
    rows = con.execute(
        "SELECT order_id, customer_id, CAST(order_ts AS VARCHAR), gross_paise "
        "FROM fct_orders").fetchall()
    con.close()
    out = {"rows": len(rows)}
    for indexed in (False, True):
        path = Path(tempfile.mkdtemp()) / "rowstore.sqlite3"
        s = sqlite3.connect(path)
        s.execute("CREATE TABLE fct_orders (order_id INT, customer_id INT, order_ts TEXT, "
                  "gross_paise INT)")
        s.executemany("INSERT INTO fct_orders VALUES (?,?,?,?)", rows)
        if indexed:
            s.execute("CREATE INDEX i_cust_ts ON fct_orders(customer_id, order_ts, order_id)")
        s.commit()
        key = "indexed" if indexed else "no_index"
        t = time.perf_counter()
        s.execute(ROWSTORE_FAST).fetchall()
        fast = time.perf_counter() - t
        t = time.perf_counter()
        s.execute(ROWSTORE_SLOW).fetchall()
        slow = time.perf_counter() - t
        s.close()
        path.unlink()
        out[key] = {"correlated_s": round(slow, 2), "window_s": round(fast, 2),
                    "speedup": round(slow / fast, 1)}
        print(f"  sqlite {key:<9} correlated {slow:>7.2f}s   window {fast:>6.2f}s   "
              f"{slow / fast:>8.1f}x")
    return out


def main() -> None:
    p = paths()
    con = duckdb.connect(str(p.warehouse), read_only=True)
    out = {}
    print(f"{'query':<42} {'before':>9} {'after':>9} {'speedup':>8}  rows")
    for name, (slow, fast) in PAIRS.items():
        t_slow, rows_slow = time_query(con, slow)
        t_fast, rows_fast = time_query(con, fast)
        same = rows_slow == rows_fast
        if not same:
            raise SystemExit(f"{name}: the rewrite returns different rows, so it is not one")
        out[name] = {"before_ms": round(t_slow * 1000, 2), "after_ms": round(t_fast * 1000, 2),
                     "speedup": round(t_slow / t_fast, 1), "rows": len(rows_slow),
                     "rows_identical": same}
        print(f"{name:<42} {t_slow * 1000:>8.1f}ms {t_fast * 1000:>8.1f}ms "
              f"{t_slow / t_fast:>7.1f}x  {len(rows_slow):,}")
    con.close()
    results = {"duckdb": out}
    if "--quick" not in sys.argv:
        print("\nthe same pair on a row store, 41k rows, one run each")
        results["sqlite"] = rowstore_pass(p.warehouse)
    dest = Path(__file__).parent / "results" / "sql_rewrite.json"
    dest.write_text(json.dumps(results, indent=2) + "\n")
    print(f"\nwrote {dest}")


if __name__ == "__main__":
    main()
