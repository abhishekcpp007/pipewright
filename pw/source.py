"""The source system: a sloppy e-commerce OLTP database.

The point of this file is that it breaks things on purpose and writes down what it broke.
Every defect it injects has a counter, the counts land in injected.json, and the data quality
tests assert the pipeline found exactly that many. A pipeline that passes against clean data
has not been tested.
"""
from __future__ import annotations

import json
import random
import sqlite3
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path

from .config import DEFAULT_DAYS, SCHEMA_DRIFT_DAY, START_DATE

CITIES = [
    "Lucknow", "Gurugram", "Pune", "Bengaluru", "Hyderabad", "Indore",
    "Kolkata", "Chennai", "Jaipur", "Kochi",
]
TIERS = ["basic", "plus", "gold"]
CATEGORIES = ["kitchen", "grocery", "apparel", "electronics", "home", "stationery"]
CHANNELS = ["android", "ios", "web", "partner"]
METHODS = ["upi", "card", "netbanking", "cod"]
# A real order walks forward through these. Nothing goes backwards.
NEXT_STATUS = {
    "placed": ["paid", "cancelled"],
    "paid": ["shipped", "refunded"],
    "shipped": ["delivered"],
    "delivered": [],
    "cancelled": [],
    "refunded": [],
}

DDL = [
    """CREATE TABLE customers (
        customer_id INTEGER PRIMARY KEY, full_name TEXT, email TEXT, city TEXT,
        tier TEXT, signup_ts TEXT, updated_at TEXT)""",
    """CREATE TABLE products (
        product_id INTEGER PRIMARY KEY, sku TEXT, title TEXT, category TEXT,
        price_paise INTEGER, is_active INTEGER, updated_at TEXT)""",
    """CREATE TABLE orders (
        order_id INTEGER PRIMARY KEY, customer_id INTEGER, status TEXT, channel TEXT,
        order_ts TEXT, updated_at TEXT)""",
    """CREATE TABLE order_items (
        order_item_id INTEGER PRIMARY KEY, order_id INTEGER, product_id INTEGER,
        qty INTEGER, unit_price_paise INTEGER, updated_at TEXT)""",
    # No primary key and no unique index. This table is an append only sink that the payment
    # gateway retries into, which is where the duplicate payment rows come from.
    """CREATE TABLE payments (
        payment_id INTEGER, order_id INTEGER, amount_paise INTEGER, method TEXT,
        status TEXT, paid_at TEXT, updated_at TEXT)""",
]

# The change feed. Every insert and every update appends the row as it looks after the change,
# which is what a log based capture tool hands you. Polling the live table on updated_at cannot
# see an order that went placed, paid, shipped between two runs: the column only holds the last
# of those. The extract supports both and bench/cdc_vs_poll.py counts what polling misses.
CDC_DDL = [
    """CREATE TABLE customers_cdc (
        customer_id INTEGER, full_name TEXT, email TEXT, city TEXT, tier TEXT,
        signup_ts TEXT, updated_at TEXT, _op TEXT)""",
    """CREATE TABLE products_cdc (
        product_id INTEGER, sku TEXT, title TEXT, category TEXT, price_paise INTEGER,
        is_active INTEGER, updated_at TEXT, _op TEXT)""",
    """CREATE TABLE orders_cdc (
        order_id INTEGER, customer_id INTEGER, status TEXT, channel TEXT, order_ts TEXT,
        updated_at TEXT, _op TEXT)""",
    """CREATE TABLE order_items_cdc (
        order_item_id INTEGER, order_id INTEGER, product_id INTEGER, qty INTEGER,
        unit_price_paise INTEGER, updated_at TEXT, _op TEXT)""",
    """CREATE TABLE payments_cdc (
        payment_id INTEGER, order_id INTEGER, amount_paise INTEGER, method TEXT,
        status TEXT, paid_at TEXT, updated_at TEXT, _op TEXT)""",
]


@dataclass
class Defects:
    """What the generator broke, by name. The DQ suite asserts against these."""

    duplicate_payment_rows: int = 0
    conflicting_payment_rows: int = 0
    null_city_customers: int = 0
    negative_payment_amounts: int = 0
    orphan_order_items: int = 0
    late_arriving_orders: int = 0
    out_of_order_customer_updates: int = 0
    zero_qty_items: int = 0
    bad_email_customers: int = 0
    counts: dict = field(default_factory=dict)

    def as_dict(self) -> dict:
        d = {k: v for k, v in self.__dict__.items() if k != "counts"}
        d["row_counts"] = self.counts
        return d


def _feed(con: sqlite3.Connection, table: str, pk: str, ids: list[int]) -> None:
    """Append the post-update image of these rows to the change feed, in chunks SQLite will take."""
    for i in range(0, len(ids), 400):
        chunk = ids[i:i + 400]
        ph = ",".join("?" * len(chunk))
        con.execute(f"INSERT INTO {table}_cdc SELECT *, 'u' FROM {table} "
                    f"WHERE {pk} IN ({ph})", chunk)


class Clock:
    """Hands out strictly increasing timestamps inside a day, so watermarks have something
    to bite on. Ties still happen across tables, which is why the extract is inclusive."""

    def __init__(self, day: date) -> None:
        self.base = datetime(day.year, day.month, day.day, 0, 0, 0)
        self.n = 0

    def tick(self, jitter: int = 1) -> str:
        self.n += jitter
        return (self.base + timedelta(seconds=self.n)).isoformat(sep=" ", timespec="seconds")


def generate(db_path: Path, days: int = DEFAULT_DAYS, seed: int = 7,
             start: str = START_DATE) -> Defects:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    if db_path.exists():
        db_path.unlink()
    con = sqlite3.connect(db_path)
    con.execute("PRAGMA journal_mode=WAL")
    for stmt in DDL + CDC_DDL:
        con.execute(stmt)
    for t in ("customers", "products", "orders", "order_items", "payments"):
        con.execute(f"CREATE INDEX i_{t}_cdc_ts ON {t}_cdc(updated_at)")
    con.execute("CREATE INDEX i_items_order ON order_items(order_id)")
    con.execute("CREATE INDEX i_pay_order ON payments(order_id)")
    con.execute("CREATE INDEX i_orders_updated ON orders(updated_at)")

    rng = random.Random(seed)
    d = Defects()
    start_day = date.fromisoformat(start)

    customers: list[int] = []
    products: list[tuple[int, int]] = []  # (product_id, price_paise)
    open_orders: list[tuple[int, str]] = []  # (order_id, status)
    cust_seq = order_seq = item_seq = pay_seq = prod_seq = 0
    drifted = False

    for day_idx in range(days):
        day = start_day + timedelta(days=day_idx)
        clock = Clock(day)

        if day_idx == SCHEMA_DRIFT_DAY and not drifted:
            # The source team ships a column mid month and tells nobody.
            con.execute("ALTER TABLE orders ADD COLUMN promo_code TEXT")
            con.execute("ALTER TABLE orders_cdc ADD COLUMN promo_code TEXT")
            drifted = True

        # ---- products
        new_products = 400 if day_idx == 0 else rng.randint(2, 8)
        rows = []
        for _ in range(new_products):
            prod_seq += 1
            price = rng.choice([4900, 9900, 14900, 24900, 39900, 74900, 129900])
            rows.append((prod_seq, f"SKU-{prod_seq:05d}", f"Item {prod_seq}",
                         rng.choice(CATEGORIES), price, 1, clock.tick()))
            products.append((prod_seq, price))
        con.executemany("INSERT INTO products VALUES (?,?,?,?,?,?,?)", rows)
        con.executemany("INSERT INTO products_cdc VALUES (?,?,?,?,?,?,?,'i')", rows)

        # price moves, so the product dimension has something to version
        price_moves = []
        repriced = {}
        for pid, _ in rng.sample(products, min(len(products), 15)):
            new_price = rng.choice([4900, 9900, 14900, 24900, 39900, 74900, 129900])
            price_moves.append((new_price, clock.tick(), pid))
            repriced[pid] = new_price
        con.executemany("UPDATE products SET price_paise=?, updated_at=? WHERE product_id=?",
                        price_moves)
        _feed(con, "products", "product_id", [m[2] for m in price_moves])
        products = [(p, repriced.get(p, pr)) for p, pr in products]

        # ---- customers
        new_customers = 2000 if day_idx == 0 else rng.randint(40, 120)
        rows = []
        for _ in range(new_customers):
            cust_seq += 1
            city = rng.choice(CITIES)
            email = f"user{cust_seq}@example.com"
            if rng.random() < 0.0025:
                city = None
                d.null_city_customers += 1
            if rng.random() < 0.002:
                email = f"user{cust_seq}.example.com"  # no @, fails the format check
                d.bad_email_customers += 1
            ts = clock.tick()
            rows.append((cust_seq, f"Customer {cust_seq}", email, city,
                         rng.choice(TIERS), ts, ts))
            customers.append(cust_seq)
        con.executemany("INSERT INTO customers VALUES (?,?,?,?,?,?,?)", rows)
        con.executemany("INSERT INTO customers_cdc VALUES (?,?,?,?,?,?,?,'i')", rows)

        # tier and city changes. These are what the type 2 dimension has to track.
        tier_moves, city_moves = [], []
        for cid in rng.sample(customers, min(len(customers), 40)):
            col, val = rng.choice([("tier", rng.choice(TIERS)), ("city", rng.choice(CITIES))])
            ts = clock.tick()
            if rng.random() < 0.05:
                # A replica lagged and the row lands with an older updated_at than the version
                # already in the warehouse. The dimension must not treat it as newer.
                ts = (datetime.fromisoformat(ts) - timedelta(days=2)).isoformat(
                    sep=" ", timespec="seconds")
                d.out_of_order_customer_updates += 1
            (tier_moves if col == "tier" else city_moves).append((val, ts, cid))
        con.executemany("UPDATE customers SET tier=?, updated_at=? WHERE customer_id=?",
                        tier_moves)
        con.executemany("UPDATE customers SET city=?, updated_at=? WHERE customer_id=?",
                        city_moves)
        _feed(con, "customers", "customer_id",
              [m[2] for m in tier_moves] + [m[2] for m in city_moves])

        # ---- orders
        n_orders = rng.randint(2800, 3600) if day_idx else 1200
        order_rows, item_rows = [], []
        for _ in range(n_orders):
            order_seq += 1
            cid = rng.choice(customers)
            ts = clock.tick()
            order_ts = ts
            if rng.random() < 0.012:
                # The mobile client was offline. The order is real, it is just old.
                back = rng.randint(1, 3)
                order_ts = (datetime.fromisoformat(ts) - timedelta(days=back)).isoformat(
                    sep=" ", timespec="seconds")
                d.late_arriving_orders += 1
            order_rows.append((order_seq, cid, "placed", rng.choice(CHANNELS), order_ts, ts))
            open_orders.append((order_seq, "placed"))
            for _ in range(rng.randint(1, 4)):
                item_seq += 1
                pid, price = rng.choice(products)
                qty = rng.randint(1, 3)
                if rng.random() < 0.0008:
                    qty = 0
                    d.zero_qty_items += 1
                item_rows.append((item_seq, order_seq, pid, qty, price, ts))

        if drifted:
            with_promo = [r + (rng.choice([None, "DIWALI10", "FIRST50"]),) for r in order_rows]
            con.executemany(
                "INSERT INTO orders (order_id,customer_id,status,channel,order_ts,updated_at,promo_code)"
                " VALUES (?,?,?,?,?,?,?)", with_promo)
            con.executemany(
                "INSERT INTO orders_cdc (order_id,customer_id,status,channel,order_ts,"
                "updated_at,promo_code,_op) VALUES (?,?,?,?,?,?,?,'i')", with_promo)
        else:
            con.executemany("INSERT INTO orders VALUES (?,?,?,?,?,?)", order_rows)
            con.executemany("INSERT INTO orders_cdc VALUES (?,?,?,?,?,?,'i')", order_rows)
        con.executemany("INSERT INTO order_items VALUES (?,?,?,?,?,?)", item_rows)
        con.executemany("INSERT INTO order_items_cdc VALUES (?,?,?,?,?,?,'i')", item_rows)

        # orphans: the item survived, the order row was rolled back
        for _ in range(2):
            item_seq += 1
            pid, price = rng.choice(products)
            orphan = (item_seq, 9_000_000 + item_seq, pid, 1, price, clock.tick())
            con.execute("INSERT INTO order_items VALUES (?,?,?,?,?,?)", orphan)
            con.execute("INSERT INTO order_items_cdc VALUES (?,?,?,?,?,?,'i')", orphan)
            d.orphan_order_items += 1

        # ---- status transitions on orders from earlier days
        still_open, moves, newly_paid = [], [], []
        for oid, status in open_orders:
            nxt = NEXT_STATUS[status]
            if nxt and rng.random() < 0.55:
                status = rng.choices(nxt, weights=[0.9, 0.1][: len(nxt)] or [1])[0]
                moves.append((status, clock.tick(), oid))
                if status in ("paid", "shipped", "delivered"):
                    newly_paid.append(oid)
            if NEXT_STATUS[status]:
                still_open.append((oid, status))
        con.executemany("UPDATE orders SET status=?, updated_at=? WHERE order_id=?", moves)
        _feed(con, "orders", "order_id", [m[2] for m in moves])
        open_orders = still_open

        # ---- payments for orders that reached paid today, one aggregate instead of a query
        # per order. The per order version took 40 of the 60 seconds this generator used to run.
        pay_rows = []
        if newly_paid:
            placeholders = ",".join("?" * len(newly_paid))
            amounts = dict(con.execute(
                f"SELECT order_id, COALESCE(SUM(qty*unit_price_paise),0) FROM order_items"
                f" WHERE order_id IN ({placeholders}) GROUP BY order_id", newly_paid).fetchall())
            already = {r[0] for r in con.execute(
                f"SELECT DISTINCT order_id FROM payments WHERE order_id IN ({placeholders})",
                newly_paid).fetchall()}
            for oid in newly_paid:
                if oid in already:
                    continue
                pay_seq += 1
                ts = clock.tick()
                pay_rows.append((pay_seq, oid, amounts.get(oid, 0), rng.choice(METHODS),
                                 "captured", ts, ts))
        con.executemany("INSERT INTO payments VALUES (?,?,?,?,?,?,?)", pay_rows)
        con.executemany("INSERT INTO payments_cdc VALUES (?,?,?,?,?,?,?,'i')", pay_rows)

        # gateway retried and the sink took it twice, byte for byte
        for row in rng.sample(pay_rows, min(len(pay_rows), 3)):
            dup = row[:-1] + (clock.tick(),)
            con.execute("INSERT INTO payments VALUES (?,?,?,?,?,?,?)", dup)
            con.execute("INSERT INTO payments_cdc VALUES (?,?,?,?,?,?,?,'i')", dup)
            d.duplicate_payment_rows += 1
        # same payment_id, different amount. Dedupe has to pick one and always the same one.
        for row in rng.sample(pay_rows, min(len(pay_rows), 2)):
            bad = (row[0], row[1], row[2] + 100, row[3], "captured", row[5], clock.tick())
            con.execute("INSERT INTO payments VALUES (?,?,?,?,?,?,?)", bad)
            con.execute("INSERT INTO payments_cdc VALUES (?,?,?,?,?,?,?,'i')", bad)
            d.conflicting_payment_rows += 1
        # a refund booked as a negative capture instead of its own refund row
        for row in rng.sample(pay_rows, min(len(pay_rows), 2)):
            pay_seq += 1
            neg = (pay_seq, row[1], -abs(row[2]), row[3], "captured", row[5], clock.tick())
            con.execute("INSERT INTO payments VALUES (?,?,?,?,?,?,?)", neg)
            con.execute("INSERT INTO payments_cdc VALUES (?,?,?,?,?,?,?,'i')", neg)
            d.negative_payment_amounts += 1

        con.commit()

    for t in ("customers", "products", "orders", "order_items", "payments"):
        d.counts[t] = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        d.counts[t + "_cdc"] = con.execute(f"SELECT COUNT(*) FROM {t}_cdc").fetchone()[0]
    con.commit()
    con.close()
    return d


def write_injected(path: Path, defects: Defects) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(defects.as_dict(), indent=2) + "\n")
