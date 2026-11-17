"""Incremental extract out of the source OLTP and into the lake.

The cursor is updated_at and the comparison is inclusive on the low side. That re-reads the
rows that sit exactly on the last watermark, which is on purpose: a lagging writer can commit
a row with a timestamp equal to one already read, and an exclusive cursor would lose it
silently. The duplicates it creates are free, because every silver model merges on a key.
At least once delivery plus an idempotent merge is the part that makes the result exactly once.
"""
from __future__ import annotations

import sqlite3
from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import pyarrow as pa

from .lake import arrow_schema, content_checksum, partition_path, write_atomic
from .metadata import Metadata


# The metadata store is one DuckDB file with one writer. When the extract runs threaded the
# runner hands in a lock and every read or write of that file goes through it.
class _NoLock(AbstractContextManager):
    def __exit__(self, *exc):
        return False


@dataclass(frozen=True)
class SourceTable:
    name: str
    primary_key: str
    cursor: str = "updated_at"

    def relation(self, mode: str) -> str:
        """cdc reads the change feed, poll reads the live table. Same cursor either way."""
        return f"{self.name}_cdc" if mode == "cdc" else self.name


SOURCE_TABLES = [
    SourceTable("customers", "customer_id"),
    SourceTable("products", "product_id"),
    SourceTable("orders", "order_id"),
    SourceTable("order_items", "order_item_id"),
    # payments has no key the source enforces, so the merge key is built in silver.
    SourceTable("payments", "payment_id"),
]


@dataclass
class ExtractResult:
    table: str
    rows: int
    bytes: int
    skipped_duplicate: bool
    path: str | None
    watermark: str | None


def _columns(con: sqlite3.Connection, table: str) -> list[tuple[str, str]]:
    return [(r[1], r[2]) for r in con.execute(f"PRAGMA table_info({table})").fetchall()]


def extract_table(con: sqlite3.Connection, meta: Metadata, src: SourceTable, run_id: str,
                  lake_root: Path, ingest_date: str, until: str | None = None,
                  lock: AbstractContextManager | None = None,
                  mode: str = "cdc") -> ExtractResult:
    guard = lock or _NoLock()
    relation = src.relation(mode)
    cols = [(c, t) for c, t in _columns(con, relation) if not c.startswith("_")]
    names = [c for c, _ in cols]
    with guard:
        wm = meta.watermark(src.name)
    where, args = [], []
    if wm:
        where.append(f"{src.cursor} >= ?")
        args.append(wm)
    if until:
        where.append(f"{src.cursor} < ?")
        args.append(until)
    sql = f"SELECT {', '.join(names)} FROM {relation}"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += f" ORDER BY {src.cursor}, {src.primary_key}"
    rows = con.execute(sql, args).fetchall()
    if not rows:
        return ExtractResult(src.name, 0, 0, False, None, wm)

    schema = arrow_schema(cols)
    table = pa.table({name: [r[i] for r in rows] for i, name in enumerate(names)},
                     schema=schema)
    checksum = content_checksum(table)
    with guard:
        already_committed = meta.manifest_has(src.name, checksum)
    if already_committed:
        # Same bytes as a batch already committed. A retry after a crash between the file
        # write and the watermark update lands here, and lands here harmlessly.
        return ExtractResult(src.name, len(rows), 0, True, None, wm)

    dest = partition_path(lake_root, src.name, ingest_date, run_id)
    nbytes = write_atomic(table, dest)
    new_wm = max(r[names.index(src.cursor)] for r in rows)
    with guard:
        # File first, then the manifest, then the watermark. A crash between the write and the
        # manifest leaves an orphan file that the content checksum will recognise next time.
        meta.add_manifest(src.name, ingest_date, run_id, str(dest), len(rows), nbytes, checksum)
        meta.set_watermark(src.name, new_wm, len(rows))
    return ExtractResult(src.name, len(rows), nbytes, False, str(dest), new_wm)


def extract_all(source_db: Path, meta: Metadata, run_id: str, lake_root: Path,
                ingest_date: str, until: str | None = None,
                mode: str = "cdc") -> list[ExtractResult]:
    con = sqlite3.connect(f"file:{source_db}?mode=ro", uri=True)
    try:
        return [extract_table(con, meta, src, run_id, lake_root, ingest_date, until, mode=mode)
                for src in SOURCE_TABLES]
    finally:
        con.close()


def day_bounds(day: str) -> tuple[str, str]:
    d = date.fromisoformat(day)
    return day, (d + timedelta(days=1)).isoformat()
