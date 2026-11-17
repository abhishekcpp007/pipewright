"""Parquet against CSV, and partition pruning against reading the lot.

The lake is written as one Parquet file per table per ingest date. This measures what that
layout is worth: how much smaller the files are than the same rows as CSV, and how much of a
scan a partition predicate skips.
"""
from __future__ import annotations

import json
import statistics
import time
from pathlib import Path

import duckdb

from pw.config import paths

TABLE = "order_items"
DAY = "2026-10-07"


def median_ms(con, sql: str, repeats: int = 5) -> float:
    con.execute(sql).fetchall()
    times = []
    for _ in range(repeats):
        t = time.perf_counter()
        con.execute(sql).fetchall()
        times.append(time.perf_counter() - t)
    return round(statistics.median(times) * 1000, 2)


def main() -> None:
    p = paths()
    lake = p.lake / "bronze" / TABLE
    if not lake.exists():
        raise SystemExit("no lake yet, run pw gen && pw replay first")
    work = p.home / "bench_tmp"
    work.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()

    glob = str(lake / "**" / "*.parquet")
    parquet_bytes = sum(f.stat().st_size for f in lake.rglob("*.parquet"))
    files = len(list(lake.rglob("*.parquet")))

    csv_path = work / f"{TABLE}.csv"
    single = work / f"{TABLE}_single.parquet"
    con.execute(f"COPY (SELECT * FROM read_parquet('{glob}', hive_partitioning = 1)) "
                f"TO '{csv_path}' (HEADER, DELIMITER ',')")
    con.execute(f"COPY (SELECT * FROM read_parquet('{glob}', hive_partitioning = 1)) "
                f"TO '{single}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    rows = con.execute(f"SELECT COUNT(*) FROM read_parquet('{glob}', hive_partitioning = 1)"
                       ).fetchone()[0]

    q_pruned = (f"SELECT COUNT(*), SUM(qty) FROM read_parquet('{glob}', hive_partitioning = 1) "
                f"WHERE ingest_date = DATE '{DAY}'")
    q_single = (f"SELECT COUNT(*), SUM(qty) FROM read_parquet('{single}') "
                f"WHERE ingest_date = DATE '{DAY}'")
    q_csv = (f"SELECT COUNT(*), SUM(qty) FROM read_csv('{csv_path}', header = true) "
             f"WHERE ingest_date = DATE '{DAY}'")
    q_all = f"SELECT COUNT(*), SUM(qty) FROM read_parquet('{glob}', hive_partitioning = 1)"

    res = {
        "table": TABLE,
        "rows": rows,
        "partitions": files,
        "parquet_bytes": parquet_bytes,
        "single_parquet_bytes": single.stat().st_size,
        "csv_bytes": csv_path.stat().st_size,
        "csv_to_parquet_ratio": round(csv_path.stat().st_size / parquet_bytes, 1),
        "one_day_partition_pruned_ms": median_ms(con, q_pruned),
        "one_day_single_parquet_ms": median_ms(con, q_single),
        "one_day_csv_ms": median_ms(con, q_csv),
        "whole_table_parquet_ms": median_ms(con, q_all),
    }
    res["pruning_speedup_vs_single_file"] = round(
        res["one_day_single_parquet_ms"] / res["one_day_partition_pruned_ms"], 1)
    res["parquet_speedup_vs_csv"] = round(res["one_day_csv_ms"]
                                          / res["one_day_partition_pruned_ms"], 1)
    dest = Path(__file__).parent / "results" / "storage.json"
    dest.write_text(json.dumps(res, indent=2) + "\n")

    print(f"{TABLE}: {rows:,} rows across {files} daily partitions")
    print(f"  parquet, partitioned   {parquet_bytes / 1e6:>7.2f} MB")
    print(f"  parquet, one file      {res['single_parquet_bytes'] / 1e6:>7.2f} MB")
    print(f"  csv                    {res['csv_bytes'] / 1e6:>7.2f} MB   "
          f"{res['csv_to_parquet_ratio']}x the partitioned parquet")
    print("\none day of that table, median of five runs")
    print(f"  partition pruned       {res['one_day_partition_pruned_ms']:>7.2f} ms")
    print(f"  single parquet file    {res['one_day_single_parquet_ms']:>7.2f} ms   "
          f"{res['pruning_speedup_vs_single_file']}x slower")
    print(f"  csv                    {res['one_day_csv_ms']:>7.2f} ms   "
          f"{res['parquet_speedup_vs_csv']}x slower")
    print(f"  whole table, no filter {res['whole_table_parquet_ms']:>7.2f} ms")
    csv_path.unlink()
    single.unlink()
    print(f"\nwrote {dest}")


if __name__ == "__main__":
    main()
