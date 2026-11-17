"""pw, the command line. Everything the pipeline does is one of these verbs."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .config import DEFAULT_DAYS, START_DATE, paths
from .dag import Graph
from .metadata import Metadata
from .quality import load_expectations, run_expectations
from .runner import backfill, replay, run_pipeline
from .source import generate, write_injected
from .warehouse import Warehouse


def cmd_gen(args) -> int:
    p = paths(args.home).prepare()
    d = generate(p.source_db, days=args.days, seed=args.seed, start=args.start)
    write_injected(p.injected, d)
    print(f"source: {p.source_db}")
    for k, v in d.as_dict()["row_counts"].items():
        print(f"  {k:<14} {v:>8,}")
    print("defects injected on purpose:")
    for k, v in d.as_dict().items():
        if k != "row_counts":
            print(f"  {k:<30} {v:>6}")
    return 0


def cmd_run(args) -> int:
    rep = run_pipeline(args.home, batch_date=args.date, selector=args.select,
                       full_refresh=args.full_refresh, skip_extract=args.no_extract,
                       retries=args.retries, threads=args.threads)
    print(rep.summary())
    for name, err in rep.failures:
        print(f"  FAILED {name}: {err}")
    for c in rep.checks:
        if not c.passed:
            flag = "ERROR" if c.severity == "error" else "warn "
            print(f"  {flag} {c.model}: {c.name} -> {c.observed} ({c.detail})")
    if args.verbose:
        for b in rep.built:
            extra = " ".join(f"{k}={v}" for k, v in b.notes.items())
            print(f"  {b.model:<24} {b.materialization:<12} rows={b.rows_after:<8} {extra}")
    return 0 if rep.status == "success" else 1


def cmd_replay(args) -> int:
    reps = replay(args.home, start=args.start, days=args.days, threads=args.threads)
    bad = [r for r in reps if r.status != "success"]
    print(f"\n{len(reps)} daily runs, {len(bad)} with failures, "
          f"{sum(r.seconds for r in reps):.1f}s total")
    return 1 if bad else 0


def cmd_backfill(args) -> int:
    reps = backfill(args.home, start=args.start, end=args.end, selector=args.select)
    bad = [r for r in reps if r.status != "success"]
    print(f"{len(reps)} partitions reprocessed, {len(bad)} with failures")
    return 1 if bad else 0


def cmd_test(args) -> int:
    p = paths(args.home)
    wh = Warehouse(p.warehouse, p.lake)
    meta = Metadata(p.metadata)
    checks = run_expectations(wh.con, load_expectations(), None, args.as_of, meta)
    width = max(len(c.name) for c in checks)
    failed = 0
    for c in checks:
        mark = "pass" if c.passed else ("WARN" if c.severity == "warn" else "FAIL")
        if not c.passed and c.severity == "error":
            failed += 1
        print(f"{mark}  {c.model:<24} {c.name:<{width}}  {c.observed}")
    print(f"\n{len(checks)} expectations, {failed} failed with severity error")
    wh.close()
    meta.close()
    return 1 if failed else 0


def cmd_lineage(args) -> int:
    g = Graph.load()
    if args.format == "mermaid":
        print(g.to_mermaid())
    elif args.format == "dot":
        print(g.to_dot())
    else:
        for layer in g.layers():
            for n in layer:
                m = g.models[n]
                deps = ", ".join(m.refs) or "source parquet"
                print(f"{m.layer:<7} {n:<24} {m.materialized:<12} <- {deps}")
    return 0


def cmd_state(args) -> int:
    p = paths(args.home)
    meta = Metadata(p.metadata)
    print("watermarks")
    for r in meta.con.execute(
            "SELECT table_name, value, rows_seen FROM watermarks ORDER BY table_name").fetchall():
        print(f"  {r[0]:<14} {r[1]}  {r[2]:>8,} rows read")
    print("\nlake partitions")
    for r in meta.con.execute(
            "SELECT table_name, COUNT(*) files, SUM(rows) rows, SUM(bytes) bytes "
            "FROM manifest GROUP BY 1 ORDER BY 1").fetchall():
        print(f"  {r[0]:<14} {r[1]:>3} files {r[2]:>9,} rows {r[3] / 1e6:>7.2f} MB")
    print("\nlast five runs")
    for r in meta.con.execute(
            "SELECT run_id, command, status, started_at FROM runs "
            "ORDER BY started_at DESC LIMIT 5").fetchall():
        print(f"  {r[0]}  {r[1]:<8} {r[2]:<8} {r[3]}")
    print("\nwhat the materializations decided, over every run so far")
    for r in meta.con.execute("""
            SELECT model,
                   SUM(COALESCE(CAST(notes->>'rows_rejected_as_stale' AS BIGINT), 0)) stale,
                   SUM(COALESCE(CAST(notes->>'late_versions_dropped' AS BIGINT), 0)) late,
                   SUM(COALESCE(CAST(notes->>'new_versions' AS BIGINT), 0)) versions,
                   SUM(COALESCE(CAST(notes->>'unchanged' AS BIGINT), 0)) unchanged
            FROM model_stats GROUP BY 1 HAVING stale + late + versions + unchanged > 0
            ORDER BY 1""").fetchall():
        bits = []
        if r[1]:
            bits.append(f"{r[1]:,} stale rows refused by the watermark guard")
        if r[2]:
            bits.append(f"{r[2]:,} late versions dropped")
        if r[3]:
            bits.append(f"{r[3]:,} new versions opened")
        if r[4]:
            bits.append(f"{r[4]:,} repeats with nothing changed")
        print(f"  {r[0]:<16} " + ", ".join(bits))

    unresolved = meta.last_failed_nodes()
    if unresolved:
        print("\nnodes that did not finish in the newest run:", ", ".join(unresolved))
    meta.close()
    return 0


def cmd_report(args) -> int:
    p = paths(args.home)
    wh = Warehouse(p.warehouse, p.lake)
    q = {
        "rows per model": "SELECT model, materialization, rows_after FROM (SELECT model, "
                          "materialization, rows_after, row_number() OVER (PARTITION BY model "
                          "ORDER BY created_at DESC) rn FROM model_stats) WHERE rn=1",
    }
    del q
    print("gold: revenue by city, top 10 days")
    rows = wh.con.execute(
        "SELECT order_date, city, orders, delivered_orders, "
        "round(gross_paise/100.0, 2) AS gross_rupees "
        "FROM agg_daily_city_revenue ORDER BY gross_paise DESC LIMIT 10").fetchall()
    for r in rows:
        print(f"  {r[0]}  {r[1]:<12} {r[2]:>5} orders  {r[3]:>5} delivered  "
              f"{r[4]:>12,.2f} rupees")
    print("\ndim_customer version history")
    for r in wh.con.execute(
            "SELECT version, COUNT(*) FROM dim_customer GROUP BY 1 ORDER BY 1").fetchall():
        print(f"  version {r[0]}: {r[1]:>6,} rows")
    print("\nquarantine by reason")
    for r in wh.con.execute(
            "SELECT source_table, reason, COUNT(*) FROM quarantine "
            "GROUP BY 1, 2 ORDER BY 3 DESC").fetchall():
        print(f"  {r[0]:<12} {r[1]:<22} {r[2]:>6}")
    if args.json:
        out = {
            "models": {r[0]: r[1] for r in wh.con.execute(
                "SELECT table_name, estimated_size FROM duckdb_tables()").fetchall()},
        }
        Path(args.json).write_text(json.dumps(out, indent=2, default=str) + "\n")
        print(f"\nwrote {args.json}")
    wh.close()
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="pw", description=__doc__)
    ap.add_argument("--home", default=None, help="data directory, defaults to $PW_HOME or ./data")
    sub = ap.add_subparsers(dest="cmd", required=True)

    g = sub.add_parser("gen", help="build the source OLTP database")
    g.add_argument("--days", type=int, default=DEFAULT_DAYS)
    g.add_argument("--seed", type=int, default=7)
    g.add_argument("--start", default=START_DATE)
    g.set_defaults(fn=cmd_gen)

    r = sub.add_parser("run", help="one pipeline run: extract, then build the models")
    r.add_argument("--date", default=None, help="batch date, bounds the extract and the models")
    r.add_argument("--select", default=None, help="model, +model, model+, tag:gold")
    r.add_argument("--full-refresh", action="store_true")
    r.add_argument("--no-extract", action="store_true")
    r.add_argument("--retries", type=int, default=1)
    r.add_argument("--threads", type=int, default=1)
    r.add_argument("-v", "--verbose", action="store_true")
    r.set_defaults(fn=cmd_run)

    rp = sub.add_parser("replay", help="run every day in order, like the scheduler would have")
    rp.add_argument("--start", default=START_DATE)
    rp.add_argument("--days", type=int, default=DEFAULT_DAYS)
    rp.add_argument("--threads", type=int, default=1)
    rp.set_defaults(fn=cmd_replay)

    b = sub.add_parser("backfill", help="reprocess partitions already in the lake")
    b.add_argument("--start", required=True)
    b.add_argument("--end", required=True)
    b.add_argument("--select", default=None)
    b.set_defaults(fn=cmd_backfill)

    t = sub.add_parser("test", help="run every expectation against the warehouse as it stands")
    t.add_argument("--as-of", default=None)
    t.set_defaults(fn=cmd_test)

    ln = sub.add_parser("lineage", help="the model graph")
    ln.add_argument("--format", choices=["text", "dot", "mermaid"], default="text")
    ln.set_defaults(fn=cmd_lineage)

    st = sub.add_parser("state", help="watermarks, partitions and recent runs")
    st.set_defaults(fn=cmd_state)

    rep = sub.add_parser("report", help="read the gold tables")
    rep.add_argument("--json", default=None)
    rep.set_defaults(fn=cmd_report)

    args = ap.parse_args(argv)
    return args.fn(args)


if __name__ == "__main__":
    sys.exit(main())
