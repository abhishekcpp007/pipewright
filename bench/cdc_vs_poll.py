"""How much a polling extract never sees.

Polling reads the live table and compares updated_at against a watermark. If a row changed
twice between two runs, the column only holds the second change and the first one is gone. The
change feed holds both. This counts the difference on the same source database, same fourteen
daily runs, same everything else.
"""
from __future__ import annotations

import json
import shutil
import sqlite3
import tempfile
from pathlib import Path

from pw.config import paths
from pw.runner import replay
from pw.source import generate, write_injected


def run_mode(source: Path, mode: str) -> dict:
    home = Path(tempfile.mkdtemp(prefix=f"pw-{mode}-"))
    p = paths(home).prepare()
    shutil.copy(source, p.source_db)
    reps = replay(home, days=14, quiet=True, mode=mode)
    import duckdb
    con = duckdb.connect(str(p.warehouse))
    out = {
        "mode": mode,
        "rows_extracted": sum(r.rows_extracted for r in reps),
        "seconds": round(sum(r.seconds for r in reps), 2),
        "order_versions_landed": con.execute(
            "SELECT COUNT(*) FROM bronze_orders").fetchone()[0],
        "distinct_order_states_seen": con.execute(
            "SELECT COUNT(*) FROM (SELECT DISTINCT order_id, status FROM bronze_orders)"
        ).fetchone()[0],
        "orders_in_fact": con.execute("SELECT COUNT(*) FROM fct_orders").fetchone()[0],
        "delivered_in_fact": con.execute(
            "SELECT COUNT(*) FROM fct_orders WHERE status = 'delivered'").fetchone()[0],
        "revenue_rupees": round(con.execute(
            "SELECT COALESCE(SUM(gross_paise), 0) / 100.0 FROM fct_orders WHERE is_revenue"
        ).fetchone()[0], 2),
    }
    con.close()
    shutil.rmtree(home, ignore_errors=True)
    return out


def main() -> None:
    work = Path(tempfile.mkdtemp(prefix="pw-src-"))
    src = work / "shop.sqlite3"
    defects = generate(src, days=14)
    write_injected(work / "injected.json", defects)
    con = sqlite3.connect(src)
    truth = {
        "order_state_changes_in_source": con.execute(
            "SELECT COUNT(*) FROM orders_cdc").fetchone()[0],
        "distinct_order_states_in_source": con.execute(
            "SELECT COUNT(*) FROM (SELECT DISTINCT order_id, status FROM orders_cdc)"
        ).fetchone()[0],
        "orders_in_source": con.execute("SELECT COUNT(*) FROM orders").fetchone()[0],
    }
    con.close()

    results = {"source": truth, "runs": [run_mode(src, m) for m in ("cdc", "poll")]}
    cdc, poll = results["runs"]
    missed = cdc["distinct_order_states_seen"] - poll["distinct_order_states_seen"]
    results["poll_missed_states"] = missed
    results["poll_missed_pct"] = round(
        missed / cdc["distinct_order_states_seen"] * 100, 1)
    results["revenue_gap_rupees"] = round(cdc["revenue_rupees"] - poll["revenue_rupees"], 2)
    out = Path(__file__).parent / "results" / "cdc_vs_poll.json"
    out.write_text(json.dumps(results, indent=2) + "\n")
    shutil.rmtree(work, ignore_errors=True)

    print(f"source holds {truth['order_state_changes_in_source']:,} order changes, "
          f"{truth['distinct_order_states_in_source']:,} distinct order states")
    print(f"{'mode':<6} {'rows out':>10} {'order rows':>11} {'states seen':>12} "
          f"{'in fact':>9} {'delivered':>10} {'revenue':>14}")
    for r in results["runs"]:
        print(f"{r['mode']:<6} {r['rows_extracted']:>10,} {r['order_versions_landed']:>11,} "
              f"{r['distinct_order_states_seen']:>12,} {r['orders_in_fact']:>9,} "
              f"{r['delivered_in_fact']:>10,} {r['revenue_rupees']:>14,.0f}")
    print(f"\npolling missed {missed:,} order states, {results['poll_missed_pct']}% of them")
    print(f"revenue gap: {results['revenue_gap_rupees']:,.0f} rupees")
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
