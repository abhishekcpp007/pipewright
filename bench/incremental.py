"""What the incremental path buys, measured against rebuilding everything.

Both numbers come off the same lake after the fourteen day replay, so the inputs are identical
and the only difference is whether the models filter to one batch or read every partition.
"""
from __future__ import annotations

import json
import shutil
import statistics
import tempfile
from pathlib import Path

from pw.config import paths
from pw.runner import replay, run_pipeline
from pw.source import generate


def prepared_home() -> Path:
    home = Path(tempfile.mkdtemp(prefix="pw-inc-"))
    p = paths(home).prepare()
    generate(p.source_db, days=14)
    replay(home, days=14, quiet=True)
    return home


def timed(home: Path, **kw) -> tuple[float, int]:
    rep = run_pipeline(home, skip_extract=True, quality=False, **kw)
    return rep.seconds, sum(b.rows_changed for b in rep.built)


def main() -> None:
    base = prepared_home()
    runs = 3
    inc, full = [], []
    for _ in range(runs):
        home = Path(tempfile.mkdtemp(prefix="pw-inc-copy-"))
        shutil.rmtree(home)
        shutil.copytree(base, home)
        inc.append(timed(home, batch_date="2026-10-14", as_of="2026-10-14"))
        full.append(timed(home, full_refresh=True, as_of="2026-10-14"))
        shutil.rmtree(home, ignore_errors=True)

    res = {
        "runs": runs,
        "incremental_seconds": round(statistics.median(x[0] for x in inc), 3),
        "full_refresh_seconds": round(statistics.median(x[0] for x in full), 3),
        "incremental_rows_touched": inc[0][1],
        "full_refresh_rows_touched": full[0][1],
    }
    res["speedup"] = round(res["full_refresh_seconds"] / res["incremental_seconds"], 2)
    res["rows_ratio"] = round(res["full_refresh_rows_touched"]
                              / max(res["incremental_rows_touched"], 1), 1)
    out = Path(__file__).parent / "results" / "incremental.json"
    out.write_text(json.dumps(res, indent=2) + "\n")
    shutil.rmtree(base, ignore_errors=True)

    print(f"one batch, incremental : {res['incremental_seconds']:>6.3f}s  "
          f"{res['incremental_rows_touched']:>9,} rows touched")
    print(f"everything, full refresh: {res['full_refresh_seconds']:>6.3f}s  "
          f"{res['full_refresh_rows_touched']:>9,} rows touched")
    print(f"\nincremental is {res['speedup']}x faster on {res['rows_ratio']}x fewer rows")
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
