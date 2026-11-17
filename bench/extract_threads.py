"""Does threading the extract help? Five source tables, one thread each at most."""
from __future__ import annotations

import json
import shutil
import statistics
import tempfile
from pathlib import Path

from pw.config import paths
from pw.metadata import Metadata
from pw.runner import run_extract
from pw.source import generate


def one(threads: int, source: Path, days: int = 14) -> float:
    home = Path(tempfile.mkdtemp(prefix=f"pw-th{threads}-"))
    p = paths(home).prepare()
    shutil.copy(source, p.source_db)
    meta = Metadata(p.metadata)
    total = 0.0
    import time
    for i in range(1, days + 1):
        day = f"2026-10-{i:02d}"
        until = f"2026-10-{i + 1:02d}" if i < 31 else "2026-11-01"
        run_id = meta.start_run("extract")
        t = time.perf_counter()
        run_extract(p, meta, run_id, day, until, threads=threads)
        total += time.perf_counter() - t
        meta.finish_run(run_id, "success")
    meta.close()
    shutil.rmtree(home, ignore_errors=True)
    return total


def main() -> None:
    work = Path(tempfile.mkdtemp(prefix="pw-src-"))
    src = work / "shop.sqlite3"
    generate(src, days=14)
    res = {}
    for threads in (1, 2, 5):
        runs = [one(threads, src) for _ in range(3)]
        res[str(threads)] = round(statistics.median(runs), 3)
        print(f"  {threads} thread(s): {res[str(threads)]:.3f}s for 14 daily extracts")
    res["speedup_5_vs_1"] = round(res["1"] / res["5"], 2)
    dest = Path(__file__).parent / "results" / "extract_threads.json"
    dest.write_text(json.dumps(res, indent=2) + "\n")
    shutil.rmtree(work, ignore_errors=True)
    print(f"\n5 threads against 1: {res['speedup_5_vs_1']}x")
    print(f"wrote {dest}")


if __name__ == "__main__":
    main()
