"""The orchestrator. Layer by layer, with retries, a quality gate per model and a resume path.

Model builds run one at a time on purpose. DuckDB already parallelises inside a single query,
so two models at once mostly fight over the same cores and the same write lock.

The extract can be threaded over the five source tables and the default is not to. I expected
it to win and it loses: 0.71x at five threads, because the metadata store is one writer and
every watermark read and manifest commit queues behind the same lock while the actual work is
milliseconds of SQLite reads. bench/extract_threads.py is where that number comes from. The
knob stays because a wider source, or object storage instead of a local disk, moves the answer.
"""
from __future__ import annotations

import os
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
from threading import Lock

from .config import Paths
from .config import paths as make_paths
from .dag import Graph
from .extract import SOURCE_TABLES, ExtractResult, day_bounds, extract_table
from .metadata import Metadata
from .quality import Check, failed_errors, load_expectations, run_expectations
from .warehouse import BuildResult, Warehouse


@dataclass
class RunReport:
    run_id: str
    batch_date: str | None
    status: str = "success"
    seconds: float = 0.0
    extracted: list[ExtractResult] = field(default_factory=list)
    built: list[BuildResult] = field(default_factory=list)
    checks: list[Check] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)
    failures: list[tuple[str, str]] = field(default_factory=list)

    @property
    def rows_extracted(self) -> int:
        return sum(e.rows for e in self.extracted)

    def summary(self) -> str:
        parts = [f"run {self.run_id}"]
        if self.batch_date:
            parts.append(f"batch {self.batch_date}")
        parts.append(f"{self.rows_extracted} rows extracted")
        parts.append(f"{len(self.built)} models")
        warn = sum(1 for c in self.checks if not c.passed and c.severity == "warn")
        err = sum(1 for c in self.checks if not c.passed and c.severity == "error")
        parts.append(f"{len(self.checks)} checks, {err} failed, {warn} warned")
        if self.skipped:
            parts.append(f"{len(self.skipped)} skipped")
        parts.append(self.status)
        return " | ".join(parts)


def _maybe_fail(node: str, attempt: int) -> None:
    """Fault injection for the retry and resume tests. PW_FAIL_ONCE=model fails attempt 1."""
    once = os.environ.get("PW_FAIL_ONCE")
    if once and once == node and attempt == 1:
        raise RuntimeError(f"injected failure in {node}")
    always = os.environ.get("PW_FAIL_ALWAYS")
    if always and always == node:
        raise RuntimeError(f"injected failure in {node}")


def run_extract(p: Paths, meta: Metadata, run_id: str, ingest_date: str,
                until: str | None, threads: int = 1,
                mode: str = "cdc") -> list[ExtractResult]:
    import sqlite3

    lock = Lock()

    def one(src):
        con = sqlite3.connect(f"file:{p.source_db}?mode=ro", uri=True, check_same_thread=False)
        try:
            with lock:
                # The metadata store is a single writer, so reads of the watermark and the
                # commit of the manifest are serialised. The Parquet write is not.
                pass
            return extract_table(con, meta, src, run_id, p.lake, ingest_date, until,
                                 lock=lock, mode=mode)
        finally:
            con.close()

    if threads <= 1:
        return [one(src) for src in SOURCE_TABLES]
    with ThreadPoolExecutor(max_workers=threads) as pool:
        return list(pool.map(one, SOURCE_TABLES))


def run_pipeline(home: str | Path | None = None, batch_date: str | None = None,
                 selector: str | None = None, full_refresh: bool = False,
                 skip_extract: bool = False, retries: int = 1, threads: int = 1,
                 until: str | None = None, as_of: str | None = None,
                 quality: bool = True, mode: str = "cdc") -> RunReport:
    t0 = time.perf_counter()
    p = make_paths(home).prepare()
    meta = Metadata(p.metadata)
    wh = Warehouse(p.warehouse, p.lake)
    graph = Graph.load()
    expectations = load_expectations()
    run_id = meta.start_run("run", selector or "")
    report = RunReport(run_id=run_id, batch_date=batch_date)

    try:
        if not skip_extract:
            t = time.perf_counter()
            if until is None and batch_date:
                until = day_bounds(batch_date)[1]
            report.extracted = run_extract(p, meta, run_id, batch_date or "unbounded",
                                           until, threads, mode)
            meta.record_task(run_id, "extract", "success", 1, time.perf_counter() - t,
                             report.rows_extracted)

        selected = graph.select(selector)
        failed: set[str] = set()
        for layer in graph.layers(selected):
            built_here: list[str] = []
            for node in layer:
                model = graph.models[node]
                blocked = sorted(set(model.refs) & failed)
                if blocked:
                    report.skipped.append(node)
                    meta.record_task(run_id, node, "upstream_failed", 0, 0.0, None,
                                     f"blocked by {', '.join(blocked)}")
                    failed.add(node)
                    continue

                attempt, last_err = 0, None
                built = None
                t = time.perf_counter()
                while attempt < max(1, retries + 1):
                    attempt += 1
                    try:
                        _maybe_fail(node, attempt)
                        built = wh.build(model, batch_date, full_refresh)
                        break
                    except Exception as e:  # noqa: BLE001 - the runner reports, it does not raise
                        last_err = e
                        time.sleep(0.05 * attempt)
                secs = time.perf_counter() - t

                if built is None:
                    report.failures.append((node, str(last_err)))
                    failed.add(node)
                    meta.record_task(run_id, node, "failed", attempt, secs, None,
                                     str(last_err))
                    continue

                report.built.append(built)
                built_here.append(node)
                meta.record_task(run_id, node, "success", attempt, secs, built.rows_after)
                meta.record_model(run_id, node, built.materialization, built.rows_after,
                                  built.rows_changed, secs, built.notes)

            if quality and built_here:
                # Tests run once the layer is whole. A referential test on one silver model
                # points at another silver model, and they are siblings in the same layer.
                checks = run_expectations(wh.con, expectations, set(built_here),
                                          as_of or batch_date, meta)
                report.checks.extend(checks)
                for c in checks:
                    meta.record_dq(run_id, c.model, c.name, c.severity, c.passed,
                                   c.observed, c.threshold, c.detail)
                for c in failed_errors(checks):
                    # Circuit open. The model stays built so it can be inspected, but
                    # nothing downstream of it runs on this batch.
                    failed.add(c.model)
                    report.failures.append(
                        (c.model, f"quality: {c.name} ({c.observed} violations)"))

        report.status = "failed" if (report.failures or report.skipped) else "success"
        meta.finish_run(run_id, report.status)
    finally:
        report.seconds = time.perf_counter() - t0
        wh.close()
        meta.close()
    return report


def replay(home: str | Path | None = None, start: str = "2026-10-01", days: int = 14,
           threads: int = 1, quiet: bool = False, mode: str = "cdc") -> list[RunReport]:
    """Fourteen daily runs, in order, the way the pipeline would have run them."""
    out = []
    d0 = date.fromisoformat(start)
    for i in range(days):
        day = (d0 + timedelta(days=i)).isoformat()
        rep = run_pipeline(home, batch_date=day, threads=threads, as_of=day, mode=mode)
        out.append(rep)
        if not quiet:
            print(rep.summary(), flush=True)
    return out


def backfill(home: str | Path | None = None, start: str = "2026-10-01", end: str = "2026-10-03",
             selector: str | None = None, quiet: bool = False) -> list[RunReport]:
    """Re-process partitions already in the lake. No extract, so the source is never touched."""
    out = []
    d, last = date.fromisoformat(start), date.fromisoformat(end)
    while d <= last:
        rep = run_pipeline(home, batch_date=d.isoformat(), selector=selector,
                           skip_extract=True, as_of=d.isoformat())
        out.append(rep)
        if not quiet:
            print("backfill " + rep.summary(), flush=True)
        d += timedelta(days=1)
    return out
