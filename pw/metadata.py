"""Run metadata. Lives in its own DuckDB file so dropping the warehouse keeps the history.

Everything the pipeline learns about itself goes here: watermarks, the file manifest, task
state per run, data quality results and per model timings. The resume path and the freshness
checks both read out of this store, so it is not a logging afterthought.
"""
from __future__ import annotations

import json
import os
import secrets
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

import duckdb

SCHEMA = [
    """CREATE TABLE IF NOT EXISTS runs (
        run_id TEXT PRIMARY KEY, command TEXT, selector TEXT, started_at TIMESTAMP,
        finished_at TIMESTAMP, status TEXT)""",
    """CREATE TABLE IF NOT EXISTS tasks (
        run_id TEXT, node TEXT, status TEXT, attempts INTEGER, seconds DOUBLE,
        rows_out BIGINT, error TEXT, finished_at TIMESTAMP)""",
    """CREATE TABLE IF NOT EXISTS watermarks (
        table_name TEXT PRIMARY KEY, value TEXT, rows_seen BIGINT, updated_at TIMESTAMP)""",
    """CREATE TABLE IF NOT EXISTS manifest (
        table_name TEXT, ingest_date DATE, run_id TEXT, path TEXT, rows BIGINT,
        bytes BIGINT, checksum TEXT, created_at TIMESTAMP)""",
    """CREATE TABLE IF NOT EXISTS dq_results (
        run_id TEXT, model TEXT, name TEXT, severity TEXT, passed BOOLEAN,
        observed BIGINT, threshold BIGINT, detail TEXT, created_at TIMESTAMP)""",
    """CREATE TABLE IF NOT EXISTS model_stats (
        run_id TEXT, model TEXT, materialization TEXT, rows_after BIGINT,
        rows_changed BIGINT, seconds DOUBLE, notes JSON, created_at TIMESTAMP)""",
]


def now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def new_run_id() -> str:
    return f"{now():%Y%m%dT%H%M%S}-{secrets.token_hex(3)}"


class Metadata:
    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self.con = duckdb.connect(str(path))
        for stmt in SCHEMA:
            self.con.execute(stmt)

    def close(self) -> None:
        self.con.close()

    # ---- runs and tasks
    def start_run(self, command: str, selector: str = "") -> str:
        run_id = new_run_id()
        self.con.execute(
            "INSERT INTO runs VALUES (?,?,?,?,NULL,'running')",
            [run_id, command, selector, now()])
        return run_id

    def finish_run(self, run_id: str, status: str) -> None:
        self.con.execute("UPDATE runs SET finished_at=?, status=? WHERE run_id=?",
                         [now(), status, run_id])

    def record_task(self, run_id: str, node: str, status: str, attempts: int,
                    seconds: float, rows_out: int | None = None,
                    error: str | None = None) -> None:
        self.con.execute("INSERT INTO tasks VALUES (?,?,?,?,?,?,?,?)",
                         [run_id, node, status, attempts, seconds, rows_out, error, now()])

    def last_failed_nodes(self) -> list[str]:
        """Nodes to pick up on a resume: whatever did not succeed in the newest finished run."""
        row = self.con.execute(
            "SELECT run_id FROM runs WHERE status <> 'running' ORDER BY started_at DESC LIMIT 1"
        ).fetchone()
        if not row:
            return []
        return [r[0] for r in self.con.execute(
            "SELECT node FROM tasks WHERE run_id=? AND status IN ('failed','skipped','upstream_failed')",
            [row[0]]).fetchall()]

    # ---- watermarks
    def watermark(self, table: str) -> str | None:
        row = self.con.execute("SELECT value FROM watermarks WHERE table_name=?",
                               [table]).fetchone()
        return row[0] if row else None

    def set_watermark(self, table: str, value: str, rows_seen: int) -> None:
        self.con.execute(
            "INSERT OR REPLACE INTO watermarks VALUES (?,?,COALESCE("
            "(SELECT rows_seen FROM watermarks WHERE table_name=?),0)+?,?)",
            [table, value, table, rows_seen, now()])

    def reset_watermark(self, table: str | None = None) -> None:
        if table:
            self.con.execute("DELETE FROM watermarks WHERE table_name=?", [table])
        else:
            self.con.execute("DELETE FROM watermarks")

    # ---- manifest
    def manifest_has(self, table: str, checksum: str) -> bool:
        row = self.con.execute(
            "SELECT 1 FROM manifest WHERE table_name=? AND checksum=? LIMIT 1",
            [table, checksum]).fetchone()
        return row is not None

    def add_manifest(self, table: str, ingest_date: str, run_id: str, path: str,
                     rows: int, nbytes: int, checksum: str) -> None:
        self.con.execute("INSERT INTO manifest VALUES (?,?,?,?,?,?,?,?)",
                         [table, ingest_date, run_id, path, rows, nbytes, checksum, now()])

    def manifest_rows(self, table: str | None = None) -> list[tuple]:
        q = "SELECT table_name, ingest_date, rows, bytes, path FROM manifest"
        args: list = []
        if table:
            q += " WHERE table_name=?"
            args.append(table)
        return self.con.execute(q + " ORDER BY table_name, ingest_date", args).fetchall()

    # ---- quality and stats
    def record_dq(self, run_id: str, model: str, name: str, severity: str, passed: bool,
                  observed: int, threshold: int, detail: str) -> None:
        self.con.execute("INSERT INTO dq_results VALUES (?,?,?,?,?,?,?,?,?)",
                         [run_id, model, name, severity, passed, observed, threshold,
                          detail, now()])

    def record_model(self, run_id: str, model: str, materialization: str, rows_after: int,
                     rows_changed: int, seconds: float, notes: dict | None = None) -> None:
        self.con.execute("INSERT INTO model_stats VALUES (?,?,?,?,?,?,?,?)",
                         [run_id, model, materialization, rows_after, rows_changed,
                          seconds, json.dumps(notes or {}), now()])


@contextmanager
def timer():
    t = time.perf_counter()
    box = {"seconds": 0.0}
    try:
        yield box
    finally:
        box["seconds"] = time.perf_counter() - t


def env_home() -> Path:
    return Path(os.environ.get("PW_HOME", "data"))
