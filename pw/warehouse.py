"""The warehouse: render a model, then build it the way its config asks for.

Four materializations. view and table are the easy ones. incremental merges a batch into a
table on a unique key and, when the model declares a watermark, refuses a row that is older
than the one already stored. scd2 keeps a validity window per version of a dimension row.
"""
from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from pathlib import Path

import duckdb

from .dag import Model

END_OF_TIME = "9999-12-31 23:59:59"
INCREMENTAL_BLOCK = re.compile(
    r"\{\%\s*if is_incremental\(\)\s*\%\}(.*?)\{\%\s*endif\s*\%\}", re.DOTALL)
REF = re.compile(r"\{\{\s*ref\('([a-z0-9_]+)'\)\s*\}\}")
SAFE_COL = re.compile(r"\{\{\s*safe_col\('([a-z0-9_]+)',\s*'([a-z0-9_]+)',\s*'(\w+)'\)\s*\}\}")


def sk_name(unique_key: str) -> str:
    return unique_key[:-3] + "_sk" if unique_key.endswith("_id") else unique_key + "_sk"


def stable_sk(key, valid_from) -> int:
    """A surrogate key that is a hash of the business key and the moment the version opened.

    The first version of this was a sequence, and a full rebuild then handed every row a
    different key while every other column stayed the same. Facts built before the rebuild
    pointed at keys that no longer meant anything. Hashing costs eight bytes of width and buys
    a dimension that rebuilds to the same keys on any machine, in any order.
    """
    h = hashlib.blake2b(f"{key}|{valid_from}".encode(), digest_size=8).digest()
    return int.from_bytes(h, "big") >> 1


@dataclass
class BuildResult:
    model: str
    materialization: str
    incremental: bool
    rows_after: int
    rows_changed: int
    notes: dict


class Warehouse:
    def __init__(self, db_path: Path, lake_root: Path) -> None:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self.con = duckdb.connect(str(db_path))
        self.lake_root = lake_root

    def close(self) -> None:
        self.con.close()

    # ---- introspection
    def exists(self, name: str) -> bool:
        row = self.con.execute(
            "SELECT 1 FROM duckdb_tables() WHERE table_name = ? UNION ALL "
            "SELECT 1 FROM duckdb_views() WHERE view_name = ? LIMIT 1", [name, name]).fetchone()
        return row is not None

    def is_table(self, name: str) -> bool:
        return self.con.execute("SELECT 1 FROM duckdb_tables() WHERE table_name=?",
                                [name]).fetchone() is not None

    def columns(self, name: str) -> list[str]:
        try:
            return [d[0] for d in self.con.execute(f'SELECT * FROM "{name}" LIMIT 0').description]
        except duckdb.Error:
            return []

    def count(self, name: str) -> int:
        if not self.exists(name):
            return 0
        return self.con.execute(f'SELECT COUNT(*) FROM "{name}"').fetchone()[0]

    # ---- rendering
    def render(self, model: Model, batch_date: str | None, incremental: bool) -> str:
        sql = model.sql
        sql = INCREMENTAL_BLOCK.sub(lambda m: m.group(1) if incremental else "", sql)
        sql = REF.sub(lambda m: f'"{m.group(1)}"', sql)
        sql = SAFE_COL.sub(self._safe_col, sql)
        sql = sql.replace("{{ lake }}", str(self.lake_root))
        sql = sql.replace("{{ this }}", f'"{model.name}"')
        sql = sql.replace("{{ batch_date }}", batch_date or "1900-01-01")
        leftover = re.search(r"\{\{(.*?)\}\}", sql)
        if leftover:
            raise ValueError(f"{model.name}: unresolved template {leftover.group(0)!r}")
        return sql

    def _safe_col(self, m: re.Match) -> str:
        rel, col, typ = m.group(1), m.group(2), m.group(3)
        if col in self.columns(rel):
            return col
        # The source had not grown this column yet on the days this run is reading. Hand the
        # model a typed NULL so the warehouse schema is the same either way.
        return f"cast(null as {typ})"

    # ---- building
    def build(self, model: Model, batch_date: str | None = None,
              full_refresh: bool = False) -> BuildResult:
        can_increment = (model.materialized in ("incremental", "scd2")
                         and self.is_table(model.name) and not full_refresh)
        sql = self.render(model, batch_date, can_increment)
        body = strip_config(sql)
        if model.materialized == "view":
            self.con.execute(f'CREATE OR REPLACE VIEW "{model.name}" AS {body}')
            return BuildResult(model.name, "view", False, self.count(model.name), 0, {})
        if model.materialized == "table":
            self.con.execute(f'CREATE OR REPLACE TABLE "{model.name}" AS {body}')
            n = self.count(model.name)
            return BuildResult(model.name, "table", False, n, n, {})
        if model.materialized == "incremental":
            return self._incremental(model, body, can_increment)
        return self._scd2(model, body, can_increment)

    def _incremental(self, model: Model, body: str, incremental: bool) -> BuildResult:
        key = model.unique_key
        if not incremental:
            self.con.execute(f'CREATE OR REPLACE TABLE "{model.name}" AS {body}')
            n = self.count(model.name)
            return BuildResult(model.name, "incremental", False, n, n, {})
        stage = f"stage_{model.name}"
        self.con.execute(f'CREATE OR REPLACE TEMP TABLE "{stage}" AS {body}')
        changed = self.con.execute(f'SELECT COUNT(*) FROM "{stage}"').fetchone()[0]
        notes: dict = {}
        self.con.execute("BEGIN TRANSACTION")
        try:
            if model.watermark:
                # Only give up the stored row if the incoming one is at least as new. This is
                # the guard that stops an out of order replica read from reverting a row.
                guard = (f'DELETE FROM "{model.name}" t WHERE EXISTS ('
                         f'SELECT 1 FROM "{stage}" s WHERE s."{key}" = t."{key}" '
                         f'AND s."{model.watermark}" >= t."{model.watermark}")')
            else:
                guard = (f'DELETE FROM "{model.name}" t WHERE EXISTS ('
                         f'SELECT 1 FROM "{stage}" s WHERE s."{key}" = t."{key}")')
            self.con.execute(guard)
            inserted = self.con.execute(
                f'INSERT INTO "{model.name}" SELECT * FROM "{stage}" s WHERE NOT EXISTS ('
                f'SELECT 1 FROM "{model.name}" t WHERE t."{key}" = s."{key}")'
            ).fetchone()
            self.con.execute("COMMIT")
        except Exception:
            self.con.execute("ROLLBACK")
            raise
        notes["rows_inserted"] = int(inserted[0]) if inserted else 0
        notes["rows_rejected_as_stale"] = changed - notes["rows_inserted"]
        return BuildResult(model.name, "incremental", True, self.count(model.name), changed, notes)

    # ---- slowly changing dimension, type 2
    def _scd2(self, model: Model, body: str, incremental: bool) -> BuildResult:
        key = model.unique_key
        sk = sk_name(key)
        wm = model.watermark or "updated_at"
        stage = f"stage_{model.name}"
        self.con.execute(f'CREATE OR REPLACE TEMP TABLE "{stage}" AS {body}')
        cols = self.columns(stage)
        for c in (key, wm, *model.check_cols):
            if c not in cols:
                raise ValueError(f"{model.name}: scd2 needs column {c!r} in the model's select")

        if not incremental or not self.is_table(model.name):
            coldefs = ", ".join(f'"{c}" {t}' for c, t in self._stage_types(stage))
            self.con.execute(f'DROP TABLE IF EXISTS "{model.name}"')
            self.con.execute(
                f'CREATE TABLE "{model.name}" ("{sk}" BIGINT, {coldefs}, '
                f'"valid_from" TIMESTAMP, "valid_to" TIMESTAMP, '
                f'"is_current" BOOLEAN, "version" INTEGER)')

        rows = self.con.execute(
            f'SELECT {", ".join(chr(34) + c + chr(34) for c in cols)} FROM "{stage}" '
            f'ORDER BY "{key}", "{wm}"').fetchall()
        state = {
            r[0]: {"sk": r[1], "version": r[2], "valid_from": r[3],
                   "checks": tuple(r[4:])}
            for r in self.con.execute(
                f'SELECT "{key}", "{sk}", "version", "valid_from", '
                f'{", ".join(chr(34) + c + chr(34) for c in model.check_cols)} '
                f'FROM "{model.name}" WHERE is_current').fetchall()
        }
        ix = {c: i for i, c in enumerate(cols)}
        check_ix = [ix[c] for c in model.check_cols]
        closures: list[tuple] = []
        inserts: list[list] = []
        # Where in this batch's inserts a surrogate key lives. A key whose second and third
        # version arrive in the same batch has to be closed in the pending row, because the
        # row it would otherwise close is not in the table yet. Missing this left two rows
        # per customer flagged is_current, and the scd2_one_current test is what caught it.
        pending: dict[int, int] = {}
        late = unchanged = new_keys = new_versions = 0

        for row in rows:
            k, ts = row[ix[key]], row[ix[wm]]
            checks = tuple(row[i] for i in check_ix)
            cur = state.get(k)
            if cur is None:
                new_sk = stable_sk(k, ts)
                pending[new_sk] = len(inserts)
                inserts.append([new_sk, *row, ts, END_OF_TIME, True, 1])
                state[k] = {"sk": new_sk, "version": 1, "valid_from": ts, "checks": checks}
                new_keys += 1
            elif ts <= cur["valid_from"]:
                # Older than the version already open. A lagging replica, not a change.
                late += 1
            elif checks == cur["checks"]:
                unchanged += 1
            else:
                if cur["sk"] in pending:
                    row_ix = pending[cur["sk"]]
                    inserts[row_ix][-3] = ts
                    inserts[row_ix][-2] = False
                else:
                    closures.append((ts, cur["sk"]))
                v = cur["version"] + 1
                new_sk = stable_sk(k, ts)
                pending[new_sk] = len(inserts)
                inserts.append([new_sk, *row, ts, END_OF_TIME, True, v])
                state[k] = {"sk": new_sk, "version": v, "valid_from": ts, "checks": checks}
                new_versions += 1

        self.con.execute("BEGIN TRANSACTION")
        try:
            if closures:
                self.con.executemany(
                    f'UPDATE "{model.name}" SET valid_to = ?, is_current = false WHERE "{sk}" = ?',
                    closures)
            if inserts:
                placeholders = ",".join("?" * (len(cols) + 5))
                self.con.executemany(
                    f'INSERT INTO "{model.name}" VALUES ({placeholders})', inserts)
            self.con.execute("COMMIT")
        except Exception:
            self.con.execute("ROLLBACK")
            raise

        notes = {"new_keys": new_keys, "new_versions": new_versions,
                 "unchanged": unchanged, "late_versions_dropped": late}
        return BuildResult(model.name, "scd2", incremental, self.count(model.name),
                           len(inserts), notes)

    def _stage_types(self, stage: str) -> list[tuple[str, str]]:
        return [(r[0], r[1]) for r in self.con.execute(
            f'SELECT column_name, column_type FROM (DESCRIBE "{stage}")').fetchall()]


def strip_config(sql: str) -> str:
    """Drop the leading comment block so the body starts at the query."""
    lines = sql.splitlines()
    i = 0
    while i < len(lines) and (lines[i].strip().startswith("--") or not lines[i].strip()):
        i += 1
    return "\n".join(lines[i:]).strip()
