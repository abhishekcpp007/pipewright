"""Declarative data quality. Expectations live in YAML, run as SQL, land in the metadata store.

Severity decides what a failure does. A warn is written down and the run carries on. An error
opens the circuit: the model is marked failed and everything downstream of it is skipped, so a
bad batch stops at silver instead of being averaged into a gold table somebody reports from.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import duckdb
import yaml

from .config import EXPECTATIONS_FILE


@dataclass
class Expectation:
    model: str
    name: str
    type: str
    severity: str = "error"
    max_failures: int = 0
    spec: dict = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        self.spec = self.spec or {}


@dataclass
class Check:
    model: str
    name: str
    severity: str
    passed: bool
    observed: int
    threshold: int
    detail: str


def load_expectations(path: Path | None = None) -> list[Expectation]:
    raw = yaml.safe_load((path or EXPECTATIONS_FILE).read_text()) or {}
    out = []
    for model, items in raw.items():
        for item in items:
            spec = {k: v for k, v in item.items()
                    if k not in ("name", "type", "severity", "max_failures")}
            out.append(Expectation(model=model, name=item["name"], type=item["type"],
                                   severity=item.get("severity", "error"),
                                   max_failures=int(item.get("max_failures", 0)),
                                   spec=spec))
    return out


def _q(col: str) -> str:
    return f'"{col}"'


def violation_sql(exp: Expectation) -> str:
    m, s = f'"{exp.model}"', exp.spec
    t = exp.type
    if t == "not_null":
        return f"SELECT COUNT(*) FROM {m} WHERE {_q(s['column'])} IS NULL"
    if t == "unique":
        cols = ", ".join(_q(c) for c in s["columns"])
        return (f"SELECT COALESCE(SUM(n - 1), 0) FROM (SELECT {cols}, COUNT(*) AS n "
                f"FROM {m} GROUP BY {cols} HAVING COUNT(*) > 1)")
    if t == "accepted_values":
        vals = ", ".join(f"'{v}'" for v in s["values"])
        return (f"SELECT COUNT(*) FROM {m} WHERE {_q(s['column'])} IS NOT NULL "
                f"AND {_q(s['column'])} NOT IN ({vals})")
    if t == "range":
        parts = []
        if "min" in s:
            parts.append(f"{_q(s['column'])} < {s['min']}")
        if "max" in s:
            parts.append(f"{_q(s['column'])} > {s['max']}")
        return f"SELECT COUNT(*) FROM {m} WHERE {' OR '.join(parts)}"
    if t == "relationship":
        return (f"SELECT COUNT(*) FROM {m} c WHERE c.{_q(s['column'])} IS NOT NULL "
                f"AND NOT EXISTS (SELECT 1 FROM \"{s['to']}\" p "
                f"WHERE p.{_q(s['field'])} = c.{_q(s['column'])})")
    if t == "row_count_min":
        return f"SELECT GREATEST({s['value']} - COUNT(*), 0) FROM {m}"
    if t == "scd2_one_current":
        key = _q(s["key"])
        return (f"SELECT COUNT(*) FROM (SELECT {key} FROM {m} WHERE is_current "
                f"GROUP BY {key} HAVING COUNT(*) <> 1)")
    if t == "scd2_no_overlap":
        key = _q(s["key"])
        return (f"SELECT COUNT(*) FROM (SELECT {key}, valid_from, valid_to, "
                f"LEAD(valid_from) OVER (PARTITION BY {key} ORDER BY valid_from) AS nxt "
                f"FROM {m}) WHERE nxt IS NOT NULL AND valid_to > nxt")
    if t == "scd2_closed_history":
        key = _q(s["key"])
        return (f"SELECT COUNT(*) FROM (SELECT {key}, COUNT(*) FILTER (WHERE is_current) AS c "
                f"FROM {m} GROUP BY {key} HAVING c = 0)")
    if t == "freshness":
        return (f"SELECT CASE WHEN MAX({_q(s['column'])}) IS NULL THEN 1 ELSE "
                f"GREATEST(date_diff('day', CAST(MAX({_q(s['column'])}) AS DATE), "
                f"CAST('{{as_of}}' AS DATE)) - {s['max_age_days']}, 0) END FROM {m}")
    if t == "custom_sql":
        return s["sql"].replace("{{ this }}", m)
    raise ValueError(f"unknown expectation type {t!r}")


def run_expectations(con: duckdb.DuckDBPyConnection, expectations: list[Expectation],
                     models: set[str] | None = None, as_of: str | None = None,
                     meta=None) -> list[Check]:
    out: list[Check] = []
    for exp in expectations:
        if models is not None and exp.model not in models:
            continue
        if exp.type == "volume_anomaly":
            out.append(_volume_anomaly(con, exp, meta))
            continue
        sql = violation_sql(exp).replace("{as_of}", as_of or "2026-10-14")
        try:
            observed = int(con.execute(sql).fetchone()[0] or 0)
            detail = ""
        except duckdb.Error as e:
            out.append(Check(exp.model, exp.name, exp.severity, False, -1,
                             exp.max_failures, f"query failed: {e}"))
            continue
        passed = observed <= exp.max_failures
        out.append(Check(exp.model, exp.name, exp.severity, passed, observed,
                         exp.max_failures, detail))
    return out


def _volume_anomaly(con, exp: Expectation, meta) -> Check:
    """A row count that collapses is a failure even when every row left is valid."""
    tol = float(exp.spec.get("tolerance_pct", 50))
    rows_now = int(con.execute(f'SELECT COUNT(*) FROM "{exp.model}"').fetchone()[0])
    history = []
    if meta is not None:
        history = [r[0] for r in meta.con.execute(
            "SELECT rows_after FROM model_stats WHERE model=? ORDER BY created_at DESC LIMIT 7",
            [exp.model]).fetchall()]
    if len(history) < 3:
        return Check(exp.model, exp.name, "warn", True, rows_now, 0,
                     "not enough history yet")
    history = sorted(history)
    median = history[len(history) // 2]
    if median == 0:
        return Check(exp.model, exp.name, exp.severity, True, rows_now, 0, "median is zero")
    drop_pct = (median - rows_now) / median * 100
    passed = drop_pct <= tol
    return Check(exp.model, exp.name, exp.severity, passed, rows_now, int(median),
                 f"median of last {len(history)} runs {median}, now {rows_now}, "
                 f"change {-drop_pct:.1f}%")


def failed_errors(checks: list[Check]) -> list[Check]:
    return [c for c in checks if not c.passed and c.severity == "error"]
