from __future__ import annotations

import duckdb
import pytest

from pw.quality import (
    Expectation,
    failed_errors,
    load_expectations,
    run_expectations,
    violation_sql,
)


@pytest.fixture
def con():
    c = duckdb.connect()
    c.execute("CREATE TABLE t (id INTEGER, city VARCHAR, amount INTEGER, ts DATE)")
    c.executemany("INSERT INTO t VALUES (?, ?, ?, ?)", [
        (1, "Pune", 100, "2026-10-01"),
        (2, None, -5, "2026-10-02"),
        (2, "Lucknow", 50, "2026-10-03"),
        (3, "Mars", 0, "2026-10-03"),
    ])
    c.execute("CREATE TABLE parent (id INTEGER)")
    c.execute("INSERT INTO parent VALUES (1), (2)")
    return c


def check(con, type_, **spec):
    exp = Expectation(model="t", name=type_, type=type_, spec=spec)
    return con.execute(violation_sql(exp).replace("{as_of}", "2026-10-04")).fetchone()[0]


def test_not_null(con):
    assert check(con, "not_null", column="city") == 1


def test_unique(con):
    assert check(con, "unique", columns=["id"]) == 1
    assert check(con, "unique", columns=["id", "city"]) == 0


def test_accepted_values_ignores_nulls(con):
    assert check(con, "accepted_values", column="city", values=["Pune", "Lucknow"]) == 1


def test_range_both_ends(con):
    assert check(con, "range", column="amount", min=1) == 2
    assert check(con, "range", column="amount", max=60) == 1
    assert check(con, "range", column="amount", min=1, max=60) == 3


def test_relationship_finds_the_orphan(con):
    assert check(con, "relationship", column="id", to="parent", field="id") == 1


def test_row_count_min_reports_the_shortfall(con):
    assert check(con, "row_count_min", value=10) == 6
    assert check(con, "row_count_min", value=2) == 0


def test_freshness_counts_days_past_the_limit(con):
    assert check(con, "freshness", column="ts", max_age_days=5) == 0
    assert check(con, "freshness", column="ts", max_age_days=0) == 1


def test_custom_sql_is_rendered_against_the_model(con):
    assert check(con, "custom_sql",
                 sql="SELECT COUNT(*) FROM {{ this }} WHERE city = 'Mars'") == 1


def test_an_unknown_type_is_rejected():
    with pytest.raises(ValueError, match="unknown expectation"):
        violation_sql(Expectation("t", "x", "telepathy"))


@pytest.fixture
def dim(con):
    con.execute("CREATE TABLE d (id INTEGER, id_sk INTEGER, valid_from TIMESTAMP, "
                "valid_to TIMESTAMP, is_current BOOLEAN)")
    return con


def test_scd2_catches_two_current_rows(dim):
    dim.executemany("INSERT INTO d VALUES (?, ?, ?, ?, ?)", [
        (1, 1, "2026-01-01", "9999-12-31", True),
        (1, 2, "2026-02-01", "9999-12-31", True),
    ])
    exp = Expectation("d", "x", "scd2_one_current", spec={"key": "id"})
    assert dim.execute(violation_sql(exp)).fetchone()[0] == 1


def test_scd2_catches_overlapping_windows(dim):
    dim.executemany("INSERT INTO d VALUES (?, ?, ?, ?, ?)", [
        (1, 1, "2026-01-01", "2026-03-01", False),
        (1, 2, "2026-02-01", "9999-12-31", True),
    ])
    exp = Expectation("d", "x", "scd2_no_overlap", spec={"key": "id"})
    assert dim.execute(violation_sql(exp)).fetchone()[0] == 1


def test_scd2_catches_a_key_with_no_open_version(dim):
    dim.execute("INSERT INTO d VALUES (1, 1, '2026-01-01', '2026-03-01', false)")
    exp = Expectation("d", "x", "scd2_closed_history", spec={"key": "id"})
    assert dim.execute(violation_sql(exp)).fetchone()[0] == 1


def test_severity_decides_whether_a_failure_stops_the_run(con):
    exps = [
        Expectation("t", "hard", "not_null", severity="error", spec={"column": "city"}),
        Expectation("t", "soft", "not_null", severity="warn", spec={"column": "city"}),
    ]
    checks = run_expectations(con, exps, {"t"})
    assert [c.passed for c in checks] == [False, False]
    assert [c.name for c in failed_errors(checks)] == ["hard"]


def test_max_failures_is_a_tolerance_not_a_target(con):
    exps = [Expectation("t", "tolerated", "not_null", max_failures=1, spec={"column": "city"})]
    assert run_expectations(con, exps, {"t"})[0].passed is True


def test_a_broken_expectation_query_fails_loudly(con):
    exps = [Expectation("t", "bad", "custom_sql",
                        spec={"sql": "SELECT COUNT(*) FROM no_such_table"})]
    c = run_expectations(con, exps, {"t"})[0]
    assert c.passed is False and "query failed" in c.detail


def test_volume_anomaly_waits_for_history(con, tmp_path):
    from pw.metadata import Metadata
    meta = Metadata(tmp_path / "m.duckdb")
    exps = [Expectation("t", "volume", "volume_anomaly", spec={"tolerance_pct": 10})]
    c = run_expectations(con, exps, {"t"}, meta=meta)[0]
    assert c.passed and "not enough history" in c.detail
    for n in (100, 100, 100, 100):
        meta.record_model("r", "t", "table", n, n, 0.1)
    c = run_expectations(con, exps, {"t"}, meta=meta)[0]
    assert c.passed is False
    assert "96.0%" in c.detail
    meta.close()


def test_the_shipped_expectations_all_have_a_known_type():
    from pw.quality import Expectation as E
    for exp in load_expectations():
        assert exp.severity in ("error", "warn")
        if exp.type != "volume_anomaly":
            violation_sql(E(exp.model, exp.name, exp.type, spec=exp.spec))
