from __future__ import annotations

import pytest

from pw.cli import main


def test_gen_then_run_then_test(tmp_path, capsys):
    home = str(tmp_path / "h")
    assert main(["--home", home, "gen", "--days", "3"]) == 0
    assert main(["--home", home, "run", "--date", "2026-10-01", "-v"]) == 0
    assert main(["--home", home, "run", "--date", "2026-10-02"]) == 0
    assert main(["--home", home, "test", "--as-of", "2026-10-02"]) == 0
    out = capsys.readouterr().out
    assert "defects injected on purpose" in out
    assert "models" in out


@pytest.mark.parametrize("fmt", ["text", "dot", "mermaid"])
def test_lineage_renders(fmt, capsys):
    assert main(["lineage", "--format", fmt]) == 0
    assert "fct_orders" in capsys.readouterr().out


def test_state_and_report_read_a_built_warehouse(built, capsys):
    assert main(["--home", str(built), "state"]) == 0
    assert main(["--home", str(built), "report"]) == 0
    out = capsys.readouterr().out
    assert "watermarks" in out and "quarantine by reason" in out


def test_replay_and_backfill_through_the_cli(tmp_path, capsys):
    home = str(tmp_path / "h")
    assert main(["--home", home, "gen", "--days", "3"]) == 0
    assert main(["--home", home, "replay", "--days", "3"]) == 0
    assert main(["--home", home, "backfill", "--start", "2026-10-01",
                 "--end", "2026-10-03"]) == 0
    assert "3 partitions reprocessed, 0 with failures" in capsys.readouterr().out


def test_select_one_model_through_the_cli(built_copy):
    assert main(["--home", str(built_copy), "run", "--select", "tag:gold",
                 "--no-extract", "--date", "2026-10-06"]) == 0


def test_an_unknown_command_exits_nonzero():
    with pytest.raises(SystemExit):
        main(["nonsense"])
