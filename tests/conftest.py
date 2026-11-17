from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from pw.config import paths
from pw.runner import replay
from pw.source import generate, write_injected

DAYS = 6


@pytest.fixture(scope="session")
def built(tmp_path_factory) -> Path:
    """One small warehouse, built day by day the way the scheduler would. Shared, read only."""
    home = tmp_path_factory.mktemp("pw-built")
    p = paths(home).prepare()
    defects = generate(p.source_db, days=DAYS)
    write_injected(p.injected, defects)
    reports = replay(home, days=DAYS, quiet=True)
    assert all(r.status == "success" for r in reports), [r.failures for r in reports]
    return home


@pytest.fixture
def built_copy(built, tmp_path) -> Path:
    home = tmp_path / "copy"
    shutil.copytree(built, home)
    return home


@pytest.fixture
def fresh(tmp_path) -> Path:
    home = tmp_path / "fresh"
    p = paths(home).prepare()
    generate(p.source_db, days=3)
    return home
