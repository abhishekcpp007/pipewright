"""Paths and knobs. Everything resolves from PW_HOME so tests can run in a tmpdir."""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

PACKAGE_DIR = Path(__file__).resolve().parent
MODEL_DIR = PACKAGE_DIR / "models"
EXPECTATIONS_FILE = PACKAGE_DIR / "expectations.yml"


@dataclass(frozen=True)
class Paths:
    home: Path

    @property
    def source_db(self) -> Path:
        return self.home / "source" / "shop.sqlite3"

    @property
    def injected(self) -> Path:
        return self.home / "source" / "injected.json"

    @property
    def lake(self) -> Path:
        return self.home / "lake"

    @property
    def warehouse(self) -> Path:
        return self.home / "warehouse" / "pw.duckdb"

    @property
    def metadata(self) -> Path:
        return self.home / "warehouse" / "meta.duckdb"

    def bronze_dir(self, table: str) -> Path:
        return self.lake / "bronze" / table

    def prepare(self) -> Paths:
        for p in (self.source_db.parent, self.lake, self.warehouse.parent):
            p.mkdir(parents=True, exist_ok=True)
        return self


def paths(home: str | os.PathLike[str] | None = None) -> Paths:
    root = Path(home) if home is not None else Path(os.environ.get("PW_HOME", "data"))
    return Paths(root.resolve())


# The generator's clock. Fixed so every machine builds the same warehouse.
START_DATE = "2026-10-01"
DEFAULT_DAYS = 14
# Day index at which the source system grows a column. Anything before this has no promo_code.
SCHEMA_DRIFT_DAY = 8
