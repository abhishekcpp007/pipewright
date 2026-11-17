"""The landing zone: hive partitioned Parquet, written atomically, hashed by content.

Two things here matter. The write goes to a temp name and is renamed into place, so a reader
never sees a half written file and a crashed run leaves no partial partition. The checksum is
over the data, not the file, so the same batch extracted twice hashes the same even if Parquet
encodes it differently, and the manifest can refuse the second copy.
"""
from __future__ import annotations

import hashlib
import io
import os
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

SQLITE_TO_ARROW = {
    "INTEGER": pa.int64(),
    "TEXT": pa.string(),
    "REAL": pa.float64(),
    "BLOB": pa.binary(),
    "": pa.string(),
}


def arrow_schema(columns: list[tuple[str, str]]) -> pa.Schema:
    """Build the schema from the source DDL instead of inferring it from values.

    Inference bites on a column that is all NULL in this batch: Arrow calls it a null type,
    Parquet stores it as one, and a later batch with real strings in it will not union with
    that file. promo_code is exactly that column.
    """
    return pa.schema([(name, SQLITE_TO_ARROW.get(t.upper().split("(")[0], pa.string()))
                      for name, t in columns])


def content_checksum(table: pa.Table) -> str:
    sink = io.BytesIO()
    with pa.ipc.new_stream(sink, table.schema) as w:
        w.write_table(table)
    return hashlib.sha256(sink.getvalue()).hexdigest()


def write_atomic(table: pa.Table, dest: Path) -> int:
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(dest.name + ".tmp")
    pq.write_table(table, tmp, compression="zstd", use_dictionary=True)
    with open(tmp, "rb") as fh:
        os.fsync(fh.fileno())
    os.replace(tmp, dest)
    return dest.stat().st_size


def partition_path(root: Path, table: str, ingest_date: str, run_id: str) -> Path:
    return root / "bronze" / table / f"ingest_date={ingest_date}" / f"part-{run_id}.parquet"


def glob_for(root: Path, table: str) -> str:
    return str(root / "bronze" / table / "**" / "*.parquet")


def partitions(root: Path, table: str) -> list[str]:
    base = root / "bronze" / table
    if not base.exists():
        return []
    return sorted(p.name.split("=", 1)[1] for p in base.iterdir() if p.is_dir())
