from __future__ import annotations

import os
import sqlite3
from typing import Iterable, List, Tuple

from .config import settings


def ensure_db_dir() -> None:
    db_dir = os.path.dirname(os.path.abspath(settings.db_path)) or "."
    os.makedirs(db_dir, exist_ok=True)


def get_connection() -> sqlite3.Connection:
    ensure_db_dir()
    conn = sqlite3.connect(settings.db_path)
    conn.row_factory = sqlite3.Row
    return conn


def create_table_if_not_exists(columns: List[str]) -> None:
    # All TEXT to keep ingestion simple and lossless; downstream can cast as needed
    col_defs = ", ".join([f'"{c}" TEXT' for c in columns])
    ddl = f"CREATE TABLE IF NOT EXISTS {settings.db_table_affordable_housing} ( {col_defs} )"
    with get_connection() as conn:
        conn.execute(ddl)


def insert_rows(columns: List[str], rows: Iterable[Tuple]) -> None:
    placeholders = ",".join(["?"] * len(columns))
    cols_csv = ", ".join([f'"{c}"' for c in columns])
    sql = f"INSERT INTO {settings.db_table_affordable_housing} ({cols_csv}) VALUES ({placeholders})"
    with get_connection() as conn:
        conn.executemany(sql, rows)
        conn.commit()


