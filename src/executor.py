import time
import sqlite3

from pathlib import Path

from src.config import SETTINGS
from src.my_types import SQLExecutionOutput


class SQLiteExecutor:
    def __init__(self, db_path: str | Path = SETTINGS.db_path) -> None:
        self.db_path = Path(db_path)

    def run(self, sql: str | None) -> SQLExecutionOutput:
        start = time.perf_counter_ns()
        error = None
        rows = []
        row_count = 0

        if sql is None:
            return SQLExecutionOutput(
                rows=[],
                row_count=0,
                timing_ms=(time.perf_counter_ns() - start) / 1_000_000,
                error=None,
            )

        try:
            uri = f"file:{self.db_path}?mode=ro"
            with sqlite3.connect(uri, uri=True) as conn:
                conn.row_factory = sqlite3.Row
                cur = conn.cursor()
                cur.execute(sql)
                rows = [dict(r) for r in cur.fetchall()]
                row_count = len(rows)
        except Exception as exc:
            error = str(exc)
            rows = []
            row_count = 0

        return SQLExecutionOutput(
            rows=rows,
            row_count=row_count,
            timing_ms=(time.perf_counter_ns() - start) / 1_000_000,
            error=error,
        )
