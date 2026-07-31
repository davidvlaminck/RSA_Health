import sqlite3
from datetime import datetime, timezone


class PipelineState:
    def __init__(self, db_path: str):
        self.db_path = db_path

    def _conn(self):
        return sqlite3.connect(self.db_path)

    def ensure(self):
        with self._conn() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS pipeline_state (
                    id INTEGER PRIMARY KEY CHECK(id = 1),
                    phase TEXT,
                    status TEXT,
                    updated_at DATETIME,
                    message TEXT
                )
            """)
            conn.execute(
                "INSERT OR IGNORE INTO pipeline_state (id, phase, status, updated_at, message) VALUES (?, ?, ?, ?, ?)",
                (1, "idle", "completed", _now(), ""),
            )
            conn.commit()

    def get(self) -> dict | None:
        with self._conn() as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT phase, status, updated_at, message FROM pipeline_state WHERE id = 1"
            ).fetchone()
            return dict(row) if row else None

    def update(self, phase: str, status: str, message: str = ""):
        with self._conn() as conn:
            conn.execute(
                "UPDATE pipeline_state SET phase = ?, status = ?, updated_at = ?, message = ? WHERE id = 1",
                (phase, status, _now(), message),
            )
            conn.commit()


def _now():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
