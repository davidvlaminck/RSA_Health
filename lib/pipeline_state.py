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
            conn.execute("""
                CREATE TABLE IF NOT EXISTS pipeline_state_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    phase TEXT NOT NULL,
                    status TEXT NOT NULL,
                    updated_at DATETIME NOT NULL,
                    message TEXT DEFAULT ''
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_pipeline_history_phase_status
                ON pipeline_state_history(phase, status, updated_at DESC)
            """)
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
            conn.execute(
                "INSERT INTO pipeline_state_history (phase, status, updated_at, message) VALUES (?, ?, ?, ?)",
                (phase, status, _now(), message),
            )
            conn.commit()

    def get_history(self) -> dict:
        with self._conn() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute("""
                SELECT h.phase, h.status, h.updated_at, h.message
                FROM pipeline_state_history h
                INNER JOIN (
                    SELECT phase, status, MAX(updated_at) AS max_updated_at
                    FROM pipeline_state_history
                    GROUP BY phase, status
                ) latest
                ON h.phase = latest.phase
                AND h.status = latest.status
                AND h.updated_at = latest.max_updated_at
                ORDER BY h.phase, h.updated_at DESC
            """).fetchall()
            result: dict = {}
            for row in rows:
                phase = row["phase"]
                if phase not in result:
                    result[phase] = []
                result[phase].append(
                    {
                        "status": row["status"],
                        "updated_at": row["updated_at"],
                        "message": row["message"],
                    }
                )
            return result


def _now():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
