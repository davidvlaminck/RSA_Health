import sys
from datetime import datetime, timezone
from pathlib import Path

import sqlite3

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from sqlite_writer.sqlite_queue_client import enqueue_sqlite_job


class PipelineState:
    def __init__(self, db_path: str):
        self.db_path = db_path

    def _conn(self):
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")
        return conn

    def get(self) -> dict | None:
        conn = self._conn()
        try:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT phase, status, updated_at, message FROM pipeline_state WHERE id = 1"
            ).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def update(self, phase: str, status: str, message: str = ""):
        enqueue_sqlite_job(
            action="update_pipeline_state",
            payload={
                "phase": phase,
                "status": status,
                "updated_at": _now(),
                "message": message,
            },
        )

    def get_history(self) -> dict:
        conn = self._conn()
        try:
            conn.row_factory = sqlite3.Row
            current = conn.execute(
                "SELECT phase, status, updated_at, message FROM pipeline_state WHERE id = 1"
            ).fetchone()
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
            if current:
                cp = current["phase"]
                result[cp] = [
                    {
                        "status": current["status"],
                        "updated_at": current["updated_at"],
                        "message": current["message"],
                    }
                ]
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
        finally:
            conn.close()

    def clear_history(self):
        conn = self._conn()
        try:
            conn.execute("DELETE FROM pipeline_state_history")
            conn.commit()
        finally:
            conn.close()


def _now():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
