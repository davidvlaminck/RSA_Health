"""SQLite pipeline state en JSON-file queue — gecombineerde module.

Deze module kan gekopieerd worden naar andere repos voor hergebruik.
Het bevat twee functionaliteiten:

1. **JSON-file queue producer** — `enqueue_sqlite_job()`:
   Andere processen (rsa_health, rsa_orchestrator, externe scripts) gebruiken
   deze functie om schrijfopdrachten aan de dedicated writer aan te bieden.
   De queue bestaat uit JSON-bestanden in een gedeelde directory.
   Een apart proces (sqlite_file_writer) consumeert deze bestanden en
   schrijft ze naar de SQLite-database.

2. **PipelineState reader** — `PipelineState`-klasse:
   Direct lees-toegang tot `pipeline_state` en `pipeline_state_history` in
   SQLite. Daardoor kunnen scripts bij herstart bepalen waar de pipeline
   staat, zonder de orchestrator afhankelijk te maken.

Gebruik in externe repos:

    pip install google-auth google-auth-oauthlib google-api-python-client  # alleen voor Drive markers

    from pipeline_state import enqueue_sqlite_job, PipelineState

    # Schrijf naar de queue
    enqueue_sqlite_job(
        action="update_pipeline_state",
        payload={
            "phase": "rsa_queries",
            "status": "running",
            "updated_at": "2026-08-09T20:00:00Z",
            "message": "Query's starten",
        },
    )

    # Lees pipeline status
    ps = PipelineState("/opt/data-platform/RSA_Health/health.db")
    current = ps.get()
    today = ps.get_today_updates()

Configuratie:
    De queue-directory wordt bepaald door de omgevingsvariabele SQLITE_QUEUE_DIR
    (standaard /opt/data-platform/sqlite_queue).
"""

import json
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import sqlite3

# Zorg dat de repo-root op sys.path staat (voor imports binnen dit project)
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

LOCAL_TZ = ZoneInfo("Europe/Brussels")

QUEUE_DIR = Path(os.environ.get("SQLITE_QUEUE_DIR", "/opt/data-platform/sqlite_queue"))
PENDING_DIR = QUEUE_DIR / "pending"


def enqueue_sqlite_job(action: str, payload: dict) -> str:
    """Plaats een JSON-job in de file queue voor de dedicated SQLite writer.

    Retourneert:
        str: job_id van de geplaatste job (voor debugging/tracking)

    Ondersteunde actions:
        insert_snapshot
            payload: {
                "timestamp": str (verplicht, ISO-8601),
                "cpu_percent": float (optioneel),
                "mem_used_gb": float (optioneel),
                "mem_total_gb": float (optioneel),
                "mem_percent": float (optioneel),
                "disk_used_gb": float (optioneel),
                "disk_total_gb": float (optioneel),
                "disk_percent": float (optioneel),
                "net_latency_ms": float (optioneel),
            }

        insert_db_snapshot
            payload: {
                "timestamp": str (verplicht),
                "db_name": str (verplicht),
                "db_type": str (optioneel, default "unknown"),
                "status": str (optioneel, default "error"),
                "latency_ms": float (optioneel),
                "error": str (optioneel),
            }

        prune_snapshots
            payload: {
                "cutoff": str (verplicht, ISO-8601 timestamp)
            }

        update_pipeline_state
            payload: {
                "phase": str (verplicht),
                "status": str (verplicht),
                "updated_at": str (verplicht, ISO-8601),
                "message": str (optioneel, default ""),
            }

            Ondersteunde phases:
                idle                       - Wacht op volgende pipeline run
                sharepoint_to_drive        - SharePoint → Drive marker detectie
                drive_download             - Drive download gestart
                arango_sync                - ArangoDB synchronisatie
                postgis_sync_pausing       - PostGIS sync pauzeren
                postgis_sync_paused        - PostGIS sync gepauzeerd
                rsa_queries                - RSA queries uitvoeren
                postgis_sync_resuming      - PostGIS sync hervatten
                postgis_sync_running       - PostGIS sync actief
                drive_upload               - Drive upload gestart
                drive_to_sharepoint        - Drive → SharePoint marker detectie

            Ondersteunde statussen:
                starting                   — Actie wordt gestart
                running                    — Actie is bezig
                completed                  — Actie succesvol voltooid
                failed                     — Actie mislukt
                aborted                    — Actie werd afgebroken (intern)
                time-out                   — Actie overschreef de tijgslimiet.
                                             RSA rapporteert dit voor rsa_queries
                                             wanneer het 3 uur niet afkrijgt.
                                             De orchestrator behandelt time-out
                                             gelijk aan completed: de pipeline gaat
                                             verder naar PostGIS resume + drive_upload
    """
    PENDING_DIR.mkdir(parents=True, exist_ok=True)

    job_id = (
        f"{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S_%f')}_"
        f"{uuid.uuid4().hex}"
    )

    tmp_path = PENDING_DIR / f"{job_id}.tmp"
    final_path = PENDING_DIR / f"{job_id}.json"

    message = {
        "job_id": job_id,
        "action": action,
        "payload": payload,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }

    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(message, f, ensure_ascii=False)
        f.flush()
        os.fsync(f.fileno())

    os.replace(tmp_path, final_path)

    return job_id


def _now():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class PipelineState:
    """Reader voor de pipeline status in SQLite.

    Alle methodes zijn read-only. Schrijven gaat via enqueue_sqlite_job().
    """

    def __init__(self, db_path: str):
        self.db_path = db_path

    def _conn(self):
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")
        return conn

    def get(self) -> dict | None:
        """Geeft de actuele pipeline status (één rij uit pipeline_state)."""
        conn = self._conn()
        try:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT phase, status, updated_at, message FROM pipeline_state WHERE id = 1"
            ).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def get_today_updates(self) -> list[dict]:
        """Geeft alle pipeline updates van vandaag (Brussels local time).

        Combineert pipeline_state (huidige status) en pipeline_state_history
        (historische updates van vandaag). Wordt gebruikt door scripts bij
        herstart om te bepalen waar in de pipeline ze zitten.
        """
        conn = self._conn()
        try:
            conn.row_factory = sqlite3.Row
            now_local = datetime.now(LOCAL_TZ)
            today_start = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
            today_start_utc = today_start.astimezone(timezone.utc)
            cutoff = today_start_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
            rows = conn.execute(
                """
                SELECT phase, status, updated_at, message
                FROM pipeline_state_history
                WHERE updated_at >= ?
                UNION ALL
                SELECT phase, status, updated_at, message
                FROM pipeline_state
                WHERE updated_at >= ?
                ORDER BY updated_at ASC
                """,
                (cutoff, cutoff),
            ).fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    def update(self, phase: str, status: str, message: str = ""):
        """Queue een status-update voor de dedicated writer (async)."""
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
        """Geeft de laatste known-status per fase, gegroepeerd per fase."""
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

    def reset(self):
        conn = self._conn()
        try:
            conn.execute("DELETE FROM pipeline_state")
            conn.execute("DELETE FROM pipeline_state_history")
            conn.execute(
                "INSERT INTO pipeline_state (id, phase, status, updated_at, message) "
                "VALUES (?, ?, ?, ?, ?)",
                (1, "idle", "completed", _now(), "Pipeline reset"),
            )
            conn.commit()
        finally:
            conn.close()
