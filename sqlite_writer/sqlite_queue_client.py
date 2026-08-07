"""Producer-helper voor de SQLite JSON-file queue.

Andere processen (rsa_health, rsa_orchestrator, enz.) gebruiken deze module
om schrijfopdrachten aan de dedicated writer aan te bieden.

De queue bestaat uit JSON-bestanden in een gedeelde directory.
Een apart proces (sqlite_file_writer) consumeert deze bestanden en
schrijft ze naar de SQLite-database.

Gebruik:
    from sqlite_writer.sqlite_queue_client import enqueue_sqlite_job

    job_id = enqueue_sqlite_job(
        action="insert_snapshot",
        payload={
            "timestamp": "2026-08-07T08:00:00Z",
            "cpu_percent": 12.5,
            "mem_used_gb": 4.2,
            "mem_total_gb": 16.0,
            "mem_percent": 26.25,
            "disk_used_gb": 120.5,
            "disk_total_gb": 256.0,
            "disk_percent": 47.07,
            "net_latency_ms": 3.2,
        },
    )

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
            starting                   - Actie wordt gestart
            running                    - Actie is bezig
            completed                  - Actie succesvol voltooid
            failed                     - Actie mislukt

Configuratie:
    De directory wordt bepaald door de omgevingsvariabele SQLITE_QUEUE_DIR
    (standaard /opt/data-platform/sqlite_queue).
"""

import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path

QUEUE_DIR = Path(os.environ.get("SQLITE_QUEUE_DIR", "/opt/data-platform/sqlite_queue"))
PENDING_DIR = QUEUE_DIR / "pending"


def enqueue_sqlite_job(action: str, payload: dict) -> str:
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
