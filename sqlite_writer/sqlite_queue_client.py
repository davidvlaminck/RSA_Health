"""Producer-helper voor de SQLite JSON-file queue.

Andere processen (rsa_health, rsa_orchestrator, enz.) gebruiken deze module
om schrijfopdrachten aan de dedicated writer aan te bieden.

Gebruik:
    from sqlite_queue_client import enqueue_sqlite_job

    job_id = enqueue_sqlite_job(
        action="insert_snapshot",
        payload={...}
    )
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
