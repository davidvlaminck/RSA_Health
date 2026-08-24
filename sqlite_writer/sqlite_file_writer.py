#!/usr/bin/env python3
"""Dedicated SQLite writer.

Leest JSON-jobbestanden uit een file-queue en schrijft ze naar SQLite.
Dit is het enige proces dat naar de SQLite-database mag schrijven.
"""

import json
import logging
import os
import shutil
import sys
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path

DB_PATH = Path(os.environ.get("DB_PATH", "/opt/data-platform/RSA_Health/health.db"))
QUEUE_DIR = Path(os.environ.get("SQLITE_QUEUE_DIR", "/opt/data-platform/sqlite_queue"))
PENDING_DIR = QUEUE_DIR / "pending"
PROCESSING_DIR = QUEUE_DIR / "processing"
DONE_DIR = QUEUE_DIR / "done"
FAILED_DIR = QUEUE_DIR / "failed"

POLL_INTERVAL_SECONDS = 1

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("sqlite_file_writer")


def ensure_directories() -> None:
    for directory in [PENDING_DIR, PROCESSING_DIR, DONE_DIR, FAILED_DIR]:
        directory.mkdir(parents=True, exist_ok=True)


def recover_processing_jobs() -> None:
    for stuck_file in PROCESSING_DIR.glob("*.json"):
        target = PENDING_DIR / stuck_file.name
        logger.warning("Herstel processing job naar pending: %s", stuck_file.name)
        os.replace(stuck_file, target)


def prune_done_queue(retention_days: int, done_dir=None) -> int:
    target_dir = done_dir if done_dir is not None else DONE_DIR
    cutoff = time.time() - retention_days * 86400
    removed = 0
    for done_file in target_dir.glob("*.json"):
        try:
            if done_file.stat().st_mtime < cutoff:
                done_file.unlink()
                removed += 1
        except FileNotFoundError:
            continue
    logger.info("Prune: %d done-jobs ouder dan %d dagen verwijderd", removed, retention_days)
    return removed


def open_database() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA busy_timeout=30000;")
    return conn


def ensure_database_schema(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            cpu_percent REAL,
            mem_used_gb REAL,
            mem_total_gb REAL,
            mem_percent REAL,
            disk_used_gb REAL,
            disk_total_gb REAL,
            disk_percent REAL,
            net_latency_ms REAL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS db_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            db_name TEXT NOT NULL,
            db_type TEXT NOT NULL,
            status TEXT NOT NULL,
            latency_ms REAL,
            error TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_state (
            id INTEGER PRIMARY KEY CHECK(id = 1),
            phase TEXT,
            status TEXT,
            updated_at DATETIME,
            message TEXT
        )
    """)
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
        CREATE TABLE IF NOT EXISTS processed_jobs (
            job_id TEXT PRIMARY KEY,
            processed_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_snapshots_timestamp ON snapshots (timestamp)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_db_snapshots_timestamp ON db_snapshots (timestamp)"
    )
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_pipeline_history_phase_status
        ON pipeline_state_history(phase, status, updated_at DESC)
    """)
    conn.execute(
        "INSERT OR IGNORE INTO pipeline_state (id, phase, status, updated_at, message) "
        "VALUES (?, ?, ?, ?, ?)",
        (
            1,
            "idle",
            "completed",
            datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "",
        ),
    )
    conn.execute("""
        CREATE TRIGGER IF NOT EXISTS trg_pipeline_state_history
        AFTER UPDATE ON pipeline_state
        FOR EACH ROW
        WHEN OLD.phase != NEW.phase OR OLD.status != NEW.status
        BEGIN
            INSERT INTO pipeline_state_history (phase, status, updated_at, message)
            VALUES (OLD.phase, OLD.status, OLD.updated_at, OLD.message);
        END
    """)
    conn.commit()


def mark_job_as_processing_if_new(conn: sqlite3.Connection, job_id: str) -> bool:
    cursor = conn.cursor()
    cursor.execute(
        "INSERT OR IGNORE INTO processed_jobs (job_id) VALUES (?)",
        (job_id,),
    )
    return cursor.rowcount == 1


def process_message(conn: sqlite3.Connection, message: dict) -> None:
    action = message["action"]
    payload = message["payload"]
    cursor = conn.cursor()

    if action == "insert_snapshot":
        cursor.execute(
            """
            INSERT INTO snapshots
                (timestamp, cpu_percent, mem_used_gb, mem_total_gb, mem_percent,
                 disk_used_gb, disk_total_gb, disk_percent, net_latency_ms)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                payload["timestamp"],
                payload.get("cpu_percent"),
                payload.get("mem_used_gb"),
                payload.get("mem_total_gb"),
                payload.get("mem_percent"),
                payload.get("disk_used_gb"),
                payload.get("disk_total_gb"),
                payload.get("disk_percent"),
                payload.get("net_latency_ms"),
            ),
        )

    elif action == "insert_db_snapshot":
        cursor.execute(
            """
            INSERT INTO db_snapshots
                (timestamp, db_name, db_type, status, latency_ms, error)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                payload["timestamp"],
                payload["db_name"],
                payload.get("db_type", "unknown"),
                payload.get("status", "error"),
                payload.get("latency_ms"),
                payload.get("error"),
            ),
        )

    elif action == "prune_snapshots":
        cutoff = payload["cutoff"]
        cursor.execute(
            "DELETE FROM snapshots WHERE timestamp < ?",
            (cutoff,),
        )
        cursor.execute(
            "DELETE FROM db_snapshots WHERE timestamp < ?",
            (cutoff,),
        )

    elif action == "update_pipeline_state":
        cursor.execute(
            """
            UPDATE pipeline_state
            SET phase = ?, status = ?, updated_at = ?, message = ?
            WHERE id = 1
            """,
            (
                payload["phase"],
                payload["status"],
                payload["updated_at"],
                payload.get("message", ""),
            ),
        )

    else:
        raise ValueError(f"Onbekende action: {action}")


def handle_job(conn: sqlite3.Connection, job_path: Path) -> None:
    processing_path = PROCESSING_DIR / job_path.name
    done_path = DONE_DIR / job_path.name
    failed_path = FAILED_DIR / job_path.name

    try:
        os.replace(job_path, processing_path)

        with open(processing_path, "r", encoding="utf-8") as f:
            message = json.load(f)

        job_id = message["job_id"]

        is_new_job = mark_job_as_processing_if_new(conn, job_id)

        if not is_new_job:
            conn.rollback()
            logger.warning("Job was al verwerkt, verplaats naar done: %s", job_id)
            shutil.move(processing_path, done_path)
            return

        process_message(conn, message)
        conn.commit()

        shutil.move(processing_path, done_path)
        logger.info("Job verwerkt: %s", job_id)

    except Exception:
        conn.rollback()
        logger.exception("Fout bij verwerken van job: %s", job_path.name)

        if processing_path.exists():
            shutil.move(processing_path, failed_path)

        time.sleep(1)


def main() -> None:
    ensure_directories()

    if len(sys.argv) > 1 and sys.argv[1] == "prune":
        retention_days = int(os.environ.get("SQLITE_QUEUE_DONE_RETENTION_DAYS", "7"))
        prune_done_queue(retention_days)
        return

    recover_processing_jobs()

    conn = open_database()
    ensure_database_schema(conn)

    logger.info("SQLite file queue writer gestart")

    while True:
        jobs = sorted(PENDING_DIR.glob("*.json"))

        if not jobs:
            time.sleep(POLL_INTERVAL_SECONDS)
            continue

        for job_path in jobs:
            handle_job(conn, job_path)


if __name__ == "__main__":
    main()
