import logging
import socket
import sqlite3
import subprocess
import sys
import threading
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psutil
import psycopg2
import tomli
from arango import ArangoClient
from fastapi import FastAPI
from fastapi.responses import FileResponse, RedirectResponse, Response
from pydantic import BaseModel

from lib.orchestrator import lifespan as orchestrator_lifespan
from lib.pipeline_state import PipelineState

CONFIG_PATH = Path(__file__).parent / "config.toml"
DB_PATH = Path(__file__).parent / "health.db"


class IndexAccessLogFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        msg = record.getMessage()
        return ' "GET / HTTP/' not in msg and ' "HEAD / HTTP/' not in msg


logging.getLogger("uvicorn.access").addFilter(IndexAccessLogFilter())

pipeline = PipelineState(DB_PATH)


def init_db():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")
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
        conn.execute(
            "INSERT OR IGNORE INTO pipeline_state (id, phase, status, updated_at, message) VALUES (?, ?, ?, ?, ?)",
            (1, "idle", "completed", datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"), ""),
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_snapshots_timestamp ON snapshots (timestamp)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_db_snapshots_timestamp ON db_snapshots (timestamp)")
        conn.commit()


def save_snapshot(server: dict, net: dict, db_results: list[dict]):
    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")
        conn.execute(
            """
            INSERT INTO snapshots (timestamp, cpu_percent, mem_used_gb, mem_total_gb, mem_percent, disk_used_gb, disk_total_gb, disk_percent, net_latency_ms)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                now,
                server.get("cpu_percent"),
                server.get("memory", {}).get("used_gb"),
                server.get("memory", {}).get("total_gb"),
                server.get("memory", {}).get("percent"),
                server.get("disk", {}).get("used_gb"),
                server.get("disk", {}).get("total_gb"),
                server.get("disk", {}).get("percent"),
                net.get("latency_ms") if net.get("status") == "ok" else None,
            ),
        )
        for entry in db_results:
            name = entry["name"]
            info = entry
            conn.execute(
                """
                INSERT INTO db_snapshots (timestamp, db_name, db_type, status, latency_ms, error)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    now,
                    name,
                    info.get("type", "unknown"),
                    info.get("status", "error"),
                    info.get("latency_ms"),
                    info.get("error"),
                ),
            )
        conn.commit()


_last_prune_date = None


def _prune_old_snapshots():
    cutoff = datetime.now(timezone.utc) - timedelta(days=30)
    cutoff_str = cutoff.isoformat().replace("+00:00", "Z")
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")
        conn.execute("DELETE FROM snapshots WHERE timestamp < ?", (cutoff_str,))
        conn.execute("DELETE FROM db_snapshots WHERE timestamp < ?", (cutoff_str,))
        conn.commit()


def _snapshot_loop(stop_event: threading.Event):
    global _last_prune_date
    _prune_old_snapshots()
    _last_prune_date = datetime.now(timezone.utc).date()

    while not stop_event.is_set():
        try:
            config, _ = load_config()
            server = check_server()
            net = check_network()
            db_results = []
            for db_cfg in config:
                db_name = db_cfg.get("database", db_cfg.get("host", "unknown"))
                db_results.append({"name": db_name, **check_db(db_cfg)})
            save_snapshot(server, net, db_results)

            now = datetime.now(timezone.utc)
            if _last_prune_date != now.date():
                _last_prune_date = now.date()
                _prune_old_snapshots()
        except Exception:
            pass
        stop_event.wait(30)


@asynccontextmanager
async def lifespan(app: FastAPI):
    stop_event = threading.Event()
    snapshot_thread = threading.Thread(target=_snapshot_loop, args=(stop_event,), daemon=True)
    snapshot_thread.start()

    async with orchestrator_lifespan(app):
        yield

    stop_event.set()
    snapshot_thread.join(timeout=5)


app = FastAPI(lifespan=lifespan)


def get_history(limit: int = 100, after: str | None = None):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")
        conn.row_factory = sqlite3.Row
        query = """
            SELECT timestamp, cpu_percent, mem_percent, disk_percent, net_latency_ms
            FROM snapshots
        """
        params = []
        if after:
            query += " WHERE timestamp >= ?"
            params.append(after)
        query += " ORDER BY id DESC LIMIT ?"
        params.append(limit)
        rows = conn.execute(query, params).fetchall()
        return [dict(row) for row in reversed(rows)]


def get_db_history(limit: int = 100, after: str | None = None):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")
        conn.row_factory = sqlite3.Row
        query = """
            SELECT timestamp, db_name, db_type, status, latency_ms
            FROM db_snapshots
        """
        params = []
        if after:
            query += " WHERE timestamp >= ?"
            params.append(after)
        query += " ORDER BY id DESC LIMIT ?"
        params.append(limit)
        rows = conn.execute(query, params).fetchall()
        return [dict(row) for row in reversed(rows)]


from lib.pipeline_state import PipelineState

pipeline = PipelineState(DB_PATH)


def run_arango_sync(timeout_seconds: int = 3600):
    script_path = Path(__file__).parent.parent / "InfraDbToArangoDb" / "main_linux_arango.py"
    if not script_path.exists():
        return {"success": False, "message": f"Arango script niet gevonden: {script_path}"}

    python_exe = sys.executable or "python3"
    pipeline.update("arango_sync", "running", "Arango sync gestart")

    try:
        process = subprocess.Popen(
            [python_exe, str(script_path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
    except Exception as e:
        pipeline.update("arango_sync", "failed", f"Kon script niet starten: {e}")
        return {"success": False, "message": str(e)}

    start_time = time.time()
    last_step = ""

    try:
        for line in process.stdout:
            line = line.strip()
            if not line:
                continue

            if "Current DB step:" in line:
                last_step = line.split("Current DB step:", 1)[1].strip()
                pipeline.update("arango_sync", "running", f"Stap: {last_step}")
            elif "[0] Creating" in line:
                pipeline.update("arango_sync", "running", "Database aan het creëren")
            elif "[1] Filling" in line:
                pipeline.update("arango_sync", "running", "Database vullen")
            elif "[2] Do some additional" in line:
                pipeline.update("arango_sync", "running", "Bijkomende data vullen")
            elif "[3] Adding indices" in line:
                pipeline.update("arango_sync", "running", "Indices en graphs aanmaken")
            elif "[4] Applying constraints" in line:
                pipeline.update("arango_sync", "running", "Constraints toepassen")
            elif "[5] Synchronising" in line:
                pipeline.update("arango_sync", "running", "Synchroniseren")
            elif "[6] Stopping" in line:
                pipeline.update("arango_sync", "running", "Afsluiten")
            elif "finished_at" in line:
                pipeline.update("arango_sync", "completed", "Arango sync voltooid")

            if time.time() - start_time > timeout_seconds:
                process.kill()
                process.wait()
                pipeline.update(
                    "arango_sync",
                    "failed",
                    f"Timeout na {timeout_seconds}s (laatste stap: {last_step or 'onbekend'})",
                )
                return {"success": False, "message": f"Timeout na {timeout_seconds} seconden"}

        return_code = process.returncode
        if return_code == 0:
            pipeline.update("arango_sync", "completed", "Arango sync voltooid")
            return {"success": True, "message": "Sync voltooid"}
        pipeline.update("arango_sync", "failed", f"Script eindigde met code {return_code}")
        return {"success": False, "message": f"Script eindigde met code {return_code}"}

    except Exception as e:
        try:
            process.kill()
            process.wait()
        except Exception:
            pass
        pipeline.update("arango_sync", "failed", f"Fout: {e}")
        return {"success": False, "message": str(e)}


RANGE_MAP = {
    "5m": timedelta(minutes=5),
    "1h": timedelta(hours=1),
    "12h": timedelta(hours=12),
    "1d": timedelta(days=1),
    "7d": timedelta(days=7),
    "1mo": timedelta(days=30),
}


@app.get("/history")
def history(range: str = "1h", limit: int = 1000):
    init_db()
    delta = RANGE_MAP.get(range, timedelta(hours=1))
    after = (datetime.now(timezone.utc) - delta).isoformat().replace("+00:00", "Z")
    return {
        "range": range,
        "snapshots": get_history(limit, after=after),
        "db_snapshots": get_db_history(limit, after=after),
    }


def load_config():
    if not CONFIG_PATH.exists():
        return [], {}
    with CONFIG_PATH.open("rb") as f:
        cfg = tomli.load(f)
    return cfg.get("databases", []), cfg.get("logs", {}).get("directory", "")


def bytes_to_gb(value_bytes: int) -> float:
    return round(value_bytes / (1024**3), 2)


def check_network() -> dict:
    targets = [
        ("1.1.1.1", 53),
        ("8.8.8.8", 53),
    ]

    for host, port in targets:
        try:
            start = time.perf_counter()
            with socket.create_connection((host, port), timeout=3):
                latency = round((time.perf_counter() - start) * 1000, 2)
            return {
                "target": f"{host}:{port}",
                "status": "ok",
                "latency_ms": latency,
            }
        except Exception:
            continue

    return {
        "target": targets[0],
        "status": "error",
        "error": "all targets unreachable",
    }


def check_server() -> dict:
    cpu_percent = psutil.cpu_percent(interval=0.5)
    mem = psutil.virtual_memory()
    disk = psutil.disk_usage("/")

    return {
        "cpu_percent": cpu_percent,
        "memory": {
            "total_gb": bytes_to_gb(mem.total),
            "used_gb": bytes_to_gb(mem.used),
            "percent": mem.percent,
        },
        "disk": {
            "total_gb": bytes_to_gb(disk.total),
            "used_gb": bytes_to_gb(disk.used),
            "percent": disk.percent,
        },
    }


def check_db(cfg: dict) -> dict:
    db_type = cfg.get("type", "unknown")
    try:
        if db_type == "arangodb":
            return _check_arangodb(cfg)
        elif db_type == "postgresql":
            return _check_postgresql(cfg)
        return {"status": "error", "error": f"unknown type: {db_type}", "type": db_type}
    except Exception as exc:
        return {"status": "error", "error": str(exc), "type": db_type}


def _check_arangodb(cfg: dict) -> dict:
    client = ArangoClient(hosts=[f"http://{cfg['host']}:{cfg['port']}"])
    sys_db = client.db(
        cfg["database"], username=cfg["username"], password=cfg["password"]
    )
    start = time.perf_counter()
    sys_db.version()
    latency = round((time.perf_counter() - start) * 1000, 2)
    return {
        "status": "ok",
        "latency_ms": latency,
        "database": cfg["database"],
        "type": "arangodb",
    }


def _check_postgresql(cfg: dict) -> dict:
    start = time.perf_counter()
    conn = psycopg2.connect(
        host=cfg["host"],
        port=cfg["port"],
        user=cfg["username"],
        password=cfg["password"],
        database=cfg["database"],
        connect_timeout=5,
    )
    conn.close()
    latency = round((time.perf_counter() - start) * 1000, 2)
    return {
        "status": "ok",
        "latency_ms": latency,
        "database": cfg["database"],
        "type": "postgresql",
    }


@app.get("/health")
def health():
    init_db()
    config, _ = load_config()
    server = check_server()
    net = check_network()
    response = {
        "running": True,
        "server": server,
        "network": net,
        "databases": [],
    }

    for db_cfg in config:
        db_name = db_cfg.get("database", db_cfg.get("host", "unknown"))
        result = check_db(db_cfg)
        response["databases"].append({"name": db_name, **result})

    save_snapshot(server, net, response["databases"])
    return response


class PipelineUpdate(BaseModel):
    phase: str
    status: str
    message: str = ""


@app.get("/pipeline/state")
def pipeline_state():
    init_db()
    pipeline.ensure()
    state = pipeline.get() or {}
    history = pipeline.get_history()
    return {"current": state, "history": history}


@app.post("/pipeline/update")
def pipeline_update(payload: PipelineUpdate):
    init_db()
    pipeline.ensure()
    valid_statuses = {"running", "completed", "failed", "aborted"}
    if payload.status not in valid_statuses:
        return {"error": f"invalid status: {payload.status}"}
    pipeline.update(payload.phase, payload.status, payload.message)
    return {"ok": True}

# NOTE: scripts that run in the same process (or have filesystem access to health.db)
# SHOULD call pipeline.update() directly instead of POSTing to this endpoint.
# This endpoint exists for external tools (Power Automate, diagnostics) only.


@app.get("/")
def index():
    return FileResponse("static/index.html")


LOG_FILES = {
    "arangodb": "arango_sync",
    "postgis": "postgis_sync",
    "rsa": "RSA",
    "rsa_orchestrator": "rsa_orchestrator",
    "rsa_health": "rsa_health",
}


@app.get("/logs")
def download_log(type: str = "all", range: str = "1d"):
    _, logs_dir = load_config()
    if not logs_dir:
        return {"error": "logs directory not configured"}
    base = Path(logs_dir)
    if not base.is_dir():
        return {"error": "logs directory not found"}

    selected = list(LOG_FILES.items()) if type == "all" else [(type, LOG_FILES[type])] if type in LOG_FILES else []
    if not selected:
        return {"error": "invalid log type"}

    cutoff = None
    if range == "1d":
        cutoff = datetime.now(timezone.utc) - timedelta(days=1)

    import zipfile, io, gzip
    buf = io.BytesIO()
    seen = set()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for key, filename in selected:
            pattern = f"{filename}.log" if range == "1d" else f"{filename}*"
            matches = sorted(base.glob(pattern))
            for file_path in matches:
                arcname = file_path.name
                if arcname in seen:
                    continue
                seen.add(arcname)
                if file_path.suffix == ".gz":
                    try:
                        with gzip.open(file_path, "rt", encoding="utf-8", errors="replace") as f:
                            content = f.readlines()
                    except Exception:
                        continue
                else:
                    try:
                        content = file_path.read_text(encoding="utf-8", errors="replace").splitlines()
                    except Exception:
                        continue
                if cutoff:
                    content = [line for line in content if _parse_log_time(line) and _parse_log_time(line) >= cutoff]
                if content:
                    zf.writestr(arcname, "\n".join(content) + "\n")

    buf.seek(0)
    range_label = "1d" if range == "1d" else "all"
    return Response(
        buf.getvalue(),
        media_type="application/zip",
        headers={"Content-Disposition": f"attachment; filename=logs_{type}_{range_label}.zip"},
    )


def _parse_log_time(line: str) -> datetime | None:
    import re
    m = re.search(r"(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2})", line)
    if not m:
        return None
    try:
        dt = datetime.fromisoformat(f"{m.group(1)} {m.group(2)}")
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


@app.get("/{full_path:path}")
def redirect_all(full_path: str):
    return RedirectResponse(url="/")
