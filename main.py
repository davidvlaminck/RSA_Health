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
from fastapi.responses import FileResponse, RedirectResponse
from pydantic import BaseModel

CONFIG_PATH = Path(__file__).parent / "config.toml"
DB_PATH = Path(__file__).parent / "health.db"

PIPELINE_TIMEOUTS = {
    "arango_sync": 4 * 3600,
    "postgis_pause": 600,
    "rsa_queries": 3 * 3600,
    "postgis_resume": 600,
}

POLL_INTERVAL_SECONDS = 30


class PipelineOrchestrator:
    TIMEOUT_ARANGO = PIPELINE_TIMEOUTS["arango_sync"]
    TIMEOUT_POSTGIS_PAUSE = PIPELINE_TIMEOUTS["postgis_pause"]
    TIMEOUT_RSA = PIPELINE_TIMEOUTS["rsa_queries"]
    TIMEOUT_POSTGIS_RESUME = PIPELINE_TIMEOUTS["postgis_resume"]
    POLL_INTERVAL = POLL_INTERVAL_SECONDS

    def __init__(self):
        self.running = False
        self._thread = None
        self._wait_phase = None
        self._wait_status = None
        self._wait_deadline = 0
        self._wait_timeout = 0
        self._last_reset_date = None

    def start(self):
        self.running = True
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def stop(self):
        self.running = False

    def _loop(self):
        while self.running:
            try:
                self._tick()
            except Exception as exc:
                pipeline.update("orchestrator", "failed", f"Fout: {exc}")
            time.sleep(self.POLL_INTERVAL)

    def _tick(self):
        self._daily_reset_check()
        state = pipeline.get()
        if not state:
            return
        phase = state.get("phase", "idle")
        status = state.get("status", "completed")

        if self._is_waiting():
            if phase == self._wait_phase and status == self._wait_status:
                self._clear_wait()
            elif time.time() > self._wait_deadline:
                timed_out = self._wait_phase
                timeout_val = self._wait_timeout
                self._clear_wait()
                pipeline.update(
                    timed_out or "orchestrator",
                    "failed",
                    f"Timeout na {timeout_val}s wachten op {timed_out}",
                )
            return

        if phase == "idle" and status == "completed":
            self._check_sharepoint_marker()
        elif phase == "sharepoint_to_drive" and status == "completed":
            self._start_drive_download()
        elif phase == "drive_download" and status == "completed":
            self._wait_for("arango_sync", "completed", self.TIMEOUT_ARANGO)
        elif phase == "arango_sync" and status == "completed":
            self._start_postgis_pause()
        elif phase == "postgis_sync_pausing" and status == "running":
            self._wait_for(
                "postgis_sync_paused", "completed", self.TIMEOUT_POSTGIS_PAUSE
            )
        elif phase == "postgis_sync_paused" and status == "completed":
            self._wait_for("rsa_queries", "completed", self.TIMEOUT_RSA)
        elif phase == "rsa_queries" and status == "completed":
            self._start_postgis_resume()
        elif phase == "postgis_sync_resuming" and status == "running":
            self._wait_for(
                "postgis_sync_running", "completed", self.TIMEOUT_POSTGIS_RESUME
            )
        elif phase == "postgis_sync_running" and status == "completed":
            self._start_drive_upload()
        elif phase == "drive_upload" and status == "completed":
            self._check_drive_to_sharepoint_marker()

    def _is_waiting(self):
        return self._wait_phase is not None

    def _wait_for(self, phase, status, timeout):
        self._wait_phase = phase
        self._wait_status = status
        self._wait_deadline = time.time() + timeout
        self._wait_timeout = timeout
        pipeline.update(
            "orchestrator", "running", f"Wachten op {phase}={status}"
        )

    def _clear_wait(self):
        self._wait_phase = None
        self._wait_status = None
        self._wait_deadline = 0
        self._wait_timeout = 0

    def _check_sharepoint_marker(self):
        marker = self._find_drive_marker("sharepoint_to_drive", "completed")
        if marker:
            pipeline.update(
                "sharepoint_to_drive", "completed", "Marker gedetecteerd"
            )

    def _check_drive_to_sharepoint_marker(self):
        marker = self._find_drive_marker("drive_to_sharepoint", "completed")
        if marker:
            pipeline.update(
                "drive_to_sharepoint", "completed", "Marker gedetecteerd"
            )

    def _find_drive_marker(self, phase, expected_status):
        # TODO: Google Drive API polling voor markerbestanden
        # Marker-formaat: YYYY-MM-DD_<phase>.<status>
        return None

    def _start_drive_download(self):
        # TODO: implement sync_drive_to_local
        pipeline.update("drive_download", "running", "Drive download gestart")

    def _start_drive_upload(self):
        # TODO: implement sync_local_to_drive
        pipeline.update("drive_upload", "running", "Drive upload gestart")

    def _start_postgis_pause(self):
        # TODO: signal PostGIS sync to pause (write fase to pipeline_state)
        pipeline.update(
            "postgis_sync_pausing", "running", "PostGIS-sync pauzeren"
        )

    def _start_postgis_resume(self):
        # TODO: signal PostGIS sync to resume
        pipeline.update(
            "postgis_sync_resuming", "running", "PostGIS-sync hervatten"
        )

    def _daily_reset_check(self):
        now = datetime.now(timezone.utc)
        if now.hour == 0 and self._last_reset_date != now.date():
            self._last_reset_date = now.date()
            pipeline.update("idle", "completed", "Dagelijkse reset")


orchestrator = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global orchestrator
    orchestrator = PipelineOrchestrator()
    orchestrator.start()
    yield
    orchestrator.stop()


app = FastAPI(lifespan=lifespan)


def init_db():
    with sqlite3.connect(DB_PATH) as conn:
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
        conn.commit()


def save_snapshot(server: dict, net: dict, db_results: dict):
    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    with sqlite3.connect(DB_PATH) as conn:
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
        for name, info in db_results.items():
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


def get_history(limit: int = 100, after: str | None = None):
    with sqlite3.connect(DB_PATH) as conn:
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
    after = (datetime.utcnow() - delta).isoformat() + "Z"
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
    return cfg.get("databases", []), cfg.get("logs", {})


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
        "databases": {},
    }

    for db_cfg in config:
        db_name = db_cfg.get("database", db_cfg.get("host", "unknown"))
        response["databases"][db_name] = check_db(db_cfg)

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
    state = pipeline.get()
    return state or {}


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


LOG_LABELS = {
    "arangodb_fill": "ArangoDB Fill Log",
    "postgresql_fill": "PostgreSQL Fill Log",
    "rsa_fill": "RSA Fill Log",
}


@app.get("/logs/{log_key}")
def download_log(log_key: str):
    _, logs = load_config()
    path = logs.get(log_key)
    if not path:
        return {"error": "log not configured"}
    file_path = Path(path)
    if not file_path.is_file():
        return {"error": "log file not found"}
    return FileResponse(
        file_path,
        filename=file_path.name,
        media_type="text/plain",
    )


@app.get("/{full_path:path}")
def redirect_all(full_path: str):
    return RedirectResponse(url="/")
