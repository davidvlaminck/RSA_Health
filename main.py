import json
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
from arango import ArangoClient
from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, Response
from pydantic import BaseModel

from orchestrator.orchestrator import lifespan as orchestrator_lifespan
from sqlite_writer.pipeline_state import PipelineState
from sqlite_writer.sqlite_file_writer import ensure_database_schema, open_database
from sqlite_writer.pipeline_state import PipelineState, enqueue_sqlite_job

CONFIG_PATH = Path(__file__).parent.parent / 'config' / "config_rsa_health.json"
DB_PATH = Path(__file__).parent / "health.db"
SERVICES_STATE_PATH = Path(__file__).parent / "services_state.json"


def _load_service_state() -> dict:
    if SERVICES_STATE_PATH.exists():
        try:
            return json.loads(SERVICES_STATE_PATH.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def _save_service_state(state: dict) -> None:
    try:
        SERVICES_STATE_PATH.write_text(json.dumps(state, indent=2), encoding="utf-8")
    except Exception:
        pass


def _get_systemd_service_info(service_name: str) -> dict | None:
    try:
        result = subprocess.run(
            ["systemctl", "show", service_name, "--no-pager", "--plain"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode != 0:
            return None
        props = {}
        for line in result.stdout.splitlines():
            if "=" in line:
                key, _, value = line.partition("=")
                props[key.strip()] = value.strip()
        active_state = props.get("ActiveState", "unknown")
        active_enter = props.get("ActiveEnterTimestamp", "")
        iso_timestamp = None
        if active_enter:
            m = __import__("re").search(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})", active_enter)
            if m:
                try:
                    iso_timestamp = datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S").isoformat()
                except Exception:
                    pass
        return {
            "active_state": active_state,
            "active_enter_timestamp": iso_timestamp,
            "load_state": props.get("LoadState", "unknown"),
            "sub_state": props.get("SubState", "unknown"),
        }
    except Exception:
        return None


def _detect_service_restart(service_name: str, info: dict, state: dict) -> dict:
    now = datetime.now(timezone.utc).isoformat()
    is_running = info.get("active_state") == "active"
    active_since = info.get("active_enter_timestamp")
    prev = state.get(service_name, {})
    if is_running and active_since:
        if prev.get("active_enter_timestamp") != active_since:
            state[service_name] = {
                "active_state": info["active_state"],
                "active_enter_timestamp": active_since,
                "last_restart_detected_at": now,
            }
            logging.info("Service restart detected: %s (active since %s)", service_name, active_since)
        else:
            state[service_name] = {
                "active_state": info["active_state"],
                "active_enter_timestamp": active_since,
                "last_restart_detected_at": prev.get("last_restart_detected_at"),
            }
    else:
        state[service_name] = {
            "active_state": info.get("active_state", "unknown"),
            "active_enter_timestamp": None,
            "last_restart_detected_at": prev.get("last_restart_detected_at"),
        }
    return dict(state[service_name])


def _get_configured_services() -> list[dict]:
    try:
        with CONFIG_PATH.open("r", encoding="utf-8") as f:
            cfg = json.load(f)
        return cfg.get("services", [])
    except Exception:
        return []


def _collect_services() -> list[dict]:
    services_cfg = _get_configured_services()
    if not services_cfg:
        return []
    state = _load_service_state()
    results = []
    for svc in services_cfg:
        name = svc.get("name", "")
        label = svc.get("label", name)
        if not name:
            continue
        info = _get_systemd_service_info(name)
        if info is None:
            results.append({
                "name": name,
                "label": label,
                "status": "unknown",
                "active_since": None,
                "last_restart_detected_at": None,
            })
            continue
        status = "ok" if info["active_state"] == "active" else "error"
        current = _detect_service_restart(name, info, state)
        _save_service_state(state)
        results.append({
            "name": name,
            "label": label,
            "status": status,
            "active_state": info["active_state"],
            "active_since": current.get("active_enter_timestamp"),
            "last_restart_detected_at": current.get("last_restart_detected_at"),
        })
    return results


def _configure_logging():
    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    for handler in logging.root.handlers:
        handler.setFormatter(formatter)
    for name in ("uvicorn", "uvicorn.access", "uvicorn.error"):
        for handler in logging.getLogger(name).handlers:
            handler.setFormatter(formatter)


class IndexAccessLogFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        msg = record.getMessage()
        return ' "GET / HTTP/' not in msg and ' "HEAD / HTTP/' not in msg


logging.getLogger("uvicorn.access").addFilter(IndexAccessLogFilter())

pipeline = PipelineState(DB_PATH)

_BLOCKED_IPS = {
    "213.209.159.175",
    "139.135.43.104",
    "186.236.254.56",
    "81.19.219.216",
    "188.240.59.20",
    "93.123.109.228",
    "47.114.87.90",
    "5.61.209.44",
    "5.61.209.92",
    "160.119.76.24",
    "36.255.33.242",
    "185.209.15.199",
    "45.198.224.26",
    "20.65.193.201",
}


class _RateLimiter:
    def __init__(self, max_requests: int = 120, window: int = 60):
        self._max = max_requests
        self._window = window
        self._hits: dict[str, list[float]] = {}
        self._lock = threading.Lock()

    def allow(self, ip: str) -> bool:
        now = time.time()
        with self._lock:
            stamps = [t for t in self._hits.get(ip, []) if now - t < self._window]
            self._hits[ip] = stamps
            if len(stamps) >= self._max:
                return False
            stamps.append(now)
            return True


def _get_rate_limit_config() -> tuple[int, int]:
    try:
        with CONFIG_PATH.open("r", encoding="utf-8") as f:
            cfg = json.load(f)
        rate_cfg = cfg.get("rate_limit", {})
        max_requests = rate_cfg.get("max_requests", 120)
        window = rate_cfg.get("window", 60)
        return max_requests, window
    except Exception:
        return 120, 60


_rate_limiter = _RateLimiter(*_get_rate_limit_config())


async def _security_middleware(request: Request, call_next):
    ip = request.client.host if request.client else "unknown"
    if ip in _BLOCKED_IPS:
        logging.warning("Blocked request from banned IP: %s %s", ip, request.url.path)
        return Response(content="Forbidden", status_code=403)
    if not _rate_limiter.allow(ip):
        logging.warning("Rate limit exceeded for IP: %s %s", ip, request.url.path)
        return Response(content="Rate limit exceeded", status_code=429)
    return await call_next(request)


def save_snapshot(server: dict, net: dict, db_results: list[dict]):
    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    enqueue_sqlite_job(
        action="insert_snapshot",
        payload={
            "timestamp": now,
            "cpu_percent": server.get("cpu_percent"),
            "mem_used_gb": server.get("memory", {}).get("used_gb"),
            "mem_total_gb": server.get("memory", {}).get("total_gb"),
            "mem_percent": server.get("memory", {}).get("percent"),
            "disk_used_gb": server.get("disk", {}).get("used_gb"),
            "disk_total_gb": server.get("disk", {}).get("total_gb"),
            "disk_percent": server.get("disk", {}).get("percent"),
            "net_latency_ms": net.get("latency_ms") if net.get("status") == "ok" else None,
        },
    )

    for entry in db_results:
        name = entry["name"]
        info = entry
        enqueue_sqlite_job(
            action="insert_db_snapshot",
            payload={
                "timestamp": now,
                "db_name": name,
                "db_type": info.get("type", "unknown"),
                "status": info.get("status", "error"),
                "latency_ms": info.get("latency_ms"),
                "error": info.get("error"),
            },
        )


_last_prune_date = None


def _prune_old_snapshots():
    cutoff = datetime.now(timezone.utc) - timedelta(days=30)
    cutoff_str = cutoff.isoformat().replace("+00:00", "Z")
    enqueue_sqlite_job(
        action="prune_snapshots",
        payload={"cutoff": cutoff_str},
    )


def _snapshot_loop(stop_event: threading.Event):
    global _last_prune_date
    _prune_old_snapshots()
    _last_prune_date = datetime.now(timezone.utc).date()
    logging.info("Snapshot loop gestart (interval 30s)")

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
            logging.info("Snapshot opgeslagen: %d databases", len(db_results))

            now = datetime.now(timezone.utc)
            if _last_prune_date != now.date():
                _last_prune_date = now.date()
                _prune_old_snapshots()
                logging.info("Oude snapshots opgeruimd")
        except Exception as exc:
            logging.error("Fout in snapshot loop: %s", exc, exc_info=True)
        stop_event.wait(30)


@asynccontextmanager
async def lifespan(app: FastAPI):
    _configure_logging()

    conn = open_database()
    ensure_database_schema(conn)
    conn.close()

    stop_event = threading.Event()
    snapshot_thread = threading.Thread(target=_snapshot_loop, args=(stop_event,), daemon=True)
    snapshot_thread.start()

    async with orchestrator_lifespan(app):
        yield

    stop_event.set()
    snapshot_thread.join(timeout=5)


app = FastAPI(lifespan=lifespan)
app.middleware("http")(_security_middleware)


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


RANGE_MIN_SNAPSHOTS = {
    "5m": 20,
    "1h": 200,
    "12h": 2000,
    "1d": 4000,
    "7d": 25000,
    "1mo": 50000,
}


@app.get("/history")
def history(range: str = "1h", limit: int = 1000):
    delta = RANGE_MAP.get(range, timedelta(hours=1))
    min_limit = RANGE_MIN_SNAPSHOTS.get(range, 1000)
    limit = max(limit, min_limit)
    after = (datetime.now(timezone.utc) - delta).isoformat().replace("+00:00", "Z")
    return {
        "range": range,
        "snapshots": get_history(limit, after=after),
        "db_snapshots": get_db_history(limit, after=after),
    }


_health_cache = {}
_health_cache_lock = threading.Lock()
_HEALTH_CACHE_TTL = 30


def _cached(cache_key, fn, *args, **kwargs):
    now = time.time()
    with _health_cache_lock:
        entry = _health_cache.get(cache_key)
        if entry and now - entry[0] < _HEALTH_CACHE_TTL:
            return entry[1]
    result = fn(*args, **kwargs)
    with _health_cache_lock:
        _health_cache[cache_key] = (now, result)
    return result


def load_config():
    if not CONFIG_PATH.exists():
        return [], {}
    with CONFIG_PATH.open("r", encoding="utf-8") as f:
        cfg = json.load(f)
    return cfg.get("databases", []), cfg.get("logs", {}).get("directory", "")


def bytes_to_gb(value_bytes: int) -> float:
    return round(value_bytes / (1024**3), 2)


def check_network() -> dict:
    return _cached("network", _check_network_impl)


def _check_network_impl() -> dict:
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
    return _cached("server", _check_server_impl)


def _check_server_impl() -> dict:
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
    cache_key = f"db:{json.dumps(cfg, sort_keys=True)}"
    return _cached(cache_key, _check_db_impl, cfg)


def _check_db_impl(cfg: dict) -> dict:
    db_type = cfg.get("type", "unknown")
    try:
        if db_type == "arangodb":
            return _check_arangodb(cfg)
        elif db_type == "postgresql":
            return _check_postgresql(cfg)
        return {"status": "error", "error": f"unknown type: {db_type}", "type": db_type}
    except Exception as exc:
        return {"status": "error", "error": str(exc), "type": db_type}


_arango_clients = {}
_arango_clients_lock = threading.Lock()
_pg_conns = {}
_pg_conns_lock = threading.Lock()


def _get_arango_client(cfg: dict):
    cache_key = f"arangodb:{cfg.get('host')}:{cfg.get('port')}"
    with _arango_clients_lock:
        client = _arango_clients.get(cache_key)
        if client is None:
            client = ArangoClient(hosts=[f"http://{cfg['host']}:{cfg['port']}"])
            _arango_clients[cache_key] = client
        return client


def _get_pg_conn(cfg: dict):
    cache_key = f"postgresql:{cfg.get('host')}:{cfg.get('port')}:{cfg.get('database')}"
    with _pg_conns_lock:
        conn = _pg_conns.get(cache_key)
        if conn is not None and conn.closed == 0:
            return conn
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
        conn = psycopg2.connect(
            host=cfg["host"],
            port=cfg["port"],
            user=cfg["username"],
            password=cfg["password"],
            database=cfg["database"],
            connect_timeout=5,
        )
        _pg_conns[cache_key] = conn
        return conn


def _check_arangodb(cfg: dict) -> dict:
    client = _get_arango_client(cfg)
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
    try:
        conn = _get_pg_conn(cfg)
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
        latency = round((time.perf_counter() - start) * 1000, 2)
        return {
            "status": "ok",
            "latency_ms": latency,
            "database": cfg["database"],
            "type": "postgresql",
        }
    except Exception:
        cache_key = f"postgresql:{cfg.get('host')}:{cfg.get('port')}:{cfg.get('database')}"
        with _pg_conns_lock:
            stale = _pg_conns.pop(cache_key, None)
            if stale is not None:
                try:
                    stale.close()
                except Exception:
                    pass
        start = time.perf_counter()
        conn = psycopg2.connect(
            host=cfg["host"],
            port=cfg["port"],
            user=cfg["username"],
            password=cfg["password"],
            database=cfg["database"],
            connect_timeout=5,
        )
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
        latency = round((time.perf_counter() - start) * 1000, 2)
        return {
            "status": "ok",
            "latency_ms": latency,
            "database": cfg["database"],
            "type": "postgresql",
        }


@app.get("/health")
def health():
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


@app.get("/services")
def services():
    return _collect_services()


class PipelineUpdate(BaseModel):
    phase: str
    status: str
    message: str = ""


@app.get("/pipeline/state")
def pipeline_state():
    state = pipeline.get() or {}
    history = pipeline.get_history()
    return {"current": state, "history": history}


@app.post("/pipeline/reset")
def pipeline_reset():
    pipeline.reset()
    return {"ok": True, "message": "Pipeline state reset"}


@app.post("/pipeline/update")
def pipeline_update(payload: PipelineUpdate):
    valid_statuses = {"starting", "running", "completed", "failed", "aborted", "time-out"}
    if payload.status not in valid_statuses:
        return {"error": f"invalid status: {payload.status}"}
    pipeline.update(payload.phase, payload.status, payload.message)
    return {"ok": True}

# NOTE: scripts that run in the same process (or have filesystem access to health.db)
# SHOULD call pipeline.update() directly instead of POSTing to this endpoint.
# This endpoint exists for external tools (Power Automate, diagnostics) only.


@app.get("/favicon.ico")
def favicon():
    return Response(status_code=204)


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
                    filtered = []
                    for line in content:
                        parsed = _parse_log_time(line)
                        if parsed is None or parsed >= cutoff:
                            filtered.append(line)
                    content = filtered
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
    return Response(status_code=404)
