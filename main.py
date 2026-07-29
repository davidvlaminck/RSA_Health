import sqlite3
import socket
import time
from datetime import datetime
from pathlib import Path

import psycopg2
import psutil
from arango import ArangoClient
from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
import tomli

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")

CONFIG_PATH = Path(__file__).parent / "config.toml"
DB_PATH = Path(__file__).parent / "health.db"


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
        conn.commit()


def save_snapshot(server: dict, net: dict, db_results: dict):
    now = datetime.utcnow().isoformat() + "Z"
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


def get_history(limit: int = 100):
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT timestamp, cpu_percent, mem_percent, disk_percent, net_latency_ms
            FROM snapshots
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [dict(row) for row in reversed(rows)]


def get_db_history(limit: int = 100):
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT timestamp, db_name, db_type, status, latency_ms
            FROM db_snapshots
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [dict(row) for row in reversed(rows)]


def load_config():
    if not CONFIG_PATH.exists():
        return []
    with CONFIG_PATH.open("rb") as f:
        cfg = tomli.load(f)
    return cfg.get("databases", [])


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
    config = load_config()
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


@app.get("/history")
def history(limit: int = 100):
    init_db()
    return {
        "snapshots": get_history(limit),
        "db_snapshots": get_db_history(limit),
    }


@app.get("/")
def index():
    return FileResponse("static/index.html")
