import time
from pathlib import Path

import psycopg2
from arango import ArangoClient
from fastapi import FastAPI
import tomli

app = FastAPI()

CONFIG_PATH = Path(__file__).parent / "config.toml"


def load_config():
    if not CONFIG_PATH.exists():
        return None
    with CONFIG_PATH.open("rb") as f:
        return tomli.load(f)


def check_arangodb(cfg: dict) -> dict:
    try:
        client = ArangoClient(
            hosts=[f"http://{cfg['host']}:{cfg['port']}"]
        )
        sys_db = client.db(
            cfg["database"],
            username=cfg["username"],
            password=cfg["password"],
        )
        start = time.perf_counter()
        sys_db.version()
        latency = round((time.perf_counter() - start) * 1000, 2)
        return {
            "status": "ok",
            "latency_ms": latency,
            "database": cfg["database"],
        }
    except Exception as exc:
        return {"status": "error", "error": str(exc)}


def check_postgresql(cfg: dict) -> dict:
    try:
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
        }
    except Exception as exc:
        return {"status": "error", "error": str(exc)}


@app.get("/health")
def health():
    config = load_config()
    response = {"running": True, "databases": {}}

    if not config:
        response["databases"]["arangodb"] = {
            "status": "not_configured",
            "error": "config.toml not found",
        }
        response["databases"]["postgresql"] = {
            "status": "not_configured",
            "error": "config.toml not found",
        }
        return response

    if "arangodb" in config:
        response["databases"]["arangodb"] = check_arangodb(config["arangodb"])
    else:
        response["databases"]["arangodb"] = {
            "status": "not_configured",
            "error": "missing [arangodb] section",
        }

    if "postgresql" in config:
        response["databases"]["postgresql"] = check_postgresql(config["postgresql"])
    else:
        response["databases"]["postgresql"] = {
            "status": "not_configured",
            "error": "missing [postgresql] section",
        }

    return response
