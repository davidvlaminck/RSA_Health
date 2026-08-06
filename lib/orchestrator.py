import logging
import os
import sys
import threading
import time
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from fastapi import FastAPI

_HERE = Path(__file__).resolve().parent
_ROOT = _HERE.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import sqlite3

from lib.pipeline_state import PipelineState

DB_PATH = Path(__file__).resolve().parent.parent / "health.db"
CONFIG_PATH = DB_PATH.parent / "config.toml"
LOCAL_TZ = ZoneInfo("Europe/Brussels")


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

    def __init__(self, pipeline: PipelineState):
        self.pipeline = pipeline
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
            except sqlite3.OperationalError as exc:
                logging.error(f"SQLite fout in orchestrator: {exc}")
            except Exception as exc:
                try:
                    self.pipeline.update("orchestrator", "failed", f"Fout: {exc}")
                except sqlite3.OperationalError:
                    logging.error(f"SQLite fout bij loggen van orchestrator fout: {exc}")
            time.sleep(self.POLL_INTERVAL)

    def _tick(self):
        self._daily_reset_check()
        state = self.pipeline.get()
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
                self.pipeline.update(
                    timed_out or "orchestrator",
                    "failed",
                    f"Timeout na {timeout_val}s wachten op {timed_out}",
                )
            return

        if phase == "idle" and status == "completed":
            self._check_sharepoint_marker()
        elif phase == "sharepoint_to_drive" and status == "running":
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
            self.pipeline.update("rsa_queries", "starting", "RSA queries starten")
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
        self.pipeline.update(
            "orchestrator", "running", f"Wachten op {phase}={status}"
        )

    def _clear_wait(self):
        self._wait_phase = None
        self._wait_status = None
        self._wait_deadline = 0
        self._wait_timeout = 0

    def _check_sharepoint_marker(self):
        running_marker = self._find_drive_marker("sharepoint_to_drive", "running")
        if running_marker:
            self.pipeline.update(
                "sharepoint_to_drive", "running", "Marker gedetecteerd"
            )
            self._delete_drive_marker(running_marker)
        completed_marker = self._find_drive_marker("sharepoint_to_drive", "completed")
        if completed_marker:
            self.pipeline.update(
                "sharepoint_to_drive", "completed", "Marker gedetecteerd"
            )
            self._delete_drive_marker(completed_marker)

    def _check_drive_to_sharepoint_marker(self):
        marker = self._find_drive_marker("drive_to_sharepoint", "completed")
        if marker:
            self.pipeline.update(
                "drive_to_sharepoint", "starting", "Drive → SharePoint starten"
            )
            self.pipeline.update(
                "drive_to_sharepoint", "running", "Power Automate bezig"
            )
            self.pipeline.update(
                "drive_to_sharepoint", "completed", "Marker gedetecteerd"
            )
            self._delete_drive_marker(marker)

    def _start_drive_download(self):
        self.pipeline.update("drive_download", "starting", "Drive download starten")
        self.pipeline.update("drive_download", "running", "Drive download gestart")

    def _start_drive_upload(self):
        self.pipeline.update("drive_upload", "starting", "Drive upload starten")
        self.pipeline.update("drive_upload", "running", "Drive upload gestart")

    def _start_postgis_pause(self):
        self.pipeline.update(
            "postgis_sync_pausing", "running", "PostGIS-sync pauzeren"
        )

    def _start_postgis_resume(self):
        self.pipeline.update(
            "postgis_sync_resuming", "running", "PostGIS-sync hervatten"
        )

    def _daily_reset_check(self):
        now_local = datetime.now(LOCAL_TZ)
        if now_local.hour == 0 and self._last_reset_date != now_local.date():
            self._last_reset_date = now_local.date()
            self.pipeline.update("idle", "completed", "Dagelijkse reset")

    def _load_drive_config(self):
        if not CONFIG_PATH.exists():
            return None
        import tomli
        with CONFIG_PATH.open("rb") as f:
            cfg = tomli.load(f)
        drive = cfg.get("drive", {})
        if not drive:
            return None
        return drive

    def _get_drive_service(self):
        from google.auth.transport.requests import Request
        from google_auth_oauthlib.flow import InstalledAppFlow
        from googleapiclient.discovery import build

        drive_cfg = self._load_drive_config()
        if not drive_cfg:
            return None
        creds_file = drive_cfg.get("service_account_file") or drive_cfg.get("credentials_file")
        if not creds_file or not Path(creds_file).is_file():
            return None

        token_path = Path(drive_cfg.get("token_file", str(DB_PATH.parent / "gdrive_token.pkl")))
        creds = None
        if token_path.exists():
            with open(token_path, "rb") as fh:
                import pickle
                creds = pickle.load(fh)
            if creds.expired and creds.refresh_token:
                creds.refresh(Request())
                with open(token_path, "wb") as fh:
                    pickle.dump(creds, fh)

        if not creds or not creds.valid:
            flow = InstalledAppFlow.from_client_secrets_file(creds_file, ["https://www.googleapis.com/auth/drive"])
            creds = flow.run_local_server(port=0)
            token_path.parent.mkdir(parents=True, exist_ok=True)
            with open(token_path, "wb") as fh:
                import pickle
                pickle.dump(creds, fh)

        return build("drive", "v3", credentials=creds)

    def _find_drive_marker(self, phase, expected_status):
        service = self._get_drive_service()
        if not service:
            return None
        drive_cfg = self._load_drive_config() or {}
        folder_id = drive_cfg.get("folder_id")
        if not folder_id:
            return None
        try:
            query = f"'{folder_id}' in parents and trashed = false"
            results = service.files().list(q=query, fields="files(id, name)").execute()
            for f in results.get("files", []):
                name = f["name"]
                if "_" not in name:
                    continue
                base, ext = name.rsplit(".", 1)
                parts = base.split("_", 1)
                if len(parts) != 2:
                    continue
                file_phase, file_status = parts[1], ext
                if file_phase == phase and file_status == expected_status:
                    return f["id"]
        except Exception:
            pass
        return None

    def _delete_drive_marker(self, file_id):
        service = self._get_drive_service()
        if not service or not file_id:
            return
        try:
            service.files().delete(fileId=file_id).execute()
        except Exception:
            pass


@asynccontextmanager
async def lifespan(app: FastAPI):
    if os.getenv("ORCHESTRATOR_ENABLED", "false").lower() != "true":
        yield
        return
    pipeline = PipelineState(DB_PATH)
    pipeline.ensure()
    orchestrator = PipelineOrchestrator(pipeline)
    orchestrator.start()
    yield
    orchestrator.stop()


def run_standalone(db_path: str):
    pipeline = PipelineState(db_path)
    pipeline.ensure()
    orchestrator = PipelineOrchestrator(pipeline)
    orchestrator.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        orchestrator.stop()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="RSA Health Pipeline Orchestrator")
    parser.add_argument(
        "--db",
        default=str(DB_PATH),
        help="Pad naar health.db (standaard: lib/../health.db)",
    )
    args = parser.parse_args()
    run_standalone(args.db)
