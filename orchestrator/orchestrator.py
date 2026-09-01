import json
import logging
import os
import subprocess
import sys
import threading
import time
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from fastapi import FastAPI

logging.basicConfig(
    format="%(asctime)s %(levelname)-8s %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)

_HERE = Path(__file__).resolve().parent
_ROOT = _HERE.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import sqlite3

from sqlite_writer.pipeline_state import PipelineState
from sqlite_writer.sqlite_file_writer import (
    ensure_database_schema,
    open_database,
    prune_done_queue,
)

DB_PATH = Path(__file__).parent.parent / "health.db"
CONFIG_PATH = DB_PATH.parent.parent / 'config' / "config_rsa_health.json"
LOCAL_TZ = ZoneInfo("Europe/Brussels")

QUEUE_DIR = Path(os.environ.get("SQLITE_QUEUE_DIR", "/opt/data-platform/sqlite_queue"))
DONE_DIR = QUEUE_DIR / "done"
QUEUE_DONE_RETENTION_DAYS = int(os.environ.get("SQLITE_QUEUE_DONE_RETENTION_DAYS", "7"))


PIPELINE_TIMEOUTS = {
    "arango_sync": 4 * 3600,
    "postgis_pause": 600,
    "rsa_queries": 3.5 * 3600,
    "postgis_resume": 600,
}

POLL_INTERVAL_SECONDS = 30

REPORTS_GIT_DIR = Path(__file__).resolve().parent.parent.parent / "RSA"
REPORTS_REFRESH_INTERVAL_SECONDS = int(
    os.environ.get("REPORTS_REFRESH_INTERVAL_SECONDS", "3600")
)


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
        self._wait_accepted_statuses = set()
        self._wait_deadline = 0
        self._wait_timeout = 0
        self._last_reset_date = None
        self._last_prune_date = None
        self._last_reports_refresh = None

    def start(self):
        self.running = True
        self._prune_queue()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
        logging.info("Orchestrator loop gestart")

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
                except Exception as log_exc:
                    logging.error(f"Fout bij loggen van orchestrator fout: {log_exc}")
            time.sleep(self.POLL_INTERVAL)

    def _tick(self):
        self._daily_reset_check()
        self._prune_check()
        self._reports_refresh_check()
        state = self.pipeline.get()
        if not state:
            return
        phase = state.get("phase", "idle")
        status = state.get("status", "completed")

        if self._is_waiting():
            if phase == self._wait_phase and status in self._wait_accepted_statuses:
                self._clear_wait()
                logging.info("Wachten voltooid: %s=%s", phase, status)
            elif time.time() > self._wait_deadline:
                timed_out = self._wait_phase
                timeout_val = self._wait_timeout
                self._clear_wait()
                self._handle_wait_timeout(timed_out, timeout_val)
            return

        if phase == "idle" and status == "completed":
            self._check_sharepoint_marker()
        elif phase == "sharepoint_to_drive" and status == "running":
            self._check_sharepoint_marker()
        elif phase == "sharepoint_to_drive" and status == "completed":
            found = self._find_drive_marker("sharepoint_to_drive", "running")
            if found:
                running_marker, _ = found
                self.pipeline.update(
                    "sharepoint_to_drive", "running", "Nieuwe run marker gedetecteerd"
                )
                logging.info("SharePoint marker gedetecteerd: running (nieuwe run)")
                self._delete_drive_marker(running_marker)
            else:
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
            self._wait_for("rsa_queries", ["completed", "time-out"], self.TIMEOUT_RSA)
        elif phase == "rsa_queries" and status in ("completed", "time-out"):
            self._start_postgis_resume()
        elif phase == "postgis_sync_resuming" and status == "running":
            self._wait_for(
                "postgis_sync_running", "completed", self.TIMEOUT_POSTGIS_RESUME
            )
        elif phase == "postgis_sync_running" and status == "completed":
            self._start_drive_upload()
        elif phase == "drive_upload" and status == "completed":
            # Signal Power Automate that the overview is ready by uploading the
            # drive_to_sharepoint.starting marker (Power Automate then performs the
            # SharePoint step and places the .completed marker we consume below).
            if not self._find_drive_marker("drive_to_sharepoint", "starting"):
                if self._create_drive_marker("drive_to_sharepoint", "starting"):
                    self.pipeline.update(
                        "drive_to_sharepoint", "starting",
                        "Marker geüpload; wacht op Power Automate",
                    )
            self._check_drive_to_sharepoint_marker()
        elif phase == "drive_to_sharepoint":
            # Keep polling for the .completed marker Power Automate places after the
            # SharePoint step. Without this branch the orchestrator would set the
            # drive_to_sharepoint phase once and never re-check for completion.
            self._check_drive_to_sharepoint_marker()

    def _is_waiting(self):
        return self._wait_phase is not None

    def _wait_for(self, phase, statuses, timeout):
        """Wacht op een van de gegeven statussen voor de fase.
        statuses mag een string zijn (één status) of een lijst van strings.
        """
        if isinstance(statuses, str):
            statuses = [statuses]
        self._wait_phase = phase
        self._wait_status = statuses[0]
        self._wait_accepted_statuses = set(statuses)
        self._wait_deadline = time.time() + timeout
        self._wait_timeout = timeout
        logging.info(
            "Wachten op %s met status %s (timeout %ss)",
            phase,
            ", ".join(statuses),
            timeout,
        )

    def _handle_wait_timeout(self, timed_out_phase: str, timeout_val: int):
        """Gebruik de juiste timeout-behandeling per fase.

        - postgis_sync_paused: gaat verder zonder pauze (per gebruikerspecificatie)
        - postgis_sync_running: gaat verder naar drive_upload
        - arango_sync: zet op failed (veiligheids-net als RSA crash)
        - rsa_queries: zet op failed (RSA zou self-timeout als time-out moeten rapporteren)
        """
        logging.warning(
            "Timeout na %ss wachten op %s", timeout_val, timed_out_phase
        )
        if timed_out_phase in ("postgis_sync_paused", "postgis_sync_running"):
            self.pipeline.update(
                timed_out_phase,
                "completed",
                f"Timeout ({timeout_val}s); doorgaan zonder wachttijd",
            )
        else:
            self.pipeline.update(
                timed_out_phase or "orchestrator",
                "failed",
                f"Timeout na {timeout_val}s wachten op {timed_out_phase}",
            )

    def _clear_wait(self):
        self._wait_phase = None
        self._wait_status = None
        self._wait_accepted_statuses = set()
        self._wait_deadline = 0
        self._wait_timeout = 0

    def _check_sharepoint_marker(self):
        found = self._find_drive_marker("sharepoint_to_drive", "running")
        if found:
            running_marker, _ = found
            state = self.pipeline.get()
            if state and state.get("phase") == "idle" and state.get("status") == "completed":
                self._clear_history()
            self.pipeline.update(
                "sharepoint_to_drive", "running", "Marker gedetecteerd"
            )
            logging.info("SharePoint marker gedetecteerd: running")
            self._delete_drive_marker(running_marker)
        found = self._find_drive_marker("sharepoint_to_drive", "completed")
        if found:
            completed_marker, _ = found
            self.pipeline.update(
                "sharepoint_to_drive", "completed", "Marker gedetecteerd"
            )
            logging.info("SharePoint marker gedetecteerd: completed")
            self._delete_drive_marker(completed_marker)

    def _check_drive_to_sharepoint_marker(self):
        found = self._find_drive_marker("drive_to_sharepoint", "completed")
        if found:
            marker, _ = found
            # also remove the .starting marker we uploaded earlier, if still present
            starting_found = self._find_drive_marker("drive_to_sharepoint", "starting")
            starting_marker = starting_found[0] if starting_found else None
            if starting_marker:
                self._delete_drive_marker(starting_marker)
            self.pipeline.update(
                "drive_to_sharepoint", "starting", "Drive → SharePoint starten"
            )
            logging.info("Drive → SharePoint marker gedetecteerd")
            self.pipeline.update(
                "drive_to_sharepoint", "running", "Power Automate bezig"
            )
            self.pipeline.update(
                "drive_to_sharepoint", "completed", "Marker gedetecteerd"
            )
            logging.info("Drive → SharePoint voltooid")
            self._delete_drive_marker(marker)

    def _start_drive_download(self):
        self.pipeline.update("drive_download", "starting", "Drive download starten")
        logging.info("Drive download starten")
        self.pipeline.update("drive_download", "running", "Drive download gestart")
        logging.info("Drive download gestart")

    def _start_drive_upload(self):
        self.pipeline.update("drive_upload", "starting", "Drive upload starten")
        logging.info("Drive upload starten")
        self.pipeline.update("drive_upload", "running", "Drive upload gestart")
        logging.info("Drive upload gestart")

    def _start_postgis_pause(self):
        self.pipeline.update(
            "postgis_sync_pausing", "running", "PostGIS-sync pauzeren"
        )
        logging.info("PostGIS-sync pauzeren")

    def _start_postgis_resume(self):
        self.pipeline.update(
            "postgis_sync_resuming", "running", "PostGIS-sync hervatten"
        )
        logging.info("PostGIS-sync hervatten")

    def _daily_reset_check(self):
        now_local = datetime.now(LOCAL_TZ)
        if now_local.hour == 0 and self._last_reset_date != now_local.date():
            self._last_reset_date = now_local.date()
            state = self.pipeline.get()
            if state and state.get("phase") == "idle" and state.get("status") == "completed":
                logging.info("Dagelijkse reset: pipeline was al idle")
                self._clear_wait()
                return
            self.pipeline.update("idle", "completed", "Dagelijkse reset")
            self._clear_history()
            self._clear_wait()
            logging.info("Dagelijkse reset uitgevoerd")

    def _prune_queue(self):
        try:
            removed = prune_done_queue(QUEUE_DONE_RETENTION_DAYS, DONE_DIR)
            if removed:
                logging.info("Queue prune: %d done-jobs verwijderd", removed)
        except Exception as exc:
            logging.error("Fout bij queue prune: %s", exc)

    def _prune_check(self):
        now_local = datetime.now(LOCAL_TZ)
        if now_local.hour == 0 and self._last_prune_date != now_local.date():
            self._last_prune_date = now_local.date()
            self._prune_queue()

    def _reports_refresh_check(self):
        interval = REPORTS_REFRESH_INTERVAL_SECONDS
        now = time.time()
        if (
            self._last_reports_refresh is not None
            and (now - self._last_reports_refresh) < interval
        ):
            return
        self._last_reports_refresh = now
        self._git_pull_reports()

    def _git_pull_reports(self):
        git_path = REPORTS_GIT_DIR
        if not git_path.is_dir():
            logging.warning("Reports git directory niet gevonden: %s", git_path)
            return
        if not (git_path / ".git").exists():
            logging.warning("Reports pad is geen git repo: %s", git_path)
            return
        try:
            result = subprocess.run(
                ["git", "-C", str(git_path), "pull"],
                capture_output=True,
                text=True,
                timeout=180,
                check=False,
            )
            if result.returncode == 0:
                out = result.stdout.strip()
                if not out or "up to date" in out.lower():
                    logging.info("Reports git pull: reeds up to date")
                else:
                    last_line = out.splitlines()[-1] if out else "voltooid"
                    logging.info("Reports git pull: %s", last_line)
            else:
                stderr = result.stderr.strip() or result.stdout.strip()
                logging.warning(
                    "Reports git pull mislukt (code %s): %s", result.returncode, stderr
                )
        except subprocess.TimeoutExpired:
            logging.warning("Reports git pull timed out")
        except Exception as exc:
            logging.warning("Reports git pull fout: %s", exc)

    def _clear_history(self):
        self.pipeline.clear_history()

    def _load_config(self):
        if not CONFIG_PATH.exists():
            return None
        try:
            with CONFIG_PATH.open("r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return None

    def _load_drive_config(self):
        cfg = self._load_config()
        if not cfg:
            return None
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
        # Markers are named "<yyyy-mm-dd>_<phase>.<status>" using the local date, so
        # only consider today's markers (avoids acting on stale markers from prior days).
        today = datetime.now(LOCAL_TZ).strftime("%Y-%m-%d")
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
                if parts[0] != today:
                    continue
                file_phase, file_status = parts[1], ext
                if file_phase == phase and file_status == expected_status:
                    return f["id"], name
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

    def _create_drive_marker(self, phase, status):
        """Upload an empty marker file ``<prefix>_<phase>.<status>`` to the Drive folder.

        Used to signal Power Automate that a pipeline step is starting (e.g.
        ``rsa_drive_to_sharepoint.starting``), so it can perform the SharePoint side
        and place the corresponding ``.completed`` marker. Returns True on success.
        """
        service = self._get_drive_service()
        if not service:
            return False
        drive_cfg = self._load_drive_config() or {}
        folder_id = drive_cfg.get("folder_id")
        if not folder_id:
            return False
        # Marker filename is "<yyyy-mm-dd>_<phase>.<status>" using the local date,
        # matching the convention Power Automate uses (e.g. "2026-08-31_drive_to_sharepoint.starting").
        # Derived from the clock so it is always correct, also after a restart.
        prefix = datetime.now(LOCAL_TZ).strftime("%Y-%m-%d")
        name = f"{prefix}_{phase}.{status}"
        try:
            import io

            from googleapiclient.http import MediaIoBaseUpload

            media = MediaIoBaseUpload(io.BytesIO(b""), mimetype="text/plain", resumable=False)
            service.files().create(
                body={"name": name, "parents": [folder_id]},
                media_body=media,
                fields="id",
            ).execute()
            logging.info("Drive marker aangemaakt: %s", name)
            return True
        except Exception as exc:
            logging.error("Fout bij aanmaken drive marker %s: %s", name, exc)
            return False


@asynccontextmanager
async def lifespan(app: FastAPI):
    if os.getenv("ORCHESTRATOR_ENABLED", "false").lower() != "true":
        yield
        return
    pipeline = PipelineState(DB_PATH)
    orchestrator = PipelineOrchestrator(pipeline)
    orchestrator.start()
    yield
    orchestrator.stop()


def run_standalone(db_path: str):
    logging.info("Orchestrator gestart (standalone)")
    conn = open_database()
    ensure_database_schema(conn)
    conn.close()

    pipeline = PipelineState(db_path)
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
