import os
import threading
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI

from lib.pipeline_state import PipelineState

DB_PATH = Path(__file__).resolve().parent.parent / "health.db"


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
            except Exception as exc:
                self.pipeline.update("orchestrator", "failed", f"Fout: {exc}")
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
        self.pipeline.update(
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
            self.pipeline.update(
                "sharepoint_to_drive", "completed", "Marker gedetecteerd"
            )

    def _check_drive_to_sharepoint_marker(self):
        marker = self._find_drive_marker("drive_to_sharepoint", "completed")
        if marker:
            self.pipeline.update(
                "drive_to_sharepoint", "completed", "Marker gedetecteerd"
            )

    def _find_drive_marker(self, phase, expected_status):
        return None

    def _start_drive_download(self):
        self.pipeline.update("drive_download", "running", "Drive download gestart")

    def _start_drive_upload(self):
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
        now = datetime.now(timezone.utc)
        if now.hour == 0 and self._last_reset_date != now.date():
            self._last_reset_date = now.date()
            self.pipeline.update("idle", "completed", "Dagelijkse reset")


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
