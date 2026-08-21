"""Backward-compatibele shim — re-export van pipeline_state.

Gebruik in plaats van dit bestand:
    from sqlite_writer.pipeline_state import enqueue_sqlite_job, PipelineState
"""

from sqlite_writer.pipeline_state import (
    PENDING_DIR,
    QUEUE_DIR,
    enqueue_sqlite_job,
)

__all__ = ["enqueue_sqlite_job", "QUEUE_DIR", "PENDING_DIR"]
