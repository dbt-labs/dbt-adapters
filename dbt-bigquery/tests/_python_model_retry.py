"""
Retry helper for a transient BigQuery Python-model infrastructure failure: serverless
batches rejected by the shared, project-wide Dataproc CPU quota.

Decision logic lives here (not in pytest) so it is importable and unit-tested in
``tests/unit/test_python_model_retry.py``; the functional conftest wires it into an autouse
fixture. Test-only — the adapter is unchanged.
"""

from __future__ import annotations

import logging
import random
import time
from typing import Any, Callable

_logger = logging.getLogger(__name__)

_JITTER_RATIO = 0.25


def is_cpu_quota_error(exc: BaseException) -> bool:
    """True only for transient Dataproc CPU-quota rejections, not real model errors."""
    message = str(exc).lower()
    return "quota" in message and "cpu" in message


def backoff_seconds(attempt: int, base: float, max_delay: float) -> float:
    """Jittered exponential backoff for a 1-based attempt, capped at ``max_delay``."""
    delay = min(base * 2 ** (attempt - 1), max_delay)
    return delay + random.uniform(0, delay * _JITTER_RATIO)


def _delete_batch_quietly(helper: Any) -> None:
    """Best-effort delete of the batch a prior attempt registered before its quota error.

    The quota error surfaces while polling, after ``create_batch`` already claimed the id,
    so re-submitting the same id would hit ``409 Already exists``. Any failure here is
    swallowed: the subsequent ``create_batch`` is the source of truth.
    """
    try:
        client = helper._batch_controller_client
        name = client.batch_path(helper._project, helper._region, helper._get_batch_id())
        client.delete_batch(name=name)
        _logger.debug("Deleted orphaned Dataproc batch before retry: %s", name)
    except Exception as exc:  # noqa: BLE001 - best-effort cleanup
        _logger.debug("No orphaned Dataproc batch to delete before retry: %s", exc)


def make_dataproc_quota_retry(
    original_submit: Callable,
    attempts: int,
    base: float,
    max_delay: float,
    sleep: Callable[[float], None] = time.sleep,
) -> Callable:
    """Re-submit serverless batches rejected by the shared, project-wide Dataproc CPU quota,
    with jittered backoff so the wait lets quota free up (an instant rerun just re-hits it).

    Deletes the orphaned batch id before each retry so the re-submit does not 409. This stays
    on the quota path, so tests that reuse a fixed id to assert a 409 are unaffected.
    """

    def wrapped(self, compiled_code):
        for attempt in range(1, attempts + 1):
            try:
                return original_submit(self, compiled_code)
            except Exception as exc:
                if attempt == attempts or not is_cpu_quota_error(exc):
                    raise
                _delete_batch_quietly(self)
                delay = backoff_seconds(attempt, base, max_delay)
                _logger.warning(
                    "Dataproc CPU quota exhausted (attempt %d/%d); retrying in %.0fs: %s",
                    attempt,
                    attempts,
                    delay,
                    exc,
                )
                sleep(delay)

    return wrapped
