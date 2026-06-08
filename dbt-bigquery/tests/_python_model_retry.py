"""
Retry helpers for transient BigQuery Python-model infrastructure failures.

Decision logic lives here (not in pytest) so it is importable and unit-tested in
``tests/unit/test_python_model_retry.py``; the functional conftest wires it into autouse
fixtures. Test-only — the adapter is unchanged.
"""

from __future__ import annotations

import logging
import random
import time
from typing import Any, Callable, Optional

_logger = logging.getLogger(__name__)

_JITTER_RATIO = 0.25


def is_cpu_quota_error(exc: BaseException) -> bool:
    """True only for transient Dataproc CPU-quota rejections, not real model errors."""
    message = str(exc).lower()
    return "quota" in message and "cpu" in message


def is_notebook_job_failure(exc: BaseException) -> bool:
    """True for a transiently failed notebook job, not a config error or a dbt timeout.

    A dbt-side polling timeout is excluded on purpose: it is a legitimate terminal outcome
    (the timeout-error test asserts it) and re-running just times out again.
    """
    message = str(exc).lower()
    if "did not complete within the designated timeout" in message:
        return False
    return "notebook" in message and ("failed" in message or "unexpected error" in message)


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


def _retry_submit(
    original_submit: Callable,
    attempts: int,
    base: float,
    max_delay: float,
    sleep: Callable[[float], None],
    *,
    should_retry: Callable[[Any, BaseException], bool],
    log_message: str,
    before_attempt: Optional[Callable[[Any], None]] = None,
    before_retry: Optional[Callable[[Any], None]] = None,
) -> Callable:
    """Wrap a helper ``submit`` to re-run it with jittered backoff while ``should_retry``
    holds. ``before_attempt`` runs at the top of every attempt; ``before_retry`` runs only
    before a re-submit (e.g. to clean up after the failed attempt)."""

    def wrapped(self, compiled_code):
        for attempt in range(1, attempts + 1):
            if before_attempt is not None:
                before_attempt(self)
            try:
                return original_submit(self, compiled_code)
            except Exception as exc:
                if attempt == attempts or not should_retry(self, exc):
                    raise
                if before_retry is not None:
                    before_retry(self)
                delay = backoff_seconds(attempt, base, max_delay)
                _logger.warning(
                    "%s (attempt %d/%d); retrying in %.0fs: %s",
                    log_message,
                    attempt,
                    attempts,
                    delay,
                    exc,
                )
                sleep(delay)

    return wrapped


def make_dataproc_quota_retry(
    original_submit: Callable,
    attempts: int,
    base: float,
    max_delay: float,
    sleep: Callable[[float], None] = time.sleep,
) -> Callable:
    """Re-submit serverless batches rejected by the shared, project-wide Dataproc CPU quota.

    Deletes the orphaned batch id before each retry so the re-submit does not 409. This
    stays on the quota path, so tests that reuse a fixed id to assert a 409 are unaffected.
    """
    return _retry_submit(
        original_submit,
        attempts,
        base,
        max_delay,
        sleep,
        should_retry=lambda self, exc: is_cpu_quota_error(exc),
        before_retry=_delete_batch_quietly,
        log_message="Dataproc CPU quota exhausted",
    )


def make_bigframes_read_tracking(original_read: Callable) -> Callable:
    """Wrap ``_read_json_from_gcs`` to record whether a notebook log was read this attempt."""

    def read_tracking(self, gcs_uri: str) -> Any:
        result = original_read(self, gcs_uri)
        if result:
            self._bf_log_retrieved = True
        return result

    return read_tracking


def make_bigframes_retry_submit(
    original_submit: Callable,
    attempts: int,
    base: float,
    max_delay: float,
    sleep: Callable[[float], None] = time.sleep,
) -> Callable:
    """Re-submit notebook jobs that fail with no execution log retrieved this attempt; a
    genuine model error always writes a readable log, so it is never retried. Pair with
    :func:`make_bigframes_read_tracking`, which sets the flag this checks."""
    return _retry_submit(
        original_submit,
        attempts,
        base,
        max_delay,
        sleep,
        before_attempt=lambda self: setattr(self, "_bf_log_retrieved", False),
        should_retry=lambda self, exc: is_notebook_job_failure(exc)
        and not getattr(self, "_bf_log_retrieved", False),
        log_message="BigFrames notebook job failed with no execution log (transient infra)",
    )
