"""
Retry helpers for transient BigQuery Python-model infrastructure failures.

The functional conftest wires these into autouse fixtures (see
``tests/functional/conftest.py`` for *why* each submit path flakes). The decision
logic lives here, separate from pytest, so it is importable and unit-testable
(``tests/unit/test_python_model_retry.py``). This is test-only resilience; it does
not change the adapter.

Each ``make_*`` factory takes the helper's original ``submit`` (and, for BigFrames,
``_read_json_from_gcs``) and returns a drop-in replacement that retries with bounded,
jittered exponential backoff. ``sleep`` is injectable so tests run without delay.
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


def is_notebook_job_failure(exc: BaseException) -> bool:
    """True for a *transiently* failed Colab notebook execution job (not config errors).

    Matches the JOB_STATE_FAILED path ("...notebook execution job... failed") and the
    unexpected-error-during-execution path. A dbt-side polling timeout is deliberately
    excluded: it is a legitimate terminal outcome (the timeout-error test asserts it) and
    re-running a model that outlasts its own timeout just times out again — it is not the
    "failed before writing a log" signature this retry is meant to absorb.
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
    """
    Best-effort removal of the batch a prior attempt may have registered before its quota
    rejection surfaced.

    The ``CPUS`` quota error is raised while *polling* the batch (``operation.result``),
    which is *after* ``create_batch`` has already claimed the id on the server. Dataproc
    batch ids are unique per workload, so a re-submit with the same id would then collide
    with that orphaned (terminal/``FAILED``) batch and raise ``409 Already exists`` —
    surfacing as a hard failure rather than retrying cleanly. Deleting it first lets the
    re-submit recreate the id.

    Every failure here is swallowed by design: if the quota error instead surfaced *at*
    ``create_batch`` nothing was created (``NotFound``); a batch still winding down raises
    ``FailedPrecondition``; and unit-test doubles may not wire a client at all. In all
    cases the subsequent ``create_batch`` is the real source of truth.
    """
    try:
        client = helper._batch_controller_client
        name = client.batch_path(helper._project, helper._region, helper._get_batch_id())
        client.delete_batch(name=name)
        _logger.debug("Deleted orphaned Dataproc batch before retry: %s", name)
    except Exception as exc:  # noqa: BLE001 - best-effort cleanup; create_batch decides truth
        _logger.debug("No orphaned Dataproc batch to delete before retry: %s", exc)


def make_dataproc_quota_retry(
    original_submit: Callable,
    attempts: int,
    base: float,
    max_delay: float,
    sleep: Callable[[float], None] = time.sleep,
) -> Callable:
    """
    Wrap ``ServerlessDataProcHelper.submit`` to re-submit batches rejected because the
    shared, project-wide Dataproc CPU quota is momentarily exhausted by other concurrent
    CI runs. A rejected submission allocates nothing, so this only adds waiting, never
    extra CPU pressure. Non-quota failures are re-raised immediately.

    Before each quota retry the prior attempt's batch is deleted (see
    :func:`_delete_batch_quietly`): the quota error surfaces only after ``create_batch``
    claimed the id, so re-submitting without clearing it would hit ``409 Already exists``.
    This stays strictly on the quota-retry path, so tests that deliberately reuse a fixed
    batch id across separate runs to assert the expected 409 are unaffected.
    """

    def submit_with_quota_retry(self, compiled_code):
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

    return submit_with_quota_retry


def make_bigframes_read_tracking(original_read: Callable) -> Callable:
    """
    Wrap ``BigFramesHelper._read_json_from_gcs`` to record, on the helper instance,
    whether a notebook execution log was successfully read this attempt. That flag is
    the discriminator the submit retry uses to tell a transient provisioning failure
    (no log) from a genuine model error (log present).
    """

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
    """
    Wrap ``BigFramesHelper.submit`` to re-submit notebook jobs that fail for transient
    Vertex AI backend reasons. Retries only when the submit raised a notebook-job failure
    *and* no execution log was retrieved that attempt (``_bf_log_retrieved`` unset) — so a
    genuine model error, which always writes a readable log, is never masked.

    Must be paired with :func:`make_bigframes_read_tracking`, which sets the flag.
    """

    def submit_with_infra_retry(self, compiled_code):
        for attempt in range(1, attempts + 1):
            self._bf_log_retrieved = False
            try:
                return original_submit(self, compiled_code)
            except Exception as exc:
                transient = is_notebook_job_failure(exc) and not getattr(
                    self, "_bf_log_retrieved", False
                )
                if attempt == attempts or not transient:
                    raise
                delay = backoff_seconds(attempt, base, max_delay)
                _logger.warning(
                    "BigFrames notebook job failed with no execution log "
                    "(transient infra; attempt %d/%d); retrying in %.0fs: %s",
                    attempt,
                    attempts,
                    delay,
                    exc,
                )
                sleep(delay)

    return submit_with_infra_retry
