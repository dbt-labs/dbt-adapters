"""
Autouse resilience for two transient Python-model submission flakes (test-only; the
adapter is unchanged). Each fixture wraps a submit path with jittered backoff:

1. Serverless batches transiently rejected by the shared project-wide Dataproc ``CPUS``
   quota (~12 vCPUs/batch) when concurrent CI runs exhaust it.
2. BigFrames notebook jobs that fail before writing their GCS log — distinct from a real
   model error, which always leaves a readable log (so we never mask one).

Caps are env-tunable (defaults in the constants below): DBT_TEST_DATAPROC_QUOTA_RETRY_*
(ATTEMPTS/BASE_DELAY/MAX_DELAY) and DBT_TEST_BIGFRAMES_RETRY_*.
"""

from __future__ import annotations

import os

import pytest

from tests._python_model_retry import (
    make_bigframes_read_tracking,
    make_bigframes_retry_submit,
    make_dataproc_quota_retry,
)

_ATTEMPTS = max(1, int(os.getenv("DBT_TEST_DATAPROC_QUOTA_RETRY_ATTEMPTS", "6")))
_BASE_DELAY = float(os.getenv("DBT_TEST_DATAPROC_QUOTA_RETRY_BASE_DELAY", "20"))
_MAX_DELAY = float(os.getenv("DBT_TEST_DATAPROC_QUOTA_RETRY_MAX_DELAY", "300"))

# Shorter than the Dataproc caps: notebook jobs are slow and fail fast.
_BF_ATTEMPTS = max(1, int(os.getenv("DBT_TEST_BIGFRAMES_RETRY_ATTEMPTS", "3")))
_BF_BASE_DELAY = float(os.getenv("DBT_TEST_BIGFRAMES_RETRY_BASE_DELAY", "15"))
_BF_MAX_DELAY = float(os.getenv("DBT_TEST_BIGFRAMES_RETRY_MAX_DELAY", "60"))


@pytest.fixture(autouse=True)
def retry_dataproc_cpu_quota(monkeypatch):
    """Retry serverless batch submits rejected by the shared Dataproc CPU quota."""
    from dbt.adapters.bigquery import python_submissions

    helper = python_submissions.ServerlessDataProcHelper
    monkeypatch.setattr(
        helper,
        "submit",
        make_dataproc_quota_retry(helper.submit, _ATTEMPTS, _BASE_DELAY, _MAX_DELAY),
    )


@pytest.fixture(autouse=True)
def retry_bigframes_transient_infra(monkeypatch):
    """Retry BigFrames notebook jobs that fail before writing a log; read-tracking
    distinguishes those from genuine model errors (which always leave a log)."""
    from dbt.adapters.bigquery import python_submissions

    helper = python_submissions.BigFramesHelper
    monkeypatch.setattr(
        helper, "_read_json_from_gcs", make_bigframes_read_tracking(helper._read_json_from_gcs)
    )
    monkeypatch.setattr(
        helper,
        "submit",
        make_bigframes_retry_submit(helper.submit, _BF_ATTEMPTS, _BF_BASE_DELAY, _BF_MAX_DELAY),
    )
