"""
Autouse resilience for a transient Python-model submission flake (test-only; the adapter is
unchanged): serverless batches transiently rejected by the shared project-wide Dataproc
``CPUS`` quota (~12 vCPUs/batch) when concurrent CI runs exhaust it. The fixture wraps the
submit path with jittered backoff so the wait lets quota free up.
"""

from __future__ import annotations

import pytest

from tests._python_model_retry import make_dataproc_quota_retry

_ATTEMPTS = 6
_BASE_DELAY = 20.0
_MAX_DELAY = 300.0


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
