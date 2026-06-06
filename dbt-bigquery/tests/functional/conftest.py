"""
Functional-test resilience for transient BigQuery Python-model infrastructure failures.

Two independent submission paths flake for environmental (not model-code) reasons:

1. Serverless Dataproc CPU-quota contention. Models submitted with the default
   ``serverless`` method each request ~12 vCPUs from a single, project-wide Dataproc
   ``CPUS`` quota shared across every CI run executing at the same time (separate PRs,
   the nightly branch matrix, releases), so a batch submission can be *transiently*
   rejected with ``RESOURCE_EXHAUSTED`` / "Insufficient 'CPUS' quota" even when the
   model is fine. A rejected submission allocates no CPUs, so the remedy is simply to
   wait for other runs to release capacity and re-submit.

2. BigFrames Vertex AI notebook-job provisioning failures. Models submitted with
   ``submission_method='bigframes'`` run as Colab notebook execution jobs. Those jobs
   can reach ``JOB_STATE_FAILED`` within seconds for transient backend/capacity reasons,
   dying before they write their execution log to GCS — so the adapter's log fetch 404s.
   That "failed + no log" signature is distinct from a real model error, which always
   produces a readable GCS log (it is exactly how the error-path tests assert their
   messages). We retry only on that transient signature, so a genuine model error is
   never masked.

The autouse fixtures below wrap each submit path with bounded, jittered exponential
backoff. This is test-only resilience; it does not change the adapter.

The caps are environment-tunable so they can be adjusted in CI without code changes:

    DBT_TEST_DATAPROC_QUOTA_RETRY_ATTEMPTS    total attempts (default 6)
    DBT_TEST_DATAPROC_QUOTA_RETRY_BASE_DELAY  first backoff, seconds (default 20)
    DBT_TEST_DATAPROC_QUOTA_RETRY_MAX_DELAY   per-attempt cap, seconds (default 300)
    DBT_TEST_BIGFRAMES_RETRY_ATTEMPTS         total attempts (default 3)
    DBT_TEST_BIGFRAMES_RETRY_BASE_DELAY       first backoff, seconds (default 15)
    DBT_TEST_BIGFRAMES_RETRY_MAX_DELAY        per-attempt cap, seconds (default 60)
"""

from __future__ import annotations

import os

import pytest

from tests.conftest import oauth_target, service_account_target
from tests._python_model_retry import (
    make_bigframes_read_tracking,
    make_bigframes_retry_submit,
    make_dataproc_quota_retry,
)

_ATTEMPTS = max(1, int(os.getenv("DBT_TEST_DATAPROC_QUOTA_RETRY_ATTEMPTS", "6")))
_BASE_DELAY = float(os.getenv("DBT_TEST_DATAPROC_QUOTA_RETRY_BASE_DELAY", "20"))
_MAX_DELAY = float(os.getenv("DBT_TEST_DATAPROC_QUOTA_RETRY_MAX_DELAY", "300"))

# BigFrames notebook-job retry caps (see module docstring). Shorter than the
# Dataproc caps because notebook jobs are slow and the failure surfaces quickly.
_BF_ATTEMPTS = max(1, int(os.getenv("DBT_TEST_BIGFRAMES_RETRY_ATTEMPTS", "3")))
_BF_BASE_DELAY = float(os.getenv("DBT_TEST_BIGFRAMES_RETRY_BASE_DELAY", "15"))
_BF_MAX_DELAY = float(os.getenv("DBT_TEST_BIGFRAMES_RETRY_MAX_DELAY", "60"))


@pytest.fixture(scope="class")
def dbt_profile_target(request):
    """
    Mirror the root ``dbt_profile_target`` but force ``threads: 1`` for ``flaky`` tests.

    The flaky Python-model tests submit one Dataproc/BigFrames job per thread, and it is
    the *parallel* submissions that trigger the transient CPU-quota and notebook-job
    provisioning failures this module retries around. Running those tests serially removes
    the self-inflicted contention while leaving concurrency unchanged for every other
    functional test (which keep the default of 4 threads).
    """
    profile_type = request.config.getoption("--profile")
    if profile_type == "oauth":
        target = oauth_target()
    elif profile_type == "service_account":
        target = service_account_target()
    else:
        raise ValueError(f"Invalid profile type '{profile_type}'")
    if request.node.get_closest_marker("flaky"):
        target["threads"] = 1
    return target


@pytest.fixture(autouse=True)
def retry_dataproc_cpu_quota(monkeypatch):
    """
    Re-submit serverless Python-model batches that are rejected because the shared,
    project-wide Dataproc CPU quota is momentarily exhausted by other concurrent CI
    runs. A rejected submission allocates nothing, so this only adds waiting, never
    extra CPU pressure. Non-quota failures (and all other submission methods) are
    left untouched.
    """
    from dbt.adapters.bigquery import python_submissions

    helper = python_submissions.ServerlessDataProcHelper
    monkeypatch.setattr(
        helper,
        "submit",
        make_dataproc_quota_retry(helper.submit, _ATTEMPTS, _BASE_DELAY, _MAX_DELAY),
    )


@pytest.fixture(autouse=True)
def retry_bigframes_transient_infra(monkeypatch):
    """
    Re-submit BigFrames notebook jobs that fail for transient Vertex AI backend reasons.

    The discriminator is whether the job's execution log made it to GCS. A real model
    error always writes a readable log (the error-path tests rely on this), whereas a
    runtime that fails to provision dies before logging, so the adapter's log fetch
    returns nothing. We therefore retry only when the submit raised a notebook-job
    failure *and* no log was retrieved during that attempt — never masking a genuine
    model error. Other submission methods are untouched.
    """
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
