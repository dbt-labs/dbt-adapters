"""
Unit tests for the Python-model submit retry wrappers in ``tests/_python_model_retry``.

These lock in the retry *decisions* without touching Dataproc or Vertex AI: a fake
``submit`` raises the relevant exception (and, for BigFrames, simulates whether the
notebook log was read) and we assert how many times it is re-invoked. ``sleep`` is
stubbed so the tests run instantly.
"""

from types import SimpleNamespace

import pytest

from tests._python_model_retry import (
    backoff_seconds,
    is_cpu_quota_error,
    is_notebook_job_failure,
    make_bigframes_read_tracking,
    make_bigframes_retry_submit,
    make_dataproc_quota_retry,
)

# Messages mirroring what each path actually raises.
_QUOTA_ERR = "Insufficient 'CPUS' quota: requested 12 but available 0"
_ALREADY_EXISTS = "409 Already exists: Failed to create batch: Batch ...custom-abc-python"
_NOTEBOOK_FAIL = "The colab notebook execution job 'projects/p/.../123' failed."
_CONFIG_ERR = "Unsupported credential method in BigFrames: 'foo'"
_MODEL_ERR = "name 'undefined_var' is not defined"

_NO_SLEEP = lambda _delay: None  # noqa: E731


class _FakeBatchClient:
    """Records delete_batch calls and builds resource names like the real client."""

    def __init__(self):
        self.deleted = []

    def batch_path(self, project, region, batch_id):
        return f"projects/{project}/locations/{region}/batches/{batch_id}"

    def delete_batch(self, name):
        self.deleted.append(name)


def _fake_helper(batch_id="custom-abc-python"):
    """A stand-in ServerlessDataProcHelper with the attrs _delete_batch_quietly reads."""
    return SimpleNamespace(
        _batch_controller_client=_FakeBatchClient(),
        _project="proj",
        _region="us-central1",
        _get_batch_id=lambda: batch_id,
    )


class TestPredicates:
    def test_cpu_quota_error_matches_only_cpu_quota(self):
        assert is_cpu_quota_error(Exception(_QUOTA_ERR)) is True
        assert is_cpu_quota_error(Exception("some other quota")) is False  # no "cpu"
        assert is_cpu_quota_error(Exception(_MODEL_ERR)) is False

    def test_notebook_job_failure_matches_failure_not_config_or_model_error(self):
        assert is_notebook_job_failure(Exception(_NOTEBOOK_FAIL)) is True
        assert is_notebook_job_failure(Exception("notebook job timeout")) is True
        assert is_notebook_job_failure(Exception(_CONFIG_ERR)) is False  # no failure verb
        assert is_notebook_job_failure(Exception(_MODEL_ERR)) is False  # not about a notebook

    def test_backoff_grows_and_is_capped(self):
        # base=10, cap=60: 10, 20, 40, 60, 60 ... plus up to 25% jitter, never below base.
        assert 10 <= backoff_seconds(1, 10, 60) <= 12.5
        assert 60 <= backoff_seconds(5, 10, 60) <= 75  # capped, then jittered


class TestDataprocQuotaRetry:
    def _wrap(self, fake_submit, attempts=3):
        return make_dataproc_quota_retry(
            fake_submit, attempts, base=1, max_delay=1, sleep=_NO_SLEEP
        )

    def test_retries_quota_error_then_succeeds(self):
        calls = []

        def fake_submit(self, code):
            calls.append(code)
            if len(calls) < 3:
                raise Exception(_QUOTA_ERR)
            return "OK"

        assert self._wrap(fake_submit)(SimpleNamespace(), "code") == "OK"
        assert len(calls) == 3

    def test_non_quota_error_is_not_retried(self):
        calls = []

        def fake_submit(self, code):
            calls.append(code)
            raise ValueError(_MODEL_ERR)

        with pytest.raises(ValueError):
            self._wrap(fake_submit)(SimpleNamespace(), "code")
        assert len(calls) == 1

    def test_exhausts_attempts_then_raises(self):
        calls = []

        def fake_submit(self, code):
            calls.append(code)
            raise Exception(_QUOTA_ERR)

        with pytest.raises(Exception, match="quota"):
            self._wrap(fake_submit, attempts=4)(SimpleNamespace(), "code")
        assert len(calls) == 4

    def test_deletes_orphaned_batch_before_each_quota_retry(self):
        """
        The quota error surfaces after create_batch claimed the id, so the prior batch
        must be cleared before re-submitting or the retry hits 409 Already exists.
        """
        calls = []

        def fake_submit(self, code):
            calls.append(code)
            if len(calls) < 3:
                raise Exception(_QUOTA_ERR)
            return "OK"

        helper = _fake_helper()
        assert self._wrap(fake_submit)(helper, "code") == "OK"
        # One delete before each of the two retries; none after the successful submit.
        expected = "projects/proj/locations/us-central1/batches/custom-abc-python"
        assert helper._batch_controller_client.deleted == [expected, expected]

    def test_no_delete_on_409_already_exists(self):
        """
        A 409 is not a quota error, so it is re-raised immediately and the duplicate-id
        batch is never deleted — preserving TestPythonDuplicateBatchIdModels' run #2.
        """
        calls = []

        def fake_submit(self, code):
            calls.append(code)
            raise Exception(_ALREADY_EXISTS)

        helper = _fake_helper()
        with pytest.raises(Exception, match="Already exists"):
            self._wrap(fake_submit)(helper, "code")
        assert len(calls) == 1
        assert helper._batch_controller_client.deleted == []

    def test_delete_failure_does_not_break_retry(self):
        """Cleanup is best-effort: a delete that raises must not abort the retry loop."""
        calls = []

        def fake_submit(self, code):
            calls.append(code)
            if len(calls) < 2:
                raise Exception(_QUOTA_ERR)
            return "OK"

        helper = _fake_helper()
        helper._batch_controller_client.delete_batch = lambda name: (_ for _ in ()).throw(
            RuntimeError("FailedPrecondition: batch not in terminal state")
        )
        assert self._wrap(fake_submit)(helper, "code") == "OK"
        assert len(calls) == 2


class TestBigframesReadTracking:
    def test_sets_flag_when_log_present_and_returns_value(self):
        log = {"cells": [{"outputs": []}]}
        tracked = make_bigframes_read_tracking(lambda self, uri: log)
        helper = SimpleNamespace(_bf_log_retrieved=False)

        assert tracked(helper, "gs://b/o") is log
        assert helper._bf_log_retrieved is True

    def test_leaves_flag_unset_when_no_log(self):
        tracked = make_bigframes_read_tracking(lambda self, uri: None)  # 404 -> None
        helper = SimpleNamespace(_bf_log_retrieved=False)

        assert tracked(helper, "gs://b/o") is None
        assert helper._bf_log_retrieved is False


class TestBigframesRetrySubmit:
    def _wrap(self, fake_submit, attempts=3):
        return make_bigframes_retry_submit(
            fake_submit, attempts, base=1, max_delay=1, sleep=_NO_SLEEP
        )

    def test_retries_when_job_failed_with_no_log_then_succeeds(self):
        """Transient infra: notebook job failed and no execution log was read."""
        calls = []

        def fake_submit(self, code):
            calls.append(code)
            if len(calls) < 2:
                # No log read this attempt -> _bf_log_retrieved stays False.
                raise Exception(_NOTEBOOK_FAIL)
            return "OK"

        assert self._wrap(fake_submit)(SimpleNamespace(), "code") == "OK"
        assert len(calls) == 2

    def test_does_not_retry_when_log_was_read(self):
        """Genuine model error: a readable log exists, so the failure is not retried."""
        calls = []

        def fake_submit(self, code):
            calls.append(code)
            self._bf_log_retrieved = True  # the adapter successfully read the error log
            raise Exception(_NOTEBOOK_FAIL)

        with pytest.raises(Exception, match="failed"):
            self._wrap(fake_submit)(SimpleNamespace(), "code")
        assert len(calls) == 1  # no retry — real error not masked

    def test_does_not_retry_non_notebook_errors(self):
        calls = []

        def fake_submit(self, code):
            calls.append(code)
            raise ValueError(_CONFIG_ERR)

        with pytest.raises(ValueError):
            self._wrap(fake_submit)(SimpleNamespace(), "code")
        assert len(calls) == 1

    def test_exhausts_attempts_then_raises(self):
        calls = []

        def fake_submit(self, code):
            calls.append(code)
            raise Exception(_NOTEBOOK_FAIL)  # always transient, never recovers

        with pytest.raises(Exception, match="failed"):
            self._wrap(fake_submit, attempts=3)(SimpleNamespace(), "code")
        assert len(calls) == 3

    def test_composes_with_read_tracking_so_logged_error_is_not_retried(self):
        """
        Faithful composition: the submit body reads the log via the *tracked* reader, so
        a real error (log present) flips the flag through the same path production uses.
        """
        calls = []
        tracked_read = make_bigframes_read_tracking(lambda self, uri: {"cells": []})

        def fake_submit(self, code):
            calls.append(code)
            self._read_json_from_gcs("gs://b/o")  # adapter reads the log -> flag set
            raise Exception(_NOTEBOOK_FAIL)

        helper = SimpleNamespace()
        helper._read_json_from_gcs = lambda uri: tracked_read(helper, uri)

        with pytest.raises(Exception, match="failed"):
            self._wrap(fake_submit)(helper, "code")
        assert len(calls) == 1
