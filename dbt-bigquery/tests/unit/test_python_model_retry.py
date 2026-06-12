"""Unit tests for the retry wrapper in ``tests/_python_model_retry``: a fake ``submit``
raises, and we assert how often it is re-invoked. ``sleep`` is stubbed for speed."""

import uuid
from types import SimpleNamespace

import pytest

from tests._python_model_retry import (
    backoff_seconds,
    is_cpu_quota_error,
    make_dataproc_quota_retry,
)

# Messages mirroring what the submit path actually raises.
_QUOTA_ERR = "Insufficient 'CPUS' quota: requested 12 but available 0"
_ALREADY_EXISTS = "409 Already exists: Failed to create batch: Batch ...custom-abc-python"
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


def _realistic_helper(config=None):
    """Helper whose _get_batch_id reads config like the real adapter: a fresh uuid per
    call when batch_id is unset, otherwise the pinned/explicit value."""
    model = {"config": config or {}}
    helper = SimpleNamespace(
        _batch_controller_client=_FakeBatchClient(),
        _project="proj",
        _region="us-central1",
        _parsed_model=model,
    )
    helper._get_batch_id = lambda: model["config"].get("batch_id", str(uuid.uuid4()))
    return helper


class TestPredicates:
    def test_cpu_quota_error_matches_only_cpu_quota(self):
        assert is_cpu_quota_error(Exception(_QUOTA_ERR)) is True
        assert is_cpu_quota_error(Exception("some other quota")) is False  # no "cpu"
        assert is_cpu_quota_error(Exception(_MODEL_ERR)) is False

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
        """The prior batch must be cleared before re-submitting, else the retry 409s."""
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
        """A 409 isn't a quota error: re-raised immediately, batch never deleted —
        preserving TestPythonDuplicateBatchIdModels' expected run #2 failure."""
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


class TestBatchIdPinning:
    """The adapter mints a fresh uuid per _get_batch_id() call when batch_id is unset, so
    the wrapper pins a stable id up front to keep create and the pre-retry delete aligned."""

    def _wrap(self, fake_submit, attempts=3):
        return make_dataproc_quota_retry(
            fake_submit, attempts, base=1, max_delay=1, sleep=_NO_SLEEP
        )

    def test_unset_batch_id_is_pinned_so_delete_matches_create(self):
        """Without pinning, each _get_batch_id() call yields a new uuid and the delete
        targets a batch that was never created, leaking it."""
        created, calls = [], []

        def fake_submit(self, code):
            created.append(self._get_batch_id())  # what create_batch would claim
            calls.append(code)
            if len(calls) < 2:
                raise Exception(_QUOTA_ERR)
            return "OK"

        helper = _realistic_helper()
        assert self._wrap(fake_submit)(helper, "code") == "OK"

        # The id stayed stable across the create -> delete -> recreate cycle...
        assert created[0] == created[1]
        # ...and the deleted batch is exactly the one the first (failed) submit created.
        deleted = helper._batch_controller_client.deleted
        assert deleted == [f"projects/proj/locations/us-central1/batches/{created[0]}"]

    def test_explicit_batch_id_is_never_overridden(self):
        helper = _realistic_helper(config={"batch_id": "custom-abc"})
        self._wrap(lambda self, code: "OK", attempts=1)(helper, "code")
        assert helper._parsed_model["config"]["batch_id"] == "custom-abc"
