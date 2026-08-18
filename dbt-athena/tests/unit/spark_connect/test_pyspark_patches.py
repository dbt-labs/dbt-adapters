"""Tests for the pyspark Spark Connect runtime workarounds."""

import sys
import types
import warnings
from unittest.mock import MagicMock

import pytest


@pytest.fixture
def fake_pyspark_modules(monkeypatch):
    """Inject fake pyspark.sql.connect.client.{reattach,core} modules."""

    class FakePool:
        def __init__(self):
            self.closed = False

        def close(self):
            self.closed = True

        def join(self):
            pass

    class FakeIterator:
        _release_thread_pool = FakePool()

        def __init__(self, *args, **kwargs):
            self._stub = kwargs.get("stub")
            self._iterator = kwargs.get("iterator")
            self._metadata = kwargs.get("metadata")

        @classmethod
        def shutdown(cls):
            if cls._release_thread_pool is not None:
                cls._release_thread_pool.close()
                cls._release_thread_pool = None

        def _call_iter(self, iter_fun):
            return iter_fun()

        def _release_until(self, until_response_id):
            self._released_until = until_response_id

        def _release_all(self):
            self._released_all = True

    class FakeSparkConnectClient:
        def __init__(self, stub=None, builder=None):
            self._stub = stub
            self._builder = builder

        @classmethod
        def retry_exception(cls, e):
            import grpc as _grpc

            return isinstance(e, _grpc.RpcError) and e.code() == _grpc.StatusCode.UNAVAILABLE

    fake_pyspark = types.ModuleType("pyspark")
    fake_sql = types.ModuleType("pyspark.sql")
    fake_connect = types.ModuleType("pyspark.sql.connect")
    # ``pyspark.sql.connect.client`` must be a package so ``pyspark.sql.connect.client.core``
    # can be imported as a submodule; setting ``__path__`` makes it a namespace package.
    fake_client = types.ModuleType("pyspark.sql.connect.client")
    fake_client.__path__ = []  # type: ignore[attr-defined]
    fake_reattach = types.ModuleType("pyspark.sql.connect.client.reattach")
    fake_reattach.ExecutePlanResponseReattachableIterator = FakeIterator
    fake_core = types.ModuleType("pyspark.sql.connect.client.core")
    fake_core.SparkConnectClient = FakeSparkConnectClient

    monkeypatch.setitem(sys.modules, "pyspark", fake_pyspark)
    monkeypatch.setitem(sys.modules, "pyspark.sql", fake_sql)
    monkeypatch.setitem(sys.modules, "pyspark.sql.connect", fake_connect)
    monkeypatch.setitem(sys.modules, "pyspark.sql.connect.client", fake_client)
    monkeypatch.setitem(sys.modules, "pyspark.sql.connect.client.reattach", fake_reattach)
    monkeypatch.setitem(sys.modules, "pyspark.sql.connect.client.core", fake_core)
    return types.SimpleNamespace(iterator=FakeIterator, client=FakeSparkConnectClient)


@pytest.fixture
def fake_reattach_module(fake_pyspark_modules):
    return fake_pyspark_modules.iterator


@pytest.fixture
def fake_spark_client_module(fake_pyspark_modules):
    return fake_pyspark_modules.client


@pytest.fixture(autouse=True)
def _reset_patch_state():
    # The patch module tracks application via a process-wide flag; reset it so each
    # test exercises a fresh first-application path.
    import dbt.adapters.athena.spark_connect.pyspark_patches as m

    m._patches_applied = False
    yield
    m._patches_applied = False


def test_shutdown_becomes_noop_after_patch(fake_reattach_module):
    from dbt.adapters.athena.spark_connect.pyspark_patches import apply_pyspark_workarounds

    apply_pyspark_workarounds()

    pool_before = fake_reattach_module._release_thread_pool
    assert pool_before is not None

    fake_reattach_module.shutdown()

    assert fake_reattach_module._release_thread_pool is pool_before
    assert pool_before.closed is False


def test_apply_is_idempotent(fake_reattach_module):
    from dbt.adapters.athena.spark_connect.pyspark_patches import apply_pyspark_workarounds

    apply_pyspark_workarounds()
    patched_shutdown = fake_reattach_module.__dict__["shutdown"]
    apply_pyspark_workarounds()

    assert fake_reattach_module.__dict__["shutdown"] is patched_shutdown


def test_release_execute_warning_is_filtered(fake_reattach_module):
    from dbt.adapters.athena.spark_connect.pyspark_patches import apply_pyspark_workarounds

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        apply_pyspark_workarounds()
        warnings.warn("ReleaseExecute failed with exception: Channel closed!")
        warnings.warn("some other warning")

    messages = [str(w.message) for w in caught]
    assert "some other warning" in messages
    assert not any("ReleaseExecute failed" in m for m in messages)


def _make_stub_with_builder(token="t1"):
    builder = MagicMock()
    builder._auth_token = token
    builder.metadata.return_value = [("x-aws-proxy-auth", token)]
    stub = MagicMock(_dbt_athena_builder=builder)
    return stub, builder


def test_client_init_stashes_builder_on_stub(fake_pyspark_modules):
    from dbt.adapters.athena.spark_connect.pyspark_patches import apply_pyspark_workarounds

    apply_pyspark_workarounds()

    builder = MagicMock()
    builder.metadata.return_value = [("x-aws-proxy-auth", "t")]
    stub = MagicMock()
    fake_pyspark_modules.client(stub=stub, builder=builder)

    assert stub._dbt_athena_builder is builder


def test_call_iter_refreshes_metadata_on_reattach(fake_reattach_module, fake_spark_client_module):
    from dbt.adapters.athena.spark_connect.pyspark_patches import apply_pyspark_workarounds

    apply_pyspark_workarounds()

    stub, builder = _make_stub_with_builder("fresh")
    iterator = fake_reattach_module(
        stub=stub, iterator=None, metadata=[("x-aws-proxy-auth", "stale")]
    )
    iterator._call_iter(lambda: "ok")

    assert iterator._metadata == [("x-aws-proxy-auth", "fresh")]


def test_call_iter_skips_refresh_when_iterator_active(
    fake_reattach_module, fake_spark_client_module
):
    from dbt.adapters.athena.spark_connect.pyspark_patches import apply_pyspark_workarounds

    apply_pyspark_workarounds()

    stub, builder = _make_stub_with_builder("t")
    iterator = fake_reattach_module(
        stub=stub, iterator="existing", metadata=[("x-aws-proxy-auth", "current")]
    )
    iterator._call_iter(lambda: "ok")

    builder.metadata.assert_not_called()
    assert iterator._metadata == [("x-aws-proxy-auth", "current")]


def test_call_iter_tolerates_missing_channel_builder(
    fake_reattach_module, fake_spark_client_module
):
    from dbt.adapters.athena.spark_connect.pyspark_patches import apply_pyspark_workarounds

    apply_pyspark_workarounds()

    stub = MagicMock(spec=[])  # no _dbt_athena_builder attribute
    iterator = fake_reattach_module(stub=stub, iterator=None, metadata=[("k", "v")])

    iterator._call_iter(lambda: "ok")
    assert iterator._metadata == [("k", "v")]


def test_call_iter_tolerates_builder_metadata_exception(
    fake_reattach_module, fake_spark_client_module
):
    from dbt.adapters.athena.spark_connect.pyspark_patches import apply_pyspark_workarounds

    apply_pyspark_workarounds()

    stub, builder = _make_stub_with_builder("t")
    builder.metadata.side_effect = RuntimeError("transient")
    iterator = fake_reattach_module(stub=stub, iterator=None, metadata=[("k", "v")])
    iterator._call_iter(lambda: "ok")  # must not raise

    assert iterator._metadata == [("k", "v")]


def test_retry_exception_allows_first_permission_denied(
    fake_reattach_module, fake_spark_client_module
):
    import grpc

    from dbt.adapters.athena.spark_connect.pyspark_patches import apply_pyspark_workarounds

    apply_pyspark_workarounds()

    stub, _ = _make_stub_with_builder("t")
    iterator = fake_reattach_module(stub=stub, iterator=None, metadata=[("k", "v")])
    assert iterator._dbt_athena_pd_retried is False

    class _Err(grpc.RpcError):
        def code(self):
            return grpc.StatusCode.PERMISSION_DENIED

    assert fake_spark_client_module.retry_exception(_Err()) is True
    assert iterator._dbt_athena_pd_retried is True


def test_call_iter_resets_pd_budget_on_success(fake_reattach_module, fake_spark_client_module):
    from dbt.adapters.athena.spark_connect.pyspark_patches import apply_pyspark_workarounds

    apply_pyspark_workarounds()

    stub, _ = _make_stub_with_builder("t")
    iterator = fake_reattach_module(stub=stub, iterator=None, metadata=[("k", "v")])
    iterator._dbt_athena_pd_retried = True
    iterator._call_iter(lambda: "ok")

    assert iterator._dbt_athena_pd_retried is False


def test_call_iter_does_not_reset_pd_budget_on_exception(
    fake_reattach_module, fake_spark_client_module
):
    from dbt.adapters.athena.spark_connect.pyspark_patches import apply_pyspark_workarounds

    apply_pyspark_workarounds()

    stub, _ = _make_stub_with_builder("t")
    iterator = fake_reattach_module(stub=stub, iterator=None, metadata=[("k", "v")])
    iterator._dbt_athena_pd_retried = True

    def _boom():
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError):
        iterator._call_iter(_boom)
    assert iterator._dbt_athena_pd_retried is True


def test_release_until_refreshes_metadata(fake_reattach_module, fake_spark_client_module):
    from dbt.adapters.athena.spark_connect.pyspark_patches import apply_pyspark_workarounds

    apply_pyspark_workarounds()

    stub, builder = _make_stub_with_builder("fresh")
    iterator = fake_reattach_module(
        stub=stub, iterator="active", metadata=[("x-aws-proxy-auth", "stale")]
    )
    iterator._release_until("response-1")

    builder.metadata.assert_called_once()
    assert iterator._metadata == [("x-aws-proxy-auth", "fresh")]
    assert iterator._released_until == "response-1"


def test_release_all_refreshes_metadata(fake_reattach_module, fake_spark_client_module):
    from dbt.adapters.athena.spark_connect.pyspark_patches import apply_pyspark_workarounds

    apply_pyspark_workarounds()

    stub, builder = _make_stub_with_builder("fresh")
    iterator = fake_reattach_module(
        stub=stub, iterator="active", metadata=[("x-aws-proxy-auth", "stale")]
    )
    iterator._release_all()

    builder.metadata.assert_called_once()
    assert iterator._metadata == [("x-aws-proxy-auth", "fresh")]
    assert iterator._released_all is True


def test_release_tolerates_missing_channel_builder(fake_reattach_module, fake_spark_client_module):
    from dbt.adapters.athena.spark_connect.pyspark_patches import apply_pyspark_workarounds

    apply_pyspark_workarounds()

    stub = MagicMock(spec=[])
    iterator = fake_reattach_module(stub=stub, iterator="active", metadata=[("k", "v")])
    iterator._release_until("r")
    iterator._release_all()  # must not raise

    assert iterator._released_until == "r"
    assert iterator._released_all is True


def test_retry_exception_rejects_second_permission_denied(
    fake_reattach_module, fake_spark_client_module
):
    import grpc

    from dbt.adapters.athena.spark_connect.pyspark_patches import apply_pyspark_workarounds

    apply_pyspark_workarounds()

    stub, _ = _make_stub_with_builder("t")
    iterator = fake_reattach_module(stub=stub, iterator=None, metadata=[("k", "v")])

    class _Err(grpc.RpcError):
        def code(self):
            return grpc.StatusCode.PERMISSION_DENIED

    assert fake_spark_client_module.retry_exception(_Err()) is True
    assert fake_spark_client_module.retry_exception(_Err()) is False
    assert iterator._dbt_athena_pd_retried is True


def test_retry_exception_rejects_after_iterator_garbage_collected(
    fake_reattach_module, fake_spark_client_module
):
    import gc

    import grpc

    from dbt.adapters.athena.spark_connect.pyspark_patches import apply_pyspark_workarounds

    apply_pyspark_workarounds()

    stub, _ = _make_stub_with_builder("t")
    fake_reattach_module(stub=stub, iterator=None, metadata=[("k", "v")])
    gc.collect()  # weakref iterator can be reclaimed once nothing holds a strong ref.

    class _Err(grpc.RpcError):
        def code(self):
            return grpc.StatusCode.PERMISSION_DENIED

    assert fake_spark_client_module.retry_exception(_Err()) is False


def test_retry_exception_still_retries_unavailable(fake_reattach_module, fake_spark_client_module):
    import grpc

    from dbt.adapters.athena.spark_connect.pyspark_patches import apply_pyspark_workarounds

    apply_pyspark_workarounds()

    class _Err(grpc.RpcError):
        def code(self):
            return grpc.StatusCode.UNAVAILABLE

    assert fake_spark_client_module.retry_exception(_Err()) is True


def test_retry_exception_does_not_retry_unrelated_codes(
    fake_reattach_module, fake_spark_client_module
):
    import grpc

    from dbt.adapters.athena.spark_connect.pyspark_patches import apply_pyspark_workarounds

    apply_pyspark_workarounds()

    class _Err(grpc.RpcError):
        def code(self):
            return grpc.StatusCode.INVALID_ARGUMENT

    assert fake_spark_client_module.retry_exception(_Err()) is False
