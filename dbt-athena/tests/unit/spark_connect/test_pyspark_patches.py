"""Tests for the pyspark Spark Connect runtime workarounds."""

import sys
import types
import warnings

import pytest


@pytest.fixture
def fake_reattach_module(monkeypatch):
    """Inject a fake pyspark.sql.connect.client.reattach with a stub iterator class."""

    class FakePool:
        def __init__(self):
            self.closed = False

        def close(self):
            self.closed = True

        def join(self):
            pass

    class FakeIterator:
        _release_thread_pool = FakePool()

        @classmethod
        def shutdown(cls):
            if cls._release_thread_pool is not None:
                cls._release_thread_pool.close()
                cls._release_thread_pool = None

    fake_pyspark = types.ModuleType("pyspark")
    fake_sql = types.ModuleType("pyspark.sql")
    fake_connect = types.ModuleType("pyspark.sql.connect")
    fake_client = types.ModuleType("pyspark.sql.connect.client")
    fake_reattach = types.ModuleType("pyspark.sql.connect.client.reattach")
    fake_reattach.ExecutePlanResponseReattachableIterator = FakeIterator

    monkeypatch.setitem(sys.modules, "pyspark", fake_pyspark)
    monkeypatch.setitem(sys.modules, "pyspark.sql", fake_sql)
    monkeypatch.setitem(sys.modules, "pyspark.sql.connect", fake_connect)
    monkeypatch.setitem(sys.modules, "pyspark.sql.connect.client", fake_client)
    monkeypatch.setitem(sys.modules, "pyspark.sql.connect.client.reattach", fake_reattach)
    return FakeIterator


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
