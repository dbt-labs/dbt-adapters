import threading
import time
from unittest.mock import MagicMock, patch

from dbt.adapters.athena.spark_connect import keepalive as keepalive_module
from dbt.adapters.athena.spark_connect.keepalive import SessionKeepalive
from dbt.adapters.athena.spark_connect.pyspark_patches import _RETRY_BLOCK_THREAD_LOCAL


def _wait_for(predicate, timeout=2.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return False


def test_sends_select_until_stopped():
    spark = MagicMock()
    keepalive = SessionKeepalive(spark, "sid-1", interval=0.02)
    keepalive.start()
    assert _wait_for(lambda: spark.sql.call_count >= 2)
    keepalive.stop()

    calls_after_stop = spark.sql.call_count
    time.sleep(0.1)
    assert spark.sql.call_count == calls_after_stop
    spark.sql.assert_called_with("SELECT 1")
    spark.sql.return_value.collect.assert_called()


def test_keeps_running_after_a_failed_operation():
    spark = MagicMock()
    spark.sql.return_value.collect.side_effect = [RuntimeError("boom"), None, None]
    keepalive = SessionKeepalive(spark, "sid-1", interval=0.02)
    keepalive.start()
    assert _wait_for(lambda: spark.sql.call_count >= 3)
    keepalive.stop()


def test_waits_for_the_interval_before_the_first_operation():
    spark = MagicMock()
    keepalive = SessionKeepalive(spark, "sid-1", interval=10)
    keepalive.start()
    time.sleep(0.1)
    keepalive.stop()
    spark.sql.assert_not_called()


def test_operations_run_with_client_retries_disabled():
    seen = []
    spark = MagicMock()
    spark.sql.side_effect = (
        lambda _q: seen.append(getattr(_RETRY_BLOCK_THREAD_LOCAL, "disabled", False))
        or MagicMock()
    )
    keepalive = SessionKeepalive(spark, "sid-1", interval=0.02)
    keepalive.start()
    assert _wait_for(lambda: len(seen) >= 1)
    keepalive.stop()
    assert all(seen)


def test_warns_once_per_failure_streak():
    spark = MagicMock()
    spark.sql.return_value.collect.side_effect = [
        RuntimeError("a"),
        RuntimeError("b"),
        None,
        RuntimeError("c"),
    ] + [None] * 100
    with patch.object(keepalive_module, "LOGGER") as logger:
        keepalive = SessionKeepalive(spark, "sid-1", interval=0.01)
        keepalive.start()
        assert _wait_for(lambda: spark.sql.call_count >= 5)
        keepalive.stop()
    warned = [c.args[0] for c in logger.warning.call_args_list]
    assert len(warned) == 2
    assert warned[0].endswith(": a") and warned[1].endswith(": c")


def test_stop_reports_an_operation_still_running():
    release = threading.Event()
    spark = MagicMock()
    spark.sql.return_value.collect.side_effect = lambda: release.wait(2)
    with (
        patch.object(keepalive_module, "_STOP_JOIN_TIMEOUT_SECONDS", 0.05),
        patch.object(keepalive_module, "LOGGER") as logger,
    ):
        keepalive = SessionKeepalive(spark, "sid-1", interval=0.01)
        keepalive.start()
        assert _wait_for(lambda: spark.sql.call_count >= 1)
        keepalive.stop()
        assert any("still finishing" in c.args[0] for c in logger.debug.call_args_list)
    release.set()
