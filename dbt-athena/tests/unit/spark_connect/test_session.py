"""Unit tests for SparkConnectSessionPool."""

from __future__ import annotations

import threading
import time
from unittest.mock import MagicMock

import pytest
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.athena.spark_connect.session import SparkConnectSessionPool


@pytest.fixture(autouse=True)
def _reset_pool_singleton():
    """Isolate each test from singleton state."""
    SparkConnectSessionPool._reset_for_tests()
    yield
    SparkConnectSessionPool._reset_for_tests()


def _make_client(session_ids, state="IDLE"):
    """Build a mock athena client that returns given session ids in order."""
    client = MagicMock()
    client.start_session.side_effect = [{"SessionId": sid, "State": "IDLE"} for sid in session_ids]
    client.get_session_status.return_value = {"Status": {"State": state}}
    return client


class TestSingleton:
    def test_returns_same_instance(self):
        pool_a = SparkConnectSessionPool()
        pool_b = SparkConnectSessionPool()
        assert pool_a is pool_b

    def test_singleton_reset_gives_new_instance(self):
        pool_a = SparkConnectSessionPool()
        SparkConnectSessionPool._reset_for_tests()
        pool_b = SparkConnectSessionPool()
        assert pool_a is not pool_b


class TestAcquire:
    def test_starts_new_session_when_empty(self):
        pool = SparkConnectSessionPool()
        client = _make_client(["sid-1"])

        sid = pool.acquire(
            key=("inv-a", "fp-a"),
            athena_client=client,
            spark_work_group="wg",
            engine_config={"CoordinatorDpuSize": 1},
            session_description="desc",
            max_sessions=2,
            timeout=5,
            polling_interval=0.01,
            session_concurrency=1,
        )

        assert sid == "sid-1"
        client.start_session.assert_called_once()
        snapshot = pool._snapshot()
        assert snapshot["sid-1"]["load"] == 1
        assert snapshot["sid-1"]["key"] == ("inv-a", "fp-a")

    def test_reuses_idle_session_with_same_key(self):
        pool = SparkConnectSessionPool()
        client = _make_client(["sid-1"])

        first = pool.acquire(
            key=("inv", "fp"),
            athena_client=client,
            spark_work_group="wg",
            engine_config={},
            session_description="desc",
            max_sessions=5,
            timeout=5,
            polling_interval=0.01,
            session_concurrency=1,
        )
        pool.release(first)

        second = pool.acquire(
            key=("inv", "fp"),
            athena_client=client,
            spark_work_group="wg",
            engine_config={},
            session_description="desc",
            max_sessions=5,
            timeout=5,
            polling_interval=0.01,
            session_concurrency=1,
        )

        assert first == second
        assert client.start_session.call_count == 1

    def test_starts_new_session_when_existing_is_busy(self):
        pool = SparkConnectSessionPool()
        client = _make_client(["sid-1", "sid-2"])

        first = pool.acquire(
            key=("inv", "fp"),
            athena_client=client,
            spark_work_group="wg",
            engine_config={},
            session_description="desc",
            max_sessions=2,
            timeout=5,
            polling_interval=0.01,
            session_concurrency=1,
        )
        # First session is still loaded.
        second = pool.acquire(
            key=("inv", "fp"),
            athena_client=client,
            spark_work_group="wg",
            engine_config={},
            session_description="desc",
            max_sessions=2,
            timeout=5,
            polling_interval=0.01,
            session_concurrency=1,
        )

        assert first != second
        assert client.start_session.call_count == 2

    def test_session_concurrency_allows_reuse_while_loaded(self):
        pool = SparkConnectSessionPool()
        client = _make_client(["sid-1"])

        first = pool.acquire(
            key=("inv", "fp"),
            athena_client=client,
            spark_work_group="wg",
            engine_config={},
            session_description="desc",
            max_sessions=2,
            timeout=5,
            polling_interval=0.01,
            session_concurrency=3,
        )
        # First session still loaded, but concurrency=3 allows reuse.
        second = pool.acquire(
            key=("inv", "fp"),
            athena_client=client,
            spark_work_group="wg",
            engine_config={},
            session_description="desc",
            max_sessions=2,
            timeout=5,
            polling_interval=0.01,
            session_concurrency=3,
        )
        third = pool.acquire(
            key=("inv", "fp"),
            athena_client=client,
            spark_work_group="wg",
            engine_config={},
            session_description="desc",
            max_sessions=2,
            timeout=5,
            polling_interval=0.01,
            session_concurrency=3,
        )

        assert first == second == third
        assert client.start_session.call_count == 1
        assert pool._snapshot()[first]["load"] == 3

    def test_session_concurrency_spills_to_new_session_at_limit(self):
        pool = SparkConnectSessionPool()
        client = _make_client(["sid-1", "sid-2"])

        first = pool.acquire(
            key=("inv", "fp"),
            athena_client=client,
            spark_work_group="wg",
            engine_config={},
            session_description="desc",
            max_sessions=2,
            timeout=5,
            polling_interval=0.01,
            session_concurrency=2,
        )
        second = pool.acquire(
            key=("inv", "fp"),
            athena_client=client,
            spark_work_group="wg",
            engine_config={},
            session_description="desc",
            max_sessions=2,
            timeout=5,
            polling_interval=0.01,
            session_concurrency=2,
        )
        # First session is at concurrency limit (2); next acquire must spill
        # to a new session rather than overloading the first.
        third = pool.acquire(
            key=("inv", "fp"),
            athena_client=client,
            spark_work_group="wg",
            engine_config={},
            session_description="desc",
            max_sessions=2,
            timeout=5,
            polling_interval=0.01,
            session_concurrency=2,
        )

        assert first == second
        assert third != first
        assert client.start_session.call_count == 2

    def test_sessions_with_different_keys_do_not_reuse(self):
        pool = SparkConnectSessionPool()
        client = _make_client(["sid-1", "sid-2"])

        first = pool.acquire(
            key=("inv-a", "fp-a"),
            athena_client=client,
            spark_work_group="wg",
            engine_config={},
            session_description="desc",
            max_sessions=5,
            timeout=5,
            polling_interval=0.01,
            session_concurrency=1,
        )
        pool.release(first)
        second = pool.acquire(
            key=("inv-b", "fp-b"),
            athena_client=client,
            spark_work_group="wg",
            engine_config={},
            session_description="desc",
            max_sessions=5,
            timeout=5,
            polling_interval=0.01,
            session_concurrency=1,
        )

        assert first != second

    def test_times_out_when_pool_full_and_no_session_freed(self):
        pool = SparkConnectSessionPool()
        client = _make_client(["sid-1"])
        client.get_session_status.return_value = {"Status": {"State": "IDLE"}}

        pool.acquire(
            key=("inv", "fp"),
            athena_client=client,
            spark_work_group="wg",
            engine_config={},
            session_description="desc",
            max_sessions=1,
            timeout=0.05,
            polling_interval=0.01,
            session_concurrency=1,
        )

        with pytest.raises(DbtRuntimeError, match="No Spark Connect session available"):
            pool.acquire(
                key=("inv", "fp"),
                athena_client=client,
                spark_work_group="wg",
                engine_config={},
                session_description="desc",
                max_sessions=1,
                timeout=0.05,
                polling_interval=0.01,
                session_concurrency=1,
            )


class TestSessionStartRetry:
    def test_retries_on_maximum_allowed_sessions(self, monkeypatch):
        pool = SparkConnectSessionPool()
        client = MagicMock()
        client.start_session.side_effect = [
            Exception("Maximum allowed sessions reached"),
            {"SessionId": "sid-ok", "State": "IDLE"},
        ]
        monkeypatch.setattr(time, "sleep", lambda *_: None)

        sid = pool.acquire(
            key=("inv", "fp"),
            athena_client=client,
            spark_work_group="wg",
            engine_config={},
            session_description="desc",
            max_sessions=1,
            timeout=5,
            polling_interval=0.01,
            session_concurrency=1,
        )

        assert sid == "sid-ok"
        assert client.start_session.call_count == 2

    def test_non_transient_error_is_not_retried(self):
        pool = SparkConnectSessionPool()
        client = MagicMock()
        client.start_session.side_effect = Exception("AccessDeniedException: nope")

        with pytest.raises(Exception, match="AccessDeniedException"):
            pool.acquire(
                key=("inv", "fp"),
                athena_client=client,
                spark_work_group="wg",
                engine_config={},
                session_description="desc",
                max_sessions=1,
                timeout=5,
                polling_interval=0.01,
                session_concurrency=1,
            )


class TestEviction:
    def test_dead_sessions_are_evicted(self):
        pool = SparkConnectSessionPool()
        client = MagicMock()
        pool.register("sid-dead", ("inv", "fp"), client)
        client.get_session_status.return_value = {"Status": {"State": "TERMINATED"}}

        evicted = pool._evict_dead_sessions(client)

        assert evicted == 1
        assert "sid-dead" not in pool._snapshot()

    def test_unknown_state_is_evicted(self):
        pool = SparkConnectSessionPool()
        client = MagicMock()
        pool.register("sid-x", ("inv", "fp"), client)
        client.get_session_status.side_effect = Exception("boom")

        evicted = pool._evict_dead_sessions(client)

        assert evicted == 1
        assert "sid-x" not in pool._snapshot()

    def test_idle_session_is_not_evicted(self):
        pool = SparkConnectSessionPool()
        client = MagicMock()
        pool.register("sid-ok", ("inv", "fp"), client)
        client.get_session_status.return_value = {"Status": {"State": "IDLE"}}

        evicted = pool._evict_dead_sessions(client)

        assert evicted == 0
        assert "sid-ok" in pool._snapshot()


class TestTerminate:
    def test_terminate_and_remove_calls_athena(self):
        pool = SparkConnectSessionPool()
        client = MagicMock()
        pool.register("sid-1", ("inv", "fp"), client)

        pool.terminate_and_remove("sid-1")

        client.terminate_session.assert_called_once_with(SessionId="sid-1")
        assert "sid-1" not in pool._snapshot()

    def test_terminate_and_remove_ignores_client_errors(self):
        pool = SparkConnectSessionPool()
        client = MagicMock()
        client.terminate_session.side_effect = Exception("boom")
        pool.register("sid-1", ("inv", "fp"), client)

        pool.terminate_and_remove("sid-1")  # Must not raise.

        assert "sid-1" not in pool._snapshot()

    def test_terminate_all_empties_pool(self):
        pool = SparkConnectSessionPool()
        client_a = MagicMock()
        client_b = MagicMock()
        pool.register("sid-a", ("inv", "fp1"), client_a)
        pool.register("sid-b", ("inv", "fp2"), client_b)

        pool.terminate_all()

        client_a.terminate_session.assert_called_once_with(SessionId="sid-a")
        client_b.terminate_session.assert_called_once_with(SessionId="sid-b")
        assert pool._snapshot() == {}

    def test_terminate_by_invocation_preserves_other_invocations(self):
        """The singleton is shared across invocations on multi-invocation hosts
        (dbt Cloud workers, test harnesses).  Cleanup must not kill sessions
        owned by other live invocations."""
        pool = SparkConnectSessionPool()
        mine = MagicMock()
        theirs = MagicMock()
        pool.register("sid-mine", ("inv-mine", "fp"), mine)
        pool.register("sid-theirs", ("inv-theirs", "fp"), theirs)

        pool.terminate_by_invocation("inv-mine")

        mine.terminate_session.assert_called_once_with(SessionId="sid-mine")
        theirs.terminate_session.assert_not_called()
        snapshot = pool._snapshot()
        assert "sid-mine" not in snapshot
        assert "sid-theirs" in snapshot

    def test_terminate_by_invocation_is_idempotent_when_no_match(self):
        pool = SparkConnectSessionPool()
        client = MagicMock()
        pool.register("sid-1", ("inv-a", "fp"), client)

        pool.terminate_by_invocation("inv-nonexistent")

        client.terminate_session.assert_not_called()
        assert "sid-1" in pool._snapshot()


class TestConcurrency:
    def test_concurrent_acquires_respect_max_sessions(self):
        pool = SparkConnectSessionPool()
        issued = []
        issued_lock = threading.Lock()
        counter = {"n": 0}

        def start_session(**_):
            with issued_lock:
                counter["n"] += 1
                sid = f"sid-{counter['n']}"
                issued.append(sid)
                return {"SessionId": sid, "State": "IDLE"}

        client = MagicMock()
        client.start_session.side_effect = start_session

        results: list = []
        errors: list = []

        def worker():
            try:
                sid = pool.acquire(
                    key=("inv", "fp"),
                    athena_client=client,
                    spark_work_group="wg",
                    engine_config={},
                    session_description="desc",
                    max_sessions=3,
                    timeout=5,
                    polling_interval=0.01,
                    session_concurrency=1,
                )
                results.append(sid)
            except Exception as e:  # pragma: no cover
                errors.append(e)

        threads = [threading.Thread(target=worker) for _ in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == []
        # Each thread should receive a unique session (max_sessions=3, 3 threads).
        assert len(set(results)) == 3
        assert len(issued) == 3

    def test_concurrent_acquires_reserve_slots_during_start(self):
        """A slow start_session call must not allow other threads to oversubscribe
        max_sessions while the slot is being filled."""
        pool = SparkConnectSessionPool()
        start_gate = threading.Event()
        counter = {"n": 0}
        counter_lock = threading.Lock()

        def slow_start_session(**_):
            # Block until main thread signals; concurrent acquires race with
            # the in-flight start in the meantime.
            start_gate.wait(timeout=2.0)
            with counter_lock:
                counter["n"] += 1
                sid = f"sid-{counter['n']}"
            return {"SessionId": sid, "State": "IDLE"}

        client = MagicMock()
        client.start_session.side_effect = slow_start_session
        client.get_session_status.return_value = {"Status": {"State": "IDLE"}}

        results: list = []
        errors: list = []

        def worker():
            try:
                sid = pool.acquire(
                    key=("inv", "fp"),
                    athena_client=client,
                    spark_work_group="wg",
                    engine_config={},
                    session_description="desc",
                    max_sessions=2,
                    timeout=2,
                    polling_interval=0.01,
                    session_concurrency=1,
                )
                results.append(sid)
            except Exception as e:  # pragma: no cover
                errors.append(e)

        threads = [threading.Thread(target=worker) for _ in range(5)]
        for t in threads:
            t.start()
        # Give workers a moment to queue up before releasing start_session.
        time.sleep(0.05)
        start_gate.set()
        for t in threads:
            t.join()

        # Only max_sessions (2) distinct sessions can ever be created, even
        # though 5 threads raced.  The extras time out.
        successful = [r for r in results if r is not None]
        # At most 2 started.
        assert client.start_session.call_count <= 2
        # Unique sessions must not exceed max_sessions.
        assert len(set(successful)) <= 2
        # Exactly (threads - max_sessions) should time out: placeholder
        # reservation makes the cap deterministic even under races.
        assert len(successful) == 2
        assert len(errors) == 3
        for err in errors:
            assert isinstance(err, DbtRuntimeError)
            assert "No Spark Connect session available" in str(err)


class TestCrossInvocationCleanup:
    def test_sessions_from_prior_invocations_are_evicted_on_acquire(self):
        pool = SparkConnectSessionPool()
        stale_client = MagicMock()
        pool.register("sid-stale", ("old-inv", "fp"), stale_client)

        new_client = _make_client(["sid-new"])
        sid = pool.acquire(
            key=("new-inv", "fp"),
            athena_client=new_client,
            spark_work_group="wg",
            engine_config={},
            session_description="desc",
            max_sessions=1,
            timeout=5,
            polling_interval=0.01,
            session_concurrency=1,
        )

        assert sid == "sid-new"
        stale_client.terminate_session.assert_called_once_with(SessionId="sid-stale")
        snapshot = pool._snapshot()
        assert "sid-stale" not in snapshot
        assert "sid-new" in snapshot


class TestReuseLivenessCheck:
    def test_dead_session_is_discarded_during_reuse(self):
        pool = SparkConnectSessionPool()
        client = _make_client(["sid-replacement"])

        # Pre-register an idle session, then make the liveness check report FAILED.
        pool.register("sid-stale", ("inv", "fp"), client)
        pool.release("sid-stale")
        client.get_session_status.return_value = {"Status": {"State": "FAILED"}}

        sid = pool.acquire(
            key=("inv", "fp"),
            athena_client=client,
            spark_work_group="wg",
            engine_config={},
            session_description="desc",
            max_sessions=5,
            timeout=5,
            polling_interval=0.01,
            session_concurrency=1,
        )

        assert sid == "sid-replacement"
        assert "sid-stale" not in pool._snapshot()

    def test_alive_session_is_reused(self):
        pool = SparkConnectSessionPool()
        client = MagicMock()
        client.get_session_status.return_value = {"Status": {"State": "IDLE"}}
        pool.register("sid-1", ("inv", "fp"), client)
        pool.release("sid-1")

        sid = pool.acquire(
            key=("inv", "fp"),
            athena_client=client,
            spark_work_group="wg",
            engine_config={},
            session_description="desc",
            max_sessions=5,
            timeout=5,
            polling_interval=0.01,
            session_concurrency=1,
        )

        assert sid == "sid-1"
        client.start_session.assert_not_called()
