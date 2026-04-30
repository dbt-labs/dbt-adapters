"""Unit tests for SparkConnectSessionPool."""

from __future__ import annotations

import threading
import time
from typing import Any
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


def _register(pool, session_id, key, athena_client):
    """Inject a session into the pool for tests, bypassing acquire()."""
    pool._sessions[session_id] = {"key": key, "client": athena_client, "load": 1}


def _acquire(pool: SparkConnectSessionPool, athena_client: Any, **overrides: Any) -> str:
    """Call ``pool.acquire`` with sensible defaults; override per-test as needed."""
    kwargs = dict(
        key=("inv", "fp"),
        athena_client=athena_client,
        spark_work_group="wg",
        engine_config={},
        session_description="desc",
        max_sessions=5,
        timeout=5,
        polling_interval=0.01,
        session_concurrency=1,
    )
    kwargs.update(overrides)
    return pool.acquire(**kwargs)


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

        sid = _acquire(
            pool,
            client,
            key=("inv-a", "fp-a"),
            engine_config={"CoordinatorDpuSize": 1},
            max_sessions=2,
        )

        assert sid == "sid-1"
        client.start_session.assert_called_once()
        snapshot = pool._snapshot()
        assert snapshot["sid-1"]["load"] == 1
        assert snapshot["sid-1"]["key"] == ("inv-a", "fp-a")

    def test_reuses_idle_session_with_same_key(self):
        pool = SparkConnectSessionPool()
        client = _make_client(["sid-1"])

        first = _acquire(pool, client)
        pool.release(first)
        second = _acquire(pool, client)

        assert first == second
        assert client.start_session.call_count == 1

    def test_starts_new_session_when_concurrency_is_saturated(self):
        """When the only session is at ``session_concurrency``, acquire must
        spill to a new session rather than oversubscribing."""
        pool = SparkConnectSessionPool()
        client = _make_client(["sid-1", "sid-2"])

        first = _acquire(pool, client, max_sessions=2)
        # First session still loaded (no release); concurrency=1 forces spill.
        second = _acquire(pool, client, max_sessions=2)

        assert first != second
        assert client.start_session.call_count == 2

    def test_session_concurrency_allows_reuse_while_loaded(self):
        pool = SparkConnectSessionPool()
        client = _make_client(["sid-1"])

        first = _acquire(pool, client, max_sessions=2, session_concurrency=3)
        second = _acquire(pool, client, max_sessions=2, session_concurrency=3)
        third = _acquire(pool, client, max_sessions=2, session_concurrency=3)

        assert first == second == third
        assert client.start_session.call_count == 1
        assert pool._snapshot()[first]["load"] == 3

    def test_session_concurrency_spills_to_new_session_at_limit(self):
        pool = SparkConnectSessionPool()
        client = _make_client(["sid-1", "sid-2"])

        first = _acquire(pool, client, max_sessions=2, session_concurrency=2)
        second = _acquire(pool, client, max_sessions=2, session_concurrency=2)
        # First session is at concurrency limit (2); next acquire must spill
        # to a new session rather than overloading the first.
        third = _acquire(pool, client, max_sessions=2, session_concurrency=2)

        assert first == second
        assert third != first
        assert client.start_session.call_count == 2

    def test_sessions_with_different_keys_do_not_reuse(self):
        pool = SparkConnectSessionPool()
        client = _make_client(["sid-1", "sid-2"])

        first = _acquire(pool, client, key=("inv-a", "fp-a"))
        pool.release(first)
        second = _acquire(pool, client, key=("inv-b", "fp-b"))

        assert first != second

    def test_times_out_when_pool_full_and_no_session_freed(self):
        pool = SparkConnectSessionPool()
        client = _make_client(["sid-1"])

        _acquire(pool, client, max_sessions=1, timeout=0.05)

        with pytest.raises(DbtRuntimeError, match="No Spark Connect session available"):
            _acquire(pool, client, max_sessions=1, timeout=0.05)


class TestSessionStartRetry:
    def test_retries_on_maximum_allowed_sessions(self, monkeypatch):
        pool = SparkConnectSessionPool()
        client = MagicMock()
        client.start_session.side_effect = [
            Exception("Maximum allowed sessions reached"),
            {"SessionId": "sid-ok", "State": "IDLE"},
        ]
        monkeypatch.setattr(time, "sleep", lambda *_: None)

        sid = _acquire(pool, client, max_sessions=1)

        assert sid == "sid-ok"
        assert client.start_session.call_count == 2

    def test_non_transient_error_is_not_retried(self):
        pool = SparkConnectSessionPool()
        client = MagicMock()
        client.start_session.side_effect = Exception("AccessDeniedException: nope")

        with pytest.raises(Exception, match="AccessDeniedException"):
            _acquire(pool, client, max_sessions=1)


class TestEviction:
    def test_dead_sessions_are_evicted(self):
        pool = SparkConnectSessionPool()
        client = MagicMock()
        _register(pool, "sid-dead", ("inv", "fp"), client)
        client.get_session_status.return_value = {"Status": {"State": "TERMINATED"}}

        evicted = pool._evict_dead_sessions(client)

        assert evicted == 1
        assert "sid-dead" not in pool._snapshot()

    def test_unknown_state_is_evicted(self):
        pool = SparkConnectSessionPool()
        client = MagicMock()
        _register(pool, "sid-x", ("inv", "fp"), client)
        client.get_session_status.side_effect = Exception("boom")

        evicted = pool._evict_dead_sessions(client)

        assert evicted == 1
        assert "sid-x" not in pool._snapshot()

    def test_idle_session_is_not_evicted(self):
        pool = SparkConnectSessionPool()
        client = MagicMock()
        _register(pool, "sid-ok", ("inv", "fp"), client)
        client.get_session_status.return_value = {"Status": {"State": "IDLE"}}

        evicted = pool._evict_dead_sessions(client)

        assert evicted == 0
        assert "sid-ok" in pool._snapshot()


class TestTerminate:
    def test_terminate_calls_athena(self):
        pool = SparkConnectSessionPool()
        client = MagicMock()
        _register(pool, "sid-1", ("inv", "fp"), client)

        pool.terminate("sid-1")

        client.terminate_session.assert_called_once_with(SessionId="sid-1")
        assert "sid-1" not in pool._snapshot()

    def test_terminate_ignores_client_errors(self):
        pool = SparkConnectSessionPool()
        client = MagicMock()
        client.terminate_session.side_effect = Exception("boom")
        _register(pool, "sid-1", ("inv", "fp"), client)

        pool.terminate("sid-1")  # Must not raise.

        assert "sid-1" not in pool._snapshot()

    def test_terminate_by_invocation_preserves_other_invocations(self):
        """The singleton is shared across invocations on multi-invocation hosts
        (dbt Cloud workers, test harnesses).  Cleanup must not kill sessions
        owned by other live invocations."""
        pool = SparkConnectSessionPool()
        mine = MagicMock()
        theirs = MagicMock()
        _register(pool, "sid-mine", ("inv-mine", "fp"), mine)
        _register(pool, "sid-theirs", ("inv-theirs", "fp"), theirs)

        pool.terminate_by_invocation("inv-mine")

        mine.terminate_session.assert_called_once_with(SessionId="sid-mine")
        theirs.terminate_session.assert_not_called()
        snapshot = pool._snapshot()
        assert "sid-mine" not in snapshot
        assert "sid-theirs" in snapshot

    def test_terminate_by_invocation_is_idempotent_when_no_match(self):
        pool = SparkConnectSessionPool()
        client = MagicMock()
        _register(pool, "sid-1", ("inv-a", "fp"), client)

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
                sid = _acquire(pool, client, max_sessions=3)
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
        """5 threads race against an in-flight ``start_session``; the pool lock
        must serialize creation so exactly ``max_sessions`` threads succeed and
        the remainder time out.

        This is deterministic, not a "best-effort" check: the 2 success / 3
        timeout split is exact because acquire holds ``self._lock`` across
        ``start_session`` and timeout=2 is well below the 5s test budget.
        """
        pool = SparkConnectSessionPool()
        start_gate = threading.Event()
        all_workers_queued = threading.Barrier(parties=6)  # 5 workers + main
        counter = {"n": 0}
        counter_lock = threading.Lock()

        def slow_start_session(**_):
            # Block until main signals; concurrent acquires race in the meantime.
            start_gate.wait(timeout=5.0)
            with counter_lock:
                counter["n"] += 1
                return {"SessionId": f"sid-{counter['n']}", "State": "IDLE"}

        client = MagicMock()
        client.start_session.side_effect = slow_start_session
        client.get_session_status.return_value = {"Status": {"State": "IDLE"}}

        results: list = []
        errors: list = []

        def worker():
            try:
                all_workers_queued.wait(timeout=5.0)
                sid = _acquire(pool, client, max_sessions=2, timeout=2)
                results.append(sid)
            except DbtRuntimeError as e:
                errors.append(e)

        threads = [threading.Thread(target=worker) for _ in range(5)]
        for t in threads:
            t.start()
        all_workers_queued.wait(timeout=5.0)
        start_gate.set()
        for t in threads:
            t.join()

        assert len(set(results)) == 2
        assert len(errors) == 3
        assert client.start_session.call_count == 2
        for err in errors:
            assert "No Spark Connect session available" in str(err)


class TestCrossInvocationCleanup:
    def test_sessions_from_prior_invocations_are_evicted_on_acquire(self):
        pool = SparkConnectSessionPool()
        stale_client = MagicMock()
        _register(pool, "sid-stale", ("old-inv", "fp"), stale_client)

        new_client = _make_client(["sid-new"])
        sid = _acquire(pool, new_client, key=("new-inv", "fp"), max_sessions=1)

        assert sid == "sid-new"
        stale_client.terminate_session.assert_called_once_with(SessionId="sid-stale")
        snapshot = pool._snapshot()
        assert "sid-stale" not in snapshot
        assert "sid-new" in snapshot

    def test_stale_sessions_are_terminated_even_when_start_session_fails(self):
        """Non-transient start_session failures must not leak prior-invocation sessions."""
        pool = SparkConnectSessionPool()
        stale_client = MagicMock()
        _register(pool, "sid-stale", ("old-inv", "fp"), stale_client)

        new_client = MagicMock()
        new_client.start_session.side_effect = Exception("AccessDeniedException: nope")

        with pytest.raises(Exception, match="AccessDeniedException"):
            _acquire(pool, new_client, key=("new-inv", "fp"), max_sessions=1)

        stale_client.terminate_session.assert_called_once_with(SessionId="sid-stale")
        assert "sid-stale" not in pool._snapshot()


class TestReuseLivenessCheck:
    def test_dead_session_is_discarded_during_reuse(self):
        """When a stale session is reserved for reuse but the liveness check
        reports it dead, the pool must discard it and start a replacement.

        ``acquire()`` uses the caller-supplied client for both the liveness
        probe (FAILED) and the replacement ``start_session`` call, so a single
        mock fields both roles.
        """
        pool = SparkConnectSessionPool()

        client = _make_client(["sid-replacement"])
        client.get_session_status.return_value = {"Status": {"State": "FAILED"}}
        _register(pool, "sid-stale", ("inv", "fp"), client)
        pool.release("sid-stale")

        sid = _acquire(pool, client)

        assert sid == "sid-replacement"
        assert "sid-stale" not in pool._snapshot()

    def test_alive_session_is_reused(self):
        pool = SparkConnectSessionPool()
        client = MagicMock()
        client.get_session_status.return_value = {"Status": {"State": "IDLE"}}
        _register(pool, "sid-1", ("inv", "fp"), client)
        pool.release("sid-1")

        sid = _acquire(pool, client)

        assert sid == "sid-1"
        client.start_session.assert_not_called()

    def test_liveness_failure_does_not_affect_other_sessions_with_same_key(self):
        """Discarding a dead session must not touch sibling sessions for the same key."""
        pool = SparkConnectSessionPool()

        client = _make_client(["sid-fresh"])

        def status(SessionId):
            return {"Status": {"State": "FAILED" if SessionId == "sid-dead" else "IDLE"}}

        client.get_session_status.side_effect = status
        _register(pool, "sid-dead", ("inv", "fp"), client)
        _register(pool, "sid-alive", ("inv", "fp"), client)
        pool.release("sid-dead")
        pool.release("sid-alive")

        sid = _acquire(pool, client, max_sessions=3)

        # Whichever of the two reuse candidates was checked first, the alive
        # one must still be present in the pool afterwards.
        snapshot = pool._snapshot()
        assert "sid-dead" not in snapshot
        assert "sid-alive" in snapshot
        assert sid in {"sid-alive", "sid-fresh"}
