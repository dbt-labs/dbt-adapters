"""Unit tests for SparkConnectSessionPool."""

from __future__ import annotations

import threading
import time
from typing import Any
from unittest.mock import MagicMock

import pytest
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.athena.spark_connect import session as session_module
from dbt.adapters.athena.spark_connect.session import SparkConnectSessionPool


def _spy_logger_warnings(monkeypatch) -> list[str]:
    """Capture LOGGER.warning calls; AdapterLogger bypasses stdlib logging."""
    captured: list[str] = []
    monkeypatch.setattr(
        session_module.LOGGER, "warning", lambda msg, *a, **k: captured.append(str(msg))
    )
    return captured


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


def _register(pool, session_id, key, athena_client, dpu=1, load=1, idle_since=None):
    """Inject a session into the pool for tests, bypassing acquire()."""
    pool._sessions[session_id] = {
        "key": key,
        "client": athena_client,
        "load": load,
        "dpu": dpu,
        "draining": False,
        "idle_since": idle_since,
        "spark": None,
    }


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
        dpu_request=1,
        dpu_budget=160,
    )
    kwargs.update(overrides)
    return pool.acquire(**kwargs)


class _Livelock(BaseException):
    """Aborts a test whose acquire keeps re-probing the same session."""


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

    def test_timeout_reports_draining_and_unknown_state_sessions(self):
        pool = SparkConnectSessionPool()
        unknown = MagicMock()
        unknown.get_session_status.side_effect = Exception("boom")
        _register(pool, "sid-unknown", ("inv", "fp"), unknown, load=0)
        _register(pool, "sid-draining", ("inv", "fp-other"), MagicMock(), load=1)
        pool._sessions["sid-draining"]["draining"] = True

        with pytest.raises(DbtRuntimeError) as excinfo:
            _acquire(pool, MagicMock(), max_sessions=1, timeout=0.05)

        assert "draining=1" in str(excinfo.value)
        assert "unknown_state_skipped=1" in str(excinfo.value)


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

        evicted = pool._evict_dead_sessions()

        assert evicted == 1
        assert "sid-dead" not in pool._snapshot()

    def test_unknown_state_is_not_evicted(self):
        pool = SparkConnectSessionPool()
        client = MagicMock()
        _register(pool, "sid-x", ("inv", "fp"), client)
        client.get_session_status.side_effect = Exception("boom")

        evicted = pool._evict_dead_sessions()

        assert evicted == 0
        assert "sid-x" in pool._snapshot()

    def test_unknown_state_idle_past_the_idle_timeout_is_terminated(self):
        pool = SparkConnectSessionPool()
        client = MagicMock()
        long_ago = time.monotonic() - session_module.SESSION_IDLE_TIMEOUT_MIN * 60 - 1
        _register(pool, "sid-x", ("inv", "fp"), client, load=0, idle_since=long_ago)
        client.get_session_status.side_effect = Exception("boom")

        assert pool._evict_dead_sessions() == 1
        assert "sid-x" not in pool._snapshot()
        client.terminate_session.assert_called_once_with(SessionId="sid-x")

    @pytest.mark.parametrize(
        "load, idle_for",
        [(0, session_module.SESSION_IDLE_TIMEOUT_MIN * 60 - 30), (1, 10**6), (0, None)],
    )
    def test_unknown_state_is_kept_unless_idle_past_the_idle_timeout(self, load, idle_for):
        pool = SparkConnectSessionPool()
        client = MagicMock()
        idle_since = None if idle_for is None else time.monotonic() - idle_for
        _register(pool, "sid-x", ("inv", "fp"), client, load=load, idle_since=idle_since)
        client.get_session_status.side_effect = Exception("boom")

        assert pool._evict_dead_sessions() == 0
        assert "sid-x" in pool._snapshot()
        client.terminate_session.assert_not_called()

    def test_release_to_zero_load_starts_idle_clock_and_reuse_clears_it(self):
        pool = SparkConnectSessionPool()
        client = MagicMock()
        client.get_session_status.return_value = {"Status": {"State": "IDLE"}}
        _register(pool, "sid-x", ("inv", "fp"), client, load=1)

        before = time.monotonic()
        pool.release("sid-x")
        assert pool._sessions["sid-x"]["idle_since"] >= before

        assert _acquire(pool, MagicMock()) == "sid-x"
        assert pool._sessions["sid-x"]["idle_since"] is None

    def test_unknown_same_key_session_past_idle_timeout_is_freed_during_acquire(self):
        pool = SparkConnectSessionPool()
        unknown = MagicMock()
        unknown.get_session_status.side_effect = Exception("boom")
        long_ago = time.monotonic() - session_module.SESSION_IDLE_TIMEOUT_MIN * 60 - 1
        _register(pool, "sid-x", ("inv", "fp"), unknown, load=0, idle_since=long_ago)

        sid = _acquire(pool, _make_client(["sid-new"]), max_sessions=1, timeout=2)

        assert sid == "sid-new"
        assert "sid-x" not in pool._snapshot()
        unknown.terminate_session.assert_called_once_with(SessionId="sid-x")

    def test_release_with_remaining_load_keeps_idle_clock_unset(self):
        pool = SparkConnectSessionPool()
        _register(pool, "sid-x", ("inv", "fp"), MagicMock(), load=2)

        pool.release("sid-x")

        assert pool._sessions["sid-x"]["idle_since"] is None

    @pytest.mark.parametrize("status", [{}, {"State": ""}])
    def test_empty_state_is_not_evicted(self, status):
        pool = SparkConnectSessionPool()
        client = MagicMock()
        _register(pool, "sid-x", ("inv", "fp"), client)
        client.get_session_status.return_value = {"Status": status}

        assert pool._evict_dead_sessions() == 0
        assert "sid-x" in pool._snapshot()

    def test_state_is_checked_with_each_sessions_own_client(self):
        pool = SparkConnectSessionPool()
        owner_dead = MagicMock()
        owner_dead.get_session_status.return_value = {"Status": {"State": "TERMINATED"}}
        owner_ok = MagicMock()
        owner_ok.get_session_status.return_value = {"Status": {"State": "IDLE"}}
        _register(pool, "sid-ok", ("inv", "fp"), owner_ok)
        _register(pool, "sid-dead", ("inv", "fp"), owner_dead)

        assert pool._evict_dead_sessions() == 1

        owner_dead.get_session_status.assert_called_once_with(SessionId="sid-dead")
        owner_ok.get_session_status.assert_called_once_with(SessionId="sid-ok")
        snapshot = pool._snapshot()
        assert "sid-dead" not in snapshot
        assert "sid-ok" in snapshot

    def test_idle_session_is_not_evicted(self):
        pool = SparkConnectSessionPool()
        client = MagicMock()
        _register(pool, "sid-ok", ("inv", "fp"), client)
        client.get_session_status.return_value = {"Status": {"State": "IDLE"}}

        evicted = pool._evict_dead_sessions()

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

    def test_terminate_drains_shared_session_instead_of_killing_it(self):
        """A transient failure on one caller must not tear a shared session
        out from under its co-tenants (session_concurrency > 1)."""
        pool = SparkConnectSessionPool()
        client = MagicMock()
        _register(pool, "sid-1", ("inv", "fp"), client, load=2)

        pool.terminate("sid-1")

        client.terminate_session.assert_not_called()
        info = pool._snapshot()["sid-1"]
        assert info["load"] == 1
        assert info["draining"] is True

    def test_release_terminates_drained_session_when_last_caller_leaves(self):
        pool = SparkConnectSessionPool()
        client = MagicMock()
        _register(pool, "sid-1", ("inv", "fp"), client, load=2)

        pool.terminate("sid-1")  # co-tenant still attached -> drains
        pool.release("sid-1")  # last caller leaves

        client.terminate_session.assert_called_once_with(SessionId="sid-1")
        assert "sid-1" not in pool._snapshot()

    def test_draining_session_is_not_reused_by_attach(self):
        pool = SparkConnectSessionPool()
        client = MagicMock()
        _register(pool, "sid-1", ("inv", "fp"), client, load=2)
        pool.terminate("sid-1")  # -> load 1, draining

        assert pool._attach(("inv", "fp"), session_concurrency=5) is None

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
        _register(pool, "sid-stale", ("old-inv", "fp"), stale_client, load=0)

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
        _register(pool, "sid-stale", ("old-inv", "fp"), stale_client, load=0)

        new_client = MagicMock()
        new_client.start_session.side_effect = Exception("AccessDeniedException: nope")

        with pytest.raises(Exception, match="AccessDeniedException"):
            _acquire(pool, new_client, key=("new-inv", "fp"), max_sessions=1)

        stale_client.terminate_session.assert_called_once_with(SessionId="sid-stale")
        assert "sid-stale" not in pool._snapshot()


class TestStaleInvocationDrain:
    def test_busy_stale_session_is_drained_not_terminated(self):
        pool = SparkConnectSessionPool()
        old_client = MagicMock()
        _register(pool, "sid-busy", ("old-inv", "fp"), old_client, load=1)

        sid = _acquire(pool, _make_client(["sid-new"]), key=("new-inv", "fp"))

        assert sid == "sid-new"
        old_client.terminate_session.assert_not_called()
        info = pool._snapshot()["sid-busy"]
        assert info["draining"] is True
        assert info["load"] == 1

    def test_idle_stale_session_is_terminated(self):
        pool = SparkConnectSessionPool()
        old_client = MagicMock()
        _register(pool, "sid-idle", ("old-inv", "fp"), old_client, load=0)

        _acquire(pool, _make_client(["sid-new"]), key=("new-inv", "fp"))

        old_client.terminate_session.assert_called_once_with(SessionId="sid-idle")
        assert "sid-idle" not in pool._snapshot()

    def test_drained_session_is_terminated_by_last_release(self):
        pool = SparkConnectSessionPool()
        old_client = MagicMock()
        _register(pool, "sid-busy", ("old-inv", "fp"), old_client, load=1)
        _acquire(pool, _make_client(["sid-new"]), key=("new-inv", "fp"))

        pool.release("sid-busy")

        old_client.terminate_session.assert_called_once_with(SessionId="sid-busy")
        assert "sid-busy" not in pool._snapshot()

    def test_drained_session_stays_until_every_caller_releases(self):
        pool = SparkConnectSessionPool()
        old_client = MagicMock()
        _register(pool, "sid-busy", ("old-inv", "fp"), old_client, load=2)
        _acquire(pool, _make_client(["sid-new"]), key=("new-inv", "fp"))

        pool.release("sid-busy")
        old_client.terminate_session.assert_not_called()
        pool.release("sid-busy")

        old_client.terminate_session.assert_called_once_with(SessionId="sid-busy")

    def test_drained_session_is_not_attached(self):
        pool = SparkConnectSessionPool()
        _register(pool, "sid-busy", ("old-inv", "fp"), MagicMock(), load=1)
        _acquire(pool, _make_client(["sid-new"]), key=("new-inv", "fp"))

        assert pool._attach(("old-inv", "fp"), session_concurrency=5) is None
        assert pool._snapshot()["sid-busy"]["load"] == 1

    def test_drained_session_still_counts_toward_dpu_budget(self):
        pool = SparkConnectSessionPool()
        old_client = MagicMock()
        _register(pool, "sid-busy", ("old-inv", "fp"), old_client, dpu=8, load=1)
        new_client = _make_client(["sid-new"])

        with pytest.raises(DbtRuntimeError, match="No Spark Connect session available"):
            _acquire(
                pool,
                new_client,
                key=("new-inv", "fp"),
                dpu_request=4,
                dpu_budget=10,
                timeout=0.05,
            )

        new_client.start_session.assert_not_called()
        assert pool._used_dpu() == 8


class TestDpuBudget:
    def test_starts_when_budget_allows(self):
        pool = SparkConnectSessionPool()
        client = _make_client(["sid-1"])

        sid = _acquire(pool, client, dpu_request=4, dpu_budget=10)

        assert sid == "sid-1"
        assert pool._snapshot()["sid-1"]["dpu"] == 4

    def test_starts_after_release_frees_budget(self):
        """Budget-blocked acquire must succeed once a registered session frees its DPUs."""
        pool = SparkConnectSessionPool()
        client = _make_client(["sid-existing", "sid-new"])

        # Saturate the budget with one existing session worth 8 DPU.
        first = _acquire(
            pool,
            client,
            key=("inv", "fp-a"),
            dpu_request=8,
            dpu_budget=10,
            max_sessions=2,
        )
        assert first == "sid-existing"

        results: list = []
        errors: list = []

        def worker():
            try:
                sid = _acquire(
                    pool,
                    client,
                    key=("inv", "fp-b"),
                    dpu_request=4,
                    dpu_budget=10,
                    max_sessions=2,
                    timeout=2,
                )
                results.append(sid)
            except Exception as e:  # pragma: no cover
                errors.append(e)

        t = threading.Thread(target=worker)
        t.start()
        # Wait briefly so the worker is in the polling loop, then free budget.
        time.sleep(0.1)
        pool.unregister(first)
        t.join(timeout=5)

        assert errors == []
        assert results == ["sid-new"]

    def test_times_out_when_budget_never_frees(self):
        pool = SparkConnectSessionPool()
        client = _make_client(["sid-1"])

        _acquire(
            pool,
            client,
            key=("inv", "fp-a"),
            dpu_request=8,
            dpu_budget=10,
            max_sessions=2,
        )

        with pytest.raises(DbtRuntimeError, match="No Spark Connect session available"):
            _acquire(
                pool,
                client,
                key=("inv", "fp-b"),
                dpu_request=4,
                dpu_budget=10,
                max_sessions=2,
                timeout=0.05,
            )

    def test_fail_fast_when_request_exceeds_budget(self):
        """Single session larger than the budget can never start; raise immediately."""
        pool = SparkConnectSessionPool()
        client = _make_client(["sid-1"])

        with pytest.raises(DbtRuntimeError, match="can never start"):
            _acquire(pool, client, dpu_request=20, dpu_budget=10)
        client.start_session.assert_not_called()

    def test_warns_when_request_equals_budget(self, monkeypatch):
        pool = SparkConnectSessionPool()
        client = _make_client(["sid-1"])
        warnings = _spy_logger_warnings(monkeypatch)

        _acquire(pool, client, dpu_request=10, dpu_budget=10)

        assert any("consumes the full DPU budget" in w for w in warnings)

    def test_drift_warning_when_aws_rejects_despite_local_budget_ok(self, monkeypatch):
        """When AWS rejects with the session limit even though client-side
        accounting said there was room, log a drift warning so the user can
        spot multi-process contention on the shared account quota."""
        pool = SparkConnectSessionPool()
        client = MagicMock()
        client.start_session.side_effect = [
            Exception("Maximum allowed sessions reached"),
            {"SessionId": "sid-ok", "State": "IDLE"},
        ]
        monkeypatch.setattr(time, "sleep", lambda *_: None)
        warnings = _spy_logger_warnings(monkeypatch)

        sid = _acquire(pool, client, dpu_request=4, dpu_budget=160, max_sessions=2)

        assert sid == "sid-ok"
        assert any("Another process may share" in w for w in warnings)


class TestReclaimIdleForBudget:
    @staticmethod
    def _no_waiting(monkeypatch):
        def fail(*_):
            raise AssertionError("acquire waited instead of reclaiming")

        monkeypatch.setattr(time, "sleep", fail)

    def test_reclaims_idle_session_of_other_key_and_starts_without_waiting(self, monkeypatch):
        self._no_waiting(monkeypatch)
        pool = SparkConnectSessionPool()
        owner = MagicMock()
        _register(pool, "sid-idle", ("inv", "fp-a"), owner, dpu=8, load=0)
        caller = _make_client(["sid-new"])

        sid = _acquire(pool, caller, key=("inv", "fp-b"), dpu_request=4, dpu_budget=10)

        assert sid == "sid-new"
        owner.terminate_session.assert_called_once_with(SessionId="sid-idle")
        caller.terminate_session.assert_not_called()
        snapshot = pool._snapshot()
        assert "sid-idle" not in snapshot
        assert snapshot["sid-new"]["load"] == 1

    def test_reclaimed_session_is_terminated_before_the_new_session_starts(self, monkeypatch):
        self._no_waiting(monkeypatch)
        pool = SparkConnectSessionPool()
        calls: list[str] = []
        owner = MagicMock()
        owner.terminate_session.side_effect = lambda **_: calls.append("terminate")
        _register(pool, "sid-idle", ("inv", "fp-a"), owner, dpu=8, load=0)
        caller = _make_client(["sid-new"])
        start = caller.start_session.side_effect
        caller.start_session.side_effect = lambda **kw: calls.append("start") or next(start)

        _acquire(pool, caller, key=("inv", "fp-b"), dpu_request=4, dpu_budget=10)

        assert calls == ["terminate", "start"]

    def test_logs_reclaimed_session_and_dpus(self, monkeypatch):
        pool = SparkConnectSessionPool()
        _register(pool, "sid-idle", ("inv", "fp-a"), MagicMock(), dpu=8, load=0)
        infos: list[str] = []
        monkeypatch.setattr(
            session_module.LOGGER, "info", lambda msg, *a, **k: infos.append(str(msg))
        )

        _acquire(
            pool, _make_client(["sid-new"]), key=("inv", "fp-b"), dpu_request=4, dpu_budget=10
        )

        assert len(infos) == 1
        assert "sid-idle" in infos[0]
        assert "8 DPUs" in infos[0]

    def test_reclaims_oldest_first_and_only_as_many_as_needed(self, monkeypatch):
        self._no_waiting(monkeypatch)
        pool = SparkConnectSessionPool()
        owner = MagicMock()
        _register(pool, "sid-1", ("inv", "fp-a"), owner, dpu=4, load=0)
        _register(pool, "sid-2", ("inv", "fp-b"), owner, dpu=4, load=0)
        _register(pool, "sid-3", ("inv", "fp-c"), owner, dpu=2, load=0)

        _acquire(
            pool, _make_client(["sid-new"]), key=("inv", "fp-d"), dpu_request=4, dpu_budget=10
        )

        owner.terminate_session.assert_called_once_with(SessionId="sid-1")
        assert set(pool._snapshot()) == {"sid-2", "sid-3", "sid-new"}

    def test_reclaims_several_when_one_is_not_enough(self, monkeypatch):
        self._no_waiting(monkeypatch)
        pool = SparkConnectSessionPool()
        owner = MagicMock()
        _register(pool, "sid-1", ("inv", "fp-a"), owner, dpu=3, load=0)
        _register(pool, "sid-2", ("inv", "fp-b"), owner, dpu=3, load=0)
        _register(pool, "sid-3", ("inv", "fp-c"), owner, dpu=4, load=0)

        _acquire(
            pool, _make_client(["sid-new"]), key=("inv", "fp-d"), dpu_request=6, dpu_budget=10
        )

        assert owner.terminate_session.call_count == 2
        assert set(pool._snapshot()) == {"sid-3", "sid-new"}

    def test_does_not_reclaim_sessions_that_are_in_use(self):
        pool = SparkConnectSessionPool()
        owner = MagicMock()
        _register(pool, "sid-busy", ("inv", "fp-a"), owner, dpu=8, load=1)
        caller = _make_client(["sid-new"])

        with pytest.raises(DbtRuntimeError, match="No Spark Connect session available"):
            _acquire(
                pool,
                caller,
                key=("inv", "fp-b"),
                dpu_request=4,
                dpu_budget=10,
                timeout=0.05,
            )

        owner.terminate_session.assert_not_called()
        caller.start_session.assert_not_called()
        assert "sid-busy" in pool._snapshot()

    def test_reclaims_nothing_when_idle_sessions_cannot_free_enough(self):
        pool = SparkConnectSessionPool()
        owner = MagicMock()
        _register(pool, "sid-idle", ("inv", "fp-a"), owner, dpu=2, load=0)
        _register(pool, "sid-busy", ("inv", "fp-b"), owner, dpu=8, load=1)

        with pytest.raises(DbtRuntimeError, match="No Spark Connect session available"):
            _acquire(
                pool,
                _make_client(["sid-new"]),
                key=("inv", "fp-c"),
                dpu_request=4,
                dpu_budget=10,
                timeout=0.05,
            )

        owner.terminate_session.assert_not_called()
        assert "sid-idle" in pool._snapshot()

    def test_reclaims_nothing_when_key_has_no_room(self):
        pool = SparkConnectSessionPool()
        owner = MagicMock()
        _register(pool, "sid-idle", ("inv", "fp-a"), owner, dpu=9, load=0)
        _register(pool, "sid-own", ("inv", "fp-b"), MagicMock(), dpu=1, load=1)

        with pytest.raises(DbtRuntimeError, match="No Spark Connect session available"):
            _acquire(
                pool,
                _make_client(["sid-new"]),
                key=("inv", "fp-b"),
                max_sessions=1,
                dpu_request=4,
                dpu_budget=10,
                timeout=0.05,
            )

        owner.terminate_session.assert_not_called()
        assert "sid-idle" in pool._snapshot()

    def test_does_not_reclaim_idle_session_of_same_key(self):
        pool = SparkConnectSessionPool()
        own = MagicMock()
        own.get_session_status.side_effect = Exception("boom")
        _register(pool, "sid-own", ("inv", "fp-a"), own, dpu=9, load=0)

        with pytest.raises(DbtRuntimeError, match="No Spark Connect session available"):
            _acquire(
                pool,
                _make_client(["sid-new"]),
                key=("inv", "fp-a"),
                max_sessions=3,
                dpu_request=4,
                dpu_budget=10,
                timeout=0.05,
            )

        own.terminate_session.assert_not_called()
        assert "sid-own" in pool._snapshot()

    def test_does_not_reclaim_draining_session(self):
        pool = SparkConnectSessionPool()
        owner = MagicMock()
        _register(pool, "sid-draining", ("inv", "fp-a"), owner, dpu=8, load=0)
        pool._sessions["sid-draining"]["draining"] = True

        with pytest.raises(DbtRuntimeError, match="No Spark Connect session available"):
            _acquire(
                pool,
                _make_client(["sid-new"]),
                key=("inv", "fp-b"),
                dpu_request=4,
                dpu_budget=10,
                timeout=0.05,
            )

        owner.terminate_session.assert_not_called()

    def test_prefers_reusing_idle_session_of_same_key(self):
        pool = SparkConnectSessionPool()
        other = MagicMock()
        own = MagicMock()
        own.get_session_status.return_value = {"Status": {"State": "IDLE"}}
        _register(pool, "sid-other", ("inv", "fp-a"), other, dpu=8, load=0)
        _register(pool, "sid-own", ("inv", "fp-b"), own, dpu=2, load=0)
        caller = _make_client(["sid-new"])

        sid = _acquire(pool, caller, key=("inv", "fp-b"), dpu_request=4, dpu_budget=10)

        assert sid == "sid-own"
        other.terminate_session.assert_not_called()
        caller.start_session.assert_not_called()
        assert "sid-other" in pool._snapshot()

    def test_does_not_reclaim_when_budget_already_fits(self):
        pool = SparkConnectSessionPool()
        owner = MagicMock()
        _register(pool, "sid-idle", ("inv", "fp-a"), owner, dpu=4, load=0)

        _acquire(
            pool, _make_client(["sid-new"]), key=("inv", "fp-b"), dpu_request=4, dpu_budget=10
        )

        owner.terminate_session.assert_not_called()
        assert "sid-idle" in pool._snapshot()

    def test_reclaimed_session_stops_its_spark_client(self):
        pool = SparkConnectSessionPool()
        _register(pool, "sid-idle", ("inv", "fp-a"), MagicMock(), dpu=8, load=0)
        spark = MagicMock()
        pool.set_spark("sid-idle", spark)

        _acquire(
            pool, _make_client(["sid-new"]), key=("inv", "fp-b"), dpu_request=4, dpu_budget=10
        )

        spark.stop.assert_called_once()


class TestAccountCapacityUnavailable:
    def test_retries_on_required_capacity_not_available(self, monkeypatch):
        pool = SparkConnectSessionPool()
        client = MagicMock()
        client.start_session.side_effect = [
            Exception(
                "InvalidRequestException: Failed to Provision session due to "
                "required capacity not being available for the account"
            ),
            {"SessionId": "sid-ok", "State": "IDLE"},
        ]
        monkeypatch.setattr(time, "sleep", lambda *_: None)

        sid = _acquire(pool, client, max_sessions=1)

        assert sid == "sid-ok"
        assert client.start_session.call_count == 2

    def test_capacity_unavailable_emits_distinct_warning(self, monkeypatch):
        pool = SparkConnectSessionPool()
        client = MagicMock()
        client.start_session.side_effect = [
            Exception("required capacity not being available for the account"),
            {"SessionId": "sid-ok", "State": "IDLE"},
        ]
        monkeypatch.setattr(time, "sleep", lambda *_: None)
        warnings = _spy_logger_warnings(monkeypatch)

        _acquire(pool, client, max_sessions=1)

        assert any("region capacity unavailable" in w for w in warnings)


class TestStartSessionThrottling:
    def test_retries_on_throttling_exception(self, monkeypatch):
        pool = SparkConnectSessionPool()
        client = MagicMock()
        client.start_session.side_effect = [
            Exception("ThrottlingException: Rate exceeded"),
            {"SessionId": "sid-ok", "State": "IDLE"},
        ]
        monkeypatch.setattr(time, "sleep", lambda *_: None)

        sid = _acquire(pool, client, max_sessions=1)

        assert sid == "sid-ok"
        assert client.start_session.call_count == 2

    def test_retries_on_bare_rate_exceeded_message(self, monkeypatch):
        pool = SparkConnectSessionPool()
        client = MagicMock()
        client.start_session.side_effect = [
            Exception("An error occurred (ThrottlingException): Rate exceeded"),
            {"SessionId": "sid-ok", "State": "IDLE"},
        ]
        monkeypatch.setattr(time, "sleep", lambda *_: None)

        sid = _acquire(pool, client, max_sessions=1)

        assert sid == "sid-ok"
        assert client.start_session.call_count == 2

    def test_throttling_emits_backoff_warning(self, monkeypatch):
        pool = SparkConnectSessionPool()
        client = MagicMock()
        client.start_session.side_effect = [
            Exception("ThrottlingException: Rate exceeded"),
            {"SessionId": "sid-ok", "State": "IDLE"},
        ]
        monkeypatch.setattr(time, "sleep", lambda *_: None)
        warnings = _spy_logger_warnings(monkeypatch)

        _acquire(pool, client, max_sessions=1)

        assert any("throttled StartSession" in w for w in warnings)

    def test_backoff_grows_and_caps(self, monkeypatch):
        """Ceiling doubles per consecutive pushback until _PUSHBACK_MAX."""
        pool = SparkConnectSessionPool()
        monkeypatch.setattr(session_module.random, "uniform", lambda _lo, hi: hi)

        assert pool._pushback_backoff(1) == pytest.approx(1.0)
        assert pool._pushback_backoff(2) == pytest.approx(2.0)
        assert pool._pushback_backoff(3) == pytest.approx(4.0)
        assert pool._pushback_backoff(99) == pytest.approx(pool._PUSHBACK_MAX_BACKOFF_SECONDS)

    def test_backoff_stays_within_ceiling(self):
        pool = SparkConnectSessionPool()
        for attempts in range(1, 8):
            value = pool._pushback_backoff(attempts)
            assert 0.0 <= value <= pool._PUSHBACK_MAX_BACKOFF_SECONDS


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

    def test_state_is_probed_with_the_sessions_own_client_not_the_callers(self):
        pool = SparkConnectSessionPool()
        owner = MagicMock()
        owner.get_session_status.return_value = {"Status": {"State": "IDLE"}}
        _register(pool, "sid-1", ("inv", "fp"), owner)
        pool.release("sid-1")
        caller = MagicMock()
        caller.get_session_status.return_value = {"Status": {"State": "TERMINATED"}}

        sid = _acquire(pool, caller)

        assert sid == "sid-1"
        owner.get_session_status.assert_called_once_with(SessionId="sid-1")
        caller.get_session_status.assert_not_called()
        caller.start_session.assert_not_called()

    def test_unknown_state_session_is_skipped_but_kept_in_pool(self):
        pool = SparkConnectSessionPool()
        owner = MagicMock()
        probes = {"n": 0}

        def failing_status(SessionId):
            probes["n"] += 1
            if probes["n"] > 3:
                raise _Livelock()
            raise Exception("boom")

        owner.get_session_status.side_effect = failing_status
        _register(pool, "sid-unknown", ("inv", "fp"), owner)
        pool.release("sid-unknown")
        caller = _make_client(["sid-new"])

        sid = _acquire(pool, caller, max_sessions=3)

        assert sid == "sid-new"
        owner.get_session_status.assert_called_once_with(SessionId="sid-unknown")
        owner.terminate_session.assert_not_called()
        snapshot = pool._snapshot()
        assert snapshot["sid-unknown"]["load"] == 0
        assert snapshot["sid-new"]["load"] == 1

    def test_unknown_state_session_is_judged_again_by_a_later_eviction(self):
        pool = SparkConnectSessionPool()
        owner = MagicMock()
        owner.get_session_status.side_effect = Exception("boom")
        _register(pool, "sid-unknown", ("inv", "fp"), owner)
        pool.release("sid-unknown")
        _acquire(pool, _make_client(["sid-new"]), max_sessions=3)
        assert "sid-unknown" in pool._snapshot()

        owner.get_session_status.side_effect = None
        owner.get_session_status.return_value = {"Status": {"State": "TERMINATED"}}

        assert pool._evict_dead_sessions() == 1
        assert "sid-unknown" not in pool._snapshot()

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


class TestSessionState:
    def test_is_session_alive_uses_the_sessions_own_client(self):
        pool = SparkConnectSessionPool()
        owner = MagicMock()
        owner.get_session_status.return_value = {"Status": {"State": "IDLE"}}
        _register(pool, "sid-1", ("inv", "fp"), owner)

        assert pool.is_session_alive("sid-1") is True
        owner.get_session_status.assert_called_once_with(SessionId="sid-1")

    @pytest.mark.parametrize("state", ["FAILED", "TERMINATED", "TERMINATING", "DEGRADED"])
    def test_is_session_alive_false_for_dead_states(self, state):
        pool = SparkConnectSessionPool()
        owner = MagicMock()
        owner.get_session_status.return_value = {"Status": {"State": state}}
        _register(pool, "sid-1", ("inv", "fp"), owner)

        assert pool.is_session_alive("sid-1") is False

    def test_is_session_alive_false_when_state_unknown(self):
        pool = SparkConnectSessionPool()
        owner = MagicMock()
        owner.get_session_status.side_effect = Exception("boom")
        _register(pool, "sid-1", ("inv", "fp"), owner)

        assert pool.is_session_alive("sid-1") is False

    @pytest.mark.parametrize("status", [{}, {"State": ""}])
    def test_is_session_alive_false_for_empty_state(self, status):
        pool = SparkConnectSessionPool()
        owner = MagicMock()
        owner.get_session_status.return_value = {"Status": status}
        _register(pool, "sid-1", ("inv", "fp"), owner)

        assert pool.is_session_alive("sid-1") is False

    def test_is_session_alive_false_for_unregistered_session(self):
        assert SparkConnectSessionPool().is_session_alive("sid-gone") is False


class TestSparkClientBinding:
    """One Spark Connect client per Athena session, owned by the pool.

    On pyspark 3.5 ``SparkSession.stop()`` does not release the server-side
    Spark Connect session, so the pool binds a single client to each Athena
    session and stops it only when that session leaves the pool.
    """

    def test_get_spark_is_none_for_unknown_or_unbound_session(self):
        pool = SparkConnectSessionPool()
        _register(pool, "sid-1", ("inv", "fp"), MagicMock())

        assert pool.get_spark("sid-missing") is None
        assert pool.get_spark("sid-1") is None

    def test_set_spark_binds_first_client_and_returns_it(self):
        pool = SparkConnectSessionPool()
        _register(pool, "sid-1", ("inv", "fp"), MagicMock())
        spark = MagicMock()

        assert pool.set_spark("sid-1", spark) is spark
        assert pool.get_spark("sid-1") is spark

    def test_set_spark_keeps_existing_client_when_already_bound(self):
        pool = SparkConnectSessionPool()
        _register(pool, "sid-1", ("inv", "fp"), MagicMock())
        first = MagicMock()
        second = MagicMock()
        pool.set_spark("sid-1", first)

        assert pool.set_spark("sid-1", second) is first
        assert pool.get_spark("sid-1") is first
        # The pool never stops the loser; that is the caller's job.
        second.stop.assert_not_called()

    def test_set_spark_on_unregistered_session_returns_client_unbound(self):
        pool = SparkConnectSessionPool()
        spark = MagicMock()

        assert pool.set_spark("sid-gone", spark) is spark
        assert pool.get_spark("sid-gone") is None

    def test_client_survives_release_and_is_reused_on_next_acquire(self):
        pool = SparkConnectSessionPool()
        client = _make_client(["sid-1"])
        spark = MagicMock()

        first = _acquire(pool, client)
        pool.set_spark(first, spark)
        pool.release(first)
        second = _acquire(pool, client)

        assert second == first
        assert pool.get_spark(second) is spark
        spark.stop.assert_not_called()

    def test_terminate_stops_client_before_terminating_session(self):
        pool = SparkConnectSessionPool()
        client = MagicMock()
        _register(pool, "sid-1", ("inv", "fp"), client)
        spark = MagicMock()
        pool.set_spark("sid-1", spark)
        order: list[str] = []
        spark.stop.side_effect = lambda: order.append("stop")
        client.terminate_session.side_effect = lambda **_: order.append("terminate")

        pool.terminate("sid-1")

        assert order == ["stop", "terminate"]
        assert "sid-1" not in pool._snapshot()

    def test_terminate_of_shared_session_keeps_client_until_drained(self):
        pool = SparkConnectSessionPool()
        client = MagicMock()
        _register(pool, "sid-1", ("inv", "fp"), client, load=2)
        spark = MagicMock()
        pool.set_spark("sid-1", spark)

        pool.terminate("sid-1")  # co-tenant still attached -> drains
        spark.stop.assert_not_called()
        assert pool.get_spark("sid-1") is spark

        pool.release("sid-1")  # last caller leaves
        spark.stop.assert_called_once()
        client.terminate_session.assert_called_once_with(SessionId="sid-1")

    def test_unregister_stops_client(self):
        pool = SparkConnectSessionPool()
        client = MagicMock()
        _register(pool, "sid-1", ("inv", "fp"), client)
        spark = MagicMock()
        pool.set_spark("sid-1", spark)

        pool.unregister("sid-1")

        spark.stop.assert_called_once()
        client.terminate_session.assert_not_called()

    def test_evict_dead_sessions_stops_client(self):
        pool = SparkConnectSessionPool()
        client = MagicMock()
        client.get_session_status.return_value = {"Status": {"State": "TERMINATED"}}
        _register(pool, "sid-dead", ("inv", "fp"), client)
        spark = MagicMock()
        pool.set_spark("sid-dead", spark)

        assert pool._evict_dead_sessions() == 1
        spark.stop.assert_called_once()

    def test_stale_invocation_cleanup_stops_client(self):
        pool = SparkConnectSessionPool()
        old_client = MagicMock()
        _register(pool, "sid-stale", ("old-inv", "fp"), old_client, load=0)
        spark = MagicMock()
        pool.set_spark("sid-stale", spark)

        _acquire(pool, _make_client(["sid-new"]), key=("new-inv", "fp"))

        spark.stop.assert_called_once()
        old_client.terminate_session.assert_called_once_with(SessionId="sid-stale")

    def test_client_stop_errors_are_ignored(self):
        pool = SparkConnectSessionPool()
        client = MagicMock()
        _register(pool, "sid-1", ("inv", "fp"), client)
        spark = MagicMock()
        spark.stop.side_effect = RuntimeError("channel already closed")
        pool.set_spark("sid-1", spark)

        pool.terminate("sid-1")  # Must not raise.

        client.terminate_session.assert_called_once_with(SessionId="sid-1")
        assert "sid-1" not in pool._snapshot()
