"""Thread-safe pool for Athena Spark Connect sessions, keyed by ``(invocation_id, fingerprint)``."""

from __future__ import annotations

import threading
import time
from typing import Dict, List, Optional, Tuple, TypedDict

from dbt_common.exceptions import DbtRuntimeError
from mypy_boto3_athena.client import AthenaClient
from mypy_boto3_athena.type_defs import EngineConfigurationTypeDef

from dbt.adapters.athena.constants import LOGGER, SESSION_IDLE_TIMEOUT_MIN

SessionKey = Tuple[str, str]


class _SessionInfo(TypedDict):
    key: SessionKey
    client: AthenaClient
    load: int
    dpu: int


class _GlobalSessionLimitReached(Exception):
    """Raised when Athena returns ``Maximum allowed sessions reached``."""


class _AccountCapacityUnavailable(Exception):
    """Raised when Athena returns ``required capacity not being available``.

    Distinct from the account session-limit signal: this is a transient
    region-level capacity shortage that the DPU budget cannot predict.
    """


class SparkConnectSessionPool:
    """Singleton pool of Athena Spark Connect sessions.

    Singleton so dbt Cloud's long-lived process can share sessions across
    invocations; ``(invocation_id, fingerprint)`` keying keeps each
    invocation logically isolated within the shared registry.
    """

    _instance: Optional["SparkConnectSessionPool"] = None
    _singleton_lock = threading.Lock()

    _DEAD_SESSION_STATES = frozenset({"FAILED", "TERMINATED", "TERMINATING", "DEGRADED"})
    _EVICTION_INTERVAL = 30.0
    _GLOBAL_LIMIT_BACKOFF_SECONDS = 5.0

    def __new__(cls) -> "SparkConnectSessionPool":
        # Avoid double-checked locking: another thread could see
        # ``cls._instance`` set before ``_initialize`` finishes.
        with cls._singleton_lock:
            if cls._instance is None:
                instance = super().__new__(cls)
                instance._initialize()
                cls._instance = instance
        return cls._instance

    def _initialize(self) -> None:
        self._lock = threading.Lock()
        self._sessions: Dict[str, _SessionInfo] = {}

    def acquire(
        self,
        key: SessionKey,
        athena_client: AthenaClient,
        spark_work_group: str,
        engine_config: EngineConfigurationTypeDef,
        session_description: str,
        max_sessions: int,
        timeout: float,
        polling_interval: float,
        session_concurrency: int,
        dpu_request: int,
        dpu_budget: int,
    ) -> str:
        """Acquire a session for ``key``, reusing or starting one.

        Reuses when load < ``session_concurrency``; starts new when
        per-key count < ``max_sessions`` AND
        ``used_dpu + dpu_request <= dpu_budget``; waits up to ``timeout``.

        Raises immediately when ``dpu_request > dpu_budget`` — no future
        release could ever satisfy the request, so waiting would deadlock.
        """
        if dpu_request > dpu_budget:
            raise DbtRuntimeError(
                f"Spark Connect session for key {key} requests {dpu_request} DPUs but "
                f"spark_connect_dpu_budget is {dpu_budget}; the session can never start. "
                f"Raise spark_connect_dpu_budget, or lower MaxConcurrentDpus / "
                f"spark.dynamicAllocation.maxExecutors for the model."
            )
        if dpu_request == dpu_budget:
            LOGGER.warning(
                f"Spark Connect session for key {key} consumes the full DPU budget "
                f"({dpu_request}/{dpu_budget}); other sessions will block until it releases."
            )

        invocation_id = key[0]
        deadline = time.monotonic() + timeout
        time_since_eviction = self._EVICTION_INTERVAL  # evict on first pass

        while True:
            new_session_id: Optional[str] = None
            global_limit_hit = False
            capacity_unavailable = False
            start_error: Optional[BaseException] = None
            budget_used = 0
            budget_ok = False
            with self._lock:
                stale_entries = self._collect_stale_invocations(invocation_id)
                reuse_candidate = self._attach(key, session_concurrency)
                if reuse_candidate is None:
                    budget_used = self._used_dpu()
                    budget_ok = budget_used + dpu_request <= dpu_budget
                    if budget_ok and self._has_room(key, max_sessions):
                        try:
                            new_session_id = self._start(
                                key,
                                athena_client,
                                spark_work_group,
                                engine_config,
                                session_description,
                                dpu_request,
                            )
                        except _GlobalSessionLimitReached:
                            global_limit_hit = True
                        except _AccountCapacityUnavailable:
                            capacity_unavailable = True
                        except Exception as e:  # noqa: BLE001 - re-raised after cleanup
                            start_error = e

            # Stale cleanup runs even on start_session failure to avoid
            # leaking prior sessions.
            if stale_entries:
                self._terminate_entries(stale_entries)
            if start_error is not None:
                raise start_error

            if global_limit_hit:
                LOGGER.warning(
                    f"Athena rejected StartSession (account session limit) for key {key}; "
                    f"client-side accounting saw used={budget_used} + request={dpu_request} "
                    f"<= budget={dpu_budget}. Another process may share the account quota."
                )
            if capacity_unavailable:
                LOGGER.warning(
                    f"Athena rejected StartSession for key {key}: AWS region capacity "
                    f"unavailable. Backing off; this is transient and budget cannot predict it."
                )

            if reuse_candidate is not None:
                # Athena may have killed the session while it sat in the pool.
                if self.is_session_alive(athena_client, reuse_candidate):
                    LOGGER.debug(f"Reusing Spark Connect session {reuse_candidate} for key {key}")
                    return reuse_candidate
                LOGGER.debug(
                    f"Discarding stale Spark Connect session {reuse_candidate} during reuse"
                )
                self.unregister(reuse_candidate)
                continue

            if new_session_id is not None:
                return new_session_id

            # Periodically evict dead sessions so stuck slots don't block.
            if time_since_eviction >= self._EVICTION_INTERVAL:
                evicted = self._evict_dead_sessions(athena_client)
                time_since_eviction = 0
                if evicted:
                    continue

            if time.monotonic() >= deadline:
                raise DbtRuntimeError(
                    f"No Spark Connect session available for key {key} within {timeout}s "
                    f"(max_sessions={max_sessions}, dpu_request={dpu_request}, "
                    f"dpu_budget={dpu_budget}, last used_dpu={budget_used})"
                )

            # Longer wait when AWS pushed back (limit or capacity) so we don't
            # hammer StartSession during a region-wide event.
            sleep_for = (
                self._GLOBAL_LIMIT_BACKOFF_SECONDS
                if (global_limit_hit or capacity_unavailable)
                else polling_interval
            )
            time.sleep(sleep_for)
            time_since_eviction += sleep_for

    def _collect_stale_invocations(self, invocation_id: str) -> List[Tuple[str, _SessionInfo]]:
        """Pop sessions from prior invocations. Caller must hold ``self._lock``.

        Prevents cruft across dbt runs in long-lived processes (e.g. dbt Cloud).
        """
        stale_sids = [
            sid for sid, info in self._sessions.items() if info["key"][0] != invocation_id
        ]
        if not stale_sids:
            return []
        LOGGER.debug(
            f"Removing {len(stale_sids)} stale Spark Connect sessions from prior invocations"
        )
        return [(sid, self._sessions.pop(sid)) for sid in stale_sids]

    def _attach(self, key: SessionKey, session_concurrency: int) -> Optional[str]:
        """Attach to a reusable session by incrementing its load.

        Caller must hold ``self._lock``. Increments load before the
        out-of-lock liveness check to prevent oversubscription.
        """
        for sid, info in self._sessions.items():
            if info["key"] == key and info["load"] < session_concurrency:
                info["load"] += 1
                return sid
        return None

    def _has_room(self, key: SessionKey, max_sessions: int) -> bool:
        """Return True if per-key count < ``max_sessions``. Caller must hold ``self._lock``."""
        count = sum(1 for info in self._sessions.values() if info["key"] == key)
        return count < max_sessions

    def _used_dpu(self) -> int:
        """Sum of DPUs reserved by registered sessions. Caller must hold ``self._lock``."""
        return sum(info["dpu"] for info in self._sessions.values())

    def _start(
        self,
        key: SessionKey,
        athena_client: AthenaClient,
        spark_work_group: str,
        engine_config: EngineConfigurationTypeDef,
        session_description: str,
        dpu: int,
    ) -> str:
        """Start a session and register it. Caller must hold ``self._lock``.

        Translates two transient AWS rejections into typed exceptions for the
        caller's backoff loop: account session limit and region capacity
        unavailable. Other errors propagate.
        """
        try:
            response = athena_client.start_session(
                Description=session_description,
                WorkGroup=spark_work_group,
                EngineConfiguration=engine_config,
                SessionIdleTimeoutInMinutes=SESSION_IDLE_TIMEOUT_MIN,
            )
        except Exception as e:  # noqa: BLE001 - transient errors handled below
            message = str(e)
            if "Maximum allowed sessions" in message:
                raise _GlobalSessionLimitReached() from e
            if "required capacity not being available" in message:
                raise _AccountCapacityUnavailable() from e
            raise

        session_id = str(response["SessionId"])
        self._sessions[session_id] = {
            "key": key,
            "client": athena_client,
            "load": 1,
            "dpu": dpu,
        }
        return session_id

    def _get_session_state(self, athena_client: AthenaClient, session_id: str) -> str:
        """Return the Athena session state, or empty string on lookup failure."""
        try:
            return athena_client.get_session_status(SessionId=session_id)["Status"].get(
                "State", ""
            )
        except Exception as e:  # noqa: BLE001 - treat unknown state as dead
            LOGGER.warning(f"Could not verify Spark Connect session {session_id} state: {e}")
            return ""

    def is_session_alive(self, athena_client: AthenaClient, session_id: str) -> bool:
        """Return True if Athena reports the session as IDLE/BUSY/CREATED."""
        state = self._get_session_state(athena_client, session_id)
        return bool(state) and state not in self._DEAD_SESSION_STATES

    def release(self, session_id: str) -> None:
        """Mark the session as idle so it can be reused."""
        with self._lock:
            info = self._sessions.get(session_id)
            if info is not None:
                info["load"] = max(info["load"] - 1, 0)

    def unregister(self, session_id: str) -> None:
        """Drop a session from the pool without terminating it on Athena."""
        with self._lock:
            self._sessions.pop(session_id, None)

    def terminate(self, session_id: str) -> None:
        """Terminate the Athena session and drop it from the pool."""
        with self._lock:
            info = self._sessions.pop(session_id, None)
        if info is not None:
            self._terminate_entries([(session_id, info)])

    def terminate_by_invocation(self, invocation_id: str) -> None:
        """Terminate only sessions for the given dbt invocation.

        Safe in multi-invocation processes (dbt Cloud, test harnesses)
        where other invocations may share the singleton.
        """
        with self._lock:
            entries = [
                (sid, info)
                for sid, info in self._sessions.items()
                if info["key"][0] == invocation_id
            ]
            for sid, _ in entries:
                self._sessions.pop(sid, None)
        self._terminate_entries(entries)

    def _terminate_entries(self, entries: List[Tuple[str, _SessionInfo]]) -> None:
        for session_id, info in entries:
            try:
                info["client"].terminate_session(SessionId=session_id)
                LOGGER.debug(f"Terminated Spark Connect session {session_id}")
            except Exception as e:  # noqa: BLE001 - best-effort cleanup
                LOGGER.warning(f"Failed to terminate Spark Connect session {session_id}: {e}")

    def _evict_dead_sessions(self, athena_client: AthenaClient) -> int:
        """Remove sessions that Athena has already terminated or degraded."""
        with self._lock:
            session_ids = list(self._sessions)

        evicted = 0
        for session_id in session_ids:
            state = self._get_session_state(athena_client, session_id)
            if not state or state in self._DEAD_SESSION_STATES:
                with self._lock:
                    if session_id in self._sessions:
                        LOGGER.debug(
                            f"Evicting dead Spark Connect session {session_id} (state={state})"
                        )
                        self._sessions.pop(session_id, None)
                        evicted += 1
        return evicted

    # -- test helpers -----------------------------------------------------

    def _snapshot(self) -> Dict[str, _SessionInfo]:
        with self._lock:
            return {sid: _SessionInfo(**info) for sid, info in self._sessions.items()}

    @classmethod
    def _reset_for_tests(cls) -> None:
        """Reset the singleton instance.  Test-only utility."""
        with cls._singleton_lock:
            cls._instance = None
