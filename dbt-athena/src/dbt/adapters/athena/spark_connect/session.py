"""Thread-safe pool for Athena Spark Connect sessions, keyed by ``(invocation_id, fingerprint)``."""

from __future__ import annotations

import threading
import time
from typing import Any, Dict, List, Optional, Tuple, TypedDict

from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.athena.constants import LOGGER, SESSION_IDLE_TIMEOUT_MIN

SessionKey = Tuple[str, str]

_PLACEHOLDER_PREFIX = "__creating_"


class _SessionInfo(TypedDict):
    key: SessionKey
    client: Any
    load: int


def _is_placeholder(session_id: str) -> bool:
    return session_id.startswith(_PLACEHOLDER_PREFIX)


class SparkConnectSessionPool:
    """Singleton pool of Athena Spark Connect sessions."""

    _instance: Optional["SparkConnectSessionPool"] = None
    _singleton_lock = threading.Lock()

    _DEAD_SESSION_STATES = frozenset({"FAILED", "TERMINATED", "TERMINATING", "DEGRADED"})
    _EVICTION_INTERVAL = 30.0
    _MAX_SESSION_RETRIES = 5
    _SESSION_RETRY_BASE_SECONDS = 5

    def __new__(cls) -> "SparkConnectSessionPool":
        # Single check under the lock.  The "double-checked locking with a
        # fast-path read" pattern is unsafe here because assigning
        # ``cls._instance`` before ``_initialize`` finishes would let another
        # thread return a half-built instance whose attributes (``_lock``,
        # ``_sessions``) are not yet set.
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
        athena_client: Any,
        spark_work_group: str,
        engine_config: Dict[str, Any],
        session_description: str,
        max_sessions: int,
        timeout: float,
        polling_interval: float,
        session_concurrency: int,
    ) -> str:
        """Acquire a session matching ``key``.

        Reuses a session with the same key whose in-flight model count is
        below ``session_concurrency``.  Otherwise starts a new session if
        the per-key session count is below ``max_sessions``.  Waits up to
        ``timeout`` seconds for a slot to open.
        """
        invocation_id = key[0]
        deadline = time.monotonic() + timeout
        time_since_eviction = self._EVICTION_INTERVAL  # evict on first pass

        while True:
            with self._lock:
                stale_entries = self._collect_stale_invocations(invocation_id)
                reuse_candidate = self._reserve_reuse_slot(key, session_concurrency)
                placeholder_id = (
                    self._reserve_placeholder(key, athena_client, max_sessions)
                    if reuse_candidate is None
                    else None
                )

            # Slow Athena API calls happen outside the lock.
            if stale_entries:
                self._terminate_entries(stale_entries)

            if reuse_candidate is not None:
                # Verify the reuse candidate is actually alive before handing
                # it back; Athena may have terminated it for idle timeout or
                # other reasons while it sat in the pool.
                if self._is_session_alive(athena_client, reuse_candidate):
                    LOGGER.debug(f"Reusing Spark Connect session {reuse_candidate} for key {key}")
                    return reuse_candidate
                LOGGER.debug(
                    f"Discarding stale Spark Connect session {reuse_candidate} during reuse"
                )
                self.remove(reuse_candidate)
                continue

            if placeholder_id is not None:
                try:
                    session_id = self._start_session(
                        athena_client, spark_work_group, engine_config, session_description
                    )
                except Exception:
                    with self._lock:
                        self._sessions.pop(placeholder_id, None)
                    raise
                with self._lock:
                    self._sessions.pop(placeholder_id, None)
                    self._sessions[session_id] = {
                        "key": key,
                        "client": athena_client,
                        "load": 1,
                    }
                return session_id

            # Pool is full for this key.  Periodically evict dead sessions
            # so stuck slots don't block indefinitely.
            if time_since_eviction >= self._EVICTION_INTERVAL:
                evicted = self._evict_dead_sessions(athena_client)
                time_since_eviction = 0
                if evicted:
                    continue

            if time.monotonic() >= deadline:
                raise DbtRuntimeError(
                    f"No Spark Connect session available for key {key} within {timeout}s "
                    f"(max_sessions={max_sessions})"
                )

            time.sleep(polling_interval)
            time_since_eviction += polling_interval

    def _collect_stale_invocations(self, invocation_id: str) -> List[Tuple[str, _SessionInfo]]:
        """Pop sessions from prior invocations.  Caller must hold ``self._lock``.

        Prevents the singleton from carrying cruft across dbt runs in
        long-lived processes (e.g. dbt Cloud workers).
        """
        stale_sids = [
            sid
            for sid, info in self._sessions.items()
            if not _is_placeholder(sid) and info["key"][0] != invocation_id
        ]
        if not stale_sids:
            return []
        LOGGER.debug(
            f"Removing {len(stale_sids)} stale Spark Connect sessions from prior invocations"
        )
        return [(sid, self._sessions.pop(sid)) for sid in stale_sids]

    def _reserve_reuse_slot(self, key: SessionKey, session_concurrency: int) -> Optional[str]:
        """Find a reusable session and increment its load.  Caller must hold ``self._lock``.

        The optimistic load increment reserves the slot before the
        out-of-lock liveness check so concurrent acquires can't
        oversubscribe ``session_concurrency``.
        """
        for sid, info in self._sessions.items():
            if _is_placeholder(sid):
                continue
            if info["key"] == key and info["load"] < session_concurrency:
                info["load"] += 1
                return sid
        return None

    def _reserve_placeholder(
        self, key: SessionKey, athena_client: Any, max_sessions: int
    ) -> Optional[str]:
        """Insert a placeholder if under the per-key limit.  Caller must hold ``self._lock``.

        Concurrent acquires see the in-flight session via the placeholder
        and won't oversubscribe ``max_sessions`` while ``start_session``
        runs outside the lock.
        """
        count = sum(1 for info in self._sessions.values() if info["key"] == key)
        if count >= max_sessions:
            return None
        placeholder_id = f"{_PLACEHOLDER_PREFIX}{threading.get_ident()}_{time.monotonic_ns()}__"
        self._sessions[placeholder_id] = {
            "key": key,
            "client": athena_client,
            "load": 1,
        }
        return placeholder_id

    def _get_session_state(self, athena_client: Any, session_id: str) -> str:
        """Return the Athena session state, or empty string on lookup failure."""
        try:
            return athena_client.get_session_status(SessionId=session_id)["Status"].get(
                "State", ""
            )
        except Exception as e:  # noqa: BLE001 - treat unknown state as dead
            LOGGER.warning(f"Could not verify Spark Connect session {session_id} state: {e}")
            return ""

    def _is_session_alive(self, athena_client: Any, session_id: str) -> bool:
        """Return True if Athena reports the session as IDLE/BUSY/CREATED."""
        state = self._get_session_state(athena_client, session_id)
        return bool(state) and state not in self._DEAD_SESSION_STATES

    def _start_session(
        self,
        athena_client: Any,
        spark_work_group: str,
        engine_config: Dict[str, Any],
        session_description: str,
    ) -> str:
        """Start a new Athena Spark session with retries on transient errors."""
        last_error: Optional[Exception] = None
        for attempt in range(1, self._MAX_SESSION_RETRIES + 1):
            try:
                response = athena_client.start_session(
                    Description=session_description,
                    WorkGroup=spark_work_group,
                    EngineConfiguration=engine_config,
                    SessionIdleTimeoutInMinutes=SESSION_IDLE_TIMEOUT_MIN,
                )
                return str(response["SessionId"])
            except Exception as e:  # noqa: BLE001 - retried below
                last_error = e
                if "Maximum allowed sessions" in str(e) and attempt < self._MAX_SESSION_RETRIES:
                    backoff = self._SESSION_RETRY_BASE_SECONDS * attempt
                    LOGGER.warning(
                        f"Athena session limit reached, retrying in {backoff}s "
                        f"(attempt {attempt}/{self._MAX_SESSION_RETRIES}): {e}"
                    )
                    time.sleep(backoff)
                    continue
                raise
        raise DbtRuntimeError(
            f"Failed to start Spark Connect session after {self._MAX_SESSION_RETRIES} attempts: "
            f"{last_error}"
        )

    def release(self, session_id: str) -> None:
        """Mark the session as idle so it can be reused."""
        with self._lock:
            info = self._sessions.get(session_id)
            if info is not None:
                info["load"] = max(info["load"] - 1, 0)

    def remove(self, session_id: str) -> None:
        """Remove a session from the pool without terminating it on Athena."""
        with self._lock:
            self._sessions.pop(session_id, None)

    def terminate_and_remove(self, session_id: str) -> None:
        """Terminate the Athena session and remove it from the pool."""
        with self._lock:
            info = self._sessions.pop(session_id, None)
        if info is not None:
            self._terminate_entries([(session_id, info)])

    def terminate_by_invocation(self, invocation_id: str) -> None:
        """Terminate only sessions owned by the given dbt invocation.

        Safe to call from adapter cleanup in multi-invocation processes
        (e.g. dbt Cloud workers, test harnesses) where other invocations
        may still be using the singleton.
        """
        with self._lock:
            entries = [
                (sid, info)
                for sid, info in self._sessions.items()
                if not _is_placeholder(sid) and info["key"][0] == invocation_id
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

    def _evict_dead_sessions(self, athena_client: Any) -> int:
        """Remove sessions that Athena has already terminated or degraded."""
        with self._lock:
            session_ids = [sid for sid in self._sessions if not _is_placeholder(sid)]

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

    def _snapshot(self) -> Dict[str, Dict[str, Any]]:
        with self._lock:
            return {sid: dict(info) for sid, info in self._sessions.items()}

    @classmethod
    def _reset_for_tests(cls) -> None:
        """Reset the singleton instance.  Test-only utility."""
        with cls._singleton_lock:
            cls._instance = None
