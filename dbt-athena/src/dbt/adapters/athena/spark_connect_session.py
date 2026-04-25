"""Thread-safe singleton pool for Athena Spark Connect sessions.

The pool is keyed by ``(invocation_id, fingerprint)`` where ``fingerprint``
is an md5 of the engine configuration.  Sessions with the same fingerprint
can be reused across models within a single dbt invocation, so long as
the per-fingerprint limit is not exceeded.

Unlike the Calculations API, Spark Connect uses a persistent gRPC channel
bound to a single session, so a dedicated pool (separate from
``AthenaSparkSessionManager``) is required to coordinate session lifecycle
across concurrent dbt threads.
"""

from __future__ import annotations

import threading
import time
from typing import Any, Dict, List, Optional, Tuple

from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.athena.constants import LOGGER, SESSION_IDLE_TIMEOUT_MIN

SessionKey = Tuple[str, str]

_PLACEHOLDER_PREFIX = "__creating_"


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
        # session_id -> {"key": SessionKey, "client": athena_client, "load": int}
        self._sessions: Dict[str, Dict[str, Any]] = {}

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
        placeholder_id: Optional[str] = None
        stale_to_terminate: List[Tuple[str, Any]] = []

        while True:
            reuse_candidate: Optional[str] = None
            with self._lock:
                # Purge sessions from prior invocations so the singleton
                # doesn't carry cruft across dbt runs in long-lived processes.
                stale_sids = [
                    sid
                    for sid, info in self._sessions.items()
                    if not _is_placeholder(sid) and info["key"][0] != invocation_id
                ]
                if stale_sids:
                    LOGGER.debug(
                        f"[pool] Removing {len(stale_sids)} stale Spark Connect "
                        f"sessions from prior invocations"
                    )
                    for sid in stale_sids:
                        info = self._sessions.pop(sid)
                        stale_to_terminate.append((sid, info["client"]))

                # Reuse a session with the same key that still has room
                # for another concurrent model.
                for sid, info in self._sessions.items():
                    if _is_placeholder(sid):
                        continue
                    if info["key"] == key and info["load"] < session_concurrency:
                        info["load"] += 1  # reserve optimistically for liveness check
                        reuse_candidate = sid
                        break

                if reuse_candidate is None:
                    # Under the per-key limit: reserve a placeholder slot so
                    # concurrent acquires see the in-flight session and don't
                    # oversubscribe max_sessions while start_session runs
                    # outside the lock.
                    count = sum(1 for info in self._sessions.values() if info["key"] == key)
                    if count < max_sessions:
                        placeholder_id = (
                            f"{_PLACEHOLDER_PREFIX}{threading.get_ident()}_{time.monotonic_ns()}__"
                        )
                        self._sessions[placeholder_id] = {
                            "key": key,
                            "client": athena_client,
                            "load": 1,
                        }

            # Terminate stale sessions outside the lock (API calls are slow).
            if stale_to_terminate:
                for sid, client in stale_to_terminate:
                    try:
                        client.terminate_session(SessionId=sid)
                        LOGGER.debug(f"Terminated stale Spark Connect session {sid}")
                    except Exception as e:  # noqa: BLE001 - best-effort cleanup
                        LOGGER.warning(f"Failed to terminate stale session {sid}: {e}")
                stale_to_terminate = []

            # Verify the reuse candidate is actually alive before handing it
            # back; Athena may have terminated it for idle timeout or other
            # reasons while it sat in the pool.
            if reuse_candidate is not None:
                if self._is_session_alive(athena_client, reuse_candidate):
                    LOGGER.debug(f"Reusing Spark Connect session {reuse_candidate} for key {key}")
                    return reuse_candidate
                # Dead - evict and try again.
                LOGGER.debug(
                    f"Discarding stale Spark Connect session {reuse_candidate} during reuse"
                )
                self.remove(reuse_candidate)
                continue

            if placeholder_id is not None:
                break

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

        # We reserved a placeholder - start a new session outside the lock so
        # slow Athena calls don't block other threads, then swap the
        # placeholder for the real session id.
        assert placeholder_id is not None
        try:
            session_id = self._start_session(
                athena_client, spark_work_group, engine_config, session_description
            )
        except Exception:
            # Free the reserved slot on failure.
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

    def _is_session_alive(self, athena_client: Any, session_id: str) -> bool:
        """Return True if Athena reports the session as IDLE/BUSY/CREATED."""
        try:
            state = athena_client.get_session_status(SessionId=session_id)["Status"].get(
                "State", ""
            )
        except Exception as e:  # noqa: BLE001 - treat unknown state as dead
            LOGGER.warning(f"Could not verify Spark Connect session {session_id} state: {e}")
            return False
        if state in self._DEAD_SESSION_STATES or not state:
            return False
        return True

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

    def register(self, session_id: str, key: SessionKey, athena_client: Any) -> None:
        with self._lock:
            self._sessions[session_id] = {
                "key": key,
                "client": athena_client,
                "load": 1,
            }

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
        if info is None:
            return
        client = info["client"]
        try:
            client.terminate_session(SessionId=session_id)
            LOGGER.debug(f"Terminated Spark Connect session {session_id}")
        except Exception as e:  # noqa: BLE001 - best-effort cleanup
            LOGGER.warning(f"Failed to terminate Spark Connect session {session_id}: {e}")

    def terminate_all(self) -> None:
        """Terminate every pooled session.

        Prefer ``terminate_by_invocation`` from adapter cleanup — this
        method drops sessions owned by other invocations too, which is
        only safe at true process shutdown.
        """
        with self._lock:
            entries = [
                (sid, info) for sid, info in self._sessions.items() if not _is_placeholder(sid)
            ]
            self._sessions.clear()
        self._terminate_entries(entries)

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

    def _terminate_entries(self, entries: List[Tuple[str, Dict[str, Any]]]) -> None:
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
            try:
                state = athena_client.get_session_status(SessionId=session_id)["Status"].get(
                    "State", ""
                )
            except Exception:  # noqa: BLE001 - treat as dead
                state = ""

            if state in self._DEAD_SESSION_STATES or not state:
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
