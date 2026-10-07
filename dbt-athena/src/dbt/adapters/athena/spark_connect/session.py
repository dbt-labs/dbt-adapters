"""Thread-safe pool for Athena Spark Connect sessions, keyed by ``(invocation_id, fingerprint)``."""

from __future__ import annotations

import random
import threading
import time
from typing import TYPE_CHECKING, Dict, List, Optional, Set, Tuple, TypedDict

from dbt_common.exceptions import DbtRuntimeError
from mypy_boto3_athena.client import AthenaClient
from mypy_boto3_athena.type_defs import EngineConfigurationTypeDef

from dbt.adapters.athena.constants import LOGGER, SESSION_IDLE_TIMEOUT_MIN

if TYPE_CHECKING:
    from pyspark.sql.connect.session import SparkSession as ConnectSparkSession

SessionKey = Tuple[str, str]


class _SessionInfo(TypedDict, total=False):
    key: SessionKey
    client: AthenaClient
    load: int
    dpu: int
    draining: bool
    idle_since: Optional[float]
    # Spark Connect client bound to this Athena session, shared by every
    # model that attaches to it. ``None`` until the first model creates it.
    spark: Optional[ConnectSparkSession]


class _GlobalSessionLimitReached(Exception):
    """Raised when Athena returns ``Maximum allowed sessions reached``."""


class _AccountCapacityUnavailable(Exception):
    """Raised when Athena returns ``required capacity not being available``.

    Distinct from the account session-limit signal: this is a transient
    region-level capacity shortage that the DPU budget cannot predict.
    """


class _StartSessionThrottled(Exception):
    """Raised when Athena throttles StartSession (``ThrottlingException`` /
    ``Rate exceeded``).

    The StartSession call-rate quota is account-wide and independent of the DPU
    budget: many small sessions fit the budget yet still burst past the rate
    limit, so the pool backs off and retries rather than failing the model.
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

    # Full-jitter exponential backoff bounds for AWS StartSession pushbacks
    # (session limit, region capacity, throttling), so concurrent workers hit
    # by the same event disperse their retries instead of colliding in lockstep.
    _PUSHBACK_BASE_BACKOFF_SECONDS = 1.0
    _PUSHBACK_MAX_BACKOFF_SECONDS = 30.0

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
        pushback_attempts = 0
        skip: Set[str] = set()

        while True:
            new_session_id: Optional[str] = None
            pushback: Optional[str] = None
            start_error: Optional[BaseException] = None
            budget_used = 0
            budget_ok = False
            reclaimed_any = False
            with self._lock:
                stale_entries = self._collect_stale_invocations(invocation_id)
                reuse_candidate = self._attach(key, session_concurrency, skip)
                if reuse_candidate is None:
                    budget_used = self._used_dpu()
                    has_room = self._has_room(key, max_sessions)
                    if has_room and budget_used + dpu_request > dpu_budget:
                        reclaimed = self._reclaim_idle_for_budget(
                            key, dpu_request, dpu_budget, budget_used
                        )
                        if reclaimed:
                            stale_entries.extend(reclaimed)
                            reclaimed_any = True
                    budget_ok = budget_used + dpu_request <= dpu_budget
                    if budget_ok and has_room:
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
                            pushback = "session_limit"
                        except _AccountCapacityUnavailable:
                            pushback = "capacity"
                        except _StartSessionThrottled:
                            pushback = "throttling"
                        except Exception as e:  # noqa: BLE001 - re-raised after cleanup
                            start_error = e

            # Stale cleanup runs even on start_session failure to avoid
            # leaking prior sessions.
            if stale_entries:
                self._terminate_entries(stale_entries)
            if start_error is not None:
                raise start_error
            # Athena counts a session against its limits until TerminateSession returns.
            if reclaimed_any:
                continue

            if pushback == "session_limit":
                LOGGER.warning(
                    f"Athena rejected StartSession (account session limit) for key {key}; "
                    f"client-side accounting saw used={budget_used} + request={dpu_request} "
                    f"<= budget={dpu_budget}. Another process may share the account quota."
                )
            elif pushback == "capacity":
                LOGGER.warning(
                    f"Athena rejected StartSession for key {key}: AWS region capacity "
                    f"unavailable. Backing off; this is transient and budget cannot predict it."
                )
            elif pushback == "throttling":
                LOGGER.warning(
                    f"Athena throttled StartSession (Rate exceeded) for key {key}; "
                    f"backing off. Lower dbt threads or spark_connect_max_sessions if persistent."
                )

            if reuse_candidate is not None:
                # Athena may have killed the session while it sat in the pool.
                state = self._session_state(reuse_candidate)
                if state is not None and state not in self._DEAD_SESSION_STATES:
                    with self._lock:
                        info = self._sessions.get(reuse_candidate)
                        if info is not None:
                            info["idle_since"] = None
                    LOGGER.debug(f"Reusing Spark Connect session {reuse_candidate} for key {key}")
                    return reuse_candidate
                if state is None:
                    LOGGER.debug(
                        f"Spark Connect session {reuse_candidate} state is unknown; "
                        f"skipping it for this acquire"
                    )
                    self._undo_attach(reuse_candidate)
                    skip.add(reuse_candidate)
                else:
                    LOGGER.debug(
                        f"Discarding stale Spark Connect session {reuse_candidate} during reuse"
                    )
                    self.unregister(reuse_candidate)
                continue

            if new_session_id is not None:
                return new_session_id

            # Periodically evict dead sessions so stuck slots don't block.
            if time_since_eviction >= self._EVICTION_INTERVAL:
                evicted = self._evict_dead_sessions()
                time_since_eviction = 0
                if evicted:
                    continue

            if time.monotonic() >= deadline:
                raise DbtRuntimeError(
                    f"No Spark Connect session available for key {key} within {timeout}s "
                    f"(max_sessions={max_sessions}, dpu_request={dpu_request}, "
                    f"dpu_budget={dpu_budget}, last used_dpu={budget_used}, "
                    f"draining={self._draining_count()}, unknown_state_skipped={len(skip)})"
                )

            if pushback is not None:
                pushback_attempts += 1
                sleep_for = self._pushback_backoff(pushback_attempts)
            else:
                pushback_attempts = 0
                sleep_for = polling_interval
            time.sleep(sleep_for)
            time_since_eviction += sleep_for

    def _pushback_backoff(self, attempts: int) -> float:
        """Full-jitter exponential backoff for consecutive StartSession pushbacks."""
        ceiling = min(
            self._PUSHBACK_MAX_BACKOFF_SECONDS,
            self._PUSHBACK_BASE_BACKOFF_SECONDS * (2 ** (attempts - 1)),
        )
        return random.uniform(0, ceiling)

    def _collect_stale_invocations(self, invocation_id: str) -> List[Tuple[str, _SessionInfo]]:
        """Pop idle sessions from prior invocations and drain busy ones.

        Caller must hold ``self._lock``. Prevents cruft across dbt runs in
        long-lived processes (e.g. dbt Cloud). A session another invocation is
        still using is only marked draining; its last ``release`` terminates it.
        """
        idle: List[Tuple[str, _SessionInfo]] = []
        newly_draining = 0
        for sid, info in list(self._sessions.items()):
            if info["key"][0] == invocation_id:
                continue
            if info["load"] == 0:
                idle.append((sid, self._sessions.pop(sid)))
            elif not info["draining"]:
                info["draining"] = True
                newly_draining += 1
        if idle:
            LOGGER.debug(
                f"Removing {len(idle)} stale Spark Connect sessions from prior invocations"
            )
        if newly_draining:
            LOGGER.debug(
                f"Draining {newly_draining} in-use Spark Connect sessions from prior invocations"
            )
        return idle

    def _reclaim_idle_for_budget(
        self, key: SessionKey, dpu_request: int, dpu_budget: int, used_dpu: int
    ) -> List[Tuple[str, _SessionInfo]]:
        """Pop the oldest idle sessions of other keys that free enough DPUs for ``key``.

        Caller must hold ``self._lock`` and terminate the returned entries
        outside it. Pops nothing unless the idle sessions can free enough.
        """
        shortfall = used_dpu + dpu_request - dpu_budget
        chosen: List[str] = []
        freed = 0
        for sid, info in self._sessions.items():
            if info["key"] == key or info["load"] > 0 or info["draining"]:
                continue
            chosen.append(sid)
            freed += info["dpu"]
            if freed >= shortfall:
                break
        if freed < shortfall:
            return []
        LOGGER.info(
            f"Reclaiming {len(chosen)} idle Spark Connect session(s) {chosen} "
            f"({freed} DPUs) of other keys to start a session for key {key}"
        )
        return [(sid, self._sessions.pop(sid)) for sid in chosen]

    def _attach(
        self, key: SessionKey, session_concurrency: int, skip: Optional[Set[str]] = None
    ) -> Optional[str]:
        """Attach to a reusable session by incrementing its load.

        Caller must hold ``self._lock``. Increments load before the
        out-of-lock liveness check to prevent oversubscription.
        """
        skip = skip or set()
        for sid, info in self._sessions.items():
            if sid in skip:
                continue
            if info["key"] == key and info["load"] < session_concurrency and not info["draining"]:
                info["load"] += 1
                return sid
        return None

    def _undo_attach(self, session_id: str) -> None:
        with self._lock:
            info = self._sessions.get(session_id)
            if info is not None:
                info["load"] = max(info["load"] - 1, 0)

    def _draining_count(self) -> int:
        with self._lock:
            return sum(1 for info in self._sessions.values() if info["draining"])

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
            if "ThrottlingException" in message or "Rate exceeded" in message:
                raise _StartSessionThrottled() from e
            raise

        session_id = str(response["SessionId"])
        self._sessions[session_id] = {
            "key": key,
            "client": athena_client,
            "load": 1,
            "dpu": dpu,
            "draining": False,
            "idle_since": None,
            "spark": None,
        }
        return session_id

    def get_spark(self, session_id: str) -> Optional[ConnectSparkSession]:
        """Return the Spark Connect client bound to ``session_id``, if any."""
        with self._lock:
            info = self._sessions.get(session_id)
            if info is None:
                return None
            return info.get("spark")

    def set_spark(self, session_id: str, spark: ConnectSparkSession) -> ConnectSparkSession:
        """Bind ``spark`` to ``session_id`` unless another client already is.

        Returns the client that is bound after the call. When another caller
        bound a client first, that client is returned and the caller must
        stop its own. When the session is no longer registered, the passed
        client is returned unbound so the caller can still finish its work.

        One client per Athena session is deliberate: the Athena Spark
        Connect server caps the number of Spark Connect sessions it will
        accept per Athena session, and a Spark 3.5 client cannot release its
        server-side session on ``stop()``. Creating a client per model
        therefore exhausts that cap after a fixed number of models; reusing
        one client per Athena session keeps the count at one.
        """
        with self._lock:
            info = self._sessions.get(session_id)
            if info is None:
                return spark
            existing = info.get("spark")
            if existing is not None:
                return existing
            info["spark"] = spark
            return spark

    def _get_session_state(self, athena_client: AthenaClient, session_id: str) -> Optional[str]:
        """Return the Athena session state, or ``None`` when it cannot be determined."""
        try:
            state = athena_client.get_session_status(SessionId=session_id)["Status"].get("State")
        except Exception as e:  # noqa: BLE001 - unknown state is not evidence of death
            LOGGER.warning(f"Could not verify Spark Connect session {session_id} state: {e}")
            return None
        return state or None

    def _session_state(self, session_id: str) -> Optional[str]:
        """Look up the state with the client that started the session."""
        with self._lock:
            info = self._sessions.get(session_id)
            client = info["client"] if info is not None else None
        if client is None:
            return None
        return self._get_session_state(client, session_id)

    def is_session_alive(self, session_id: str) -> bool:
        """Return True if Athena reports the session as IDLE/BUSY/CREATED."""
        state = self._session_state(session_id)
        return state is not None and state not in self._DEAD_SESSION_STATES

    def release(self, session_id: str) -> None:
        """Mark the session as idle so it can be reused.

        When the last caller leaves a draining session (one abandoned by a
        transient failure while co-tenants were still attached), terminate
        it on Athena instead of leaving it to leak until idle timeout.
        """
        with self._lock:
            info = self._sessions.get(session_id)
            if info is None:
                return
            info["load"] = max(info["load"] - 1, 0)
            if info["load"] == 0:
                info["idle_since"] = time.monotonic()
            if info["load"] > 0 or not info["draining"]:
                return
            self._sessions.pop(session_id, None)
        self._terminate_entries([(session_id, info)])

    def unregister(self, session_id: str) -> None:
        """Drop a session from the pool without terminating it on Athena."""
        with self._lock:
            info = self._sessions.pop(session_id, None)
        if info is not None:
            self._stop_spark(session_id, info)

    def terminate(self, session_id: str) -> None:
        """Detach the calling model after a transient failure.

        Terminates the Athena session only when this was its last caller.
        When other models are still attached (``session_concurrency`` > 1),
        the session is marked draining instead: co-tenants keep running,
        no new caller attaches, and it is terminated once the last caller
        releases it (see ``release``). This prevents one model's transient
        failure from tearing the shared session out from under its
        co-tenants.
        """
        with self._lock:
            info = self._sessions.get(session_id)
            if info is None:
                return
            info["load"] = max(info["load"] - 1, 0)
            if info["load"] > 0:
                info["draining"] = True
                return
            self._sessions.pop(session_id, None)
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
            self._stop_spark(session_id, info)
            try:
                info["client"].terminate_session(SessionId=session_id)
                LOGGER.debug(f"Terminated Spark Connect session {session_id}")
            except Exception as e:  # noqa: BLE001 - best-effort cleanup
                LOGGER.warning(f"Failed to terminate Spark Connect session {session_id}: {e}")

    def _evict_dead_sessions(self) -> int:
        """Remove sessions that Athena reports as terminated or degraded.

        Sessions whose state cannot be determined are kept.
        """
        with self._lock:
            session_ids = list(self._sessions)

        evicted = 0
        idle_timeout_seconds = SESSION_IDLE_TIMEOUT_MIN * 60
        for session_id in session_ids:
            state = self._session_state(session_id)
            if state is None:
                with self._lock:
                    info = self._sessions.get(session_id)
                    idle_since = info.get("idle_since") if info is not None else None
                    expired = (
                        info is not None
                        and info["load"] == 0
                        and idle_since is not None
                        and time.monotonic() - idle_since >= idle_timeout_seconds
                    )
                    if expired:
                        self._sessions.pop(session_id, None)
                if expired and info is not None:
                    LOGGER.warning(
                        f"Dropping Spark Connect session {session_id}: its state is unknown "
                        f"and it has been idle longer than the session idle timeout"
                    )
                    self._terminate_entries([(session_id, info)])
                    evicted += 1
                continue
            if state in self._DEAD_SESSION_STATES:
                with self._lock:
                    info = self._sessions.pop(session_id, None)
                if info is not None:
                    LOGGER.debug(
                        f"Evicting dead Spark Connect session {session_id} (state={state})"
                    )
                    self._stop_spark(session_id, info)
                    evicted += 1
        return evicted

    @staticmethod
    def _stop_spark(session_id: str, info: _SessionInfo) -> None:
        """Best-effort ``stop()`` of the client bound to a session being dropped.

        Called outside ``self._lock``: on pyspark 3.5 ``stop()`` only closes
        the local gRPC channel, but the call is still kept off the lock so a
        slow or raising client never blocks other workers.
        """
        spark = info.get("spark")
        if spark is None:
            return
        try:
            spark.stop()
        except Exception as e:  # noqa: BLE001 - best-effort cleanup
            LOGGER.debug(f"Ignoring error while stopping Spark client for {session_id}: {e}")

    # -- test helpers -----------------------------------------------------

    def _snapshot(self) -> Dict[str, _SessionInfo]:
        with self._lock:
            return {sid: _SessionInfo(**info) for sid, info in self._sessions.items()}

    @classmethod
    def _reset_for_tests(cls) -> None:
        """Reset the singleton instance.  Test-only utility."""
        with cls._singleton_lock:
            cls._instance = None
