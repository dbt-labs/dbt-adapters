from __future__ import annotations

import random
import threading
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Dict, List, Literal, Optional, Set, Tuple, TypedDict, Union

from dbt_common.exceptions import DbtRuntimeError
from mypy_boto3_athena.client import AthenaClient
from mypy_boto3_athena.type_defs import EngineConfigurationTypeDef

from dbt.adapters.athena.constants import LOGGER, SESSION_IDLE_TIMEOUT_MIN

if TYPE_CHECKING:
    from pyspark.sql.connect.session import SparkSession as ConnectSparkSession

SessionKey = Tuple[str, str]


class _RequiredSessionInfo(TypedDict):
    key: SessionKey
    client: AthenaClient
    load: int
    dpu: int
    draining: bool
    idle_since: Optional[float]


class _SessionInfo(_RequiredSessionInfo, total=False):
    spark: Optional[ConnectSparkSession]


PushbackCategory = Literal["session_limit", "capacity", "throttling"]

_SESSION_LIMIT: PushbackCategory = "session_limit"
_CAPACITY: PushbackCategory = "capacity"
_THROTTLING: PushbackCategory = "throttling"

_PUSHBACK_PATTERNS: Tuple[Tuple[PushbackCategory, Tuple[str, ...]], ...] = (
    (_SESSION_LIMIT, ("Maximum allowed sessions",)),
    (_CAPACITY, ("required capacity not being available",)),
    (_THROTTLING, ("ThrottlingException", "Rate exceeded")),
)


_PUSHBACK_WARNINGS: Dict[PushbackCategory, str] = {
    _SESSION_LIMIT: (
        "Athena rejected StartSession (account session limit) for key {key}; "
        "client-side accounting saw used={used} + request={request} "
        "<= budget={budget}. Another process may share the account quota."
    ),
    _CAPACITY: (
        "Athena rejected StartSession for key {key}: AWS region capacity "
        "unavailable. Backing off; this is transient and budget cannot predict it."
    ),
    _THROTTLING: (
        "Athena throttled StartSession (Rate exceeded) for key {key}; "
        "backing off. Lower dbt threads or spark_connect_max_sessions if persistent."
    ),
}


def _classify_pushback(message: str) -> Optional[PushbackCategory]:
    for category, patterns in _PUSHBACK_PATTERNS:
        if any(pattern in message for pattern in patterns):
            return category
    return None


class _StartSessionPushback(Exception):
    """AWS rejected StartSession for a transient reason the DPU budget cannot predict."""

    def __init__(self, category: PushbackCategory) -> None:
        super().__init__(category)
        self.category = category


@dataclass(frozen=True)
class _AcquireRequest:
    key: SessionKey
    athena_client: AthenaClient
    spark_work_group: str
    engine_config: EngineConfigurationTypeDef
    session_description: str
    max_sessions: int
    timeout: float
    polling_interval: float
    session_concurrency: int
    dpu_request: int
    dpu_budget: int


@dataclass
class _Decision:

    stale_entries: List[Tuple[str, _SessionInfo]] = field(default_factory=list)
    reuse_candidate: Optional[str] = None
    new_session_id: Optional[str] = None
    pushback: Optional[PushbackCategory] = None
    start_error: Optional[BaseException] = None
    budget_used: int = 0
    reclaimed_any: bool = False


class _Retry:
    pass


_RETRY = _Retry()


@dataclass(frozen=True)
class _Wait:
    pushback: Optional[PushbackCategory]
    budget_used: int


_Attempt = Union[str, _Retry, _Wait]


class SparkConnectSessionPool:
    """Process-wide singleton so sessions are shared across invocations in long-lived processes."""

    _instance: Optional["SparkConnectSessionPool"] = None
    _singleton_lock = threading.Lock()

    _DEAD_SESSION_STATES = frozenset({"FAILED", "TERMINATED", "TERMINATING", "DEGRADED"})
    _EVICTION_INTERVAL = 30.0

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

        req = _AcquireRequest(
            key=key,
            athena_client=athena_client,
            spark_work_group=spark_work_group,
            engine_config=engine_config,
            session_description=session_description,
            max_sessions=max_sessions,
            timeout=timeout,
            polling_interval=polling_interval,
            session_concurrency=session_concurrency,
            dpu_request=dpu_request,
            dpu_budget=dpu_budget,
        )
        deadline = time.monotonic() + timeout
        time_since_eviction = self._EVICTION_INTERVAL  # evict on first pass
        pushback_attempts = 0
        skip: Set[str] = set()

        while True:
            attempt = self._try_once(req, skip)
            if isinstance(attempt, str):
                return attempt
            if isinstance(attempt, _Retry):
                continue

            if time_since_eviction >= self._EVICTION_INTERVAL:
                evicted = self._evict_dead_sessions()
                time_since_eviction = 0
                if evicted:
                    continue

            if time.monotonic() >= deadline:
                raise DbtRuntimeError(
                    f"No Spark Connect session available for key {key} within {timeout}s "
                    f"(max_sessions={max_sessions}, dpu_request={dpu_request}, "
                    f"dpu_budget={dpu_budget}, last used_dpu={attempt.budget_used}, "
                    f"draining={self._draining_count()}, unknown_state_skipped={len(skip)})"
                )

            if attempt.pushback is not None:
                pushback_attempts += 1
                sleep_for = self._pushback_backoff(pushback_attempts)
            else:
                pushback_attempts = 0
                sleep_for = polling_interval
            time.sleep(sleep_for)
            time_since_eviction += sleep_for

    def _try_once(self, req: _AcquireRequest, skip: Set[str]) -> _Attempt:
        decision = self._decide(req, skip)

        # Stale cleanup runs even on start_session failure to avoid
        # leaking prior sessions.
        if decision.stale_entries:
            self._terminate_entries(decision.stale_entries)
        if decision.start_error is not None:
            raise decision.start_error
        # Athena counts a session against its limits until TerminateSession returns.
        if decision.reclaimed_any:
            return _RETRY

        if decision.pushback is not None:
            LOGGER.warning(self._pushback_warning(req, decision.pushback, decision.budget_used))

        candidate = decision.reuse_candidate
        if candidate is not None:
            if self._confirm_reuse(req.key, candidate, skip):
                return candidate
            return _RETRY

        if decision.new_session_id is not None:
            return decision.new_session_id

        return _Wait(pushback=decision.pushback, budget_used=decision.budget_used)

    def _decide(self, req: _AcquireRequest, skip: Set[str]) -> _Decision:
        key = req.key
        decision = _Decision()
        with self._lock:
            decision.stale_entries = self._collect_stale_invocations(key[0])
            decision.reuse_candidate = self._attach(key, req.session_concurrency, skip)
            if decision.reuse_candidate is not None:
                return decision
            decision.budget_used = self._used_dpu()
            has_room = self._has_room(key, req.max_sessions)
            if has_room and decision.budget_used + req.dpu_request > req.dpu_budget:
                reclaimed = self._reclaim_idle_for_budget(
                    key, req.dpu_request, req.dpu_budget, decision.budget_used
                )
                if reclaimed:
                    decision.stale_entries.extend(reclaimed)
                    decision.reclaimed_any = True
            budget_ok = decision.budget_used + req.dpu_request <= req.dpu_budget
            if budget_ok and has_room:
                try:
                    decision.new_session_id = self._start(
                        key,
                        req.athena_client,
                        req.spark_work_group,
                        req.engine_config,
                        req.session_description,
                        req.dpu_request,
                    )
                except _StartSessionPushback as e:
                    decision.pushback = e.category
                except Exception as e:  # noqa: BLE001 - re-raised after cleanup
                    decision.start_error = e
        return decision

    @staticmethod
    def _pushback_warning(
        req: _AcquireRequest, category: PushbackCategory, budget_used: int
    ) -> str:
        return _PUSHBACK_WARNINGS[category].format(
            key=req.key,
            used=budget_used,
            request=req.dpu_request,
            budget=req.dpu_budget,
        )

    def _confirm_reuse(self, key: SessionKey, candidate: str, skip: Set[str]) -> bool:
        # Athena may have killed the session while it sat in the pool.
        state = self._session_state(candidate)
        if state is not None and state not in self._DEAD_SESSION_STATES:
            with self._lock:
                info = self._sessions.get(candidate)
                if info is not None:
                    info["idle_since"] = None
            LOGGER.debug(f"Reusing Spark Connect session {candidate} for key {key}")
            return True
        if state is None:
            LOGGER.debug(
                f"Spark Connect session {candidate} state is unknown; "
                f"skipping it for this acquire"
            )
            self._undo_attach(candidate)
            skip.add(candidate)
        else:
            LOGGER.debug(f"Discarding stale Spark Connect session {candidate} during reuse")
            self.unregister(candidate)
        return False

    def _pushback_backoff(self, attempts: int) -> float:
        ceiling = min(
            self._PUSHBACK_MAX_BACKOFF_SECONDS,
            self._PUSHBACK_BASE_BACKOFF_SECONDS * (2 ** (attempts - 1)),
        )
        return random.uniform(0, ceiling)

    def _collect_stale_invocations(self, invocation_id: str) -> List[Tuple[str, _SessionInfo]]:
        """Caller must hold ``self._lock``."""
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
        """Caller must hold ``self._lock`` and terminate the returned entries outside it."""
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
        """Caller must hold ``self._lock``. The load is raised before the out-of-lock liveness check
        to prevent oversubscription.
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
        count = sum(1 for info in self._sessions.values() if info["key"] == key)
        return count < max_sessions

    def _used_dpu(self) -> int:
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
        try:
            response = athena_client.start_session(
                Description=session_description,
                WorkGroup=spark_work_group,
                EngineConfiguration=engine_config,
                SessionIdleTimeoutInMinutes=SESSION_IDLE_TIMEOUT_MIN,
            )
        except Exception as e:  # noqa: BLE001 - transient errors handled below
            category = _classify_pushback(str(e))
            if category is not None:
                raise _StartSessionPushback(category) from e
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
        with self._lock:
            info = self._sessions.get(session_id)
            if info is None:
                return None
            return info.get("spark")

    def set_spark(self, session_id: str, spark: ConnectSparkSession) -> ConnectSparkSession:
        """Return the client bound after the call: the existing one if another caller bound first
        (the caller must stop its own), or ``spark`` unbound if the session is gone.
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
        try:
            state = athena_client.get_session_status(SessionId=session_id)["Status"].get("State")
        except Exception as e:  # noqa: BLE001 - unknown state is not evidence of death
            LOGGER.warning(f"Could not verify Spark Connect session {session_id} state: {e}")
            return None
        return state or None

    def _session_state(self, session_id: str) -> Optional[str]:
        with self._lock:
            info = self._sessions.get(session_id)
            client = info["client"] if info is not None else None
        if client is None:
            return None
        return self._get_session_state(client, session_id)

    def is_session_alive(self, session_id: str) -> bool:
        state = self._session_state(session_id)
        return state is not None and state not in self._DEAD_SESSION_STATES

    def release(self, session_id: str) -> None:
        """The last caller to leave a draining session terminates it on Athena."""
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
        with self._lock:
            info = self._sessions.pop(session_id, None)
        if info is not None:
            self._stop_spark(session_id, info)

    def terminate(self, session_id: str) -> None:
        """Detach after a transient failure. If other callers are still attached the session is only
        marked draining, and the last one to release terminates it.
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
        """Touch only sessions of ``invocation_id``; the singleton is shared across invocations."""
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
        """Sessions with an unknown state are kept until idle past the session idle timeout."""
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
        """Called outside ``self._lock`` so a slow or raising client cannot block other workers."""
        spark = info.get("spark")
        if spark is None:
            return
        try:
            spark.stop()
        except Exception as e:  # noqa: BLE001 - best-effort cleanup
            LOGGER.debug(f"Ignoring error while stopping Spark client for {session_id}: {e}")

    def _snapshot(self) -> Dict[str, _SessionInfo]:
        with self._lock:
            return {sid: _SessionInfo(**info) for sid, info in self._sessions.items()}

    @classmethod
    def _reset_for_tests(cls) -> None:
        with cls._singleton_lock:
            cls._instance = None
