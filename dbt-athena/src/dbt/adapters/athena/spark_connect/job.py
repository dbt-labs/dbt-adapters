"""Spark Connect submitter for Athena Apache Spark 3.5+ python models."""

from __future__ import annotations

import json
import os
import random
import threading
import time
import traceback
from functools import cached_property
from hashlib import md5
from typing import Any, Dict, NamedTuple, Optional, Tuple, TypedDict

import botocore
from dbt_common.exceptions import DbtRuntimeError
from dbt_common.invocation import get_invocation_id
from mypy_boto3_athena.client import AthenaClient
from mypy_boto3_athena.type_defs import (
    EngineConfigurationTypeDef,
    GetSessionEndpointResponseTypeDef,
)
from tenacity import (
    RetryError,
    Retrying,
    retry_if_exception_type,
    stop_after_delay,
    wait_random_exponential,
)

from dbt.adapters.athena.config import AthenaSparkSessionConfig
from dbt.adapters.athena.connections import AthenaCredentials
from dbt.adapters.athena.constants import (
    DEFAULT_SPARK_CONNECT_DPU_BUDGET,
    DEFAULT_SPARK_CONNECT_MAX_RETRIES,
    DEFAULT_SPARK_CONNECT_MAX_SESSIONS,
    DEFAULT_SPARK_CONNECT_POOL_ACQUIRE_TIMEOUT,
    DEFAULT_SPARK_CONNECT_SESSION_CONCURRENCY,
    LOGGER,
)
from dbt.adapters.athena.exceptions import SparkSessionTerminatedError
from dbt.adapters.athena.spark_connect.channel import create_athena_channel_builder
from dbt.adapters.athena.spark_connect.errors import (
    is_grpc_permission_denied,
    is_transient_spark_error,
)
from dbt.adapters.athena.spark_connect.session import SparkConnectSessionPool


class SparkConnectResult(TypedDict):
    SparkConnect: bool
    SparkSessionId: Optional[str]


# Cap GetSessionEndpoint wait so it cannot consume the whole execution budget.
_ENDPOINT_READY_TIMEOUT_SECONDS = 180

# Cap the per-poll backoff so a long throttle storm cannot stretch any single
# wait past 30s; the deadline still bounds total wait.
_ENDPOINT_POLL_MAX_WAIT_SECONDS = 30


class _EndpointNotReady(Exception):
    """Internal sentinel: GetSessionEndpoint should be polled again."""


def _spark_max_executors(engine_config: EngineConfigurationTypeDef) -> Optional[int]:
    """Return ``spark.dynamicAllocation.maxExecutors`` from Spark Connect engine_config.

    Spark Connect uses Classifications (not SparkProperties) to carry
    spark-defaults; the value is a string and must be parsed.
    """
    classifications = engine_config.get("Classifications") or []
    for entry in classifications:
        if entry.get("Name") != "spark-defaults":
            continue
        raw = (entry.get("Properties") or {}).get("spark.dynamicAllocation.maxExecutors")
        if raw is None:
            return None
        try:
            return int(raw)
        except (TypeError, ValueError):
            return None
    return None


class _AttemptResult(NamedTuple):
    result: Optional[SparkConnectResult]
    error: Optional[BaseException]
    done: bool
    session_id: Optional[str] = None


class SparkConnectSubmitter:
    """Submits compiled python code to Athena via Spark Connect."""

    def __init__(
        self,
        athena_client: AthenaClient,
        credentials: AthenaCredentials,
        config: AthenaSparkSessionConfig,
        engine_config: EngineConfigurationTypeDef,
        timeout: int,
        polling_interval: float,
        relation_name: Optional[str],
    ) -> None:
        self.athena_client = athena_client
        self.credentials = credentials
        self.config = config
        self.engine_config = engine_config
        self.timeout = timeout
        self.polling_interval = polling_interval
        self.relation_name = relation_name

    @cached_property
    def _pool(self) -> SparkConnectSessionPool:
        return SparkConnectSessionPool()

    @cached_property
    def _session_fingerprint(self) -> str:
        """md5 of engine config + workgroup + engine version.

        Sessions with matching fingerprint may be reused across models.
        Workgroup and engine version are included so two models that differ
        only in those attributes never accidentally share a session.
        """
        payload = {
            "engine_config": self.engine_config,
            "spark_work_group": self.credentials.spark_work_group,
            "spark_engine_version": self.config.spark_engine_version,
        }
        # ``usedforsecurity=False`` is required on FIPS-enforced Python builds
        # (e.g. RHEL in FIPS mode); md5 here is purely a session-key fingerprint.
        return md5(
            json.dumps(payload, sort_keys=True, ensure_ascii=True, default=str).encode("utf-8"),
            usedforsecurity=False,
        ).hexdigest()

    @cached_property
    def _session_key(self) -> Tuple[str, str]:
        return (get_invocation_id(), self._session_fingerprint)

    @cached_property
    def _max_sessions(self) -> int:
        return self.credentials.spark_connect_max_sessions or DEFAULT_SPARK_CONNECT_MAX_SESSIONS

    @cached_property
    def _session_concurrency(self) -> int:
        return (
            self.credentials.spark_connect_session_concurrency
            or DEFAULT_SPARK_CONNECT_SESSION_CONCURRENCY
        )

    @cached_property
    def _dpu_budget(self) -> int:
        return self.credentials.spark_connect_dpu_budget or DEFAULT_SPARK_CONNECT_DPU_BUDGET

    @cached_property
    def _pool_acquire_timeout(self) -> float:
        return (
            self.credentials.spark_connect_pool_acquire_timeout
            or DEFAULT_SPARK_CONNECT_POOL_ACQUIRE_TIMEOUT
        )

    @cached_property
    def _max_retries(self) -> int:
        value = self.credentials.spark_connect_max_retries
        if value is None:
            return DEFAULT_SPARK_CONNECT_MAX_RETRIES
        return value

    @cached_property
    def _dpu_request(self) -> int:
        """DPUs reserved against the budget when starting a session.

        ``MaxConcurrentDpus`` is the AWS-side hard cap; with dynamic
        allocation, Spark scales up to ``maxExecutors + 1`` (executors +
        driver). The true peak is the smaller of the two.
        """
        max_concurrent = int(self.engine_config["MaxConcurrentDpus"])
        max_executors = _spark_max_executors(self.engine_config)
        if max_executors is None:
            return max_concurrent
        return min(max_concurrent, max_executors + 1)

    @cached_property
    def _session_description(self) -> str:
        return f"dbt: {get_invocation_id()} - {self._session_fingerprint}"

    def submit(self, compiled_code: str) -> SparkConnectResult:
        """Submit code, retrying transient errors with a fresh session.

        Pool-acquire wait and Spark execution use independent budgets.
        ``spark_connect_pool_acquire_timeout`` bounds the cumulative waiting
        in the session pool (free, no DPU spend). ``self.timeout`` bounds
        each attempt's Spark execution time individually; transient
        failures discard their session's work, so charging the next retry
        for time it cannot reuse would deny it a real chance to complete.
        """
        if not compiled_code.strip():
            return SparkConnectResult(SparkConnect=True, SparkSessionId=None)

        pool_start = time.monotonic()
        last_error: Optional[BaseException] = None
        last_session_id: Optional[str] = None
        total_attempts = self._max_retries + 1

        for attempt in range(1, total_attempts + 1):
            outcome = self._attempt(compiled_code, attempt, pool_start)
            if outcome.done:
                assert outcome.result is not None
                return outcome.result
            assert outcome.error is not None
            last_error = outcome.error
            last_session_id = outcome.session_id

            is_last_attempt = attempt >= total_attempts
            if is_last_attempt:
                break

            backoff = min(2**attempt, 30) + random.uniform(0, 1)
            if backoff >= self.timeout:
                LOGGER.warning(
                    f"Model {self.relation_name} (session {last_session_id}) - "
                    f"Transient Spark Connect error on "
                    f"attempt {attempt}/{total_attempts}, "
                    f"but backoff ({backoff:.1f}s) is at least the per-attempt "
                    f"execution budget ({self.timeout:.1f}s); giving up."
                )
                break
            LOGGER.warning(
                f"Model {self.relation_name} (session {last_session_id}) - "
                f"Transient Spark Connect error "
                f"(attempt {attempt}/{total_attempts}), "
                f"retrying in {backoff:.1f}s with new session: "
                f"{type(last_error).__name__}: {last_error}"
            )
            time.sleep(backoff)

        raise DbtRuntimeError(
            f"Spark Connect execution failed after {total_attempts} "
            f"attempts (last session {last_session_id}): "
            f"{type(last_error).__name__}: {last_error}"
        ) from last_error

    def _is_transient_failure(self, e: BaseException) -> bool:
        return is_transient_spark_error(e)

    def _acquire_session(self, pool_timeout: float) -> str:
        """Acquire a Spark Connect session from the pool."""
        os.environ.setdefault("SPARK_CONNECT_MODE_ENABLED", "1")

        spark_work_group = self.credentials.spark_work_group
        if not spark_work_group:
            raise DbtRuntimeError(
                "spark_work_group must be set in the Athena profile to submit "
                "python models via Spark Connect (spark_engine_version=3.5)."
            )
        return self._pool.acquire(
            key=self._session_key,
            athena_client=self.athena_client,
            spark_work_group=spark_work_group,
            engine_config=self.engine_config,
            session_description=self._session_description,
            max_sessions=self._max_sessions,
            timeout=pool_timeout,
            polling_interval=self.polling_interval,
            session_concurrency=self._session_concurrency,
            dpu_request=self._dpu_request,
            dpu_budget=self._dpu_budget,
        )

    def _wait_for_endpoint(
        self, session_id: str, remaining_budget: float
    ) -> GetSessionEndpointResponseTypeDef:
        """Poll GetSessionEndpoint until the endpoint is ready.

        Bounded by ``min(remaining_budget, _ENDPOINT_READY_TIMEOUT_SECONDS)``
        so endpoint-wait stays within the caller's remaining attempt budget
        and never exceeds the per-endpoint cap, regardless of time already
        spent on session acquisition or prior retries.
        """
        deadline_seconds = min(remaining_budget, _ENDPOINT_READY_TIMEOUT_SECONDS)

        def _poll() -> GetSessionEndpointResponseTypeDef:
            try:
                response = self.athena_client.get_session_endpoint(SessionId=session_id)
            except botocore.exceptions.ClientError as e:
                error_code = e.response.get("Error", {}).get("Code", "")
                if error_code == "ThrottlingException":
                    LOGGER.debug(f"Session {session_id} endpoint throttled, backing off")
                else:
                    LOGGER.debug(f"Waiting for session {session_id} endpoint: {e}")
                raise _EndpointNotReady() from e
            if response.get("EndpointUrl") and response.get("AuthToken"):
                return response
            if response.get("EndpointUrl"):
                # Athena occasionally returns endpoint_url a moment before
                # AuthToken is populated; treat as not-ready and retry.
                LOGGER.debug(f"Session {session_id} endpoint returned without AuthToken, retrying")
            raise _EndpointNotReady()

        try:
            for attempt in Retrying(
                stop=stop_after_delay(deadline_seconds),
                wait=wait_random_exponential(
                    multiplier=self.polling_interval,
                    max=_ENDPOINT_POLL_MAX_WAIT_SECONDS,
                ),
                retry=retry_if_exception_type(_EndpointNotReady),
                reraise=False,
            ):
                with attempt:
                    return _poll()
        except RetryError:
            pass

        raise DbtRuntimeError(
            f"Session {session_id} endpoint did not become ready within "
            f"{deadline_seconds}s (endpoint-wait deadline, not execution timeout)"
        )

    def _attempt(
        self,
        compiled_code: str,
        attempt: int,
        pool_start: float,
    ) -> _AttemptResult:
        """Run one attempt; ``done=True`` on success, ``done=False`` on transient failure."""
        pool_remaining = self._pool_acquire_timeout - (time.monotonic() - pool_start)
        if pool_remaining <= 0:
            raise DbtRuntimeError(
                f"Spark Connect session pool acquire timed out after "
                f"{self._pool_acquire_timeout} seconds."
            )
        session_id = self._acquire_session(pool_remaining)

        attempt_start = time.monotonic()
        spark = None
        timer: Optional[threading.Timer] = None
        timeout_event = threading.Event()
        terminate_session = False

        def _elapsed() -> float:
            return time.monotonic() - attempt_start

        try:
            response = self._wait_for_endpoint(session_id, self.timeout)
            channel_builder = create_athena_channel_builder(
                self.athena_client,
                session_id,
                response["EndpointUrl"],
                initial_auth_token=response.get("AuthToken"),
                initial_token_expiry=response.get("AuthTokenExpirationTime"),
            )

            from pyspark.sql.connect.session import (
                SparkSession as ConnectSparkSession,
            )

            spark = ConnectSparkSession.builder.channelBuilder(channel_builder).create()

            exec_remaining = self.timeout - _elapsed()
            if exec_remaining <= 0:
                raise DbtRuntimeError(
                    f"Spark Connect execution timed out after {self.timeout} seconds."
                )

            def _on_timeout() -> None:
                timeout_event.set()
                LOGGER.warning(
                    f"Model {self.relation_name} (session {session_id}) - "
                    f"Execution timed out after {self.timeout}s"
                )
                if spark is not None:
                    spark.interruptAll()

            timer = threading.Timer(exec_remaining, _on_timeout)
            timer.start()

            exec_globals: Dict[str, Any] = {"spark": spark}
            exec(compiled_code, exec_globals)  # noqa: S102 - user model code
            return _AttemptResult(
                result=SparkConnectResult(SparkConnect=True, SparkSessionId=session_id),
                error=None,
                done=True,
            )
        except DbtRuntimeError:
            raise
        except Exception as e:
            if timeout_event.is_set():
                raise DbtRuntimeError(
                    f"Spark Connect execution timed out after {self.timeout} seconds."
                ) from e

            # 403 with a dead session means Athena ended the session itself,
            # so a fresh session cannot resume the work.
            if is_grpc_permission_denied(e) and not self._pool.is_session_alive(
                self.athena_client, session_id
            ):
                LOGGER.error(
                    f"Model {self.relation_name} (session {session_id}) - "
                    f"Athena terminated the Spark session\n{traceback.format_exc()}"
                )
                raise SparkSessionTerminatedError(
                    f"Athena terminated Spark session {session_id}; "
                    f"check session state and workgroup DPU/quota. "
                    f"Underlying error: {type(e).__name__}: {e}"
                ) from e

            transient = self._is_transient_failure(e)
            terminate_session = transient
            total_attempts = self._max_retries + 1
            is_last_attempt = attempt >= total_attempts

            if not transient or is_last_attempt:
                LOGGER.error(
                    f"Model {self.relation_name} (session {session_id}) - "
                    f"Spark Connect execution failed "
                    f"(attempt {attempt}/{total_attempts}): "
                    f"{type(e).__name__}: {e}\n{traceback.format_exc()}"
                )
                if not transient:
                    raise DbtRuntimeError(
                        f"Spark Connect execution failed (session {session_id}): "
                        f"{type(e).__name__}: {e}"
                    ) from e

            return _AttemptResult(
                result=None,
                error=e,
                done=False,
                session_id=session_id,
            )
        finally:
            # Cancel the watchdog timer first and wait for any already-fired
            # callback to finish.  Otherwise spark.interruptAll() running in
            # the timer thread can race with spark.stop() below.
            if timer is not None:
                timer.cancel()
                timer.join(timeout=5)
            if spark is not None:
                try:
                    spark.stop()
                except Exception as e:  # noqa: BLE001 - best-effort cleanup
                    LOGGER.debug(f"Ignoring error while stopping Spark session: {e}")
            if terminate_session:
                self._pool.terminate(session_id)
            else:
                self._pool.release(session_id)
