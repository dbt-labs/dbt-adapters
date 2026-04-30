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
    DEFAULT_SPARK_CONNECT_MAX_SESSIONS,
    DEFAULT_SPARK_CONNECT_SESSION_CONCURRENCY,
    LOGGER,
)
from dbt.adapters.athena.spark_connect.channel import create_athena_channel_builder
from dbt.adapters.athena.spark_connect.errors import is_transient_spark_error
from dbt.adapters.athena.spark_connect.session import SparkConnectSessionPool


class SparkConnectResult(TypedDict):
    SparkConnect: bool
    SparkSessionId: Optional[str]


_MAX_RETRIES = 3

# Cap GetSessionEndpoint wait so it cannot consume the whole execution budget.
_ENDPOINT_READY_TIMEOUT_SECONDS = 180

# Cap the per-poll backoff so a long throttle storm cannot stretch any single
# wait past 30s; the deadline still bounds total wait.
_ENDPOINT_POLL_MAX_WAIT_SECONDS = 30


class _EndpointNotReady(Exception):
    """Internal sentinel: GetSessionEndpoint should be polled again."""


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
    def _session_description(self) -> str:
        return f"dbt: {get_invocation_id()} - {self._session_fingerprint}"

    def submit(self, compiled_code: str) -> SparkConnectResult:
        """Submit code, retrying transient errors with a fresh session."""
        # dbt submits a ghost empty calculation alongside every python model;
        # skip pool acquisition to avoid spending DPUs on nothing.
        if not compiled_code.strip():
            return SparkConnectResult(SparkConnect=True, SparkSessionId=None)

        start_time = time.monotonic()
        last_error: Optional[BaseException] = None
        last_session_id: Optional[str] = None

        for attempt in range(1, _MAX_RETRIES + 1):
            outcome = self._attempt(compiled_code, attempt, start_time)
            if outcome.done and outcome.result is not None:
                return outcome.result
            last_error = outcome.error
            last_session_id = outcome.session_id

            is_last_attempt = attempt >= _MAX_RETRIES
            if is_last_attempt:
                break

            backoff = min(2**attempt, 30) + random.uniform(0, 1)
            remaining = self.timeout - (time.monotonic() - start_time)
            if backoff >= remaining:
                LOGGER.warning(
                    f"Model {self.relation_name} (session {last_session_id}) - "
                    f"Transient Spark Connect error on "
                    f"attempt {attempt}/{_MAX_RETRIES}, "
                    f"but remaining budget ({remaining:.1f}s) is below backoff "
                    f"({backoff:.1f}s); giving up."
                )
                break
            LOGGER.warning(
                f"Model {self.relation_name} (session {last_session_id}) - "
                f"Transient Spark Connect error "
                f"(attempt {attempt}/{_MAX_RETRIES}), "
                f"retrying in {backoff:.1f}s with new session: "
                f"{type(last_error).__name__}: {last_error}"
            )
            time.sleep(backoff)

        raise DbtRuntimeError(
            f"Spark Connect execution failed after {_MAX_RETRIES} "
            f"attempts (last session {last_session_id}): "
            f"{type(last_error).__name__}: {last_error}"
        ) from last_error

    def _acquire_session(self) -> str:
        """Acquire a Spark Connect session from the pool."""
        # pyspark's @try_remote_functions reads this env var at call time.
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
            timeout=self.timeout,
            polling_interval=self.polling_interval,
            session_concurrency=self._session_concurrency,
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
        start_time: float,
    ) -> _AttemptResult:
        """Run one attempt; ``done=True`` on success, ``done=False`` on transient failure.

        Non-retriable failures and timeouts raise; broken sessions are
        terminated, healthy ones released back to the pool.
        """
        session_id = self._acquire_session()
        spark = None
        timer: Optional[threading.Timer] = None
        timeout_event = threading.Event()
        terminate_session = False

        try:
            remaining = self.timeout - (time.monotonic() - start_time)
            if remaining <= 0:
                raise DbtRuntimeError(
                    f"Spark Connect execution timed out after {self.timeout} seconds."
                )

            response = self._wait_for_endpoint(session_id, remaining)
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

            remaining = self.timeout - (time.monotonic() - start_time)
            if remaining <= 0:
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

            timer = threading.Timer(remaining, _on_timeout)
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

            transient = is_transient_spark_error(e)
            is_last_attempt = attempt >= _MAX_RETRIES

            if transient:
                # Terminate the broken session whether or not we retry:
                # leaving it in the pool risks a later model reusing it and
                # hitting the same failure ("Session not active" etc.).
                terminate_session = True

            if not transient or is_last_attempt:
                LOGGER.error(
                    f"Model {self.relation_name} (session {session_id}) - "
                    f"Spark Connect execution failed "
                    f"(attempt {attempt}/{_MAX_RETRIES}): "
                    f"{type(e).__name__}: {e}\n{traceback.format_exc()}"
                )
                if not transient:
                    raise DbtRuntimeError(
                        f"Spark Connect execution failed (session {session_id}): "
                        f"{type(e).__name__}: {e}"
                    ) from e

            return _AttemptResult(result=None, error=e, done=False, session_id=session_id)
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
