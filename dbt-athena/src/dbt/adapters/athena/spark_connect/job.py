"""Spark Connect submitter for Athena Apache Spark 3.5+ python models.

Owns the Spark Connect-specific submission path: session acquisition from
the pool, GetSessionEndpoint readiness wait, gRPC channel construction,
user-code execution, and transient-error retry.  ``AthenaPythonJobHelper``
delegates to this class when ``spark_engine_version`` is ``"3.5"``.
"""

from __future__ import annotations

import json
import os
import random
import threading
import time
import traceback
from functools import cached_property
from hashlib import md5
from typing import Any, Dict, NamedTuple, Optional

import botocore
from dbt_common.exceptions import DbtRuntimeError
from dbt_common.invocation import get_invocation_id

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

_MAX_RETRIES = 3

# Cap GetSessionEndpoint wait so it cannot consume the whole execution budget.
_ENDPOINT_READY_TIMEOUT_SECONDS = 180


class _AttemptResult(NamedTuple):
    result: Optional[Dict[str, Any]]
    error: Optional[BaseException]
    done: bool


class SparkConnectSubmitter:
    """Submits compiled python code to Athena via Spark Connect."""

    def __init__(
        self,
        athena_client: Any,
        credentials: AthenaCredentials,
        config: AthenaSparkSessionConfig,
        engine_config: Dict[str, Any],
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
            "spark_engine_version": str(self.config.config.get("spark_engine_version", "")),
        }
        # ``usedforsecurity=False`` is required on FIPS-enforced Python builds
        # (e.g. RHEL in FIPS mode); md5 here is purely a session-key fingerprint.
        return md5(
            json.dumps(payload, sort_keys=True, ensure_ascii=True, default=str).encode("utf-8"),
            usedforsecurity=False,
        ).hexdigest()

    @cached_property
    def _session_key(self) -> tuple:
        return (get_invocation_id(), self._session_fingerprint)

    @cached_property
    def _max_sessions(self) -> int:
        return self._resolve_int_credential(
            "spark_connect_max_sessions", DEFAULT_SPARK_CONNECT_MAX_SESSIONS
        )

    @cached_property
    def _session_concurrency(self) -> int:
        return self._resolve_int_credential(
            "spark_connect_session_concurrency", DEFAULT_SPARK_CONNECT_SESSION_CONCURRENCY
        )

    def _resolve_int_credential(self, field_name: str, default: int) -> int:
        # AthenaCredentials.__post_init__ normalizes these fields to int,
        # so here we only apply the default when the user didn't set them.
        configured = getattr(self.credentials, field_name, None)
        return default if configured is None else configured

    def _session_description(self) -> str:
        invocation = get_invocation_id()
        return f"dbt: {invocation} - {self._session_fingerprint}"

    def submit(self, compiled_code: str) -> Any:
        """Submit code via Spark Connect (Apache Spark 3.5+).

        Transient Spark Connect errors (credential propagation failures,
        gRPC pool shutdown) are retried with a new session up to
        ``_MAX_RETRIES`` times.  ``self.timeout`` is a hard execution-time
        limit that covers both endpoint wait and code execution.

        Empty ``compiled_code`` bypasses both Athena and the session pool —
        dbt submits a "ghost" empty calculation alongside every python
        model and we have nothing to run.  No session is acquired in this
        case because starting one would cost DPUs for no benefit; models
        with real code will still fingerprint-match and share a session
        via the pool.
        """
        if not compiled_code.strip():
            return {"SparkConnect": True, "SparkSessionId": None}

        start_time = time.monotonic()
        last_error: Optional[BaseException] = None

        for attempt in range(1, _MAX_RETRIES + 1):
            outcome = self._attempt(compiled_code, attempt, start_time)
            if outcome.done:
                return outcome.result
            last_error = outcome.error

            is_last_attempt = attempt >= _MAX_RETRIES
            if is_last_attempt:
                break

            backoff = min(2**attempt, 30) + random.uniform(0, 1)
            remaining = self.timeout - (time.monotonic() - start_time)
            if backoff >= remaining:
                # No budget left to retry; surface the last error as
                # "failed after N attempts" rather than silently sleeping
                # past the execution timeout.
                LOGGER.warning(
                    f"Model {self.relation_name} - Transient Spark Connect error on "
                    f"attempt {attempt}/{_MAX_RETRIES}, "
                    f"but remaining budget ({remaining:.1f}s) is below backoff "
                    f"({backoff:.1f}s); giving up."
                )
                break
            LOGGER.warning(
                f"Model {self.relation_name} - Transient Spark Connect error "
                f"(attempt {attempt}/{_MAX_RETRIES}), "
                f"retrying in {backoff:.1f}s with new session: "
                f"{type(last_error).__name__}: {last_error}"
            )
            time.sleep(backoff)

        # All retries exhausted — re-raise the last error with a wrapping
        # message so operators can distinguish "failed once" from "failed
        # after N attempts".
        raise DbtRuntimeError(
            f"Spark Connect execution failed after {_MAX_RETRIES} "
            f"attempts: {type(last_error).__name__}: {last_error}"
        ) from last_error

    def _acquire_session(self) -> str:
        """Acquire a Spark Connect session from the pool."""
        # pyspark's @try_remote_functions reads this env var at call time.
        os.environ.setdefault("SPARK_CONNECT_MODE_ENABLED", "1")

        spark_work_group = self.credentials.spark_work_group
        if not spark_work_group:
            # Spark Connect cannot target an Athena workgroup if none is
            # configured; fail fast with a clear message rather than letting
            # boto3 surface a less helpful validation error.
            raise DbtRuntimeError(
                "spark_work_group must be set in the Athena profile to submit "
                "python models via Spark Connect (spark_engine_version=3.5)."
            )
        return self._pool.acquire(
            key=self._session_key,
            athena_client=self.athena_client,
            spark_work_group=spark_work_group,
            engine_config=self.engine_config,
            session_description=self._session_description(),
            max_sessions=self._max_sessions,
            timeout=self.timeout,
            polling_interval=self.polling_interval,
            session_concurrency=self._session_concurrency,
        )

    def _wait_for_endpoint(self, session_id: str) -> Dict[str, Any]:
        """Poll GetSessionEndpoint until the endpoint is ready.

        Bounded by ``min(self.timeout, _ENDPOINT_READY_TIMEOUT_SECONDS)`` so
        slow endpoint provisioning cannot consume the full execution budget
        reserved for user code.
        """
        deadline_seconds = min(self.timeout, _ENDPOINT_READY_TIMEOUT_SECONDS)
        timer: float = 0
        # ``throttle_base`` is the exponential-backoff base (without jitter),
        # so successive throttles compute 1→2→4→…→30 rather than doubling a
        # jittered value that drifts unpredictably.
        throttle_base: float = 0
        while True:
            throttled = False
            try:
                response = self.athena_client.get_session_endpoint(SessionId=session_id)
                endpoint_url = response.get("EndpointUrl")
                if endpoint_url:
                    if not response.get("AuthToken"):
                        # Retry instead of failing fast: Athena occasionally
                        # returns an endpoint_url a moment before AuthToken
                        # is populated.
                        LOGGER.debug(
                            f"Session {session_id} endpoint returned without AuthToken, "
                            f"retrying"
                        )
                    else:
                        return response
            except botocore.exceptions.ClientError as e:
                error_code = e.response.get("Error", {}).get("Code", "")
                if error_code == "ThrottlingException":
                    throttled = True
                    throttle_base = min(max(throttle_base, 1) * 2, 30)
                    LOGGER.debug(
                        f"Session {session_id} endpoint throttled, "
                        f"backing off ~{throttle_base:.1f}s"
                    )
                else:
                    LOGGER.debug(f"Waiting for session {session_id} endpoint: {e}")

            if not throttled:
                # Non-throttle path (success-without-token or transient error):
                # reset the backoff so we don't inherit stale pressure.
                throttle_base = 0

            if timer >= deadline_seconds:
                raise DbtRuntimeError(
                    f"Session {session_id} endpoint did not become ready within "
                    f"{deadline_seconds}s (endpoint-wait deadline, not execution "
                    f"timeout)"
                )
            sleep_time = (
                throttle_base + random.uniform(0, 1) if throttled else self.polling_interval
            )
            time.sleep(sleep_time)
            timer += sleep_time

    def _attempt(
        self,
        compiled_code: str,
        attempt: int,
        start_time: float,
    ) -> _AttemptResult:
        """Run one Spark Connect attempt.

        Returns an ``_AttemptResult`` whose semantics are:
          * ``done=True, result=<dict>`` — success, caller should return result.
          * ``done=False, error=<exc>`` — transient failure, caller may retry.

        Non-retriable failures and timeouts raise directly (caller sees the
        exception instead of a return value).

        Session lifecycle: on transient failure the session is terminated
        (it is likely broken); on success or acquire failure it is released
        back to the pool for reuse.
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

            response = self._wait_for_endpoint(session_id)
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
                    f"Model {self.relation_name} - " f"Execution timed out after {self.timeout}s"
                )
                if spark is not None:
                    spark.interruptAll()

            timer = threading.Timer(remaining, _on_timeout)
            timer.start()

            exec_globals: Dict[str, Any] = {"spark": spark}
            exec(compiled_code, exec_globals)  # noqa: S102 - user model code
            return _AttemptResult(
                result={"SparkConnect": True, "SparkSessionId": session_id},
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
                    f"Model {self.relation_name} - Spark Connect execution failed "
                    f"(attempt {attempt}/{_MAX_RETRIES}): "
                    f"{type(e).__name__}: {e}\n{traceback.format_exc()}"
                )
                if not transient:
                    raise DbtRuntimeError(
                        f"Spark Connect execution failed: {type(e).__name__}: {e}"
                    ) from e

            # Transient + not last attempt → let the caller retry with a new
            # session.
            return _AttemptResult(result=None, error=e, done=False)
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
                # Terminate the failed session so DPUs are released;
                # the next attempt acquires a fresh one from the pool.
                self._pool.terminate_and_remove(session_id)
            else:
                self._pool.release(session_id)
