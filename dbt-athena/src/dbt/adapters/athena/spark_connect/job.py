from __future__ import annotations

import json
import random
import threading
import time
import traceback
import uuid
from hashlib import md5
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Optional,
    Tuple,
    TypedDict,
    Union,
)

import boto3
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
    LOGGER,
    SparkConnectRetryCategory,
)
from dbt.adapters.athena.exceptions import SparkSessionTerminatedError
from dbt.adapters.athena.session import get_boto3_session_from_credentials
from dbt.adapters.athena.spark_connect.channel import create_athena_channel_builder
from dbt.adapters.athena.spark_connect.errors import (
    SESSION_ENDED,
    classify_transient_spark_error,
    is_grpc_permission_denied,
    is_session_ended_error,
)
from dbt.adapters.athena.spark_connect.keepalive import SessionKeepalive
from dbt.adapters.athena.spark_connect.session import SparkConnectSessionPool

if TYPE_CHECKING:
    from pyspark.sql.connect.session import SparkSession as ConnectSparkSession


class SparkConnectResult(TypedDict):
    SparkConnect: bool
    SparkSessionId: Optional[str]


_ENDPOINT_READY_TIMEOUT_SECONDS = 180

_ENDPOINT_POLL_MAX_WAIT_SECONDS = 30


class _EndpointNotReady(Exception):
    pass


def _spark_max_executors(engine_config: EngineConfigurationTypeDef) -> Optional[int]:
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


class _TransientAttemptFailure(Exception):
    def __init__(
        self,
        error: BaseException,
        session_id: str,
        session_ended: bool,
        retryable: bool,
        category: SparkConnectRetryCategory,
    ) -> None:
        super().__init__(str(error))
        self.error = error
        self.session_id = session_id
        self.session_ended = session_ended
        self.retryable = retryable
        self.category = category


class _ExecutionGuard:

    def __init__(
        self,
        *,
        spark: ConnectSparkSession,
        session_id: str,
        relation_name: Optional[str],
        timeout: float,
        budget: float,
        keepalive_interval: float,
        timeout_event: threading.Event,
    ) -> None:
        self._spark = spark
        self._session_id = session_id
        self._relation_name = relation_name
        self._timeout = timeout
        self._budget = budget
        self._keepalive_interval = keepalive_interval
        self._timeout_event = timeout_event
        # Tags are thread-local in the Spark Connect client, so the watchdog
        # can cancel only this model's operations on the shared client.
        self._tag = f"dbt-model-{uuid.uuid4().hex}"
        self._tagged = False
        self._keepalive: Optional[SessionKeepalive] = None
        self._timer: Optional[threading.Timer] = None
        self._timer_started = False

    def __enter__(self) -> "_ExecutionGuard":
        try:
            self._spark.addTag(self._tag)
            self._tagged = True
            if self._keepalive_interval > 0:
                self._keepalive = SessionKeepalive(
                    self._spark, self._session_id, self._keepalive_interval
                )
                self._keepalive.start()
            self._timer = threading.Timer(self._budget, self._on_timeout)
            self._timer.start()
            self._timer_started = True
        except BaseException:
            self._release()
            raise
        return self

    def __exit__(self, *exc_info: Any) -> None:
        self._release()

    def _on_timeout(self) -> None:
        self._timeout_event.set()
        LOGGER.warning(
            f"Model {self._relation_name} (session {self._session_id}) - "
            f"Execution timed out after {self._timeout}s"
        )
        self._spark.interruptTag(self._tag)

    def _release(self) -> None:
        if self._keepalive is not None:
            self._keepalive.stop()
        # Cancel the watchdog timer first and wait for any already-fired
        # callback to finish, so interruptTag() cannot race with the
        # tag removal below.
        if self._timer is not None:
            self._timer.cancel()
            # A timer whose start() failed cannot be joined, and the join error
            # would replace the exception that is being propagated.
            if self._timer_started:
                self._timer.join(timeout=5)
        if self._tagged:
            try:
                self._spark.removeTag(self._tag)
            except Exception as e:  # noqa: BLE001 - best-effort cleanup
                LOGGER.debug(f"Ignoring error while removing Spark tag: {e}")


class SparkConnectSubmitter:

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
        self._pool = SparkConnectSessionPool()

    @property
    def _total_attempts(self) -> int:
        return self.credentials.effective_spark_connect_max_retries + 1

    @property
    def _session_fingerprint(self) -> str:
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

    @property
    def _session_key(self) -> Tuple[str, str]:
        return (get_invocation_id(), self._session_fingerprint)

    @property
    def _dpu_request(self) -> int:
        """The peak is min(MaxConcurrentDpus, maxExecutors + 1); the +1 is the driver."""
        max_concurrent = int(self.engine_config["MaxConcurrentDpus"])
        max_executors = _spark_max_executors(self.engine_config)
        if max_executors is None:
            return max_concurrent
        return min(max_concurrent, max_executors + 1)

    @property
    def _session_description(self) -> str:
        return f"dbt: {get_invocation_id()} - {self._session_fingerprint}"

    def submit(self, compiled_code: str) -> SparkConnectResult:
        """``spark_connect_pool_acquire_timeout`` bounds the cumulative pool wait; ``self.timeout`` applies to each attempt."""
        if not compiled_code.strip():
            return SparkConnectResult(SparkConnect=True, SparkSessionId=None)

        self._install_assumed_default_session()

        pool_start = time.monotonic()
        total_attempts = self._total_attempts
        attempt = 0

        while True:
            attempt += 1
            try:
                return self._attempt(compiled_code, attempt, pool_start)
            except _TransientAttemptFailure as failure:
                if attempt >= total_attempts or not failure.retryable:
                    raise self._final_error(failure, attempt) from failure.error

                backoff = min(2**attempt, 30) + random.uniform(0, 1)
                if backoff >= self.timeout:
                    LOGGER.warning(
                        f"Model {self.relation_name} (session {failure.session_id}) - "
                        f"Transient Spark Connect error on "
                        f"attempt {attempt}/{total_attempts}, "
                        f"but backoff ({backoff:.1f}s) is at least the per-attempt "
                        f"execution budget ({self.timeout:.1f}s); giving up."
                    )
                    raise self._final_error(failure, attempt) from failure.error
                LOGGER.warning(
                    f"Model {self.relation_name} (session {failure.session_id}) - "
                    f"Transient Spark Connect error "
                    f"(attempt {attempt}/{total_attempts}), "
                    f"retrying in {backoff:.1f}s with new session: "
                    f"{type(failure.error).__name__}: {failure.error}"
                )
                time.sleep(backoff)

    def _final_error(self, failure: _TransientAttemptFailure, attempts_made: int) -> Exception:
        error = failure.error
        if failure.session_ended:
            return SparkSessionTerminatedError(
                f"Athena terminated Spark session {failure.session_id}; "
                f"check session state and workgroup DPU/quota. "
                f"Underlying error: {type(error).__name__}: {error}"
            )
        if not failure.retryable:
            return DbtRuntimeError(
                f"Spark Connect execution failed (session {failure.session_id}); not retried "
                f"because transient category '{failure.category}' is not in "
                f"spark_connect_retry_on: {type(error).__name__}: {error}"
            )
        return DbtRuntimeError(
            f"Spark Connect execution failed after {attempts_made} "
            f"attempts (last session {failure.session_id}): "
            f"{type(error).__name__}: {error}"
        )

    def _install_assumed_default_session(self) -> None:
        # Spark Connect runs the model body client-side via exec(), so a bare
        # boto3.client(...) in the model uses the process-default session (the
        # caller), not assume_role_arn.
        if not self.credentials.assume_role_arn:
            return
        assumed = get_boto3_session_from_credentials(self.credentials)
        # Assign directly; setup_default_session(botocore_session=assumed._session)
        # re-registers the creating-client-class.s3 handler and breaks the model's
        # first boto3.client("s3") with a duplicate upload_file injection error.
        boto3.DEFAULT_SESSION = assumed

    def _acquire_session(self, pool_timeout: float) -> str:
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
            max_sessions=self.credentials.effective_spark_connect_max_sessions,
            timeout=pool_timeout,
            polling_interval=self.polling_interval,
            session_concurrency=self.credentials.effective_spark_connect_session_concurrency,
            dpu_request=self._dpu_request,
            dpu_budget=self.credentials.effective_spark_connect_dpu_budget,
        )

    def _wait_for_endpoint(
        self, session_id: str, remaining_budget: float
    ) -> GetSessionEndpointResponseTypeDef:
        deadline_seconds = min(remaining_budget, _ENDPOINT_READY_TIMEOUT_SECONDS)

        def _poll() -> GetSessionEndpointResponseTypeDef:
            try:
                response = self.athena_client.get_session_endpoint(SessionId=session_id)
            except botocore.exceptions.ClientError as e:
                if is_session_ended_error(e):
                    raise
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

    def _get_or_create_spark(self, session_id: str) -> ConnectSparkSession:
        """pyspark 3.5's ``SparkSession.stop()`` does not release the server-side session
        (``ReleaseSession`` is Spark 4 only), and Athena rejects new Spark Connect sessions
        once too many pile up, so one client is shared per Athena session instead of one per
        model. Models on the same Athena session therefore share temp views and session conf.
        """
        spark = self._pool.get_spark(session_id)
        if spark is not None:
            LOGGER.debug(f"Reusing Spark Connect client for session {session_id}")
            return spark

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

        created = ConnectSparkSession.builder.channelBuilder(channel_builder).create()
        shared = self._pool.set_spark(session_id, created)
        if shared is not created:
            try:
                created.stop()
            except Exception as e:  # noqa: BLE001 - best-effort cleanup
                LOGGER.debug(f"Ignoring error while stopping duplicate Spark client: {e}")
        return shared

    def _attempt(
        self,
        compiled_code: str,
        attempt: int,
        pool_start: float,
    ) -> SparkConnectResult:
        pool_timeout = self.credentials.effective_spark_connect_pool_acquire_timeout
        pool_remaining = pool_timeout - (time.monotonic() - pool_start)
        if pool_remaining <= 0:
            raise DbtRuntimeError(
                f"Spark Connect session pool acquire timed out after {pool_timeout} seconds."
            )
        session_id = self._acquire_session(pool_remaining)

        attempt_start = time.monotonic()
        timeout_event = threading.Event()
        terminate_session = False

        try:
            spark = self._get_or_create_spark(session_id)

            budget = self.timeout - (time.monotonic() - attempt_start)
            if budget <= 0:
                raise DbtRuntimeError(
                    f"Spark Connect execution timed out after {self.timeout} seconds."
                )

            guard = _ExecutionGuard(
                spark=spark,
                session_id=session_id,
                relation_name=self.relation_name,
                timeout=self.timeout,
                budget=budget,
                keepalive_interval=self.credentials.effective_spark_connect_keepalive_interval,
                timeout_event=timeout_event,
            )
            with guard:
                exec_globals: Dict[str, Any] = {"spark": spark}
                try:
                    exec(compiled_code, exec_globals)  # noqa: S102 - user model code
                except DbtRuntimeError:
                    raise
                except Exception as e:
                    # Classify before the guard's cleanup so a watchdog firing
                    # during cleanup cannot turn this failure into a timeout.
                    raise self._failure_for(e, session_id, attempt, timeout_event) from e
            return SparkConnectResult(SparkConnect=True, SparkSessionId=session_id)
        except _TransientAttemptFailure:
            terminate_session = True
            raise
        except DbtRuntimeError:
            raise
        except Exception as e:
            failure = self._failure_for(e, session_id, attempt, timeout_event)
            terminate_session = isinstance(failure, _TransientAttemptFailure)
            raise failure from e
        finally:
            # The client stays bound to the Athena session; the pool stops it
            # when the session is terminated or evicted.
            if terminate_session:
                self._pool.terminate(session_id)
            else:
                self._pool.release(session_id)

    def _failure_for(
        self, e: Exception, session_id: str, attempt: int, timeout_event: threading.Event
    ) -> Exception:
        if timeout_event.is_set():
            return DbtRuntimeError(
                f"Spark Connect execution timed out after {self.timeout} seconds."
            )
        return self._classify_failure(e, session_id, attempt)

    def _classify_failure(
        self, e: Exception, session_id: str, attempt: int
    ) -> Union[DbtRuntimeError, _TransientAttemptFailure]:
        category = classify_transient_spark_error(e)
        total_attempts = self._total_attempts
        is_last_attempt = attempt >= total_attempts

        session_ended = (
            is_grpc_permission_denied(e) or is_session_ended_error(e)
        ) and not self._pool.is_session_alive(session_id)
        if session_ended:
            category = SESSION_ENDED
        retryable = category in self.credentials.effective_spark_connect_retry_on

        if not retryable or is_last_attempt:
            LOGGER.error(
                f"Model {self.relation_name} (session {session_id}) - "
                f"Spark Connect execution failed "
                f"(attempt {attempt}/{total_attempts}, "
                f"transient category: {category}"
                f"{'' if retryable or category is None else ', excluded by spark_connect_retry_on'}): "
                f"{type(e).__name__}: {e}\n{traceback.format_exc()}"
            )

        if category is None:
            return DbtRuntimeError(
                f"Spark Connect execution failed (session {session_id}): "
                f"{type(e).__name__}: {e}"
            )
        return _TransientAttemptFailure(e, session_id, session_ended, retryable, category)
