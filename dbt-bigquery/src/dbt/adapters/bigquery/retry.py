from typing import Any, Callable, Optional

from google.api_core.exceptions import GoogleAPICallError, RetryError
from google.api_core.future.polling import DEFAULT_POLLING
from google.api_core.retry import Retry
from google.cloud.bigquery.retry import DEFAULT_JOB_RETRY, _job_should_retry
from requests.exceptions import ConnectionError, RequestException

from dbt.adapters.contracts.connection import Connection, ConnectionState
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.exceptions.connection import FailedToConnectError

from dbt.adapters.bigquery.clients import create_bigquery_client
from dbt.adapters.bigquery.credentials import BigQueryCredentials


_logger = AdapterLogger("BigQuery")

_MINUTE = 60.0
_DAY = 24 * 60 * 60.0
_DEFAULT_POLLING_RETRY_DEADLINE = 10 * _MINUTE

# Throttle reasons that say nothing about job health: the job is fine, BQ is
# just busy. We back these off without a jobs.get reload to avoid piling extra
# API pressure onto a getQueryResults rate-limit storm.
_RATE_LIMIT_REASONS = frozenset({"rateLimitExceeded", "jobRateLimitExceeded"})


def _is_rate_limit_error(error: Exception) -> bool:
    if isinstance(error, RetryError):
        error = error.cause
    errors = getattr(error, "errors", None)
    if not errors:
        return False
    return errors[0].get("reason") in _RATE_LIMIT_REASONS


class RetryFactory:

    def __init__(self, credentials: BigQueryCredentials) -> None:
        self._retries = credentials.job_retries or 0
        self._job_creation_timeout = credentials.job_creation_timeout_seconds
        self._job_execution_timeout = credentials.job_execution_timeout_seconds
        self._job_deadline = credentials.job_retry_deadline_seconds

    def create_job_creation_timeout(self, fallback: float = _MINUTE) -> float:
        return (
            self._job_creation_timeout or fallback
        )  # keep _MINUTE here so it's not overridden by passing fallback=None

    def create_job_execution_timeout(self, fallback: float = _DAY) -> float:
        return (
            self._job_execution_timeout or fallback
        )  # keep _DAY here so it's not overridden by passing fallback=None

    def create_retry(self, fallback: Optional[float] = None) -> Retry:
        return DEFAULT_JOB_RETRY.with_timeout(self._job_execution_timeout or fallback or _DAY)

    def create_polling(self, model_timeout: Optional[float] = None) -> Retry:
        return DEFAULT_POLLING.with_timeout(model_timeout or self._job_execution_timeout or _DAY)

    def create_reopen_with_deadline(self, connection: Connection) -> Retry:
        """
        This strategy mimics what was accomplished with _retry_and_handle
        """

        retry = DEFAULT_JOB_RETRY.with_delay(maximum=3.0).with_predicate(
            _DeferredException(self._retries)
        )

        # there is no `with_on_error` method, but we want to retain the defaults on `DEFAULT_JOB_RETRY
        retry._on_error = _create_reopen_on_error(connection)

        # don't override the default deadline to None if the user did not provide one,
        # the process will never end
        if deadline := self._job_deadline:
            return retry.with_deadline(deadline)

        return retry

    def create_query_job_polling_retry(self, query_job: Any) -> Retry:
        """
        Build a retry for query_job.result() polling that:
        - Uses job_retry_deadline_seconds (default 10 min) as the retry budget,
          decoupled from job_execution_timeout_seconds so that a long-running job
          does not silently burn the full execution timeout on getQueryResults errors.
        - Uses a slower retry cadence to dampen burst pressure on getQueryResults
          when many jobs are polling concurrently.
        - Short-circuits immediately when the underlying BQ job has reached a
          terminal failed state (state == DONE + error_result), avoiding the
          scenario where a permanently-dead job is polled for hours.
        """
        deadline = self._job_deadline or _DEFAULT_POLLING_RETRY_DEADLINE
        predicate = _TerminalJobAwarePredicate(query_job, self._retries)
        return (
            DEFAULT_JOB_RETRY.with_predicate(predicate)
            .with_deadline(deadline)
            .with_delay(initial=5.0, maximum=60.0, multiplier=2.0)
        )


class _DeferredException:
    """
    Count ALL errors, not just retryable errors, up to a threshold.
    Raise the next error, regardless of whether it is retryable.
    """

    def __init__(self, retries: int) -> None:
        self._retries: int = retries
        self._error_count = 0

    def __call__(self, error: Exception) -> bool:
        # exit immediately if the user does not want retries
        if self._retries == 0:
            return False

        # count all errors
        self._error_count += 1

        # if the error is retryable, and we haven't breached the threshold, log and continue
        if _job_should_retry(error) and self._error_count <= self._retries:
            _logger.debug(
                f"Retry attempt {self._error_count} of {self._retries} after error: {repr(error)}"
            )
            return True

        # otherwise raise
        return False


class _TerminalJobAwarePredicate:
    """
    Retry predicate for query_job.result() polling.

    Short-circuits when the underlying BQ job has reached a terminal failed state
    (state == "DONE" with error_result set), even if _job_should_retry would
    otherwise consider the error retryable. This prevents dbt from polling
    getQueryResults for hours against a job that BQ has already killed.

    The authoritative jobs.get reload is only performed for ambiguous errors
    (e.g. internalError/backendError) that could indicate a dead job. Pure
    throttle errors (rateLimitExceeded) skip the reload and back off directly,
    since the job is healthy and the extra jobs.get would only add pressure to
    the rate limit being waited out.

    Retry cadence and overall budget are controlled by the enclosing Retry
    object. This predicate only decides whether polling should continue.
    """

    def __init__(self, query_job: Any, retries: int) -> None:
        self._query_job = query_job
        self._retries = retries

    def __call__(self, error: Exception) -> bool:
        # If the user opted out of retries, bail immediately. Avoids an
        # unnecessary jobs.get round-trip via query_job.reload() that can never
        # change the outcome.
        if self._retries == 0:
            return False

        if not _job_should_retry(error):
            return False

        # Rate-limit errors are healthy-job throttles (see class docstring) — skip the reload.
        if not _is_rate_limit_error(error):
            # Confirm the job isn't already terminally dead. Never raise from a
            # predicate — that crashes the whole result() call — so reload failures
            # are logged (expected vs unexpected) and fall through to retry.
            try:
                self._query_job.reload()
                if self._query_job.state == "DONE" and self._query_job.error_result:
                    _logger.debug(
                        f"Job {self._query_job.job_id} is in a terminal failed state; "
                        "stopping getQueryResults polling."
                    )
                    return False
            except (GoogleAPICallError, RequestException) as reload_error:
                _logger.warning(
                    f"Expected transient error reloading job state during retry "
                    f"predicate (falling back to standard retry logic): {reload_error}"
                )
            except Exception as reload_error:
                _logger.warning(
                    f"Unexpected error reloading job state during retry predicate "
                    f"(falling back to standard retry logic): {reload_error}"
                )

        _logger.debug(
            f"Retrying getQueryResults for job {self._query_job.job_id} "
            f"after retryable error: {repr(error)}"
        )
        return True


def _create_reopen_on_error(connection: Connection) -> Callable[[Exception], None]:

    def on_error(error: Exception):
        if isinstance(error, (ConnectionResetError, ConnectionError)):
            # Don't reopen a connection cancel_open() deliberately closed. Doing
            # so lets the retry resubmit the job and re-poll it, so a cancelled
            # run never terminates. Re-raise instead to let cancellation surface.
            if connection.state == ConnectionState.CLOSED:
                raise error
            _logger.warning("Reopening connection after {!r}".format(error))
            connection.handle.close()

            try:
                connection.handle = create_bigquery_client(connection.credentials)
                connection.state = ConnectionState.OPEN  # type:ignore

            except Exception as e:
                _logger.debug(
                    f"""Got an error when attempting to create a bigquery " "client: '{e}'"""
                )
                connection.handle = None
                connection.state = ConnectionState.FAIL  # type:ignore
                raise FailedToConnectError(str(e))

    return on_error
