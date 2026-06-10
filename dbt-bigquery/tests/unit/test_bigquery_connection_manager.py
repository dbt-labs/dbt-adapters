import json
import unittest
from requests.exceptions import ConnectionError
from unittest.mock import patch, MagicMock, Mock, ANY

import dbt.adapters
import google.cloud.bigquery
from dbt.adapters.bigquery import BigQueryCredentials
from dbt.adapters.bigquery import BigQueryRelation
from dbt.adapters.bigquery.connections import BigQueryConnectionManager
from dbt.adapters.bigquery.retry import _TerminalJobAwarePredicate, RetryFactory


class TestBigQueryConnectionManager(unittest.TestCase):
    def setUp(self):
        self.credentials = Mock(BigQueryCredentials)
        self.credentials.method = "oauth"
        self.credentials.job_retries = 1
        self.credentials.job_retry_deadline_seconds = 1
        self.credentials.scopes = tuple()
        self.credentials.job_execution_timeout_seconds = 1

        self.mock_client = Mock(google.cloud.bigquery.Client)

        self.mock_connection = MagicMock()
        self.mock_connection.name = "test_connection"  # Must be a string for fire_event
        self.mock_connection.handle = self.mock_client
        self.mock_connection.credentials = self.credentials
        self.mock_connection._bq_model_location = None

        self.connections = BigQueryConnectionManager(
            profile=Mock(credentials=self.credentials, query_comment=None),
            mp_context=Mock(),
        )
        self.connections.get_thread_connection = lambda: self.mock_connection

    @patch(
        "dbt.adapters.bigquery.retry.create_bigquery_client",
        return_value=Mock(google.cloud.bigquery.Client),
    )
    def test_retry_connection_reset(self, mock_client_factory):
        new_mock_client = mock_client_factory.return_value

        @self.connections._retry.create_reopen_with_deadline(self.mock_connection)
        def generate_connection_reset_error():
            raise ConnectionResetError

        assert self.mock_connection.handle is self.mock_client

        with self.assertRaises(ConnectionResetError):
            # this will always raise the error, we just want to test that the connection was reopening in between
            generate_connection_reset_error()

        assert self.mock_connection.handle is new_mock_client
        assert new_mock_client is not self.mock_client

    def test_is_retryable(self):
        _is_retryable = google.cloud.bigquery.retry._job_should_retry
        exceptions = dbt.adapters.bigquery.impl.google.cloud.exceptions
        internal_server_error = exceptions.InternalServerError("code broke")
        bad_request_error = exceptions.BadRequest("code broke")
        connection_error = ConnectionError("code broke")
        client_error = exceptions.ClientError("bad code")
        rate_limit_error = exceptions.Forbidden(
            "code broke", errors=[{"reason": "rateLimitExceeded"}]
        )
        service_unavailable_error = exceptions.ServiceUnavailable("service is unavailable")

        self.assertTrue(_is_retryable(internal_server_error))
        self.assertFalse(
            _is_retryable(bad_request_error)
        )  # this was removed after initially being included
        self.assertTrue(_is_retryable(connection_error))
        self.assertFalse(_is_retryable(client_error))
        self.assertTrue(_is_retryable(rate_limit_error))
        self.assertTrue(_is_retryable(service_unavailable_error))

    def test_drop_dataset(self):
        mock_table = Mock()
        mock_table.reference = "table1"
        self.mock_client.list_tables.return_value = [mock_table]

        self.connections.drop_dataset("project", "dataset")

        self.mock_client.list_tables.assert_not_called()
        self.mock_client.delete_table.assert_not_called()
        self.mock_client.delete_dataset.assert_called_once()

    @patch("dbt.adapters.bigquery.connections.QueryJobConfig")
    def test_query_and_results(self, MockQueryJobConfig):
        self.mock_client.query.return_value = Mock(job_id="1")
        self.connections._query_and_results(
            self.mock_connection,
            "sql",
            {"dry_run": True},
            job_id=1,
        )

        MockQueryJobConfig.assert_called_once()
        self.mock_client.query.assert_called_once_with(
            query="sql",
            job_config=MockQueryJobConfig(),
            job_id=1,
            job_retry=None,
            timeout=self.credentials.job_creation_timeout_seconds,
            location=None,
        )

    def test_copy_bq_table_appends(self):
        self._copy_table(write_disposition=dbt.adapters.bigquery.impl.WRITE_APPEND)
        self.mock_client.copy_table.assert_called_once_with(
            [self._table_ref("project", "dataset", "table1")],
            self._table_ref("project", "dataset", "table2"),
            job_config=ANY,
            retry=ANY,
        )
        args, kwargs = self.mock_client.copy_table.call_args
        self.assertEqual(
            kwargs["job_config"].write_disposition, dbt.adapters.bigquery.impl.WRITE_APPEND
        )

    def test_copy_bq_table_truncates(self):
        self._copy_table(write_disposition=dbt.adapters.bigquery.impl.WRITE_TRUNCATE)
        args, kwargs = self.mock_client.copy_table.call_args
        self.mock_client.copy_table.assert_called_once_with(
            [self._table_ref("project", "dataset", "table1")],
            self._table_ref("project", "dataset", "table2"),
            job_config=ANY,
            retry=ANY,
        )
        args, kwargs = self.mock_client.copy_table.call_args
        self.assertEqual(
            kwargs["job_config"].write_disposition, dbt.adapters.bigquery.impl.WRITE_TRUNCATE
        )

    def test_job_labels_valid_json(self):
        expected = {"key": "value"}
        labels = self.connections._labels_from_query_comment(json.dumps(expected))
        self.assertEqual(labels, expected)

    def test_job_labels_invalid_json(self):
        labels = self.connections._labels_from_query_comment("not json")
        self.assertEqual(labels, {"query_comment": "not_json"})

    def test_list_dataset_correctly_calls_lists_datasets(self):
        mock_dataset = Mock(dataset_id="d1")
        mock_list_dataset = Mock(return_value=[mock_dataset])
        self.mock_client.list_datasets = mock_list_dataset
        result = self.connections.list_dataset("project")
        self.mock_client.list_datasets.assert_called_once_with(
            project="project", max_results=10000, retry=ANY
        )
        assert result == ["d1"]

    def _table_ref(self, proj, ds, table):
        return self.connections.table_ref(proj, ds, table)

    def _copy_table(self, write_disposition):
        source = BigQueryRelation.create(database="project", schema="dataset", identifier="table1")
        destination = BigQueryRelation.create(
            database="project", schema="dataset", identifier="table2"
        )
        self.connections.copy_bq_table(source, destination, write_disposition)

    @patch("dbt.adapters.bigquery.connections.QueryJobConfig")
    def test_raw_execute_retries_with_fresh_job_id(self, MockQueryJobConfig):
        exceptions = dbt.adapters.bigquery.impl.google.cloud.exceptions
        job_ids_used = []

        def capture_job_id(*args, **kwargs):
            job_ids_used.append(kwargs.get("job_id"))
            if len(job_ids_used) < 2:
                raise exceptions.ServiceUnavailable("Service unavailable")
            mock_job = Mock(job_id=kwargs.get("job_id"), location="US", project="project")
            mock_job.result.return_value = iter([])
            return mock_job

        self.mock_client.query.side_effect = capture_job_id
        self.connections.raw_execute("SELECT 1")
        self.assertEqual(self.mock_client.query.call_count, 2)
        self.assertNotEqual(job_ids_used[0], job_ids_used[1])

    @patch("dbt.adapters.bigquery.connections.QueryJobConfig")
    def test_raw_execute_no_retry_on_non_retryable_error(self, MockQueryJobConfig):
        exceptions = dbt.adapters.bigquery.impl.google.cloud.exceptions
        self.mock_client.query.side_effect = exceptions.BadRequest("Syntax error")
        from dbt_common.exceptions import DbtDatabaseError

        with self.assertRaises(DbtDatabaseError):
            self.connections.raw_execute("SELECT * FORM table")
        self.assertEqual(self.mock_client.query.call_count, 1)

    @patch("dbt.adapters.bigquery.connections.QueryJobConfig")
    def test_result_failure_triggers_retry(self, MockQueryJobConfig):
        exceptions = dbt.adapters.bigquery.impl.google.cloud.exceptions
        call_count = {"query": 0}

        def make_query_job(*args, **kwargs):
            call_count["query"] += 1
            mock_job = Mock(job_id=f"job_{call_count['query']}", location="US", project="project")
            if call_count["query"] == 1:
                mock_job.result.side_effect = exceptions.ServiceUnavailable("Service unavailable")
            else:
                mock_job.result.return_value = iter([])
            return mock_job

        self.mock_client.query.side_effect = make_query_job
        self.connections.raw_execute("SELECT 1")
        self.assertEqual(self.mock_client.query.call_count, 2)

    @patch("dbt.adapters.bigquery.connections.QueryJobConfig")
    def test_query_and_results_passes_timeout_to_result(self, MockQueryJobConfig):
        """Test that _query_and_results passes a timeout to query_job.result()"""
        mock_job = Mock(job_id="test_job", location="US", project="project")
        mock_job.result.return_value = iter([])
        self.mock_client.query.return_value = mock_job

        self.connections._query_and_results(
            self.mock_connection,
            "SELECT 1",
            {"dry_run": False},
            job_id="test_job",
        )

        # Verify result() was called with a timeout parameter
        mock_job.result.assert_called_once()
        call_kwargs = mock_job.result.call_args[1]
        self.assertIn("timeout", call_kwargs)
        # timeout should be job_execution_timeout (1) + 30 second buffer = 31
        self.assertEqual(call_kwargs["timeout"], 31)

    @patch("dbt.adapters.bigquery.connections.QueryJobConfig")
    def test_query_and_results_polling_timeout_includes_buffer(self, MockQueryJobConfig):
        """Test that the polling timeout is job_execution_timeout + 30 seconds buffer"""
        # Set a specific job_execution_timeout and recreate the connection manager
        self.credentials.job_execution_timeout_seconds = 120
        connections = BigQueryConnectionManager(
            profile=Mock(credentials=self.credentials, query_comment=None),
            mp_context=Mock(),
        )
        connections.get_thread_connection = lambda: self.mock_connection

        mock_job = Mock(job_id="test_job", location="US", project="project")
        mock_job.result.return_value = iter([])
        self.mock_client.query.return_value = mock_job

        connections._query_and_results(
            self.mock_connection,
            "SELECT 1",
            {"dry_run": False},
            job_id="test_job",
        )

        call_kwargs = mock_job.result.call_args[1]
        # timeout should be 120 + 30 = 150
        self.assertEqual(call_kwargs["timeout"], 150)

    @patch("dbt.adapters.bigquery.connections.QueryJobConfig")
    def test_retryable_google_api_error_is_reraised(self, MockQueryJobConfig):
        """Test that retryable GoogleAPICallError is re-raised for retry mechanism"""
        exceptions = dbt.adapters.bigquery.impl.google.cloud.exceptions

        mock_job = Mock(job_id="test_job", location="US", project="project")
        # ServiceUnavailable is a retryable error
        mock_job.result.side_effect = exceptions.ServiceUnavailable("Service unavailable")
        self.mock_client.query.return_value = mock_job

        # Should raise ServiceUnavailable, not DbtDatabaseError
        with self.assertRaises(exceptions.ServiceUnavailable):
            self.connections._query_and_results(
                self.mock_connection,
                "SELECT 1",
                {"dry_run": False},
                job_id="test_job",
            )

    @patch("dbt.adapters.bigquery.connections.QueryJobConfig")
    def test_query_and_results_uses_model_timeout_from_job_params(self, MockQueryJobConfig):
        """Test that _query_and_results uses job_timeout_ms from job_params when set"""
        mock_job = Mock(job_id="test_job", location="US", project="project")
        mock_job.result.return_value = iter([])
        self.mock_client.query.return_value = mock_job

        # Pass model-level timeout via job_params (as raw_execute would)
        self.connections._query_and_results(
            self.mock_connection,
            "SELECT 1",
            {"dry_run": False, "job_timeout_ms": 60000},
            job_id="test_job",
        )

        call_kwargs = mock_job.result.call_args[1]
        # polling timeout should be model timeout (60) + 30 second buffer = 90
        self.assertEqual(call_kwargs["timeout"], 90)

    @patch("dbt.adapters.bigquery.connections.QueryJobConfig")
    def test_query_and_results_falls_back_to_profile_timeout(self, MockQueryJobConfig):
        """Test that _query_and_results falls back to profile-level timeout when no model timeout"""
        mock_job = Mock(job_id="test_job", location="US", project="project")
        mock_job.result.return_value = iter([])
        self.mock_client.query.return_value = mock_job

        self.connections._query_and_results(
            self.mock_connection,
            "SELECT 1",
            {"dry_run": False},
            job_id="test_job",
        )

        call_kwargs = mock_job.result.call_args[1]
        # profile timeout is 1, so polling timeout = 1 + 30 = 31
        self.assertEqual(call_kwargs["timeout"], 31)

    @patch("dbt.adapters.bigquery.connections.QueryJobConfig")
    def test_query_and_results_uses_polling_retry(self, MockQueryJobConfig):
        """query_job.result() should receive a retry built from create_query_job_polling_retry,
        not a raw DEFAULT_JOB_RETRY.with_timeout(execution_timeout)."""
        mock_job = Mock(job_id="test_job", location="US", project="project")
        mock_job.result.return_value = iter([])
        self.mock_client.query.return_value = mock_job

        with patch.object(
            self.connections._retry,
            "create_query_job_polling_retry",
            wraps=self.connections._retry.create_query_job_polling_retry,
        ) as spy:
            self.connections._query_and_results(
                self.mock_connection,
                "SELECT 1",
                {"dry_run": False},
                job_id="test_job",
            )
            spy.assert_called_once_with(mock_job)

        call_kwargs = mock_job.result.call_args[1]
        self.assertIn("retry", call_kwargs)

    def test_copy_bq_table_respects_model_timeout(self):
        """Test that copy_bq_table uses the model-level timeout when set"""
        mock_copy_job = Mock()
        self.mock_client.copy_table.return_value = mock_copy_job

        # Set model timeout on the connection object (as pre_model_hook would)
        self.mock_connection._bq_model_timeout = 45.0

        source = BigQueryRelation.create(database="project", schema="dataset", identifier="table1")
        destination = BigQueryRelation.create(
            database="project", schema="dataset", identifier="table2"
        )
        self.connections.copy_bq_table(
            source, destination, dbt.adapters.bigquery.impl.WRITE_TRUNCATE
        )

        # Verify copy_job.result was called with the model timeout
        mock_copy_job.result.assert_called_once_with(timeout=45.0)

        # Clean up
        self.mock_connection._bq_model_timeout = None

    @patch("dbt.adapters.bigquery.connections.QueryJobConfig")
    def test_raw_execute_passes_model_location_to_client_query(self, MockQueryJobConfig):
        """When _bq_model_location is set, raw_execute passes location to client.query()."""
        mock_job = Mock(job_id="test_job", location="US", project="project")
        mock_job.result.return_value = iter([])
        self.mock_client.query.return_value = mock_job

        self.mock_connection._bq_model_location = "US"
        try:
            self.connections.raw_execute("SELECT 1")
        finally:
            self.mock_connection._bq_model_location = None

        _, kwargs = self.mock_client.query.call_args
        self.assertEqual(kwargs.get("location"), "US")

    @patch("dbt.adapters.bigquery.connections.QueryJobConfig")
    def test_raw_execute_passes_none_location_when_not_set(self, MockQueryJobConfig):
        """When _bq_model_location is absent, client.query() receives location=None (client default applies)."""
        mock_job = Mock(job_id="test_job", location="EU", project="project")
        _, kwargs = self.mock_client.query.call_args
        self.assertIsNone(kwargs.get("location"))

    def test_raw_execute_uses_credential_reservation(self, MockQueryJobConfig):
        """Test that reservation from credentials flows into job_params"""
        self.credentials.reservation = (
            "projects/project1/locations/US/reservations/test-reservation"
        )
        self.credentials.maximum_bytes_billed = None
        self.mock_connection._bq_model_reservation = None
        mock_job = Mock(job_id="job1", location="US", project="project1")
        mock_job.result.return_value = iter([])
        self.mock_client.query.return_value = mock_job

        self.connections.raw_execute("SELECT 1")

        call_kwargs = MockQueryJobConfig.call_args[1]
        self.assertEqual(
            call_kwargs["reservation"],
            "projects/project1/locations/US/reservations/test-reservation",
        )

    @patch("dbt.adapters.bigquery.connections.QueryJobConfig")
    def test_raw_execute_uses_model_reservation(self, MockQueryJobConfig):
        """Test that a model-level reservation stored on the connection flows into job_params"""
        self.credentials.reservation = None
        self.credentials.maximum_bytes_billed = None
        self.mock_connection._bq_model_reservation = (
            "projects/project1/locations/US/reservations/model-reservation"
        )
        mock_job = Mock(job_id="job1", location="US", project="project1")
        mock_job.result.return_value = iter([])
        self.mock_client.query.return_value = mock_job

        self.connections.raw_execute("SELECT 1")

        call_kwargs = MockQueryJobConfig.call_args[1]
        self.assertEqual(
            call_kwargs["reservation"],
            "projects/project1/locations/US/reservations/model-reservation",
        )
        # Clean up
        self.mock_connection._bq_model_reservation = None

    @patch("dbt.adapters.bigquery.connections.QueryJobConfig")
    def test_raw_execute_model_reservation_overrides_credential(self, MockQueryJobConfig):
        """Test that model-level reservation takes priority over credential-level"""
        self.credentials.reservation = (
            "projects/project1/locations/US/reservations/credential-reservation"
        )
        self.credentials.maximum_bytes_billed = None
        self.mock_connection._bq_model_reservation = (
            "projects/project1/locations/US/reservations/model-reservation"
        )
        mock_job = Mock(job_id="job1", location="US", project="project1")
        mock_job.result.return_value = iter([])
        self.mock_client.query.return_value = mock_job

        self.connections.raw_execute("SELECT 1")

        call_kwargs = MockQueryJobConfig.call_args[1]
        self.assertEqual(
            call_kwargs["reservation"],
            "projects/project1/locations/US/reservations/model-reservation",
        )
        # Clean up
        self.mock_connection._bq_model_reservation = None

    @patch("dbt.adapters.bigquery.connections.QueryJobConfig")
    def test_raw_execute_no_reservation_when_not_set(self, MockQueryJobConfig):
        """Test that reservation is absent from job_params when not configured"""
        self.credentials.reservation = None
        self.credentials.maximum_bytes_billed = None
        self.mock_connection._bq_model_reservation = None
        mock_job = Mock(job_id="job1", location="US", project="project1")
        mock_job.result.return_value = iter([])
        self.mock_client.query.return_value = mock_job

        self.connections.raw_execute("SELECT 1")

        call_kwargs = MockQueryJobConfig.call_args[1]
        self.assertNotIn("reservation", call_kwargs)


class TestTerminalJobAwarePredicate(unittest.TestCase):
    """Unit tests for the _TerminalJobAwarePredicate used in query_job.result() polling."""

    def _make_internal_error(self):
        """Reproduce the Tyson error: 400 with reason=internalError."""
        exceptions = dbt.adapters.bigquery.impl.google.cloud.exceptions
        return exceptions.BadRequest(
            "internal error during execution",
            errors=[{"reason": "internalError"}],
        )

    def _make_non_retryable_error(self):
        exceptions = dbt.adapters.bigquery.impl.google.cloud.exceptions
        return exceptions.BadRequest("syntax error", errors=[{"reason": "invalidQuery"}])

    def _make_rate_limit_error(self):
        """Reproduce the reported getQueryResults rate-limit error."""
        exceptions = dbt.adapters.bigquery.impl.google.cloud.exceptions
        return exceptions.Forbidden(
            "Exceeded rate limits: too many api requests per user per method",
            errors=[{"reason": "rateLimitExceeded"}],
        )

    def test_short_circuits_on_terminal_failed_job(self):
        """Core regression test: internalError on a DONE+failed job must not retry."""
        mock_job = Mock()
        mock_job.job_id = "job_abc"
        mock_job.state = "DONE"
        mock_job.error_result = {"reason": "internalError", "message": "internal error"}

        predicate = _TerminalJobAwarePredicate(mock_job, retries=3)
        result = predicate(self._make_internal_error())

        self.assertFalse(result)
        mock_job.reload.assert_called_once()

    def test_retries_when_job_still_running(self):
        """A retryable error on a still-running job should be retried."""
        mock_job = Mock()
        mock_job.job_id = "job_abc"
        mock_job.state = "RUNNING"
        mock_job.error_result = None

        predicate = _TerminalJobAwarePredicate(mock_job, retries=3)
        result = predicate(self._make_internal_error())

        self.assertTrue(result)
        mock_job.reload.assert_called_once()

    def test_continues_retrying_on_running_job(self):
        """Predicate should continue allowing retries while the job is still running."""
        mock_job = Mock()
        mock_job.job_id = "job_abc"
        mock_job.state = "RUNNING"
        mock_job.error_result = None

        predicate = _TerminalJobAwarePredicate(mock_job, retries=1)

        self.assertTrue(predicate(self._make_internal_error()))
        self.assertTrue(predicate(self._make_internal_error()))
        self.assertTrue(predicate(self._make_internal_error()))

    def test_no_retry_when_retries_zero(self):
        """job_retries=0 means never retry, and skips the jobs.get reload call."""
        mock_job = Mock()
        mock_job.state = "RUNNING"
        mock_job.error_result = None

        predicate = _TerminalJobAwarePredicate(mock_job, retries=0)
        result = predicate(self._make_internal_error())

        self.assertFalse(result)
        mock_job.reload.assert_not_called()

    def test_non_retryable_error_skips_reload(self):
        """Non-retryable errors should bail out immediately without calling job.reload()."""
        mock_job = Mock()

        predicate = _TerminalJobAwarePredicate(mock_job, retries=3)
        result = predicate(self._make_non_retryable_error())

        self.assertFalse(result)
        mock_job.reload.assert_not_called()

    def test_rate_limit_error_retries_without_reload(self):
        """A rate-limit error says nothing about job health: retry without the
        extra jobs.get reload, so we don't add pressure to the rate limit."""
        mock_job = Mock()
        mock_job.job_id = "job_abc"
        mock_job.state = "RUNNING"
        mock_job.error_result = None

        predicate = _TerminalJobAwarePredicate(mock_job, retries=3)
        result = predicate(self._make_rate_limit_error())

        self.assertTrue(result)
        mock_job.reload.assert_not_called()

    def test_retry_path_logs_debug(self):
        """The continue-polling path should emit a debug line for observability."""
        mock_job = Mock()
        mock_job.job_id = "job_abc"
        mock_job.state = "RUNNING"
        mock_job.error_result = None

        predicate = _TerminalJobAwarePredicate(mock_job, retries=3)

        with patch("dbt.adapters.bigquery.retry._logger") as mock_logger:
            result = predicate(self._make_rate_limit_error())

        self.assertTrue(result)
        mock_logger.debug.assert_called_once()

    def test_reload_expected_api_error_logs_warning(self):
        """Expected API errors during reload() log at warning and fall through."""
        from google.api_core.exceptions import InternalServerError

        mock_job = Mock()
        mock_job.job_id = "job_abc"
        mock_job.reload.side_effect = InternalServerError("jobs.get is flaky")

        predicate = _TerminalJobAwarePredicate(mock_job, retries=3)

        with patch("dbt.adapters.bigquery.retry._logger") as mock_logger:
            result = predicate(self._make_internal_error())

        self.assertTrue(result)
        mock_logger.warning.assert_called_once()

    def test_reload_transport_error_logs_warning(self):
        """Expected transport errors during reload() log at warning and fall through."""
        from requests.exceptions import ConnectionError as RequestsConnectionError

        mock_job = Mock()
        mock_job.job_id = "job_abc"
        mock_job.reload.side_effect = RequestsConnectionError("connection reset")

        predicate = _TerminalJobAwarePredicate(mock_job, retries=3)

        with patch("dbt.adapters.bigquery.retry._logger") as mock_logger:
            result = predicate(self._make_internal_error())

        self.assertTrue(result)
        mock_logger.warning.assert_called_once()

    def test_reload_unexpected_error_logs_warning(self):
        """Unexpected exceptions during reload() (auth bugs, programming errors)
        log at warning level but still fall through to retry rather than crashing."""
        mock_job = Mock()
        mock_job.job_id = "job_abc"
        mock_job.reload.side_effect = ValueError("programming error")

        predicate = _TerminalJobAwarePredicate(mock_job, retries=3)

        with patch("dbt.adapters.bigquery.retry._logger") as mock_logger:
            result = predicate(self._make_internal_error())

        self.assertTrue(result)
        mock_logger.warning.assert_called_once()

    def test_terminal_job_wins_over_remaining_retries(self):
        """Even if retries budget remains, terminal job state stops polling."""
        mock_job = Mock()
        mock_job.job_id = "job_abc"
        mock_job.state = "DONE"
        mock_job.error_result = {"reason": "internalError"}

        predicate = _TerminalJobAwarePredicate(mock_job, retries=10)
        result = predicate(self._make_internal_error())

        self.assertFalse(result)


class TestRetryFactoryPollingRetry(unittest.TestCase):
    """Behavior tests for RetryFactory.create_query_job_polling_retry.

    These tests exercise the returned Retry object against a stub callable
    rather than asserting on private Retry internals (e.g. ``_deadline``,
    ``_predicate``), which are not part of google-api-core's public API.
    """

    def _make_credentials(
        self, job_retries=3, job_retry_deadline_seconds=None, job_execution_timeout_seconds=28800
    ):
        creds = Mock(spec=BigQueryCredentials)
        creds.job_retries = job_retries
        creds.job_retry_deadline_seconds = job_retry_deadline_seconds
        creds.job_execution_timeout_seconds = job_execution_timeout_seconds
        creds.job_creation_timeout_seconds = 60
        return creds

    def _make_internal_error(self):
        exceptions = dbt.adapters.bigquery.impl.google.cloud.exceptions
        return exceptions.BadRequest(
            "internal error during execution",
            errors=[{"reason": "internalError"}],
        )

    def _make_rate_limit_error(self):
        """Reproduce the reported getQueryResults rate-limit error."""
        exceptions = dbt.adapters.bigquery.impl.google.cloud.exceptions
        return exceptions.Forbidden(
            "Exceeded rate limits: too many api requests per user per method "
            "for this user_method (JobService.getQueryResults)",
            errors=[{"reason": "rateLimitExceeded"}],
        )

    def test_rate_limit_on_running_job_retries_beyond_job_retries_then_succeeds(self):
        """Regression test for #1957: transient rateLimitExceeded errors on a
        still-running job must be retried past the job_retries count and allowed
        to succeed, rather than surfacing as a hard failure after job_retries.
        """
        creds = self._make_credentials(job_retries=1)
        factory = RetryFactory(creds)

        mock_job = Mock()
        mock_job.job_id = "job_abc"
        mock_job.state = "RUNNING"
        mock_job.error_result = None

        retry = factory.create_query_job_polling_retry(mock_job)
        err = self._make_rate_limit_error()

        call_count = {"n": 0}

        def fail_then_succeed():
            call_count["n"] += 1
            # Fail more times than job_retries (1) before succeeding.
            if call_count["n"] <= 5:
                raise err
            return "results"

        # Patch sleep so exponential backoff doesn't slow the test down.
        with patch("time.sleep"):
            result = retry(fail_then_succeed)()

        self.assertEqual(result, "results")
        self.assertEqual(
            call_count["n"],
            6,
            "Transient rate-limit errors must retry past job_retries until success",
        )

    def test_internal_error_on_running_job_retries_beyond_job_retries(self):
        """A still-running job hitting transient internalError is retried well
        beyond the job_retries count (no attempt cap on the polling retry)."""
        creds = self._make_credentials(job_retries=2)
        factory = RetryFactory(creds)

        mock_job = Mock()
        mock_job.job_id = "job_abc"
        mock_job.state = "RUNNING"
        mock_job.error_result = None

        retry = factory.create_query_job_polling_retry(mock_job)
        err = self._make_internal_error()

        call_count = {"n": 0}

        def fail_then_succeed():
            call_count["n"] += 1
            if call_count["n"] <= 4:  # > 1 initial + 2 retries
                raise err
            return "results"

        with patch("time.sleep"):
            result = retry(fail_then_succeed)()

        self.assertEqual(result, "results")
        self.assertGreater(call_count["n"], 3)

    def test_retry_short_circuits_on_terminal_failed_job(self):
        """Tyson regression: a terminal failed job should fail on the first attempt."""
        creds = self._make_credentials(job_retries=5)
        factory = RetryFactory(creds)

        mock_job = Mock()
        mock_job.job_id = "job_abc"
        mock_job.state = "DONE"
        mock_job.error_result = {"reason": "internalError"}

        retry = factory.create_query_job_polling_retry(mock_job)
        exceptions = dbt.adapters.bigquery.impl.google.cloud.exceptions
        err = self._make_internal_error()

        call_count = {"n": 0}

        def always_fail():
            call_count["n"] += 1
            raise err

        with self.assertRaises(exceptions.BadRequest):
            retry(always_fail)()

        self.assertEqual(call_count["n"], 1, "Terminal-failed job should not be retried at all")

    @patch("dbt.adapters.bigquery.retry.DEFAULT_JOB_RETRY")
    def test_factory_passes_job_retry_deadline_seconds_to_retry(self, mock_retry):
        """Deadline plumbing: job_retry_deadline_seconds (not job_execution_timeout_seconds)
        is what gets passed to Retry.with_deadline().

        Mocks at the SDK boundary (the public with_predicate / with_deadline
        chain) instead of reading private Retry internals like ``_deadline``.
        """
        creds = self._make_credentials(
            job_retry_deadline_seconds=300,
            job_execution_timeout_seconds=28800,
        )
        factory = RetryFactory(creds)
        factory.create_query_job_polling_retry(Mock())

        chained = mock_retry.with_predicate.return_value
        chained.with_deadline.assert_called_once_with(300)
        chained.with_deadline.return_value.with_delay.assert_called_once_with(
            initial=5.0,
            maximum=60.0,
            multiplier=2.0,
        )

    @patch("dbt.adapters.bigquery.retry.DEFAULT_JOB_RETRY")
    def test_factory_falls_back_to_default_deadline(self, mock_retry):
        """When job_retry_deadline_seconds is unset, default to the 600s fallback,
        NOT to job_execution_timeout_seconds.
        """
        creds = self._make_credentials(
            job_retry_deadline_seconds=None,
            job_execution_timeout_seconds=28800,
        )
        factory = RetryFactory(creds)
        factory.create_query_job_polling_retry(Mock())

        chained = mock_retry.with_predicate.return_value
        chained.with_deadline.assert_called_once_with(600)
        chained.with_deadline.return_value.with_delay.assert_called_once_with(
            initial=5.0,
            maximum=60.0,
            multiplier=2.0,
        )
