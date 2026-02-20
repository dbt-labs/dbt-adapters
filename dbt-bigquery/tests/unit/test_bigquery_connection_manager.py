import json
import unittest
from requests.exceptions import ConnectionError
from unittest.mock import patch, MagicMock, Mock, ANY

import dbt.adapters
import google.cloud.bigquery

from dbt.adapters.bigquery import BigQueryCredentials
from dbt.adapters.bigquery import BigQueryRelation
from dbt.adapters.bigquery.connections import BigQueryConnectionManager


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
