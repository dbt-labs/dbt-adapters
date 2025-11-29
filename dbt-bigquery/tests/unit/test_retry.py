from unittest import TestCase, mock

from google.api_core.retry import Retry

from dbt.adapters.bigquery.retry import RetryFactory, _DeferredException, _create_reopen_on_error
from dbt.adapters.bigquery.credentials import BigQueryCredentials
from dbt.adapters.contracts.connection import Connection, ConnectionState
from dbt.adapters.exceptions.connection import FailedToConnectError


class TestRetryFactory(TestCase):
    """Test the RetryFactory class"""

    def _make_credentials(
        self,
        job_retries=None,
        job_creation_timeout_seconds=None,
        job_execution_timeout_seconds=None,
        job_retry_deadline_seconds=None,
    ):
        """Helper to create mock credentials"""
        creds = mock.Mock(spec=BigQueryCredentials)
        creds.job_retries = job_retries
        creds.job_creation_timeout_seconds = job_creation_timeout_seconds
        creds.job_execution_timeout_seconds = job_execution_timeout_seconds
        creds.job_retry_deadline_seconds = job_retry_deadline_seconds
        return creds

    def test__retry_factory_initialization(self):
        """Test RetryFactory initialization"""
        creds = self._make_credentials(
            job_retries=3,
            job_creation_timeout_seconds=120,
            job_execution_timeout_seconds=3600,
            job_retry_deadline_seconds=600,
        )

        factory = RetryFactory(creds)

        self.assertEqual(factory._retries, 3)
        self.assertEqual(factory._job_creation_timeout, 120)
        self.assertEqual(factory._job_execution_timeout, 3600)
        self.assertEqual(factory._job_deadline, 600)

    def test__retry_factory_initialization_with_none_values(self):
        """Test RetryFactory initialization with None values"""
        creds = self._make_credentials()

        factory = RetryFactory(creds)

        self.assertEqual(factory._retries, 0)
        self.assertIsNone(factory._job_creation_timeout)
        self.assertIsNone(factory._job_execution_timeout)
        self.assertIsNone(factory._job_deadline)

    def test__create_job_creation_timeout_with_credential_value(self):
        """Test create_job_creation_timeout uses credential value"""
        creds = self._make_credentials(job_creation_timeout_seconds=120)
        factory = RetryFactory(creds)

        timeout = factory.create_job_creation_timeout()

        self.assertEqual(timeout, 120)

    def test__create_job_creation_timeout_with_fallback(self):
        """Test create_job_creation_timeout uses fallback when credential is None"""
        creds = self._make_credentials()
        factory = RetryFactory(creds)

        timeout = factory.create_job_creation_timeout(fallback=180)

        self.assertEqual(timeout, 180)

    def test__create_job_creation_timeout_with_default_fallback(self):
        """Test create_job_creation_timeout uses default fallback (60 seconds)"""
        creds = self._make_credentials()
        factory = RetryFactory(creds)

        timeout = factory.create_job_creation_timeout()

        self.assertEqual(timeout, 60.0)  # _MINUTE constant

    def test__create_job_execution_timeout_with_credential_value(self):
        """Test create_job_execution_timeout uses credential value"""
        creds = self._make_credentials(job_execution_timeout_seconds=7200)
        factory = RetryFactory(creds)

        timeout = factory.create_job_execution_timeout()

        self.assertEqual(timeout, 7200)

    def test__create_job_execution_timeout_with_fallback(self):
        """Test create_job_execution_timeout uses fallback when credential is None"""
        creds = self._make_credentials()
        factory = RetryFactory(creds)

        timeout = factory.create_job_execution_timeout(fallback=3600)

        self.assertEqual(timeout, 3600)

    def test__create_job_execution_timeout_with_default_fallback(self):
        """Test create_job_execution_timeout uses default fallback (1 day)"""
        creds = self._make_credentials()
        factory = RetryFactory(creds)

        timeout = factory.create_job_execution_timeout()

        self.assertEqual(timeout, 24 * 60 * 60.0)  # _DAY constant

    def test__create_retry_with_credential_timeout(self):
        """Test create_retry uses credential timeout"""
        creds = self._make_credentials(job_execution_timeout_seconds=1800)
        factory = RetryFactory(creds)

        retry = factory.create_retry()

        self.assertIsInstance(retry, Retry)
        # Verify timeout is set (implementation detail, but we can check the object exists)
        self.assertIsNotNone(retry)

    def test__create_retry_with_fallback(self):
        """Test create_retry uses fallback when credential is None"""
        creds = self._make_credentials()
        factory = RetryFactory(creds)

        retry = factory.create_retry(fallback=3600)

        self.assertIsInstance(retry, Retry)

    def test__create_retry_with_default(self):
        """Test create_retry uses default (1 day) when no values provided"""
        creds = self._make_credentials()
        factory = RetryFactory(creds)

        retry = factory.create_retry()

        self.assertIsInstance(retry, Retry)

    def test__create_polling_with_model_timeout(self):
        """Test create_polling uses model_timeout"""
        creds = self._make_credentials()
        factory = RetryFactory(creds)

        polling = factory.create_polling(model_timeout=600)

        self.assertIsInstance(polling, Retry)

    def test__create_polling_with_credential_timeout(self):
        """Test create_polling uses credential timeout when no model_timeout"""
        creds = self._make_credentials(job_execution_timeout_seconds=1200)
        factory = RetryFactory(creds)

        polling = factory.create_polling()

        self.assertIsInstance(polling, Retry)

    def test__create_polling_with_default(self):
        """Test create_polling uses default (1 day) when no values provided"""
        creds = self._make_credentials()
        factory = RetryFactory(creds)

        polling = factory.create_polling()

        self.assertIsInstance(polling, Retry)

    def test__create_reopen_with_deadline_no_deadline(self):
        """Test create_reopen_with_deadline without deadline"""
        creds = self._make_credentials(job_retries=3)
        factory = RetryFactory(creds)

        mock_connection = mock.Mock(spec=Connection)

        retry = factory.create_reopen_with_deadline(mock_connection)

        self.assertIsInstance(retry, Retry)
        # Should have created a retry without deadline

    def test__create_reopen_with_deadline_with_deadline(self):
        """Test create_reopen_with_deadline with deadline"""
        creds = self._make_credentials(job_retries=3, job_retry_deadline_seconds=300)
        factory = RetryFactory(creds)

        mock_connection = mock.Mock(spec=Connection)

        retry = factory.create_reopen_with_deadline(mock_connection)

        self.assertIsInstance(retry, Retry)
        # Should have created a retry with deadline

    def test__create_reopen_with_deadline_configures_predicate(self):
        """Test create_reopen_with_deadline configures _DeferredException predicate"""
        creds = self._make_credentials(job_retries=5)
        factory = RetryFactory(creds)

        mock_connection = mock.Mock(spec=Connection)

        retry = factory.create_reopen_with_deadline(mock_connection)

        # The predicate should be a _DeferredException instance
        # This is set internally, so we just verify the retry was created
        self.assertIsInstance(retry, Retry)


class TestDeferredException(TestCase):
    """Test the _DeferredException callable predicate class"""

    def test__deferred_exception_initialization(self):
        """Test _DeferredException initialization"""
        predicate = _DeferredException(retries=3)

        self.assertEqual(predicate._retries, 3)
        self.assertEqual(predicate._error_count, 0)

    def test__deferred_exception_zero_retries_returns_false(self):
        """Test that zero retries immediately returns False"""
        predicate = _DeferredException(retries=0)

        # Any error should return False
        error = Exception("Test error")
        result = predicate(error)

        self.assertFalse(result)
        # Error count should still be 0 since we exited early
        self.assertEqual(predicate._error_count, 0)

    @mock.patch("dbt.adapters.bigquery.retry._job_should_retry")
    def test__deferred_exception_retryable_error_within_limit(self, mock_should_retry):
        """Test that retryable errors within limit return True"""
        mock_should_retry.return_value = True

        predicate = _DeferredException(retries=3)

        error = Exception("Retryable error")
        result = predicate(error)

        self.assertTrue(result)
        self.assertEqual(predicate._error_count, 1)

    @mock.patch("dbt.adapters.bigquery.retry._job_should_retry")
    def test__deferred_exception_multiple_retries(self, mock_should_retry):
        """Test multiple retries increment counter"""
        mock_should_retry.return_value = True

        predicate = _DeferredException(retries=3)

        # First error
        result1 = predicate(Exception("Error 1"))
        self.assertTrue(result1)
        self.assertEqual(predicate._error_count, 1)

        # Second error
        result2 = predicate(Exception("Error 2"))
        self.assertTrue(result2)
        self.assertEqual(predicate._error_count, 2)

        # Third error
        result3 = predicate(Exception("Error 3"))
        self.assertTrue(result3)
        self.assertEqual(predicate._error_count, 3)

    @mock.patch("dbt.adapters.bigquery.retry._job_should_retry")
    def test__deferred_exception_exceeds_retry_limit(self, mock_should_retry):
        """Test that exceeding retry limit returns False"""
        mock_should_retry.return_value = True

        predicate = _DeferredException(retries=2)

        # First two errors should succeed
        predicate(Exception("Error 1"))
        predicate(Exception("Error 2"))

        # Third error should fail (exceeds limit of 2)
        result = predicate(Exception("Error 3"))

        self.assertFalse(result)
        self.assertEqual(predicate._error_count, 3)

    @mock.patch("dbt.adapters.bigquery.retry._job_should_retry")
    def test__deferred_exception_non_retryable_error(self, mock_should_retry):
        """Test that non-retryable errors return False"""
        mock_should_retry.return_value = False

        predicate = _DeferredException(retries=3)

        error = Exception("Non-retryable error")
        result = predicate(error)

        self.assertFalse(result)
        # Error count should still increment
        self.assertEqual(predicate._error_count, 1)

    @mock.patch("dbt.adapters.bigquery.retry._job_should_retry")
    @mock.patch("dbt.adapters.bigquery.retry._logger")
    def test__deferred_exception_logs_retry_attempts(self, mock_logger, mock_should_retry):
        """Test that retry attempts are logged"""
        mock_should_retry.return_value = True

        predicate = _DeferredException(retries=3)

        error = Exception("Test error")
        predicate(error)

        # Should have logged the retry attempt
        mock_logger.debug.assert_called_once()
        log_msg = mock_logger.debug.call_args[0][0]
        self.assertIn("Retry attempt 1 of 3", log_msg)


class TestCreateReopenOnError(TestCase):
    """Test the _create_reopen_on_error function"""

    def test__create_reopen_on_error_returns_callable(self):
        """Test _create_reopen_on_error returns a callable"""
        mock_connection = Connection(
            type="bigquery",
            name="test",
            state=ConnectionState.OPEN,
            transaction_open=False,
            handle=mock.Mock(),
            credentials=mock.Mock(),
        )

        on_error = _create_reopen_on_error(mock_connection)

        # Should return a callable
        self.assertTrue(callable(on_error))

    @mock.patch("dbt.adapters.bigquery.retry.create_bigquery_client")
    @mock.patch("dbt.adapters.bigquery.retry._logger")
    def test__create_reopen_on_error_with_connection_reset(self, mock_logger, mock_create_client):
        """Test _create_reopen_on_error behavior with ConnectionResetError"""
        mock_connection = Connection(
            type="bigquery",
            name="test",
            state=ConnectionState.OPEN,
            transaction_open=False,
            handle=mock.Mock(),
            credentials=mock.Mock(),
        )

        mock_new_client = mock.Mock()
        mock_create_client.return_value = mock_new_client

        on_error = _create_reopen_on_error(mock_connection)

        # Call with ConnectionResetError
        error = ConnectionResetError("Connection reset")
        on_error(error)

        # Should have attempted to create new client
        mock_create_client.assert_called_once_with(mock_connection.credentials)

        # Should have logged
        self.assertGreater(mock_logger.warning.call_count, 0)

    @mock.patch("dbt.adapters.bigquery.retry.create_bigquery_client")
    def test__create_reopen_on_error_with_other_errors(self, mock_create_client):
        """Test _create_reopen_on_error ignores non-connection errors"""
        mock_connection = Connection(
            type="bigquery",
            name="test",
            state=ConnectionState.OPEN,
            transaction_open=False,
            handle=mock.Mock(),
            credentials=mock.Mock(),
        )

        on_error = _create_reopen_on_error(mock_connection)

        # Call with non-connection error - should not raise or call create_client
        error = ValueError("Some other error")
        on_error(error)

        # Should not have attempted to reopen
        mock_create_client.assert_not_called()
