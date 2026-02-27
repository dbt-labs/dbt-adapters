"""Unit tests for polling behavior in PyhiveConnectionWrapper."""

import time
import unittest
from unittest import mock

from dbt_common.exceptions import DbtRuntimeError, DbtDatabaseError
from dbt.adapters.spark.connections import PyhiveConnectionWrapper

try:
    from TCLIService.ttypes import TOperationState as ThriftState
except ImportError:
    ThriftState = None


class TestPyhivePolling(unittest.TestCase):
    """Test polling behavior fixes for long-running queries."""

    def setUp(self):
        """Set up mock cursor and connection."""
        self.mock_connection = mock.MagicMock()
        self.mock_cursor = mock.MagicMock()
        self.mock_connection.cursor.return_value = self.mock_cursor

    @mock.patch("dbt.adapters.spark.connections.time.sleep")
    def test_polling_sleeps_between_polls(self, mock_sleep):
        """Verify that polling actually sleeps between poll attempts."""
        wrapper = PyhiveConnectionWrapper(self.mock_connection, poll_interval=5)
        wrapper._cursor = self.mock_cursor

        # Mock poll responses: pending -> pending -> success
        poll_responses = [
            mock.MagicMock(operationState=ThriftState.RUNNING_STATE, errorMessage=None),
            mock.MagicMock(operationState=ThriftState.RUNNING_STATE, errorMessage=None),
            mock.MagicMock(operationState=ThriftState.FINISHED_STATE, errorMessage=None),
        ]
        self.mock_cursor.poll.side_effect = poll_responses

        # Execute query
        wrapper.execute("SELECT 1")

        # Verify sleep was called twice (once for each PENDING state)
        self.assertEqual(mock_sleep.call_count, 2)
        mock_sleep.assert_called_with(5)

    @mock.patch("dbt.adapters.spark.connections.time.sleep")
    @mock.patch("dbt.adapters.spark.connections.time.time")
    def test_query_timeout_enforced(self, mock_time, mock_sleep):
        """Verify that query timeout is enforced when configured."""
        wrapper = PyhiveConnectionWrapper(self.mock_connection, poll_interval=5, query_timeout=10)
        wrapper._cursor = self.mock_cursor

        # Mock time progression: 0, 5, 11 (exceeds 10s timeout)
        mock_time.side_effect = [0, 5, 11]

        # Mock poll responses: always pending
        self.mock_cursor.poll.return_value = mock.MagicMock(
            operationState=ThriftState.RUNNING_STATE, errorMessage=None
        )

        # Execute query and expect timeout
        with self.assertRaises(DbtRuntimeError) as context:
            wrapper.execute("SELECT 1")

        self.assertIn("exceeded timeout", str(context.exception))
        self.assertIn("10 seconds", str(context.exception))

    @mock.patch("dbt.adapters.spark.connections.time.sleep")
    @mock.patch("dbt.adapters.spark.connections.time.time")
    def test_query_timeout_zero(self, mock_time, mock_sleep):
        """Verify that query_timeout of 0 causes immediate timeout."""
        wrapper = PyhiveConnectionWrapper(
            self.mock_connection, poll_interval=5, query_timeout=0, query_retries=0
        )
        wrapper._cursor = self.mock_cursor

        # Mock time progression: start_time=0, first check has elapsed > 0
        mock_time.side_effect = [0, 0.001]  # Even tiny elapsed time exceeds 0

        # Mock poll responses: query starts in pending state
        self.mock_cursor.poll.return_value = mock.MagicMock(
            operationState=ThriftState.RUNNING_STATE, errorMessage=None
        )

        # Execute query and expect immediate timeout
        with self.assertRaises(DbtRuntimeError) as context:
            wrapper.execute("SELECT 1")

        error_msg = str(context.exception)
        self.assertIn("exceeded timeout", error_msg.lower())
        self.assertIn("0 second", error_msg)

    @mock.patch("dbt.adapters.spark.connections.time.sleep")
    def test_connection_lost_during_polling_ttransport(self, mock_sleep):
        """Verify that TTransportException during polling is handled gracefully."""
        wrapper = PyhiveConnectionWrapper(self.mock_connection, poll_interval=5, query_retries=0)
        wrapper._cursor = self.mock_cursor

        # Mock poll responses: pending -> connection lost
        from thrift.transport.TTransport import TTransportException

        poll_responses = [
            mock.MagicMock(operationState=ThriftState.RUNNING_STATE, errorMessage=None),
            TTransportException(message="TSocket read 0 bytes"),
        ]
        self.mock_cursor.poll.side_effect = poll_responses

        # Execute query and expect helpful error
        with self.assertRaises(DbtRuntimeError) as context:
            wrapper.execute("SELECT 1")

        error_msg = str(context.exception).lower()
        self.assertIn("connection lost", error_msg)
        self.assertIn("long-running queries", error_msg)
        # Verify original exception type is included
        self.assertIn("ttransportexception", error_msg)

    @mock.patch("dbt.adapters.spark.connections.time.sleep")
    def test_connection_reset_during_polling(self, mock_sleep):
        """Verify that ConnectionResetError during polling is handled."""
        wrapper = PyhiveConnectionWrapper(self.mock_connection, poll_interval=5, query_retries=0)
        wrapper._cursor = self.mock_cursor

        # Mock poll responses: pending -> connection reset
        poll_responses = [
            mock.MagicMock(operationState=ThriftState.RUNNING_STATE, errorMessage=None),
            ConnectionResetError("Connection reset by peer"),
        ]
        self.mock_cursor.poll.side_effect = poll_responses

        # Execute query and expect helpful error
        with self.assertRaises(DbtRuntimeError) as context:
            wrapper.execute("SELECT 1")

        error_msg = str(context.exception).lower()
        self.assertIn("connection lost", error_msg)
        self.assertIn("connectionreseterror", error_msg)

    @mock.patch("dbt.adapters.spark.connections.time.sleep")
    def test_broken_pipe_during_polling(self, mock_sleep):
        """Verify that BrokenPipeError during polling is handled."""
        wrapper = PyhiveConnectionWrapper(self.mock_connection, poll_interval=5, query_retries=0)
        wrapper._cursor = self.mock_cursor

        # Mock poll responses: pending -> broken pipe
        poll_responses = [
            mock.MagicMock(operationState=ThriftState.RUNNING_STATE, errorMessage=None),
            BrokenPipeError("Broken pipe"),
        ]
        self.mock_cursor.poll.side_effect = poll_responses

        # Execute query and expect helpful error
        with self.assertRaises(DbtRuntimeError) as context:
            wrapper.execute("SELECT 1")

        error_msg = str(context.exception).lower()
        self.assertIn("connection lost", error_msg)
        self.assertIn("brokenpipeerror", error_msg)

    @mock.patch("dbt.adapters.spark.connections.time.sleep")
    def test_eof_error_during_polling(self, mock_sleep):
        """Verify that EOFError during polling is handled."""
        wrapper = PyhiveConnectionWrapper(self.mock_connection, poll_interval=5, query_retries=0)
        wrapper._cursor = self.mock_cursor

        # Mock poll responses: pending -> EOF
        poll_responses = [
            mock.MagicMock(operationState=ThriftState.RUNNING_STATE, errorMessage=None),
            EOFError("EOF when reading a line"),
        ]
        self.mock_cursor.poll.side_effect = poll_responses

        # Execute query and expect helpful error
        with self.assertRaises(DbtRuntimeError) as context:
            wrapper.execute("SELECT 1")

        error_msg = str(context.exception).lower()
        self.assertIn("connection lost", error_msg)
        self.assertIn("eoferror", error_msg)

    @mock.patch("dbt.adapters.spark.connections.time.sleep")
    def test_no_timeout_by_default(self, mock_sleep):
        """Verify that queries can run indefinitely if no timeout is set."""
        wrapper = PyhiveConnectionWrapper(
            self.mock_connection, poll_interval=5, query_timeout=None
        )
        wrapper._cursor = self.mock_cursor

        # Mock poll responses: many pending states, then success
        poll_responses = [
            mock.MagicMock(operationState=ThriftState.RUNNING_STATE, errorMessage=None)
            for _ in range(100)
        ] + [mock.MagicMock(operationState=ThriftState.FINISHED_STATE, errorMessage=None)]
        self.mock_cursor.poll.side_effect = poll_responses

        # Execute query - should not timeout
        wrapper.execute("SELECT 1")

        # Verify it polled many times
        self.assertEqual(mock_sleep.call_count, 100)

    @mock.patch("dbt.adapters.spark.connections.time.sleep")
    def test_error_message_from_server(self, mock_sleep):
        """Verify that error messages from the server are properly raised."""
        wrapper = PyhiveConnectionWrapper(self.mock_connection, poll_interval=5)
        wrapper._cursor = self.mock_cursor

        # Mock poll response with error message
        error_response = mock.MagicMock(
            operationState=ThriftState.ERROR_STATE, errorMessage="Table not found: test_table"
        )
        self.mock_cursor.poll.return_value = error_response

        # Execute query and expect database error
        with self.assertRaises(DbtDatabaseError) as context:
            wrapper.execute("SELECT * FROM test_table")

        self.assertIn("Table not found", str(context.exception))

    @mock.patch("dbt.adapters.spark.connections.time.sleep")
    def test_cancelled_query_raises_error(self, mock_sleep):
        """Verify that cancelled queries raise an appropriate error."""
        wrapper = PyhiveConnectionWrapper(self.mock_connection, poll_interval=5)
        wrapper._cursor = self.mock_cursor

        # Mock poll response: cancelled state without error message
        cancelled_response = mock.MagicMock(
            operationState=ThriftState.CANCELED_STATE, errorMessage=None
        )
        self.mock_cursor.poll.return_value = cancelled_response

        # Execute query and expect error
        with self.assertRaises(DbtDatabaseError) as context:
            wrapper.execute("SELECT 1")

        self.assertIn("CANCELED", str(context.exception))

    @mock.patch("dbt.adapters.spark.connections.time.sleep")
    def test_custom_poll_interval(self, mock_sleep):
        """Verify that custom poll interval is respected."""
        wrapper = PyhiveConnectionWrapper(self.mock_connection, poll_interval=10)
        wrapper._cursor = self.mock_cursor

        # Mock poll responses: pending -> success
        poll_responses = [
            mock.MagicMock(operationState=ThriftState.RUNNING_STATE, errorMessage=None),
            mock.MagicMock(operationState=ThriftState.FINISHED_STATE, errorMessage=None),
        ]
        self.mock_cursor.poll.side_effect = poll_responses

        # Execute query
        wrapper.execute("SELECT 1")

        # Verify sleep was called with custom interval
        mock_sleep.assert_called_once_with(10)

    @mock.patch("dbt.adapters.spark.connections.time.sleep")
    def test_non_connection_exception_reraised(self, mock_sleep):
        """Verify that non-connection exceptions are re-raised as-is."""
        wrapper = PyhiveConnectionWrapper(self.mock_connection, poll_interval=5, query_retries=0)
        wrapper._cursor = self.mock_cursor

        # Mock poll responses: pending -> some other exception
        poll_responses = [
            mock.MagicMock(operationState=ThriftState.RUNNING_STATE, errorMessage=None),
            ValueError("Some unexpected error"),
        ]
        self.mock_cursor.poll.side_effect = poll_responses

        # Execute query and expect ValueError to be re-raised (not wrapped)
        with self.assertRaises(ValueError) as context:
            wrapper.execute("SELECT 1")

        self.assertIn("unexpected error", str(context.exception))

    @mock.patch("dbt.adapters.spark.connections.time.sleep")
    def test_exception_caught_by_type_not_message(self, mock_sleep):
        """Verify that exceptions are caught by type, not by parsing message strings."""
        wrapper = PyhiveConnectionWrapper(self.mock_connection, poll_interval=5, query_retries=0)
        wrapper._cursor = self.mock_cursor

        # Create a ConnectionResetError with a message that doesn't contain typical keywords
        # If we were parsing strings, this might not be caught, but it should be caught by type
        unusual_error = ConnectionResetError("xyz123")  # No "reset" or "peer" in message

        poll_responses = [
            mock.MagicMock(operationState=ThriftState.RUNNING_STATE, errorMessage=None),
            unusual_error,
        ]
        self.mock_cursor.poll.side_effect = poll_responses

        # Execute query and expect it to be caught as connection error
        with self.assertRaises(DbtRuntimeError) as context:
            wrapper.execute("SELECT 1")

        error_msg = str(context.exception)
        # Should be wrapped as connection lost error (caught by type)
        self.assertIn("connection lost", error_msg.lower())
        # Should include the exception type name
        self.assertIn("connectionreseterror", error_msg.lower())
        # Should include original message even though it's unusual
        self.assertIn("xyz123", error_msg)

    @mock.patch("dbt.adapters.spark.connections.time.sleep")
    def test_chained_exception_preserved(self, mock_sleep):
        """Verify that exception chaining is preserved for connection errors."""
        wrapper = PyhiveConnectionWrapper(self.mock_connection, poll_interval=5, query_retries=0)
        wrapper._cursor = self.mock_cursor

        # Mock poll responses: pending -> connection lost
        original_error = ConnectionResetError("Connection reset by peer")
        poll_responses = [
            mock.MagicMock(operationState=ThriftState.RUNNING_STATE, errorMessage=None),
            original_error,
        ]
        self.mock_cursor.poll.side_effect = poll_responses

        # Execute query and verify exception chaining
        with self.assertRaises(DbtRuntimeError) as context:
            wrapper.execute("SELECT 1")

        # Verify the original exception is preserved in the chain
        self.assertIsNotNone(context.exception.__cause__)
        self.assertIsInstance(context.exception.__cause__, ConnectionResetError)

    @mock.patch("dbt.adapters.spark.connections.time.sleep")
    def test_query_retry_on_connection_loss(self, mock_sleep):
        """Verify that queries are retried on connection loss."""
        wrapper = PyhiveConnectionWrapper(self.mock_connection, poll_interval=5, query_retries=2)
        wrapper._cursor = self.mock_cursor

        # Mock cursor() to return fresh cursors on retry
        fresh_cursor = mock.MagicMock()
        self.mock_connection.cursor.return_value = fresh_cursor

        # First attempt: pending -> connection lost
        # Second attempt: pending -> success
        first_attempt = [
            mock.MagicMock(operationState=ThriftState.RUNNING_STATE, errorMessage=None),
            ConnectionResetError("Connection reset by peer"),
        ]
        second_attempt = [
            mock.MagicMock(operationState=ThriftState.RUNNING_STATE, errorMessage=None),
            mock.MagicMock(operationState=ThriftState.FINISHED_STATE, errorMessage=None),
        ]

        self.mock_cursor.poll.side_effect = first_attempt
        fresh_cursor.poll.side_effect = second_attempt

        # Execute query - should succeed after retry
        wrapper.execute("SELECT 1")

        # Verify sleep was called for polling + retry delay
        self.assertGreater(mock_sleep.call_count, 1)

    @mock.patch("dbt.adapters.spark.connections.time.sleep")
    def test_query_retry_exhausted(self, mock_sleep):
        """Verify that error is raised when all retries are exhausted."""
        wrapper = PyhiveConnectionWrapper(self.mock_connection, poll_interval=5, query_retries=1)
        wrapper._cursor = self.mock_cursor

        # Mock cursor() to return fresh cursors
        fresh_cursor1 = mock.MagicMock()
        fresh_cursor2 = mock.MagicMock()
        self.mock_connection.cursor.side_effect = [fresh_cursor1, fresh_cursor2]

        # Both attempts fail with connection loss
        connection_error = ConnectionResetError("Connection reset by peer")
        self.mock_cursor.poll.side_effect = [
            mock.MagicMock(operationState=ThriftState.RUNNING_STATE, errorMessage=None),
            connection_error,
        ]
        fresh_cursor1.poll.side_effect = [
            mock.MagicMock(operationState=ThriftState.RUNNING_STATE, errorMessage=None),
            connection_error,
        ]

        # Execute query and expect failure after retries
        with self.assertRaises(DbtRuntimeError) as context:
            wrapper.execute("SELECT 1")

        error_msg = str(context.exception)
        self.assertIn("after 2 attempts", error_msg)
        self.assertIn("consider increasing 'query_retries'", error_msg.lower())

    @mock.patch("dbt.adapters.spark.connections.time.sleep")
    def test_no_retry_on_database_errors(self, mock_sleep):
        """Verify that database errors are not retried, only connection errors."""
        wrapper = PyhiveConnectionWrapper(self.mock_connection, poll_interval=5, query_retries=2)
        wrapper._cursor = self.mock_cursor

        # Mock poll response with database error (not connection error)
        error_response = mock.MagicMock(
            operationState=ThriftState.ERROR_STATE, errorMessage="Table not found: test_table"
        )
        self.mock_cursor.poll.return_value = error_response

        # Execute query - should fail immediately without retry
        with self.assertRaises(DbtDatabaseError) as context:
            wrapper.execute("SELECT * FROM test_table")

        # Should not retry - only 1 attempt
        self.assertEqual(self.mock_cursor.execute.call_count, 1)


if __name__ == "__main__":
    unittest.main()
