import redshift_connector

from multiprocessing import get_context
from unittest import TestCase, mock

from dbt_common.clients import agate_helper
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.redshift import (
    Plugin as RedshiftPlugin,
    RedshiftAdapter,
)
from tests.unit.utils import config_from_parts_or_dicts, inject_adapter


class TestQuery(TestCase):
    def setUp(self):
        profile_cfg = {
            "outputs": {
                "test": {
                    "type": "redshift",
                    "dbname": "redshift",
                    "user": "root",
                    "host": "thishostshouldnotexist.test.us-east-1",
                    "pass": "password",
                    "port": 5439,
                    "schema": "public",
                }
            },
            "target": "test",
        }

        project_cfg = {
            "name": "X",
            "version": "0.1",
            "profile": "test",
            "project-root": "/tmp/dbt/does-not-exist",
            "quoting": {
                "identifier": False,
                "schema": True,
            },
            "config-version": 2,
        }

        self.config = config_from_parts_or_dicts(project_cfg, profile_cfg)
        self._adapter = None

    @property
    def adapter(self):
        if self._adapter is None:
            self._adapter = RedshiftAdapter(self.config, get_context("spawn"))
            inject_adapter(self._adapter, RedshiftPlugin)
        return self._adapter

    def test_execute_with_fetch(self):
        cursor = mock.Mock()
        table = agate_helper.empty_table()
        with mock.patch.object(self.adapter.connections, "add_query") as mock_add_query:
            mock_add_query.return_value = (
                None,
                cursor,
            )
            with mock.patch.object(self.adapter.connections, "get_response") as mock_get_response:
                mock_get_response.return_value = {}
                with mock.patch.object(
                    self.adapter.connections, "get_result_from_cursor"
                ) as mock_get_result_from_cursor:
                    mock_get_result_from_cursor.return_value = table
                    self.adapter.connections.execute(sql="select * from test", fetch=True)
        mock_add_query.assert_called_once_with("select * from test", False)
        mock_get_result_from_cursor.assert_called_once_with(cursor, None)
        mock_get_response.assert_called_once_with(cursor)

    def test_execute_without_fetch(self):
        cursor = mock.Mock()
        with mock.patch.object(self.adapter.connections, "add_query") as mock_add_query:
            mock_add_query.return_value = (
                None,
                cursor,
            )
            with mock.patch.object(self.adapter.connections, "get_response") as mock_get_response:
                mock_get_response.return_value = {}
                with mock.patch.object(
                    self.adapter.connections, "get_result_from_cursor"
                ) as mock_get_result_from_cursor:
                    self.adapter.connections.execute(sql="select * from test2", fetch=False)
        mock_add_query.assert_called_once_with("select * from test2", False)
        mock_get_result_from_cursor.assert_not_called()
        mock_get_response.assert_called_once_with(cursor)

    def test_add_query_success(self):
        """Test that add_query executes SQL using single cursor with retry logic."""
        mock_cursor = mock.MagicMock()
        mock_connection = mock.MagicMock()
        mock_connection.handle.cursor.return_value = mock_cursor
        mock_connection.transaction_open = False
        mock_connection.credentials.autocommit = True
        mock_connection.credentials.retries = 1
        mock_connection.name = "test_connection"

        with mock.patch.object(
            self.adapter.connections, "get_thread_connection", return_value=mock_connection
        ):
            with mock.patch.object(self.adapter.connections, "_initialize_sqlparse_lexer"):
                with mock.patch.object(self.adapter.connections, "exception_handler"):
                    with mock.patch.object(self.adapter.connections, "_execute_with_retry"):
                        connection, cursor = self.adapter.connections.add_query(
                            "select * from test3"
                        )

        # Verify single cursor was created
        mock_connection.handle.cursor.assert_called_once()
        assert cursor == mock_cursor

    def test_add_query_with_empty_sql(self):
        """Test that add_query raises error for empty SQL."""
        mock_connection = mock.MagicMock()
        mock_connection.handle.cursor.return_value = mock.MagicMock()
        mock_connection.transaction_open = False
        mock_connection.credentials.autocommit = True
        mock_connection.name = "test_connection"

        with mock.patch.object(
            self.adapter.connections, "get_thread_connection", return_value=mock_connection
        ):
            with mock.patch.object(self.adapter.connections, "_initialize_sqlparse_lexer"):
                with self.assertRaisesRegex(
                    DbtRuntimeError, "Tried to run invalid SQL:  on test_connection"
                ):
                    self.adapter.connections.add_query(sql="")

    def test_add_query_multi_statement(self):
        """Test that add_query splits and executes multiple statements."""
        mock_cursor = mock.MagicMock()
        mock_connection = mock.MagicMock()
        mock_connection.handle.cursor.return_value = mock_cursor
        mock_connection.transaction_open = False
        mock_connection.credentials.autocommit = True
        mock_connection.credentials.retries = 1
        mock_connection.name = "test_connection"

        with mock.patch.object(
            self.adapter.connections, "get_thread_connection", return_value=mock_connection
        ):
            with mock.patch.object(self.adapter.connections, "_initialize_sqlparse_lexer"):
                with mock.patch.object(
                    self.adapter.connections, "_execute_with_retry"
                ) as mock_execute:
                    self.adapter.connections.add_query("select 1; select 2")

        # Both statements should be executed
        assert mock_execute.call_count == 2

    def test_execute_with_retry_uses_exponential_backoff(self):
        """Test that _execute_with_retry uses exponential backoff on transient errors."""
        mock_cursor = mock.MagicMock()
        mock_connection = mock.MagicMock()
        mock_connection.credentials.retries = 3
        mock_connection.name = "test_connection"

        # Fail twice, then succeed
        mock_cursor.execute.side_effect = [
            redshift_connector.InterfaceError("transient error 1"),
            redshift_connector.InterfaceError("transient error 2"),
            None,  # Success
        ]

        with mock.patch.object(
            self.adapter.connections, "get_thread_connection", return_value=mock_connection
        ):
            with mock.patch("time.sleep") as mock_sleep:
                with mock.patch("time.perf_counter", return_value=0):
                    with mock.patch("dbt_common.events.functions.fire_event"):
                        self.adapter.connections._execute_with_retry(
                            cursor=mock_cursor, sql="select 1"
                        )

        # Should have retried twice with exponential backoff
        assert mock_cursor.execute.call_count == 3
        assert mock_sleep.call_count == 2
        # First backoff is 1s, second is 2s
        mock_sleep.assert_any_call(1.0)
        mock_sleep.assert_any_call(2.0)

    def test_execute_with_retry_raises_after_exhausting_retries(self):
        """Test that _execute_with_retry raises after exhausting retries."""
        mock_cursor = mock.MagicMock()
        mock_connection = mock.MagicMock()
        mock_connection.credentials.retries = 2
        mock_connection.name = "test_connection"

        mock_cursor.execute.side_effect = redshift_connector.InterfaceError("persistent error")

        with mock.patch.object(
            self.adapter.connections, "get_thread_connection", return_value=mock_connection
        ):
            with mock.patch("time.sleep"):
                with mock.patch("time.perf_counter", return_value=0):
                    with mock.patch("dbt_common.events.functions.fire_event"):
                        with self.assertRaises(redshift_connector.InterfaceError):
                            self.adapter.connections._execute_with_retry(
                                cursor=mock_cursor, sql="select 1"
                            )

        # Initial attempt + 2 retries = 3 total attempts
        assert mock_cursor.execute.call_count == 3
