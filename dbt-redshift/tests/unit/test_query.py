import redshift_connector

from multiprocessing import get_context
from unittest import TestCase, mock

from dbt.adapters.sql.connections import SQLConnectionManager
from dbt_common.clients import agate_helper
from dbt_common.exceptions import DbtRuntimeError, DbtDatabaseError

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

    @mock.patch.object(SQLConnectionManager, "get_thread_connection")
    def mock_cursor(self, mock_get_thread_conn):
        conn = mock.MagicMock
        mock_get_thread_conn.return_value = conn
        mock_handle = mock.MagicMock
        conn.return_value = mock_handle
        mock_cursor = mock.MagicMock
        mock_handle.return_value = mock_cursor
        return mock_cursor

    def test_execute_with_fetch(self):
        cursor = mock.Mock()
        table = agate_helper.empty_table()
        with mock.patch.object(self.adapter.connections, "add_query") as mock_add_query:
            mock_add_query.return_value = (
                None,
                cursor,
            )  # when mock_add_query is called, it will always return None, cursor
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
            )  # when mock_add_query is called, it will always return None, cursor
            with mock.patch.object(self.adapter.connections, "get_response") as mock_get_response:
                mock_get_response.return_value = {}
                with mock.patch.object(
                    self.adapter.connections, "get_result_from_cursor"
                ) as mock_get_result_from_cursor:
                    self.adapter.connections.execute(sql="select * from test2", fetch=False)
        mock_add_query.assert_called_once_with("select * from test2", False)
        mock_get_result_from_cursor.assert_not_called()
        mock_get_response.assert_called_once_with(cursor)

    def test_execute_with_retry_all(self):
        cursor = mock.Mock()
        self.adapter.connections.close = mock.MagicMock()
        self.adapter.connections.open = mock.MagicMock()
        with mock.patch.object(
            self.adapter.connections, "get_thread_connection"
        ) as mock_get_thread_connection:
            mock_get_thread_connection.return_value = mock.MagicMock(name="test_connection")
            with mock.patch.object(self.adapter.connections, "add_query") as mock_add_query:
                self.adapter.connections.profile.credentials.retries = 3
                self.adapter.connections.profile.credentials.retry_all = True
                mock_add_query.side_effect = [
                    DbtDatabaseError("test"),
                    DbtDatabaseError("test2"),
                    (None, cursor),
                ]
                with mock.patch.object(
                    self.adapter.connections, "get_response"
                ) as mock_get_response:
                    mock_get_response.return_value = {}
                    with mock.patch.object(
                        self.adapter.connections, "get_result_from_cursor"
                    ) as mock_get_result_from_cursor:
                        mock_get_result_from_cursor.return_value = agate_helper.empty_table()
                        self.adapter.connections.execute(sql="select * from test", fetch=True)
        assert mock_add_query.call_count == 3

    def test_add_query_success(self):
        cursor = mock.Mock()
        with mock.patch.object(SQLConnectionManager, "add_query") as mock_add_query:
            mock_add_query.return_value = None, cursor
            self.adapter.connections.add_query("select * from test3")
        mock_add_query.assert_called_once_with(
            "select * from test3",
            True,
            bindings=None,
            abridge_sql_log=False,
            retryable_exceptions=(
                redshift_connector.InterfaceError,
                redshift_connector.InternalError,
            ),
            retry_limit=1,
        )

    def test_add_query_with_no_cursor(self):
        with mock.patch.object(
            self.adapter.connections, "get_thread_connection"
        ) as mock_get_thread_connection:
            mock_get_thread_connection.return_value = None
            with self.assertRaisesRegex(DbtRuntimeError, "Tried to run invalid SQL:  on <None>"):
                self.adapter.connections.add_query(sql="")
        mock_get_thread_connection.assert_called_once()
