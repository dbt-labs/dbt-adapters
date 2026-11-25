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

    def _add_query_on(self, autocommit, transaction_open, in_transaction, auto_begin=False):
        mock_connection = mock.MagicMock()
        mock_connection.credentials.autocommit = autocommit
        mock_connection.transaction_open = transaction_open
        mock_connection.handle.in_transaction = in_transaction

        with mock.patch(
            "dbt.adapters.sql.SQLConnectionManager.add_query",
            return_value=(mock_connection, mock.MagicMock()),
        ):
            with mock.patch.object(self.adapter.connections, "_initialize_sqlparse_lexer"):
                self.adapter.connections.add_query(
                    "insert into t values (1)", auto_begin=auto_begin
                )
        return mock_connection

    def test_add_query_tracks_driver_transaction_without_autocommit(self):
        """With autocommit off, a query run with auto_begin=False still opens a
        transaction in redshift_connector, so dbt must track it to commit it."""
        connection = self._add_query_on(
            autocommit=False, transaction_open=False, in_transaction=True
        )
        assert connection.transaction_open is True

    def test_add_query_leaves_state_when_driver_has_no_transaction(self):
        connection = self._add_query_on(
            autocommit=False, transaction_open=False, in_transaction=False
        )
        assert connection.transaction_open is False

    def test_add_query_leaves_state_with_autocommit(self):
        connection = self._add_query_on(
            autocommit=True, transaction_open=False, in_transaction=True
        )
        assert connection.transaction_open is False

    def test_add_query_tracks_driver_transaction_before_next_statement(self):
        """The flag is set after each statement, so it is already True when a
        later statement in the same call runs (and possibly fails)."""
        mock_connection = mock.MagicMock()
        mock_connection.credentials.autocommit = False
        mock_connection.transaction_open = False
        mock_connection.handle.in_transaction = True
        seen_on_second_statement = []

        def fake_add_query(query, *args, **kwargs):
            if query.startswith("select"):
                seen_on_second_statement.append(mock_connection.transaction_open)
                raise DbtRuntimeError("boom")
            return mock_connection, mock.MagicMock()

        with mock.patch(
            "dbt.adapters.sql.SQLConnectionManager.add_query", side_effect=fake_add_query
        ):
            with mock.patch.object(self.adapter.connections, "_initialize_sqlparse_lexer"):
                with self.assertRaisesRegex(DbtRuntimeError, "boom"):
                    self.adapter.connections.add_query(
                        "insert into t values (1); select bad_col from t", auto_begin=False
                    )

        assert seen_on_second_statement == [True]

    def test_add_query_never_clears_transaction_flag(self):
        """The update is one-way: a statement that leaves the driver idle, such
        as a literal commit, doesn't clear a transaction dbt is tracking."""
        connection = self._add_query_on(
            autocommit=False, transaction_open=True, in_transaction=False
        )
        assert connection.transaction_open is True
