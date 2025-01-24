from multiprocessing import get_context
from unittest import TestCase, mock

import pytest
from dbt.adapters.exceptions import FailedToConnectError
from unittest.mock import MagicMock, call

import redshift_connector

from dbt.adapters.redshift import (
    Plugin as RedshiftPlugin,
    RedshiftAdapter,
    RedshiftCredentials,
)
from tests.unit.utils import (
    config_from_parts_or_dicts,
    inject_adapter,
    mock_connection,
)


class TestConnection(TestCase):

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

    def test_cancel_open_connections_empty(self):
        self.assertEqual(len(list(self.adapter.cancel_open_connections())), 0)

    def test_cancel_open_connections_master(self):
        key = self.adapter.connections.get_thread_identifier()
        self.adapter.connections.thread_connections[key] = mock_connection("master")
        self.assertEqual(len(list(self.adapter.cancel_open_connections())), 0)

    def test_cancel_open_connections_single(self):
        master = mock_connection("master")
        model = mock_connection("model")

        key = self.adapter.connections.get_thread_identifier()
        self.adapter.connections.thread_connections.update(
            {
                key: master,
                1: model,
            }
        )
        with mock.patch.object(self.adapter.connections, "add_query") as add_query:
            query_result = mock.MagicMock()
            cursor = mock.Mock()
            cursor.fetchone.return_value = (42,)
            add_query.side_effect = [(None, cursor), (None, query_result)]

            self.assertEqual(len(list(self.adapter.cancel_open_connections())), 1)
            add_query.assert_has_calls(
                [
                    call(f"select pg_terminate_backend({model.backend_pid})"),
                ]
            )

        master.handle.backend_pid.assert_not_called()

    @mock.patch("redshift_connector.connect", MagicMock())
    def test_connection_has_backend_pid(self):
        backend_pid = 42

        cursor = mock.MagicMock()
        execute = cursor().__enter__().execute
        execute().fetchone.return_value = (backend_pid,)
        redshift_connector.connect().cursor = cursor

        connection = self.adapter.acquire_connection("dummy")
        connection.handle
        assert connection.backend_pid == backend_pid

        execute.assert_has_calls(
            [
                call("select pg_backend_pid()"),
            ]
        )

    @mock.patch("redshift_connector.connect", MagicMock())
    def test_backend_pid_used_in_pg_terminate_backend(self):
        with mock.patch.object(self.adapter.connections, "add_query") as add_query:
            backend_pid = 42
            query_result = (backend_pid,)

            cursor = mock.MagicMock()
            cursor().__enter__().execute().fetchone.return_value = query_result
            redshift_connector.connect().cursor = cursor

            connection = self.adapter.acquire_connection("dummy")
            connection.handle

            self.adapter.connections.cancel(connection)

            add_query.assert_has_calls(
                [
                    call(f"select pg_terminate_backend({backend_pid})"),
                ]
            )

    def test_retry_able_exceptions_trigger_retry(self):
        with mock.patch.object(self.adapter.connections, "add_query") as add_query:
            connection_mock = mock_connection("model", state="closed")
            connection_mock.credentials = RedshiftCredentials.from_dict(
                {
                    "type": "redshift",
                    "dbname": "redshift",
                    "user": "root",
                    "host": "thishostshouldnotexist.test.us-east-1",
                    "pass": "password",
                    "port": 5439,
                    "schema": "public",
                    "retries": 2,
                }
            )

            connect_mock = MagicMock()
            connect_mock.side_effect = [
                redshift_connector.InterfaceError("retryable interface error<1>"),
                redshift_connector.InterfaceError("retryable interface error<2>"),
                redshift_connector.InterfaceError("retryable interface error<3>"),
            ]

            with mock.patch("redshift_connector.connect", connect_mock):
                with pytest.raises(FailedToConnectError) as e:
                    connection = self.adapter.connections.open(connection_mock)
            assert str(e.value) == "Database Error\n  retryable interface error<3>"
            assert connect_mock.call_count == 3
