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
        with mock.patch.object(self.adapter.connections, "add_query"):
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
                    self.adapter.connections.open(connection_mock)
            assert str(e.value) == "Database Error\n  retryable interface error<3>"
            assert connect_mock.call_count == 3

    def test_retry_relation_could_not_open_relation_with_oid(self):
        with mock.patch.object(self.adapter.connections, "add_query") as add_query_mock:
            add_query_mock.side_effect = [
                redshift_connector.ProgrammingError("could not open relation with OID"),
                redshift_connector.ProgrammingError("could not open relation with OID"),
                redshift_connector.ProgrammingError("could not open relation with OID"),
            ]
            self.adapter.connections.get_thread_connection = MagicMock()
            self.adapter.connections.close = MagicMock()
            self.adapter.connections.open = MagicMock()
            with pytest.raises(Exception) as e:
                self.adapter.connections.execute("select 1", auto_begin=True)
            assert "could not open relation with OID" in str(e.value)
            assert add_query_mock.call_count == 2

    def test_tcp_keepalive_parameters(self):
        test_cases = [
            # Test default behavior (tcp_keepalive=True by default)
            {
                "name": "default_tcp_keepalive",
                "tcp_keepalive": True,
                "tcp_keepalive_idle": None,
                "tcp_keepalive_interval": None,
                "tcp_keepalive_count": None,
                "expected_kwargs": {"tcp_keepalive": True},
            },
            # Test with tcp_keepalive disabled
            {
                "name": "tcp_keepalive_disabled",
                "tcp_keepalive": False,
                "tcp_keepalive_idle": None,
                "tcp_keepalive_interval": None,
                "tcp_keepalive_count": None,
                "expected_kwargs": {},
            },
            # Test with all TCP keepalive parameters enabled
            {
                "name": "all_tcp_keepalive_params",
                "tcp_keepalive": True,
                "tcp_keepalive_idle": 600,
                "tcp_keepalive_interval": 60,
                "tcp_keepalive_count": 5,
                "expected_kwargs": {
                    "tcp_keepalive": True,
                    "tcp_keepalive_idle": 600,
                    "tcp_keepalive_interval": 60,
                    "tcp_keepalive_count": 5,
                },
            },
            # Test with partial TCP keepalive parameters
            {
                "name": "partial_tcp_keepalive_params",
                "tcp_keepalive": True,
                "tcp_keepalive_idle": 300,
                "tcp_keepalive_interval": None,
                "tcp_keepalive_count": None,
                "expected_kwargs": {"tcp_keepalive": True, "tcp_keepalive_idle": 300},
            },
        ]

        for test_case in test_cases:
            with self.subTest(test_case["name"]):
                tcp_keepalive = test_case["tcp_keepalive"]
                tcp_keepalive_idle = test_case["tcp_keepalive_idle"]
                tcp_keepalive_interval = test_case["tcp_keepalive_interval"]
                tcp_keepalive_count = test_case["tcp_keepalive_count"]
                expected_kwargs = test_case["expected_kwargs"]

                credentials_dict = {
                    "type": "redshift",
                    "dbname": "redshift",
                    "user": "root",
                    "host": "test-host.redshift.amazonaws.com",
                    "pass": "password",
                    "port": 5439,
                    "schema": "public",
                    "tcp_keepalive": tcp_keepalive,
                }

                # Add optional parameters if they are not None
                if tcp_keepalive_idle is not None:
                    credentials_dict["tcp_keepalive_idle"] = tcp_keepalive_idle
                if tcp_keepalive_interval is not None:
                    credentials_dict["tcp_keepalive_interval"] = tcp_keepalive_interval
                if tcp_keepalive_count is not None:
                    credentials_dict["tcp_keepalive_count"] = tcp_keepalive_count

                credentials = RedshiftCredentials.from_dict(credentials_dict)

                from dbt.adapters.redshift.connections import get_connection_method

                with mock.patch("redshift_connector.connect") as mock_connect:
                    mock_connect.return_value = MagicMock()
                    connect_method = get_connection_method(credentials)
                    connect_method()
                    mock_connect.assert_called_once()
                    actual_kwargs = mock_connect.call_args[1]

                    for key, value in expected_kwargs.items():
                        self.assertIn(
                            key, actual_kwargs, f"Expected {key} to be in connection kwargs"
                        )
                        self.assertEqual(
                            actual_kwargs[key],
                            value,
                            f"Expected {key}={value}, got {actual_kwargs[key]}",
                        )

                    if not tcp_keepalive:
                        tcp_keepalive_keys = [
                            "tcp_keepalive",
                            "tcp_keepalive_idle",
                            "tcp_keepalive_interval",
                            "tcp_keepalive_count",
                        ]
                        for key in tcp_keepalive_keys:
                            self.assertNotIn(
                                key,
                                actual_kwargs,
                                f"Unexpected {key} in connection kwargs when tcp_keepalive is False",
                            )

                    self.assertEqual(actual_kwargs["host"], "test-host.redshift.amazonaws.com")
                    self.assertEqual(actual_kwargs["port"], 5439)
                    self.assertEqual(actual_kwargs["database"], "redshift")
                    self.assertEqual(actual_kwargs["auto_create"], False)
                    self.assertEqual(actual_kwargs["is_serverless"], False)

    def test_tcp_keepalive_credentials_validation(self):
        valid_credentials = RedshiftCredentials.from_dict(
            {
                "type": "redshift",
                "dbname": "redshift",
                "user": "root",
                "host": "test-host.redshift.amazonaws.com",
                "pass": "password",
                "port": 5439,
                "schema": "public",
                "tcp_keepalive": True,
                "tcp_keepalive_idle": 600,
                "tcp_keepalive_interval": 60,
                "tcp_keepalive_count": 5,
            }
        )

        assert valid_credentials.tcp_keepalive is True
        assert valid_credentials.tcp_keepalive_idle == 600
        assert valid_credentials.tcp_keepalive_interval == 60
        assert valid_credentials.tcp_keepalive_count == 5

        default_credentials = RedshiftCredentials.from_dict(
            {
                "type": "redshift",
                "dbname": "redshift",
                "user": "root",
                "host": "test-host.redshift.amazonaws.com",
                "pass": "password",
                "port": 5439,
                "schema": "public",
            }
        )

        assert default_credentials.tcp_keepalive is True
        assert default_credentials.tcp_keepalive_idle is None
        assert default_credentials.tcp_keepalive_interval is None
        assert default_credentials.tcp_keepalive_count is None
