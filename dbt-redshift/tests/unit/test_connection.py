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

        # Mock the cursor context manager pattern used in _get_backend_pid
        # The original implementation uses c.execute(sql).fetchone() chain
        mock_cursor = MagicMock()
        mock_cursor.execute.return_value.fetchone.return_value = (backend_pid,)
        mock_cursor_cm = MagicMock()
        mock_cursor_cm.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor_cm.__exit__ = MagicMock(return_value=False)
        redshift_connector.connect().cursor.return_value = mock_cursor_cm

        connection = self.adapter.acquire_connection("dummy")
        connection.handle
        assert connection.backend_pid == backend_pid

        mock_cursor.execute.assert_called_with("select pg_backend_pid()")

    @mock.patch("redshift_connector.connect", MagicMock())
    def test_backend_pid_used_in_pg_terminate_backend(self):
        with mock.patch.object(self.adapter.connections, "add_query") as add_query:
            backend_pid = 42

            # Mock the cursor context manager pattern
            # The original implementation uses c.execute(sql).fetchone() chain
            mock_cursor = MagicMock()
            mock_cursor.execute.return_value.fetchone.return_value = (backend_pid,)
            mock_cursor_cm = MagicMock()
            mock_cursor_cm.__enter__ = MagicMock(return_value=mock_cursor)
            mock_cursor_cm.__exit__ = MagicMock(return_value=False)
            redshift_connector.connect().cursor.return_value = mock_cursor_cm

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


class TestQueryGroup(TestCase):
    """Tests for query_group credential and connection behavior."""

    def test_credentials_query_group_from_dict(self):
        credentials = RedshiftCredentials.from_dict(
            {
                "type": "redshift",
                "dbname": "redshift",
                "user": "root",
                "host": "test-host.redshift.amazonaws.com",
                "pass": "password",
                "port": 5439,
                "schema": "public",
                "query_group": "dbt_test_group",
            }
        )
        assert credentials.query_group == "dbt_test_group"

    def test_credentials_query_group_default_none(self):
        credentials = RedshiftCredentials.from_dict(
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
        assert credentials.query_group is None

    @mock.patch("redshift_connector.connect", MagicMock())
    def test_open_executes_set_query_group_when_configured(self):
        mock_cursor = MagicMock()
        mock_handle = MagicMock()
        mock_handle.cursor.return_value = mock_cursor

        connect_mock = MagicMock()
        connect_mock.return_value = mock_handle

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
                    "query_group": "dbt_test_group",
                }
            },
            "target": "test",
        }
        project_cfg = {
            "name": "X",
            "version": "0.1",
            "profile": "test",
            "project-root": "/tmp/dbt/does-not-exist",
            "quoting": {"identifier": False, "schema": True},
            "config-version": 2,
        }
        config = config_from_parts_or_dicts(project_cfg, profile_cfg)
        adapter = RedshiftAdapter(config, get_context("spawn"))
        inject_adapter(adapter, RedshiftPlugin)

        connection = mock_connection("test", state="closed")
        connection.credentials = config.credentials
        connection.handle = None

        with mock.patch("redshift_connector.connect", connect_mock):
            adapter.connections.open(connection)

        mock_cursor.execute.assert_any_call("SET query_group TO 'dbt_test_group'")


class TestAutocommitBehavior(TestCase):
    """Tests for autocommit-aware transaction management with behavior flag."""

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
                    "autocommit": True,
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

    def test_begin_is_noop_with_autocommit_and_behavior_flag(self):
        """Test that begin() doesn't send BEGIN when autocommit=True and behavior flag is set."""
        mock_connection = MagicMock()
        mock_connection.credentials.autocommit = True
        mock_connection.transaction_open = False

        # Enable behavior flag
        self.adapter.connections.set_skip_transactions_checker(lambda: True)

        with mock.patch.object(
            self.adapter.connections, "get_thread_connection", return_value=mock_connection
        ):
            with mock.patch.object(self.adapter.connections, "add_begin_query") as mock_add_begin:
                self.adapter.connections.begin()

        # Should not have called add_begin_query
        mock_add_begin.assert_not_called()

    def test_commit_is_noop_with_autocommit_and_behavior_flag(self):
        """Test that commit() doesn't send COMMIT when autocommit=True and behavior flag is set."""
        mock_connection = MagicMock()
        mock_connection.credentials.autocommit = True
        mock_connection.transaction_open = True

        # Enable behavior flag
        self.adapter.connections.set_skip_transactions_checker(lambda: True)

        with mock.patch.object(
            self.adapter.connections, "get_thread_connection", return_value=mock_connection
        ):
            with mock.patch.object(
                self.adapter.connections, "add_commit_query"
            ) as mock_add_commit:
                self.adapter.connections.commit()

        # Should not have called add_commit_query
        mock_add_commit.assert_not_called()

    def test_rollback_is_noop_with_autocommit_and_behavior_flag(self):
        """Test that rollback_if_open() doesn't rollback when autocommit=True and behavior flag is set."""
        mock_connection = MagicMock()
        mock_connection.credentials.autocommit = True
        mock_connection.transaction_open = True
        mock_connection.handle = MagicMock()

        # Enable behavior flag
        self.adapter.connections.set_skip_transactions_checker(lambda: True)

        with mock.patch.object(
            self.adapter.connections, "get_thread_connection", return_value=mock_connection
        ):
            # This should be a no-op
            self.adapter.connections.rollback_if_open()

        # Should not have called rollback on the handle
        mock_connection.handle.rollback.assert_not_called()

    def test_begin_sends_begin_without_autocommit(self):
        """Test that begin() sends BEGIN when autocommit=False."""
        mock_connection = MagicMock()
        mock_connection.credentials.autocommit = False
        mock_connection.transaction_open = False

        with mock.patch.object(
            self.adapter.connections, "get_thread_connection", return_value=mock_connection
        ):
            with mock.patch.object(self.adapter.connections, "add_begin_query") as mock_add_begin:
                self.adapter.connections.begin()

        # Should have called add_begin_query
        mock_add_begin.assert_called_once()

    def test_begin_sends_begin_with_autocommit_but_no_behavior_flag(self):
        """Test that begin() sends BEGIN when autocommit=True but behavior flag is NOT set."""
        mock_connection = MagicMock()
        mock_connection.credentials.autocommit = True
        mock_connection.transaction_open = False

        # Behavior flag NOT set (checker returns False)
        self.adapter.connections.set_skip_transactions_checker(lambda: False)

        with mock.patch.object(
            self.adapter.connections, "get_thread_connection", return_value=mock_connection
        ):
            with mock.patch.object(self.adapter.connections, "add_begin_query") as mock_add_begin:
                self.adapter.connections.begin()

        # Should have called add_begin_query because behavior flag is not set
        mock_add_begin.assert_called_once()

    def test_commit_sends_commit_with_autocommit_but_no_behavior_flag(self):
        """Test that commit() sends COMMIT when autocommit=True but behavior flag is NOT set."""
        mock_connection = MagicMock()
        mock_connection.credentials.autocommit = True
        mock_connection.transaction_open = True
        mock_connection.name = "test_connection"  # Required for logging events

        # Behavior flag NOT set (checker returns False)
        self.adapter.connections.set_skip_transactions_checker(lambda: False)

        with mock.patch.object(
            self.adapter.connections, "get_thread_connection", return_value=mock_connection
        ):
            with mock.patch.object(
                self.adapter.connections, "add_commit_query"
            ) as mock_add_commit:
                self.adapter.connections.commit()

        # Should have called add_commit_query because behavior flag is not set
        mock_add_commit.assert_called_once()

    @mock.patch("redshift_connector.connect", MagicMock())
    def test_retryable_exceptions_is_tuple_when_retry_all_true(self):
        """Test that retryable_exceptions is a proper tuple when retry_all=True.

        This is a regression test for a bug where retryable_exceptions was set to
        a single exception class instead of a tuple, causing a TypeError:
        'type' object is not iterable when the base class called
        tuple(retryable_exceptions).
        """
        from dbt.adapters.redshift.connections import RedshiftConnectionManager

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
                "retries": 1,
                "retry_all": True,
            }
        )

        # Mock retry_connection to capture the retryable_exceptions argument
        captured_exceptions = {}

        def capture_retry_connection(
            connection, connect, logger, retry_limit, retryable_exceptions
        ):
            captured_exceptions["value"] = retryable_exceptions
            # Return a mock connection to avoid actual connection
            connection.state = "open"
            connection.handle = MagicMock()
            return connection

        with mock.patch.object(
            RedshiftConnectionManager, "retry_connection", side_effect=capture_retry_connection
        ):
            with mock.patch.object(
                RedshiftConnectionManager, "_get_backend_pid", return_value=None
            ):
                RedshiftConnectionManager.open(connection_mock)

        # Verify that retryable_exceptions is a tuple (iterable)
        # This should not raise TypeError: 'type' object is not iterable
        retryable = captured_exceptions["value"]
        assert isinstance(retryable, tuple), f"Expected tuple, got {type(retryable)}"
        # Verify it contains the expected exception type
        assert redshift_connector.Error in retryable
        # Verify tuple() works on it (this is what the base class does)
        assert tuple(retryable) == retryable
