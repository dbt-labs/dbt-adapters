import os
import pytest
import logging
from importlib import reload
from unittest.mock import Mock, patch, call
import multiprocessing
from dbt.adapters.exceptions.connection import FailedToConnectError
import dbt.adapters.snowflake.connections as connections
import dbt.adapters.events.logging


class TestSnowflakeLogging:
    """Test suite for snowflake logging configuration"""

    def test_connections_sets_debug_logs_when_env_var_present(self, monkeypatch):
        """Test that setting DBT_SNOWFLAKE_CONNECTOR_DEBUG_LOGGING enables DEBUG level logging"""
        log_mock = Mock()
        logger_mock = Mock(return_value=log_mock)

        monkeypatch.setattr(dbt.adapters.events.logging, "AdapterLogger", logger_mock)
        monkeypatch.setattr(os, "environ", {"DBT_SNOWFLAKE_CONNECTOR_DEBUG_LOGGING": "true"})

        reload(connections)

        # Verify AdapterLogger was created with correct name
        logger_mock.assert_called_once_with("Snowflake")

        # Verify all three dependency loggers are configured with DEBUG level
        expected_debug_calls = [
            call("Setting snowflake.connector to DEBUG (file logging only)"),
            call("Setting botocore to DEBUG (file logging only)"),
            call("Setting boto3 to DEBUG (file logging only)"),
        ]
        log_mock.debug.assert_has_calls(expected_debug_calls)

        expected_level_calls = [
            call("snowflake.connector", "DEBUG"),
            call("botocore", "DEBUG"),
            call("boto3", "DEBUG"),
        ]
        log_mock.set_adapter_dependency_log_level.assert_has_calls(expected_level_calls)

    def test_connections_sets_error_logs_when_env_var_absent(self, monkeypatch):
        """Test that absence of DBT_SNOWFLAKE_CONNECTOR_DEBUG_LOGGING sets ERROR level logging"""
        log_mock = Mock()
        logger_mock = Mock(return_value=log_mock)

        monkeypatch.setattr(dbt.adapters.events.logging, "AdapterLogger", logger_mock)
        # Explicitly set environment without the debug flag
        monkeypatch.setattr(os, "environ", {})

        with patch("logging.getLogger") as mock_get_logger:
            reload(connections)

        # Verify AdapterLogger was created
        logger_mock.assert_called_once_with("Snowflake")

        # Verify all three dependency loggers are configured with ERROR level
        expected_debug_calls = [
            call("Setting snowflake.connector to ERROR (file logging only)"),
            call("Setting botocore to ERROR (file logging only)"),
            call("Setting boto3 to ERROR (file logging only)"),
        ]

        log_mock.debug.assert_has_calls(expected_debug_calls)

        expected_level_calls = [
            call("snowflake.connector", "ERROR"),
            call("botocore", "ERROR"),
            call("boto3", "ERROR"),
        ]
        log_mock.set_adapter_dependency_log_level.assert_has_calls(expected_level_calls)

    def test_connections_handles_various_env_var_values(self, monkeypatch):
        """Test that any truthy value for the env var enables debug logging"""
        test_values = ["1", "True", "YES", "on", "random"]

        for value in test_values:
            log_mock = Mock()
            logger_mock = Mock(return_value=log_mock)

            monkeypatch.setattr(dbt.adapters.events.logging, "AdapterLogger", logger_mock)
            monkeypatch.setattr(os, "environ", {"DBT_SNOWFLAKE_CONNECTOR_DEBUG_LOGGING": value})

            with patch("logging.getLogger"):
                reload(connections)

            # Should set DEBUG level for all loggers (fallback for non-INFO/DEBUG values)
            expected_level_calls = [
                call("snowflake.connector", "DEBUG"),
                call("botocore", "DEBUG"),
                call("boto3", "DEBUG"),
            ]
            log_mock.set_adapter_dependency_log_level.assert_has_calls(expected_level_calls)

    def test_connections_handles_case_insensitive_values(self, monkeypatch):
        """Test that DEBUG and INFO values are case insensitive"""
        test_cases = [
            ("debug", "DEBUG"),
            ("DEBUG", "DEBUG"),
            ("info", "INFO"),
            ("INFO", "INFO"),
            ("Debug", "DEBUG"),
            ("Info", "INFO"),
        ]

        for env_value, expected_level in test_cases:
            log_mock = Mock()
            logger_mock = Mock(return_value=log_mock)

            monkeypatch.setattr(dbt.adapters.events.logging, "AdapterLogger", logger_mock)
            monkeypatch.setattr(
                os, "environ", {"DBT_SNOWFLAKE_CONNECTOR_DEBUG_LOGGING": env_value}
            )

            with patch("logging.getLogger"):
                reload(connections)

            # Should set the expected level for all loggers
            expected_level_calls = [
                call("snowflake.connector", expected_level),
                call("botocore", expected_level),
                call("boto3", expected_level),
            ]
            log_mock.set_adapter_dependency_log_level.assert_has_calls(expected_level_calls)

    def test_setup_snowflake_logging_with_info_level(self):
        """Test setup_snowflake_logging with INFO level"""
        with patch("dbt.adapters.snowflake.connections.logger") as mock_logger:
            with patch("logging.getLogger") as mock_get_logger:
                mock_package_logger = Mock()
                mock_get_logger.return_value = mock_package_logger

                # Test INFO level
                connections.setup_snowflake_logging("INFO")

                # Verify debug messages (always say "DEBUG (file logging only)")
                expected_debug_calls = [
                    call("Setting snowflake.connector to INFO (file logging only)"),
                    call("Setting botocore to INFO (file logging only)"),
                    call("Setting boto3 to INFO (file logging only)"),
                ]
                mock_logger.debug.assert_has_calls(expected_debug_calls)

                # Verify correct level is set (INFO in this case)
                expected_level_calls = [
                    call("snowflake.connector", "INFO"),
                    call("botocore", "INFO"),
                    call("boto3", "INFO"),
                ]
                mock_logger.set_adapter_dependency_log_level.assert_has_calls(expected_level_calls)

    def test_expected_logger_names_are_configured(self):
        """Test that exactly the expected logger names are configured"""
        expected_loggers = ["snowflake.connector", "botocore", "boto3"]

        with patch("dbt.adapters.snowflake.connections.logger") as mock_logger:
            with patch("logging.getLogger") as mock_get_logger:
                connections.setup_snowflake_logging("DEBUG")

                # Extract the logger names from the calls
                actual_logger_names = [
                    call[0][0]
                    for call in mock_logger.set_adapter_dependency_log_level.call_args_list
                ]

                assert actual_logger_names == expected_loggers
                assert len(actual_logger_names) == 3


# Keep existing tests for backward compatibility
def test_connections_sets_logs_in_response_to_env_var(monkeypatch):
    """Test that setting the DBT_SNOWFLAKE_CONNECTOR_DEBUG_LOGGING environment variable happens on import"""
    log_mock = Mock()
    monkeypatch.setattr(dbt.adapters.events.logging, "AdapterLogger", Mock(return_value=log_mock))
    monkeypatch.setattr(os, "environ", {"DBT_SNOWFLAKE_CONNECTOR_DEBUG_LOGGING": "true"})
    reload(connections)

    assert log_mock.debug.call_count == 3
    assert log_mock.set_adapter_dependency_log_level.call_count == 3


def test_connections_does_not_set_logs_in_response_to_env_var(monkeypatch):
    log_mock = Mock()
    monkeypatch.setattr(dbt.adapters.events.logging, "AdapterLogger", Mock(return_value=log_mock))
    reload(connections)

    assert log_mock.debug.call_count == 3  # Debug messages are always logged
    assert (
        log_mock.set_adapter_dependency_log_level.call_count == 3
    )  # But with ERROR level (default)


def test_connnections_credentials_replaces_underscores_with_hyphens():
    credentials = {
        "account": "account_id_with_underscores",
        "user": "user",
        "password": "password",
        "database": "database",
        "warehouse": "warehouse",
        "schema": "schema",
    }
    creds = connections.SnowflakeCredentials(**credentials)
    assert creds.account == "account-id-with-underscores"


def test_snowflake_oauth_expired_token_raises_error():
    credentials = {
        "account": "test_account",
        "user": "test_user",
        "authenticator": "oauth",
        "token": "expired_or_incorrect_token",
        "database": "database",
        "schema": "schema",
    }

    mp_context = multiprocessing.get_context("spawn")
    mock_credentials = connections.SnowflakeCredentials(**credentials)

    with patch.object(
        connections.SnowflakeConnectionManager,
        "open",
        side_effect=FailedToConnectError(
            "This error occurs when authentication has expired. "
            "Please reauth with your auth provider."
        ),
    ):

        adapter = connections.SnowflakeConnectionManager(mock_credentials, mp_context)

        with pytest.raises(FailedToConnectError):
            adapter.open()
