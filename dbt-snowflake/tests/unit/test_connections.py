import os
import pytest
from importlib import reload
from unittest.mock import Mock, patch
import multiprocessing
from dbt.adapters.exceptions.connection import FailedToConnectError
import dbt.adapters.snowflake.connections as connections
import dbt.adapters.events.logging
from dbt.adapters.events.types import AdapterEventWarning


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

    assert log_mock.debug.call_count == 0
    assert log_mock.set_adapter_dependency_log_level.call_count == 0


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


@pytest.mark.parametrize(
    "oauth_client_id, oauth_client_secret",
    [(None, "test_secret"), ("test_client", None), ("test_client", "test_secret")],
)
def test_snowflake_oauth_authenticator_not_set_logs_warning(oauth_client_id, oauth_client_secret):
    credentials = {
        "account": "test_account",
        "user": "test_user",
        "database": "database",
        "schema": "schema",
        "authenticator": "not oauth or OAUTH_AUTHORIZATION_CODE",
    }
    if oauth_client_id is not None:
        credentials["oauth_client_id"] = oauth_client_id
    if oauth_client_secret is not None:
        credentials["oauth_client_secret"] = oauth_client_secret

    with patch("dbt.adapters.snowflake.connections.warn_or_error") as mock_warn:
        connections.SnowflakeCredentials(**credentials)

        # Verify the warning was triggered
        mock_warn.assert_called_once()
        args, _ = mock_warn.call_args
        assert isinstance(args[0], AdapterEventWarning)
        assert "Authenticator is not set to oauth nor OAUTH_AUTHORIZATION_CODE" in args[0].base_msg


def test_snowflake_non_oauth_authenticator_without_oauth_args_does_not_log_warnings():
    credentials = {
        "account": "test_account",
        "user": "test_user",
        "database": "database",
        "schema": "schema",
        "authenticator": "not oauth or OAUTH_AUTHORIZATION_CODE",
    }

    with patch("dbt.adapters.snowflake.connections.warn_or_error") as mock_warn:
        connections.SnowflakeCredentials(**credentials)

        # Verify no warning was triggered
        mock_warn.assert_not_called()


@pytest.mark.parametrize(
    "authenticator",
    ["oauth", "OAUTH_AUTHORIZATION_CODE"],
)
def test_snowflake_oauth_with_params_does_not_log_warning(authenticator):
    credentials = {
        "account": "test_account",
        "user": "test_user",
        "database": "database",
        "schema": "schema",
        "authenticator": authenticator,
        "oauth_client_id": "test_id",
        "oauth_client_secret": "test_secret",
    }
    print(authenticator)

    with patch("dbt.adapters.snowflake.connections.warn_or_error") as mock_warn:
        connections.SnowflakeCredentials(**credentials)

        # Verify no warning was triggered
        mock_warn.assert_not_called()
