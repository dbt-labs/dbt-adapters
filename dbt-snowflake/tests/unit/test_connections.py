import os
import pytest
import logging
from importlib import reload
from unittest.mock import Mock, patch, call
import multiprocessing
from dbt.adapters.exceptions.connection import FailedToConnectError
import dbt.adapters.snowflake.connections as connections
import dbt.adapters.events.logging


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


def test_adbc_auth_args_basic():
    """Test that adbc_auth_args builds correct kwargs for basic user/pass auth."""
    creds = connections.SnowflakeCredentials(
        account="test-account",
        user="test_user",
        password="test_password",
        database="test_database",
        warehouse="test_warehouse",
        schema="public",
    )
    args = creds.adbc_auth_args()
    assert args["adbc.snowflake.sql.account"] == "test-account"
    assert args["username"] == "test_user"
    assert args["password"] == "test_password"
    assert args["adbc.snowflake.sql.db"] == "test_database"
    assert args["adbc.snowflake.sql.warehouse"] == "test_warehouse"
    assert args["adbc.snowflake.sql.schema"] == "public"


def test_adbc_auth_args_externalbrowser():
    """Test that externalbrowser auth maps to auth_ext_browser."""
    creds = connections.SnowflakeCredentials(
        account="test-account",
        user="test_user",
        database="test_database",
        schema="public",
        authenticator="externalbrowser",
    )
    args = creds.adbc_auth_args()
    assert args["adbc.snowflake.sql.auth_type"] == "auth_ext_browser"


def test_adbc_auth_args_oauth():
    """Test that oauth auth maps correctly."""
    creds = connections.SnowflakeCredentials(
        account="test-account",
        database="test_database",
        schema="public",
        authenticator="oauth",
        token="my-oauth-token",
    )
    args = creds.adbc_auth_args()
    assert args["adbc.snowflake.sql.auth_type"] == "auth_oauth"
    assert args["adbc.snowflake.sql.client_option.auth_token"] == "my-oauth-token"


def test_adbc_auth_args_role():
    """Test that role is included when set."""
    creds = connections.SnowflakeCredentials(
        account="test-account",
        user="test_user",
        database="test_database",
        schema="public",
        role="test_role",
    )
    args = creds.adbc_auth_args()
    assert args["adbc.snowflake.sql.role"] == "test_role"


def test_adbc_auth_args_private_key_path():
    """Test that private_key_path is passed through."""
    creds = connections.SnowflakeCredentials(
        account="test-account",
        user="test_user",
        database="test_database",
        schema="public",
        private_key_path="/tmp/test_key.p8",
    )
    args = creds.adbc_auth_args()
    assert args["adbc.snowflake.sql.client_option.jwt_private_key"] == "/tmp/test_key.p8"
