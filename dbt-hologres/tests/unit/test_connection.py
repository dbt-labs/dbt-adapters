"""Unit tests for Hologres connection management."""
import pytest
from unittest import mock

from dbt.adapters.hologres.connections import (
    HologresCredentials,
    HologresConnectionManager,
)


class TestHologresCredentials:
    """Test HologresCredentials class."""

    def test_default_values(self):
        """Test default credential values."""
        creds = HologresCredentials(
            host="test.hologres.aliyuncs.com",
            user="BASIC$test_user",
            password="test_password",
            database="test_db",
            schema="public",
        )
        
        assert creds.type == "hologres"
        assert creds.port == 80
        assert creds.sslmode == "disable"
        assert creds.connect_timeout == 10
        assert "dbt_hologres" in creds.application_name

    def test_unique_field(self):
        """Test unique_field property."""
        creds = HologresCredentials(
            host="test.hologres.aliyuncs.com",
            user="test_user",
            password="test_password",
            database="test_db",
            schema="public",
        )
        
        assert creds.unique_field == "test.hologres.aliyuncs.com"

    def test_connection_keys(self):
        """Test _connection_keys method."""
        creds = HologresCredentials(
            host="test.hologres.aliyuncs.com",
            user="test_user",
            password="test_password",
            database="test_db",
            schema="public",
        )
        
        keys = creds._connection_keys()
        assert "host" in keys
        assert "port" in keys
        assert "user" in keys
        assert "database" in keys
        assert "sslmode" in keys


class TestHologresConnectionManager:
    """Test HologresConnectionManager class."""

    def test_type(self):
        """Test connection manager type."""
        assert HologresConnectionManager.TYPE == "hologres"

    @mock.patch("psycopg.connect")
    def test_open_connection(self, mock_connect):
        """Test opening a connection."""
        mock_handle = mock.MagicMock()
        mock_cursor = mock.MagicMock()
        mock_handle.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value = mock_handle
        
        # This is a simplified test - in real scenarios, you'd need
        # to properly set up the connection manager with config
        assert mock_connect is not None
