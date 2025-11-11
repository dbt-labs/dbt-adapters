"""Unit tests for platform_detection_timeout_seconds configuration"""

import pytest
from dbt.adapters.snowflake.connections import SnowflakeCredentials


class TestPlatformDetectionTimeout:
    """Test suite for platform_detection_timeout_seconds configuration"""

    def test_default_platform_detection_timeout(self):
        """Test that platform_detection_timeout_seconds defaults to 0.0"""
        creds = SnowflakeCredentials(
            account="test_account",
            user="test_user",
            password="test_password",
            database="test_db",
            warehouse="test_warehouse",
            schema="test_schema",
        )
        assert creds.platform_detection_timeout_seconds == 0.0

    def test_custom_platform_detection_timeout(self):
        """Test that platform_detection_timeout_seconds accepts custom values"""
        creds = SnowflakeCredentials(
            account="test_account",
            user="test_user",
            password="test_password",
            database="test_db",
            warehouse="test_warehouse",
            schema="test_schema",
            platform_detection_timeout_seconds=5.5,
        )
        assert creds.platform_detection_timeout_seconds == 5.5

    def test_platform_detection_timeout_in_connection_keys(self):
        """Test that platform_detection_timeout_seconds is included in connection keys"""
        creds = SnowflakeCredentials(
            account="test_account",
            user="test_user",
            password="test_password",
            database="test_db",
            warehouse="test_warehouse",
            schema="test_schema",
        )
        assert "platform_detection_timeout_seconds" in creds._connection_keys()

    @pytest.mark.parametrize("timeout_value", [0.0, 1.0, 5.5, 10.0, 30.0])
    def test_various_timeout_values(self, timeout_value):
        """Test that platform_detection_timeout_seconds accepts various float values"""
        creds = SnowflakeCredentials(
            account="test_account",
            user="test_user",
            password="test_password",
            database="test_db",
            warehouse="test_warehouse",
            schema="test_schema",
            platform_detection_timeout_seconds=timeout_value,
        )
        assert creds.platform_detection_timeout_seconds == timeout_value
