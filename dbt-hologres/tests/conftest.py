"""Pytest configuration for dbt-hologres tests."""
import pytest
import os


@pytest.fixture(scope="session")
def hologres_credentials():
    """Provide Hologres credentials from environment variables."""
    return {
        "host": os.getenv("DBT_HOLOGRES_HOST", "localhost"),
        "port": int(os.getenv("DBT_HOLOGRES_PORT", "80")),
        "user": os.getenv("DBT_HOLOGRES_USER", "test_user"),
        "password": os.getenv("DBT_HOLOGRES_PASSWORD", "test_password"),
        "database": os.getenv("DBT_HOLOGRES_DATABASE", "test_db"),
        "schema": os.getenv("DBT_HOLOGRES_SCHEMA", "public"),
    }


@pytest.fixture(scope="session")
def dbt_profile_target(hologres_credentials):
    """Provide dbt profile configuration for tests."""
    return {
        "type": "hologres",
        "threads": 1,
        "host": hologres_credentials["host"],
        "port": hologres_credentials["port"],
        "user": hologres_credentials["user"],
        "pass": hologres_credentials["password"],
        "dbname": hologres_credentials["database"],
        "schema": hologres_credentials["schema"],
    }
