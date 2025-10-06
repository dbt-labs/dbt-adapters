import pytest
import os

# Import the fuctional fixtures as a plugin
# Note: fixtures with session scope need to be local

pytest_plugins = ["dbt.tests.fixtures.project"]


# The profile dictionary, used to write out profiles.yml
@pytest.fixture(scope="class")
def dbt_profile_target():
    return {
        "type": "snowflake",
        "threads": 4,
        "account": "localstack",
        "host": "snowflake.localhost.localstack.cloud",
        "port": 4566,
        "user": "test",
        "password": "test",
        "database": "TEST",
        "warehouse": "test",
    }
