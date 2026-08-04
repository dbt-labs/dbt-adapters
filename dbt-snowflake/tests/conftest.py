import pytest
import os

# Import the fuctional fixtures as a plugin
# Note: fixtures with session scope need to be local

pytest_plugins = ["dbt.tests.fixtures.project"]


# The profile dictionary, used to write out profiles.yml
@pytest.fixture(scope="class")
def dbt_profile_target():
    profile = {
        "type": "snowflake",
        "threads": 4,
        "account": os.getenv("SNOWFLAKE_TEST_ACCOUNT"),
        "database": os.getenv("SNOWFLAKE_TEST_DATABASE"),
        "warehouse": os.getenv("SNOWFLAKE_TEST_WAREHOUSE"),
    }

    # Support PAT or password authentication
    authenticator = os.getenv("SNOWFLAKE_TEST_AUTHENTICATOR")
    if authenticator:
        profile["authenticator"] = authenticator
        if os.getenv("SNOWFLAKE_TEST_TOKEN"):
            profile["token"] = os.getenv("SNOWFLAKE_TEST_TOKEN")
    if os.getenv("SNOWFLAKE_TEST_USER"):
        profile["user"] = os.getenv("SNOWFLAKE_TEST_USER")
    if os.getenv("SNOWFLAKE_TEST_PASSWORD"):
        profile["password"] = os.getenv("SNOWFLAKE_TEST_PASSWORD")

    # Optional parameters allow testing against local DEV Snowflake instances.
    if os.getenv("SNOWFLAKE_TEST_HOST"):
        profile["host"] = os.getenv("SNOWFLAKE_TEST_HOST")
    if os.getenv("SNOWFLAKE_TEST_PORT"):
        profile["port"] = int(os.getenv("SNOWFLAKE_TEST_PORT"))
    if os.getenv("SNOWFLAKE_TEST_PROTOCOL"):
        profile["protocol"] = os.getenv("SNOWFLAKE_TEST_PROTOCOL")
    return profile
