import os

from dbt.tests.util import run_dbt
import pytest


class TestPATAuth:
    @pytest.fixture(scope="class", autouse=True)
    def dbt_profile_target(self):
        return {
            "type": "snowflake",
            "threads": 4,
            "account": os.getenv("SNOWFLAKE_TEST_ACCOUNT"),
            "user": os.getenv("SNOWFLAKE_TEST_USER"),
            "authenticator": "programmatic_access_token",
            "token": os.getenv("SNOWFLAKE_TEST_TOKEN"),
            "database": os.getenv("SNOWFLAKE_TEST_DATABASE"),
            "warehouse": os.getenv("SNOWFLAKE_TEST_WAREHOUSE"),
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {"my_model.sql": "select 1 as id"}

    def test_connection(self, project):
        run_dbt()
