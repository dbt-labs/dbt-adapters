"""
Create a connections.toml file at ~/.snowflake/connections.toml
or you can specify a different folder using env variable SNOWFLAKE_HOME.

The file should have an entry similar to the following
with your credentials. Any type of authentication can be used.

[default]
user = "test_user"
warehouse = "test_warehouse"
database = "test_database"
schema = "test_schema"
role = "test_role"
password = "test_password"
authenticator = "snowflake"

You can name you connection something other than "default" by also setting
the SNOWFLAKE_DEFAULT_CONNECTION_NAME environment variable.

On Linux and Mac OS you will need to set the following
permissions on your connections.toml or you will receive an error.

chown $USER ~/.snowflake/connections.toml
chmod 0600 ~/.snowflake/connections.toml

"""

import os

from dbt.tests.util import run_dbt
import pytest


class TestConnectionName:
    @pytest.fixture(scope="class", autouse=True)
    def dbt_profile_target(self):
        return {
            "type": "snowflake",
            "threads": 4,
            "account": os.getenv("SNOWFLAKE_TEST_ACCOUNT"),
            "database": os.getenv("SNOWFLAKE_TEST_DATABASE"),
            "warehouse": os.getenv("SNOWFLAKE_TEST_WAREHOUSE"),
            "connection_name": os.getenv("SNOWFLAKE_DEFAULT_CONNECTION_NAME", "default"),
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {"my_model.sql": "select 1 as id"}

    def test_connection(self, project):
        run_dbt()
