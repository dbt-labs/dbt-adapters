"""
This class creates a connections.toml file at ~/.snowflake/connections.toml
or a different folder using env variable SNOWFLAKE_HOME.

The script will populate the toml file based on the testing environment variables
but will look something like:

[default]
account = "SNOWFLAKE_TEST_ACCOUNT"
authenticator = "snowflake"
database = "SNOWFLAKE_TEST_DATABASE"
password = "SNOWFLAKE_TEST_PASSWORD"
role = "DBT_TEST_USER_1"
user = "SNOWFLAKE_TEST_USER"
warehouse = "SNOWFLAKE_TEST_WAREHOUSE"

By putting the password in the connections.toml file and the connection_name in the
profiles.yml, we can test that we can connect based on credentials in the connections.toml

"""

import os
import pytest
import tempfile

from dbt.tests.util import run_dbt


class TestConnectionName:
    @pytest.fixture(scope="class", autouse=True)
    def dbt_profile_target(self):
        # We are returning a profile that does not contain the password
        return {
            "type": "snowflake",
            "threads": 4,
            "account": os.getenv("SNOWFLAKE_TEST_ACCOUNT"),
            "database": os.getenv("SNOWFLAKE_TEST_DATABASE"),
            "warehouse": os.getenv("SNOWFLAKE_TEST_WAREHOUSE"),
            "connection_name": "default",
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {"my_model.sql": "select 1 as id"}

    def test_connection(self, project):

        # We are creating a toml file that contains the password
        connections_for_toml = f"""
[default]
account = "{ os.getenv("SNOWFLAKE_TEST_ACCOUNT") }"
authenticator = "snowflake"
database = "{ os.getenv("SNOWFLAKE_TEST_DATABASE") }"
password = "{ os.getenv("SNOWFLAKE_TEST_PASSWORD") }"
role = "{ os.getenv("DBT_TEST_USER_1") }"
user = "{ os.getenv("SNOWFLAKE_TEST_USER") }"
warehouse = "{ os.getenv("SNOWFLAKE_TEST_WAREHOUSE") }"
"""
        temp_dir = tempfile.gettempdir()
        connections_toml = os.path.join(temp_dir, "connections.toml")
        os.environ["SNOWFLAKE_HOME"] = temp_dir

        with open(connections_toml, "w") as f:
            f.write(connections_for_toml)
        os.chmod(connections_toml, 0o600)

        run_dbt()

        os.unlink(connections_toml)
