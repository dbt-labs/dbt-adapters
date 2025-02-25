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

connections_toml_template = f"""[default]
account = "{account}"
authenticator = "snowflake"
database = "{database}"
password = "{password}"
role = "{role}"
user = "{user}"
warehouse = "{warehouse}"
"""

import os
import pytest
import tempfile

from dbt.tests.util import run_dbt


class TestConnectionName:

    @pytest.fixture(scope="class", autouse=True)
    def connection_name(self):
        # We are creating a toml file that contains the password
        snowflake_home_dir = os.path.expanduser("~/.snowflake")
        config_toml = os.path.join(snowflake_home_dir, "config.toml")
        connections_toml = os.path.join(snowflake_home_dir, "connections.toml")
        os.environ["SNOWFLAKE_HOME"] = snowflake_home_dir

        if not os.path.exists(snowflake_home_dir):
            os.makedirs(snowflake_home_dir)

        with open(config_toml, "w") as f:
            f.write('default_connection_name = "default"')
        os.chmod(config_toml, 0o600)

        with open(connections_toml, "w") as f:
            f.write(
                connections_toml_template.format(
                    account=os.getenv("SNOWFLAKE_TEST_ACCOUNT"),
                    database=os.getenv("SNOWFLAKE_TEST_DATABASE"),
                    password=os.getenv("SNOWFLAKE_TEST_PASSWORD"),
                    role=os.getenv("SNOWFLAKE_TEST_ROLE"),
                    user=os.getenv("SNOWFLAKE_TEST_USER"),
                    warehouse=os.getenv("SNOWFLAKE_TEST_WAREHOUSE"),
                ).strip()
            )
        os.chmod(connections_toml, 0o600)

        yield "default"

        del os.environ["SNOWFLAKE_HOME"]
        os.unlink(config_toml)
        os.unlink(connections_toml)

    @pytest.fixture(scope="class", autouse=True)
    def dbt_profile_target(self, connection_name):
        # We are returning a profile that does not contain the password
        return {
            "type": "snowflake",
            "threads": 4,
            "account": os.getenv("SNOWFLAKE_TEST_ACCOUNT"),
            "database": os.getenv("SNOWFLAKE_TEST_DATABASE"),
            "warehouse": os.getenv("SNOWFLAKE_TEST_WAREHOUSE"),
            "connection_name": connection_name,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {"my_model.sql": "select 1 as id"}

    def test_connection(self, project):
        run_dbt()
