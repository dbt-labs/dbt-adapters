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
role = "SNOWFLAKE_TEST_ROLE"
user = "SNOWFLAKE_TEST_USER"
warehouse = "SNOWFLAKE_TEST_WAREHOUSE"

By putting the password in the connections.toml file and the connection_name in the
profiles.yml, we can test that we can connect based on credentials in the connections.toml

"""

from dbt.tests.util import run_dbt
import tempfile
import pytest
import os

connections_toml_template = """
[{name}]
account = "{account}"
authenticator = "snowflake"
database = "{database}"
password = "{password}"
role = "{role}"
user = "{user}"
warehouse = "{warehouse}"
"""


class TestConnectionName:

    @pytest.fixture(scope="class", autouse=True)
    def dbt_profile_target(self, tmp_path):
        # We are returning a profile that does not contain the password
        return {
            "type": "snowflake",
            "threads": 4,
            "account": os.getenv("SNOWFLAKE_TEST_ACCOUNT"),
            "database": os.getenv("SNOWFLAKE_TEST_DATABASE"),
            "warehouse": os.getenv("SNOWFLAKE_TEST_WAREHOUSE"),
            "connection_name": "default",
            "connections_file_path": tmp_path / "connections.toml",
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {"my_model.sql": "select 1 as id"}

    # Test that we can write a connections.toml and use it to connect
    def test_connection(self, project, dbt_profile_target):
        connections_toml = dbt_profile_target.connections_file_path

        # We are creating a toml file that contains the password
        connections_toml.write_text(
            connections_toml_template.format(
                name=dbt_profile_target.connection_name,
                account=dbt_profile_target.account,
                database=dbt_profile_target.database,
                password=os.getenv("SNOWFLAKE_TEST_PASSWORD"),
                role=os.getenv("SNOWFLAKE_TEST_ROLE"),
                user=os.getenv("SNOWFLAKE_TEST_USER"),
                warehouse=dbt_profile_target.warehouse,
            )
        )
        connections_toml.chmod(0o600)

        run_dbt()

        connections_toml.unlink()
