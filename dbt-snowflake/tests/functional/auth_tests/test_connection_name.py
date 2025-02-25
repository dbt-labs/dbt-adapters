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

connections_toml_template = """[{name}]
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

    @pytest.fixture(autouse=True)
    def connection_name(self, tmp_path, monkeypatch):
        # We are creating a toml file that contains the password
        connection_name = "default"
        config_toml = tmp_path / "config.toml"
        connections_toml = tmp_path / "connections.toml"
        monkeypatch.setenv("SNOWFLAKE_HOME", str(tmp_path.absolute()))

        config_toml.write_text('default_connection_name = "default"\n')
        config_toml.chmod(0o600)

        connections_toml.write_text(
            connections_toml_template.format(
                name=connection_name,
                account=os.getenv("SNOWFLAKE_TEST_ACCOUNT"),
                database=os.getenv("SNOWFLAKE_TEST_DATABASE"),
                password=os.getenv("SNOWFLAKE_TEST_PASSWORD"),
                role=os.getenv("SNOWFLAKE_TEST_ROLE"),
                user=os.getenv("SNOWFLAKE_TEST_USER"),
                warehouse=os.getenv("SNOWFLAKE_TEST_WAREHOUSE"),
            ).strip()
        )
        connections_toml.chmod(0o600)

        yield connection_name

        monkeypatch.delenv("SNOWFLAKE_HOME")
        config_toml.unlink()
        connections_toml.unlink()

    def test_connection(self, project):
        run_dbt()
