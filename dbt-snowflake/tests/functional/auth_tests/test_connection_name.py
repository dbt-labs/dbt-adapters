"""
This class sets the profile to use a connection_name and connections_file_path.
The connection_name is "default" and the connections.toml file is set to "~/connections.toml"
During the test we set the HOME to a temporary folder where we write this file.

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

By putting the password in the connections.toml file and the connection_name
& connections_file_path in the profiles.yml, we can test that we can connect
based on credentials in the connections.toml

"""

from dbt.tests.util import run_dbt
import tempfile
import pytest
import os
from pathlib import Path

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
    def dbt_profile_target(self):
        # We are returning a profile that does not contain the password
        return {
            "type": "snowflake",
            "threads": 4,
            "account": os.getenv("SNOWFLAKE_TEST_ACCOUNT"),
            "database": os.getenv("SNOWFLAKE_TEST_DATABASE"),
            "warehouse": os.getenv("SNOWFLAKE_TEST_WAREHOUSE"),
            "connection_name": "default",
            "connections_file_path": "~/connections.toml",
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {"my_model.sql": "select 1 as id"}

    # Test that we can write a connections.toml and use it to connect
    def test_connection(self, project, tmp_path, monkeypatch):

        with monkeypatch.context() as m:
            # Set HOME to our temporary folder for later tilde expansion in the driver
            m.setenv("HOME", str(tmp_path.absolute()))

            connections_toml = tmp_path / "connections.toml"

            # We are creating a toml file that contains the password
            connections_toml.write_text(
                connections_toml_template.format(
                    name="default",
                    account=os.getenv("SNOWFLAKE_TEST_ACCOUNT"),
                    database=os.getenv("SNOWFLAKE_TEST_DATABASE"),
                    password=os.getenv("SNOWFLAKE_TEST_PASSWORD"),
                    role=os.getenv("SNOWFLAKE_TEST_ROLE"),
                    user=os.getenv("SNOWFLAKE_TEST_USER"),
                    warehouse=os.getenv("SNOWFLAKE_TEST_WAREHOUSE"),
                )
            )
            connections_toml.chmod(0o600)

            run_dbt()
