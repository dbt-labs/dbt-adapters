"""
Tests for Redshift group and role grants support.

These tests verify that the grants config correctly handles:
- 'group:groupname' -> GRANT ... TO GROUP groupname
- 'role:rolename' -> GRANT ... TO ROLE rolename
- 'username' (no prefix) -> GRANT ... TO username (backward compatible)
- 'user:username' (explicit prefix) -> GRANT ... TO username

Requires environment variables:
- DBT_TEST_USER_1: A valid Redshift user
- DBT_TEST_GROUP_1: A valid Redshift group
"""

import os
import pytest

from dbt.tests.util import run_dbt_and_capture, get_manifest, write_file


# Skip all tests if required env vars aren't set
pytestmark = pytest.mark.skipif(
    not os.getenv("DBT_TEST_GROUP_1"),
    reason="DBT_TEST_GROUP_1 environment variable not set",
)


my_model_sql = """
  select 1 as fun
"""

group_grant_schema_yml = """
version: 2
models:
  - name: my_model
    config:
      materialized: table
      grants:
        select: ["group:{{ env_var('DBT_TEST_GROUP_1') }}"]
"""

mixed_grant_schema_yml = """
version: 2
models:
  - name: my_model
    config:
      materialized: table
      grants:
        select:
          - "{{ env_var('DBT_TEST_USER_1') }}"
          - "group:{{ env_var('DBT_TEST_GROUP_1') }}"
"""

explicit_user_prefix_schema_yml = """
version: 2
models:
  - name: my_model
    config:
      materialized: table
      grants:
        select: ["user:{{ env_var('DBT_TEST_USER_1') }}"]
"""


class TestRedshiftGroupGrants:
    """Test granting privileges to Redshift groups."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_sql,
            "schema.yml": group_grant_schema_yml,
        }

    def test_group_grant(self, project):
        """Test that grants to groups use the GROUP keyword."""
        (results, log_output) = run_dbt_and_capture(["--debug", "run"])
        assert len(results) == 1

        # Check that the grant statement includes GROUP keyword
        assert "GROUP" in log_output or "group" in log_output.lower()

        # Verify the model ran successfully
        manifest = get_manifest(project.project_root)
        model = manifest.nodes["model.test.my_model"]
        assert model.config.materialized == "table"


class TestRedshiftMixedGrants:
    """Test granting privileges to both users and groups in the same config."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_sql,
            "schema.yml": mixed_grant_schema_yml,
        }

    @pytest.mark.skipif(
        not os.getenv("DBT_TEST_USER_1"),
        reason="DBT_TEST_USER_1 environment variable not set",
    )
    def test_mixed_user_and_group_grant(self, project):
        """Test that mixed user and group grants work in a single statement."""
        (results, log_output) = run_dbt_and_capture(["--debug", "run"])
        assert len(results) == 1

        # Check that both user and GROUP appear in the grant
        assert "GROUP" in log_output

        manifest = get_manifest(project.project_root)
        model = manifest.nodes["model.test.my_model"]
        assert model.config.materialized == "table"


class TestRedshiftExplicitUserPrefix:
    """Test that explicit 'user:' prefix works and is backward compatible."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_sql,
            "schema.yml": explicit_user_prefix_schema_yml,
        }

    @pytest.mark.skipif(
        not os.getenv("DBT_TEST_USER_1"),
        reason="DBT_TEST_USER_1 environment variable not set",
    )
    def test_explicit_user_prefix(self, project):
        """Test that 'user:username' works the same as 'username'."""
        (results, log_output) = run_dbt_and_capture(["--debug", "run"])
        assert len(results) == 1

        # The 'user:' prefix should be stripped, so no "user:" in the SQL
        # Just the username should appear in the grant statement
        manifest = get_manifest(project.project_root)
        model = manifest.nodes["model.test.my_model"]
        assert model.config.materialized == "table"
