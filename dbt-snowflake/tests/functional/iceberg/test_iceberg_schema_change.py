import pytest
from dbt.tests.util import run_dbt, write_file


_MODEL_ICEBERG_BASE = """
{{
  config(
    materialized="incremental",
    table_format="iceberg",
    external_volume="s3_iceberg_snow",
    on_schema_change="append_new_columns"
  )
}}

select 1 as id,
cast('John' as varchar) as first_name
"""

_MODEL_ICEBERG_ADDED_COLUMN = """
{{
  config(
    materialized="incremental",
    table_format="iceberg",
    external_volume="s3_iceberg_snow",
    on_schema_change="append_new_columns"
  )
}}

select 1 as id,
cast('John' as varchar) as first_name,
cast('Smith' as varchar) as last_name
"""

_MODEL_ICEBERG_ADDED_STRING_COLUMN = """
{{
  config(
    materialized="incremental",
    table_format="iceberg",
    external_volume="s3_iceberg_snow",
    on_schema_change="append_new_columns"
  )
}}

select 1 as id,
cast('John' as varchar) as first_name,
cast('Smith' as string) as last_name
"""

_MODEL_ICEBERG_ADDED_SIZED_VARCHAR_COLUMN = """
{{
  config(
    materialized="incremental",
    table_format="iceberg",
    external_volume="ICEBERG_SANDBOX",
    catalog="SNOWFLAKE",
    on_schema_change="append_new_columns"
  )
}}

select 1 as id,
cast('John' as varchar) as first_name,
cast('Smith' as varchar(134217728)) as last_name
"""


class TestIcebergSchemaChange:
    """
    Test schema changes with Iceberg tables to ensure VARCHAR columns work correctly.

    This tests the fix for the bug where adding VARCHAR columns to Iceberg tables
    fails because dbt generates VARCHAR(16777216) which is not supported by Snowflake
    Iceberg tables. The fix should use STRING instead for Iceberg tables.
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_iceberg_base.sql": _MODEL_ICEBERG_BASE,
        }

    def test_iceberg_varchar_column_addition(self, project):
        """Test that adding VARCHAR columns to Iceberg tables works correctly."""

        # First, create the initial table
        run_dbt(["run", "--select", "test_iceberg_base"])

        # Verify the table was created successfully
        results = run_dbt(["run", "--select", "test_iceberg_base"])
        assert len(results) == 1

        # Now add a VARCHAR column by updating the model
        write_file(_MODEL_ICEBERG_ADDED_COLUMN, "models", "test_iceberg_base.sql")

        # This should not fail with the varchar size error
        results = run_dbt(["run", "--select", "test_iceberg_base"])
        assert len(results) == 1
        assert results[0].status == "success"

    def test_iceberg_string_column_addition(self, project):
        """Test that adding STRING columns to Iceberg tables works correctly."""

        # First, create the initial table
        run_dbt(["run", "--select", "test_iceberg_base"])

        # Now add a STRING column by updating the model
        write_file(_MODEL_ICEBERG_ADDED_STRING_COLUMN, "models", "test_iceberg_base.sql")

        # This should work fine
        results = run_dbt(["run", "--select", "test_iceberg_base"])
        assert len(results) == 1
        assert results[0].status == "success"

    def test_iceberg_max_varchar_column_addition(self, project):
        """Test that adding VARCHAR with max size to Iceberg tables works correctly."""

        # First, create the initial table
        run_dbt(["run", "--select", "test_iceberg_base"])

        # Now add a VARCHAR column with max size by updating the model
        write_file(_MODEL_ICEBERG_ADDED_SIZED_VARCHAR_COLUMN, "models", "test_iceberg_base.sql")

        # This should work fine
        results = run_dbt(["run", "--select", "test_iceberg_base"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestIcebergSchemaChangeIntegration:

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_iceberg.sql": _MODEL_ICEBERG_BASE,
        }

    def test_reproduce_and_fix_bug(self, project):

        # Step 1: Create the initial incremental iceberg table
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

        # Step 2: Modify the model to add new column (this used to fail)
        write_file(_MODEL_ICEBERG_ADDED_COLUMN, "models", "test_iceberg.sql")

        # Step 3: Run dbt build again - this should now work with our fix
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"
