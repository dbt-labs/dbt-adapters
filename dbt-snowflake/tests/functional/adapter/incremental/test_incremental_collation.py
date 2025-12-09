"""Tests for preserving collation on VARCHAR columns during incremental runs."""

import pytest
from dbt.tests.util import run_dbt, check_relations_equal


# Initial table with collation
_MODEL_BASE_TABLE = """
{{
    config(
        materialized='table',
    )
}}

select 1 as id, COLLATE('initial', 'en-ci')  as some_string_col
"""

# Incremental model that will trigger schema changes
_MODEL_INCREMENTAL_COLLATION = """
{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='sync_all_columns'
    )
}}

select
    id,
    some_string_col,
    {% if is_incremental() %}
        'new_value' as description
    {% endif %}
from {{ ref('base_table') }}
"""

# Expected result table
_MODEL_EXPECTED = """
{{
    config(materialized='table')
}}

select
    1 as id,
    'initial' as some_string_col,
    'new_value' as description
"""


class TestIncrementalCollation:
    """Test that collation is preserved during incremental runs with schema changes.
    For more info on collation: https://docs.snowflake.com/en/sql-reference/collation
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "base_table.sql": _MODEL_BASE_TABLE,
            "incremental_collation.sql": _MODEL_INCREMENTAL_COLLATION,
            "expected.sql": _MODEL_EXPECTED,
        }

    def test_collation_preserved_on_incremental(self, project):
        # First run: create seed table
        results = run_dbt(["run", "--select", "base_table"])
        assert len(results) == 1

        # Get the collation of the name column from seed table
        sql = f"""
        describe table {project.database}.{project.test_schema}.base_table
        """
        results = project.run_sql(sql, fetch="all")

        # Find the 'some_string_col' column and check its type includes collation
        col_type = None
        for row in results:
            if row[0].lower() == "some_string_col":  # column name
                col_type = row[1]  # column type
                break

        assert col_type is not None, "some_string_col column not found in base_table"
        # Verify collation is present in the type
        assert "COLLATE" in col_type.upper(), f"Expected collation in type but got: {col_type}"

        # First incremental run: create table without description column
        results = run_dbt(["run", "--select", "incremental_collation"])
        assert len(results) == 1

        project.run_sql(
            f"alter table {project.database}.{project.test_schema}.incremental_collation alter column some_string_col set data type varchar(134217728) collate 'en-ci'"
        )

        results = run_dbt(["run", "--select", "incremental_collation"])
        assert len(results) == 1

        # Verify the collation is still present after the incremental run
        sql = f"""
        describe table {project.database}.{project.test_schema}.incremental_collation
        """
        results = project.run_sql(sql, fetch="all")

        # Check the 'name' column still has collation
        col_type_after = None
        for row in results:
            if row[0].lower() == "some_string_col":
                col_type_after = row[1]
                break

        assert (
            col_type_after is not None
        ), "some_string_col column not found in incremental_collation"
        assert (
            "COLLATE" in col_type_after.upper()
        ), f"Collation was lost after incremental run. Got: {col_type_after}"

        # Verify the data is correct
        run_dbt(["run", "--select", "expected"])
        check_relations_equal(project.adapter, ["incremental_collation", "expected"])
