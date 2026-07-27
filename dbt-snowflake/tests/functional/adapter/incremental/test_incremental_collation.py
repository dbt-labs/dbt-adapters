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


def _get_column_type(project, table_name, column_name):
    sql = f"""
    describe table {project.database}.{project.test_schema}.{table_name}
    """
    results = project.run_sql(sql, fetch="all")
    for row in results:
        if row[0].lower() == column_name:
            return row[1]
    raise ValueError(f"Column {column_name} not found in table {table_name}")


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

        # Find the 'some_string_col' column and check its type includes collation
        col_type = _get_column_type(project, "base_table", "some_string_col")

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
        col_type_after = _get_column_type(project, "incremental_collation", "some_string_col")
        # Check the 'name' column still has collation
        assert (
            col_type_after is not None
        ), "some_string_col column not found in incremental_collation"
        assert (
            "COLLATE" in col_type_after.upper()
        ), f"Collation was lost after incremental run. Got: {col_type_after}"

        # Verify the data is correct
        run_dbt(["run", "--select", "expected"])
        check_relations_equal(project.adapter, ["incremental_collation", "expected"])


_MODEL_HAS_COLLATION_STG_NO_COLLATION = """
{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='sync_all_columns')
}}

select * from {{ source('test', 'stg_no_collation') }}
"""

_MODEL_HAS_COLLATION = """
{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='sync_all_columns')
}}

select * from {{ source('test', 'stg_has_collation') }}
"""

_MODEL_NO_COLLATION = """
{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='sync_all_columns')
}}

select * from {{ source('test', 'stg_has_collation') }}
"""


class TestIncrementalCollationPreservedOnSchemaChange:
    """Test that collation is preserved during incremental runs with schema changes.
    Tests the following scenarios:
    - Incremental run with schema change on a column with no collation to a column with collation
    - Incremental run with schema change on a column with different collation than source
    - Incremental run with schema change on a column with collation to a column with no collation
    To do this we simulate a schema change by creating the tables ahead of time and specifying the collation on the source / target tables.
    Then when we run the models dbt will detect the schema change and apply the correct collation to the target table.
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_stg_no_collation.sql": _MODEL_HAS_COLLATION_STG_NO_COLLATION,
            "has_collation.sql": _MODEL_HAS_COLLATION,
            "no_collation.sql": _MODEL_NO_COLLATION,
            "schema.yml": """
                            sources:
                            - name: test
                              schema: "{{ target.schema }}"
                              tables:
                                - name: stg_no_collation
                                - name: stg_has_collation
                            """,
        }

    def test_collation_preserved_on_schema_change(self, project):
        # create all the tables ahead of time so we can specify the collation on the source / target tables
        project.run_sql(
            f"""
        create or replace TABLE {project.database}.{project.test_schema}.model_stg_no_collation (
            ID VARCHAR(5),
            ORDER_DATE DATE,
            STATUS VARCHAR(8) COLLATE 'en-ci',
            ORDER_ID VARCHAR(5)
        );"""
        )

        project.run_sql(
            f"""
        create or replace TABLE {project.database}.{project.test_schema}.STG_NO_COLLATION (
            ID VARCHAR(5),
            ORDER_DATE DATE,
            STATUS VARCHAR(10),
            ORDER_ID VARCHAR(5)
        );"""
        )
        project.run_sql(
            f"""
        create or replace TABLE {project.database}.{project.test_schema}.STG_HAS_COLLATION (
            ID VARCHAR(5),
            NAME VARCHAR(12) COLLATE 'en-ci',
            AGE INT
        );"""
        )
        project.run_sql(
            f"""
        create or replace TABLE {project.database}.{project.test_schema}.HAS_COLLATION (
            ID VARCHAR(5),
            NAME VARCHAR(10) COLLATE 'en-cs',
            AGE INT
        );"""
        )
        project.run_sql(
            f"""
        create or replace TABLE {project.database}.{project.test_schema}.NO_COLLATION (
            ID VARCHAR(5),
            NAME VARCHAR(10),
            AGE INT
        );"""
        )

        results = run_dbt(["run"])
        results = run_dbt(["run"])
        assert len(results) == 3

        # Find the various test columns and check the collation is still present
        col_type_status = _get_column_type(project, "model_stg_no_collation", "status")

        assert (
            "COLLATE" in col_type_status.upper()
        ), f"Collation was lost after incremental run. Got: {col_type_status}"

        assert (
            "en-ci" in col_type_status
        ), f"Collation was not set to en-ci. Got: {col_type_status}"

        col_type_name = _get_column_type(project, "has_collation", "name")

        assert col_type_name.lower() == "varchar(12) collate 'en-cs'"

        col_type_name_no_collation = _get_column_type(project, "no_collation", "name")

        assert col_type_name_no_collation.lower() == "varchar(12)"
