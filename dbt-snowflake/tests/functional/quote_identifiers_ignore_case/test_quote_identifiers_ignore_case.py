import pytest
from dbt.tests.util import run_dbt, check_relation_types, relation_from_name
from tests.functional.utils import query_relation_type

# Test data
SEED_DATA = """
id,name,value
1,test_name_1,100
2,test_name_2,200
3,test_name_3,300
""".strip()

# Model definitions for different materialization types
TABLE_MODEL = """
{{ config(
    materialized='table',
) }}
select * from {{ ref('my_seed') }}
"""

VIEW_MODEL = """
{{ config(
    materialized='view',
) }}
select * from {{ ref('my_seed') }}
"""

INCREMENTAL_MODEL = """
{{ config(
    materialized='incremental',
    unique_key='id',
) }}
select * from {{ ref('my_seed') }}
"""

DYNAMIC_TABLE_MODEL = """
{{ config(
    materialized='dynamic_table',
    snowflake_warehouse='DBT_TESTING',
    target_lag='1 minute',
    refresh_mode='INCREMENTAL',
) }}
select * from {{ ref('my_seed') }}
"""

ICEBERG_TABLE_MODEL = """
{{ config(
    materialized='table',
    table_format='iceberg',
    external_volume='s3_iceberg_snow',
) }}
select * from {{ ref('my_seed') }}
"""

INCREMENTAL_ICEBERG_MODEL = """
{{ config(
    materialized='incremental',
    table_format='iceberg',
    incremental_strategy='append',
    unique_key='id',
    external_volume='s3_iceberg_snow',
) }}
select * from {{ ref('my_table') }}
"""

DYNAMIC_ICEBERG_MODEL = """
{{ config(
    materialized='dynamic_table',
    snowflake_warehouse='DBT_TESTING',
    target_lag='1 minute',
    refresh_mode='INCREMENTAL',
    table_format='iceberg',
    external_volume='s3_iceberg_snow',
    base_location_subpath='subpath',
) }}
select * from {{ ref('my_table') }}
"""


class TestQuotedIdentifiersIgnoreCase:
    """
    Test that validates dbt can successfully run all materialization types
    when quoted_identifiers_ignore_case parameter is set to TRUE on a schema.

    This test:
    1. Updates the test schema to set the parameter to TRUE
    2. Invokes dbt (including a dynamic_table model)
    3. Asserts we correctly created the objects
    4. Turns the parameter off
    5. Invokes dbt again
    """

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"my_seed.csv": SEED_DATA}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_table.sql": TABLE_MODEL,
            "my_view.sql": VIEW_MODEL,
            "my_incremental.sql": INCREMENTAL_MODEL,
            "my_dynamic_table.sql": DYNAMIC_TABLE_MODEL,
            "my_iceberg_table.sql": ICEBERG_TABLE_MODEL,
            "my_incremental_iceberg.sql": INCREMENTAL_ICEBERG_MODEL,
            "my_dynamic_iceberg.sql": DYNAMIC_ICEBERG_MODEL,
        }

    def _set_schema_parameter(self, project, value: bool):
        """Set the quoted_identifiers_ignore_case parameter on the schema"""
        value_str = "true" if value else "false"
        project.run_sql(
            f"alter schema {project.test_schema} set quoted_identifiers_ignore_case = {value_str}"
        )

    def _assert_relations_exist_and_correct_type(self, project):
        """Assert that all relations exist and are of the correct type"""
        expected_relations = {
            "my_table": "table",
            "my_view": "view",
            "my_incremental": "table",
            "my_dynamic_table": "dynamic_table",
            "my_iceberg_table": "table",
            "my_incremental_iceberg": "table",
            "my_dynamic_iceberg": "dynamic_table",
        }

        for relation_name, expected_type in expected_relations.items():
            # Check that relation exists and has correct type
            actual_type = query_relation_type(project, relation_name)
            assert (
                actual_type == expected_type
            ), f"Expected {relation_name} to be {expected_type}, got {actual_type}"

    def test_all_materializations_with_quoted_identifiers_ignore_case(self, project):
        """
        Test that all materialization types work correctly when
        quoted_identifiers_ignore_case is set to TRUE, then FALSE
        """

        self._set_schema_parameter(project, True)
        run_dbt(["seed"])
        results = run_dbt(["run"])
        assert len(results) == 7, f"Expected 7 models to run, got {len(results) if results else 0}"

        self._assert_relations_exist_and_correct_type(project)

        self._set_schema_parameter(project, False)

        results = run_dbt(["run", "--full-refresh"])
        assert len(results) == 7, f"Expected 7 models to run, got {len(results)}"

        self._assert_relations_exist_and_correct_type(project)
