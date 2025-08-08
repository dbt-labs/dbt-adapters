from dbt.tests.adapter.incremental.test_incremental_on_schema_change import (
    BaseIncrementalOnSchemaChange,
    BaseIncrementalOnSchemaChangeSetup,
)
from dbt.tests.util import run_dbt
import pytest


class TestIncrementalOnSchemaChange(BaseIncrementalOnSchemaChange):
    pass


# New fixtures for testing column names with spaces and special characters
_MODEL_A_SPECIAL_COLUMNS = """
{{
    config(materialized='table')
}}

with source_data as (

    select 1 as id, 'aaa' as "field with space", 'bbb' as "field-with-dash", 111 as "field@special", 'TTT' as "field.with.dots"
    union all select 2 as id, 'ccc' as "field with space", 'ddd' as "field-with-dash", 222 as "field@special", 'UUU' as "field.with.dots"
    union all select 3 as id, 'eee' as "field with space", 'fff' as "field-with-dash", 333 as "field@special", 'VVV' as "field.with.dots"
    union all select 4 as id, 'ggg' as "field with space", 'hhh' as "field-with-dash", 444 as "field@special", 'WWW' as "field.with.dots"
    union all select 5 as id, 'iii' as "field with space", 'jjj' as "field-with-dash", 555 as "field@special", 'XXX' as "field.with.dots"
    union all select 6 as id, 'kkk' as "field with space", 'lll' as "field-with-dash", 666 as "field@special", 'YYY' as "field.with.dots"

)

select id
       ,"field with space"
       ,"field-with-dash"
       ,"field@special"
       ,"field.with.dots"

from source_data
"""

_MODEL_INCREMENTAL_APPEND_NEW_COLUMNS_SPECIAL = """
{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='append_new_columns'
    )
}}

WITH source_data AS (SELECT * FROM {{ ref('model_a_special_columns') }} )

{% if is_incremental() %}

SELECT id,
       "field with space",
       "field-with-dash",
       "field@special",
       "field.with.dots"
FROM source_data WHERE id NOT IN (SELECT id from {{ this }} )

{% else %}

SELECT id,
       "field with space",
       "field-with-dash"
FROM source_data where id <= 3

{% endif %}
"""

_MODEL_INCREMENTAL_APPEND_NEW_COLUMNS_SPECIAL_TARGET = """
{{
    config(materialized='table')
}}

with source_data as (

    select * from {{ ref('model_a_special_columns') }}

)

select id
       ,"field with space"
       ,"field-with-dash"
       ,CASE WHEN id <= 3 THEN NULL ELSE "field@special" END AS "field@special"
       ,CASE WHEN id <= 3 THEN NULL ELSE "field.with.dots" END AS "field.with.dots"

from source_data
"""


class TestIncrementalOnSchemaChangeSpecialColumns(BaseIncrementalOnSchemaChangeSetup):
    """Test incremental models with on_schema_change when column names contain spaces and special characters"""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_a_special_columns.sql": _MODEL_A_SPECIAL_COLUMNS,
            "incremental_append_new_columns_special.sql": _MODEL_INCREMENTAL_APPEND_NEW_COLUMNS_SPECIAL,
            "incremental_append_new_columns_special_target.sql": _MODEL_INCREMENTAL_APPEND_NEW_COLUMNS_SPECIAL_TARGET,
        }

    def test_incremental_append_new_columns_with_special_characters(self, project):
        """Test that incremental models with on_schema_change='append_new_columns' work correctly with column names containing spaces and special characters"""

        # First run - creates initial table with id, "field with space", "field-with-dash"
        run_dbt(
            [
                "run",
                "--models",
                "model_a_special_columns incremental_append_new_columns_special incremental_append_new_columns_special_target",
            ]
        )

        # Second run - should append new columns "field@special" and "field.with.dots"
        run_dbt(
            [
                "run",
                "--models",
                "model_a_special_columns incremental_append_new_columns_special incremental_append_new_columns_special_target",
            ]
        )

        # Verify that the incremental model matches the target
        from dbt.tests.util import check_relations_equal

        check_relations_equal(
            project.adapter,
            [
                "incremental_append_new_columns_special",
                "incremental_append_new_columns_special_target",
            ],
        )
