import pytest

from dbt.tests.adapter.incremental.test_incremental_on_schema_change import (
    BaseIncrementalOnSchemaChangeSetup,
    BaseIncrementalOnSchemaChange,
)

from dbt.tests.adapter.incremental.fixtures import (
    _MODELS__A,
    _MODELS__INCREMENTAL_SYNC_ALL_COLUMNS_TARGET,
)

from dbt.tests.util import run_dbt


class TestIncrementalOnSchemaChange(BaseIncrementalOnSchemaChange):
    pass


_MODELS__INCREMENTAL_SYNC_ALL_COLUMNS_DYNAMIC_INSERT_OVERWRITE = """
{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='sync_all_columns',
        partition_by={
            "field": "id",
            "data_type": "int64",
            "range": {
                "start": 1,
                "end": 6,
                "interval": 1
            }
        },
        incremental_strategy='insert_overwrite'
    )
}}

WITH source_data AS (SELECT * FROM {{ ref('model_a') }} )

{% set string_type = 'string' %}

{% if is_incremental() %}

SELECT id,
       cast(field1 as {{string_type}}) as field1,
       cast(field3 as {{string_type}}) as field3, -- to validate new fields
       cast(field4 as {{string_type}}) AS field4 -- to validate new fields

FROM source_data WHERE id > _dbt_max_partition

{% else %}

select id,
       cast(field1 as {{string_type}}) as field1,
       cast(field2 as {{string_type}}) as field2

from source_data where id <= 3

{% endif %}
"""

_MODELS__INCREMENTAL_APPEND_NEW_COLUMNS_DYNAMIC_INSERT_OVERWRITE = """
{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='append_new_columns',
        partition_by={
            "field": "id",
            "data_type": "int64",
            "range": {
                "start": 1,
                "end": 6,
                "interval": 1
            }
        },
        incremental_strategy='insert_overwrite'
    )
}}

{% set string_type = 'string' %}

WITH source_data AS (SELECT * FROM {{ ref('model_a') }} )

{% if is_incremental()  %}

SELECT id,
       cast(field1 as {{string_type}}) as field1,
       cast(field2 as {{string_type}}) as field2,
       cast(field3 as {{string_type}}) as field3,
       cast(field4 as {{string_type}}) as field4
FROM source_data WHERE id > _dbt_max_partition

{% else %}

SELECT id,
       cast(field1 as {{string_type}}) as field1,
       cast(field2 as {{string_type}}) as field2
FROM source_data where id <= 3

{% endif %}
"""

_MODELS__INCREMENTAL_APPEND_NEW_COLUMNS_DYNAMIC_INSERT_OVERWRITE_TARGET = """
{{
    config(materialized='table')
}}

{% set string_type = 'string' %}

with source_data as (

    select * from {{ ref('model_a') }}

)

select id
       ,cast(field1 as {{string_type}}) as field1
       ,cast(field2 as {{string_type}}) as field2
       ,cast(CASE WHEN id <= 3 THEN NULL ELSE field3 END as {{string_type}}) AS field3
       ,cast(CASE WHEN id <= 3 THEN NULL ELSE field4 END as {{string_type}}) AS field4

from source_data
"""

_MODELS__INCREMENTAL_SYNC_ALL_COLUMNS_DYNAMIC_INSERT_OVERWRITE_TARGET = """
{{
    config(materialized='table')
}}

with source_data as (

    select * from {{ ref('model_a') }}

)

{% set string_type = 'string' %}

select id
       ,cast(field1 as {{string_type}}) as field1
       --,field2
       ,cast(case when id <= 3 then null else field3 end as {{string_type}}) as field3
       ,cast(case when id <= 3 then null else field4 end as {{string_type}}) as field4

from source_data
order by id
"""


class TestIncrementalOnSchemaChangeBigQuerySpecific(BaseIncrementalOnSchemaChangeSetup):

    def test_run_incremental_append_new_columns_dynamic_insert_overwrite(self, project):
        select = "model_a incremental_append_new_columns_dynamic_insert_overwrite incremental_append_new_columns_dynamic_insert_overwrite_target"
        compare_source = "incremental_append_new_columns_dynamic_insert_overwrite"
        compare_target = "incremental_append_new_columns_dynamic_insert_overwrite_target"
        self.run_twice_and_assert(select, compare_source, compare_target, project)

    def test_run_incremental_sync_all_columns_dynamic_insert_overwrite(self, project):
        select = "model_a incremental_sync_all_columns_dynamic_insert_overwrite incremental_sync_all_columns_dynamic_insert_overwrite_target"
        compare_source = "incremental_sync_all_columns_dynamic_insert_overwrite"
        compare_target = "incremental_sync_all_columns_dynamic_insert_overwrite_target"
        self.run_twice_and_assert(select, compare_source, compare_target, project)

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incremental_sync_all_columns_dynamic_insert_overwrite.sql": _MODELS__INCREMENTAL_SYNC_ALL_COLUMNS_DYNAMIC_INSERT_OVERWRITE,
            "incremental_sync_all_columns_dynamic_insert_overwrite_target.sql": _MODELS__INCREMENTAL_SYNC_ALL_COLUMNS_DYNAMIC_INSERT_OVERWRITE_TARGET,
            "incremental_append_new_columns_dynamic_insert_overwrite.sql": _MODELS__INCREMENTAL_APPEND_NEW_COLUMNS_DYNAMIC_INSERT_OVERWRITE,
            "incremental_append_new_columns_dynamic_insert_overwrite_target.sql": _MODELS__INCREMENTAL_APPEND_NEW_COLUMNS_DYNAMIC_INSERT_OVERWRITE_TARGET,
            "model_a.sql": _MODELS__A,
        }


# Test fixtures for special character column names
_MODEL_A_SPECIAL_CHARS = """
{{
    config(materialized='table')
}}

with source_data as (

    select 1 as id, 'bbb' as `select`, 111 as `field-with-dash`
    union all select 2 as id, 'ddd' as `select`, 222 as `field-with-dash`
    union all select 3 as id, 'fff' as `select`, 333 as `field-with-dash`
    union all select 4 as id, 'hhh' as `select`, 444 as `field-with-dash`
    union all select 5 as id, 'jjj' as `select`, 555 as `field-with-dash`
    union all select 6 as id, 'lll' as `select`, 666 as `field-with-dash`

)

select id
       ,`select`
       ,`field-with-dash`

from source_data
"""

_MODEL_INCREMENTAL_APPEND_NEW_SPECIAL_CHARS = """
{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='append_new_columns'
    )
}}

WITH source_data AS (SELECT * FROM {{ ref('model_a_special_chars') }} )

{% if is_incremental() %}

SELECT id,
       `select`,
       `field-with-dash`
FROM source_data WHERE id NOT IN (SELECT id from {{ this }} )

{% else %}

SELECT id,
       `select`
FROM source_data where id <= 3

{% endif %}
"""

_MODEL_INCREMENTAL_APPEND_NEW_SPECIAL_CHARS_TARGET = """
{{
    config(materialized='table')
}}

with source_data as (

    select * from {{ ref('model_a_special_chars') }}

)

select id
       ,`select`
       ,CASE WHEN id <= 3 THEN NULL ELSE `field-with-dash` END AS `field-with-dash`

from source_data
"""


class TestIncrementalOnSchemaChangeSpecialChars(BaseIncrementalOnSchemaChangeSetup):
    """Test incremental models with special character column names for BigQuery

    Tests column quoting with SQL keywords and dashes in column names.
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_a_special_chars.sql": _MODEL_A_SPECIAL_CHARS,
            "incremental_append_new_special_chars.sql": _MODEL_INCREMENTAL_APPEND_NEW_SPECIAL_CHARS,
            "incremental_append_new_special_chars_target.sql": _MODEL_INCREMENTAL_APPEND_NEW_SPECIAL_CHARS_TARGET,
        }

    def test_incremental_append_new_columns_with_special_characters(self, project):
        """Test that incremental models work correctly with special character column names when adding new columns"""

        # First run - creates initial table
        run_dbt(
            [
                "run",
                "--models",
                "model_a_special_chars incremental_append_new_special_chars incremental_append_new_special_chars_target",
            ]
        )

        # Second run - should append new columns with special characters
        run_dbt(
            [
                "run",
                "--models",
                "model_a_special_chars incremental_append_new_special_chars incremental_append_new_special_chars_target",
            ]
        )

        # Verify results match expected output
        from dbt.tests.util import check_relations_equal

        check_relations_equal(
            project.adapter,
            [
                "incremental_append_new_special_chars",
                "incremental_append_new_special_chars_target",
            ],
        )
