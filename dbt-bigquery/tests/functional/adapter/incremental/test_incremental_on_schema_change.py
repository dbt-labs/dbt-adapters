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

_MODELS__STRUCT_BASE = """
{{
    config(materialized='table')
}}

with source_data as (
    select 1 as id, struct('foo' as nested_field) as payload union all
    select 2 as id, struct('bar' as nested_field) as payload
)

select * from source_data
"""

_MODELS__INCREMENTAL_STRUCT_APPEND = """
{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='append_new_columns'
    )
}}

with source_data as (
    select 1 as id, struct('foo' as nested_field, cast(null as string) as extra_field) as payload union all
    select 2 as id, struct('bar' as nested_field, 'baz' as extra_field) as payload union all
    select 3 as id, struct('baz' as nested_field, 'qux' as extra_field) as payload
)

{% if is_incremental() %}
    select id, struct(payload.nested_field as nested_field, payload.extra_field as extra_field) as payload from source_data
{% else %}
    select id, struct(payload.nested_field as nested_field) as payload from source_data where id <= 2
{% endif %}
"""

_MODELS__INCREMENTAL_STRUCT_APPEND_EXPECTED = """
{{
    config(materialized='table')
}}

select
    id,
    struct(payload.nested_field as nested_field,
           payload.extra_field as extra_field) as payload
from {{ ref('incremental_struct_append') }}
"""

_MODELS__INCREMENTAL_STRUCT_SYNC = """
{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='sync_all_columns'
    )
}}

with source_data as (
    select 1 as id, struct('foo' as nested_field, 'baz' as extra_field) as payload union all
    select 2 as id, struct('bar' as nested_field, 'qux' as extra_field) as payload
)

{% if is_incremental() %}
    select id, struct(payload.nested_field as nested_field) as payload from source_data
{% else %}
    select * from source_data
{% endif %}
"""

_MODELS__INCREMENTAL_STRUCT_SYNC_EXPECTED = """
{{
    config(materialized='table')
}}

select
    id,
    struct(payload.nested_field as nested_field) as payload
from {{ ref('incremental_struct_sync') }}
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


_MODELS__DEEPLY_NESTED_STRUCT_BASE = """
{{
    config(materialized='table')
}}

with source_data as (
    select 1 as id,
        struct(
            'level1' as l1_field,
            struct(
                'level2' as l2_field,
                struct(
                    'level3' as l3_field
                ) as level3
            ) as level2
        ) as payload
    union all
    select 2 as id,
        struct(
            'level1_b' as l1_field,
            struct(
                'level2_b' as l2_field,
                struct(
                    'level3_b' as l3_field
                ) as level3
            ) as level2
        ) as payload
)

select * from source_data
"""

_MODELS__INCREMENTAL_DEEPLY_NESTED_STRUCT_APPEND = """
{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='append_new_columns'
    )
}}

with source_data as (
    select 1 as id,
        struct(
            'level1' as l1_field,
            'new_l1' as l1_new_field,
            struct(
                'level2' as l2_field,
                'new_l2' as l2_new_field,
                struct(
                    'level3' as l3_field,
                    'new_l3' as l3_new_field
                ) as level3
            ) as level2
        ) as payload
    union all
    select 2 as id,
        struct(
            'level1_b' as l1_field,
            'new_l1_b' as l1_new_field,
            struct(
                'level2_b' as l2_field,
                'new_l2_b' as l2_new_field,
                struct(
                    'level3_b' as l3_field,
                    'new_l3_b' as l3_new_field
                ) as level3
            ) as level2
        ) as payload
    union all
    select 3 as id,
        struct(
            'level1_c' as l1_field,
            'new_l1_c' as l1_new_field,
            struct(
                'level2_c' as l2_field,
                'new_l2_c' as l2_new_field,
                struct(
                    'level3_c' as l3_field,
                    'new_l3_c' as l3_new_field
                ) as level3
            ) as level2
        ) as payload
)

{% if is_incremental() %}
    -- Explicitly construct STRUCT with fields in BigQuery's "append at end" order
    select id,
        struct(
            payload.l1_field as l1_field,
            struct(
                payload.level2.l2_field as l2_field,
                struct(
                    payload.level2.level3.l3_field as l3_field,
                    payload.level2.level3.l3_new_field as l3_new_field
                ) as level3,
                payload.level2.l2_new_field as l2_new_field
            ) as level2,
            payload.l1_new_field as l1_new_field
        ) as payload
    from source_data
{% else %}
    select id,
        struct(
            payload.l1_field as l1_field,
            struct(
                payload.level2.l2_field as l2_field,
                struct(
                    payload.level2.level3.l3_field as l3_field
                ) as level3
            ) as level2
        ) as payload
    from source_data where id <= 2
{% endif %}
"""

_MODELS__INCREMENTAL_DEEPLY_NESTED_STRUCT_APPEND_EXPECTED = """
{{
    config(materialized='table')
}}

with source_data as (
    select 1 as id,
        struct(
            'level1' as l1_field,
            struct(
                'level2' as l2_field,
                struct(
                    'level3' as l3_field,
                    cast(null as string) as l3_new_field
                ) as level3,
                cast(null as string) as l2_new_field
            ) as level2,
            cast(null as string) as l1_new_field
        ) as payload
    union all
    select 2 as id,
        struct(
            'level1_b' as l1_field,
            struct(
                'level2_b' as l2_field,
                struct(
                    'level3_b' as l3_field,
                    cast(null as string) as l3_new_field
                ) as level3,
                cast(null as string) as l2_new_field
            ) as level2,
            cast(null as string) as l1_new_field
        ) as payload
    union all
    select 3 as id,
        struct(
            'level1_c' as l1_field,
            struct(
                'level2_c' as l2_field,
                struct(
                    'level3_c' as l3_field,
                    'new_l3_c' as l3_new_field
                ) as level3,
                'new_l2_c' as l2_new_field
            ) as level2,
            'new_l1_c' as l1_new_field
        ) as payload
)

select * from source_data
"""


class TestIncrementalStructOnSchemaChange(BaseIncrementalOnSchemaChangeSetup):

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "struct_base.sql": _MODELS__STRUCT_BASE,
            "incremental_struct_append.sql": _MODELS__INCREMENTAL_STRUCT_APPEND,
            "incremental_struct_append_expected.sql": _MODELS__INCREMENTAL_STRUCT_APPEND_EXPECTED,
            "incremental_struct_sync.sql": _MODELS__INCREMENTAL_STRUCT_SYNC,
            "incremental_struct_sync_expected.sql": _MODELS__INCREMENTAL_STRUCT_SYNC_EXPECTED,
        }

    def test_incremental_append_struct_fields(self, project):
        run_dbt(
            [
                "run",
                "--models",
                "struct_base incremental_struct_append",
            ]
        )
        # Second run should update the schema and succeed
        run_dbt(
            [
                "run",
                "--models",
                "struct_base incremental_struct_append",
            ]
        )
        # If the model runs successfully, the schema update worked.
        # The expected model verifies the data is correct
        run_dbt(
            [
                "run",
                "--models",
                "incremental_struct_append_expected",
            ]
        )

    @pytest.mark.skip(
        reason="BigQuery does not support removing fields from STRUCT columns via schema update"
    )
    def test_incremental_sync_struct_fields(self, project):
        # Note: This test demonstrates a BigQuery limitation.
        # BigQuery allows ADDING fields to STRUCT columns but not REMOVING them.
        # To remove fields, you would need to drop and recreate the column (losing data)
        # or recreate the entire table.
        run_dbt(
            [
                "run",
                "--models",
                "struct_base incremental_struct_sync",
            ]
        )
        run_dbt(
            [
                "run",
                "--models",
                "struct_base incremental_struct_sync",
            ]
        )
        from dbt.tests.util import check_relations_equal

        check_relations_equal(
            project.adapter,
            ["incremental_struct_sync", "incremental_struct_sync_expected"],
        )


class TestIncrementalDeeplyNestedStructOnSchemaChange(BaseIncrementalOnSchemaChangeSetup):
    """Test that BigQuery supports schema updates for deeply nested STRUCT columns.

    BigQuery supports arbitrary levels of nesting (soft limit ~100 levels).
    This test verifies that the recursive implementation in _merge_nested_fields
    correctly handles adding fields at multiple nesting levels.
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "deeply_nested_struct_base.sql": _MODELS__DEEPLY_NESTED_STRUCT_BASE,
            "incremental_deeply_nested_struct_append.sql": _MODELS__INCREMENTAL_DEEPLY_NESTED_STRUCT_APPEND,
            "incremental_deeply_nested_struct_append_expected.sql": _MODELS__INCREMENTAL_DEEPLY_NESTED_STRUCT_APPEND_EXPECTED,
        }

    def test_incremental_append_deeply_nested_struct_fields(self, project):
        """Test adding fields at multiple nesting levels simultaneously."""
        # First run - creates initial table with 3-level nested STRUCT
        results = run_dbt(
            [
                "run",
                "--models",
                "deeply_nested_struct_base incremental_deeply_nested_struct_append",
            ]
        )
        assert len(results) == 2

        # Second run - should add new fields at all 3 nesting levels
        results = run_dbt(
            [
                "run",
                "--models",
                "deeply_nested_struct_base incremental_deeply_nested_struct_append",
            ]
        )
        assert len(results) == 2

        # Verify row count - should have 3 rows (2 from first run, 1 new from second)
        relation = project.adapter.Relation.create(
            database=project.database,
            schema=project.test_schema,
            identifier="incremental_deeply_nested_struct_append",
        )

        result = project.run_sql(f"SELECT COUNT(*) as cnt FROM {relation}", fetch="one")

        assert result[0] == 3, f"Expected 3 rows, got {result[0]}"
