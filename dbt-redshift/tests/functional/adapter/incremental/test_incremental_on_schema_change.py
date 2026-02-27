from dbt.tests.adapter.incremental.test_incremental_on_schema_change import (
    BaseIncrementalOnSchemaChange,
    BaseIncrementalOnSchemaChangeSetup,
)
from dbt.tests.util import check_relations_equal, run_dbt
import pytest


class TestIncrementalOnSchemaChange(BaseIncrementalOnSchemaChange):
    pass


# ---- Column type change (redshift__alter_column_type): VARCHAR expand + int→bigint ----
_INCREMENTAL_VARCHAR_EXPAND = """
{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='sync_all_columns'
    )
}}
with source_data as (
    select 1 as id, 'Alice' as name
    union all select 2, 'Bob'
)

{% if is_incremental() %}
select id, cast(name as varchar(256)) as name from source_data where id not in (select id from {{ this }})
{% else %}
select id, cast(name as varchar(20)) as name from source_data
{% endif %}
"""

_INCREMENTAL_VARCHAR_EXPAND_TARGET = """
{{
    config(materialized='table')
}}
with source_data as (
    select 1 as id, 'Alice' as name
    union all select 2, 'Bob'
)
select id, cast(name as varchar(256)) as name from source_data
"""

_INCREMENTAL_INT_TO_BIGINT = """
{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='sync_all_columns'
    )
}}
with source_data as (
    select 1 as id, 100 as counter
    union all select 2, 200
)
{% if is_incremental() %}
select id, counter::bigint as counter from source_data where id not in (select id from {{ this }})
{% else %}
select id, counter::integer as counter from source_data
{% endif %}
"""

_INCREMENTAL_INT_TO_BIGINT_TARGET = """
{{
    config(materialized='table')
}}
with source_data as (
    select 1 as id, 100 as counter
    union all select 2, 200
)
select id, counter::bigint as counter from source_data
"""

# Test fixtures for special character column names
_MODEL_A_SPECIAL_CHARS = """
{{
    config(materialized='table')
}}

with source_data as (

    select 1 as id, 'bbb' as "select", 111 as "field-with-dash"
    union all select 2 as id, 'ddd' as "select", 222 as "field-with-dash"
    union all select 3 as id, 'fff' as "select", 333 as "field-with-dash"
    union all select 4 as id, 'hhh' as "select", 444 as "field-with-dash"
    union all select 5 as id, 'jjj' as "select", 555 as "field-with-dash"
    union all select 6 as id, 'lll' as "select", 666 as "field-with-dash"

)

select id
       ,"select"
       ,"field-with-dash"

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
       "select",
       "field-with-dash"
FROM source_data WHERE id NOT IN (SELECT id from {{ this }} )

{% else %}

SELECT id,
       "select"
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
       ,"select"
       ,CASE WHEN id <= 3 THEN NULL ELSE "field-with-dash" END AS "field-with-dash"

from source_data
"""

_MODEL_INCREMENTAL_SYNC_ALL_SPECIAL_CHARS = """
{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='sync_all_columns'
    )
}}

WITH source_data AS (SELECT * FROM {{ ref('model_a_special_chars') }} )

{% if is_incremental() %}

SELECT id,
       "field-with-dash"

FROM source_data WHERE id NOT IN (SELECT id from {{ this }} )

{% else %}

SELECT id,
       "select"

FROM source_data where id <= 3

{% endif %}
"""

_MODEL_INCREMENTAL_SYNC_ALL_SPECIAL_CHARS_TARGET = """
{{
    config(materialized='table')
}}

with source_data as (

    select * from {{ ref('model_a_special_chars') }}

)

select id
       --,"select" (removed in sync)
       ,CASE WHEN id <= 3 THEN NULL ELSE "field-with-dash" END AS "field-with-dash"

from source_data
order by id
"""


class TestIncrementalOnSchemaChangeSpecialChars(BaseIncrementalOnSchemaChangeSetup):
    """Test incremental models with special character column names for Redshift

    Tests column quoting with SQL keywords and dashes in column names.
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_a_special_chars.sql": _MODEL_A_SPECIAL_CHARS,
            "incremental_append_new_special_chars.sql": _MODEL_INCREMENTAL_APPEND_NEW_SPECIAL_CHARS,
            "incremental_append_new_special_chars_target.sql": _MODEL_INCREMENTAL_APPEND_NEW_SPECIAL_CHARS_TARGET,
            "incremental_sync_all_special_chars.sql": _MODEL_INCREMENTAL_SYNC_ALL_SPECIAL_CHARS,
            "incremental_sync_all_special_chars_target.sql": _MODEL_INCREMENTAL_SYNC_ALL_SPECIAL_CHARS_TARGET,
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

        check_relations_equal(
            project.adapter,
            [
                "incremental_append_new_special_chars",
                "incremental_append_new_special_chars_target",
            ],
        )

    def test_incremental_sync_all_columns_with_special_characters(self, project):
        """Test that incremental models work correctly with special character column names when syncing all columns (add/remove)"""

        # First run - creates initial table
        run_dbt(
            [
                "run",
                "--models",
                "model_a_special_chars incremental_sync_all_special_chars incremental_sync_all_special_chars_target",
            ]
        )

        # Second run - should sync columns (remove "select", add "field-with-dash" and "field.with.dots")
        run_dbt(
            [
                "run",
                "--models",
                "model_a_special_chars incremental_sync_all_special_chars incremental_sync_all_special_chars_target",
            ]
        )

        check_relations_equal(
            project.adapter,
            ["incremental_sync_all_special_chars", "incremental_sync_all_special_chars_target"],
        )


class TestIncrementalOnSchemaChangeColumnType(BaseIncrementalOnSchemaChangeSetup):
    """Test incremental schema change when column *type* changes (redshift__alter_column_type).

    - VARCHAR expand: expand_target_column_types -> native ALTER COLUMN TYPE.
    - INTEGER -> BIGINT: process_schema_changes -> default add/copy/drop/rename.
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incremental_varchar_expand.sql": _INCREMENTAL_VARCHAR_EXPAND,
            "incremental_varchar_expand_target.sql": _INCREMENTAL_VARCHAR_EXPAND_TARGET,
            "incremental_int_to_bigint.sql": _INCREMENTAL_INT_TO_BIGINT,
            "incremental_int_to_bigint_target.sql": _INCREMENTAL_INT_TO_BIGINT_TARGET,
        }

    def test_incremental_varchar_expand_succeeds_and_matches_target(self, project):
        select = "incremental_varchar_expand incremental_varchar_expand_target"
        # First run - creates initial table
        run_dbt(["run", "--select", select])
        # Second run - should expand the column type
        run_dbt(["run", "--select", select])
        check_relations_equal(
            project.adapter,
            ["incremental_varchar_expand", "incremental_varchar_expand_target"],
        )

    def test_incremental_int_to_bigint_succeeds_and_matches_target(self, project):
        select = "incremental_int_to_bigint incremental_int_to_bigint_target"
        # First run - creates initial table
        run_dbt(["run", "--select", select])
        # Second run - should convert the column type to bigint
        run_dbt(["run", "--select", select])
        check_relations_equal(
            project.adapter,
            ["incremental_int_to_bigint", "incremental_int_to_bigint_target"],
        )
