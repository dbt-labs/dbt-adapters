import pytest

from dbt.tests.util import run_dbt

from dbt.tests.adapter.incremental.test_incremental_on_schema_change import (
    BaseIncrementalOnSchemaChangeSetup,
)


class IncrementalOnSchemaChangeIgnoreFail(BaseIncrementalOnSchemaChangeSetup):
    def test_run_incremental_ignore(self, project):
        select = "model_a incremental_ignore incremental_ignore_target"
        compare_source = "incremental_ignore"
        compare_target = "incremental_ignore_target"
        self.run_twice_and_assert(select, compare_source, compare_target, project)

    def test_run_incremental_fail_on_schema_change(self, project):
        select = "model_a incremental_fail"
        run_dbt(["run", "--models", select, "--full-refresh"])
        results_two = run_dbt(["run", "--models", select], expect_pass=False)
        assert "Compilation Error" in results_two[1].message


@pytest.mark.skip_profile("databricks_sql_endpoint", "spark_http_odbc")
class TestAppendOnSchemaChange(IncrementalOnSchemaChangeIgnoreFail):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+incremental_strategy": "append",
            }
        }


@pytest.mark.skip_profile("databricks_sql_endpoint", "spark_session", "spark_http_odbc")
class TestInsertOverwriteOnSchemaChange(IncrementalOnSchemaChangeIgnoreFail):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+file_format": "parquet",
                "+partition_by": "id",
                "+incremental_strategy": "insert_overwrite",
            }
        }


@pytest.mark.skip_profile("apache_spark", "spark_session")
class TestDeltaOnSchemaChange(BaseIncrementalOnSchemaChangeSetup):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+file_format": "delta",
                "+incremental_strategy": "merge",
                "+unique_key": "id",
            }
        }

    def run_incremental_sync_all_columns(self, project):
        select = "model_a incremental_sync_all_columns incremental_sync_all_columns_target"
        run_dbt(["run", "--models", select, "--full-refresh"])
        # Delta Lake doesn"t support removing columns -- show a nice compilation error
        results = run_dbt(["run", "--models", select], expect_pass=False)
        assert "Compilation Error" in results[1].message

    def run_incremental_sync_remove_only(self, project):
        select = "model_a incremental_sync_remove_only incremental_sync_remove_only_target"
        run_dbt(["run", "--models", select, "--full-refresh"])
        # Delta Lake doesn"t support removing columns -- show a nice compilation error
        results = run_dbt(["run", "--models", select], expect_pass=False)
        assert "Compilation Error" in results[1].message

    def test_run_incremental_append_new_columns(self, project):
        # only adding new columns in supported
        self.run_incremental_append_new_columns(project)
        # handling columns that have been removed doesn"t work on Delta Lake today
        # self.run_incremental_append_new_columns_remove_one(project)

    def test_run_incremental_sync_all_columns(self, project):
        self.run_incremental_sync_all_columns(project)
        self.run_incremental_sync_remove_only(project)


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


@pytest.mark.skip_profile("databricks_sql_endpoint", "spark_http_odbc")
class TestAppendOnSchemaChangeSpecialChars(BaseIncrementalOnSchemaChangeSetup):
    """Test incremental models with special character column names using append strategy

    Tests column quoting with SQL keywords and dashes in column names.
    Note: Spaces and dots in column names are not supported by some databases.
    """

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+incremental_strategy": "append",
            }
        }

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
