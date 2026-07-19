import pytest

from dbt.contracts.results import RunStatus
from dbt.tests.util import run_dbt, run_dbt_and_capture


models__explicit_insert_overwrite_sql = """
{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    table_type='hive',
    s3_data_naming='schema_table_unique'
) }}

select 1 as id
"""

models__default_strategy_sql = """
{{ config(
    materialized='incremental',
    table_type='hive'
) }}

select 1 as id
"""

models__unsafe_location_sql = """
{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    table_type='hive',
    s3_data_naming='schema_table'
) }}

select 1 as id
"""


class TestIncrementalHiveUnpartitioned:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "explicit_insert_overwrite.sql": models__explicit_insert_overwrite_sql,
            "default_strategy.sql": models__default_strategy_sql,
            "unsafe_location.sql": models__unsafe_location_sql,
        }

    def test_explicit_insert_overwrite_replaces_all_rows(self, project):
        relation_name = "explicit_insert_overwrite"
        args = ["run", "--select", relation_name]

        first_run = run_dbt(args)
        assert first_run.results[0].status == RunStatus.Success
        assert (
            project.run_sql(
                f"select count(*) from {project.test_schema}.{relation_name}", fetch="one"
            )[0]
            == 1
        )

        second_run = run_dbt(args)
        assert second_run.results[0].status == RunStatus.Success
        assert (
            project.run_sql(
                f"select count(*) from {project.test_schema}.{relation_name}", fetch="one"
            )[0]
            == 1
        )

    def test_omitted_strategy_preserves_append_behavior(self, project):
        relation_name = "default_strategy"
        args = ["run", "--select", relation_name]

        first_run = run_dbt(args)
        assert first_run.results[0].status == RunStatus.Success

        second_run = run_dbt(args)
        assert second_run.results[0].status == RunStatus.Success
        assert (
            project.run_sql(
                f"select count(*) from {project.test_schema}.{relation_name}", fetch="one"
            )[0]
            == 2
        )

    def test_nonisolated_location_preserves_append_behavior(self, project):
        relation_name = "unsafe_location"
        args = ["run", "--select", relation_name]

        first_run = run_dbt(args)
        assert first_run.results[0].status == RunStatus.Success

        second_run, stdout = run_dbt_and_capture(args)
        assert second_run.results[0].status == RunStatus.Success
        assert (
            project.run_sql(
                f"select count(*) from {project.test_schema}.{relation_name}", fetch="one"
            )[0]
            == 2
        )

        assert "dbt-athena cannot replace an unpartitioned Hive table" in stdout
        assert "previous releases." in stdout
