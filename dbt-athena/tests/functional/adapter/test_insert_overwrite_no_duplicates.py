"""
Regression test for insert_overwrite data duplication caused by stale S3 files that have no
corresponding Glue partition metadata.

This state can arise when a previous run:
  1. Called clean_up_partitions (removes Glue metadata + S3 files for a partition)
  2. Started the INSERT INTO — writing new files to S3
  3. Failed before the INSERT completed (so no Glue partition was registered)

On the next run, clean_up_partitions queries Glue, finds nothing, and skips S3 cleanup. The new
INSERT then writes files alongside the stale ones. When Glue finally registers the partition, it
points to the directory containing both old and new files — causing duplicates.

The test simulates this state directly using ALTER TABLE DROP PARTITION, which removes the Glue
partition metadata without touching S3 files, exactly replicating the post-failure state.
"""

import pytest

from dbt.contracts.results import RunStatus
from dbt.tests.util import run_dbt

insert_overwrite_model_sql = """
{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partitioned_by=['date_column'],
    s3_data_naming='table',
    format='parquet'
) }}
select
    1 as value,
    cast(from_iso8601_date('{{ var("logical_date") }}') as date) as date_column
"""


class TestInsertOverwriteNoDuplicatesAfterStaleS3Files:
    @pytest.fixture(scope="class")
    def models(self):
        return {"insert_overwrite_no_dupes.sql": insert_overwrite_model_sql}

    def test_no_duplicates_after_stale_s3_files(self, project):
        relation_name = "insert_overwrite_no_dupes"
        count_query = f"select count(*) as cnt from {project.test_schema}.{relation_name}"
        date_filter_query = (
            f"select count(*) as cnt from {project.test_schema}.{relation_name} "
            f"where date_column = date '2024-01-01'"
        )

        first_run = run_dbt(
            ["run", "--select", relation_name, "--vars", '{"logical_date": "2024-01-01"}']
        )
        assert first_run.results[0].status == RunStatus.Success
        assert project.run_sql(count_query, fetch="all")[0][0] == 1

        # Simulate a previous run's partial failure: drop the Glue partition metadata but leave
        # the S3 files in place. This is exactly the state left when clean_up_partitions succeeds
        # but the subsequent INSERT fails mid-execution.
        project.run_sql(
            f"alter table {project.test_schema}.{relation_name} "
            f"drop partition (date_column='2024-01-01')"
        )

        # Without the fix, this run would find no Glue metadata for date_column=2024-01-01,
        # skip S3 cleanup, then write new files alongside the stale ones — producing 2 rows.
        second_run = run_dbt(
            ["run", "--select", relation_name, "--vars", '{"logical_date": "2024-01-01"}']
        )
        assert second_run.results[0].status == RunStatus.Success
        assert project.run_sql(date_filter_query, fetch="all")[0][0] == 1
