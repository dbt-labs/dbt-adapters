"""
Test that clean_up_partitions handles expression length chunking correctly.

The Glue GetPartitions API has a 2048 character limit for the Expression parameter.
When deleting many partitions, we need to chunk them into multiple API calls.
This test creates enough partitions to trigger that chunking logic.

NOTE: This test takes approximately 2+ minutes to run due to creating and processing
150 partitions with real AWS API calls.
"""

import pytest

from dbt.contracts.results import RunStatus
from dbt.tests.util import run_dbt

# This model creates 150 partitions (days from 2023-01-01 to 2023-05-30)
# Each partition expression like "(date_column='2023-01-01')" is ~26 chars
# 150 partitions would be ~3900 chars when OR-ed together, requiring chunking
models__many_partitions_sql = """
{{ config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partitioned_by=['date_column']
    )
}}
select
    random() as rnd,
    cast(date_column as date) as date_column
from (
    values (
        sequence(from_iso8601_date('2023-01-01'), from_iso8601_date('2023-05-30'), interval '1' day)
    )
) as t1(date_array)
cross join unnest(date_array) as t2(date_column)
{% if is_incremental() %}
    where date_column >= date '{{ var('start_date') }}'
{% endif %}
"""


class TestPartitionChunking:
    """
    Test that insert_overwrite works correctly with many partitions that
    exceed the 2048 character API expression limit.
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {"many_partitions.sql": models__many_partitions_sql}

    def test__partition_chunking(self, project):
        """
        Test that we can handle many partitions that require chunking
        the GetPartitions API expression parameter.
        """
        relation_name = "many_partitions"
        model_run_result_row_count_query = (
            f"select count(*) as records from {project.test_schema}.{relation_name}"
        )

        # First run - creates table with 150 partitions (Jan 1 - May 30)
        first_model_run = run_dbt(["run", "--select", relation_name])
        first_model_run_result = first_model_run.results[0]

        assert first_model_run_result.status == RunStatus.Success

        records_count_first_run = project.run_sql(model_run_result_row_count_query, fetch="all")[
            0
        ][0]

        # 150 days worth of records
        assert records_count_first_run == 150

        # Second run - incremental update starting from March 1
        # This will delete and recreate 91 partitions (Mar 1 - May 30)
        # Expression for 91 partitions = ~2730 chars, requiring ~2 chunks
        incremental_model_run = run_dbt(
            [
                "run",
                "--select",
                relation_name,
                "--vars",
                '{"start_date": "2023-03-01"}',
            ]
        )

        incremental_model_run_result = incremental_model_run.results[0]

        # Verify the incremental run succeeded (tests chunking worked)
        assert incremental_model_run_result.status == RunStatus.Success

        records_count_incremental_run = project.run_sql(
            model_run_result_row_count_query, fetch="all"
        )[0][0]

        # Should still have 150 records
        assert records_count_incremental_run == 150

        # Verify we actually have the expected date range
        min_date_query = (
            f"select min(date_column) as min_date from {project.test_schema}.{relation_name}"
        )
        max_date_query = (
            f"select max(date_column) as max_date from {project.test_schema}.{relation_name}"
        )

        min_date = project.run_sql(min_date_query, fetch="all")[0][0]
        max_date = project.run_sql(max_date_query, fetch="all")[0][0]

        assert str(min_date) == "2023-01-01"
        assert str(max_date) == "2023-05-30"
