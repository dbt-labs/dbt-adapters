import pytest

from dbt.tests.util import run_dbt_and_capture
from dbt.tests.adapter.incremental.test_incremental_microbatch import (
    BaseMicrobatch,
    patch_microbatch_end_time,
)

_input_model_sql = """
{{ config(materialized='table', event_time='event_time') }}
select 1 as id, CAST(from_iso8601_timestamp('2020-01-01T00:00:00.000000Z') as timestamp) as event_time
union all
select 2 as id, CAST(from_iso8601_timestamp('2020-01-02T00:00:00.000000Z') as timestamp) as event_time
union all
select 3 as id, CAST(from_iso8601_timestamp('2020-01-03T00:00:00.000000Z') as timestamp) as event_time
"""

# No requirement for a unique_id for athena microbatch!
_microbatch_model_sql = """
{{ config(
    materialized='incremental',
    incremental_strategy='microbatch',
    event_time='event_time',
    batch_size='day',
    begin=modules.datetime.datetime(2020, 1, 1, 0, 0, 0),
    partitioned_by=['date_day']
    )
}}
select * from {{ ref('input_model') }}
"""

_microbatch_model_no_partitioned_by_sql = """
{{ config(
    materialized='incremental',
    incremental_strategy='microbatch',
    event_time='event_time',
    batch_size='day',
    begin=modules.datetime.datetime(2020, 1, 1, 0, 0, 0)
    )
}}
select * from {{ ref('input_model') }}
"""


class TestAthenaMicrobatch(BaseMicrobatch):
    @pytest.fixture(scope="class")
    def microbatch_model_sql(self) -> str:
        return _microbatch_model_sql

    @pytest.fixture(scope="class")
    def input_model_sql(self) -> str:
        return _input_model_sql


class TestAthenaMicrobatchMissingPartitionBy:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "microbatch.sql": _microbatch_model_no_partitioned_by_sql,
            "input_model.sql": _input_model_sql,
        }

    def test_execution_failure_no_partition_by(self, project):
        with patch_microbatch_end_time("2020-01-03 13:57:00"):
            _, stdout = run_dbt_and_capture(["run"], expect_pass=False)
        assert (
            "dbt-athena 'microbatch' incremental strategy requires a `partitioned_by` config"
            in stdout
        )
