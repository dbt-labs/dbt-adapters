import pytest

from dbt.tests.util import run_dbt_and_capture
from dbt.tests.adapter.incremental.test_incremental_microbatch import (
    BaseMicrobatch,
    patch_microbatch_end_time,
)

# No requirement for a unique_id for athena microbatch!
_microbatch_model_no_unique_id_sql = """
{{ config(materialized='incremental', incremental_strategy='microbatch', event_time='event_time', batch_size='day', begin=modules.datetime.datetime(2020, 1, 1, 0, 0, 0), partition_by=['date_day']) }}
select *, cast(event_time as date) as date_day
from {{ ref('input_model') }}
"""

microbatch_input_sql = """
{{ config(materialized='table', event_time='event_time') }}
select 1 as id, TIMESTAMP '2020-01-01 00:00:00-0' as event_time
union all
select 2 as id, TIMESTAMP '2020-01-02 00:00:00-0' as event_time
union all
select 3 as id, TIMESTAMP '2020-01-03 00:00:00-0' as event_time
"""

microbatch_model_no_partition_by_sql = """
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
        return _microbatch_model_no_unique_id_sql


class TestAthenaMicrobatchMissingPartitionBy:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "microbatch.sql": microbatch_model_no_partition_by_sql,
            "input_model.sql": microbatch_input_sql,
        }

    def test_execution_failure_no_partition_by(self, project):
        with patch_microbatch_end_time("2020-01-03 13:57:00"):
            _, stdout = run_dbt_and_capture(["run"], expect_pass=False)
        assert (
            "dbt-athena 'microbatch' incremental strategy requires a `partition_by` config"
            in stdout
        )
