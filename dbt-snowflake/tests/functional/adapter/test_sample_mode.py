from dbt.tests.adapter.sample_mode.test_sample_mode import (
    BaseSampleModeTest,
)
import pytest


_input_model_sql = """
{{ config(materialized='table', event_time='event_time') }}
select 1 as id, to_timestamp_tz('2025-01-01 01:25:00-0') as event_time
union all
select 2 as id, to_timestamp_tz('2025-01-02 13:47:00-0') as event_time
union all
select 3 as id, to_timestamp_tz('2025-01-03 01:32:00-0') as event_time
"""


class TestSnowflakeSampleMode(BaseSampleModeTest):
    @pytest.fixture(scope="class")
    def input_model_sql(self) -> str:
        return _input_model_sql
