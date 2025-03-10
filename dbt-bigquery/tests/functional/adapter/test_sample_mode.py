import pytest

from dbt.tests.adapter.sample_mode.test_sample_mode import (
    BaseSampleModeTest,
)

_input_model_sql = """
select 1 as id, cast('2025-01-01 01:25:00-0' as datetime) as event_time
UNION ALL
select 2 as id, cast('2025-01-02 13:47:00-0' as datetime) as event_time
UNION ALL
select 3 as id, cast('2025-01-03 01:32:00-0' as datetime) as event_time
"""


class TestBigQuerySampleMode(BaseSampleModeTest):
    @pytest.fixture(scope="class")
    def input_model_sql(self) -> str:
        return _input_model_sql
