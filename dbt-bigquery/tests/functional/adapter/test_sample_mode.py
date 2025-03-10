import datetime
import os
from unittest import mock
import pytest

from dbt.tests.adapter.sample_mode.test_sample_mode import (
    BaseSampleModeTest,
)
from dbt.tests.util import run_dbt

now = datetime.datetime.now()
twelve_hours_ago = now - datetime.timedelta(hours=12)
two_days_ago = now - datetime.timedelta(days=2)

_input_model_sql = f"""
{{{{ config(materialized='table', event_time='event_time') }}}}
select 1 as id, cast('{two_days_ago.strftime('%Y-%m-%d %H:%M:%S-0')}' as timestamp) as event_time
UNION ALL
select 2 as id, cast('{twelve_hours_ago.strftime('%Y-%m-%d %H:%M:%S-0')}' as timestamp) as event_time
UNION ALL
select 3 as id, cast('{now.strftime('%Y-%m-%d %H:%M:%S-0')}' as timestamp) as event_time
"""


class TestBigQuerySampleMode(BaseSampleModeTest):
    @pytest.fixture(scope="class")
    def input_model_sql(self) -> str:
        return _input_model_sql

    @mock.patch.dict(os.environ, {"DBT_EXPERIMENTAL_SAMPLE_MODE": "True"})
    def test_sample_mode(self, project) -> None:
        _ = run_dbt(["run"])
        self.assert_row_count(
            project=project,
            relation_name="model_that_samples_input_sql",
            expected_row_count=3,
        )

        _ = run_dbt(["run", "--sample=1 day"])
        self.assert_row_count(
            project=project,
            relation_name="model_that_samples_input_sql",
            expected_row_count=2,
        )
