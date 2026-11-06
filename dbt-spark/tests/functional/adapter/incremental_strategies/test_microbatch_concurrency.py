import pytest

from dbt.adapters.capability import Capability
from dbt.tests.adapter.incremental.test_incremental_microbatch import (
    BaseMicrobatch,
    patch_microbatch_end_time,
)
from dbt.tests.util import relation_from_name, run_dbt, run_dbt_and_capture

_input_model_sql = """
{{ config(materialized='table', event_time='event_time') }}
select 1 as id, TIMESTAMP '2020-01-01 00:00:00-0' as event_time
union all
select 2 as id, TIMESTAMP '2020-01-02 00:00:00-0' as event_time
union all
select 3 as id, TIMESTAMP '2020-01-03 00:00:00-0' as event_time
"""

_microbatch_model_sql = """
{{{{ config(
    materialized='incremental',
    incremental_strategy='microbatch',
    event_time='event_time',
    batch_size='day',
    begin=modules.datetime.datetime(2020, 1, 1, 0, 0, 0),
    partition_by=['date_day'],
    file_format='{file_format}',
    concurrent_batches=true,
) }}}}
select *, cast(event_time as date) as date_day
from {{{{ ref('input_model') }}}}
"""

_NOT_SUPPORTED_WARNING = "does not support running batches concurrently"

_SKIPPED_PROFILES = (
    "databricks_http_cluster",
    "databricks_sql_endpoint",
    "spark_session",
    "spark_http_odbc",
)


class BaseSparkMicrobatchConcurrency(BaseMicrobatch):
    file_format = "parquet"

    @pytest.fixture(scope="class")
    def input_model_sql(self) -> str:
        return _input_model_sql

    @pytest.fixture(scope="class")
    def microbatch_model_sql(self) -> str:
        return _microbatch_model_sql.format(file_format=self.file_format)

    def assert_rows_in_their_own_partition(self, project, expected_ids):
        relation = relation_from_name(project.adapter, "microbatch_model")
        rows = project.run_sql(
            f"select id, cast(date_day as string) from {relation} order by id", fetch="all"
        )
        # each id lives in the partition of its own day: a batch that read another
        # batch's temp view would have overwritten its partition with the wrong rows
        assert [tuple(r) for r in rows] == [(i, f"2020-01-0{i}") for i in expected_ids]

    def test_adapter_supports_microbatch_concurrency(self, project):
        assert project.adapter.supports(Capability.MicrobatchConcurrency)

    def test_concurrent_backfill(self, project, insert_two_rows_sql):
        # initial run creates the relation (dbt-core runs the first batch sequentially)
        with patch_microbatch_end_time("2020-01-03 13:57:00"):
            _, logs = run_dbt_and_capture(["run", "--threads", "4"])
        assert _NOT_SUPPORTED_WARNING not in logs
        self.assert_rows_in_their_own_partition(project, [1, 2, 3])

        project.run_sql(insert_two_rows_sql)

        # relation exists now, so the five daily batches run concurrently
        with patch_microbatch_end_time("2020-01-05 14:57:00"):
            results = run_dbt(
                [
                    "run",
                    "--select",
                    "microbatch_model",
                    "--threads",
                    "4",
                    "--event-time-start",
                    "2020-01-01",
                    "--event-time-end",
                    "2020-01-06",
                ]
            )
        assert len(results[0].batch_results.successful) == 5
        self.assert_rows_in_their_own_partition(project, [1, 2, 3, 4, 5])


@pytest.mark.skip_profile(*_SKIPPED_PROFILES)
class TestSparkMicrobatchConcurrencyParquet(BaseSparkMicrobatchConcurrency):
    file_format = "parquet"


@pytest.mark.skip_profile(*_SKIPPED_PROFILES)
class TestSparkMicrobatchConcurrencyIceberg(BaseSparkMicrobatchConcurrency):
    file_format = "iceberg"
