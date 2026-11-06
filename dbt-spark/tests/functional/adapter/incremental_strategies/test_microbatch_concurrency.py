from typing import List, Optional

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

_NOT_SUPPORTED_WARNING = "does not support running batches concurrently"

_SKIPPED_PROFILES = (
    "databricks_http_cluster",
    "databricks_sql_endpoint",
    "spark_session",
    "spark_http_odbc",
)

# five daily batches: dbt-core runs the first and last sequentially, and the three
# in between concurrently when the adapter and model allow it
_BACKFILL_ARGS = ["--event-time-start", "2020-01-01", "--event-time-end", "2020-01-06"]
_THREADS = ["--threads", "4"]


def microbatch_model_sql(
    file_format: str,
    partition_by: str = "['date_day']",
    concurrent_batches: Optional[bool] = True,
    extra_sql: str = "",
) -> str:
    concurrent = (
        ""
        if concurrent_batches is None
        else f"concurrent_batches={str(concurrent_batches).lower()},"
    )
    return f"""
{{{{ config(
    materialized='incremental',
    incremental_strategy='microbatch',
    event_time='event_time',
    batch_size='day',
    begin=modules.datetime.datetime(2020, 1, 1, 0, 0, 0),
    partition_by={partition_by},
    file_format='{file_format}',
    {concurrent}
) }}}}
select *, cast(event_time as date) as date_day
from {{{{ ref('input_model') }}}}
{extra_sql}
"""


class BatchThreads:
    """dbt callback recording which thread ran each microbatch batch."""

    def __init__(self):
        self.by_batch = {}

    def __call__(self, event):
        if event.info.name == "LogStartBatch":
            self.by_batch[event.data.batch_index] = event.info.thread

    def threads(self, batch_indexes: Optional[List[int]] = None) -> set:
        return {t for i, t in self.by_batch.items() if batch_indexes is None or i in batch_indexes}


class BaseSparkMicrobatch(BaseMicrobatch):
    file_format = "parquet"
    partition_by = "['date_day']"
    concurrent_batches: Optional[bool] = True
    extra_sql = ""

    @pytest.fixture(scope="class")
    def input_model_sql(self) -> str:
        return _input_model_sql

    @pytest.fixture(scope="class")
    def microbatch_model_sql(self) -> str:
        return microbatch_model_sql(
            self.file_format, self.partition_by, self.concurrent_batches, self.extra_sql
        )

    @pytest.fixture(autouse=True)
    def reset_state(self, project):
        """The project (and its schema) is shared by the tests of a class: start each
        test from three input rows and no microbatch table."""
        relation = relation_from_name(project.adapter, "microbatch_model")
        project.run_sql(f"drop table if exists {relation}")
        run_dbt(["run", "--select", "input_model"])

    def rows(self, project):
        relation = relation_from_name(project.adapter, "microbatch_model")
        return [
            tuple(r)
            for r in project.run_sql(
                f"select id, cast(cast(event_time as date) as string) from {relation} order by id",
                fetch="all",
            )
        ]

    def assert_rows_in_their_own_day(self, project, expected_ids):
        # each id belongs to the batch of its own day: a batch that read another
        # batch's temp view would have overwritten its partition with the wrong rows
        assert self.rows(project) == [(i, f"2020-01-0{i}") for i in expected_ids]

    def build_then_backfill(self, project, insert_two_rows_sql, extra_args=None, expect_pass=True):
        """Create the relation, add two days of input, backfill all five days."""
        with patch_microbatch_end_time("2020-01-03 13:57:00"):
            run_dbt(["run"] + _THREADS)
        self.assert_rows_in_their_own_day(project, [1, 2, 3])
        project.run_sql(insert_two_rows_sql)

        threads = BatchThreads()
        with patch_microbatch_end_time("2020-01-05 14:57:00"):
            results = run_dbt(
                ["run", "--select", "microbatch_model"]
                + _THREADS
                + _BACKFILL_ARGS
                + (extra_args or []),
                expect_pass=expect_pass,
                callbacks=[threads],
            )
        return results, threads


class BaseSparkMicrobatchConcurrency(BaseSparkMicrobatch):
    def test_adapter_supports_microbatch_concurrency(self, project):
        assert project.adapter.supports(Capability.MicrobatchConcurrency)

    def test_concurrent_backfill(self, project, insert_two_rows_sql):
        with patch_microbatch_end_time("2020-01-03 13:57:00"):
            _, logs = run_dbt_and_capture(["run"] + _THREADS)
        assert _NOT_SUPPORTED_WARNING not in logs
        project.run_sql(insert_two_rows_sql)

        threads = BatchThreads()
        with patch_microbatch_end_time("2020-01-05 14:57:00"):
            results = run_dbt(
                ["run", "--select", "microbatch_model"] + _THREADS + _BACKFILL_ARGS,
                callbacks=[threads],
            )
        assert len(results[0].batch_results.successful) == 5
        assert not results[0].batch_results.failed
        # the middle batches really ran in parallel, on different threads
        assert len(threads.threads([2, 3, 4])) > 1, threads.by_batch
        self.assert_rows_in_their_own_day(project, [1, 2, 3, 4, 5])

    def test_concurrent_backfill_is_idempotent(self, project, insert_two_rows_sql):
        self.build_then_backfill(project, insert_two_rows_sql)
        # re-running the same concurrent backfill overwrites the same partitions
        with patch_microbatch_end_time("2020-01-05 14:57:00"):
            run_dbt(["run", "--select", "microbatch_model"] + _THREADS + _BACKFILL_ARGS)
        self.assert_rows_in_their_own_day(project, [1, 2, 3, 4, 5])


@pytest.mark.skip_profile(*_SKIPPED_PROFILES)
class TestSparkMicrobatchConcurrencyParquet(BaseSparkMicrobatchConcurrency):
    file_format = "parquet"


@pytest.mark.skip_profile(*_SKIPPED_PROFILES)
class TestSparkMicrobatchConcurrencyIceberg(BaseSparkMicrobatchConcurrency):
    file_format = "iceberg"


@pytest.mark.skip_profile(*_SKIPPED_PROFILES)
class TestSparkMicrobatchConcurrencyIcebergPartitionTransform(BaseSparkMicrobatchConcurrency):
    """Iceberg hidden partitioning: partition by a transform of event_time."""

    file_format = "iceberg"
    partition_by = "['days(event_time)']"


@pytest.mark.skip_profile(*_SKIPPED_PROFILES)
class TestSparkMicrobatchConcurrentBatchesDisabled(BaseSparkMicrobatch):
    """`concurrent_batches: false` keeps every batch on a single thread."""

    file_format = "iceberg"
    concurrent_batches = False

    def test_batches_run_sequentially(self, project, insert_two_rows_sql):
        results, threads = self.build_then_backfill(project, insert_two_rows_sql)
        assert len(results[0].batch_results.successful) == 5
        assert len(threads.by_batch) == 5
        assert len(threads.threads()) == 1, threads.by_batch
        self.assert_rows_in_their_own_day(project, [1, 2, 3, 4, 5])


@pytest.mark.skip_profile(*_SKIPPED_PROFILES)
class TestSparkMicrobatchSelfReferenceRunsSequentially(BaseSparkMicrobatch):
    """Without a `concurrent_batches` config, a model referencing {{ this }} stays sequential."""

    file_format = "iceberg"
    concurrent_batches = None
    extra_sql = "-- depends on {{ this }}"

    def test_batches_run_sequentially(self, project, insert_two_rows_sql):
        results, threads = self.build_then_backfill(project, insert_two_rows_sql)
        assert len(results[0].batch_results.successful) == 5
        assert len(threads.threads()) == 1, threads.by_batch
        self.assert_rows_in_their_own_day(project, [1, 2, 3, 4, 5])


class BaseSparkMicrobatchConcurrentBatchFailure(BaseSparkMicrobatch):
    """A failing batch must not affect the batches running alongside it."""

    # fail exactly one batch, chosen by its id, whatever order Spark evaluates filters in
    extra_sql = (
        "{% if model.batch and model.batch.id == var('fail_batch_id', '') %}"
        "where assert_true(false) is null"
        "{% endif %}"
    )

    def test_failed_batch_does_not_affect_other_batches(self, project, insert_two_rows_sql):
        results, threads = self.build_then_backfill(
            project,
            insert_two_rows_sql,
            extra_args=["--vars", "{fail_batch_id: '20200103'}"],
            expect_pass=False,
        )
        batch_results = results[0].batch_results
        assert len(batch_results.failed) == 1
        assert batch_results.failed[0][0].day == 3
        assert len(batch_results.successful) == 4
        # day 3 keeps the data from the initial build; the other days were rewritten
        self.assert_rows_in_their_own_day(project, [1, 2, 3, 4, 5])

        # re-running from the failed day onwards (without the failure) completes the backfill
        with patch_microbatch_end_time("2020-01-05 14:57:00"):
            results = run_dbt(
                ["run", "--select", "microbatch_model"]
                + ["--event-time-start", "2020-01-03", "--event-time-end", "2020-01-06"]
                + _THREADS
            )
        batch_results = results[0].batch_results
        assert not batch_results.failed
        assert any(start.day == 3 for start, _ in batch_results.successful)
        self.assert_rows_in_their_own_day(project, [1, 2, 3, 4, 5])


@pytest.mark.skip_profile(*_SKIPPED_PROFILES)
class TestSparkMicrobatchConcurrentBatchFailureParquet(BaseSparkMicrobatchConcurrentBatchFailure):
    file_format = "parquet"


@pytest.mark.skip_profile(*_SKIPPED_PROFILES)
class TestSparkMicrobatchConcurrentBatchFailureIceberg(BaseSparkMicrobatchConcurrentBatchFailure):
    file_format = "iceberg"
