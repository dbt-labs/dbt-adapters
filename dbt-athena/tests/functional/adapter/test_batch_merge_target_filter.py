import re

import pytest

from dbt.contracts.results import RunStatus
from dbt.tests.util import run_dbt

# Uses DAY(date_column) hidden partitioning to verify that the target filter
# rewrites date_trunc('day', date_column) -> date_trunc('day', target.date_column)
models__batch_merge_target_filter_sql = """
{{ config(
    materialized='incremental',
    table_type='iceberg',
    incremental_strategy='merge',
    unique_key='id',
    partitioned_by=['DAY(date_column)'],
    force_batch=True
) }}

{% if is_incremental() %}

select * from (
    values
    (1, 'updated_1', cast('2024-01-01' as date)),
    (2, 'updated_2', cast('2024-01-02' as date)),
    (5, 'new_5', cast('2024-01-01' as date))
) as t (id, name, date_column)

{% else %}

select * from (
    values
    (1, 'original_1', cast('2024-01-01' as date)),
    (2, 'original_2', cast('2024-01-02' as date)),
    (3, 'original_3', cast('2024-01-01' as date)),
    (4, 'original_4', cast('2024-01-02' as date))
) as t (id, name, date_column)

{% endif %}
"""

# When incremental_predicates is set, auto target filter should be disabled
models__batch_merge_predicates_guard_sql = """
{{ config(
    materialized='incremental',
    table_type='iceberg',
    incremental_strategy='merge',
    unique_key='id',
    partitioned_by=['DAY(date_column)'],
    force_batch=True,
    incremental_predicates=["src.id <> 999"]
) }}

{% if is_incremental() %}

select * from (
    values
    (1, 'updated_1', cast('2024-01-01' as date)),
    (5, 'new_5', cast('2024-01-01' as date))
) as t (id, name, date_column)

{% else %}

select * from (
    values
    (1, 'original_1', cast('2024-01-01' as date)),
    (2, 'original_2', cast('2024-01-02' as date))
) as t (id, name, date_column)

{% endif %}
"""


class TestBatchMergeTargetFilter:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_batch_merge_target_filter.sql": models__batch_merge_target_filter_sql,
        }

    def test__batch_merge_includes_target_partition_filter(self, project, capsys):
        """
        Check that batch_iceberg_merge generates a target partition filter in the MERGE ON clause,
        and that the merge result data is correct.
        """

        relation_name = "test_batch_merge_target_filter"
        model_run_result_row_count_query = (
            f"select count(*) as records from {project.test_schema}.{relation_name}"
        )

        # First run: creates table with 4 rows
        first_model_run = run_dbt(["run", "--select", relation_name])
        first_model_run_result = first_model_run.results[0]
        assert first_model_run_result.status == RunStatus.Success

        records_count_first_run = project.run_sql(model_run_result_row_count_query, fetch="all")[0][0]
        assert records_count_first_run == 4

        # Drain first run output so capsys only captures the second run
        capsys.readouterr()

        # Second run: incremental with debug logs to capture SQL
        second_model_run = run_dbt(["run", "-d", "--select", relation_name])
        second_model_run_result = second_model_run.results[0]
        assert second_model_run_result.status == RunStatus.Success

        # Capture logs and verify target partition filter is present in the MERGE ON clause
        out, _ = capsys.readouterr()
        assert "date_trunc('day', target.date_column)" in out, (
            "Expected target partition filter with date_trunc('day', target.date_column) in the MERGE ON clause"
        )
        # Verify the filter appears in an ON clause context, not just anywhere in output
        assert re.search(
            r"on\s*\(.*date_trunc\('day',\s*target\.date_column\)",
            out,
            re.DOTALL | re.IGNORECASE,
        ), "Target partition filter should appear within the MERGE ON clause"

        # Verify final row count: 4 original - 2 updated + 2 updated + 1 new = 5
        records_count_second_run = project.run_sql(model_run_result_row_count_query, fetch="all")[0][0]
        assert records_count_second_run == 5

        # Verify that existing rows were updated correctly
        updated_row_query = (
            f"select name from {project.test_schema}.{relation_name} where id = 1"
        )
        updated_name = project.run_sql(updated_row_query, fetch="all")[0][0]
        assert updated_name == "updated_1"

        # Verify that rows not in incremental source were preserved
        preserved_row_query = (
            f"select name from {project.test_schema}.{relation_name} where id = 3"
        )
        preserved_name = project.run_sql(preserved_row_query, fetch="all")[0][0]
        assert preserved_name == "original_3"


class TestBatchMergePredicatesGuard:
    """When incremental_predicates is set, auto target partition filter should be disabled."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_predicates_guard.sql": models__batch_merge_predicates_guard_sql,
        }

    def test__predicates_guard_disables_auto_target_filter(self, project, capsys):
        relation_name = "test_predicates_guard"
        count_query = f"select count(*) as records from {project.test_schema}.{relation_name}"

        # First run: creates table with 2 rows
        first_run = run_dbt(["run", "--select", relation_name])
        assert first_run.results[0].status == RunStatus.Success
        assert project.run_sql(count_query, fetch="all")[0][0] == 2

        capsys.readouterr()

        # Second run: incremental with debug logs
        second_run = run_dbt(["run", "-d", "--select", relation_name])
        assert second_run.results[0].status == RunStatus.Success

        out, _ = capsys.readouterr()

        # Auto target filter should NOT be present
        assert "date_trunc('day', target.date_column)" not in out, (
            "Auto target partition filter should be disabled when incremental_predicates is set"
        )
        # But incremental_predicates should still be in the ON clause
        assert "src.id <> 999" in out, (
            "User-defined incremental_predicates should still appear in the MERGE ON clause"
        )

        # Merge should still work correctly: 2 original + 1 new = 3
        assert project.run_sql(count_query, fetch="all")[0][0] == 3


# Plain partition key (no Iceberg hidden partitioning)
models__batch_merge_plain_partition_sql = """
{{ config(
    materialized='incremental',
    table_type='iceberg',
    incremental_strategy='merge',
    unique_key='id',
    partitioned_by=['region'],
    force_batch=True
) }}

{% if is_incremental() %}

select * from (
    values
    (1, 'updated_1', 'us-east-1'),
    (5, 'new_5', 'eu-west-1')
) as t (id, name, region)

{% else %}

select * from (
    values
    (1, 'original_1', 'us-east-1'),
    (2, 'original_2', 'eu-west-1'),
    (3, 'original_3', 'us-east-1')
) as t (id, name, region)

{% endif %}
"""


class TestBatchMergePlainPartition:
    """Plain partition key should generate target.column filter."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_plain_partition.sql": models__batch_merge_plain_partition_sql,
        }

    def test__plain_partition_target_filter(self, project, capsys):
        relation_name = "test_plain_partition"
        count_query = f"select count(*) as records from {project.test_schema}.{relation_name}"

        # First run: creates table with 3 rows
        first_run = run_dbt(["run", "--select", relation_name])
        assert first_run.results[0].status == RunStatus.Success
        assert project.run_sql(count_query, fetch="all")[0][0] == 3

        capsys.readouterr()

        # Second run: incremental
        second_run = run_dbt(["run", "-d", "--select", relation_name])
        assert second_run.results[0].status == RunStatus.Success

        out, _ = capsys.readouterr()

        # Target filter should use target.region
        assert "target.region" in out, (
            "Expected target partition filter with target.region in the MERGE ON clause"
        )
        assert re.search(
            r"on\s*\(.*target\.region",
            out,
            re.DOTALL | re.IGNORECASE,
        ), "Target partition filter should appear within the MERGE ON clause"

        # 3 original + 1 new = 4
        assert project.run_sql(count_query, fetch="all")[0][0] == 4

        # Verify update applied
        updated = project.run_sql(
            f"select name from {project.test_schema}.{relation_name} where id = 1",
            fetch="all",
        )[0][0]
        assert updated == "updated_1"
