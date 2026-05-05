import pytest

from dbt.tests.util import run_dbt

from tests.functional.adapter.source_volume_tests import files


class TestPartitionVolume:
    @pytest.fixture(scope="class")
    def models(self):
        return {"schema.yml": files.PARTITION_SCHEMA_YML}

    @pytest.fixture(scope="class", autouse=True)
    def setup(self, project, unique_schema):
        project.run_sql(
            f"""create table {unique_schema}.partitioned_volume_table (
                id int64,
                created_date date
            )
            partition by created_date
            as (
                select 1 as id, date('2024-01-01') as created_date union all
                select 2 as id, date('2024-01-02') as created_date union all
                select 3 as id, date('2024-01-03') as created_date
            );"""
        )

    def test_collect_source_volume_partitions(self, project, unique_schema):
        """Invoke the partition volume macro and verify it completes successfully."""
        results = run_dbt(
            [
                "run-operation",
                "collect_source_volume_partitions",
                "--args",
                f'{{database: "{project.database}", schema: "{unique_schema}", identifier: "partitioned_volume_table", partition_field: "created_date", partition_range: 3}}',
            ]
        )
        assert len(results) == 1
        assert results[0].status == "success"
