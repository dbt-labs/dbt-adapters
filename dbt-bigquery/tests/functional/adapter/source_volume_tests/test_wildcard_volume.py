import pytest

from dbt.tests.util import run_dbt

from tests.functional.adapter.source_volume_tests import files


class TestWildcardVolume:
    @pytest.fixture(scope="class")
    def models(self):
        return {"schema.yml": files.WILDCARD_SCHEMA_YML}

    @pytest.fixture(scope="class", autouse=True)
    def setup(self, project, unique_schema):
        project.run_sql(
            f"create table {unique_schema}.events_20240101 as (select 1 as id);"
        )
        project.run_sql(
            f"create table {unique_schema}.events_20240102 as (select 1 as id union all select 2);"
        )

    def test_collect_source_volume_wildcard(self, project, unique_schema):
        """Invoke the wildcard volume macro and verify it completes successfully."""
        results = run_dbt(
            [
                "run-operation",
                "collect_source_volume_wildcard",
                "--args",
                f'{{database: "{project.database}", schema: "{unique_schema}", table_pattern: "^events_\\\\d{{8}}$"}}',
            ]
        )
        assert len(results) == 1
        assert results[0].status == "success"
