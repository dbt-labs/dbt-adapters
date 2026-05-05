import pytest

from dbt.tests.util import run_dbt

from tests.functional.adapter.source_volume_tests import files


class TestTableVolume:
    @pytest.fixture(scope="class")
    def models(self):
        return {"schema.yml": files.SCHEMA_YML}

    @pytest.fixture(scope="class", autouse=True)
    def setup(self, project, unique_schema):
        project.run_sql(
            f"create table {unique_schema}.volume_test_table as (select 1 as id union all select 2 union all select 3);"
        )

    def test_collect_source_volume(self, project, unique_schema):
        """Invoke the volume macro and verify it completes successfully."""
        results = run_dbt(
            [
                "run-operation",
                "collect_source_volume",
                "--args",
                f'{{database: "{project.database}", schema: "{unique_schema}", identifier: "volume_test_table"}}',
            ]
        )
        assert len(results) == 1
        assert results[0].status == "success"
