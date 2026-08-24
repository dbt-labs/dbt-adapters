import pytest

from dbt.tests.util import run_dbt
from tests.functional.fixtures.profiles import spark_session_target


INCREMENTAL_MODEL = """
{{ config(materialized='incremental', incremental_strategy='append') }}

select 1 as id
{% if is_incremental() %}
union all
select 2 as id
{% endif %}
"""


@pytest.mark.skip_profile(
    "apache_spark",
    "spark_http_odbc",
    "databricks_cluster",
    "databricks_http_cluster",
    "databricks_sql_endpoint",
)
class TestThreePartIdentifiers:
    @pytest.fixture(scope="class")
    def dbt_profile_target(self):
        target = spark_session_target()
        target["catalog"] = "spark_catalog"
        return target

    @pytest.fixture(scope="class")
    def models(self):
        return {"events.sql": INCREMENTAL_MODEL}

    def test_catalog_qualified_incremental_and_docs(self, project):
        assert project.database == "spark_catalog"

        first_run = run_dbt(["run"])
        second_run = run_dbt(["run"])
        assert len(first_run) == 1
        assert len(second_run) == 1

        with project.adapter.connection_named("__test"):
            relation = project.adapter.get_relation(
                database=project.database,
                schema=project.test_schema,
                identifier="events",
            )
            assert relation is not None
            assert str(relation) == f"spark_catalog.{project.test_schema}.events"
            assert project.adapter.check_schema_exists(project.database, project.test_schema)

        row_count = project.run_sql(f"select count(*) from {relation}", fetch="one")
        assert row_count[0] == 3

        catalog = run_dbt(["docs", "generate"])
        table = catalog.nodes["model.test.events"]
        assert table.metadata.database == "spark_catalog"
        assert table.metadata.schema == project.test_schema
