import pytest

from dbt.tests.util import run_dbt, run_dbt_and_capture
from tests.functional.fixtures.profiles import spark_session_target


INCREMENTAL_MODEL = """
{{ config(materialized='incremental', incremental_strategy='append') }}

select 1 as id
{% if is_incremental() %}
union all
select 2 as id
{% endif %}
"""

TABLE_MODEL = """
{{ config(materialized='table', alias='table_events') }}

select 1 as id
"""

VIEW_MODEL = """
{{ config(materialized='view', alias='view_events') }}

select 1 as id
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
    def project_config_update(self):
        return {
            "quoting": {
                "database": True,
                "schema": True,
                "identifier": True,
            }
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "events.sql": INCREMENTAL_MODEL,
            "quoted_events.sql": TABLE_MODEL,
            "quoted_view.sql": VIEW_MODEL,
        }

    def test_catalog_qualified_incremental_and_docs(self, project):
        assert project.database == "spark_catalog"

        first_run, first_run_logs = run_dbt_and_capture(["--debug", "run"])
        second_run = run_dbt(["run"])
        assert len(first_run) == 3
        assert len(second_run) == 3

        rendered_logs = " ".join(first_run_logs.split())
        table_relation = f"`spark_catalog`.`{project.test_schema}`.`table_events`"
        view_relation = f"`spark_catalog`.`{project.test_schema}`.`view_events`"
        assert f"create table {table_relation}" in rendered_logs
        assert f"create or replace view {view_relation}" in rendered_logs

        with project.adapter.connection_named("__test"):
            relation = project.adapter.get_relation(
                database=project.database,
                schema=project.test_schema,
                identifier="events",
            )
            assert relation is not None
            assert str(relation) == f"`spark_catalog`.`{project.test_schema}`.`events`"
            quoted_relation = project.adapter.get_relation(
                database=project.database,
                schema=project.test_schema,
                identifier="table_events",
            )
            assert quoted_relation is not None
            assert str(quoted_relation) == table_relation
            quoted_view = project.adapter.get_relation(
                database=project.database,
                schema=project.test_schema,
                identifier="view_events",
            )
            assert quoted_view is not None
            assert str(quoted_view) == view_relation
            assert project.adapter.check_schema_exists(
                project.database.upper(), project.test_schema.upper()
            )

        row_count = project.run_sql(f"select count(*) from {relation}", fetch="one")
        assert row_count[0] == 3

        catalog = run_dbt(["docs", "generate"])
        table = catalog.nodes["model.test.events"]
        assert table.metadata.database == "spark_catalog"
        assert table.metadata.schema == project.test_schema
        quoted_table = catalog.nodes["model.test.quoted_events"]
        assert quoted_table.metadata.database == "spark_catalog"
        assert quoted_table.metadata.schema == project.test_schema
        quoted_view = catalog.nodes["model.test.quoted_view"]
        assert quoted_view.metadata.database == "spark_catalog"
        assert quoted_view.metadata.schema == project.test_schema
