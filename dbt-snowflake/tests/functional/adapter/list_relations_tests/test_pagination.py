import os

import pytest

from dbt.tests.util import run_dbt, run_dbt_and_capture

"""
Testing rationale:
- snowflake SHOW TERSE OBJECTS command returns at max 10K objects in a single call
- when dbt attempts to write into a schema with more than 10K objects, compilation will fail
  unless we paginate the result
- we default pagination to 10 pages, but users want to configure this
  - we instead use that here to force failures by making it smaller
"""


TABLE = """
{{ config(materialized='table') }}
select 1 as id
"""


VIEW = """
{{ config(materialized='view') }}
select id from {{ ref('my_model_base') }}
"""


DYNAMIC_TABLE = (
    """
{{ config(
    materialized='dynamic_table',
    target_lag='1 hour',
    snowflake_warehouse='"""
    + os.getenv("SNOWFLAKE_TEST_WAREHOUSE", "")
    + """',
) }}

select id from {{ ref('my_model_base') }}
"""
)


class BaseConfig:
    VIEWS = 90
    DYNAMIC_TABLES = 10

    @pytest.fixture(scope="class")
    def models(self):
        my_models = {"my_model_base.sql": TABLE}
        for view in range(self.VIEWS):
            my_models[f"my_model_{view}.sql"] = VIEW
        for dynamic_table in range(self.DYNAMIC_TABLES):
            my_models[f"my_dynamic_table_{dynamic_table}.sql"] = DYNAMIC_TABLE
        return my_models

    @pytest.fixture(scope="class", autouse=True)
    def setup(self, project):
        run_dbt(["run"])

    def test_list_relations(self, project):
        kwargs = {"schema_relation": project.test_schema}
        with project.adapter.connection_named("__test"):
            relations = project.adapter.execute_macro(
                "snowflake__list_relations_without_caching", kwargs=kwargs
            )
        assert len(relations) == self.VIEWS + self.DYNAMIC_TABLES + 1

    def test_on_run(self, project):
        _, logs = run_dbt_and_capture(["run"])
        assert "list_relations_per_page" not in logs
        assert "list_relations_page_limit" not in logs


class TestListRelationsWithoutCachingSmall(BaseConfig):
    pass


class TestListRelationsWithoutCachingLarge(BaseConfig):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {
                "list_relations_per_page": 10,
                "list_relations_page_limit": 20,
            }
        }


class TestListRelationsWithoutCachingTooLarge(BaseConfig):
    """
    We raise a warning, not an error, if the number of relations exceeds the page limit times the iteration limit.
    In this case, we return the maximum number of relations (e.g. 5 pages * 10 relations per page) and continue.
    """

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {
                "list_relations_per_page": 10,
                "list_relations_page_limit": 5,
            }
        }

    def test_list_relations(self, project):
        kwargs = {"schema_relation": project.test_schema}
        with project.adapter.connection_named("__test"):
            relations = project.adapter.execute_macro(
                "snowflake__list_relations_without_caching", kwargs=kwargs
            )
            assert (
                len(relations) == 10 * 5
            )  # the maximum from the settings, not the number of relations (101)

    def test_on_run(self, project):
        # the warning should only show in this scenario
        _, logs = run_dbt_and_capture(["run"])
        assert "list_relations_per_page" in logs
        assert "list_relations_page_limit" in logs
