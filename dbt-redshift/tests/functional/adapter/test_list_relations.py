import pytest

from dbt.tests.util import run_dbt


MY_TABLE = """
{{ config(materialized='table') }}
select 1 as id
"""

MY_VIEW = """
{{ config(materialized='view', bind=True) }}
select * from {{ ref('my_table') }}
"""

MY_MATERIALIZED_VIEW = """
{{ config(materialized='materialized_view') }}
select * from {{ ref('my_table') }}
"""


class TestListRelationsWithoutCaching:
    """Functional test: verifies list_relations_without_caching returns correct types."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_table.sql": MY_TABLE,
            "my_view.sql": MY_VIEW,
            "my_materialized_view.sql": MY_MATERIALIZED_VIEW,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "test_list_relations"}

    @pytest.fixture(scope="class", autouse=True)
    def setup(self, project):
        run_dbt(["run"])

    def test_list_relations_returns_all_types(self, project):
        """Verify the adapter lists tables, views, and materialized views."""
        with project.adapter.connection_named("_test"):
            schema_relation = project.adapter.Relation.create(
                database=project.database,
                schema=project.test_schema,
            )
            relations = project.adapter.list_relations_without_caching(schema_relation)

        relation_map = {rel.identifier: rel.type for rel in relations}

        assert relation_map["my_table"] == "table"
        assert relation_map["my_view"] == "view"
        assert relation_map["my_materialized_view"] == "materialized_view"


class TestListRelationsWithShowApis(TestListRelationsWithoutCaching):
    """Same tests but with show_apis flag enabled."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "test_list_relations_show_apis",
            "flags": {"redshift_use_show_apis": True},
        }
