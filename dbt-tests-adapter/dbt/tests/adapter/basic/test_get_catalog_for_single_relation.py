import pytest

from dbt.adapters.capability import Capability


models__my_model_sql = """
{{
    config(
        materialized='view',
    )
}}

select * from {{ ref('seed') }}
"""

seed__my_seed_csv = """id,first_name,email,ip_address,updated_at
1,Larry,lking0@miitbeian.gov.cn,69.135.206.194,2008-09-12 19:08:31
"""


class BaseGetCatalogForSingleRelation:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "my_seed.csv": seed__my_seed_csv,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": models__my_model_sql,
        }

    @pytest.fixture(autouse=True)
    def clean_up(self, project):
        yield
        with project.adapter.connection_named("__test"):
            relation = project.adapter.Relation.create(
                database=project.database, schema=project.test_schema
            )
            project.adapter.drop_schema(relation)

    def test_get_catalog_for_single_relation(self, project):
        adapter = project.adapter

        if adapter.supports(Capability.GetCatalogForSingleRelation):
            my_model_relation = adapter.Relation.create(
                database=project.database,
                schema=project.test_schema,
                identifier="my_model",
            )

            expected_catalog_table = 1

            assert (
                project.adapter.get_catalog_for_single_relation(my_model_relation)
                == expected_catalog_table
            )


class TestGetCatalogForSingleRelation(BaseGetCatalogForSingleRelation):
    pass
