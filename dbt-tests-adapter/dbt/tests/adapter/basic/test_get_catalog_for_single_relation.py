import pytest

models__model_sql = """
{{
    config(
        materialized='view',
    )
}}

select * from {{ ref('seed') }}
"""

seed__seed_csv = """id,first_name,email,ip_address,updated_at
1,Larry,lking0@miitbeian.gov.cn,69.135.206.194,2008-09-12 19:08:31
"""


class BaseGetCatalogForSingleRelation:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "seed.csv": seed__seed_csv,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model.sql": models__model_sql,
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
        assert (
            project.adapter.get_catalog_for_single_relation(
                database=project.database,
                schema=project.test_schema,
                identifier="model",
                quote_columns=False,
            )
            == 1
        )


class TestGetCatalogForSingleRelation(BaseGetCatalogForSingleRelation):
    pass
