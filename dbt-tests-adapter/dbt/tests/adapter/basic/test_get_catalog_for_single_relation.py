import pytest

from dbt.tests.util import run_dbt, get_connection

models__my_table_model_sql = """
select * from {{ ref('my_seed') }}
"""


models__my_view_model_sql = """
{{
    config(
        materialized='view',
    )
}}

select * from {{ ref('my_seed') }}
"""

seed__my_seed_csv = """id,first_name,email,ip_address,updated_at
1,Larry,lking0@miitbeian.gov.cn,69.135.206.194,2008-09-12 19:08:31
"""


class BaseGetCatalogForSingleRelation:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "get_catalog_for_single_relation"}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "my_seed.csv": seed__my_seed_csv,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_view_model.sql": models__my_view_model_sql,
            "my_table_model.sql": models__my_table_model_sql,
        }

    @pytest.fixture(scope="class")
    def expected_catalog_my_seed(self, project):
        raise NotImplementedError(
            "To use this test, please implement `get_catalog_for_single_relation`, inherited from `SQLAdapter`."
        )

    @pytest.fixture(scope="class")
    def expected_catalog_my_model(self, project):
        raise NotImplementedError(
            "To use this test, please implement `get_catalog_for_single_relation`, inherited from `SQLAdapter`."
        )

    def get_relation_for_identifier(self, project, identifier):
        return project.adapter.get_relation(
            database=project.database,
            schema=project.test_schema,
            identifier=identifier,
        )

    def test_get_catalog_for_single_relation(
        self, project, expected_catalog_my_seed, expected_catalog_my_view_model
    ):
        results = run_dbt(["seed"])
        assert len(results) == 1

        my_seed_relation = self.get_relation_for_identifier(project, "my_seed")

        with get_connection(project.adapter):
            actual_catalog_my_seed = project.adapter.get_catalog_for_single_relation(
                my_seed_relation
            )

        assert actual_catalog_my_seed == expected_catalog_my_seed

        results = run_dbt(["run"])
        assert len(results) == 2

        my_view_model_relation = self.get_relation_for_identifier(project, "my_view_model")

        with get_connection(project.adapter):
            actual_catalog_my_view_model = project.adapter.get_catalog_for_single_relation(
                my_view_model_relation
            )

        assert actual_catalog_my_view_model == expected_catalog_my_view_model
