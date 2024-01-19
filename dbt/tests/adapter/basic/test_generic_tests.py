import pytest

from dbt.tests.adapter.basic import files
from dbt.tests.util import run_dbt


class BaseGenericTests:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "generic_tests"}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "base.csv": files.seeds_base_csv,
            "schema.yml": files.generic_test_seed_yml,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "view_model.sql": files.base_view_sql,
            "table_model.sql": files.base_table_sql,
            "schema.yml": files.schema_base_yml,
            "schema_view.yml": files.generic_test_view_yml,
            "schema_table.yml": files.generic_test_table_yml,
        }

    @pytest.fixture(autouse=True)
    def clean_up(self, project):
        yield
        with project.adapter.connection_named("__test"):
            relation = project.adapter.Relation.create(
                database=project.database, schema=project.test_schema
            )
            project.adapter.drop_schema(relation)

    pass

    def test_generic_tests(self, project):
        # seed command
        results = run_dbt(["seed"])

        # test command selecting base model
        results = run_dbt(["test", "-m", "base"])
        assert len(results) == 1

        # run command
        results = run_dbt(["run"])
        assert len(results) == 2

        # test command, all tests
        results = run_dbt(["test"])
        assert len(results) == 3


class TestGenericTests(BaseGenericTests):
    pass
