import pytest

from dbt.tests.util import run_dbt


MY_TABLE = """
{{ config(materialized='table') }}
select 1 as id
"""


class TestListSchemas:
    """Functional test: verifies list_schemas returns the test schema."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"my_table.sql": MY_TABLE}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "test_list_schemas"}

    @pytest.fixture(scope="class", autouse=True)
    def setup(self, project):
        run_dbt(["run"])

    def test_list_schemas_includes_test_schema(self, project):
        with project.adapter.connection_named("_test"):
            schemas = project.adapter.list_schemas(project.database)

        assert project.test_schema.lower() in [s.lower() for s in schemas]

    def test_check_schema_exists_for_real_schema(self, project):
        with project.adapter.connection_named("_test"):
            exists = project.adapter.check_schema_exists(project.database, project.test_schema)
        assert exists

    def test_check_schema_exists_for_fake_schema(self, project):
        with project.adapter.connection_named("_test"):
            exists = project.adapter.check_schema_exists(
                project.database, "this_schema_does_not_exist"
            )
        assert not exists


class TestListSchemasWithDatasharing(TestListSchemas):
    """Same tests but with datasharing config enabled."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "test_list_schemas_datasharing"}

    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target):
        return {"default": {**dbt_profile_target, "datasharing": True}}
