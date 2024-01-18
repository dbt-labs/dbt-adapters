import pytest

from dbt.tests.adapter.column_types.test_column_types import BaseColumnTypes

from tests.functional.column_type_tests import fixtures


class TestPostgresColumnTypes(BaseColumnTypes):
    @pytest.fixture(scope="class")
    def models(self):
        return {"model.sql": fixtures.model_sql, "schema.yml": fixtures.schema_yml}

    def test_run_and_test(self, project):
        self.run_and_test()
