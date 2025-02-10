import pytest

from dbt.tests.adapter.column_types import fixtures
from dbt.tests.util import run_dbt


class BaseColumnTypes:
    @pytest.fixture(scope="class")
    def macros(self):
        return {"test_is_type.sql": fixtures.macro_test_is_type_sql}

    def run_and_test(self):
        results = run_dbt(["run"])
        assert len(results) == 1
        results = run_dbt(["test"])
        assert len(results) == 1


class BasePostgresColumnTypes(BaseColumnTypes):
    @pytest.fixture(scope="class")
    def models(self):
        return {"model.sql": fixtures.model_sql, "schema.yml": fixtures.schema_yml}

    def test_run_and_test(self, project):
        self.run_and_test()


class TestPostgresColumnTypes(BasePostgresColumnTypes):
    pass
