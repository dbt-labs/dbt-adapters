import pytest
from dbt.tests.util import run_dbt

from tests.functional.custom_aliases import fixtures


class TestAliases:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model1.sql": fixtures.model1_sql,
            "model2.sql": fixtures.model2_sql,
            "schema.yml": fixtures.schema_yml,
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {"macros.sql": fixtures.macros_sql}

    def test_customer_alias_name(self, project):
        results = run_dbt(["run"])
        assert len(results) == 2

        results = run_dbt(["test"])
        assert len(results) == 2


class TestAliasesWithConfig(TestAliases):
    @pytest.fixture(scope="class")
    def macros(self):
        return {"macros.sql": fixtures.macros_config_sql}
