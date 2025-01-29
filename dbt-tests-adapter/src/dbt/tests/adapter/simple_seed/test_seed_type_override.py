import pytest

from dbt.tests.adapter.simple_seed import fixtures, seeds
from dbt.tests.util import run_dbt


class BaseSimpleSeedColumnOverride:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": fixtures.properties__schema_yml,
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "seed_enabled.csv": seeds.seeds__enabled_in_config_csv,
            "seed_disabled.csv": seeds.seeds__disabled_in_config_csv,
            "seed_tricky.csv": seeds.seeds__tricky_csv,
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {"schema_test.sql": fixtures.macros__schema_test}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seeds": {
                "test": {
                    "enabled": False,
                    "quote_columns": True,
                    "seed_enabled": {"enabled": True, "+column_types": self.seed_enabled_types()},
                    "seed_tricky": {
                        "enabled": True,
                        "+column_types": self.seed_tricky_types(),
                    },
                },
            },
        }

    @staticmethod
    def seed_enabled_types():
        return {
            "seed_id": "text",
            "birthday": "date",
        }

    @staticmethod
    def seed_tricky_types():
        return {
            "seed_id_str": "text",
            "looks_like_a_bool": "text",
            "looks_like_a_date": "text",
        }

    def test_simple_seed_with_column_override(self, project):
        seed_results = run_dbt(["seed", "--show"])
        assert len(seed_results) == 2
        test_results = run_dbt(["test"])
        assert len(test_results) == 10


class TestSimpleSeedColumnOverride(BaseSimpleSeedColumnOverride):
    pass
