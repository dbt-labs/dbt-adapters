import pytest

from dbt.tests.adapter.simple_copy import fixtures
from dbt.tests.util import run_dbt, check_relations_equal


class BaseSimpleCopyUppercase:
    @pytest.fixture(scope="class")
    def dbt_profile_target(self):
        return {
            "type": "postgres",
            "threads": 4,
            "host": "localhost",
            "port": 5432,
            "user": "root",
            "pass": "password",
            "dbname": "dbtMixedCase",
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "ADVANCED_INCREMENTAL.sql": fixtures._MODELS__ADVANCED_INCREMENTAL,
            "COMPOUND_SORT.sql": fixtures._MODELS__COMPOUND_SORT,
            "DISABLED.sql": fixtures._MODELS__DISABLED,
            "EMPTY.sql": fixtures._MODELS__EMPTY,
            "GET_AND_REF.sql": fixtures._MODELS_GET_AND_REF_UPPERCASE,
            "INCREMENTAL.sql": fixtures._MODELS__INCREMENTAL,
            "INTERLEAVED_SORT.sql": fixtures._MODELS__INTERLEAVED_SORT,
            "MATERIALIZED.sql": fixtures._MODELS__MATERIALIZED,
            "VIEW_MODEL.sql": fixtures._MODELS__VIEW_MODEL,
        }

    @pytest.fixture(scope="class")
    def properties(self):
        return {
            "schema.yml": fixtures._PROPERTIES__SCHEMA_YML,
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"seed.csv": fixtures._SEEDS__SEED_INITIAL}

    def test_simple_copy_uppercase(self, project):
        # Load the seed file and check that it worked
        results = run_dbt(["seed"])
        assert len(results) == 1

        # Run the project and ensure that all the models loaded
        results = run_dbt()
        assert len(results) == 7

        check_relations_equal(
            project.adapter, ["seed", "VIEW_MODEL", "INCREMENTAL", "MATERIALIZED", "GET_AND_REF"]
        )


class TestSimpleCopyUppercase(BaseSimpleCopyUppercase):
    pass
