import pytest

from dbt.tests.util import run_dbt, check_relations_equal
import models
import schemas
import seeds


class SimpleCopyUppercase:
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
            "ADVANCED_INCREMENTAL.sql": models.ADVANCED_INCREMENTAL,
            "COMPOUND_SORT.sql": models.COMPOUND_SORT,
            "DISABLED.sql": models.DISABLED,
            "EMPTY.sql": models.EMPTY,
            "GET_AND_REF.sql": models.GET_AND_REF_UPPERCASE,
            "INCREMENTAL.sql": models.INCREMENTAL,
            "INTERLEAVED_SORT.sql": models.INTERLEAVED_SORT,
            "MATERIALIZED.sql": models.MATERIALIZED,
            "VIEW_MODEL.sql": models.VIEW_MODEL,
        }

    @pytest.fixture(scope="class")
    def properties(self):
        return {"schema.yml": schemas.SCHEMA_YML}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"seed.csv": seeds.INITIAL}

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
