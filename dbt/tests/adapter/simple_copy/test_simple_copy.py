# mix in biguery
# mix in snowflake
from pathlib import Path

import pytest

from dbt.tests.util import run_dbt, rm_file, write_file, check_relations_equal
import models
import schemas
import seeds


class SimpleCopySetup:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "advanced_incremental.sql": models.ADVANCED_INCREMENTAL,
            "compound_sort.sql": models.COMPOUND_SORT,
            "disabled.sql": models.DISABLED,
            "empty.sql": models.EMPTY,
            "get_and_ref.sql": models.GET_AND_REF,
            "incremental.sql": models.INCREMENTAL,
            "interleaved_sort.sql": models.INTERLEAVED_SORT,
            "materialized.sql": models.MATERIALIZED,
            "view_model.sql": models.VIEW_MODEL,
        }

    @pytest.fixture(scope="class")
    def properties(self):
        return {"schema.yml": schemas.SCHEMA_YML}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"seed.csv": seeds.INITIAL}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"seeds": {"quote_columns": False}}


class SimpleCopy(SimpleCopySetup):
    def test_simple_copy(self, project):
        # Load the seed file and check that it worked
        results = run_dbt(["seed"])
        assert len(results) == 1

        # Run the project and ensure that all the models loaded
        results = run_dbt()
        assert len(results) == 7
        check_relations_equal(
            project.adapter, ["seed", "view_model", "incremental", "materialized", "get_and_ref"]
        )

        # Change the seed.csv file and see if everything is the same, i.e. everything has been updated
        main_seed_file = project.project_root / Path("seeds") / Path("seed.csv")
        rm_file(main_seed_file)
        write_file(seeds.UPDATE, main_seed_file)
        results = run_dbt(["seed"])
        assert len(results) == 1
        results = run_dbt()
        assert len(results) == 7
        check_relations_equal(
            project.adapter, ["seed", "view_model", "incremental", "materialized", "get_and_ref"]
        )

    def test_simple_copy_with_materialized_views(self, project):
        project.run_sql(f"create table {project.test_schema}.unrelated_table (id int)")
        sql = f"""
            create materialized view {project.test_schema}.unrelated_materialized_view as (
                select * from {project.test_schema}.unrelated_table
            )
        """
        project.run_sql(sql)
        sql = f"""
            create view {project.test_schema}.unrelated_view as (
                select * from {project.test_schema}.unrelated_materialized_view
            )
        """
        project.run_sql(sql)
        results = run_dbt(["seed"])
        assert len(results) == 1
        results = run_dbt()
        assert len(results) == 7


class EmptyModelsArentRun(SimpleCopySetup):
    def test_dbt_doesnt_run_empty_models(self, project):
        results = run_dbt(["seed"])
        assert len(results) == 1
        results = run_dbt()
        assert len(results) == 7

        tables = project.get_tables_in_schema()

        assert "empty" not in tables.keys()
        assert "disabled" not in tables.keys()
