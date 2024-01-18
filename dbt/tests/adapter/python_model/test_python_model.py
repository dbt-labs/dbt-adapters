import os

import pytest
import yaml

from dbt.tests.util import run_dbt
import models
import schemas
import seeds


class BasePythonModelTests:
    @pytest.fixture(scope="class", autouse=True)
    def setEnvVars(self):
        os.environ["DBT_TEST_SCHEMA_NAME_VARIABLE"] = "test_run_schema"

        yield

        del os.environ["DBT_TEST_SCHEMA_NAME_VARIABLE"]

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"source.csv": seeds.source_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": schemas.schema_yml,
            "my_sql_model.sql": models.basic_sql,
            "my_versioned_sql_model_v1.sql": models.basic_sql,
            "my_python_model.py": models.basic_python,
            "second_sql_model.sql": models.second_sql,
        }

    def test_singular_tests(self, project):
        # test command
        vars_dict = {
            "test_run_schema": project.test_schema,
        }

        run_dbt(["seed", "--vars", yaml.safe_dump(vars_dict)])
        results = run_dbt(["run", "--vars", yaml.safe_dump(vars_dict)])
        assert len(results) == 4


class BasePythonIncrementalTests:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"+incremental_strategy": "merge"}}

    @pytest.fixture(scope="class")
    def models(self):
        return {"m_1.sql": models.m_1, "incremental.py": models.incremental_python}

    def test_incremental(self, project):
        # create m_1 and run incremental model the first time
        run_dbt(["run"])
        test_schema_relation = project.adapter.Relation.create(
            database=project.database, schema=project.test_schema
        )
        assert (
            project.run_sql(
                f"select count(*) from {test_schema_relation}.incremental",
                fetch="one",
            )[0]
            == 5
        )
        # running incremental model again will not cause any changes in the result model
        run_dbt(["run", "-s", "incremental"])
        assert (
            project.run_sql(
                f"select count(*) from {test_schema_relation}.incremental",
                fetch="one",
            )[0]
            == 5
        )
        # add 3 records with one supposed to be filtered out
        project.run_sql(f"insert into {test_schema_relation}.m_1(id) values (0), (6), (7)")
        # validate that incremental model would correctly add 2 valid records to result model
        run_dbt(["run", "-s", "incremental"])
        assert (
            project.run_sql(
                f"select count(*) from {test_schema_relation}.incremental",
                fetch="one",
            )[0]
            == 7
        )
