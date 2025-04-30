import os
from pprint import pformat
from typing import Dict

import pytest
import yaml

from dbt.tests.util import relation_from_name, run_dbt


basic_sql = """
select 1 as id union all
select 1 as id union all
select 1 as id union all
select 1 as id union all
select 1 as id union all
select 1 as id
"""
basic_python = """
def model(dbt, _):
    dbt.config(
        materialized='table',
    )
    df =  dbt.ref("my_sql_model")
    df2 = dbt.ref("my_versioned_sql_model", v=1)
    df3 = dbt.ref("my_versioned_sql_model", version=1)
    df4 = dbt.ref("test", "my_versioned_sql_model", v=1)
    df5 = dbt.ref("test", "my_versioned_sql_model", version=1)
    df6 = dbt.source("test_source", "test_table")
    df = df.limit(2)
    return df
"""

_input_model_sql = """
{{ config(materialized='table', event_time='event_time') }}
select 1 as id, TIMESTAMP '2025-01-01 01:25:00-0' as event_time
UNION ALL
select 2 as id, TIMESTAMP '2025-01-02 13:47:00-0' as event_time
UNION ALL
select 3 as id, TIMESTAMP '2025-01-03 01:32:00-0' as event_time
"""

_model_that_refs_input_py = """
def model(dbt, session):
    df = dbt.ref("input_model")
    return df
"""

second_sql = """
select * from {{ref('my_python_model')}}
"""
schema_yml = """version: 2
models:
  - name: my_versioned_sql_model
    versions:
      - v: 1

sources:
  - name: test_source
    loader: custom
    schema: "{{ var(env_var('DBT_TEST_SCHEMA_NAME_VARIABLE')) }}"
    quoting:
      identifier: True
    tags:
      - my_test_source_tag
    tables:
      - name: test_table
        identifier: source
"""

seeds__source_csv = """favorite_color,id,first_name,email,ip_address,updated_at
blue,1,Larry,lking0@miitbeian.gov.cn,'69.135.206.194',2008-09-12 19:08:31
blue,2,Larry,lperkins1@toplist.cz,'64.210.133.162',1978-05-09 04:15:14
"""


class BasePythonModelTests:
    @pytest.fixture(scope="class", autouse=True)
    def setEnvVars(self):
        os.environ["DBT_TEST_SCHEMA_NAME_VARIABLE"] = "test_run_schema"

        yield

        del os.environ["DBT_TEST_SCHEMA_NAME_VARIABLE"]

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"source.csv": seeds__source_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": schema_yml,
            "my_sql_model.sql": basic_sql,
            "my_versioned_sql_model_v1.sql": basic_sql,
            "my_python_model.py": basic_python,
            "second_sql_model.sql": second_sql,
        }

    def test_singular_tests(self, project):
        # test command
        vars_dict = {
            "test_run_schema": project.test_schema,
        }

        run_dbt(["seed", "--vars", yaml.safe_dump(vars_dict)])
        results = run_dbt(["run", "--vars", yaml.safe_dump(vars_dict)])
        assert len(results) == 4


m_1 = """
{{config(materialized='table')}}
select 1 as id union all
select 2 as id union all
select 3 as id union all
select 4 as id union all
select 5 as id
"""

incremental_python = """
def model(dbt, session):
    dbt.config(materialized="incremental", unique_key='id')
    df = dbt.ref("m_1")
    if dbt.is_incremental:
        # incremental runs should only apply to part of the data
        df = df.filter(df.id > 5)
    return df
"""


class BasePythonIncrementalTests:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"+incremental_strategy": "merge"}}

    @pytest.fixture(scope="class")
    def models(self):
        return {"m_1.sql": m_1, "incremental.py": incremental_python}

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


class _BaseSkipRefFiltering:
    @pytest.fixture(scope="class")
    def model_that_refs_input_py(self) -> str:
        """
        This is the SQL that references the input_model which can be sampled
        """
        return _model_that_refs_input_py

    @pytest.fixture(scope="class")
    def input_model_sql(self) -> str:
        """
        This is the SQL that defines the input model to be sampled, including any {{ config(..) }}.
        event_time is a required configuration of this input
        """
        return _input_model_sql

    @pytest.fixture(scope="class")
    def models(self, model_that_refs_input_py: str, input_model_sql: str) -> Dict[str, str]:
        return {
            "input_model.sql": input_model_sql,
            "model_that_refs_input.py": model_that_refs_input_py,
        }

    def assert_row_count(self, project, relation_name: str, expected_row_count: int):
        relation = relation_from_name(project.adapter, relation_name)
        result = project.run_sql(f"select * from {relation}", fetch="all")

        assert len(result) == expected_row_count, f"{relation_name}:{pformat(result)}"


class BasePythonEmptyTests(_BaseSkipRefFiltering):
    def test_empty_mode(self, project) -> None:
        _ = run_dbt(["run"])
        self.assert_row_count(
            project=project,
            relation_name="model_that_refs_input",
            expected_row_count=3,
        )

        _ = run_dbt(["run", "--empty"])
        self.assert_row_count(
            project=project,
            relation_name="model_that_refs_input",
            expected_row_count=3,
        )


class BasePythonSampleTests(_BaseSkipRefFiltering):
    def test_sample_mode(self, project) -> None:
        _ = run_dbt(["run"])
        self.assert_row_count(
            project=project,
            relation_name="model_that_refs_input",
            expected_row_count=3,
        )

        _ = run_dbt(["run", "--sample={'start': '2025-01-03', 'end': '2025-01-04'}"])
        self.assert_row_count(
            project=project,
            relation_name="model_that_refs_input",
            expected_row_count=3,
        )
