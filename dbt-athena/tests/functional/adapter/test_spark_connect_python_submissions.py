import os

import pytest

from dbt.tests.adapter.python_model.test_python_model import (
    BasePythonIncrementalTests,
    BasePythonModelTests,
    basic_sql,
    schema_yml,
    second_sql,
)
from dbt.tests.util import run_dbt

# spark_engine_version="3.5" routes execution through Spark Connect.
# DBT_TEST_ATHENA_SPARK_WORK_GROUP must point to a workgroup whose
# engine version is set to Apache Spark 3.5.
spark_connect_python = """
def model(dbt, _):
    dbt.config(
        materialized='table',
        spark_engine_version='3.5',
    )
    df = dbt.ref("my_sql_model")
    df2 = dbt.ref("my_versioned_sql_model", v=1)
    df3 = dbt.ref("my_versioned_sql_model", version=1)
    df4 = dbt.ref("test", "my_versioned_sql_model", v=1)
    df5 = dbt.ref("test", "my_versioned_sql_model", version=1)
    df6 = dbt.source("test_source", "test_table")
    df = df.limit(2)
    return df
"""


requires_spark_workgroup = pytest.mark.skipif(
    not os.getenv("DBT_TEST_ATHENA_SPARK_WORK_GROUP"),
    reason="DBT_TEST_ATHENA_SPARK_WORK_GROUP must point to a Spark 3.5 workgroup.",
)


@requires_spark_workgroup
class TestSparkConnectPythonModel(BasePythonModelTests):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        # Athena defaults SQL models to views, but Spark's HiveExternalCatalog
        # cannot resolve Athena views (no S3 location), so dbt.ref() blows up
        # with "Can not create a Path from an empty string".  Force tables.
        return {"models": {"+materialized": "table"}}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": schema_yml,
            "my_sql_model.sql": basic_sql,
            "my_versioned_sql_model_v1.sql": basic_sql,
            "my_python_model.py": spark_connect_python,
            "second_sql_model.sql": second_sql,
        }


_iceberg_seed_sql = """
{{ config(materialized='table', table_type='iceberg') }}
select id from (values 1, 2, 3, 4, 5) as t(id)
"""

_incremental_python = """
def model(dbt, session):
    dbt.config(
        materialized='incremental',
        unique_key='id',
        spark_engine_version='3.5',
        table_type='iceberg',
        incremental_strategy='merge',
    )
    df = dbt.ref("m_1")
    if dbt.is_incremental:
        df = df.filter(df.id > 5)
    return df
"""


@requires_spark_workgroup
class TestSparkConnectPythonIncremental(BasePythonIncrementalTests):
    """Spark Connect must round-trip dbt.is_incremental and the merge
    strategy (Iceberg required, since Hive external tables don't support
    MERGE on Athena)."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"m_1.sql": _iceberg_seed_sql, "incremental.py": _incremental_python}


_parallel_a_python = """
def model(dbt, _):
    dbt.config(materialized='table', spark_engine_version='3.5')
    df = dbt.ref("my_sql_model")
    return df.limit(1)
"""

_parallel_b_python = """
def model(dbt, _):
    dbt.config(materialized='table', spark_engine_version='3.5')
    df = dbt.ref("my_sql_model")
    return df.limit(2)
"""


@requires_spark_workgroup
class TestSparkConnectPythonMultiModel:
    """End-to-end smoke test: two python models with matching fingerprints
    both succeed via the Spark Connect path (session reuse semantics are
    covered by the unit tests in ``tests/unit/spark_connect/test_session.py``)."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"+materialized": "table"}}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_sql_model.sql": basic_sql,
            "py_a.py": _parallel_a_python,
            "py_b.py": _parallel_b_python,
        }

    def test_two_python_models_run(self, project):
        results = run_dbt(["run"])
        assert len(results) == 3
        assert all(r.status == "success" for r in results)
