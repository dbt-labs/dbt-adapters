import os

import pytest

from dbt.tests.adapter.python_model.test_python_model import (
    BasePythonModelTests,
    basic_sql,
    schema_yml,
    second_sql,
)

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


@pytest.mark.skipif(
    not os.getenv("DBT_TEST_ATHENA_SPARK_WORK_GROUP"),
    reason="DBT_TEST_ATHENA_SPARK_WORK_GROUP must point to a Spark 3.5 workgroup.",
)
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
