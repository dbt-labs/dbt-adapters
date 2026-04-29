"""Functional tests for the use_iceberg_write_to config.

The new branch in athena__py_save_table_as routes Iceberg Python models
through DataFrameWriterV2 (writeTo().createOrReplace()) instead of the
spark_ctas SQL path, enabling Iceberg-native partition transforms
(day/bucket/...) and atomic replacement without the __ha intermediate
table.

Gated on DBT_TEST_ATHENA_SPARK_WORK_GROUP — same workgroup the Spark
Connect tests use, since both paths require an Athena Spark workgroup
with Iceberg support.
"""

import os

import pytest

from dbt.tests.util import run_dbt

requires_spark_workgroup = pytest.mark.skipif(
    not os.getenv("DBT_TEST_ATHENA_SPARK_WORK_GROUP"),
    reason="DBT_TEST_ATHENA_SPARK_WORK_GROUP must point to an Athena Spark workgroup.",
)


_iceberg_writeto_partitioned = """
def model(dbt, spark_session):
    dbt.config(
        materialized='table',
        table_type='iceberg',
        spark_engine_version='3.5',
        use_iceberg_write_to=True,
        partitioned_by=['day(created_at)', 'bucket(user_id, 4)'],
    )
    from pyspark.sql import functions as F
    rows = [
        (1, '2026-01-01 00:00:00', 'a'),
        (2, '2026-01-02 00:00:00', 'b'),
        (3, '2026-01-03 00:00:00', 'c'),
    ]
    df = spark_session.createDataFrame(rows, ['user_id', 'created_at', 'name'])
    return df.withColumn('created_at', F.to_timestamp('created_at'))
"""


@requires_spark_workgroup
class TestUseIcebergWriteToPartitioned:
    """writeTo() with day/bucket partition transforms must produce a working
    Iceberg table. The spark_ctas path can't express these transforms, so
    this case is the primary motivation for the feature."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"iceberg_writeto_partitioned.py": _iceberg_writeto_partitioned}

    def test_creates_partitioned_iceberg_table(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

    def test_idempotent_replace(self, project):
        # createOrReplace must succeed against an existing target without
        # falling through the legacy __ha rename flow.
        run_dbt(["run"])
        results = run_dbt(["run"])
        assert all(r.status == "success" for r in results)


_iceberg_writeto_unpartitioned = """
def model(dbt, spark_session):
    dbt.config(
        materialized='table',
        table_type='iceberg',
        spark_engine_version='3.5',
        use_iceberg_write_to=True,
    )
    return spark_session.createDataFrame([(1,), (2,), (3,)], ['id'])
"""


@requires_spark_workgroup
class TestUseIcebergWriteToUnpartitioned:
    """use_iceberg_write_to without partitioned_by should still succeed
    (the partitionedBy() call is conditional on partitioned_by being set)."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"iceberg_writeto_unpartitioned.py": _iceberg_writeto_unpartitioned}

    def test_creates_unpartitioned_iceberg_table(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


_iceberg_writeto_with_table_properties = """
def model(dbt, spark_session):
    dbt.config(
        materialized='table',
        table_type='iceberg',
        spark_engine_version='3.5',
        use_iceberg_write_to=True,
        table_properties={'format-version': '2'},
    )
    return spark_session.createDataFrame([(1,), (2,)], ['id'])
"""


@requires_spark_workgroup
class TestUseIcebergWriteToTableProperties:
    """table_properties must be forwarded as DataFrameWriterV2 .tableProperty()
    calls and survive JSON escaping."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"iceberg_writeto_props.py": _iceberg_writeto_with_table_properties}

    def test_writeto_with_table_properties(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


_iceberg_writeto_on_hive_table = """
def model(dbt, spark_session):
    dbt.config(
        materialized='table',
        table_type='hive',
        spark_engine_version='3.5',
        use_iceberg_write_to=True,
    )
    return spark_session.createDataFrame([(1,)], ['id'])
"""


@requires_spark_workgroup
class TestUseIcebergWriteToRequiresIceberg:
    """use_iceberg_write_to with a non-iceberg table_type must surface a
    compiler error rather than silently writing the wrong format."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"iceberg_writeto_invalid.py": _iceberg_writeto_on_hive_table}

    def test_compiles_to_error(self, project):
        results = run_dbt(["run"], expect_pass=False)
        assert any("use_iceberg_write_to" in (r.message or "") for r in results)
