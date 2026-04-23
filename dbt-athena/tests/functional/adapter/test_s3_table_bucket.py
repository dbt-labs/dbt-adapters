"""
Integration tests for S3 Table Bucket support.

Requires a real S3 Table Bucket and a named Athena data catalog pointing to it.
Set DBT_TEST_ATHENA_S3TB_DATABASE to the named catalog (e.g. 'dbt_athena_s3tb_test').
Skipped if the env var is not set.
"""

import os

import pytest

from dbt.tests.util import run_dbt

S3TB_DATABASE = os.getenv("DBT_TEST_ATHENA_S3TB_DATABASE")

pytestmark = pytest.mark.skipif(
    not S3TB_DATABASE,
    reason="DBT_TEST_ATHENA_S3TB_DATABASE not set — skipping S3 Table Bucket integration tests",
)


TABLE_MODEL = """
{{ config(
    materialized='table',
    format='parquet'
) }}
select 1 as id, 'hello' as name
"""

INCREMENTAL_MERGE_MODEL = """
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='id',
    format='parquet'
) }}
select 1 as id, 'hello' as name
{% if is_incremental() %}
  union all
  select 2 as id, 'world' as name
{% endif %}
"""

INCREMENTAL_APPEND_MODEL = """
{{ config(
    materialized='incremental',
    incremental_strategy='append',
    format='parquet'
) }}
select 1 as id, 'row' as name
"""

VIEW_MODEL = """
{{ config(materialized='view') }}
select 1 as id
"""

SEED_CSV = """id,name
1,alice
2,bob
"""

SNAPSHOT_SQL = """
{% snapshot s3tb_snapshot %}
{{ config(
    target_schema=var('schema'),
    strategy='check',
    unique_key='id',
    check_cols=['name'],
    table_type='iceberg',
) }}
select 1 as id, 'snap' as name
{% endsnapshot %}
"""


@pytest.fixture(scope="class")
def dbt_profile_target():
    """Override profile to point at the S3TB catalog."""
    return {
        "type": "athena",
        "s3_staging_dir": os.getenv("DBT_TEST_ATHENA_S3_STAGING_DIR"),
        "s3_tmp_table_dir": os.getenv("DBT_TEST_ATHENA_S3_TMP_TABLE_DIR"),
        "region_name": os.getenv("DBT_TEST_ATHENA_REGION_NAME"),
        "database": S3TB_DATABASE,
        "schema": os.getenv("DBT_TEST_ATHENA_SCHEMA", "dbt_s3tb_test"),
        "work_group": os.getenv("DBT_TEST_ATHENA_WORK_GROUP"),
        "threads": int(os.getenv("DBT_TEST_ATHENA_THREADS", "1")),
        "poll_interval": float(os.getenv("DBT_TEST_ATHENA_POLL_INTERVAL", "1.0")),
        "num_retries": int(os.getenv("DBT_TEST_ATHENA_NUM_RETRIES", "2")),
        "aws_profile_name": os.getenv("DBT_TEST_ATHENA_AWS_PROFILE_NAME") or None,
    }


class TestS3TableBucketTable:
    @pytest.fixture(scope="class")
    def models(self):
        return {"s3tb_table.sql": TABLE_MODEL}

    def test_table_first_run(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

    def test_table_rerun(self, project):
        """Second run does drop-and-recreate."""
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

        # Re-run
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestS3TableBucketIncrementalMerge:
    @pytest.fixture(scope="class")
    def models(self):
        return {"s3tb_merge.sql": INCREMENTAL_MERGE_MODEL}

    def test_incremental_merge(self, project):
        # First run — full build
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

        # Second run — incremental merge
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

    def test_incremental_full_refresh(self, project):
        """--full-refresh does drop-and-recreate."""
        run_dbt(["run"])
        results = run_dbt(["run", "--full-refresh"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestS3TableBucketIncrementalAppend:
    @pytest.fixture(scope="class")
    def models(self):
        return {"s3tb_append.sql": INCREMENTAL_APPEND_MODEL}

    def test_incremental_append(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

        # Incremental append
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestS3TableBucketViewError:
    @pytest.fixture(scope="class")
    def models(self):
        return {"s3tb_view.sql": VIEW_MODEL}

    def test_view_raises_error(self, project):
        results = run_dbt(["run"], expect_pass=False)
        assert len(results) == 1
        assert "CREATE VIEW is not supported on S3 Table Bucket" in results[0].message


class TestS3TableBucketSeedError:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"s3tb_seed.csv": SEED_CSV}

    def test_seed_raises_error(self, project):
        results = run_dbt(["seed"], expect_pass=False)
        assert any(
            "Seeds are not supported for S3 Table Bucket" in str(r.message) for r in results
        )


class TestS3TableBucketSnapshot:
    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"s3tb_snapshot.sql": SNAPSHOT_SQL}

    @pytest.fixture(scope="class")
    def models(self):
        return {}

    def test_snapshot(self, project):
        # Initial snapshot
        results = run_dbt(["snapshot", "--vars", f"schema: {project.test_schema}"])
        assert len(results) == 1
        assert results[0].status == "success"

        # Subsequent snapshot
        results = run_dbt(["snapshot", "--vars", f"schema: {project.test_schema}"])
        assert len(results) == 1
        assert results[0].status == "success"
