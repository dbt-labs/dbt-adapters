"""Functional tests for the S3 Tables catalog (``catalog_type: s3_tables``) on Athena.

These run against a real AWS S3 Tables catalog and validate the end-to-end flow
across materializations: create (location-free Iceberg DDL), the table landing in
S3 Tables, querying it, and idempotent re-runs (which drop via the Glue Data
Catalog rather than a SQL ``DROP TABLE``).

Prerequisites (provisioned once per account/region, NOT by these tests):
  * an S3 Tables table bucket named ``dbt-athena-s3tables-integration-testing``
  * the ``s3tablescatalog`` Glue federation (``aws glue create-catalog`` with the
    ``aws:s3tables`` connection over ``bucket/*``)
and the test role needs S3 Tables read/write (e.g. ``AmazonS3TablesFullAccess``).
The tests create/drop only their own namespace + tables on the fly. If the bucket
is absent or the role lacks S3 Tables access (e.g. a contributor fork), they skip.
"""

import os

import boto3
import pytest
from botocore.exceptions import ClientError, UnknownServiceError

from dbt.tests.util import run_dbt, run_dbt_and_capture, write_config_file

# Skip if installed dbt-core doesn't support use_catalogs_v2 yet.
try:
    from dbt.contracts.project import ProjectFlags as _PF

    _has_catalogs_v2 = hasattr(_PF, "use_catalogs_v2")
except ImportError:
    _has_catalogs_v2 = False

pytestmark = pytest.mark.skipif(
    not _has_catalogs_v2,
    reason="dbt-core does not support use_catalogs_v2 yet",
)

# Pre-provisioned table bucket (see module docstring), reused across runs; concurrent
# test classes stay isolated via unique namespaces (each uses project.test_schema).
S3_TABLES_BUCKET = "dbt-athena-s3tables-integration-testing"
# AWS's fixed federated-catalog name for S3 Tables.
GLUE_S3TABLES_CATALOG = "s3tablescatalog"


def _session() -> boto3.Session:
    return boto3.Session(
        profile_name=os.getenv("DBT_TEST_ATHENA_AWS_PROFILE_NAME") or None,
        region_name=os.getenv("DBT_TEST_ATHENA_REGION_NAME"),
    )


class BaseS3TablesCatalog:
    """Shared provisioning + config for S3 Tables functional tests.

    Subclasses provide ``models`` (and optionally ``snapshots``) plus a test method.
    """

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"use_catalogs_v2": True}}

    @pytest.fixture(scope="class", autouse=True)
    def s3_tables_infra(self):
        """Resolve the pre-provisioned table bucket; skip if it's absent/inaccessible.

        The bucket and the s3tablescatalog Glue federation are provisioned once per
        account/region (see module docstring), not here. We only confirm the bucket
        is reachable so a missing prerequisite or a permission-less role (forks) skips
        cleanly instead of failing deep in a dbt run.
        """
        session = _session()
        try:
            s3t = session.client("s3tables")
        except UnknownServiceError:
            pytest.skip("installed botocore has no s3tables client")

        region = session.region_name
        account = session.client("sts").get_caller_identity()["Account"]
        bucket_arn = f"arn:aws:s3tables:{region}:{account}:bucket/{S3_TABLES_BUCKET}"

        try:
            s3t.get_table_bucket(tableBucketARN=bucket_arn)
        except ClientError as e:
            code = e.response["Error"]["Code"]
            if code in ("AccessDeniedException", "AccessDenied"):
                pytest.skip("role lacks S3 Tables access (expected on forks)")
            if code in ("NotFoundException", "NotFound"):
                pytest.skip(
                    f"pre-provisioned S3 Tables bucket '{S3_TABLES_BUCKET}' not found; "
                    "provision it + the s3tablescatalog Glue federation once per account/region"
                )
            raise

        yield {
            "bucket_arn": bucket_arn,
            "database": f"{GLUE_S3TABLES_CATALOG}/{S3_TABLES_BUCKET}",
        }

    @pytest.fixture(autouse=True)
    def namespace(self, project, s3_tables_infra):
        """Create the S3 Tables namespace matching the model schema; clean it up after."""
        s3t = _session().client("s3tables")
        arn = s3_tables_infra["bucket_arn"]
        ns = project.test_schema
        try:
            s3t.create_namespace(tableBucketARN=arn, namespace=[ns])
        except ClientError as e:
            if e.response["Error"]["Code"] != "ConflictException":
                raise
        yield ns
        try:
            for t in s3t.list_tables(tableBucketARN=arn, namespace=ns).get("tables", []):
                s3t.delete_table(tableBucketARN=arn, namespace=ns, name=t["name"])
            s3t.delete_namespace(tableBucketARN=arn, namespace=ns)
        except ClientError:
            pass

    @pytest.fixture
    def catalogs(self, s3_tables_infra):
        return {
            "catalogs": [
                {
                    "name": "s3_tables_catalog",
                    "type": "s3_tables",
                    "table_format": "iceberg",
                    "config": {
                        "athena": {
                            "catalog_database": s3_tables_infra["database"],
                            "file_format": "parquet",
                        }
                    },
                }
            ]
        }

    def _table_type(self, s3_tables_infra, namespace, table):
        s3t = _session().client("s3tables")
        for t in s3t.list_tables(
            tableBucketARN=s3_tables_infra["bucket_arn"], namespace=namespace
        ).get("tables", []):
            if t["name"] == table:
                return t.get("type")
        return None


MODEL__TABLE = """
{{ config(materialized='table', catalog_name='s3_tables_catalog') }}
select 1 as id, 'alpha' as name
union all
select 2 as id, 'beta' as name
"""

MODEL__INCREMENTAL = """
{{ config(materialized='incremental', catalog_name='s3_tables_catalog',
          incremental_strategy='merge', unique_key='id') }}
select 1 as id, 'a' as val
{% if is_incremental() %}
union all select 2 as id, 'b' as val
{% endif %}
"""

SNAPSHOT__CHECK = """
{% snapshot s3t_snapshot %}
{{ config(target_schema=schema, catalog_name='s3_tables_catalog',
          unique_key='id', strategy='check', check_cols=['val']) }}
select 1 as id, '{{ var("snap_val", "a") }}' as val
{% endsnapshot %}
"""


class TestAthenaS3TablesTable(BaseS3TablesCatalog):
    @pytest.fixture(scope="class")
    def models(self):
        return {"s3t_model.sql": MODEL__TABLE}

    def test_create_query_and_rerun(self, project, catalogs, s3_tables_infra, namespace):
        write_config_file(catalogs, project.project_root, "catalogs.yml")
        database = s3_tables_infra["database"]

        # First run: routes to S3 Tables via catalog_database; location-free Iceberg DDL.
        _, stdout = run_dbt_and_capture(["--debug", "run"])
        assert "is_external=false" in stdout.replace(" ", "")
        assert "external_location=" not in stdout

        # Landed in S3 Tables (managed Iceberg tables report type "customer").
        assert self._table_type(s3_tables_infra, namespace, "s3t_model") == "customer"

        count = project.run_sql(
            f'select count(*) from "{database}"."{namespace}"."s3t_model"', fetch="one"
        )[0]
        assert count == 2

        # Idempotent re-run: existing table dropped via Glue (a SQL DROP would no-op
        # against awsdatacatalog and fail with TABLE_ALREADY_EXISTS).
        assert len(run_dbt(["run"])) == 1
        count = project.run_sql(
            f'select count(*) from "{database}"."{namespace}"."s3t_model"', fetch="one"
        )[0]
        assert count == 2


class TestAthenaS3TablesIncremental(BaseS3TablesCatalog):
    @pytest.fixture(scope="class")
    def models(self):
        return {"s3t_inc.sql": MODEL__INCREMENTAL}

    def test_incremental_merge(self, project, catalogs, s3_tables_infra, namespace):
        write_config_file(catalogs, project.project_root, "catalogs.yml")
        database = s3_tables_infra["database"]
        rel = f'"{database}"."{namespace}"."s3t_inc"'

        # First run: create (1 row). Exercises create_table_as for s3_tables.
        assert len(run_dbt(["run"])) == 1
        assert self._table_type(s3_tables_infra, namespace, "s3t_inc") == "customer"
        assert project.run_sql(f"select count(*) from {rel}", fetch="one")[0] == 1

        # Second run: is_incremental adds id=2 -> MERGE into the Iceberg table. This also
        # exercises the end-of-materialization step that must skip Glue version-expiry
        # for S3 Tables (else GetTableVersions raises EntityNotFoundException).
        assert len(run_dbt(["run"])) == 1
        assert project.run_sql(f"select count(*) from {rel}", fetch="one")[0] == 2


class TestAthenaS3TablesSnapshot(BaseS3TablesCatalog):
    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"s3t_snapshot.sql": SNAPSHOT__CHECK}

    @pytest.fixture(scope="class")
    def models(self):
        # No models; snapshot selects a literal so the test is self-contained.
        return {}

    def _snap_count(self, project, database, namespace):
        return project.run_sql(
            f'select count(*) from "{database}"."{namespace}"."s3t_snapshot"', fetch="one"
        )[0]

    def test_snapshot_check_strategy(self, project, catalogs, s3_tables_infra, namespace):
        write_config_file(catalogs, project.project_root, "catalogs.yml")
        database = s3_tables_infra["database"]

        # First snapshot: create the Iceberg snapshot table in S3 Tables.
        assert len(run_dbt(["snapshot"])) == 1
        assert self._table_type(s3_tables_infra, namespace, "s3t_snapshot") == "customer"
        assert self._snap_count(project, database, namespace) == 1

        # Flip the value -> check strategy records a new version via MERGE.
        assert len(run_dbt(["snapshot", "--vars", '{"snap_val": "b"}'])) == 1
        assert self._snap_count(project, database, namespace) == 2
