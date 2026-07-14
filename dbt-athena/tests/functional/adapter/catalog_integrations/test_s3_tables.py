"""Functional tests for the S3 Tables catalog (``catalog_type: s3_tables``) on Athena.

These provision a real AWS S3 Tables bucket + Glue federation and validate the
end-to-end flow: create (location-free Iceberg DDL), the table actually landing
in S3 Tables, querying it, and an idempotent re-run (which drops via the Glue
Data Catalog rather than a SQL ``DROP TABLE``).

Requires ``use_catalogs_v2`` support and AWS permissions for the S3 Tables API
and ``glue:CreateCatalog`` (to register the ``s3tablescatalog`` federation).
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

# Stable per-account/region table bucket reused across runs; concurrent test classes
# stay isolated via unique namespaces (each uses project.test_schema).
S3_TABLES_BUCKET = "dbt-athena-s3tables-integration-testing"
GLUE_S3TABLES_CATALOG = "s3tablescatalog"

MODEL__S3_TABLES = """
{{ config(materialized='table', catalog_name='s3_tables_catalog') }}
select 1 as id, 'alpha' as name
union all
select 2 as id, 'beta' as name
"""


def _session() -> boto3.Session:
    return boto3.Session(
        profile_name=os.getenv("DBT_TEST_ATHENA_AWS_PROFILE_NAME") or None,
        region_name=os.getenv("DBT_TEST_ATHENA_REGION_NAME"),
    )


class TestAthenaS3TablesCatalog:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"use_catalogs_v2": True}}

    @pytest.fixture(scope="class")
    def models(self):
        return {"s3t_model.sql": MODEL__S3_TABLES}

    @pytest.fixture(scope="class", autouse=True)
    def s3_tables_infra(self):
        """Ensure a table bucket + the s3tablescatalog Glue federation exist."""
        session = _session()
        try:
            s3t = session.client("s3tables")
        except UnknownServiceError:
            pytest.skip("installed botocore has no s3tables client")

        region = session.region_name
        account = session.client("sts").get_caller_identity()["Account"]
        bucket_arn = f"arn:aws:s3tables:{region}:{account}:bucket/{S3_TABLES_BUCKET}"

        try:
            s3t.create_table_bucket(name=S3_TABLES_BUCKET)
        except ClientError as e:
            if e.response["Error"]["Code"] != "ConflictException":
                raise

        glue = session.client("glue")
        try:
            glue.get_catalog(CatalogId=GLUE_S3TABLES_CATALOG)
        except ClientError:
            all_buckets = f"arn:aws:s3tables:{region}:{account}:bucket/*"
            allow_all = [
                {
                    "Principal": {"DataLakePrincipalIdentifier": "IAM_ALLOWED_PRINCIPALS"},
                    "Permissions": ["ALL"],
                }
            ]
            glue.create_catalog(
                Name=GLUE_S3TABLES_CATALOG,
                CatalogInput={
                    "FederatedCatalog": {
                        "Identifier": all_buckets,
                        "ConnectionName": "aws:s3tables",
                    },
                    "CreateDatabaseDefaultPermissions": allow_all,
                    "CreateTableDefaultPermissions": allow_all,
                    "AllowFullTableExternalDataAccess": "True",
                },
            )

        yield {
            "bucket_arn": bucket_arn,
            "database": f"{GLUE_S3TABLES_CATALOG}/{S3_TABLES_BUCKET}",
        }

    @pytest.fixture(autouse=True)
    def namespace(self, project, s3_tables_infra):
        """Create the S3 Tables namespace matching the model's schema; clean it up after."""
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

    def _table_names(self, s3_tables_infra, namespace):
        s3t = _session().client("s3tables")
        tables = s3t.list_tables(
            tableBucketARN=s3_tables_infra["bucket_arn"], namespace=namespace
        ).get("tables", [])
        return {t["name"]: t.get("type") for t in tables}

    def test_s3_tables_create_query_and_rerun(self, project, catalogs, s3_tables_infra, namespace):
        write_config_file(catalogs, project.project_root, "catalogs.yml")
        database = s3_tables_infra["database"]

        # First run: the model routes to the S3 Tables catalog via catalog_database,
        # and the DDL is location-free Iceberg (is_external=false, no LOCATION).
        _, stdout = run_dbt_and_capture(["--debug", "run"])
        assert "is_external=false" in stdout.replace(" ", "")
        assert "external_location=" not in stdout

        # The table really landed in S3 Tables (managed Iceberg tables report type "customer").
        tables = self._table_names(s3_tables_infra, namespace)
        assert tables.get("s3t_model") == "customer"

        # It's queryable via the federated catalog.
        count = project.run_sql(
            f'select count(*) from "{database}"."{namespace}"."s3t_model"', fetch="one"
        )[0]
        assert count == 2

        # Idempotent re-run: the existing table is dropped via the Glue Data Catalog
        # (a SQL DROP would no-op against awsdatacatalog and fail with TABLE_ALREADY_EXISTS).
        assert len(run_dbt(["run"])) == 1
        count = project.run_sql(
            f'select count(*) from "{database}"."{namespace}"."s3t_model"', fetch="one"
        )[0]
        assert count == 2
