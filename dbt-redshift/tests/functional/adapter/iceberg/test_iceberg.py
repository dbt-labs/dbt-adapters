"""Functional tests for Apache Iceberg (AWS Glue Data Catalog) support on Redshift.

These require a live Redshift cluster with:
  - a pre-provisioned external schema mapped to an AWS Glue Data Catalog database
    (set REDSHIFT_TEST_ICEBERG_EXTERNAL_SCHEMA), and
  - an empty, writable S3 prefix in the cluster's region
    (set REDSHIFT_TEST_ICEBERG_LOCATION).
The dbt client must also have AWS credentials with S3 delete access to that prefix
(used to purge the location on re-create, since Redshift DROP leaves S3 data behind).

The tests are skipped unless both env vars are set.
"""

import os

import pytest

from dbt.tests.util import run_dbt, write_file

from tests.functional.adapter.iceberg import models


ICEBERG_EXTERNAL_SCHEMA = os.getenv("REDSHIFT_TEST_ICEBERG_EXTERNAL_SCHEMA")
ICEBERG_LOCATION = os.getenv("REDSHIFT_TEST_ICEBERG_LOCATION")

pytestmark = pytest.mark.skipif(
    not (ICEBERG_EXTERNAL_SCHEMA and ICEBERG_LOCATION),
    reason="Requires REDSHIFT_TEST_ICEBERG_EXTERNAL_SCHEMA and REDSHIFT_TEST_ICEBERG_LOCATION",
)


class IcebergSetup:
    @pytest.fixture(scope="class")
    def dbt_profile_target(self):
        # Iceberg DDL cannot run inside a multi-statement transaction, so the
        # connection must use autocommit.
        return {
            "type": "redshift",
            "host": os.getenv("REDSHIFT_TEST_HOST"),
            "port": int(os.getenv("REDSHIFT_TEST_PORT", "5439")),
            "dbname": os.getenv("REDSHIFT_TEST_DBNAME"),
            "user": os.getenv("REDSHIFT_TEST_USER"),
            "pass": os.getenv("REDSHIFT_TEST_PASS"),
            "region": os.getenv("REDSHIFT_TEST_REGION"),
            "threads": 1,
            "autocommit": True,
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {"generate_schema_name.sql": models.GENERATE_SCHEMA_NAME}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "vars": {
                "iceberg_external_schema": ICEBERG_EXTERNAL_SCHEMA,
                "iceberg_location": ICEBERG_LOCATION,
            },
            "flags": {"redshift_skip_autocommit_transaction_statements": True},
            # Redshift Iceberg CREATE TABLE requires case-insensitive identifiers.
            "models": {"+pre-hook": "set enable_case_sensitive_identifier to off"},
        }

    def _count(self, project, identifier):
        result = project.run_sql(
            f"select count(*) from {ICEBERG_EXTERNAL_SCHEMA}.{identifier}", fetch="one"
        )
        return result[0]


class TestIcebergTable(IcebergSetup):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "base_table.sql": models.BASE_TABLE,
            "iceberg_table.sql": models.ICEBERG_TABLE,
            "iceberg_table_partitioned.sql": models.ICEBERG_TABLE_PARTITIONED,
            "view_on_iceberg.sql": models.VIEW_ON_ICEBERG,
        }

    def test_build_and_idempotent_rerun(self, project):
        assert len(run_dbt(["run"])) == 4
        assert self._count(project, "iceberg_table") == 2
        assert self._count(project, "iceberg_table_partitioned") == 2

        # Re-run: Iceberg has no CREATE OR REPLACE, so each table is dropped, its S3
        # prefix purged, and re-created. Counts must stay stable (no errors, no dupes).
        assert len(run_dbt(["run"])) == 4
        assert self._count(project, "iceberg_table") == 2
        assert self._count(project, "iceberg_table_partitioned") == 2


class TestIcebergIncremental(IcebergSetup):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "base_table.sql": models.BASE_TABLE,
            "iceberg_incremental.sql": models.ICEBERG_INCREMENTAL,
        }

    def test_incremental_append(self, project):
        assert len(run_dbt(["run"])) == 2
        assert self._count(project, "iceberg_incremental") == 2

        # Re-run with no new source rows: is_incremental() must engage (external
        # tables are in the relation cache) so nothing is duplicated.
        run_dbt(["run", "--select", "iceberg_incremental"])
        assert self._count(project, "iceberg_incremental") == 2

        # Add a new source row -> only it should be appended.
        write_file(
            models.BASE_TABLE_WITH_NEW_ROW,
            project.project_root,
            "models",
            "base_table.sql",
        )
        run_dbt(["run"])
        assert self._count(project, "iceberg_incremental") == 3

        # Full refresh rebuilds in place (drop + purge S3 + CTAS).
        run_dbt(["run", "--select", "iceberg_incremental", "--full-refresh"])
        assert self._count(project, "iceberg_incremental") == 3
