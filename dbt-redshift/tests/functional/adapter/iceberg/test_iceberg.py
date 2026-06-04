"""Functional tests for Apache Iceberg (AWS Glue Data Catalog) support on Redshift.

These require a live Redshift cluster with a pre-provisioned external schema mapped
to an AWS Glue Data Catalog database, plus an empty S3 prefix the cluster can write
to. They are skipped unless both env vars are set:

    REDSHIFT_TEST_ICEBERG_EXTERNAL_SCHEMA
    REDSHIFT_TEST_ICEBERG_LOCATION
"""

import os

import pytest

from dbt.tests.util import run_dbt, check_relations_equal

from tests.functional.adapter.iceberg import models


ICEBERG_EXTERNAL_SCHEMA = os.getenv("REDSHIFT_TEST_ICEBERG_EXTERNAL_SCHEMA")
ICEBERG_LOCATION = os.getenv("REDSHIFT_TEST_ICEBERG_LOCATION")

pytestmark = pytest.mark.skipif(
    not (ICEBERG_EXTERNAL_SCHEMA and ICEBERG_LOCATION),
    reason="Requires REDSHIFT_TEST_ICEBERG_EXTERNAL_SCHEMA and REDSHIFT_TEST_ICEBERG_LOCATION",
)


# Route only catalog-backed (Iceberg) models to the external Glue schema; everything
# else keeps the default test-schema behavior.
_GENERATE_SCHEMA_NAME = """
{% macro generate_schema_name(custom_schema_name, node) -%}
  {%- if node.config.get('catalog_name') == 'glue' -%}
    {{ var('iceberg_external_schema') }}
  {%- else -%}
    {{ default__generate_schema_name(custom_schema_name, node) }}
  {%- endif -%}
{%- endmacro %}
"""


class IcebergSetup:
    @pytest.fixture(scope="class")
    def macros(self):
        return {"generate_schema_name.sql": _GENERATE_SCHEMA_NAME}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "vars": {
                "iceberg_external_schema": ICEBERG_EXTERNAL_SCHEMA,
                "iceberg_location": ICEBERG_LOCATION,
            }
        }


class TestIcebergTable(IcebergSetup):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "base_table.sql": models.SEED_BASE,
            "iceberg_table.sql": models.ICEBERG_TABLE,
            "iceberg_table_partitioned.sql": models.ICEBERG_TABLE_PARTITIONED,
            "view_on_iceberg.sql": models.VIEW_ON_ICEBERG,
        }

    def test_build_and_idempotent_rerun(self, project):
        results = run_dbt(["run"])
        assert len(results) == 4

        # Iceberg has no CREATE OR REPLACE; a rerun must drop-then-create cleanly.
        results = run_dbt(["run"])
        assert len(results) == 4

        check_relations_equal(project.adapter, ["base_table", "iceberg_table"])


class TestIcebergIncremental(IcebergSetup):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "base_table.sql": models.SEED_BASE,
            "iceberg_incremental.sql": models.ICEBERG_INCREMENTAL,
        }

    def test_incremental_append_and_full_refresh(self, project):
        results = run_dbt(["run"])
        assert len(results) == 2

        # incremental run (append strategy) against the Iceberg target
        results = run_dbt(["run", "--select", "iceberg_incremental"])
        assert len(results) == 1

        # full-refresh must rebuild via drop + CTAS (no rename)
        results = run_dbt(["run", "--select", "iceberg_incremental", "--full-refresh"])
        assert len(results) == 1
