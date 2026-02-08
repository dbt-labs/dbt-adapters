"""
Functional tests for Iceberg table support in dbt-redshift.

These tests require:
1. A Redshift cluster with access to AWS Glue Data Catalog
2. An external schema pointing to a Glue database
3. An S3 bucket for Iceberg table data
4. Appropriate IAM permissions for Glue and S3

Environment variables needed:
- REDSHIFT_ICEBERG_EXTERNAL_SCHEMA: Name of the external schema (e.g., "iceberg_schema")
- REDSHIFT_ICEBERG_S3_BUCKET: S3 bucket for table data (e.g., "s3://my-bucket/iceberg")

The external schema must be created in Redshift before running these tests:

    CREATE EXTERNAL SCHEMA iceberg_schema
    FROM DATA CATALOG
    DATABASE 'my_glue_database'
    IAM_ROLE 'arn:aws:iam::123456789012:role/RedshiftGlueRole'
    REGION 'us-east-1';
"""

import os
import pytest
from dbt.tests.adapter.catalog_integrations.test_catalog_integration import (
    BaseCatalogIntegrationValidation,
)
from dbt.tests.util import run_dbt


# Environment variables for test configuration
EXTERNAL_SCHEMA = os.getenv("REDSHIFT_ICEBERG_EXTERNAL_SCHEMA", "")
S3_BUCKET = os.getenv("REDSHIFT_ICEBERG_S3_BUCKET", "")

# Skip all tests if environment is not configured
pytestmark = pytest.mark.skipif(
    not (EXTERNAL_SCHEMA and S3_BUCKET),
    reason="Iceberg test environment not configured. Set REDSHIFT_ICEBERG_EXTERNAL_SCHEMA and REDSHIFT_ICEBERG_S3_BUCKET.",
)


MODEL__BASIC_ICEBERG_TABLE = """
{{ config(
    materialized='table',
    catalog='glue_catalog'
) }}
select 1 as id, 'test' as name
"""


MODEL__PARTITIONED_ICEBERG_TABLE = """
{{ config(
    materialized='table',
    catalog='glue_catalog',
    partition_by=['region']
) }}
select 1 as id, 'test' as name, 'us-east-1' as region
"""


MODEL__CUSTOM_STORAGE_URI_TABLE = f"""
{{{{ config(
    materialized='table',
    catalog='glue_catalog',
    storage_uri='{S3_BUCKET}/custom_path/my_table/'
) }}}}
select 1 as id, 'test' as name
"""


class TestIcebergBasicTable(BaseCatalogIntegrationValidation):
    """Test basic Iceberg table creation."""

    @pytest.fixture(scope="class")
    def catalogs(self):
        return {
            "catalogs": [
                {
                    "name": "glue_catalog",
                    "active_write_integration": "glue_integration",
                    "write_integrations": [
                        {
                            "name": "glue_integration",
                            "catalog_type": "glue",
                            "table_format": "iceberg",
                            "file_format": "parquet",
                            "external_volume": S3_BUCKET,
                            "adapter_properties": {
                                "external_schema": EXTERNAL_SCHEMA,
                            },
                        }
                    ],
                },
            ]
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "basic_iceberg_table.sql": MODEL__BASIC_ICEBERG_TABLE,
        }

    def test_basic_iceberg_table(self, project):
        """Test creating a basic Iceberg table."""
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestIcebergPartitionedTable(BaseCatalogIntegrationValidation):
    """Test partitioned Iceberg table creation."""

    @pytest.fixture(scope="class")
    def catalogs(self):
        return {
            "catalogs": [
                {
                    "name": "glue_catalog",
                    "active_write_integration": "glue_integration",
                    "write_integrations": [
                        {
                            "name": "glue_integration",
                            "catalog_type": "glue",
                            "table_format": "iceberg",
                            "file_format": "parquet",
                            "external_volume": S3_BUCKET,
                            "adapter_properties": {
                                "external_schema": EXTERNAL_SCHEMA,
                            },
                        }
                    ],
                },
            ]
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "partitioned_iceberg_table.sql": MODEL__PARTITIONED_ICEBERG_TABLE,
        }

    def test_partitioned_iceberg_table(self, project):
        """Test creating a partitioned Iceberg table."""
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestIcebergCustomStorageUri(BaseCatalogIntegrationValidation):
    """Test Iceberg table with custom storage URI."""

    @pytest.fixture(scope="class")
    def catalogs(self):
        return {
            "catalogs": [
                {
                    "name": "glue_catalog",
                    "active_write_integration": "glue_integration",
                    "write_integrations": [
                        {
                            "name": "glue_integration",
                            "catalog_type": "glue",
                            "table_format": "iceberg",
                            "file_format": "parquet",
                            "external_volume": S3_BUCKET,
                            "adapter_properties": {
                                "external_schema": EXTERNAL_SCHEMA,
                            },
                        }
                    ],
                },
            ]
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "custom_storage_table.sql": MODEL__CUSTOM_STORAGE_URI_TABLE,
        }

    def test_custom_storage_uri(self, project):
        """Test creating an Iceberg table with custom storage URI."""
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"
