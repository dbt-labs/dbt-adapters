import os
from datetime import datetime as dt
import pytest
from dbt.tests.adapter.catalog_integrations.test_catalog_integration import (
    BaseCatalogIntegrationValidation,
)
from dbt.tests.util import run_dbt

_BQ_BUCKET = f"gs://{os.getenv('BIGQUERY_TEST_ICEBERG_BUCKET')}"
_STATIC_URI = f"{_BQ_BUCKET}/{str(dt.now())}"

MODEL__BASIC_ICEBERG_TABLE = """
                            {{ config(materialized='table', catalog='basic_iceberg_catalog') }}
                            select 1 as id
                            """

MODEL__SPECIFY_LOCATION_TABLE = """
                            {{ config(materialized='table', catalog='basic_iceberg_catalog',
                            base_location_root='custom_location') }}
                            select 1 as id
                            """

MODEL__SPECIFY_URI_TABLE = (
    """
                            {{ config(materialized='table', catalog='basic_iceberg_catalog',
                            storage_uri='"""
    + _STATIC_URI
    + """') }}
                            select 1 as id
                            """
)


class TestGenericCatalogIntegration(BaseCatalogIntegrationValidation):

    @pytest.fixture(scope="class")
    def catalogs(self):
        return {
            "catalogs": [
                {
                    "name": "basic_iceberg_catalog",
                    "active_write_integration": "basic_iceberg_catalog_integration",
                    "write_integrations": [
                        {
                            "name": "basic_iceberg_catalog_integration",
                            "catalog_type": "biglake_metastore",
                            "file_format": "parquet",
                            "table_format": "iceberg",
                            "external_volume": _BQ_BUCKET,
                        }
                    ],
                },
            ]
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "models": {
                "basic_iceberg_table.sql": MODEL__BASIC_ICEBERG_TABLE,
                "specify_location_table.sql": MODEL__SPECIFY_LOCATION_TABLE,
                "specify_uri_table.sql": MODEL__SPECIFY_URI_TABLE,
            }
        }

    def test_basic_iceberg_catalog_integration(self, project):
        run_dbt(["run"])
