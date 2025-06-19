import os
from datetime import datetime as dt
import pytest
from dbt.tests.adapter.catalog_integrations.test_catalog_integration import (
    BaseCatalogIntegrationValidation,
)
from dbt.tests.util import run_dbt

MODEL__BASIC_ICEBERG_TABLE = """
                            {{ config(materialized='table', catalog_name='basic_iceberg_rest_catalog') }}
                            select 1 as id
                            """


class TestSnowflakeIcebergRestCatalogIntegration(BaseCatalogIntegrationValidation):

    @pytest.fixture(scope="class")
    def catalogs(self):
        return {
            "catalogs": [
                {
                    "name": "basic_iceberg_rest_catalog",
                    "active_write_integration": "basic_iceberg_rest_catalog_integration",
                    "write_integrations": [
                        {
                            "name": "basic_iceberg_rest_catalog_integration",
                            "catalog_type": "iceberg_rest",
                            "catalog_name": "POLARIS",
                            "table_format": "iceberg",
                            "external_volume": os.getenv("SNOWFLAKE_TEST_ICEBERG_REST_VOLUME", "s3_iceberg_rest"),
                            "adapter_properties": {
                                "rest_endpoint": os.getenv("SNOWFLAKE_TEST_ICEBERG_REST_ENDPOINT", "https://polaris.endpoint")
                            }
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
            }
        }

    @pytest.mark.skipif(
        not os.getenv("SNOWFLAKE_REST_TESTS"),
        reason="Iceberg REST tests require SNOWFLAKE_REST_TESTS=1 environment variable"
    )
    def test_basic_iceberg_rest_catalog_integration(self, project):
        run_dbt(["run"]) 