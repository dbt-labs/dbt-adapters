import os
import pytest
from dbt.tests.adapter.catalog_integrations.test_catalog_integration import (
    BaseCatalogIntegrationValidation,
)
from dbt.tests.util import run_dbt

MODEL__BASIC_ICEBERG_TABLE = """
                            {{ config(materialized='table',
                             catalog_name='basic_iceberg_rest_catalog') }}
                            select 1 as id
                            """

MODEL__ICEBERG_TABLE_WITH_CATALOG_CONFIG = """
                            {{ config(materialized='table', catalog_name='basic_iceberg_rest_catalog',
                            target_file_size='16MB', max_data_extension_time_in_days=1, auto_refresh='true') }}
                            select 1 as id
                            """


class TestSnowflakeIcebergRestCatalogIntegration(BaseCatalogIntegrationValidation):

    @pytest.fixture(scope="class")
    def catalogs(self):
        return {
            "catalogs": [
                {
                    "name": "basic_iceberg_rest_catalog",
                    "active_write_integration": "iceberg_rest_catalog_with_linked_db_integration",
                    "write_integrations": [
                        {
                            "name": "iceberg_rest_catalog_with_linked_db_integration",
                            "catalog_type": "iceberg_rest",
                            "table_format": "iceberg",
                            "adapter_properties": {
                                "catalog_linked_database": os.getenv(
                                    "SNOWFLAKE_TEST_CATALOG_LINKED_DATABASE"
                                ),
                                "max_data_extension_time_in_days": 1,
                                "target_file_size": "AUTO",
                                "auto_refresh": "true",
                            },
                        }
                    ],
                },
            ]
        }

    # can only use alphanumeric characters in schema names in catalog linked databases
    # until it's GA
    @pytest.fixture(scope="class")
    def unique_schema(self, request, prefix) -> str:
        test_file = request.module.__name__
        # We only want the last part of the name
        test_file = test_file.split(".")[-1]
        unique_schema = f"{prefix}_{test_file}"
        return unique_schema.replace("_", "")

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "models": {
                "basic_iceberg_table.sql": MODEL__BASIC_ICEBERG_TABLE,
                "iceberg_table_with_catalog_config.sql": MODEL__ICEBERG_TABLE_WITH_CATALOG_CONFIG,
            }
        }

    def test_basic_iceberg_rest_catalog_integration(self, project):
        result = run_dbt(["run"])
        assert len(result) == 2
        run_dbt(["run"])
