import re
import pytest
from dbt.tests.adapter.catalog_integrations.test_catalog_integration import (
    BaseCatalogIntegrationValidation,
)
from dbt.tests.util import run_dbt


def get_cleaned_model_ddl_from_file(file_name: str) -> str:
    with open(f"target/run/test/models/{file_name}", "r") as ddl_file:
        return re.sub(r"\s+", " ", ddl_file.read())


MODEL__BASIC_ICEBERG_TABLE = """
                            {{ config(materialized='table', catalog_name='basic_iceberg_catalog') }}
                            select 1 as id
                            """

MODEL__ICEBERG_TABLE_W_CONFIGS = """
                            {{ config(materialized='table',
                                catalog_name='basic_iceberg_catalog',
                                data_retention_time_in_days=1,
                                change_tracking=False,
                                max_data_extension_time_in_days=30,
                                storage_serialization_policy='COMPATIBLE')
                                }}
                            select 1 as id
                            """


class TestSnowflakeBuiltInCatalogIntegration(BaseCatalogIntegrationValidation):

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
                            "catalog_type": "BUILT_IN",
                            "table_format": "iceberg",
                            "external_volume": f"s3_iceberg_snow",
                            "adapter_properties": {
                                "storage_serialization_policy": "OPTIMIZED",
                                "max_data_extension_time_in_days": 60,
                                "data_retention_time_in_days": 0,
                                "change_tracking": True,
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
            "iceberg_table_with_configs.sql": MODEL__ICEBERG_TABLE_W_CONFIGS,
        }

    def test_basic_iceberg_catalog_integration(self, project):
        run_dbt(["run"])
        iceberg_sql = get_cleaned_model_ddl_from_file("basic_iceberg_table.sql")
        assert "storage_serialization_policy = 'OPTIMIZED'" in iceberg_sql
        assert "max_data_extension_time_in_days = 60" in iceberg_sql
        assert "change_tracking = TRUE" in iceberg_sql
        assert "data_retention_time_in_days = 0" in iceberg_sql
        iceberg_table_with_configs_sql = get_cleaned_model_ddl_from_file(
            "iceberg_table_with_configs.sql"
        )
        assert "storage_serialization_policy = 'COMPATIBLE'" in iceberg_table_with_configs_sql
        assert "max_data_extension_time_in_days = 30" in iceberg_table_with_configs_sql
        assert "change_tracking = FALSE" in iceberg_table_with_configs_sql
        assert "data_retention_time_in_days = 1" in iceberg_table_with_configs_sql
