import re
import pytest
from dbt.tests.adapter.catalog_integrations.test_catalog_integration import (
    BaseCatalogIntegrationValidation,
)
from dbt.tests.util import run_dbt


def get_cleaned_model_ddl_from_file(file_name: str) -> str:
    with open(f"target/run/test/models/{file_name}", "r") as ddl_file:
        return re.sub(r"\s+", " ", ddl_file.read())


def get_cleaned_compiled_ddl_from_file(file_name: str) -> str:
    with open(f"target/compiled/test/models/{file_name}", "r") as ddl_file:
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
        # external_volume present → base_location must be emitted
        assert "base_location" in iceberg_sql
        iceberg_table_with_configs_sql = get_cleaned_model_ddl_from_file(
            "iceberg_table_with_configs.sql"
        )
        assert "storage_serialization_policy = 'COMPATIBLE'" in iceberg_table_with_configs_sql
        assert "max_data_extension_time_in_days = 30" in iceberg_table_with_configs_sql
        # change_tracking=false is ignored for Iceberg (Snowflake forbids turning it off)
        assert "change_tracking" not in iceberg_table_with_configs_sql
        assert "data_retention_time_in_days = 1" in iceberg_table_with_configs_sql


MODEL__MANAGED_STORAGE_ICEBERG_TABLE = """
    {{ config(materialized='table', table_format='iceberg') }}
    select 1 as id
    """

SEED = """
id,value
1,100
2,200
""".strip()

MODEL__MANAGED_STORAGE_DYNAMIC_ICEBERG_TABLE = """
    {{ config(
        materialized='dynamic_table',
        snowflake_warehouse='DBT_TESTING',
        target_lag='2 minutes',
        table_format='iceberg',
    ) }}
    select * from {{ ref('seed') }}
    """


class TestSnowflakeManagedStorageIcebergDDL:
    """
    Verifies that base_location is omitted when no external_volume is configured
    (Snowflake-managed storage / Horizon). A successful run proves the DDL was
    accepted by Snowflake — it rejects base_location for managed storage.
    """

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"seed.csv": SEED}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "managed_iceberg.sql": MODEL__MANAGED_STORAGE_ICEBERG_TABLE,
            "managed_dynamic_iceberg.sql": MODEL__MANAGED_STORAGE_DYNAMIC_ICEBERG_TABLE,
        }

    def test_managed_storage_omits_base_location(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])

        table_sql = get_cleaned_model_ddl_from_file("managed_iceberg.sql")
        assert "base_location" not in table_sql
        assert "catalog = 'SNOWFLAKE'" in table_sql

        dynamic_sql = get_cleaned_model_ddl_from_file("managed_dynamic_iceberg.sql")
        assert "base_location" not in dynamic_sql
        assert "catalog = 'SNOWFLAKE'" in dynamic_sql
