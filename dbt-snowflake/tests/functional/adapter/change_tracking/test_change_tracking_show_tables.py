import os

import pytest

from dbt.tests.adapter.catalog_integrations.test_catalog_integration import (
    BaseCatalogIntegrationValidation,
)
from dbt.tests.util import run_dbt

from tests.functional.utils import query_change_tracking_from_show_tables

MODEL_NATIVE_CT_ON = """
{{ config(materialized='table', transient=false, change_tracking=true) }}
-- Native tables default to transient; change tracking requires a non-transient table.
select 1 as id
"""

MODEL_NATIVE_CT_OFF = """
{{ config(materialized='table', transient=false, change_tracking=false) }}
select 1 as id
"""

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


class TestNativeTableChangeTrackingShowTables:
    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        return {
            "native_ct_on.sql": MODEL_NATIVE_CT_ON,
            "native_ct_off.sql": MODEL_NATIVE_CT_OFF,
        }

    @pytest.fixture(scope="class", autouse=True)
    def setup(self, project):
        run_dbt(["run"])

    def test_change_tracking_on_native_table(self, project):
        assert query_change_tracking_from_show_tables(project, "native_ct_on") == "ON"

    def test_change_tracking_off_native_table(self, project):
        assert query_change_tracking_from_show_tables(project, "native_ct_off") == "OFF"


class TestIcebergTableChangeTrackingShowTables(BaseCatalogIntegrationValidation):
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
                            "external_volume": os.environ.get(
                                "SNOWFLAKE_TEST_ICEBERG_EXTERNAL_VOLUME",
                                "s3_iceberg_snow",
                            ),
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

    @pytest.fixture(scope="class", autouse=True)
    def setup(self, project, write_catalogs_config_file):
        run_dbt(["run"])

    def test_change_tracking_on_iceberg_from_catalog_default(self, project):
        assert query_change_tracking_from_show_tables(project, "basic_iceberg_table") == "ON"

    def test_change_tracking_off_iceberg_from_model_config(self, project):
        assert (
            query_change_tracking_from_show_tables(project, "iceberg_table_with_configs")
            == "OFF"
        )
