import pytest
from dbt.tests.util import run_dbt

BASIC_ICEBERG_TABLE_MODEL = """
{{
  config(
    materialized = "table",
    catalog_name = "test_catalog",
  )
}}
select * from 1 as id
"""

class TestManagedIcebergCatalogIntegration:
    @pytest.fixture
    def catalogs(self):
        return {
            "catalogs": [
                {
                    "name": "test_catalog",
                    "write_integrations": [
                        {
                            "name": "write_integration_name",
                            "external_volume": "s3_iceberg_snow",
                            "table_format": "iceberg",
                            "catalog_type": "managed",
                        }
                    ],
                }
            ]
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"enable_iceberg_materializations": True}}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "basic_table.sql": BASIC_ICEBERG_TABLE_MODEL,
        }
    def test_managed_iceberg_catalog_integration(self, project):
        run_dbt(["run"])
