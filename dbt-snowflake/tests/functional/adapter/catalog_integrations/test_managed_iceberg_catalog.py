import pytest
from dbt.tests.util import run_dbt, write_config_file
from dbt.tests.adapter.catalog_integrations.test_catalog_integration import BaseCatalogIntegration


class TestManagedIcebergCatalogIntegration(BaseCatalogIntegration):

    @pytest.fixture(scope="class", autouse=True)
    def write_catalog_integration(self, project):
        return {
            "name": "write_integration_name",
            "external_volume": "s3_iceberg_snow",
            "table_format": "iceberg",
            "catalog_type": "managed",
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"enable_iceberg_materializations": True}}
