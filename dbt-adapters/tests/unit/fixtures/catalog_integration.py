from typing import Dict

import pytest

from dbt.adapters.contracts.catalog import CatalogIntegration, CatalogIntegrationConfig, CatalogIntegrationType
from dbt.adapters.relation_configs.formats import TableFormat


class FakeCatalogIntegration(CatalogIntegration):
    fake_property: int

    def _handle_adapter_properties(self, adapter_properties: Dict) -> None:
        if 'fake_property' in adapter_properties:
            self.fake_property = adapter_properties['fake_property']

    def render_ddl_predicates(self, relation):
        return "mocked"


catalog = FakeCatalogIntegration(
    integration_config=CatalogIntegrationConfig(
        catalog_type=CatalogIntegrationType.managed.value,
        catalog_name="snowflake_managed",
        integration_name="test_integration",
        table_format=TableFormat.ICEBERG,
        external_volume="test_volume",
    )
)


@pytest.fixture
def fake_catalog_integration():
    return catalog
