from dbt.adapters.clients.catalogs import add_catalog, get_catalog
from dbt.adapters.contracts.catalog import CatalogIntegration, CatalogIntegrationConfig, CatalogIntegrationType
from dbt.adapters.relation_configs.formats import TableFormat


class FakeCatalogIntegration(CatalogIntegration):
    def render_ddl_predicates(self, relation):
        return "mocked"


def test_adding_catalog_integration():
    catalog = FakeCatalogIntegration(
        integration_config=CatalogIntegrationConfig(
            catalog_type=CatalogIntegrationType.glue.value,
            catalog_name="snowflake_managed",
            integration_name="test_integration",
            table_format=TableFormat.ICEBERG,
            external_volume="test_volume",
        )
    )
    add_catalog(catalog, catalog_name="fake_catalog")
    get_catalog("fake_catalog")
