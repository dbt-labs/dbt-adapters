from dbt.adapters.contracts.catalog import CatalogIntegration


class CatalogIntegrations:
    def get(self, name: str) -> CatalogIntegration:
        return self.integrations[name]

    @property
    def integrations(self) -> dict[str, CatalogIntegration]:
        return self.integrations

    def add_integration(self, integration: CatalogIntegration):
        self.integrations[integration.name] = integration


_CATALOG_CLIENT = CatalogIntegrations()


def get_catalog(integration_name: str) -> CatalogIntegration:
    return _CATALOG_CLIENT.get(integration_name)


def add_catalog(integration: CatalogIntegration):
    _CATALOG_CLIENT.add_integration(integration)
