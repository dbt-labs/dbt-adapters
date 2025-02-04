from dbt.adapters.protocol import CatalogIntegrationProtocol


class CatalogIntegrations:
    def __init__(self):
        self._integrations = {}

    def get(self, name: str) -> CatalogIntegrationProtocol:
        return self.integrations[name]

    @property
    def integrations(self) -> dict[str, CatalogIntegrationProtocol]:
        return self._integrations

    def add_integration(self, integration: CatalogIntegrationProtocol, catalog_name: str):
        self._integrations[catalog_name] = integration


_CATALOG_CLIENT = CatalogIntegrations()


def get_catalog(integration_name: str) -> CatalogIntegrationProtocol:
    return _CATALOG_CLIENT.get(integration_name)


def add_catalog(integration: CatalogIntegrationProtocol, catalog_name: str):
    _CATALOG_CLIENT.add_integration(integration, catalog_name)
