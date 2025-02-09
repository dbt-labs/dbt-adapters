from dbt.adapters.protocol import CatalogIntegrationProtocol
from dbt.adapters.exceptions import DbtCatalogIntegrationAlreadyExistsError

from typing import Optional

class CatalogIntegrations:
    def __init__(self):
        self._integrations = {}

    def get(self, name: str) -> Optional[CatalogIntegrationProtocol]:
        if name in self._integrations:
            return self._integrations[name]
        return None

    @property
    def integrations(self) -> dict[str, CatalogIntegrationProtocol]:
        return self._integrations

    def add_integration(self, integration: CatalogIntegrationProtocol, catalog_name: str):
        if catalog_name in self._integrations:
            raise DbtCatalogIntegrationAlreadyExistsError(catalog_name)
        self._integrations[catalog_name] = integration

_CATALOG_CLIENT = CatalogIntegrations()


def get_catalog(integration_name: str) -> Optional[CatalogIntegrationProtocol]:
    return _CATALOG_CLIENT.get(integration_name)

def add_catalog(integration: CatalogIntegrationProtocol, catalog_name: str):
    _CATALOG_CLIENT.add_integration(integration, catalog_name)
