from dbt_common.exceptions import DbtValidationError

from dbt.adapters.contracts.catalog import CatalogIntegration


class CatalogIntegrations:
    def __init__(self):
        self._integrations = {}

    def get(self, name: str) -> CatalogIntegration:
        return self.integrations[name]

    @property
    def integrations(self) -> dict[str, CatalogIntegration]:
        return self._integrations

    def add_integration(self, integration: CatalogIntegration, catalog_name: str):
        self._integrations[catalog_name] = integration


_CATALOG_CLIENT = CatalogIntegrations()


def get_catalog(integration_name: str) -> CatalogIntegration:
    try:
        return _CATALOG_CLIENT.get(integration_name)
    except KeyError:
        raise DbtValidationError(
            f"Catalog integration '{integration_name}' not found in the catalog client"
        )


def add_catalog(integration: CatalogIntegration, catalog_name: str):
    _CATALOG_CLIENT.add_integration(integration, catalog_name)
