from typing import Dict, Type

from dbt.adapters.catalogs._exceptions import (
    DbtCatalogIntegrationAlreadyExistsError,
    DbtCatalogIntegrationNotFoundError,
    DbtCatalogNotSupportedError,
)
from dbt.adapters.catalogs._integration import (
    CatalogIntegration,
    CatalogIntegrationConfig,
)


class CatalogIntegrationClient:
    def __init__(self, supported_catalogs: Dict[str, Type[CatalogIntegration]]):
        self._supported_catalogs = supported_catalogs
        self.catalogs: Dict[str, CatalogIntegration] = {}

    def add(self, catalog: CatalogIntegrationConfig) -> CatalogIntegration:
        if catalog.catalog_type not in self._supported_catalogs:
            raise DbtCatalogNotSupportedError(
                catalog.catalog_type, list(self._supported_catalogs.keys())
            )
        if catalog.name in self.catalogs:
            raise DbtCatalogIntegrationAlreadyExistsError(catalog.name)

        catalog_factory = self._supported_catalogs[catalog.catalog_type]
        self.catalogs[catalog.name] = catalog_factory(catalog)

        return self.get(catalog.name)

    def get(self, name: str) -> CatalogIntegration:
        try:
            return self.catalogs[name]
        except KeyError:
            raise DbtCatalogIntegrationNotFoundError(name, list(self.catalogs.keys()))
