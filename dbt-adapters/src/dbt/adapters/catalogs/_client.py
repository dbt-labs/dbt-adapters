from typing import Dict, Type

from dbt.adapters.catalogs._exceptions import (
    DbtCatalogIntegrationAlreadyExistsError,
    DbtCatalogIntegrationNotFoundError,
    DbtCatalogIntegrationNotSupportedError,
)
from dbt.adapters.catalogs._integration import (
    CatalogIntegration,
    CatalogIntegrationConfig,
)


class CatalogIntegrationClient:
    """
    A repository class that manages catalog integrations

    This class manages all types of catalog integrations,
    supporting operations like registering new integrations and retrieving existing ones.
    There is only one instance of this class per adapter.

    Attributes:
        __supported_catalogs (Dict[str, Type[CatalogIntegration]]): a dictionary of supported
            catalog types mapped to their corresponding factory classes
        __catalog_integrations (Dict[str, CatalogIntegration]): a dictionary of catalog
            integration names mapped to their instances
    """

    def __init__(self, supported_catalogs: Dict[str, Type[CatalogIntegration]]):
        self.__supported_catalogs = supported_catalogs
        self.__catalog_integrations: Dict[str, CatalogIntegration] = {}

    def add(self, catalog_integration: CatalogIntegrationConfig) -> CatalogIntegration:
        try:
            catalog_factory = self.__supported_catalogs[catalog_integration.catalog_type]
        except KeyError:
            raise DbtCatalogIntegrationNotSupportedError(
                catalog_integration.catalog_type, self.__supported_catalogs.keys()
            )

        if catalog_integration.name in self.__catalog_integrations.keys():
            raise DbtCatalogIntegrationAlreadyExistsError(catalog_integration.name)

        self.__catalog_integrations[catalog_integration.name] = catalog_factory(
            catalog_integration
        )

        return self.get(catalog_integration.name)

    def get(self, name: str) -> CatalogIntegration:
        try:
            return self.__catalog_integrations[name]
        except KeyError:
            raise DbtCatalogIntegrationNotFoundError(name, self.__catalog_integrations.keys())
