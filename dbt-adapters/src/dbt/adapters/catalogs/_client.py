from typing import Dict, Iterable, Type

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

    def __init__(self, supported_catalogs: Iterable[Type[CatalogIntegration]]):
        self.__supported_catalogs: Dict[str, Type[CatalogIntegration]] = {
            catalog.catalog_type.casefold(): catalog for catalog in supported_catalogs
        }
        self.__catalog_integrations: Dict[str, CatalogIntegration] = {}

    def add(self, config: CatalogIntegrationConfig) -> CatalogIntegration:
        factory = self.__catalog_integration_factory(config.catalog_type)
        if config.name in self.__catalog_integrations:
            raise DbtCatalogIntegrationAlreadyExistsError(config.name)
        self.__catalog_integrations[config.name] = factory(config)
        return self.get(config.name)

    def get(self, name: str) -> CatalogIntegration:
        try:
            return self.__catalog_integrations[name]
        except KeyError:
            raise DbtCatalogIntegrationNotFoundError(name, self.__catalog_integrations.keys())

    def __catalog_integration_factory(self, catalog_type: str) -> Type[CatalogIntegration]:
        try:
            return self.__supported_catalogs[catalog_type.casefold()]
        except KeyError as e:
            raise DbtCatalogIntegrationNotSupportedError(
                catalog_type, self.__supported_catalogs.keys()
            ) from e
