import abc
from typing import Self, ValuesView

from dbt_config.catalog_config import ExternalCatalog

from dbt.adapters.base import BaseRelation, BaseConnectionManager


class ExternalCatalogIntegration(abc.ABC):
    name: str
    external_catalog: ExternalCatalog
    _connection_manager: BaseConnectionManager
    _exists: bool

    @classmethod
    def create(cls, external_catalog: ExternalCatalog, connection_manager: BaseConnectionManager) -> Self:
        integration = ExternalCatalogIntegration()
        integration.external_catalog = external_catalog
        integration.name = external_catalog.name
        _connection_manager = connection_manager
        return integration

    @abc.abstractmethod
    def _exists(self) -> bool:
        pass

    def exists(self) -> bool:
        return self._exists
    @abc.abstractmethod
    def relation_exists(self, relation: BaseRelation) -> bool:
        pass

    @abc.abstractmethod
    def refresh_relation(self, table_name: str) -> None:
        pass

    @abc.abstractmethod
    def create_relation(self, table_name: str) -> None:
        pass


class ExternalCatalogIntegrations:
    def get(self, name: str) -> ExternalCatalogIntegration:
        return self.integrations[name]

    @property
    def integrations(self) -> dict[str, ExternalCatalogIntegration]:
        return self.integrations

    @classmethod
    def from_json_strings(cls, json_strings: ValuesView[str],
                          integration_class: ExternalCatalogIntegration,
                          connection_manager: BaseConnectionManager) -> Self:
        new_instance = cls()
        for json_string in json_strings:
            external_catalog = ExternalCatalog.model_validate_json(json_string)
            integration = integration_class.create(external_catalog, connection_manager)
            new_instance.integrations[integration.name] = integration
        return new_instance
