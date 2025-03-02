import abc
from typing import Dict, Optional, Protocol

from dbt.adapters.contracts.relation import RelationConfig


class CatalogIntegrationConfig(Protocol):
    name: str
    type: str
    table_format: str
    external_volume: Optional[str] = None
    namespace: Optional[str] = None
    adapter_properties: Optional[dict] = None


class CatalogIntegration(abc.ABC):
    """
    Create a CatalogIntegration given user config

    This class should be subclassed by specific integrations in an adapter.
    A catalog integration is a platform's way of interacting with an external catalog.
    """

    name: str
    type: str
    table_format: str
    external_volume: Optional[str] = None
    namespace: Optional[str] = None

    def __init__(self, config: CatalogIntegrationConfig) -> None:
        self.name = config.name
        self.type = config.type
        self.table_format = config.table_format
        self.external_volume = config.external_volume
        self.namespace = config.namespace
        if config.adapter_properties:
            self._handle_adapter_properties(config.adapter_properties)

    @abc.abstractmethod
    def _handle_adapter_properties(self, adapter_properties: Dict) -> None:
        pass

    @abc.abstractmethod
    def render_ddl_predicates(self, relation, config: RelationConfig) -> str:
        raise NotImplementedError("render_ddl_predicates not implemented")
