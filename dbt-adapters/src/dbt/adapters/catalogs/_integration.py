import abc
from dataclasses import dataclass
from typing import Any, Dict, Optional

from dbt.adapters.contracts.relation import RelationConfig


@dataclass
class CatalogIntegrationConfig:
    name: str
    catalog_type: str
    table_format: str
    external_volume: Optional[str] = None
    adapter_properties: Optional[dict] = None


class CatalogIntegration(abc.ABC):
    """
    Create a CatalogIntegration given user config

    This class should be subclassed by specific integrations in an adapter.
    A catalog integration is a platform's way of interacting with an external catalog.
    """

    name: str
    catalog_type: str
    table_format: str
    external_volume: Optional[str] = None

    def __init__(self, config: CatalogIntegrationConfig) -> None:
        self.name = config.name
        self.catalog_type = config.catalog_type
        self.table_format = config.table_format
        self.external_volume = config.external_volume
        if config.adapter_properties:
            self._handle_adapter_properties(config.adapter_properties)

    @abc.abstractmethod
    def _handle_adapter_properties(self, adapter_properties: Dict[str, Any]) -> None:
        pass

    @abc.abstractmethod
    def render_ddl_predicates(self, relation, config: RelationConfig) -> str:
        raise NotImplementedError("render_ddl_predicates not implemented")
