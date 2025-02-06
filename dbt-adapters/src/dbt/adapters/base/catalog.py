import abc
from dataclasses import dataclass
from enum import Enum
from typing import Optional, Dict

from dbt.adapters.contracts.relation import RelationConfig
from dbt.adapters.relation_configs.formats import TableFormat
from dbt.adapters.protocol import CatalogIntegrationProtocol

class CatalogIntegrationType(Enum):
    managed = "managed"
    iceberg_rest = "iceberg_rest"
    glue = "glue"
    unity = "unity"


@dataclass
class CatalogIntegrationConfig:
    catalog_name: str
    integration_name: str
    table_format: str
    catalog_type: str
    external_volume: Optional[str] = None
    namespace: Optional[str] = None
    adapter_properties: Optional[Dict] = None


class CatalogIntegration(abc.ABC, CatalogIntegrationProtocol):
    """
    Implements the CatalogIntegrationProtocol as an abstract class should be subclassed by
    specific integrations in the adapters.

    A catalog integration is a platform's way of interacting with an external catalog. 
    """

    catalog_name: str
    integration_name: str
    table_format: TableFormat
    integration_type: CatalogIntegrationType
    external_volume: Optional[str] = None
    namespace: Optional[str] = None

    def __init__(self, integration_config: CatalogIntegrationConfig):
        self.catalog_name = integration_config.catalog_name
        self.integration_name = integration_config.integration_name
        self.table_format = TableFormat(integration_config.table_format)
        self.type = CatalogIntegrationType(integration_config.catalog_type)
        self.external_volume = integration_config.external_volume
        self.namespace = integration_config.namespace
        if integration_config.adapter_properties:
            self._handle_adapter_properties(integration_config.adapter_properties)

    @abc.abstractmethod
    def _handle_adapter_properties(self, adapter_properties: Dict) -> None: ...

    @abc.abstractmethod
    def render_ddl_predicates(self, relation, config: RelationConfig) -> str: ...
