import abc
from dataclasses import dataclass
from enum import Enum
from typing import Optional, Dict

from dbt.adapters.contracts.relation import RelationConfig
from dbt.adapters.relation_configs.formats import TableFormat


class CatalogIntegrationType(Enum):
    managed = 'managed'
    iceberg_rest = 'iceberg_rest'
    glue = 'glue'
    unity = 'unity'


@dataclass
class CatalogIntegrationConfig:
    catalog_name: str
    integration_name: str
    table_format: str
    catalog_type: str
    external_volume: Optional[str] = None
    namespace: Optional[str] = None
    adapter_properties: Optional[Dict] = None


class CatalogIntegration(abc.ABC):
    """
    An external catalog integration is a connection to an external catalog that can be used to
    interact with the catalog. This class is an abstract base class that should be subclassed by
    specific integrations in the adapters.

    Implements the CatalogIntegrationProtocol.

    """
    catalog_name: str
    integration_name: str
    table_format: TableFormat
    integration_type: CatalogIntegrationType
    external_volume: Optional[str] = None
    namespace: Optional[str] = None

    def __init__(
            self, integration_config: CatalogIntegrationConfig
    ):
        self.catalog_name = integration_config.catalog_name
        self.integration_name = integration_config.integration_name
        self.table_format = TableFormat(integration_config.table_format)
        self.type = CatalogIntegrationType(integration_config.catalog_type)
        self.external_volume = integration_config.external_volume
        self.namespace = integration_config.namespace
        self._handle_adapter_properties(integration_config.adapter_properties)

    def _handle_adapter_properties(self, adapter_properties: Dict) -> None:
        ...

    def render_ddl_predicates(self, relation, config: RelationConfig) -> str:
        ...
