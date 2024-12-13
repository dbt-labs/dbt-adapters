import abc
from enum import Enum
from typing import Optional, Tuple, List, Dict

from dbt.adapters.protocol import CatalogIntegrationConfig
from dbt.adapters.relation_configs.formats import TableFormat


class CatalogIntegrationType(Enum):
    iceberg_rest = 'iceberg_rest'
    glue = 'glue'
    unity = 'unity'


class CatalogIntegration(abc.ABC):
    """
    An external catalog integration is a connection to an external catalog that can be used to
    interact with the catalog. This class is an abstract base class that should be subclassed by
    specific integrations in the adapters.

    """
    integration_name: str
    table_format: TableFormat
    integration_type: CatalogIntegrationType
    external_volume: Optional[str] = None
    namespace: Optional[str] = None

    def __init__(
        self, integration_config: CatalogIntegrationConfig
    ):
        self.name = integration_config.name
        self.table_format = TableFormat(integration_config.table_format)
        self.type = CatalogIntegrationType(integration_config.type)
        self.external_volume = integration_config.external_volume
        self.namespace = integration_config.namespace
        self._handle_adapter_configs(integration_config.adapter_configs)

    def _handle_adapter_configs(self, adapter_configs: Dict) -> None:
        ...

