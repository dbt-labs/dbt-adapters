from enum import Enum
from typing import Any, Dict, Optional
from typing_extensions import Protocol


class CatalogIntegrationConfig(Protocol):
    """
    Represents the user configuration required to describe a catalog integration

    This class serves as a blueprint for catalog integration configurations,
    providing details about the catalog type, name, and other optional
    properties necessary for integration. It is designed to be used with
    any implementation that requires a catalog configuration protocol,
    ensuring a standardized structure and attributes are in place.

    Attributes:
        name (str): the name of the catalog integration in the data platform, e.g. "my_favorite_iceberg_catalog"
            - this is required for dbt to correctly reference catalogs by name from model configuration
            - expected to be unique within the adapter, if not the entire data platform
        catalog_type (str): the type of the catalog integration in the data platform, e.g. "iceberg_rest"
            - this is required for dbt to determine the correct method for parsing user configuration
            - usually a combination of the catalog and the way in which the data platform interacts with it
        table_format (Optional[str]): the table format this catalog uses
            - this is commonly unique to each catalog type, and should only be required from the user for catalogs that support multiple formats
        external_volume (Optional[str]): external storage volume identifier
            - while this is a separate concept from catalogs, we feel it is more user-friendly to group it with the catalog configuration
            - a result of this grouping is that there can only be one external volume per catalog integration, but many catalogs can share the same volume
        adapter_properties (Optional[Dict[str, Any]]):
            - additional, adapter-specific properties are nested here to avoid future collision when expanding the catalog integration protocol
    """

    name: str
    catalog_type: str
    table_format: Optional[str]
    external_volume: Optional[str]
    adapter_properties: Optional[Dict[str, Any]]


class CatalogIntegrationMode(Enum):
    READ = "r"
    WRITE = "w"


class CatalogIntegration:
    """
    Represent a catalog integration for a given user config

    This class should be subclassed by specific catalog integration types in an adapter.
    A catalog integration is a specific platform's way of interacting with a specific catalog.

    Attributes:
        name (str): the name of the catalog integration in the data platform, e.g. "my_favorite_iceberg_catalog"
            - this is required for dbt to correctly reference catalogs by name from model configuration
            - expected to be unique within the adapter, if not the entire data platform
        catalog_type (str): the type of the catalog integration in the data platform, e.g. "iceberg_rest"
            - this is required for dbt to determine the correct method for parsing user configuration
            - usually a combination of the catalog and the way in which the data platform interacts with it
        allows_writes (bool): identifies whether this catalog integration supports writes
            - this is required for dbt to correctly identify whether a catalog is writable during parse time
            - this is determined by the catalog integration type, hence it is a class attribute
        table_format (Optional[str]): the table format this catalog uses
            - this is commonly determined by the catalog integration type, hence it is usually a class attribute
            - it should only be required from the user for catalogs that support multiple formats
        external_volume (Optional[str]): external storage volume identifier
            - while this is a separate concept from catalogs, we feel it is more user-friendly to group it with the catalog configuration
            - a result of this grouping is that there can only be one external volume per catalog integration, but many catalogs can share the same volume
    """

    allows_writes: CatalogIntegrationMode = CatalogIntegrationMode.READ

    def __init__(self, config: CatalogIntegrationConfig):
        self.name: str = config.name
        self.catalog_type: str = config.catalog_type
        self.table_format: Optional[str] = config.table_format or None
        self.external_volume: Optional[str] = config.external_volume or None
