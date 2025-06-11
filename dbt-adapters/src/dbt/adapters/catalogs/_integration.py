import abc
from typing import Any, Dict, Optional
from typing_extensions import Protocol

from dbt.adapters.contracts.relation import RelationConfig


class CatalogIntegrationConfig(Protocol):
    """
    Represents the user configuration required to describe a catalog integration

    This class serves as a blueprint for catalog integration configurations,
    providing details about the catalog type, name, and other optional
    properties necessary for integration. It is designed to be used with
    any implementation that requires a catalog configuration protocol,
    ensuring a standardized structure and attributes are in place.

    Attributes:
        name (str): the name of the catalog integration in the dbt project, e.g. "my_iceberg_operational_data"
            - a unique name for this catalog integration to be referenced in a model configuration
        catalog_type (str): the type of the catalog integration in the data platform, e.g. "iceberg_rest"
            - this is required for dbt to determine the correct method for parsing user configuration
            - usually a combination of the catalog and the way in which the data platform interacts with it
        catalog_name (Optional[str]): the name of the catalog integration in the data platform, e.g. "my_favorite_iceberg_catalog"
            - this is required for dbt to correctly reference catalogs by name from model configuration
            - expected to be unique within the data platform, but many dbt catalog integrations can share the same catalog name
        table_format (Optional[str]): the table format this catalog uses
            - this is commonly unique to each catalog type, and should only be required from the user for catalogs that support multiple formats
        external_volume (Optional[str]): external storage volume identifier
            - while this is a separate concept from catalogs, we feel it is more user-friendly to group it with the catalog configuration
            - it's possible to use a default external volume at the user, database, or account level, hence this is optional
            - a result of this grouping is that there can only be one external volume per catalog integration, but many catalogs can share the same volume
            - a user should create a new dbt catalog if they want to use a different external volume for a given catalog integration
        adapter_properties (Optional[Dict[str, Any]]):
            - additional, adapter-specific properties are nested here to avoid future collision when expanding the catalog integration protocol
    """

    name: str
    catalog_type: str
    catalog_name: Optional[str]
    table_format: Optional[str]
    external_volume: Optional[str]
    file_format: Optional[str]
    adapter_properties: Dict[str, Any]


class CatalogRelation(Protocol):
    catalog_name: Optional[str]
    table_format: Optional[str]
    external_volume: Optional[str]
    file_format: Optional[str]


class CatalogIntegration(abc.ABC):
    """
    Represent a catalog integration for a given user config

    This class should be implemented by specific catalog integration types in an adapter.
    A catalog integration is a specific platform's way of interacting with a specific catalog.

    Attributes:
        name (str): the name of the catalog integration in the dbt project, e.g. "my_iceberg_operational_data"
            - a unique name for this catalog integration to be referenced in a model configuration
        catalog_type (str): the type of the catalog integration in the data platform, e.g. "iceberg_rest"
            - this is a name for this particular implementation of the catalog integration, hence it is a class attribute
        catalog_name (Optional[str]): the name of the catalog integration in the data platform, e.g. "my_favorite_iceberg_catalog"
            - this is required for dbt to correctly reference catalogs by name from model configuration
            - expected to be unique within the data platform, but many dbt catalog integrations can share the same catalog name
        table_format (Optional[str]): the table format this catalog uses
            - this is commonly unique to each catalog type, and should only be required from the user for catalogs that support multiple formats
        external_volume (Optional[str]): external storage volume identifier
            - while this is a separate concept from catalogs, we feel it is more user-friendly to group it with the catalog configuration
            - it's possible to use a default external volume at the user, database, or account level, hence this is optional
            - a result of this grouping is that there can only be one external volume per catalog integration, but many catalogs can share the same volume
            - a user should create a new dbt catalog if they want to use a different external volume for a given catalog integration
        allows_writes (bool): identifies whether this catalog integration supports writes
            - this is required for dbt to correctly identify whether a catalog is writable during parse time
            - this is determined by the catalog integration type, hence it is a class attribute
    """

    catalog_type: str
    table_format: Optional[str] = None
    file_format: Optional[str] = None
    allows_writes: bool = False

    def __init__(self, config: CatalogIntegrationConfig) -> None:
        # table_format is often fixed for a catalog type, allow it to be defined at the class level
        if config.table_format is not None:
            self.table_format = config.table_format
        self.name: str = config.name
        self.catalog_name: Optional[str] = config.catalog_name
        self.external_volume: Optional[str] = config.external_volume
        self.file_format: Optional[str] = config.file_format

    def build_relation(self, config: RelationConfig) -> CatalogRelation:
        """
        Builds relation configuration within the context of this catalog integration.

        This method is a placeholder and must be implemented in subclasses to provide
        custom logic for building a relation.

        Args:
            config: User-provided model configuration.

        Returns:
            A `CatalogRelation` object constructed based on the input configuration.

        Raises:
            NotImplementedError: Raised when this method is not implemented in a subclass.
        """
        raise NotImplementedError(
            f"`{self.__class__.__name__}.build_relation` must be implemented to use this feature"
        )
