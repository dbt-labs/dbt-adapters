import abc
from typing_extensions import Protocol


class CatalogIntegrationConfig(Protocol):
    name: str
    catalog_type: str


class CatalogIntegration(abc.ABC):
    """
    Create a CatalogIntegration given user config

    This class should be subclassed by specific integrations in an adapter.
    A catalog integration is a platform's way of interacting with an external catalog.

    name: the name of the catalog integration in the data platform, e.g. "my_favorite_iceberg_catalog"
    catalog_type: the type of the catalog integration in the data platform, e.g. "iceberg_rest"
        This should be constant for each type of catalog integration, and should not be set by the user.
        It should be validated, but that validation is left to the adapter maintainer for flexibility.
    table_format: the table format associated with this catalog integration, e.g. "iceberg"
        This should be constant for each type of catalog integration, and should not be set by the user.
    """

    catalog_type: str
    table_format: str

    def __init__(self, config: CatalogIntegrationConfig):
        self.name: str = config.name
