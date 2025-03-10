from dataclasses import dataclass
from typing import Optional, Union, TYPE_CHECKING
from typing_extensions import Self

from dbt.adapters.catalogs import CatalogIntegration, CatalogIntegrationConfig
from dbt.adapters.contracts.relation import RelationConfig
from dbt.adapters.relation_configs import RelationResults
from dbt_common.exceptions import DbtInternalError, DbtValidationError

from dbt.adapters.snowflake.utils import set_boolean

if TYPE_CHECKING:
    import agate


@dataclass
class IcebergRESTCatalogIntegrationConfig(CatalogIntegrationConfig):
    name: str
    catalog_type: str = "iceberg_rest"
    external_volume: Optional[str] = None
    namespace: Optional[str] = None


class IcebergRESTTable:
    """
    Model a table from Snowflake's Iceberg REST Catalog Integration:
    https://docs.snowflake.com/en/sql-reference/sql/create-iceberg-table-rest

    Model a table from Snowflake's AWS Glue Catalog Integration:
    https://docs.snowflake.com/en/sql-reference/sql/create-iceberg-table-aws-glue
    """

    def __init__(self, relation_config: RelationConfig) -> None:
        catalog = relation_config.config.extra.get("catalog")
        self.catalog_table_name: str = catalog.get("catalog_table_name")
        self.replace_invalid_characters: bool = catalog.get("replace_invalid_characters")
        self.auto_refresh: bool = catalog.get("auto_refresh")

    @property
    def catalog_table_name(self) -> str:
        return self.__catalog_table_name

    @catalog_table_name.setter
    def catalog_table_name(self, value: Optional[str]) -> None:
        if value and len(value) > 0:
            self.__catalog_table_name = value
        raise DbtValidationError("table_name is required for IcebergREST catalogs")

    @property
    def replace_invalid_characters(self) -> bool:
        return self.__replace_invalid_characters

    @replace_invalid_characters.setter
    def replace_invalid_characters(self, value: Union[bool, str]) -> None:
        self.__replace_invalid_characters = set_boolean(
            "replace_invalid_characters", value, default=False
        )

    @property
    def auto_refresh(self) -> bool:
        return self.__auto_refresh

    @auto_refresh.setter
    def auto_refresh(self, value: Union[bool, str]) -> None:
        self.__auto_refresh = set_boolean("auto_refresh", value, default=False)


class IcebergRESTCatalogIntegration(CatalogIntegration):
    """
    Implements Snowflake's Iceberg REST Catalog Integration:
    https://docs.snowflake.com/en/sql-reference/sql/create-catalog-integration-rest

    Implements Snowflake's AWS Glue Catalog Integration:
    https://docs.snowflake.com/en/sql-reference/sql/create-catalog-integration-glue

    While external volumes are a separate, but related concept in Snowflake,
    we assume that a catalog integration is always associated with an external volume.
    """

    table_format: str = "iceberg"

    def __init__(self, config: CatalogIntegrationConfig) -> None:
        super().__init__(config)
        if config.catalog_type in ["iceberg_rest", "aws_glue"]:
            self.catalog_type = config.catalog_type
        else:
            raise DbtInternalError(
                f"Attempting to create IcebergREST catalog integration for catalog {self.name} with catalog type {config.catalog_type}."
            )
        if isinstance(config, IcebergRESTCatalogIntegrationConfig):
            self.namespace: Optional[str] = config.namespace
            self.external_volume: Optional[str] = config.external_volume

    def table(self, relation_config: RelationConfig) -> IcebergRESTTable:
        return IcebergRESTTable(relation_config)

    @classmethod
    def from_relation_results(cls, relation_results: RelationResults) -> Self:
        table: "agate.Row" = relation_results["table"][0]
        catalog: "agate.Row" = relation_results["catalog"][0]
        config = IcebergRESTCatalogIntegrationConfig(
            name=catalog.get("catalog_name"),
            catalog_type=catalog.get("catalog_type"),
            external_volume=table.get("external_volume_name"),
            namespace=catalog.get("namespace"),
        )
        return cls(config)
