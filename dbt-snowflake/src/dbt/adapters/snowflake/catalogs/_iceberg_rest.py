from dataclasses import dataclass, field
from typing import Any, Dict, Optional, TYPE_CHECKING
from typing_extensions import Self

from dbt.adapters.catalogs import CatalogIntegration, CatalogIntegrationConfig
from dbt.adapters.relation_configs import RelationResults
from dbt_common.exceptions import DbtInternalError

if TYPE_CHECKING:
    import agate


@dataclass
class IcebergRESTCatalogIntegrationConfig(CatalogIntegrationConfig):
    """
    Implements the CatalogIntegrationConfig protocol for integrating with an Iceberg REST or AWS Glue catalog

    This class extends CatalogIntegrationConfig to add default settings.

    Attributes:
        name (str): the name of the catalog integration
        catalog_type (str): the type of catalog integration
        -   must be "iceberg_rest" or "aws_glue"
        external_volume (Optional[str]): the external volume associated with the catalog integration
        -   if left empty, the default for the database/account will be used
        adapter_properties (Optional[Dict[str, Any]]): a dictionary containing additional
            adapter-specific properties for the catalog
        -   this is only parsed for `namespace`
    """

    name: str
    catalog_type: str
    external_volume: Optional[str]
    adapter_properties: Optional[Dict[str, Any]] = field(default_factory=dict)


class IcebergRESTCatalogIntegration(CatalogIntegration):
    """
    Implements Snowflake's Iceberg REST Catalog Integration:
    https://docs.snowflake.com/en/sql-reference/sql/create-catalog-integration-rest

    Implements Snowflake's AWS Glue Catalog Integration:
    https://docs.snowflake.com/en/sql-reference/sql/create-catalog-integration-glue

    While external volumes are a separate, but related concept in Snowflake,
    we assume that a catalog integration is always associated with an external volume.

    Attributes:
        name (str): the name of the catalog integration, e.g. "my_iceberg_rest_catalog"
        catalog_type (str): the type of catalog integration
        -   must be "iceberg_rest" or "aws_glue"
        external_volume (str): the external volume associated with the catalog integration
        -   if left empty, the default for the database/account will be used
        table_format (str): the table format this catalog uses
        -   must be "iceberg"
        allows_writes (bool): identifies whether this catalog integration supports writes
        -   must be False
    """

    table_format: str = "iceberg"
    allows_writes: bool = False

    def __init__(self, config: CatalogIntegrationConfig) -> None:
        super().__init__(config)
        if self.catalog_type not in ["iceberg_rest", "aws_glue"]:
            raise DbtInternalError(
                f"Attempting to create IcebergREST catalog integration for catalog {self.name} with catalog type {config.catalog_type}."
            )
        if self.table_format and self.table_format != "iceberg":
            raise DbtInternalError(
                f"Unsupported table format for catalog {self.name}: {self.table_format}. Expected `iceberg` or unset."
            )

        self.namespace = config.adapter_properties.get("namespace")

    @classmethod
    def from_relation_results(cls, relation_results: RelationResults) -> Self:
        table: "agate.Row" = relation_results["table"][0]
        catalog: "agate.Row" = relation_results["catalog"][0]

        adapter_properties = {}
        if namespace := catalog.get("namespace"):
            adapter_properties["namespace"] = namespace

        config = IcebergRESTCatalogIntegrationConfig(
            name=catalog.get("catalog_name"),
            catalog_type=catalog.get("catalog_type"),
            external_volume=table.get("external_volume_name"),
            adapter_properties=adapter_properties,
        )
        return cls(config)
