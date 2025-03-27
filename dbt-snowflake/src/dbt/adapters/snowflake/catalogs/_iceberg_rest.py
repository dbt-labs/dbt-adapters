from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from dbt.adapters.catalogs import CatalogIntegration, CatalogIntegrationConfig
from dbt.adapters.contracts.relation import RelationConfig


@dataclass
class IcebergRESTCatalogRelation:
    base_location: str
    catalog_name: str
    external_volume: Optional[str] = None
    table_format: str = "iceberg"


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
        table_format (str): the table format this catalog uses
        -   must be "iceberg"
        adapter_properties (Optional[Dict[str, Any]]): a dictionary containing additional
            adapter-specific properties for the catalog
        -   this is only parsed for `namespace`
    """

    name: str
    catalog_type: str
    external_volume: Optional[str]
    table_format: str = "iceberg"
    adapter_properties: Dict[str, Any] = field(default_factory=dict)


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

    catalog_type: str = "iceberg_rest"
    table_format: str = "iceberg"
    allows_writes: bool = False

    def __init__(self, config: CatalogIntegrationConfig) -> None:
        super().__init__(config)
        if config.adapter_properties:
            self.namespace = config.adapter_properties.get("namespace")

    def build_relation(self, config: RelationConfig) -> IcebergRESTCatalogRelation:
        return IcebergRESTCatalogRelation(
            base_location=self.__base_location(config),
            external_volume=config.config.extra.get("external_volume", self.external_volume),
            catalog_name=self.catalog_name,
        )

    @staticmethod
    def __base_location(config: RelationConfig) -> str:
        # If the base_location_root config is supplied, overwrite the default value ("_dbt/")
        prefix = config.config.extra.get("base_location_root", "_dbt")

        base_location = f"{prefix}/{config.schema}/{config.identifier}"

        if subpath := config.config.extra.get("base_location_subpath"):
            base_location += f"/{subpath}"

        return base_location


class IcebergAWSGlueCatalogIntegration(IcebergRESTCatalogIntegration):
    catalog_type: str = "aws_glue"
