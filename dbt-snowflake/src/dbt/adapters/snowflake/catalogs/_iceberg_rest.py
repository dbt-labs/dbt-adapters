from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from dbt.adapters.catalogs import CatalogIntegration, CatalogIntegrationConfig
from dbt.adapters.contracts.relation import RelationConfig

from dbt.adapters.snowflake.catalogs._parse_relation_config import (
    auto_refresh,
    catalog_namespace,
    catalog_table,
    external_volume,
    replace_invalid_characters,
)


@dataclass
class IcebergRESTCatalogRelation:
    """
    Represents a Snowflake Iceberg REST or AWS Glue catalog relation:
    https://docs.snowflake.com/en/sql-reference/sql/create-iceberg-table-rest
    https://docs.snowflake.com/en/sql-reference/sql/create-iceberg-table-aws-glue
    """

    catalog_table: str
    catalog_name: Optional[str] = None
    catalog_namespace: Optional[str] = None
    external_volume: Optional[str] = None
    replace_invalid_characters: bool = False
    auto_refresh: bool = False
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
            self.catalog_namespace = config.adapter_properties.get("catalog_namespace")
            self.replace_invalid_characters = config.adapter_properties.get(
                "replace_invalid_characters"
            )
            self.auto_refresh = config.adapter_properties.get("auto_refresh")
        else:
            self.catalog_namespace = None
            self.replace_invalid_characters = None
            self.auto_refresh = None

    def build_relation(self, model: RelationConfig) -> IcebergRESTCatalogRelation:

        # booleans need to be handled explicitly since False is "None-sey"
        _replace_invalid_characters = replace_invalid_characters(model)
        if _replace_invalid_characters is None:
            _replace_invalid_characters = self.replace_invalid_characters

        _auto_refresh = auto_refresh(model)
        if _auto_refresh is None:
            _auto_refresh = self.auto_refresh

        return IcebergRESTCatalogRelation(
            catalog_table=catalog_table(model),
            catalog_name=self.catalog_name,
            catalog_namespace=catalog_namespace(model) or self.catalog_namespace,
            external_volume=external_volume(model) or self.external_volume,
            replace_invalid_characters=_replace_invalid_characters,
            auto_refresh=_auto_refresh,
        )


class IcebergAWSGlueCatalogIntegration(IcebergRESTCatalogIntegration):
    catalog_type: str = "aws_glue"
