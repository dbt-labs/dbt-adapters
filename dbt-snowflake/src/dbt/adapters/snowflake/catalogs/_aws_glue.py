from dataclasses import dataclass

from typing import Optional, Union

from dbt.adapters.base import BaseRelation
from dbt.adapters.catalogs import CatalogIntegration, CatalogIntegrationConfig
from dbt.adapters.contracts.relation import RelationConfig
from dbt_common.exceptions import DbtInternalError

from dbt.adapters.snowflake.utils import set_boolean


@dataclass
class IcebergGlueConfig(CatalogIntegrationConfig):
    name: str
    table_name: str
    catalog_type: str = "glue"
    external_volume: Optional[str] = None
    namespace: Optional[str] = None
    replace_invalid_characters: Optional[Union[bool, str]] = None
    auto_refresh: Optional[Union[bool, str]] = None


class AWSGlueCatalog(CatalogIntegration):
    """
    Implement Snowflake's AWS Glue Catalog Integration:
    https://docs.snowflake.com/en/sql-reference/sql/create-iceberg-table-aws-glue
    https://docs.snowflake.com/en/sql-reference/sql/create-catalog-integration-glue
    """

    catalog_type = "glue"
    table_format = "iceberg"

    def __init__(self, config: CatalogIntegrationConfig):
        super().__init__(config)
        if config.catalog_type != "iceberg_rest":
            raise DbtInternalError(
                f"Attempting to create AWS Glue catalog integration for catalog {self.name} with catalog type {config.catalog_type}."
            )
        if isinstance(config, IcebergGlueConfig):
            self.table_name = config.table_name
            self.external_volume = config.external_volume
            self.namespace = config.namespace
            self.auto_refresh = config.auto_refresh  # type:ignore
            self.replace_invalid_characters = config.replace_invalid_characters  # type:ignore

    @property
    def auto_refresh(self) -> bool:
        return self._auto_refresh

    @auto_refresh.setter
    def auto_refresh(self, value: Union[bool, str]) -> None:
        self._auto_refresh = set_boolean("auto_refresh", value, default=False)

    @property
    def replace_invalid_characters(self) -> bool:
        return self._replace_invalid_characters

    @replace_invalid_characters.setter
    def replace_invalid_characters(self, value: Union[bool, str]) -> None:
        self._replace_invalid_characters = set_boolean(
            "replace_invalid_characters", value, default=False
        )

    def render_ddl_predicates(self, relation: BaseRelation, config: RelationConfig) -> str:
        ddl_predicate = f"""
                   external_volume = '{self.external_volume}'
                   catalog = '{self.integration_name}'
                   """
        if self.namespace:
            ddl_predicate += f"CATALOG_NAMESPACE = '{self.namespace}'\n"
        if self.auto_refresh:
            ddl_predicate += f"auto_refresh = {self.auto_refresh}\n"
        if self.replace_invalid_characters:
            ddl_predicate += f"replace_invalid_characters = {self.replace_invalid_characters}\n"
        return ddl_predicate
