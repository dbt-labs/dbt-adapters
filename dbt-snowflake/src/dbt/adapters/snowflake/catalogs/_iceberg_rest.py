from dataclasses import dataclass
import textwrap
from typing import Any, Dict, Optional, Union, TYPE_CHECKING

from dbt.adapters.base import BaseRelation
from dbt.adapters.catalogs import CatalogIntegration, CatalogIntegrationConfig
from dbt.adapters.contracts.relation import RelationConfig
from dbt.adapters.relation_configs import RelationResults
from dbt_common.exceptions import DbtInternalError

from dbt.adapters.snowflake.utils import set_boolean

if TYPE_CHECKING:
    import agate


@dataclass
class IcebergRESTConfig(CatalogIntegrationConfig):
    name: str
    table_name: str
    catalog_type: str = "iceberg_rest"
    external_volume: Optional[str] = None
    namespace: Optional[str] = None
    replace_invalid_characters: Optional[Union[bool, str]] = None
    auto_refresh: Optional[Union[bool, str]] = None


class IcebergRESTCatalog(CatalogIntegration):
    """
    Implement Snowflake's Iceberg REST Catalog Integration:
    https://docs.snowflake.com/en/sql-reference/sql/create-iceberg-table-rest
    https://docs.snowflake.com/en/sql-reference/sql/create-catalog-integration-rest
    """

    catalog_type: str = "iceberg_rest"
    table_format: str = "iceberg"

    def __init__(self, config: CatalogIntegrationConfig):
        super().__init__(config)
        if config.catalog_type != "iceberg_rest":
            raise DbtInternalError(
                f"Attempting to create IcebergREST catalog integration for catalog {self.name} with catalog type {config.catalog_type}."
            )
        if isinstance(config, IcebergRESTConfig):
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
        """
        {{ optional('external_volume', dynamic_table.catalog.external_volume) }}
        {{ optional('catalog', dynamic_table.catalog.name) }}
        base_location = '{{ dynamic_table.catalog.base_location }}'
        :param config:
        :param relation:
        :return:
        """
        base_location: str = f"{config.get('base_location_root', '_dbt')}"
        base_location += f"/{relation.schema}/{relation.name}"

        if sub_path := config.get("base_location_subpath"):
            base_location += f"/{sub_path}"

        ddl_predicate = f"""
                external_volume = '{self.external_volume}'
                catalog = 'snowflake'
                base_location = '{base_location}'
                """
        if self.auto_refresh:
            ddl_predicate += f"auto_refresh = {self.auto_refresh}\n"
        if self.replace_invalid_characters:
            ddl_predicate += f"replace_invalid_characters = {self.replace_invalid_characters}\n"
        return textwrap.indent(textwrap.dedent(ddl_predicate), " " * 10)

    @classmethod
    def parse_relation_results(cls, relation_results: RelationResults) -> Dict[str, Any]:
        # this try block can be removed once enable_iceberg_materializations is retired
        try:
            catalog_results: "agate.Table" = relation_results["catalog"]
        except KeyError:
            # this happens when `enable_iceberg_materializations` is turned off
            return {}

        if len(catalog_results) == 0:
            # this happens when the dynamic table is a standard dynamic table (e.g. not iceberg)
            return {}

        # for now, if we get catalog results, it's because this is an iceberg table
        # this is because we only run `show iceberg tables` to get catalog metadata
        # this will need to be updated once this is in `show objects`
        catalog: "agate.Row" = catalog_results.rows[0]
        config_dict = {
            "table_format": "iceberg",
            "name": catalog.get("catalog_name"),
            "external_volume": catalog.get("external_volume_name"),
            "base_location": catalog.get("base_location"),
        }

        return config_dict
