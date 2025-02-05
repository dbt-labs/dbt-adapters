from typing import Dict, Optional, Any

import textwrap

from dbt.adapters.base import BaseRelation
from dbt.adapters.base.catalog import CatalogIntegration, CatalogIntegrationType
from dbt.adapters.contracts.relation import RelationConfig
from dbt.adapters.relation_configs import RelationResults
from dbt.exceptions import DbtValidationError
_AUTO_REFRESH_VALUES = ["TRUE", "FALSE"]
_REPLACE_INVALID_CHARACTERS_VALUES = ["TRUE", "FALSE"]

class SnowflakeManagedIcebergCatalogIntegration(CatalogIntegration):
    catalog_type = CatalogIntegrationType.managed
    auto_refresh: Optional[str] = None  # "TRUE" | "FALSE"
    replace_invalid_characters: Optional[str] = None  # "TRUE" | "FALSE"

    def _handle_adapter_properties(self, adapter_properties: Optional[Dict]) -> None:
        if adapter_properties:
            if auto_refresh := adapter_properties.get("auto_refresh"):
                if auto_refresh not in _AUTO_REFRESH_VALUES:
                    raise DbtValidationError(f"Invalid auto_refresh value: {auto_refresh}")
                self.auto_refresh = auto_refresh
            if replace_invalid_characters := adapter_properties.get("replace_invalid_characters"):
                if replace_invalid_characters not in _REPLACE_INVALID_CHARACTERS_VALUES:
                    raise DbtValidationError(f"Invalid replace_invalid_characters value: {replace_invalid_characters}")
                self.replace_invalid_characters = replace_invalid_characters

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
        import agate

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


class SnowflakeGlueCatalogIntegration(CatalogIntegration):
    catalog_type = CatalogIntegrationType.glue
    auto_refresh: Optional[str] = None  # "TRUE" | "FALSE"
    replace_invalid_characters: Optional[str] = None  # "TRUE" | "FALSE"

    def _handle_adapter_properties(self, adapter_properties: Optional[Dict]) -> None:
        if adapter_properties:
            if "auto_refresh" in adapter_properties:
                self.auto_refresh = adapter_properties["auto_refresh"]
            if "replace_invalid_characters" in adapter_properties:
                self.replace_invalid_characters = adapter_properties["replace_invalid_characters"]

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
