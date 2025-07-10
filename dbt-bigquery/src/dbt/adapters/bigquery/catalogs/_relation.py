from dataclasses import dataclass
from typing import Optional

from dbt.adapters.catalogs import CatalogRelation

from dbt.adapters.bigquery import constants


@dataclass
class BigQueryCatalogRelation(CatalogRelation):
    catalog_type: str = constants.DEFAULT_INFO_SCHEMA_CATALOG.catalog_type
    catalog_name: Optional[str] = constants.DEFAULT_INFO_SCHEMA_CATALOG.name
    table_format: Optional[str] = constants.INFO_SCHEMA_TABLE_FORMAT
    file_format: Optional[str] = constants.INFO_SCHEMA_FILE_FORMAT
    external_volume: Optional[str] = None
    storage_uri: Optional[str] = None
