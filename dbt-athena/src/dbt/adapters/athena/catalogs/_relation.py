from dataclasses import dataclass
from typing import Optional

from dbt.adapters.catalogs import CatalogRelation

from dbt.adapters.athena import constants


@dataclass
class AthenaCatalogRelation(CatalogRelation):
    catalog_type: str = constants.DEFAULT_INFO_SCHEMA_CATALOG.catalog_type
    catalog_name: Optional[str] = constants.DEFAULT_INFO_SCHEMA_CATALOG.name
    table_format: Optional[str] = constants.HIVE_TABLE_FORMAT
    file_format: Optional[str] = constants.PARQUET_FILE_FORMAT
    external_volume: Optional[str] = None
