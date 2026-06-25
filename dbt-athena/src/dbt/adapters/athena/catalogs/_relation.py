from dataclasses import dataclass
from typing import Optional

from dbt.adapters.catalogs import CatalogRelation

from dbt.adapters.athena import constants


@dataclass
class AthenaCatalogRelation(CatalogRelation):
    # NOTE: only `table_format` is currently consumed (by resolve_table_type() to choose
    # hive vs iceberg). `file_format` and `external_volume` are carried for protocol parity
    # but are NOT yet honored in the generated DDL — format still comes from the model
    # `format` config and location from `external_location` / `s3_data_dir`. Wiring them
    # through is tracked as a follow-up.
    catalog_type: str = constants.DEFAULT_INFO_SCHEMA_CATALOG.catalog_type
    catalog_name: Optional[str] = constants.DEFAULT_INFO_SCHEMA_CATALOG.name
    table_format: Optional[str] = constants.HIVE_TABLE_FORMAT
    file_format: Optional[str] = constants.PARQUET_FILE_FORMAT
    external_volume: Optional[str] = None
