from dataclasses import dataclass
from typing import Optional

from dbt.adapters.catalogs import register_catalog_config
from dbt_common.dataclass_schema import dbtClassMixin
from dbt_common.exceptions import DbtValidationError


@dataclass
class BiglakeMetastoreBigqueryConfig(dbtClassMixin):
    external_volume: str
    file_format: str
    base_location_root: Optional[str] = None

    def __post_init__(self) -> None:
        if not self.external_volume.strip():
            raise DbtValidationError("'external_volume' must be non-empty")
        if not self.external_volume.startswith("gs://"):
            raise DbtValidationError(
                "'external_volume' must be a path to a Cloud Storage bucket (gs://<bucket_name>)"
            )
        if self.file_format.lower() != "parquet":
            raise DbtValidationError("file_format must be 'parquet'")
        if self.base_location_root is not None and not self.base_location_root.strip():
            raise DbtValidationError("'base_location_root' cannot be blank")


register_catalog_config("biglake_metastore", "bigquery", BiglakeMetastoreBigqueryConfig)
