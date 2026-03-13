from dataclasses import dataclass
from typing import List, Optional

from dbt.adapters.redshift import constants


@dataclass
class RedshiftCatalogRelation:
    """
    Represents the catalog-specific configuration for a Redshift relation.

    This class holds all the information needed to create and manage
    tables in different catalog types (Glue/Iceberg or standard Redshift).

    Attributes:
        catalog_type: The type of catalog (e.g., 'glue', 'INFO_SCHEMA')
        catalog_name: The name of the catalog integration in dbt project
        table_format: The table format (e.g., 'iceberg', 'default')
        file_format: The file format for data storage (e.g., 'parquet')
        external_volume: The S3 base path for Iceberg table data
        storage_uri: The full S3 path for this specific table
        glue_database: The AWS Glue database name
        external_schema: The Redshift external schema name
        partition_by: List of columns to partition by
    """

    catalog_type: str = constants.DEFAULT_INFO_SCHEMA_CATALOG.catalog_type
    catalog_name: Optional[str] = constants.DEFAULT_INFO_SCHEMA_CATALOG.name
    table_format: Optional[str] = constants.DEFAULT_TABLE_FORMAT
    file_format: Optional[str] = constants.DEFAULT_FILE_FORMAT
    external_volume: Optional[str] = None
    storage_uri: Optional[str] = None
    glue_database: Optional[str] = None
    external_schema: Optional[str] = None
    partition_by: Optional[List[str]] = None

    @property
    def is_iceberg_format(self) -> bool:
        """Check if this relation uses Iceberg table format."""
        return self.table_format == constants.ICEBERG_TABLE_FORMAT
