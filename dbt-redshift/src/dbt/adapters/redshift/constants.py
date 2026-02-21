from types import SimpleNamespace


# Table formats
ICEBERG_TABLE_FORMAT = "iceberg"
DEFAULT_TABLE_FORMAT = "default"

# File formats
PARQUET_FILE_FORMAT = "parquet"
DEFAULT_FILE_FORMAT = "default"

# Catalog types
GLUE_CATALOG_TYPE = "glue"
INFO_SCHEMA_CATALOG_TYPE = "INFO_SCHEMA"


DEFAULT_INFO_SCHEMA_CATALOG = SimpleNamespace(
    name="info_schema",
    catalog_name="info_schema",
    catalog_type=INFO_SCHEMA_CATALOG_TYPE,
    table_format=DEFAULT_TABLE_FORMAT,
    external_volume=None,
    file_format=DEFAULT_FILE_FORMAT,
    adapter_properties={},
)

DEFAULT_GLUE_CATALOG = SimpleNamespace(
    name="glue",
    catalog_name="glue",
    catalog_type=GLUE_CATALOG_TYPE,
    table_format=ICEBERG_TABLE_FORMAT,
    external_volume=None,
    file_format=PARQUET_FILE_FORMAT,
    adapter_properties={},
)
