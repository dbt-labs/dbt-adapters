from types import SimpleNamespace


ADAPTER_TYPE = "bigquery"


INFO_SCHEMA_TABLE_FORMAT = "default"
ICEBERG_TABLE_FORMAT = "iceberg"


INFO_SCHEMA_FILE_FORMAT = "default"
PARQUET_FILE_FORMAT = "parquet"


GENERIC_CATALOG_TYPE = "generic"


DEFAULT_INFO_SCHEMA_CATALOG = SimpleNamespace(
    name="info_schema",
    catalog_name="info_schema",
    catalog_type=GENERIC_CATALOG_TYPE,
    table_format=INFO_SCHEMA_TABLE_FORMAT,
    external_volume=None,
    adapter_properties={
        "file_format": INFO_SCHEMA_FILE_FORMAT,
    },
)
DEFAULT_ICEBERG_CATALOG = SimpleNamespace(
    name="managed_iceberg",
    catalog_name="managed_iceberg",
    catalog_type=GENERIC_CATALOG_TYPE,
    table_format=ICEBERG_TABLE_FORMAT,
    external_volume=None,
    adapter_properties={
        "file_format": PARQUET_FILE_FORMAT,
    },
)
