from types import SimpleNamespace

ADAPTER_TYPE = "snowflake"

DEFAULT_PYTHON_VERSION_FOR_PYTHON_MODELS = "3.9"

INFO_SCHEMA_TABLE_FORMAT = "DEFAULT"
ICEBERG_TABLE_FORMAT = "ICEBERG"

DEFAULT_INFO_SCHEMA_CATALOG = SimpleNamespace(
    name="INFO_SCHEMA",  # these don't show up in Snowflake; this is a dbt convention
    catalog_type="INFO_SCHEMA",  # these don't show up in Snowflake; this is a dbt convention
    external_volume=None,
    file_format=None,
    adapter_properties={},
)
# catalog names must be uppercase since Snowflake will uppercase them in their metadata tables
DEFAULT_BUILT_IN_CATALOG = SimpleNamespace(
    name="SNOWFLAKE",
    catalog_type="BUILT_IN",
    # assume you are using the default volume or specify in the model
    external_volume=None,
    file_format=None,
    adapter_properties={},
)

SnowflakeIcebergTableRelationParameters = SimpleNamespace(
    storage_serialization_policy="storage_serialization_policy",
    data_retention_time_in_days="data_retention_time_in_days",
    max_data_extension_time_in_days="max_data_extension_time_in_days",
    change_tracking="change_tracking",
    automatic_clustering="automatic_clustering",
)
