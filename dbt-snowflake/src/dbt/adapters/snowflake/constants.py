from types import SimpleNamespace


ADAPTER_TYPE = "snowflake"


DEFAULT_PYTHON_VERSION_FOR_PYTHON_MODELS = "3.9"


NATIVE_TABLE_FORMAT = "DEFAULT"
ICEBERG_TABLE_FORMAT = "ICEBERG"


# these catalog names must be uppercase since Snowflake will uppercase them in their metadata tables
DEFAULT_NATIVE_CATALOG = SimpleNamespace(
    name="NATIVE",
    catalog_type="NATIVE",
    external_volume=None,
    adapter_properties={},
)
DEFAULT_ICEBERG_CATALOG = SimpleNamespace(
    name="SNOWFLAKE",
    catalog_type="ICEBERG_MANAGED",
    # assume you are using the default volume or specify in the model
    external_volume=None,
    adapter_properties={},
)
