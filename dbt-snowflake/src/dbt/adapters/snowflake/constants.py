from types import SimpleNamespace


DEFAULT_PYTHON_VERSION_FOR_PYTHON_MODELS = "3.9"

# these catalog names must be uppercase since Snowflake will uppercase them in their metadata tables
DEFAULT_CATALOG = SimpleNamespace(
    name="NATIVE", catalog_type="native", external_volume=None, adapter_properties={}
)
DEFAULT_ICEBERG_CATALOG = SimpleNamespace(
    name="SNOWFLAKE",
    catalog_type="iceberg_managed",
    # assume you are using the default volume or specify in the model
    external_volume=None,
    adapter_properties={},
)

ICEBERG_TABLE_FORMAT = "iceberg"
ADAPTER_TYPE = "snowflake"
