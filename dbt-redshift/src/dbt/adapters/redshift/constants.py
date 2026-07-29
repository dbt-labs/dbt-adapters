from types import SimpleNamespace

ADAPTER_TYPE = "redshift"

# table formats
DEFAULT_TABLE_FORMAT = "default"
ICEBERG_TABLE_FORMAT = "iceberg"

# catalog types
GLUE_CATALOG_TYPE = "glue"

# Redshift Iceberg tables are created in an external schema mapped to an AWS Glue
# Data Catalog database. The external volume maps to the Iceberg ``LOCATION`` (the
# S3 prefix the table data is written to).
DEFAULT_GLUE_CATALOG = SimpleNamespace(
    name="glue",
    catalog_type=GLUE_CATALOG_TYPE,
    catalog_name="glue",
    external_volume=None,
    table_format=ICEBERG_TABLE_FORMAT,
    file_format=None,
    adapter_properties={},
)
