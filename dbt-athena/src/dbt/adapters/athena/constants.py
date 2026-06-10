from types import SimpleNamespace

from dbt.adapters.events.logging import AdapterLogger

DEFAULT_THREAD_COUNT = 4
DEFAULT_RETRY_ATTEMPTS = 3
DEFAULT_POLLING_INTERVAL = 5
DEFAULT_SPARK_COORDINATOR_DPU_SIZE = 1
DEFAULT_SPARK_MAX_CONCURRENT_DPUS = 2
DEFAULT_SPARK_EXECUTOR_DPU_SIZE = 1
DEFAULT_CALCULATION_TIMEOUT = 43200  # seconds = 12 hours
SESSION_IDLE_TIMEOUT_MIN = 10  # minutes

DEFAULT_SPARK_PROPERTIES = {
    # https://docs.aws.amazon.com/athena/latest/ug/notebooks-spark-table-formats.html
    "iceberg": {
        "spark.sql.catalog.spark_catalog": "org.apache.iceberg.spark.SparkSessionCatalog",
        "spark.sql.catalog.spark_catalog.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
        "spark.sql.catalog.spark_catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    },
    "hudi": {
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.hudi.catalog.HoodieCatalog",
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.sql.extensions": "org.apache.spark.sql.hudi.HoodieSparkSessionExtension",
    },
    "delta_lake": {
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    },
    # https://docs.aws.amazon.com/athena/latest/ug/notebooks-spark-encryption.html
    "spark_encryption": {
        "spark.authenticate": "true",
        "spark.io.encryption.enabled": "true",
        "spark.network.crypto.enabled": "true",
    },
    # https://docs.aws.amazon.com/athena/latest/ug/spark-notebooks-cross-account-glue.html
    "spark_cross_account_catalog": {"spark.hadoop.aws.glue.catalog.separator": "/"},
    # https://docs.aws.amazon.com/athena/latest/ug/notebooks-spark-requester-pays.html
    "spark_requester_pays": {"spark.hadoop.fs.s3.useRequesterPaysHeader": "true"},
}

LOGGER = AdapterLogger(__name__)

# Catalog integration (catalogs.yml v2) constants
ICEBERG_TABLE_FORMAT = "iceberg"
HIVE_TABLE_FORMAT = "hive"
PARQUET_FILE_FORMAT = "parquet"
GLUE_CATALOG_TYPE = "glue"
INFO_SCHEMA_CATALOG_TYPE = "info_schema"

# Default catalog registered for every model that does not reference a catalog.
# Maps to Athena's standard Hive table_type.
DEFAULT_INFO_SCHEMA_CATALOG = SimpleNamespace(
    name="info_schema",
    catalog_name="info_schema",
    catalog_type=INFO_SCHEMA_CATALOG_TYPE,
    table_format=HIVE_TABLE_FORMAT,
    external_volume=None,
    file_format=PARQUET_FILE_FORMAT,
    adapter_properties={},
)

# Default writable Iceberg catalog backed by the AWS Glue Data Catalog.
DEFAULT_GLUE_CATALOG = SimpleNamespace(
    name="glue",
    catalog_name="glue",
    catalog_type=GLUE_CATALOG_TYPE,
    table_format=ICEBERG_TABLE_FORMAT,
    external_volume=None,
    file_format=PARQUET_FILE_FORMAT,
    adapter_properties={},
)
