from dbt.adapters.catalogs import CatalogIntegration, CatalogIntegrationConfig
from dbt.adapters.contracts.relation import RelationConfig

from dbt.adapters.athena import constants
from dbt.adapters.athena.catalogs._relation import AthenaCatalogRelation


class S3TablesCatalogIntegration(CatalogIntegration):
    """Writable Iceberg catalog backed by AWS S3 Tables.

    S3 Tables manages storage automatically — no explicit S3 location is set
    in the CREATE TABLE DDL. An S3 Tables bucket surfaces in Athena/Glue as the
    federated catalog "s3tablescatalog/<bucket>", which is the model's database;
    the namespace maps to the model's schema.

    Users declare just the bucket name via ``table_bucket``; we derive the
    federated ``catalog_database`` from it so the model's database is routed
    through ``generate_database_name`` without repeating the "s3tablescatalog/"
    prefix. Setting ``catalog_database`` directly is still honored (e.g. to point
    at a bucket in another account/region).
    """

    catalog_type = constants.S3_TABLES_CATALOG_TYPE
    table_format = constants.ICEBERG_TABLE_FORMAT
    file_format = constants.PARQUET_FILE_FORMAT
    allows_writes = True

    def __init__(self, config: CatalogIntegrationConfig) -> None:
        super().__init__(config)
        table_bucket = (getattr(config, "adapter_properties", None) or {}).get("table_bucket")
        if table_bucket:
            self.catalog_database = f"{constants.S3_TABLES_GLUE_CATALOG_PREFIX}/{table_bucket}"

    def build_relation(self, model: RelationConfig) -> AthenaCatalogRelation:
        """
        Args:
            model: `config.model` (not `model`) from the jinja context
        """
        return AthenaCatalogRelation(
            catalog_type=self.catalog_type,
            catalog_name=self.catalog_name,
            table_format=self.table_format,
            file_format=self.file_format,
            # S3 Tables manages storage automatically; no external_volume needed.
            external_volume=None,
            catalog_database=self.catalog_database,
        )
