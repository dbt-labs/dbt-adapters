from types import SimpleNamespace

from dbt.adapters.catalogs import CatalogIntegration, CatalogIntegrationMode
from dbt.adapters.contracts.relation import RelationConfig


ICEBERG_MANAGED_CATALOG = SimpleNamespace(
    **{
        "name": "snowflake",
        "catalog_type": "iceberg_managed",
        "table_format": "iceberg",
        "external_volume": None,  # assume you are using the default volume or specify in the model
        "adapter_properties": {},
    }
)


class IcebergManagedTable:
    def __init__(self, config: RelationConfig) -> None:
        self.base_location = self.build_base_location(config)
        self.external_volume = config.get("external_volume")  # type:ignore

    @staticmethod
    def build_base_location(config: RelationConfig) -> str:
        # If the base_location_root config is supplied, overwrite the default value ("_dbt/")
        prefix = config.get("base_location_root", "_dbt")  # type:ignore

        base_location = f"{prefix}/{config.schema}/{config.identifier}"

        if subpath := config.get("base_location_subpath"):  # type:ignore
            base_location += f"/{subpath}"

        return base_location


class IcebergManagedCatalogIntegration(CatalogIntegration):
    allows_writes = CatalogIntegrationMode.WRITE

    @staticmethod
    def catalog_table(self, config: RelationConfig) -> IcebergManagedTable:
        return IcebergManagedTable(config)
