from typing import Dict

from dbt_config.config import ExternalCatalogConfig, ExternalCatalog, Type as CatalogType


class CatalogConfig:
    identifier: str
    type: CatalogType



class CatalogManager:
    catalogs: Dict[str, ExternalCatalog]

    def add_catalog(self, catalog: ExternalCatalog):
        self.catalogs[catalog.name] = catalog

    def get_catalog(self, catalog_name: str):
        return self.catalogs.get(catalog_name)
