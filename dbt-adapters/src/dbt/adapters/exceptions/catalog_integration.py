from typing import List

from dbt_common.exceptions import DbtRuntimeError


class DbtCatalogIntegrationAlreadyExistsError(DbtRuntimeError):
    def __init__(self, catalog_name: str):
        self.catalog_name = catalog_name
        msg = f"Catalog integration {self.catalog_name} already exists"
        super().__init__(msg)


class DbtCatalogIntegrationNotFoundError(DbtRuntimeError):
    def __init__(self, catalog_name: str, existing_catalog_names: List[str]):
        self.catalog_name = catalog_name
        msg = f"Catalog integration {self.catalog_name} not found. Maybe you meant one of these catalog names: {existing_catalog_names}?"
        super().__init__(msg)
