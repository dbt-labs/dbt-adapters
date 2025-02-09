
from dbt_common.exceptions import DbtRuntimeError


class DbtCatalogIntegrationAlreadyExistsError(DbtRuntimeError):
    def __init__(self, catalog_name: str):
        self.catalog_name = catalog_name
        msg = f"Catalog integration {self.catalog_name} already exists"
        super().__init__(msg)
