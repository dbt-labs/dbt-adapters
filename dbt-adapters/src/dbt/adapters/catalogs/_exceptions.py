from typing import List

from dbt_common.exceptions import DbtRuntimeError, DbtValidationError


class DbtCatalogIntegrationAlreadyExistsError(DbtRuntimeError):
    def __init__(self, catalog_name: str):
        self.catalog_name = catalog_name
        msg = f"Catalog already exists: {self.catalog_name}."
        super().__init__(msg)


class DbtCatalogIntegrationNotFoundError(DbtRuntimeError):
    def __init__(self, catalog_name: str, existing_catalog_names: List[str]):
        self.catalog_name = catalog_name
        msg = (
            f"Catalog not found."
            f"Received: {self.catalog_name}"
            f"Expected: {existing_catalog_names}?"
        )
        super().__init__(msg)


class DbtCatalogNotSupportedError(DbtValidationError):
    def __init__(self, catalog_type: str, supported_catalog_types: List[str]):
        self.catalog_type = catalog_type
        msg = (
            f"Catalog type is not supported."
            f"Received: {catalog_type}"
            f"Expected: {supported_catalog_types}"
        )
        super().__init__(msg)
