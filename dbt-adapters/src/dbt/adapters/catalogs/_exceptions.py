from typing import Iterable

from dbt_common.exceptions import DbtConfigError


class DbtCatalogIntegrationAlreadyExistsError(DbtConfigError):
    def __init__(self, catalog_name: str) -> None:
        self.catalog_name = catalog_name
        msg = f"Catalog already exists: {self.catalog_name}."
        super().__init__(msg)


class DbtCatalogIntegrationNotFoundError(DbtConfigError):
    def __init__(self, catalog_name: str, existing_catalog_names: Iterable[str]) -> None:
        self.catalog_name = catalog_name
        msg = (
            f"Catalog not found."
            f"Received: {self.catalog_name}"
            f"Expected one of: {', '.join(existing_catalog_names)}?"
        )
        super().__init__(msg)


class DbtCatalogIntegrationNotSupportedError(DbtConfigError):
    def __init__(self, catalog_type: str, supported_catalog_types: Iterable[str]) -> None:
        self.catalog_type = catalog_type
        msg = (
            f"Catalog type is not supported.\n"
            f"Received: {catalog_type}\n"
            f"Expected one of: {', '.join(supported_catalog_types)}"
        )
        super().__init__(msg)


class InvalidCatalogIntegrationConfigError(DbtConfigError):
    def __init__(self, catalog_name: str, msg: str) -> None:
        self.catalog_name = catalog_name
        msg = f"Invalid catalog integration config: {self.catalog_name}. {msg}"
        super().__init__(msg)
