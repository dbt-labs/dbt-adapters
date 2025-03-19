from dataclasses import dataclass
from typing import Any, Dict, Optional

import pytest

from dbt.adapters.catalogs import (
    CatalogIntegration,
    CatalogIntegrationClient,
    CatalogIntegrationConfig,
    DbtCatalogIntegrationAlreadyExistsError,
    DbtCatalogIntegrationNotFoundError,
    DbtCatalogIntegrationNotSupportedError,
)


@dataclass
class FakeCatalogIntegrationConfig(CatalogIntegrationConfig):
    name: str
    catalog_type: str
    table_format: Optional[str] = None
    external_volume: Optional[str] = None
    adapter_properties: Optional[Dict[str, Any]] = None


@pytest.fixture(scope="function")
def fake_client() -> CatalogIntegrationClient:
    return CatalogIntegrationClient({"fake": CatalogIntegration})


@pytest.fixture
def fake_catalog() -> CatalogIntegrationConfig:
    return FakeCatalogIntegrationConfig(
        name="test_integration",
        catalog_type="fake",
    )


@pytest.fixture
def fake_unsupported_catalog() -> CatalogIntegrationConfig:
    return FakeCatalogIntegrationConfig(
        name="test_integration",
        catalog_type="banana",
    )


def test_adding_catalog_integration(fake_client, fake_catalog):
    with pytest.raises(DbtCatalogIntegrationNotFoundError):
        fake_client.get(fake_catalog.name)
    registered_catalog = fake_client.add(fake_catalog)
    assert registered_catalog.name == fake_catalog.name


def test_getting_catalog_integration(fake_client, fake_catalog):
    with pytest.raises(DbtCatalogIntegrationNotFoundError):
        fake_client.get(fake_catalog.name)
    fake_client.add(fake_catalog)
    registered_catalog = fake_client.get(fake_catalog.name)
    assert registered_catalog.name == fake_catalog.name


def test_adding_catalog_integration_that_already_exists(fake_client, fake_catalog):
    fake_client.add(fake_catalog)
    with pytest.raises(DbtCatalogIntegrationAlreadyExistsError) as e:
        fake_client.add(fake_catalog)
    assert e.value.catalog_name == fake_catalog.name
    assert fake_catalog.name in str(e.value)


def test_getting_catalog_integration_that_does_not_exist(fake_client, fake_catalog):
    fake_client.add(fake_catalog)
    with pytest.raises(DbtCatalogIntegrationNotFoundError) as e:
        fake_client.get("non_existent_catalog")
    assert e.value.catalog_name == "non_existent_catalog"
    assert fake_catalog.name in str(e.value)
    assert "non_existent_catalog" in str(e.value)


def test_adding_unsupported_catalog_integration(
    fake_client, fake_unsupported_catalog, fake_catalog
):
    with pytest.raises(DbtCatalogIntegrationNotSupportedError) as e:
        fake_client.add(fake_unsupported_catalog)
    assert e.value.catalog_type == fake_unsupported_catalog.catalog_type
    assert fake_unsupported_catalog.catalog_type in str(e.value)
    assert fake_catalog.catalog_type in str(e.value)
