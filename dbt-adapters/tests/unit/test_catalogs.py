from argparse import Namespace

import pytest

from dbt.adapters.catalogs import (
    CatalogIntegration,
    CatalogIntegrationClient,
    CatalogIntegrationConfig,
    DbtCatalogIntegrationAlreadyExistsError,
    DbtCatalogIntegrationNotFoundError,
    DbtCatalogNotSupportedError,
)


@pytest.fixture(scope="function")
def fake_client() -> CatalogIntegrationClient:
    return CatalogIntegrationClient({"managed": CatalogIntegration})


@pytest.fixture
def fake_catalog() -> CatalogIntegrationConfig:
    return Namespace(
        name="test_integration",
        type="managed",
        table_format="iceberg",
    )


@pytest.fixture
def fake_unsupported_catalog() -> CatalogIntegrationConfig:
    return Namespace(
        name="test_integration",
        type="banana",
        table_format="iceberg",
    )


def test_adding_catalog_integration(fake_client, fake_catalog):
    assert fake_catalog.name not in fake_client.catalogs
    fake_client.add(fake_catalog)
    assert fake_catalog.name in fake_client.catalogs


def test_getting_catalog_integration(fake_client, fake_catalog):
    assert fake_client.catalogs == {}
    fake_client.add(fake_catalog)
    assert fake_client.get(fake_catalog.name).name == fake_catalog.name


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


def test_unsupported_catalog_integration(fake_client, fake_unsupported_catalog, fake_catalog):
    with pytest.raises(DbtCatalogNotSupportedError) as e:
        fake_client.add(fake_unsupported_catalog)
    assert e.value.catalog_type == fake_unsupported_catalog.type
    assert fake_unsupported_catalog.type in str(e.value)
    assert fake_catalog.type in str(e.value)
