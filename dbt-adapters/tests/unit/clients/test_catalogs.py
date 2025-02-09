import pytest
from dbt.adapters.clients.catalogs import add_catalog, get_catalog
from dbt.adapters.exceptions.catalog_integration import (
    DbtCatalogIntegrationAlreadyExistsError,
    DbtCatalogIntegrationNotFoundError,
)


def test_adding_and_getting_catalog_integration(fake_catalog_integration):
    catalog = fake_catalog_integration
    add_catalog(catalog, catalog_name="fake_catalog")
    assert get_catalog("fake_catalog").catalog_name == catalog.catalog_name


def test_adding_catalog_integration_that_already_exists(fake_catalog_integration):
    catalog = fake_catalog_integration
    catalog_name = "fake_catalog_2"
    add_catalog(catalog, catalog_name=catalog_name)
    with pytest.raises(DbtCatalogIntegrationAlreadyExistsError):
        add_catalog(catalog, catalog_name=catalog_name)


def test_getting_catalog_integration_that_does_not_exist(fake_catalog_integration):
    catalog = fake_catalog_integration
    add_catalog(catalog, catalog_name="fake_existing_catalog")
    with pytest.raises(DbtCatalogIntegrationNotFoundError) as e:
        get_catalog("non_existent_catalog")
    assert e.value.catalog_name == "non_existent_catalog"
    assert "fake_existing_catalog" in str(e.value)
