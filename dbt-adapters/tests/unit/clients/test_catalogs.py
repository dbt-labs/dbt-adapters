from dbt.adapters.clients.catalogs import add_catalog, get_catalog


def test_adding_catalog_integration(fake_catalog_integration):
    catalog = fake_catalog_integration
    add_catalog(catalog, catalog_name="fake_catalog")
    get_catalog("fake_catalog")
