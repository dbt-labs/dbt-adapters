from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, Dict, Optional

import pytest

from dbt.adapters.catalogs import (
    CatalogIntegration,
    CatalogIntegrationClient,
    CatalogIntegrationConfig,
    CatalogRelation,
    DbtCatalogIntegrationAlreadyExistsError,
    DbtCatalogIntegrationNotFoundError,
    DbtCatalogIntegrationNotSupportedError,
)
from dbt.adapters.contracts.relation import RelationConfig


@dataclass
class FakeCatalogIntegrationConfig(CatalogIntegrationConfig):
    name: str
    catalog_type: str
    catalog_name: Optional[str] = None
    table_format: Optional[str] = None
    external_volume: Optional[str] = None
    file_format: Optional[str] = None
    adapter_properties: Optional[Dict[str, Any]] = None


@dataclass
class FakeRelationConfig:
    resource_type = ""
    name = ""
    description = ""
    database = ""
    schema = ""
    identifier = ""
    compiled_code = None
    meta = {}
    tags = []
    quoting_dict = {}
    config = None
    catalog_name = None
    external_volume: str = "fake_relation_volume"


class FakeCatalogIntegration(CatalogIntegration):
    catalog_type: str = "fake"
    allows_writes: bool = False
    table_format: str = "fake_format"

    def build_relation(self, config: RelationConfig) -> CatalogRelation:
        return SimpleNamespace(
            catalog_name=self.catalog_name,
            table_format=self.table_format,
            external_volume=config.external_volume or self.external_volume,
        )


@pytest.fixture(scope="function")
def fake_client() -> CatalogIntegrationClient:
    return CatalogIntegrationClient([FakeCatalogIntegration])


@pytest.fixture
def fake_catalog() -> CatalogIntegrationConfig:
    return FakeCatalogIntegrationConfig(
        name="fake_integration",
        catalog_type="fake",
        external_volume="fake_volume",
    )


@pytest.fixture
def fake_unsupported_catalog() -> CatalogIntegrationConfig:
    return FakeCatalogIntegrationConfig(
        name="fake_integration",
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


def test_build_relation(fake_client, fake_catalog):
    fake_client.add(fake_catalog)
    catalog = fake_client.get("fake_integration")
    relation_config = FakeRelationConfig()
    relation = catalog.build_relation(relation_config)
    assert relation.catalog_name == catalog.catalog_name
    assert relation.table_format == catalog.table_format
    assert relation.external_volume == relation_config.external_volume
