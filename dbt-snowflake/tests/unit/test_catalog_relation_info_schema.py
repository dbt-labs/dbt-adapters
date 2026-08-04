from copy import deepcopy
from types import SimpleNamespace

import pytest

from dbt.adapters.snowflake import constants
from dbt.adapters.snowflake.catalogs import InfoSchemaCatalogIntegration


@pytest.fixture
def fake_integration() -> InfoSchemaCatalogIntegration:
    return InfoSchemaCatalogIntegration(constants.DEFAULT_INFO_SCHEMA_CATALOG)


model_base = SimpleNamespace(
    database="my_database",
    schema="my_schema",
    identifier="my_table",
    config={"materialized": "table"},
)


@pytest.mark.parametrize(
    "config,expected",
    [
        (None, None),
        (False, "FALSE"),
        (True, "TRUE"),
        ("False", "FALSE"),
        ("True", "TRUE"),
    ],
)
def test_change_tracking_model_config(fake_integration, config, expected):
    model = deepcopy(model_base)
    model.config.update({"change_tracking": config})
    relation = fake_integration.build_relation(model)
    assert relation.change_tracking == expected


@pytest.mark.parametrize(
    "user_input",
    [
        "0",
        "",
        "None",
    ],
)
def test_change_tracking_invalid_model_config(fake_integration, user_input):
    model = deepcopy(model_base)
    model.config.update({"change_tracking": user_input})
    with pytest.raises(ValueError) as e:
        fake_integration.build_relation(model)
    assert "Invalid value for change_tracking" in str(e.value)


def test_change_tracking_not_set(fake_integration):
    model = deepcopy(model_base)
    relation = fake_integration.build_relation(model)
    assert relation.change_tracking is None


def test_change_tracking_from_adapter_properties():
    catalog_config = SimpleNamespace(
        name="INFO_SCHEMA",
        catalog_type="INFO_SCHEMA",
        external_volume=None,
        file_format=None,
        adapter_properties={"change_tracking": True},
    )
    integration = InfoSchemaCatalogIntegration(catalog_config)
    model = deepcopy(model_base)
    relation = integration.build_relation(model)
    assert relation.change_tracking == "TRUE"


def test_model_config_overrides_adapter_properties():
    catalog_config = SimpleNamespace(
        name="INFO_SCHEMA",
        catalog_type="INFO_SCHEMA",
        external_volume=None,
        file_format=None,
        adapter_properties={"change_tracking": True},
    )
    integration = InfoSchemaCatalogIntegration(catalog_config)
    model = deepcopy(model_base)
    model.config.update({"change_tracking": False})
    relation = integration.build_relation(model)
    assert relation.change_tracking == "FALSE"
