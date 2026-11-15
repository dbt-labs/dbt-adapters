from copy import deepcopy
from types import SimpleNamespace

import pytest

from dbt.adapters.snowflake import constants
from dbt.adapters.snowflake.catalogs import BuiltInCatalogIntegration


@pytest.fixture
def fake_integration() -> BuiltInCatalogIntegration:
    return BuiltInCatalogIntegration(constants.DEFAULT_BUILT_IN_CATALOG)


model_base = SimpleNamespace(
    database="my_database",
    schema="my_schema",
    identifier="my_table",
    config={
        "catalog": "snowflake",
        "external_volume": "s3_iceberg_snow",
    },
)


@pytest.mark.parametrize(
    "config,expected",
    [
        (
            {},
            "_dbt/my_schema/my_table",
        ),
        (
            {"base_location_root": None, "base_location_subpath": None},
            "_dbt/my_schema/my_table",
        ),
        (
            {"base_location_root": "root_path", "base_location_subpath": "subpath"},
            "root_path/my_schema/my_table/subpath",
        ),
        (
            {"base_location_subpath": "subpath"},
            "_dbt/my_schema/my_table/subpath",
        ),
        (
            {"base_location_root": "root_path"},
            "root_path/my_schema/my_table",
        ),
    ],
)
def test_iceberg_base_location_built_in(fake_integration, config, expected):
    """Test when base_location_root and base_location_subpath are provided"""
    model = deepcopy(model_base)
    model.config.update(config)
    relation = fake_integration.build_relation(model)
    assert relation.base_location == expected


@pytest.mark.parametrize(
    "config,expected",
    [
        (None, None),
        (False, None),  # false ignored for Iceberg
        (True, "TRUE"),
        ("False", None),  # false ignored for Iceberg
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
        name="SNOWFLAKE",
        catalog_type="BUILT_IN",
        external_volume="s3_iceberg_snow",
        file_format=None,
        adapter_properties={"change_tracking": True},
    )
    integration = BuiltInCatalogIntegration(catalog_config)
    model = deepcopy(model_base)
    relation = integration.build_relation(model)
    assert relation.change_tracking == "TRUE"


def test_model_config_overrides_adapter_properties():
    catalog_config = SimpleNamespace(
        name="SNOWFLAKE",
        catalog_type="BUILT_IN",
        external_volume="s3_iceberg_snow",
        file_format=None,
        adapter_properties={"change_tracking": True},
    )
    integration = BuiltInCatalogIntegration(catalog_config)
    model = deepcopy(model_base)
    model.config.update({"change_tracking": False})
    relation = integration.build_relation(model)
    # model false overrides adapter true, then is omitted (None) since Iceberg can't disable it
    assert relation.change_tracking is None


@pytest.mark.parametrize(
    "config,expected",
    [
        ({}, None),
        ({"iceberg_version": None}, None),
        ({"iceberg_version": 3}, 3),
        ({"iceberg_version": 1}, 1),
    ],
)
def test_iceberg_version_model_config(fake_integration, config, expected):
    model = deepcopy(model_base)
    model.config.update(config)
    relation = fake_integration.build_relation(model)
    assert relation.iceberg_version == expected


def test_iceberg_version_catalog_default():
    catalog_config = SimpleNamespace(
        name="SNOWFLAKE",
        catalog_type="BUILT_IN",
        external_volume=None,
        file_format=None,
        adapter_properties={"iceberg_version": 3},
    )
    integration = BuiltInCatalogIntegration(catalog_config)
    model = deepcopy(model_base)
    relation = integration.build_relation(model)
    assert relation.iceberg_version == 3


def test_iceberg_version_model_overrides_catalog():
    catalog_config = SimpleNamespace(
        name="SNOWFLAKE",
        catalog_type="BUILT_IN",
        external_volume=None,
        file_format=None,
        adapter_properties={"iceberg_version": 1},
    )
    integration = BuiltInCatalogIntegration(catalog_config)
    model = deepcopy(model_base)
    model.config.update({"iceberg_version": 3})
    relation = integration.build_relation(model)
    assert relation.iceberg_version == 3


# --- managed storage default behavior flag ---


def _make_no_ev_model():
    """Model with table_format=iceberg but no external_volume in config or catalog."""
    return SimpleNamespace(
        database="my_database",
        schema="my_schema",
        identifier="my_table",
        config={"table_format": "iceberg"},
    )


def _make_no_ev_integration():
    catalog_config = SimpleNamespace(
        name="SNOWFLAKE",
        catalog_type="BUILT_IN",
        external_volume=None,
        file_format=None,
        adapter_properties=None,
    )
    return BuiltInCatalogIntegration(catalog_config)


def test_managed_storage_default_on_emits_snowflake_managed():
    integration = _make_no_ev_integration()
    integration.use_snowflake_managed_storage_default = True
    relation = integration.build_relation(_make_no_ev_model())
    assert relation.external_volume == "SNOWFLAKE_MANAGED"
    assert relation.base_location is None


def test_managed_storage_default_off_omits_external_volume():
    integration = _make_no_ev_integration()
    integration.use_snowflake_managed_storage_default = False
    relation = integration.build_relation(_make_no_ev_model())
    assert relation.external_volume is None
    assert relation.base_location is not None  # generated from schema/identifier


def test_explicit_snowflake_managed_in_model_config_always_suppresses_base_location():
    """Explicit SNOWFLAKE_MANAGED in model config suppresses base_location regardless of the flag."""
    integration = _make_no_ev_integration()
    model = _make_no_ev_model()
    model.config = {"table_format": "iceberg", "external_volume": "SNOWFLAKE_MANAGED"}
    for flag in (True, False):
        integration.use_snowflake_managed_storage_default = flag
        relation = integration.build_relation(model)
        assert relation.external_volume == "SNOWFLAKE_MANAGED"
        assert relation.base_location is None
