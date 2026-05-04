import pytest

from dbt.adapters.snowflake.catalogs._v2 import (
    HorizonSnowflakeConfig,
    LinkedSnowflakeConfig,
)
from dbt.adapters.snowflake.impl import SnowflakeAdapter
from dbt_common.exceptions import DbtValidationError


# ===== CATALOG_V2_CONFIGS class attribute =====


def test_horizon_registered():
    assert SnowflakeAdapter.CATALOG_V2_CONFIGS["horizon"] is HorizonSnowflakeConfig


@pytest.mark.parametrize("v2_type", ["glue", "iceberg_rest", "unity"])
def test_linked_types_registered(v2_type):
    assert SnowflakeAdapter.CATALOG_V2_CONFIGS[v2_type] is LinkedSnowflakeConfig


# ===== HorizonSnowflakeConfig =====


def test_horizon_minimal_valid():
    cfg = HorizonSnowflakeConfig(external_volume="my_vol")
    assert cfg.external_volume == "my_vol"


def test_horizon_full_valid():
    cfg = HorizonSnowflakeConfig(
        external_volume="my_vol",
        base_location_root="root",
        change_tracking=True,
        data_retention_time_in_days=30,
        max_data_extension_time_in_days=14,
        storage_serialization_policy="OPTIMIZED",
    )
    assert cfg.data_retention_time_in_days == 30


def test_horizon_blank_external_volume():
    with pytest.raises(DbtValidationError, match="external_volume.*non-empty"):
        HorizonSnowflakeConfig(external_volume="   ")


def test_horizon_blank_base_location_root():
    with pytest.raises(DbtValidationError, match="base_location_root.*blank"):
        HorizonSnowflakeConfig(external_volume="vol", base_location_root="  ")


def test_horizon_retention_out_of_range():
    with pytest.raises(DbtValidationError, match="must be in 0..=90"):
        HorizonSnowflakeConfig(external_volume="vol", data_retention_time_in_days=91)


def test_horizon_invalid_storage_policy():
    with pytest.raises(DbtValidationError, match="invalid"):
        HorizonSnowflakeConfig(external_volume="vol", storage_serialization_policy="BAD")


def test_horizon_validate_rejects_unknown_keys():
    with pytest.raises(Exception, match="Additional properties"):
        HorizonSnowflakeConfig.validate({"external_volume": "vol", "unknown_key": "x"})


# ===== LinkedSnowflakeConfig =====


def test_linked_minimal_valid():
    cfg = LinkedSnowflakeConfig(catalog_database="DB")
    assert cfg.catalog_database == "DB"


def test_linked_full_valid():
    cfg = LinkedSnowflakeConfig(
        catalog_database="DB",
        auto_refresh=True,
        max_data_extension_time_in_days=7,
        target_file_size="64mb",
    )
    assert cfg.target_file_size == "64mb"


def test_linked_blank_catalog_database():
    with pytest.raises(DbtValidationError, match="catalog_database.*non-empty"):
        LinkedSnowflakeConfig(catalog_database="  ")


def test_linked_invalid_target_file_size():
    with pytest.raises(DbtValidationError, match="invalid"):
        LinkedSnowflakeConfig(catalog_database="DB", target_file_size="999MB")


def test_linked_validate_rejects_unknown_keys():
    with pytest.raises(Exception, match="Additional properties"):
        LinkedSnowflakeConfig.validate({"catalog_database": "DB", "bogus": True})
