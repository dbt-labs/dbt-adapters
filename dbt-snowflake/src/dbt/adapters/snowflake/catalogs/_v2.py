from dataclasses import dataclass
from typing import Any, Optional

from dbt_common.dataclass_schema import dbtClassMixin
from dbt_common.exceptions import DbtValidationError


def _check_int_range(field_name: str, val: int, max_val: int) -> None:
    if not (0 <= val <= max_val):
        raise DbtValidationError(f"'{field_name}' must be in 0..={max_val}")


def _check_enum(field_name: str, val: Any, allowed: set) -> None:
    if str(val).strip().lower() not in allowed:
        raise DbtValidationError(
            f"'{field_name}' value '{val}' is invalid. Must be {sorted(allowed)}"
        )


@dataclass
class HorizonSnowflakeConfig(dbtClassMixin):
    external_volume: str
    base_location_root: Optional[str] = None
    change_tracking: Optional[bool] = None
    data_retention_time_in_days: Optional[int] = None
    max_data_extension_time_in_days: Optional[int] = None
    storage_serialization_policy: Optional[str] = None

    def __post_init__(self) -> None:
        if not self.external_volume.strip():
            raise DbtValidationError("'external_volume' must be non-empty")
        if self.base_location_root is not None and not self.base_location_root.strip():
            raise DbtValidationError("'base_location_root' cannot be blank")
        if self.data_retention_time_in_days is not None:
            _check_int_range("data_retention_time_in_days", self.data_retention_time_in_days, 90)
        if self.max_data_extension_time_in_days is not None:
            _check_int_range(
                "max_data_extension_time_in_days", self.max_data_extension_time_in_days, 90
            )
        if self.storage_serialization_policy is not None:
            _check_enum(
                "storage_serialization_policy",
                self.storage_serialization_policy,
                {"compatible", "optimized"},
            )


@dataclass
class LinkedSnowflakeConfig(dbtClassMixin):
    """Shared config for glue, iceberg_rest, and unity on snowflake."""

    catalog_database: str
    auto_refresh: Optional[bool] = None
    max_data_extension_time_in_days: Optional[int] = None
    target_file_size: Optional[str] = None

    def __post_init__(self) -> None:
        if not self.catalog_database.strip():
            raise DbtValidationError("'catalog_database' must be non-empty")
        if self.max_data_extension_time_in_days is not None:
            _check_int_range(
                "max_data_extension_time_in_days", self.max_data_extension_time_in_days, 90
            )
        if self.target_file_size is not None:
            _check_enum(
                "target_file_size",
                self.target_file_size,
                {"auto", "16mb", "32mb", "64mb", "128mb"},
            )
