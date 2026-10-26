from collections.abc import Sequence
from dataclasses import dataclass, fields
from typing import Any, Dict, List, Optional

from dbt_common.dataclass_schema import StrEnum
from dbt_common.exceptions import CompilationError, DbtRuntimeError
from typing_extensions import Self

from dbt.adapters.contracts.relation import RelationConfig
from dbt.adapters.snowflake.relation_configs.base import SnowflakeRelationConfigBase
from dbt.adapters.snowflake.relation_configs._normalize import normalize_target_lag, split_keys


class ConfigKey(StrEnum):
    SEARCH_COLUMN = "search_column"
    ATTRIBUTES = "attributes"
    SNOWFLAKE_WAREHOUSE = "snowflake_warehouse"
    TARGET_LAG = "target_lag"
    PRIMARY_KEY = "primary_key"
    EMBEDDING_MODEL = "embedding_model"
    REFRESH_MODE = "refresh_mode"
    INITIALIZE = "initialize"
    FULL_INDEX_BUILD_INTERVAL_DAYS = "full_index_build_interval_days"
    REQUEST_LOGGING = "request_logging"
    AUTO_SUSPEND = "auto_suspend"
    COMMENT = "comment"


class MetadataKey(StrEnum):
    ATTRIBUTE_COLUMNS = "attribute_columns"
    PRIMARY_KEY_COLUMNS = "primary_key_columns"
    WAREHOUSE = "warehouse"
    INDEXING_ERROR = "indexing_error"


class RefreshMode(StrEnum):
    FULL = "FULL"
    INCREMENTAL = "INCREMENTAL"

    @classmethod
    def default(cls) -> Self:
        return cls("INCREMENTAL")


class Initialize(StrEnum):
    ON_CREATE = "ON_CREATE"
    ON_SCHEDULE = "ON_SCHEDULE"

    @classmethod
    def default(cls) -> Self:
        return cls("ON_CREATE")


def _identifier(value: str) -> str:
    value = value.strip()
    if value.startswith('"') and value.endswith('"'):
        return value[1:-1].replace('""', '"')
    return value.upper()


@dataclass(frozen=True)
class SnowflakeCortexSearchConfig(SnowflakeRelationConfigBase):
    search_column: Optional[str] = None
    attributes: Optional[List[str]] = None
    snowflake_warehouse: Optional[str] = None
    target_lag: Optional[str] = None
    primary_key: Optional[List[str]] = None
    embedding_model: Optional[str] = None
    refresh_mode: Optional[RefreshMode] = None
    initialize: Optional[Initialize] = None
    full_index_build_interval_days: Optional[int] = None
    request_logging: Optional[bool] = None
    auto_suspend: Optional[int] = None
    comment: Optional[str] = None

    @classmethod
    def parse_relation_config(cls, relation_config: RelationConfig) -> Dict[str, Any]:
        config = relation_config.config
        return {field.name: config.get(field.name) if config else None for field in fields(cls)}

    def __post_init__(self) -> None:
        for name in (ConfigKey.SEARCH_COLUMN, ConfigKey.TARGET_LAG):
            self._require_string(name, getattr(self, name))
        if not self.attributes:
            raise CompilationError(
                f"cortex_search_service requires a non-empty list for `{ConfigKey.ATTRIBUTES}`."
            )
        for name in (ConfigKey.ATTRIBUTES, ConfigKey.PRIMARY_KEY):
            columns = getattr(self, name)
            if columns is not None:
                if not isinstance(columns, Sequence) or isinstance(columns, str):
                    raise CompilationError(
                        f"cortex_search_service `{name}` must be a list of column names."
                    )
                if any(not isinstance(column, str) or not column.strip() for column in columns):
                    raise CompilationError(
                        f"cortex_search_service `{name}` must contain non-empty column names."
                    )
        if self.embedding_model is not None:
            self._require_string(ConfigKey.EMBEDDING_MODEL, self.embedding_model)
        if self.comment is not None and not isinstance(self.comment, str):
            raise CompilationError(
                f"cortex_search_service `{ConfigKey.COMMENT}` must be a string."
            )
        for name, enum_type in (
            (ConfigKey.REFRESH_MODE, RefreshMode),
            (ConfigKey.INITIALIZE, Initialize),
        ):
            allowed = [option.value for option in enum_type.__members__.values()]
            value = getattr(self, name)
            if value is not None:
                if not isinstance(value, str) or value.strip().upper() not in allowed:
                    raise CompilationError(
                        f"cortex_search_service `{name}` must be one of {', '.join(allowed)}."
                    )
                object.__setattr__(self, name, enum_type(value.strip().upper()))
        for name, minimum in (
            (ConfigKey.FULL_INDEX_BUILD_INTERVAL_DAYS, 1),
            (ConfigKey.AUTO_SUSPEND, 1800),
        ):
            value = getattr(self, name)
            if value is not None and (type(value) is not int or value < minimum):
                raise CompilationError(
                    f"cortex_search_service `{name}` must be an integer >= {minimum}."
                )
        if self.full_index_build_interval_days is not None and not self.primary_key:
            raise CompilationError(
                f"cortex_search_service `{ConfigKey.FULL_INDEX_BUILD_INTERVAL_DAYS}` "
                f"requires a non-empty `{ConfigKey.PRIMARY_KEY}`."
            )
        if self.request_logging is not None and not isinstance(self.request_logging, bool):
            raise CompilationError(
                f"cortex_search_service `{ConfigKey.REQUEST_LOGGING}` must be a boolean."
            )

    def warehouse_parameter(self, default_warehouse: Optional[str]) -> str:
        warehouse = self.snowflake_warehouse or default_warehouse
        return self._require_string(ConfigKey.SNOWFLAKE_WAREHOUSE, warehouse)

    def configuration_changes(
        self, metadata: Dict[str, Any], default_warehouse: Optional[str]
    ) -> Dict[str, Any]:
        alter: Dict[str, Any] = {}
        replace = []
        if (
            _identifier(self._require_string(ConfigKey.SEARCH_COLUMN, self.search_column))
            != metadata[ConfigKey.SEARCH_COLUMN]
        ):
            replace.append(ConfigKey.SEARCH_COLUMN)
        for name, column in (
            (ConfigKey.ATTRIBUTES, MetadataKey.ATTRIBUTE_COLUMNS),
            (ConfigKey.PRIMARY_KEY, MetadataKey.PRIMARY_KEY_COLUMNS),
        ):
            desired = getattr(self, name)
            if desired is not None:
                existing_columns = {
                    _identifier(part.strip()) if part.strip().startswith('"') else part.strip()
                    for part in split_keys(metadata.get(column) or "")
                    if part
                }
                if {_identifier(part) for part in desired} != existing_columns:
                    replace.append(name)
        warehouse = self.warehouse_parameter(default_warehouse)
        if _identifier(warehouse) != metadata[MetadataKey.WAREHOUSE]:
            alter[MetadataKey.WAREHOUSE] = warehouse
        if normalize_target_lag(self.target_lag) != normalize_target_lag(
            metadata[ConfigKey.TARGET_LAG]
        ):
            alter[ConfigKey.TARGET_LAG] = self.target_lag
        for name in (
            ConfigKey.EMBEDDING_MODEL,
            ConfigKey.REFRESH_MODE,
            ConfigKey.FULL_INDEX_BUILD_INTERVAL_DAYS,
            ConfigKey.REQUEST_LOGGING,
            ConfigKey.AUTO_SUSPEND,
            ConfigKey.COMMENT,
        ):
            desired = getattr(self, name)
            if desired is None:
                continue
            if name not in metadata:
                raise DbtRuntimeError(
                    f"Could not read Cortex Search configuration `{name}`; "
                    "cannot reconcile the configured value."
                )
            existing = metadata[name]
            if name == ConfigKey.REQUEST_LOGGING and isinstance(existing, str):
                existing = existing.lower() == "true"
            elif (
                name in (ConfigKey.FULL_INDEX_BUILD_INTERVAL_DAYS, ConfigKey.AUTO_SUSPEND)
                and existing is not None
            ):
                existing = int(existing)
            elif name == ConfigKey.REFRESH_MODE and isinstance(existing, str):
                existing = existing.upper()
            if desired != existing:
                if name in (ConfigKey.EMBEDDING_MODEL, ConfigKey.REFRESH_MODE):
                    replace.append(name)
                else:
                    alter[name] = desired
        return {"alter": alter, "replace": replace}

    @staticmethod
    def _require_string(name: str, value: Any) -> str:
        if not isinstance(value, str) or not value.strip():
            raise CompilationError(
                f"cortex_search_service requires a non-empty string for `{name}`."
            )
        return value
