from collections.abc import Sequence
from dataclasses import dataclass, fields
from typing import Any, Dict, List, Optional

from dbt_common.exceptions import CompilationError

from dbt.adapters.contracts.relation import RelationConfig
from dbt.adapters.snowflake.relation_configs.base import SnowflakeRelationConfigBase


@dataclass(frozen=True)
class SnowflakeCortexSearchConfig(SnowflakeRelationConfigBase):
    search_column: Optional[str] = None
    attributes: Optional[List[str]] = None
    snowflake_warehouse: Optional[str] = None
    target_lag: Optional[str] = None
    primary_key: Optional[List[str]] = None
    embedding_model: Optional[str] = None
    refresh_mode: Optional[str] = None
    initialize: Optional[str] = None
    full_index_build_interval_days: Optional[int] = None
    request_logging: Optional[bool] = None
    auto_suspend: Optional[int] = None
    comment: Optional[str] = None

    @classmethod
    def parse_relation_config(cls, relation_config: RelationConfig) -> Dict[str, Any]:
        config = relation_config.config
        return {field.name: config.get(field.name) if config else None for field in fields(cls)}

    def __post_init__(self) -> None:
        for name in ("search_column", "target_lag"):
            self._require_string(name, getattr(self, name))
        if not self.attributes:
            raise CompilationError(
                "cortex_search_service requires a non-empty list for `attributes`."
            )
        for name in ("attributes", "primary_key"):
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
            self._require_string("embedding_model", self.embedding_model)
        if self.comment is not None and not isinstance(self.comment, str):
            raise CompilationError("cortex_search_service `comment` must be a string.")
        for name, allowed in (
            ("refresh_mode", ("FULL", "INCREMENTAL")),
            ("initialize", ("ON_CREATE", "ON_SCHEDULE")),
        ):
            value = getattr(self, name)
            if value is not None:
                if not isinstance(value, str) or value.strip().upper() not in allowed:
                    raise CompilationError(
                        f"cortex_search_service `{name}` must be one of {', '.join(allowed)}."
                    )
                object.__setattr__(self, name, value.strip().upper())
        for name, minimum in (("full_index_build_interval_days", 1), ("auto_suspend", 1800)):
            value = getattr(self, name)
            if value is not None and (type(value) is not int or value < minimum):
                raise CompilationError(
                    f"cortex_search_service `{name}` must be an integer >= {minimum}."
                )
        if self.full_index_build_interval_days is not None and not self.primary_key:
            raise CompilationError(
                "cortex_search_service `full_index_build_interval_days` requires a non-empty `primary_key`."
            )
        if self.request_logging is not None and not isinstance(self.request_logging, bool):
            raise CompilationError("cortex_search_service `request_logging` must be a boolean.")

    def warehouse_parameter(self, default_warehouse: Optional[str]) -> str:
        warehouse = self.snowflake_warehouse or default_warehouse
        return self._require_string("snowflake_warehouse", warehouse)

    @staticmethod
    def _require_string(name: str, value: Any) -> str:
        if not isinstance(value, str) or not value.strip():
            raise CompilationError(
                f"cortex_search_service requires a non-empty string for `{name}`."
            )
        return value
