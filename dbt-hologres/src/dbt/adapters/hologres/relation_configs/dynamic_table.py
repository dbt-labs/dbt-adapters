from dataclasses import dataclass, field
from typing import FrozenSet, List, Optional, Set

from dbt.adapters.relation_configs import (
    RelationConfigBase,
    RelationConfigChangeAction,
    RelationResults,
)
from dbt.adapters.contracts.relation import RelationConfig
from dbt_common.dataclass_schema import dbtClassMixin


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class HologresDynamicTableConfig(dbtClassMixin):
    """
    Configuration for Hologres Dynamic Table (物化视图).
    
    Dynamic Table is Hologres's implementation of materialized views with automatic refresh.
    """
    freshness: str  # Required: e.g., "30 minutes", "1 hours"
    auto_refresh_enable: bool = True
    auto_refresh_mode: str = "auto"  # auto | incremental | full
    computing_resource: str = "serverless"  # serverless | local | warehouse_name
    base_table_cdc_format: str = "stream"  # stream | binlog
    
    # Partition related
    partition_key: Optional[str] = None
    partition_type: Optional[str] = None  # logical | physical
    partition_key_time_format: Optional[str] = None
    auto_refresh_partition_active_time: Optional[str] = None
    
    # Table properties
    orientation: str = "column"
    distribution_key: Optional[List[str]] = None
    clustering_key: Optional[List[str]] = None
    event_time_column: Optional[List[str]] = None
    bitmap_columns: Optional[List[str]] = None
    dictionary_encoding_columns: Optional[List[str]] = None
    time_to_live_in_seconds: Optional[int] = None
    storage_mode: str = "hot"  # hot | cold

    @classmethod
    def from_dict(cls, config_dict) -> "HologresDynamicTableConfig":
        kwargs_dict = {}
        
        # Required field
        if "freshness" in config_dict:
            kwargs_dict["freshness"] = config_dict["freshness"]
        
        # Optional fields with defaults
        optional_fields = [
            "auto_refresh_enable", "auto_refresh_mode", "computing_resource",
            "base_table_cdc_format", "partition_key", "partition_type",
            "partition_key_time_format", "auto_refresh_partition_active_time",
            "orientation", "distribution_key", "clustering_key", "event_time_column",
            "bitmap_columns", "dictionary_encoding_columns", "time_to_live_in_seconds",
            "storage_mode"
        ]
        
        for field_name in optional_fields:
            if field_name in config_dict:
                kwargs_dict[field_name] = config_dict[field_name]
        
        return cls(**kwargs_dict)

    @classmethod
    def from_config(cls, relation_config: RelationConfig) -> "HologresDynamicTableConfig":
        """Parse dynamic table config from RelationConfig"""
        config_dict = relation_config.config.extra
        return cls.from_dict(config_dict)

    @classmethod
    def from_relation_results(cls, relation_results: RelationResults) -> "HologresDynamicTableConfig":
        """Parse dynamic table config from query results"""
        # This would be implemented to parse from system tables
        # For now, return a minimal config
        return cls(freshness="1 hours")


@dataclass
class HologresDynamicTableConfigChangeCollection:
    """Collection of dynamic table configuration changes"""
    freshness: Optional[str] = None
    auto_refresh_mode: Optional[str] = None
    auto_refresh_enable: Optional[bool] = None
    computing_resource: Optional[str] = None

    @property
    def has_changes(self) -> bool:
        return any([
            self.freshness is not None,
            self.auto_refresh_mode is not None,
            self.auto_refresh_enable is not None,
            self.computing_resource is not None,
        ])

    @property
    def requires_full_refresh(self) -> bool:
        # Most dynamic table config changes don't require full refresh
        # Only changes to query logic would require that
        return False
