import pytest

from dbt.adapters.snowflake.relation_configs import (
    SnowflakeDynamicTableConfig,
    SnowflakeDynamicTableConfigChangeset,
    SnowflakeDynamicTableInitializationWarehouseConfigChange,
)
from dbt.adapters.relation_configs import RelationConfigChangeAction


class TestSnowflakeDynamicTableConfig:
    """Tests for SnowflakeDynamicTableConfig"""

    def test_snowflake_initialization_warehouse_is_optional(self):
        """Verify snowflake_initialization_warehouse is optional and defaults to None"""
        config = SnowflakeDynamicTableConfig.from_dict(
            {
                "name": "test_table",
                "schema_name": "test_schema",
                "database_name": "test_db",
                "query": "SELECT 1",
                "target_lag": "1 minute",
                "snowflake_warehouse": "TEST_WH",
                # snowflake_initialization_warehouse is intentionally omitted
            }
        )

        assert config.name == "test_table"
        assert config.snowflake_warehouse == "TEST_WH"
        assert config.snowflake_initialization_warehouse is None

    def test_snowflake_initialization_warehouse_can_be_set(self):
        """Verify snowflake_initialization_warehouse can be explicitly set"""
        config = SnowflakeDynamicTableConfig.from_dict(
            {
                "name": "test_table",
                "schema_name": "test_schema",
                "database_name": "test_db",
                "query": "SELECT 1",
                "target_lag": "1 minute",
                "snowflake_warehouse": "TEST_WH",
                "snowflake_initialization_warehouse": "INIT_WH",
            }
        )

        assert config.snowflake_warehouse == "TEST_WH"
        assert config.snowflake_initialization_warehouse == "INIT_WH"


class TestSnowflakeDynamicTableConfigChangeset:
    """Tests for SnowflakeDynamicTableConfigChangeset"""

    def test_changeset_without_initialization_warehouse_has_no_changes(self):
        """Verify changeset is empty when no changes"""
        changeset = SnowflakeDynamicTableConfigChangeset()

        assert changeset.snowflake_initialization_warehouse is None
        assert not changeset.has_changes
        assert not changeset.requires_full_refresh

    def test_changeset_with_initialization_warehouse_change(self):
        """Verify changeset detects initialization warehouse changes"""
        changeset = SnowflakeDynamicTableConfigChangeset(
            snowflake_initialization_warehouse=SnowflakeDynamicTableInitializationWarehouseConfigChange(
                action=RelationConfigChangeAction.alter,
                context="NEW_INIT_WH",
            )
        )

        assert changeset.snowflake_initialization_warehouse is not None
        assert changeset.has_changes
        assert not changeset.requires_full_refresh  # warehouse changes don't require full refresh

    def test_initialization_warehouse_change_does_not_require_full_refresh(self):
        """Verify initialization warehouse changes can be applied via ALTER"""
        change = SnowflakeDynamicTableInitializationWarehouseConfigChange(
            action=RelationConfigChangeAction.alter,
            context="NEW_INIT_WH",
        )

        assert not change.requires_full_refresh
