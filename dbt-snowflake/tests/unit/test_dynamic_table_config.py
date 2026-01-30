"""
Unit tests for SnowflakeDynamicTableConfig, specifically testing the immutable_where parameter.
"""

import pytest

from dbt.adapters.relation_configs import RelationConfigChangeAction
from dbt.adapters.snowflake.relation_configs import (
    SnowflakeDynamicTableConfig,
    SnowflakeDynamicTableConfigChangeset,
    SnowflakeDynamicTableImmutableWhereConfigChange,
)


class TestImmutableWhereOptional:
    """Tests to verify immutable_where is an optional parameter."""

    def test_immutable_where_is_optional(self):
        """immutable_where should be optional and default to None."""
        config = SnowflakeDynamicTableConfig.from_dict(
            {
                "name": "test_table",
                "schema_name": "test_schema",
                "database_name": "test_db",
                "query": "SELECT 1",
                "target_lag": "1 hour",
                "snowflake_warehouse": "MY_WH",
            }
        )
        assert config.immutable_where is None

    def test_immutable_where_can_be_set(self):
        """immutable_where should be settable to an expression."""
        config = SnowflakeDynamicTableConfig.from_dict(
            {
                "name": "test_table",
                "schema_name": "test_schema",
                "database_name": "test_db",
                "query": "SELECT 1",
                "target_lag": "1 hour",
                "snowflake_warehouse": "MY_WH",
                "immutable_where": "ts < CURRENT_TIMESTAMP() - INTERVAL '1 day'",
            }
        )
        assert config.immutable_where == "ts < CURRENT_TIMESTAMP() - INTERVAL '1 day'"

    def test_immutable_where_can_be_explicit_none(self):
        """immutable_where can be explicitly set to None."""
        config = SnowflakeDynamicTableConfig.from_dict(
            {
                "name": "test_table",
                "schema_name": "test_schema",
                "database_name": "test_db",
                "query": "SELECT 1",
                "target_lag": "1 hour",
                "snowflake_warehouse": "MY_WH",
                "immutable_where": None,
            }
        )
        assert config.immutable_where is None


class TestImmutableWhereChangeset:
    """Tests for immutable_where change detection in SnowflakeDynamicTableConfigChangeset."""

    def test_changeset_without_immutable_where_has_no_changes(self):
        """A changeset with no immutable_where change should not require full refresh."""
        changeset = SnowflakeDynamicTableConfigChangeset()
        assert changeset.immutable_where is None
        assert changeset.has_changes is False
        assert changeset.requires_full_refresh is False

    def test_changeset_with_immutable_where_change(self):
        """A changeset with immutable_where change should have changes but not require full refresh."""
        changeset = SnowflakeDynamicTableConfigChangeset(
            immutable_where=SnowflakeDynamicTableImmutableWhereConfigChange(
                action=RelationConfigChangeAction.alter,
                context="ts < CURRENT_TIMESTAMP() - INTERVAL '7 days'",
            )
        )
        assert changeset.immutable_where is not None
        assert changeset.has_changes is True
        assert changeset.requires_full_refresh is False

    def test_immutable_where_change_does_not_require_full_refresh(self):
        """Changing immutable_where should not require a full refresh (can be altered in place)."""
        change = SnowflakeDynamicTableImmutableWhereConfigChange(
            action=RelationConfigChangeAction.alter,
            context="ts < CURRENT_TIMESTAMP() - INTERVAL '30 days'",
        )
        assert change.requires_full_refresh is False

    def test_immutable_where_unset_change(self):
        """Unsetting immutable_where (setting to None) should be valid."""
        changeset = SnowflakeDynamicTableConfigChangeset(
            immutable_where=SnowflakeDynamicTableImmutableWhereConfigChange(
                action=RelationConfigChangeAction.alter,
                context=None,
            )
        )
        assert changeset.immutable_where is not None
        assert changeset.immutable_where.context is None
        assert changeset.has_changes is True
        assert changeset.requires_full_refresh is False
