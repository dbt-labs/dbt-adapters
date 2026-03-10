"""
Unit tests for SnowflakeDynamicTableConfig, testing:
- snowflake_initialization_warehouse parameter
- immutable_where parameter
- transient parameter
"""

import pytest

from dbt.adapters.relation_configs import RelationConfigChangeAction
from dbt.adapters.snowflake.relation_configs import (
    SnowflakeDynamicTableConfig,
    SnowflakeDynamicTableConfigChangeset,
    SnowflakeDynamicTableImmutableWhereConfigChange,
    SnowflakeDynamicTableInitializationWarehouseConfigChange,
    SnowflakeDynamicTableTransientConfigChange,
)


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

    def test_initialization_warehouse_unset_change(self):
        """Unsetting initialization_warehouse (setting to None) should be valid."""
        changeset = SnowflakeDynamicTableConfigChangeset(
            snowflake_initialization_warehouse=SnowflakeDynamicTableInitializationWarehouseConfigChange(
                action=RelationConfigChangeAction.alter,
                context=None,
            )
        )

        assert changeset.snowflake_initialization_warehouse is not None
        assert changeset.snowflake_initialization_warehouse.context is None
        assert changeset.has_changes
        assert not changeset.requires_full_refresh


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


class TestTransientOptional:
    """Tests to verify transient is an optional parameter for dynamic tables."""

    def test_transient_is_optional(self):
        """transient should be optional and default to None (use behavior flag)."""
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
        # None means "not explicitly set by user, use behavior flag default"
        assert config.transient is None

    def test_transient_can_be_set_true(self):
        """transient should be settable to True."""
        config = SnowflakeDynamicTableConfig.from_dict(
            {
                "name": "test_table",
                "schema_name": "test_schema",
                "database_name": "test_db",
                "query": "SELECT 1",
                "target_lag": "1 hour",
                "snowflake_warehouse": "MY_WH",
                "transient": True,
            }
        )
        assert config.transient is True

    def test_transient_can_be_set_false(self):
        """transient can be explicitly set to False."""
        config = SnowflakeDynamicTableConfig.from_dict(
            {
                "name": "test_table",
                "schema_name": "test_schema",
                "database_name": "test_db",
                "query": "SELECT 1",
                "target_lag": "1 hour",
                "snowflake_warehouse": "MY_WH",
                "transient": False,
            }
        )
        assert config.transient is False

    def test_transient_can_be_none(self):
        """transient can be None (meaning not explicitly set by user)."""
        config = SnowflakeDynamicTableConfig.from_dict(
            {
                "name": "test_table",
                "schema_name": "test_schema",
                "database_name": "test_db",
                "query": "SELECT 1",
                "target_lag": "1 hour",
                "snowflake_warehouse": "MY_WH",
                "transient": None,
            }
        )
        assert config.transient is None


class TestTransientChangeset:
    """Tests for transient change detection in SnowflakeDynamicTableConfigChangeset."""

    def test_changeset_without_transient_has_no_changes(self):
        """A changeset with no transient change should not have changes."""
        changeset = SnowflakeDynamicTableConfigChangeset()
        assert changeset.transient is None
        assert changeset.has_changes is False
        assert changeset.requires_full_refresh is False

    def test_changeset_with_transient_change(self):
        """A changeset with transient change should have changes and require full refresh."""
        changeset = SnowflakeDynamicTableConfigChangeset(
            transient=SnowflakeDynamicTableTransientConfigChange(
                action=RelationConfigChangeAction.create,
                context=True,
            )
        )
        assert changeset.transient is not None
        assert changeset.has_changes is True
        # Transient changes REQUIRE full refresh (cannot be altered)
        assert changeset.requires_full_refresh is True

    def test_transient_change_requires_full_refresh(self):
        """Changing transient should require a full refresh (cannot be altered in place)."""
        change = SnowflakeDynamicTableTransientConfigChange(
            action=RelationConfigChangeAction.create,
            context=True,
        )
        assert change.requires_full_refresh is True

    def test_transient_change_to_false_requires_full_refresh(self):
        """Changing transient to False should also require full refresh."""
        change = SnowflakeDynamicTableTransientConfigChange(
            action=RelationConfigChangeAction.create,
            context=False,
        )
        assert change.requires_full_refresh is True

    def test_changeset_with_transient_and_other_changes(self):
        """A changeset with transient and other changes should require full refresh."""
        changeset = SnowflakeDynamicTableConfigChangeset(
            transient=SnowflakeDynamicTableTransientConfigChange(
                action=RelationConfigChangeAction.create,
                context=True,
            ),
            immutable_where=SnowflakeDynamicTableImmutableWhereConfigChange(
                action=RelationConfigChangeAction.alter,
                context="id < 100",
            ),
        )
        assert changeset.has_changes is True
        # Even though immutable_where doesn't require full refresh,
        # transient does, so the entire changeset requires full refresh
        assert changeset.requires_full_refresh is True


class TestTransientChangeDetectionLogic:
    """
    Tests for transient change detection in SnowflakeRelation.dynamic_table_config_changeset().

    Transient is only compared when:
    - The user explicitly set transient in their config (not None)
    - The existing transient column is present in the relation results (not None)
    """

    @staticmethod
    def _make_relation_results(transient=None):
        import agate

        dt_row_data = {
            "name": "test_table",
            "schema_name": "test_schema",
            "database_name": "test_db",
            "text": "SELECT 1",
            "target_lag": "1 hour",
            "warehouse": "MY_WH",
            "refresh_mode": "AUTO",
            "immutable_where": None,
        }
        column_types = [agate.Text()] * len(dt_row_data)

        if transient is not None:
            dt_row_data["transient"] = transient
            column_types.append(agate.Boolean())

        return {
            "dynamic_table": agate.Table(
                [list(dt_row_data.values())],
                list(dt_row_data.keys()),
                column_types,
            )
        }

    @staticmethod
    def _make_relation_config(transient=None):
        from unittest.mock import MagicMock

        relation_config = MagicMock()
        relation_config.identifier = "test_table"
        relation_config.schema = "test_schema"
        relation_config.database = "test_db"
        relation_config.compiled_code = "SELECT 1"
        relation_config.config.extra = {
            "target_lag": "1 hour",
            "snowflake_warehouse": "MY_WH",
            "transient": transient,
        }
        relation_config.config.get = lambda key, default=None: relation_config.config.extra.get(
            key, default
        )
        return relation_config

    def test_no_transient_config_no_column_no_change(self):
        """When user doesn't set transient and column is absent, no change is detected."""
        from dbt.adapters.snowflake.relation import SnowflakeRelation

        relation_results = self._make_relation_results()
        relation_config = self._make_relation_config(transient=None)

        changeset = SnowflakeRelation.dynamic_table_config_changeset(
            relation_results, relation_config
        )

        if changeset is not None:
            assert changeset.transient is None

    def test_explicit_transient_matches_existing_no_change(self):
        """When user sets transient=True and existing is also True, no change."""
        from dbt.adapters.snowflake.relation import SnowflakeRelation

        relation_results = self._make_relation_results(transient=True)
        relation_config = self._make_relation_config(transient=True)

        changeset = SnowflakeRelation.dynamic_table_config_changeset(
            relation_results, relation_config
        )

        if changeset is not None:
            assert changeset.transient is None

    def test_explicit_transient_differs_from_existing_triggers_change(self):
        """When user sets transient=True but existing is False, change is detected."""
        from dbt.adapters.snowflake.relation import SnowflakeRelation

        relation_results = self._make_relation_results(transient=False)
        relation_config = self._make_relation_config(transient=True)

        changeset = SnowflakeRelation.dynamic_table_config_changeset(
            relation_results, relation_config
        )

        assert changeset is not None
        assert changeset.transient is not None
        assert changeset.transient.context is True
        assert changeset.requires_full_refresh is True

    def test_explicit_non_transient_differs_from_existing_triggers_change(self):
        """When user sets transient=False but existing is True, change is detected."""
        from dbt.adapters.snowflake.relation import SnowflakeRelation

        relation_results = self._make_relation_results(transient=True)
        relation_config = self._make_relation_config(transient=False)

        changeset = SnowflakeRelation.dynamic_table_config_changeset(
            relation_results, relation_config
        )

        assert changeset is not None
        assert changeset.transient is not None
        assert changeset.transient.context is False
        assert changeset.requires_full_refresh is True

    def test_explicit_transient_with_no_column_no_change(self):
        """When user sets transient but column is absent in results, no change."""
        from dbt.adapters.snowflake.relation import SnowflakeRelation

        relation_results = self._make_relation_results()
        relation_config = self._make_relation_config(transient=True)

        changeset = SnowflakeRelation.dynamic_table_config_changeset(
            relation_results, relation_config
        )

        if changeset is not None:
            assert changeset.transient is None

    def test_no_config_ignores_existing_transient_true(self):
        """When user omits transient, no change even if existing table is transient."""
        from dbt.adapters.snowflake.relation import SnowflakeRelation

        relation_results = self._make_relation_results(transient=True)
        relation_config = self._make_relation_config(transient=None)

        changeset = SnowflakeRelation.dynamic_table_config_changeset(
            relation_results, relation_config
        )

        if changeset is not None:
            assert changeset.transient is None

    def test_no_config_ignores_existing_transient_false(self):
        """When user omits transient, no change even if existing table is non-transient."""
        from dbt.adapters.snowflake.relation import SnowflakeRelation

        relation_results = self._make_relation_results(transient=False)
        relation_config = self._make_relation_config(transient=None)

        changeset = SnowflakeRelation.dynamic_table_config_changeset(
            relation_results, relation_config
        )

        if changeset is not None:
            assert changeset.transient is None
