"""
Unit tests for SnowflakeDynamicTableConfig, testing:
- snowflake_initialization_warehouse parameter
- refresh_warehouse parameter and warehouse_parameter property
- immutable_where parameter
- transient parameter
- scheduler parameter
"""

import pytest

from dbt.adapters.relation_configs import RelationConfigChangeAction
from dbt.adapters.snowflake.relation_configs import (
    SnowflakeDynamicTableConfig,
    SnowflakeDynamicTableConfigChangeset,
    SnowflakeDynamicTableImmutableWhereConfigChange,
    SnowflakeDynamicTableInitializationWarehouseConfigChange,
    SnowflakeDynamicTableSchedulerConfigChange,
    SnowflakeDynamicTableTransientConfigChange,
    SnowflakeDynamicTableWarehouseConfigChange,
)
from dbt_common.exceptions import CompilationError


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


class TestSchedulerOptional:
    """Tests to verify scheduler is an optional parameter for dynamic tables."""

    def test_scheduler_is_optional(self):
        """scheduler should be optional and default to None."""
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
        assert config.scheduler is None

    def test_scheduler_can_be_set_enable(self):
        """scheduler should be settable to ENABLE."""
        config = SnowflakeDynamicTableConfig.from_dict(
            {
                "name": "test_table",
                "schema_name": "test_schema",
                "database_name": "test_db",
                "query": "SELECT 1",
                "target_lag": "1 hour",
                "snowflake_warehouse": "MY_WH",
                "scheduler": "ENABLE",
            }
        )
        assert config.scheduler == "ENABLE"

    def test_scheduler_can_be_set_disable(self):
        """scheduler should be settable to DISABLE."""
        config = SnowflakeDynamicTableConfig.from_dict(
            {
                "name": "test_table",
                "schema_name": "test_schema",
                "database_name": "test_db",
                "query": "SELECT 1",
                "snowflake_warehouse": "MY_WH",
                "scheduler": "DISABLE",
            }
        )
        assert config.scheduler == "DISABLE"

    def test_scheduler_can_be_none(self):
        """scheduler can be explicitly set to None."""
        config = SnowflakeDynamicTableConfig.from_dict(
            {
                "name": "test_table",
                "schema_name": "test_schema",
                "database_name": "test_db",
                "query": "SELECT 1",
                "target_lag": "1 hour",
                "snowflake_warehouse": "MY_WH",
                "scheduler": None,
            }
        )
        assert config.scheduler is None


class TestSchedulerValidation:
    """Tests for compile-time validation of scheduler/target_lag combinations."""

    @staticmethod
    def _make_relation_config(scheduler=None, target_lag=None):
        from unittest.mock import MagicMock

        relation_config = MagicMock()
        relation_config.identifier = "test_table"
        relation_config.schema = "test_schema"
        relation_config.database = "test_db"
        relation_config.compiled_code = "SELECT 1"
        extra = {"snowflake_warehouse": "MY_WH"}
        if scheduler is not None:
            extra["scheduler"] = scheduler
        if target_lag is not None:
            extra["target_lag"] = target_lag
        relation_config.config.extra = extra
        relation_config.config.get = lambda key, default=None: relation_config.config.extra.get(
            key, default
        )
        return relation_config

    def test_no_target_lag_no_scheduler_defaults_to_disable(self):
        """Case 1: no target_lag, no scheduler -> scheduler inferred as DISABLE."""
        relation_config = self._make_relation_config(scheduler=None, target_lag=None)

        config = SnowflakeDynamicTableConfig.from_relation_config(relation_config)

        assert config.target_lag is None
        assert config.scheduler == "DISABLE"

    def test_target_lag_set_no_scheduler_defaults_to_enable(self):
        """Case 2: target_lag set, no scheduler -> scheduler inferred as ENABLE."""
        relation_config = self._make_relation_config(scheduler=None, target_lag="2 minutes")

        config = SnowflakeDynamicTableConfig.from_relation_config(relation_config)

        assert config.target_lag == "2 minutes"
        assert config.scheduler == "ENABLE"

    def test_no_target_lag_scheduler_disable_is_valid(self):
        """Case 3: no target_lag, scheduler='DISABLE' -> valid."""
        relation_config = self._make_relation_config(scheduler="DISABLE", target_lag=None)

        config = SnowflakeDynamicTableConfig.from_relation_config(relation_config)

        assert config.target_lag is None
        assert config.scheduler == "DISABLE"

    def test_target_lag_set_scheduler_disable_is_invalid(self):
        """Case 4: target_lag set, scheduler='DISABLE' -> CompilationError."""
        relation_config = self._make_relation_config(scheduler="DISABLE", target_lag="1 hour")

        with pytest.raises(CompilationError, match="requires `target_lag` to be omitted"):
            SnowflakeDynamicTableConfig.from_relation_config(relation_config)

    def test_scheduler_enable_valid_when_target_lag_set(self):
        relation_config = self._make_relation_config(scheduler="ENABLE", target_lag="1 hour")

        config = SnowflakeDynamicTableConfig.from_relation_config(relation_config)

        assert config.scheduler == "ENABLE"
        assert config.target_lag == "1 hour"

    def test_scheduler_enable_invalid_when_target_lag_none(self):
        relation_config = self._make_relation_config(scheduler="ENABLE", target_lag=None)

        with pytest.raises(CompilationError, match="requires `target_lag`"):
            SnowflakeDynamicTableConfig.from_relation_config(relation_config)

    def test_invalid_scheduler_literal_still_rejected(self):
        relation_config = self._make_relation_config(scheduler="ENABLED", target_lag="1 hour")

        with pytest.raises(CompilationError, match="Invalid value for `scheduler`"):
            SnowflakeDynamicTableConfig.from_relation_config(relation_config)


class TestSchedulerChangeset:
    """Tests for scheduler change detection in SnowflakeDynamicTableConfigChangeset."""

    def test_changeset_without_scheduler_has_no_changes(self):
        """A changeset with no scheduler change should not have changes."""
        changeset = SnowflakeDynamicTableConfigChangeset()
        assert changeset.scheduler is None
        assert changeset.has_changes is False
        assert changeset.requires_full_refresh is False

    def test_changeset_with_scheduler_change(self):
        """A changeset with scheduler change should have changes but not require full refresh."""
        changeset = SnowflakeDynamicTableConfigChangeset(
            scheduler=SnowflakeDynamicTableSchedulerConfigChange(
                action=RelationConfigChangeAction.alter,
                context="DISABLE",
            )
        )
        assert changeset.scheduler is not None
        assert changeset.has_changes is True
        assert changeset.requires_full_refresh is False

    def test_scheduler_change_does_not_require_full_refresh(self):
        """Changing scheduler should not require a full refresh (can be altered in place)."""
        change = SnowflakeDynamicTableSchedulerConfigChange(
            action=RelationConfigChangeAction.alter,
            context="ENABLE",
        )
        assert change.requires_full_refresh is False

    def test_scheduler_change_to_disable(self):
        """Changing scheduler to DISABLE should be a valid change."""
        changeset = SnowflakeDynamicTableConfigChangeset(
            scheduler=SnowflakeDynamicTableSchedulerConfigChange(
                action=RelationConfigChangeAction.alter,
                context="DISABLE",
            )
        )
        assert changeset.scheduler is not None
        assert changeset.scheduler.context == "DISABLE"
        assert changeset.has_changes is True
        assert changeset.requires_full_refresh is False


class TestTargetLagOptional:
    """Tests to verify target_lag is now optional."""

    def test_target_lag_can_be_none(self):
        """target_lag should now accept None (for scheduler=DISABLE scenarios)."""
        config = SnowflakeDynamicTableConfig.from_dict(
            {
                "name": "test_table",
                "schema_name": "test_schema",
                "database_name": "test_db",
                "query": "SELECT 1",
                "snowflake_warehouse": "MY_WH",
                "scheduler": "DISABLE",
            }
        )
        assert config.target_lag is None
        assert config.scheduler == "DISABLE"

    def test_target_lag_can_still_be_set(self):
        """target_lag should still work when provided."""
        config = SnowflakeDynamicTableConfig.from_dict(
            {
                "name": "test_table",
                "schema_name": "test_schema",
                "database_name": "test_db",
                "query": "SELECT 1",
                "target_lag": "5 minutes",
                "snowflake_warehouse": "MY_WH",
            }
        )
        assert config.target_lag == "5 minutes"

    def test_target_lag_defaults_to_none(self):
        """target_lag should default to None when omitted."""
        config = SnowflakeDynamicTableConfig.from_dict(
            {
                "name": "test_table",
                "schema_name": "test_schema",
                "database_name": "test_db",
                "query": "SELECT 1",
                "snowflake_warehouse": "MY_WH",
            }
        )
        assert config.target_lag is None


class TestSchedulerChangeDetectionLogic:
    """
    Tests for scheduler change detection in SnowflakeRelation.dynamic_table_config_changeset().
    """

    @staticmethod
    def _make_relation_results(scheduler=None, target_lag="1 hour"):
        import agate

        dt_row_data = {
            "name": "test_table",
            "schema_name": "test_schema",
            "database_name": "test_db",
            "text": "SELECT 1",
            "target_lag": target_lag,
            "warehouse": "MY_WH",
            "refresh_mode": "AUTO",
            "immutable_where": None,
        }
        column_types = [agate.Text()] * len(dt_row_data)

        if scheduler is not None:
            dt_row_data["scheduler"] = scheduler
            column_types.append(agate.Text())

        return {
            "dynamic_table": agate.Table(
                [list(dt_row_data.values())],
                list(dt_row_data.keys()),
                column_types,
            )
        }

    @staticmethod
    def _make_relation_config(scheduler=None, target_lag="1 hour"):
        from unittest.mock import MagicMock

        relation_config = MagicMock()
        relation_config.identifier = "test_table"
        relation_config.schema = "test_schema"
        relation_config.database = "test_db"
        relation_config.compiled_code = "SELECT 1"
        extra = {
            "snowflake_warehouse": "MY_WH",
        }
        if target_lag is not None:
            extra["target_lag"] = target_lag
        if scheduler is not None:
            extra["scheduler"] = scheduler
        relation_config.config.extra = extra
        relation_config.config.get = lambda key, default=None: relation_config.config.extra.get(
            key, default
        )
        return relation_config

    def test_no_scheduler_config_no_column_no_change(self):
        """When user doesn't set scheduler and column is absent, no change detected."""
        from dbt.adapters.snowflake.relation import SnowflakeRelation

        relation_results = self._make_relation_results()
        relation_config = self._make_relation_config(scheduler=None)

        changeset = SnowflakeRelation.dynamic_table_config_changeset(
            relation_results, relation_config
        )

        if changeset is not None:
            assert changeset.scheduler is None

    def test_scheduler_disable_matches_no_change(self):
        """When user sets scheduler=DISABLE and SHOW output is DISABLE, no change."""
        from dbt.adapters.snowflake.relation import SnowflakeRelation

        relation_results = self._make_relation_results(scheduler="DISABLE", target_lag=None)
        relation_config = self._make_relation_config(scheduler="DISABLE", target_lag=None)

        changeset = SnowflakeRelation.dynamic_table_config_changeset(
            relation_results, relation_config
        )

        if changeset is not None:
            assert changeset.scheduler is None

    def test_scheduler_enable_matches_no_change(self):
        """When user sets scheduler=ENABLE and SHOW output is ENABLE, no change."""
        from dbt.adapters.snowflake.relation import SnowflakeRelation

        relation_results = self._make_relation_results(scheduler="ENABLE")
        relation_config = self._make_relation_config(scheduler="ENABLE")

        changeset = SnowflakeRelation.dynamic_table_config_changeset(
            relation_results, relation_config
        )

        if changeset is not None:
            assert changeset.scheduler is None

    def test_scheduler_change_enable_to_disable(self):
        """When user sets scheduler=DISABLE but SHOW output is ENABLE, change detected."""
        from dbt.adapters.snowflake.relation import SnowflakeRelation

        relation_results = self._make_relation_results(scheduler="ENABLE")
        relation_config = self._make_relation_config(scheduler="DISABLE", target_lag=None)

        changeset = SnowflakeRelation.dynamic_table_config_changeset(
            relation_results, relation_config
        )

        assert changeset is not None
        assert changeset.scheduler is not None
        assert changeset.scheduler.context == "DISABLE"
        assert changeset.requires_full_refresh is False

    def test_scheduler_change_disable_to_enable(self):
        """When user sets scheduler=ENABLE but SHOW output is DISABLE, change detected."""
        from dbt.adapters.snowflake.relation import SnowflakeRelation

        relation_results = self._make_relation_results(scheduler="DISABLE", target_lag=None)
        relation_config = self._make_relation_config(scheduler="ENABLE", target_lag="1 hour")

        changeset = SnowflakeRelation.dynamic_table_config_changeset(
            relation_results, relation_config
        )

        assert changeset is not None
        assert changeset.scheduler is not None
        assert changeset.scheduler.context == "ENABLE"
        assert changeset.requires_full_refresh is False


class TestRefreshWarehouseOptional:
    """Tests to verify refresh_warehouse is an optional parameter."""

    def test_refresh_warehouse_is_optional(self):
        """refresh_warehouse should be optional and default to None."""
        config = SnowflakeDynamicTableConfig.from_dict(
            {
                "name": "test_table",
                "schema_name": "test_schema",
                "database_name": "test_db",
                "query": "SELECT 1",
                "target_lag": "1 hour",
                "snowflake_warehouse": "MY_EXECUTION_WH",
            }
        )
        assert config.refresh_warehouse is None

    def test_refresh_warehouse_can_be_set(self):
        """refresh_warehouse can be set independently of snowflake_warehouse."""
        config = SnowflakeDynamicTableConfig.from_dict(
            {
                "name": "test_table",
                "schema_name": "test_schema",
                "database_name": "test_db",
                "query": "SELECT 1",
                "target_lag": "1 hour",
                "snowflake_warehouse": "MY_LARGE_EXECUTION_WH",
                "refresh_warehouse": "MY_SMALL_REFRESH_WH",
            }
        )
        assert config.snowflake_warehouse == "MY_LARGE_EXECUTION_WH"
        assert config.refresh_warehouse == "MY_SMALL_REFRESH_WH"

    def test_warehouse_parameter_falls_back_to_snowflake_warehouse(self):
        """warehouse_parameter returns snowflake_warehouse when refresh_warehouse is not set."""
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
        assert config.warehouse_parameter == "MY_WH"

    def test_warehouse_parameter_uses_refresh_warehouse_when_set(self):
        """warehouse_parameter returns refresh_warehouse when it is set."""
        config = SnowflakeDynamicTableConfig.from_dict(
            {
                "name": "test_table",
                "schema_name": "test_schema",
                "database_name": "test_db",
                "query": "SELECT 1",
                "target_lag": "1 hour",
                "snowflake_warehouse": "MY_LARGE_EXECUTION_WH",
                "refresh_warehouse": "MY_SMALL_REFRESH_WH",
            }
        )
        assert config.warehouse_parameter == "MY_SMALL_REFRESH_WH"

    def test_refresh_warehouse_can_be_explicit_none(self):
        """refresh_warehouse can be explicitly set to None."""
        config = SnowflakeDynamicTableConfig.from_dict(
            {
                "name": "test_table",
                "schema_name": "test_schema",
                "database_name": "test_db",
                "query": "SELECT 1",
                "target_lag": "1 hour",
                "snowflake_warehouse": "MY_WH",
                "refresh_warehouse": None,
            }
        )
        assert config.refresh_warehouse is None
        assert config.warehouse_parameter == "MY_WH"


class TestRefreshWarehouseChangeset:
    """Tests for refresh_warehouse change detection via warehouse_parameter comparison."""

    def test_changeset_without_warehouse_change_has_no_changes(self):
        """A changeset with no warehouse change should not have changes."""
        changeset = SnowflakeDynamicTableConfigChangeset()
        assert changeset.snowflake_warehouse is None
        assert not changeset.has_changes
        assert not changeset.requires_full_refresh

    def test_changeset_with_warehouse_change_does_not_require_full_refresh(self):
        """A warehouse changeset entry should have changes but not require full refresh."""
        changeset = SnowflakeDynamicTableConfigChangeset(
            snowflake_warehouse=SnowflakeDynamicTableWarehouseConfigChange(
                action=RelationConfigChangeAction.alter,
                context="NEW_REFRESH_WH",
            )
        )
        assert changeset.snowflake_warehouse is not None
        assert changeset.has_changes
        assert not changeset.requires_full_refresh

    @staticmethod
    def _make_relation_results(warehouse="MY_WH"):
        import agate

        dt_row_data = {
            "name": "test_table",
            "schema_name": "test_schema",
            "database_name": "test_db",
            "text": "SELECT 1",
            "target_lag": "1 hour",
            "warehouse": warehouse,
            "refresh_mode": "AUTO",
            "immutable_where": None,
        }
        column_types = [agate.Text()] * len(dt_row_data)
        return {
            "dynamic_table": agate.Table(
                [list(dt_row_data.values())],
                list(dt_row_data.keys()),
                column_types,
            )
        }

    @staticmethod
    def _make_relation_config(snowflake_warehouse="MY_WH", refresh_warehouse=None):
        from unittest.mock import MagicMock

        relation_config = MagicMock()
        relation_config.identifier = "test_table"
        relation_config.schema = "test_schema"
        relation_config.database = "test_db"
        relation_config.compiled_code = "SELECT 1"
        extra = {
            "target_lag": "1 hour",
            "snowflake_warehouse": snowflake_warehouse,
        }
        if refresh_warehouse is not None:
            extra["refresh_warehouse"] = refresh_warehouse
        relation_config.config.extra = extra
        relation_config.config.get = lambda key, default=None: relation_config.config.extra.get(
            key, default
        )
        return relation_config

    def test_no_refresh_warehouse_no_change(self):
        """When refresh_warehouse is not set and the existing warehouse matches, no change."""
        from dbt.adapters.snowflake.relation import SnowflakeRelation

        relation_results = self._make_relation_results(warehouse="MY_WH")
        relation_config = self._make_relation_config(snowflake_warehouse="MY_WH")

        changeset = SnowflakeRelation.dynamic_table_config_changeset(
            relation_results, relation_config
        )

        if changeset is not None:
            assert changeset.snowflake_warehouse is None

    def test_refresh_warehouse_differs_from_existing_triggers_change(self):
        """Setting refresh_warehouse to a value different from the existing warehouse triggers a change."""
        from dbt.adapters.snowflake.relation import SnowflakeRelation

        # Existing table has MY_WH as its refresh warehouse
        relation_results = self._make_relation_results(warehouse="MY_WH")
        # User now wants MY_SMALL_REFRESH_WH as the refresh warehouse
        relation_config = self._make_relation_config(
            snowflake_warehouse="MY_LARGE_WH",
            refresh_warehouse="MY_SMALL_REFRESH_WH",
        )

        changeset = SnowflakeRelation.dynamic_table_config_changeset(
            relation_results, relation_config
        )

        assert changeset is not None
        assert changeset.snowflake_warehouse is not None
        assert changeset.snowflake_warehouse.context == "MY_SMALL_REFRESH_WH"
        assert changeset.has_changes
        assert not changeset.requires_full_refresh

    def test_refresh_warehouse_matches_existing_no_change(self):
        """When refresh_warehouse equals the existing warehouse, no change is detected."""
        from dbt.adapters.snowflake.relation import SnowflakeRelation

        relation_results = self._make_relation_results(warehouse="MY_SMALL_WH")
        relation_config = self._make_relation_config(
            snowflake_warehouse="MY_LARGE_WH",
            refresh_warehouse="MY_SMALL_WH",
        )

        changeset = SnowflakeRelation.dynamic_table_config_changeset(
            relation_results, relation_config
        )

        if changeset is not None:
            assert changeset.snowflake_warehouse is None
