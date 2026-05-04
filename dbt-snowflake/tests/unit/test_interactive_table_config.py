"""
Unit tests for SnowflakeInteractiveTableConfig, testing:
- cluster_by (required)
- target_lag (optional)
- snowflake_warehouse (optional, required when target_lag is set)
- config changeset detection
"""

import pytest

from dbt.adapters.relation_configs import RelationConfigChangeAction
from dbt.adapters.snowflake.relation_configs import (
    SnowflakeInteractiveTableConfig,
    SnowflakeInteractiveTableConfigChangeset,
    SnowflakeInteractiveTableClusterByConfigChange,
    SnowflakeInteractiveTableTargetLagConfigChange,
    SnowflakeInteractiveTableWarehouseConfigChange,
)
from dbt_common.exceptions import CompilationError


class TestSnowflakeInteractiveTableConfig:
    """Tests for SnowflakeInteractiveTableConfig."""

    def test_static_interactive_table(self):
        """A static interactive table only requires cluster_by."""
        config = SnowflakeInteractiveTableConfig.from_dict(
            {
                "name": "test_table",
                "schema_name": "test_schema",
                "database_name": "test_db",
                "query": "SELECT 1",
                "cluster_by": "id",
            }
        )

        assert config.name == "test_table"
        assert config.cluster_by == "id"
        assert config.target_lag is None
        assert config.snowflake_warehouse is None

    def test_dynamic_interactive_table(self):
        """A dynamic interactive table has target_lag and warehouse."""
        config = SnowflakeInteractiveTableConfig.from_dict(
            {
                "name": "test_table",
                "schema_name": "test_schema",
                "database_name": "test_db",
                "query": "SELECT 1",
                "cluster_by": "region, product_id",
                "target_lag": "5 minutes",
                "snowflake_warehouse": "REFRESH_WH",
            }
        )

        assert config.cluster_by == "region, product_id"
        assert config.target_lag == "5 minutes"
        assert config.snowflake_warehouse == "REFRESH_WH"

    def test_target_lag_is_optional(self):
        """target_lag should be optional and default to None."""
        config = SnowflakeInteractiveTableConfig.from_dict(
            {
                "name": "test_table",
                "schema_name": "test_schema",
                "database_name": "test_db",
                "query": "SELECT 1",
                "cluster_by": "id",
            }
        )
        assert config.target_lag is None

    def test_snowflake_warehouse_is_optional(self):
        """snowflake_warehouse should be optional and default to None."""
        config = SnowflakeInteractiveTableConfig.from_dict(
            {
                "name": "test_table",
                "schema_name": "test_schema",
                "database_name": "test_db",
                "query": "SELECT 1",
                "cluster_by": "id",
            }
        )
        assert config.snowflake_warehouse is None


class TestSnowflakeInteractiveTableConfigValidation:
    """Tests for compile-time validation of interactive table configs."""

    @staticmethod
    def _make_relation_config(cluster_by=None, target_lag=None, snowflake_warehouse=None):
        from unittest.mock import MagicMock

        relation_config = MagicMock()
        relation_config.identifier = "test_table"
        relation_config.schema = "test_schema"
        relation_config.database = "test_db"
        relation_config.compiled_code = "SELECT 1"
        extra = {}
        if target_lag is not None:
            extra["target_lag"] = target_lag
        if snowflake_warehouse is not None:
            extra["snowflake_warehouse"] = snowflake_warehouse
        relation_config.config.extra = extra
        relation_config.config.get = lambda key, default=None: extra.get(key, default)
        if cluster_by is not None:
            relation_config.config.get = lambda key, default=None: (
                cluster_by if key == "cluster_by" else extra.get(key, default)
            )
        return relation_config

    def test_cluster_by_required(self):
        """cluster_by is required for interactive tables."""
        relation_config = self._make_relation_config(cluster_by=None)

        with pytest.raises(CompilationError, match="cluster_by"):
            SnowflakeInteractiveTableConfig.from_relation_config(relation_config)

    def test_target_lag_requires_warehouse(self):
        """Setting target_lag without snowflake_warehouse should raise an error."""
        relation_config = self._make_relation_config(
            cluster_by="id", target_lag="5 minutes", snowflake_warehouse=None
        )

        with pytest.raises(CompilationError, match="snowflake_warehouse"):
            SnowflakeInteractiveTableConfig.from_relation_config(relation_config)

    def test_valid_static_config(self):
        """A valid static config should parse successfully."""
        relation_config = self._make_relation_config(cluster_by="id")

        config = SnowflakeInteractiveTableConfig.from_relation_config(relation_config)
        assert config.cluster_by == "id"
        assert config.target_lag is None

    def test_valid_dynamic_config(self):
        """A valid dynamic config with target_lag and warehouse should parse successfully."""
        relation_config = self._make_relation_config(
            cluster_by="region", target_lag="10 minutes", snowflake_warehouse="MY_WH"
        )

        config = SnowflakeInteractiveTableConfig.from_relation_config(relation_config)
        assert config.cluster_by == "region"
        assert config.target_lag == "10 minutes"
        assert config.snowflake_warehouse == "MY_WH"


class TestSnowflakeInteractiveTableConfigChangeset:
    """Tests for SnowflakeInteractiveTableConfigChangeset."""

    def test_empty_changeset_has_no_changes(self):
        """An empty changeset should report no changes."""
        changeset = SnowflakeInteractiveTableConfigChangeset()

        assert not changeset.has_changes
        assert not changeset.requires_full_refresh

    def test_cluster_by_change_requires_full_refresh(self):
        """Cluster by changes require full refresh (no ALTER support)."""
        changeset = SnowflakeInteractiveTableConfigChangeset(
            cluster_by=SnowflakeInteractiveTableClusterByConfigChange(
                action=RelationConfigChangeAction.create,
                context="new_col",
            )
        )

        assert changeset.has_changes
        assert changeset.requires_full_refresh

    def test_target_lag_change_requires_full_refresh(self):
        """Target lag changes require full refresh (no ALTER support)."""
        changeset = SnowflakeInteractiveTableConfigChangeset(
            target_lag=SnowflakeInteractiveTableTargetLagConfigChange(
                action=RelationConfigChangeAction.create,
                context="10 minutes",
            )
        )

        assert changeset.has_changes
        assert changeset.requires_full_refresh

    def test_warehouse_change_requires_full_refresh(self):
        """Warehouse changes require full refresh (no ALTER support)."""
        changeset = SnowflakeInteractiveTableConfigChangeset(
            snowflake_warehouse=SnowflakeInteractiveTableWarehouseConfigChange(
                action=RelationConfigChangeAction.create,
                context="NEW_WH",
            )
        )

        assert changeset.has_changes
        assert changeset.requires_full_refresh

    def test_all_changes_require_full_refresh(self):
        """All config change types require full refresh for interactive tables."""
        cluster_change = SnowflakeInteractiveTableClusterByConfigChange(
            action=RelationConfigChangeAction.create,
            context="col1",
        )
        assert cluster_change.requires_full_refresh

        lag_change = SnowflakeInteractiveTableTargetLagConfigChange(
            action=RelationConfigChangeAction.create,
            context="5 minutes",
        )
        assert lag_change.requires_full_refresh

        wh_change = SnowflakeInteractiveTableWarehouseConfigChange(
            action=RelationConfigChangeAction.create,
            context="WH",
        )
        assert wh_change.requires_full_refresh


class TestInteractiveTableChangeDetectionLogic:
    """Tests for change detection in SnowflakeRelation.interactive_table_config_changeset()."""

    @staticmethod
    def _make_relation_results(cluster_by="id", target_lag=None, warehouse=None):
        import agate

        row_data = {
            "name": "test_table",
            "schema_name": "test_schema",
            "database_name": "test_db",
            "text": "SELECT 1",
            "cluster_by": cluster_by,
            "target_lag": target_lag,
            "warehouse": warehouse,
        }
        column_types = [agate.Text()] * len(row_data)

        return {
            "interactive_table": agate.Table(
                [list(row_data.values())],
                list(row_data.keys()),
                column_types,
            )
        }

    @staticmethod
    def _make_relation_config(cluster_by="id", target_lag=None, snowflake_warehouse=None):
        from unittest.mock import MagicMock

        relation_config = MagicMock()
        relation_config.identifier = "test_table"
        relation_config.schema = "test_schema"
        relation_config.database = "test_db"
        relation_config.compiled_code = "SELECT 1"
        extra = {}
        if target_lag is not None:
            extra["target_lag"] = target_lag
        if snowflake_warehouse is not None:
            extra["snowflake_warehouse"] = snowflake_warehouse
        relation_config.config.extra = extra
        relation_config.config.get = lambda key, default=None: (
            cluster_by if key == "cluster_by" else extra.get(key, default)
        )
        return relation_config

    def test_no_changes_returns_none(self):
        """When config matches existing, no changeset is returned."""
        from dbt.adapters.snowflake.relation import SnowflakeRelation

        relation_results = self._make_relation_results(cluster_by="id")
        relation_config = self._make_relation_config(cluster_by="id")

        changeset = SnowflakeRelation.interactive_table_config_changeset(
            relation_results, relation_config
        )

        assert changeset is None

    def test_cluster_by_change_detected(self):
        """When cluster_by changes, a changeset is returned."""
        from dbt.adapters.snowflake.relation import SnowflakeRelation

        relation_results = self._make_relation_results(cluster_by="id")
        relation_config = self._make_relation_config(cluster_by="region")

        changeset = SnowflakeRelation.interactive_table_config_changeset(
            relation_results, relation_config
        )

        assert changeset is not None
        assert changeset.cluster_by is not None
        assert changeset.cluster_by.context == "region"
        assert changeset.requires_full_refresh

    def test_target_lag_change_detected(self):
        """When target_lag changes, a changeset is returned."""
        from dbt.adapters.snowflake.relation import SnowflakeRelation

        relation_results = self._make_relation_results(
            cluster_by="id", target_lag="5 minutes", warehouse="MY_WH"
        )
        relation_config = self._make_relation_config(
            cluster_by="id", target_lag="10 minutes", snowflake_warehouse="MY_WH"
        )

        changeset = SnowflakeRelation.interactive_table_config_changeset(
            relation_results, relation_config
        )

        assert changeset is not None
        assert changeset.target_lag is not None
        assert changeset.target_lag.context == "10 minutes"

    def test_target_lag_removal_detected(self):
        """When target_lag is removed (dynamic -> static), a changeset is returned."""
        from dbt.adapters.snowflake.relation import SnowflakeRelation

        relation_results = self._make_relation_results(
            cluster_by="id", target_lag="5 minutes", warehouse="MY_WH"
        )
        relation_config = self._make_relation_config(
            cluster_by="id", target_lag=None, snowflake_warehouse=None
        )

        changeset = SnowflakeRelation.interactive_table_config_changeset(
            relation_results, relation_config
        )

        assert changeset is not None
        assert changeset.target_lag is not None
        assert changeset.target_lag.context is None
        assert changeset.requires_full_refresh

    def test_warehouse_change_detected(self):
        """When warehouse changes, a changeset is returned."""
        from dbt.adapters.snowflake.relation import SnowflakeRelation

        relation_results = self._make_relation_results(
            cluster_by="id", target_lag="5 minutes", warehouse="OLD_WH"
        )
        relation_config = self._make_relation_config(
            cluster_by="id", target_lag="5 minutes", snowflake_warehouse="NEW_WH"
        )

        changeset = SnowflakeRelation.interactive_table_config_changeset(
            relation_results, relation_config
        )

        assert changeset is not None
        assert changeset.snowflake_warehouse is not None
        assert changeset.snowflake_warehouse.context == "NEW_WH"


class TestInteractiveTableParseRelationResults:
    """Tests for parsing Snowflake SHOW TABLES results into config."""

    @staticmethod
    def _make_agate_results(
        name="test_table",
        schema_name="test_schema",
        database_name="test_db",
        text="SELECT 1",
        cluster_by="id",
        target_lag=None,
        warehouse=None,
    ):
        import agate

        row_data = {
            "name": name,
            "schema_name": schema_name,
            "database_name": database_name,
            "text": text,
            "cluster_by": cluster_by,
            "target_lag": target_lag,
            "warehouse": warehouse,
        }
        column_types = [agate.Text()] * len(row_data)

        return {
            "interactive_table": agate.Table(
                [list(row_data.values())],
                list(row_data.keys()),
                column_types,
            )
        }

    def test_parse_static_interactive_table(self):
        """Parsing a static interactive table result."""
        results = self._make_agate_results(cluster_by="customer_id")
        config = SnowflakeInteractiveTableConfig.from_relation_results(results)

        assert config.name == "test_table"
        assert config.cluster_by == "customer_id"
        assert config.target_lag is None
        assert config.snowflake_warehouse is None

    def test_parse_dynamic_interactive_table(self):
        """Parsing a dynamic interactive table result."""
        results = self._make_agate_results(
            cluster_by="region",
            target_lag="5 minutes",
            warehouse="REFRESH_WH",
        )
        config = SnowflakeInteractiveTableConfig.from_relation_results(results)

        assert config.cluster_by == "region"
        assert config.target_lag == "5 minutes"
        assert config.snowflake_warehouse == "REFRESH_WH"

    def test_parse_multi_column_cluster_by(self):
        """Multi-column cluster_by values are preserved."""
        results = self._make_agate_results(cluster_by="region, product_id")
        config = SnowflakeInteractiveTableConfig.from_relation_results(results)
        assert config.cluster_by == "region, product_id"
