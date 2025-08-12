import unittest
from unittest.mock import MagicMock, patch

from dbt.adapters.bigquery.relation import BigQueryRelation
from dbt.adapters.bigquery.relation_configs import (
    BigQueryMaterializedViewConfig,
    BigQueryOptionsConfig,
)
from dbt.adapters.contracts.relation import RelationConfig


class TestBigQueryRelationConfigs(unittest.TestCase):
    def test_materialized_view_config_changeset_with_resource_tags(self):
        """Test that materialized view config changeset handles resource_tags safely."""

        # Create mock existing materialized view config
        existing_options = BigQueryOptionsConfig(
            enable_refresh=True,
            refresh_interval_minutes=30,
            labels={"env": "prod"},
            tags=None,  # No tags initially
        )
        existing_mv_config = BigQueryMaterializedViewConfig(
            table_id="test_table",
            dataset_id="test_dataset",
            project_id="test_project",
            options=existing_options,
        )

        # Create mock relation config with resource_tags
        relation_config = MagicMock(spec=RelationConfig)
        relation_config.identifier = "test_table"
        relation_config.schema = "test_dataset"
        relation_config.database = "test_project"
        relation_config.config = {
            "enable_refresh": True,
            "refresh_interval_minutes": 30,
            "labels": {"env": "prod"},
            "resource_tags": {
                "test-project/team": "data",
                "test-project/owner": "analytics",
            },  # New tags
        }

        # Create new materialized view config with tags
        new_options = BigQueryOptionsConfig(
            enable_refresh=True,
            refresh_interval_minutes=30,
            labels={"env": "prod"},
            tags={
                "test-project/team": "data",
                "test-project/owner": "analytics",
            },  # New tags added
        )
        new_mv_config = BigQueryMaterializedViewConfig(
            table_id="test_table",
            dataset_id="test_dataset",
            project_id="test_project",
            options=new_options,
        )

        # Mock the materialized_view_from_relation_config method
        with patch.object(
            BigQueryRelation, "materialized_view_from_relation_config", return_value=new_mv_config
        ):
            # This should not raise an "unhashable type: 'dict'" error
            changeset = BigQueryRelation.materialized_view_config_changeset(
                existing_mv_config, relation_config
            )

            # Should detect changes due to different tags
            self.assertIsNotNone(changeset)
            self.assertIsNotNone(changeset.options)

    def test_materialized_view_config_changeset_no_changes(self):
        """Test that no changeset is returned when configs are identical."""

        # Create identical configs
        options = BigQueryOptionsConfig(
            enable_refresh=True,
            refresh_interval_minutes=30,
            labels={"env": "prod"},
            tags={"test-project/team": "data"},
        )
        mv_config = BigQueryMaterializedViewConfig(
            table_id="test_table",
            dataset_id="test_dataset",
            project_id="test_project",
            options=options,
        )

        relation_config = MagicMock(spec=RelationConfig)

        # Mock the materialized_view_from_relation_config method to return identical config
        with patch.object(
            BigQueryRelation, "materialized_view_from_relation_config", return_value=mv_config
        ):
            changeset = BigQueryRelation.materialized_view_config_changeset(
                mv_config, relation_config
            )

            # Should return None when no changes detected
            self.assertIsNone(changeset)

    def test_materialized_view_config_changeset_tags_none_to_dict(self):
        """Test changeset detection when tags change from None to dict."""

        # Existing config with no tags
        existing_options = BigQueryOptionsConfig(
            enable_refresh=True, refresh_interval_minutes=30, tags=None
        )
        existing_mv_config = BigQueryMaterializedViewConfig(
            table_id="test_table",
            dataset_id="test_dataset",
            project_id="test_project",
            options=existing_options,
        )

        # New config with tags
        new_options = BigQueryOptionsConfig(
            enable_refresh=True, refresh_interval_minutes=30, tags={"test-project/env": "dev"}
        )
        new_mv_config = BigQueryMaterializedViewConfig(
            table_id="test_table",
            dataset_id="test_dataset",
            project_id="test_project",
            options=new_options,
        )

        relation_config = MagicMock(spec=RelationConfig)

        with patch.object(
            BigQueryRelation, "materialized_view_from_relation_config", return_value=new_mv_config
        ):
            changeset = BigQueryRelation.materialized_view_config_changeset(
                existing_mv_config, relation_config
            )

            # Should detect changes when tags are added
            self.assertIsNotNone(changeset)
            self.assertIsNotNone(changeset.options)

    def test_materialized_view_config_changeset_tags_dict_to_none(self):
        """Test changeset detection when tags change from dict to None."""

        # Existing config with tags
        existing_options = BigQueryOptionsConfig(
            enable_refresh=True, refresh_interval_minutes=30, tags={"test-project/env": "dev"}
        )
        existing_mv_config = BigQueryMaterializedViewConfig(
            table_id="test_table",
            dataset_id="test_dataset",
            project_id="test_project",
            options=existing_options,
        )

        # New config with no tags
        new_options = BigQueryOptionsConfig(
            enable_refresh=True, refresh_interval_minutes=30, tags=None
        )
        new_mv_config = BigQueryMaterializedViewConfig(
            table_id="test_table",
            dataset_id="test_dataset",
            project_id="test_project",
            options=new_options,
        )

        relation_config = MagicMock(spec=RelationConfig)

        with patch.object(
            BigQueryRelation, "materialized_view_from_relation_config", return_value=new_mv_config
        ):
            changeset = BigQueryRelation.materialized_view_config_changeset(
                existing_mv_config, relation_config
            )

            # Should detect changes when tags are removed
            self.assertIsNotNone(changeset)
            self.assertIsNotNone(changeset.options)
