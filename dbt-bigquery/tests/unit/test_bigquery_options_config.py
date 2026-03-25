import unittest

from dbt.adapters.bigquery.relation_configs._options import BigQueryOptionsConfig


class TestBigQueryOptionsConfig(unittest.TestCase):
    """Unit tests for BigQuery options config with tags"""

    def test_from_dict_with_tags(self):
        """Test creating BigQueryOptionsConfig from dict with tags"""
        config_dict = {
            "enable_refresh": True,
            "refresh_interval_minutes": 45,
            "description": "Test materialized view",
            "labels": {"env": "dev", "version": "1.0"},
            "tags": {"test-project/team": "data", "test-project/cost_center": "analytics"},
        }

        options = BigQueryOptionsConfig.from_dict(config_dict)

        # Verify all fields are correctly set
        self.assertTrue(options.enable_refresh)
        self.assertEqual(options.refresh_interval_minutes, 45)
        self.assertEqual(options.description, "Test materialized view")
        self.assertEqual(options.labels, {"env": "dev", "version": "1.0"})
        self.assertEqual(
            options.tags, {"test-project/team": "data", "test-project/cost_center": "analytics"}
        )

    def test_as_ddl_dict_with_tags(self):
        """Test generating DDL dict with tags"""
        options = BigQueryOptionsConfig(
            enable_refresh=True,
            refresh_interval_minutes=30,
            description="Test MV",
            labels={"env": "test"},
            tags={"test-project/team": "data"},
        )

        ddl_dict = options.as_ddl_dict()

        # Verify the output format
        self.assertTrue(ddl_dict["enable_refresh"])
        self.assertEqual(ddl_dict["refresh_interval_minutes"], 30)
        self.assertEqual(ddl_dict["description"], '"""Test MV"""')

        # Check labels and tags are properly formatted as arrays of tuples
        expected_labels = [("env", "test")]
        expected_tags = [("test-project/team", "data")]

        self.assertEqual(ddl_dict["labels"], expected_labels)
        self.assertEqual(ddl_dict["tags"], expected_tags)

    def test_tags_none_not_included_in_ddl(self):
        """Test that None tags are not included in DDL dict"""
        options = BigQueryOptionsConfig(enable_refresh=True, tags=None)

        ddl_dict = options.as_ddl_dict()

        # tags should not be in the output when None
        self.assertNotIn("tags", ddl_dict)
        self.assertIn("enable_refresh", ddl_dict)

    def test_parse_relation_config_with_resource_tags(self):
        """Test parse_relation_config method handles resource_tags correctly"""
        from unittest.mock import Mock

        # Mock relation config with resource_tags
        mock_relation_config = Mock()
        mock_config = Mock()
        mock_config.extra = {
            "enable_refresh": True,
            "refresh_interval_minutes": 30,
            "description": "Test view",
            "labels": {"env": "test"},
            "resource_tags": {"test-project/team": "data", "test-project/project": "analytics"},
        }
        mock_config.persist_docs = True
        mock_relation_config.config = mock_config

        config_dict = BigQueryOptionsConfig.parse_relation_config(mock_relation_config)

        # Verify resource_tags gets mapped to tags
        self.assertEqual(
            config_dict["tags"], {"test-project/team": "data", "test-project/project": "analytics"}
        )
        self.assertEqual(config_dict["labels"], {"env": "test"})
        self.assertEqual(config_dict["description"], "Test view")
        self.assertTrue(config_dict["enable_refresh"])

    def test_parse_relation_config_without_resource_tags(self):
        """Test parse_relation_config when resource_tags is not present"""
        from unittest.mock import Mock

        # Mock relation config without resource_tags
        mock_relation_config = Mock()
        mock_config = Mock()
        mock_config.extra = {"enable_refresh": False, "labels": {"env": "prod"}}
        mock_config.persist_docs = True
        mock_relation_config.config = mock_config

        config_dict = BigQueryOptionsConfig.parse_relation_config(mock_relation_config)

        # Verify tags is not in the config_dict when resource_tags is not present
        self.assertNotIn("tags", config_dict)
        self.assertEqual(config_dict["labels"], {"env": "prod"})
        self.assertFalse(config_dict["enable_refresh"])

    def test_expiration_timestamp_formatted_as_bq_literal(self):
        """Test that expiration_timestamp is rendered as a BigQuery TIMESTAMP literal, not a raw datetime."""
        from datetime import datetime

        dt = datetime(2025, 7, 18, 18, 56, 22, 153305)
        options = BigQueryOptionsConfig(expiration_timestamp=dt)

        ddl_dict = options.as_ddl_dict()

        self.assertEqual(
            ddl_dict["expiration_timestamp"],
            "TIMESTAMP '2025-07-18 18:56:22.153305'",
        )
        # Must not be a raw datetime object (which would cause a BigQuery syntax error)
        self.assertIsInstance(ddl_dict["expiration_timestamp"], str)

    def test_expiration_timestamp_string_expression_preserved(self):
        """Test that a SQL string expression for expiration_timestamp is passed through unchanged."""
        sql_expr = "TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 4 HOUR)"
        options = BigQueryOptionsConfig(expiration_timestamp=sql_expr)

        ddl_dict = options.as_ddl_dict()

        self.assertIn("expiration_timestamp", ddl_dict)
        self.assertEqual(ddl_dict["expiration_timestamp"], sql_expr)

    def test_expiration_timestamp_tz_aware_normalized_to_utc(self):
        """Test that a tz-aware datetime is normalized to UTC before formatting."""
        from datetime import datetime, timezone, timedelta

        # +02:00 offset — 2025-07-18 20:56:22 local == 2025-07-18 18:56:22 UTC
        tz_plus2 = timezone(timedelta(hours=2))
        dt_aware = datetime(2025, 7, 18, 20, 56, 22, 153305, tzinfo=tz_plus2)
        options = BigQueryOptionsConfig(expiration_timestamp=dt_aware)

        ddl_dict = options.as_ddl_dict()

        self.assertEqual(
            ddl_dict["expiration_timestamp"],
            "TIMESTAMP '2025-07-18 18:56:22.153305'",
        )


if __name__ == "__main__":
    unittest.main()
