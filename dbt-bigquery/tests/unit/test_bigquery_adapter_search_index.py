import unittest
from unittest.mock import MagicMock, patch

from dbt.adapters.bigquery import BigQueryAdapter, BigQueryRelation
from dbt.adapters.bigquery.relation_configs import (
    BigQuerySearchIndexConfig,
    BigQuerySearchIndexConfigChange,
)
from dbt.adapters.contracts.relation import RelationConfig, RelationType
from dbt.adapters.relation_configs import RelationConfigChangeAction

from .utils import config_from_parts_or_dicts, inject_adapter


class TestBigQueryAdapterSearchIndex(unittest.TestCase):
    def setUp(self):
        self.raw_profile = {
            "outputs": {
                "oauth": {
                    "type": "bigquery",
                    "method": "oauth",
                    "project": "dbt-unit-000000",
                    "schema": "dummy_schema",
                    "threads": 1,
                }
            },
            "target": "oauth",
        }
        self.project_cfg = {
            "name": "X",
            "version": "0.1",
            "project-root": "/tmp/dbt/does-not-exist",
            "profile": "default",
            "config-version": 2,
        }
        config = config_from_parts_or_dicts(self.project_cfg, self.raw_profile)
        self.adapter = BigQueryAdapter(config, MagicMock())
        inject_adapter(self.adapter)

    def test_search_index_config_changeset_create(self):
        relation = BigQueryRelation.create(database="db", schema="schema", identifier="ident")

        # Scenario: No existing index, new config wants one
        existing_config = None
        relation_config = MagicMock()
        relation_config.config.extra = {"search_index": True}

        changeset = relation.search_index_config_changeset(existing_config, relation_config)

        self.assertIsNotNone(changeset)
        self.assertEqual(changeset.action, RelationConfigChangeAction.alter)
        self.assertEqual(changeset.context.columns, frozenset(["ALL COLUMNS"]))

    def test_search_index_config_changeset_no_change(self):
        relation = BigQueryRelation.create(database="db", schema="schema", identifier="ident")

        # Scenario: Existing index matches new config
        existing_config = BigQuerySearchIndexConfig(columns=frozenset(["ALL COLUMNS"]))
        relation_config = MagicMock()
        relation_config.config.extra = {"search_index": True}

        changeset = relation.search_index_config_changeset(existing_config, relation_config)

        self.assertIsNone(changeset)

    @patch.object(BigQueryAdapter, "describe_search_index")
    @patch.object(BigQueryAdapter, "drop_search_index")
    @patch.object(BigQueryAdapter, "create_search_index")
    def test_manage_search_index_create(self, mock_create, mock_drop, mock_describe):
        relation = BigQueryRelation.create(database="db", schema="schema", identifier="ident")

        # Scenario: No existing index
        mock_describe.return_value = None

        # New config wants index
        relation_config = MagicMock()
        relation_config.config.extra = {"search_index": True}

        self.adapter.manage_search_index(relation, relation_config)

        # Verify DROP then CREATE are called
        mock_drop.assert_called_once_with(relation)
        mock_create.assert_called_once()
        self.assertEqual(mock_create.call_args[0][0], relation)
        self.assertEqual(mock_create.call_args[0][1].columns, frozenset(["ALL COLUMNS"]))

    @patch.object(BigQueryAdapter, "describe_search_index")
    @patch.object(BigQueryAdapter, "drop_search_index")
    @patch.object(BigQueryAdapter, "create_search_index")
    def test_manage_search_index_no_change(self, mock_create, mock_drop, mock_describe):
        relation = BigQueryRelation.create(database="db", schema="schema", identifier="ident")

        # Scenario: Existing index matches
        mock_describe.return_value = BigQuerySearchIndexConfig(columns=frozenset(["ALL COLUMNS"]))

        relation_config = MagicMock()
        relation_config.config.extra = {"search_index": True}

        self.adapter.manage_search_index(relation, relation_config)

        # Verify NO drop/create calls
        self.assertFalse(mock_drop.called)
        self.assertFalse(mock_create.called)

    @patch.object(BigQueryAdapter, "describe_search_index")
    @patch.object(BigQueryAdapter, "drop_search_index")
    @patch.object(BigQueryAdapter, "create_search_index")
    def test_manage_search_index_drop_only(self, mock_create, mock_drop, mock_describe):
        relation = BigQueryRelation.create(database="db", schema="schema", identifier="ident")

        # Scenario: Existing index, but new config is False
        mock_describe.return_value = BigQuerySearchIndexConfig(columns=frozenset(["ALL COLUMNS"]))

        relation_config = MagicMock()
        relation_config.config.extra = {"search_index": False}

        self.adapter.manage_search_index(relation, relation_config)

        # Verify DROP is called, but not CREATE
        mock_drop.assert_called_once_with(relation)
        self.assertFalse(mock_create.called)
