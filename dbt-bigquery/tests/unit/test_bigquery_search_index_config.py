import unittest
from unittest.mock import MagicMock

from dbt.adapters.bigquery.relation_configs import (
    BigQuerySearchIndexConfig,
    BigQuerySearchIndexConfigChange,
)
from dbt.adapters.relation_configs import RelationConfigChangeAction


class TestBigQuerySearchIndexConfig(unittest.TestCase):
    def test_from_dict_defaults(self):
        config_dict = {"columns": ["ALL COLUMNS"]}
        config = BigQuerySearchIndexConfig.from_dict(config_dict)

        self.assertEqual(config.columns, frozenset(["ALL COLUMNS"]))
        self.assertEqual(config.analyzer, "LOG_ANALYZER")
        self.assertEqual(config.data_types, frozenset(["STRING"]))
        self.assertIsNone(config.name)
        self.assertIsNone(config.analyzer_options)
        self.assertIsNone(config.default_index_column_granularity)
        self.assertEqual(config.column_options, {})

    def test_from_dict_full(self):
        config_dict = {
            "columns": ["col1", "col2"],
            "name": "my_idx",
            "analyzer": "NO_OP_ANALYZER",
            "analyzer_options": '{"option": "val"}',
            "data_types": ["STRING", "INT64"],
            "default_index_column_granularity": "COLUMN",
            "column_options": {"col1": {"index_granularity": "GLOBAL"}},
        }
        config = BigQuerySearchIndexConfig.from_dict(config_dict)

        self.assertEqual(config.columns, frozenset(["col1", "col2"]))
        self.assertEqual(config.name, "my_idx")
        self.assertEqual(config.analyzer, "NO_OP_ANALYZER")
        self.assertEqual(config.analyzer_options, '{"option": "val"}')
        self.assertEqual(config.data_types, frozenset(["STRING", "INT64"]))
        self.assertEqual(config.default_index_column_granularity, "COLUMN")
        self.assertEqual(config.column_options, {"col1": {"index_granularity": "GLOBAL"}})

    def test_parse_relation_config_bool_true(self):
        relation_config = MagicMock()
        relation_config.config.extra = {"search_index": True}

        config_dict = BigQuerySearchIndexConfig.parse_relation_config(relation_config)
        self.assertEqual(config_dict, {"columns": ["ALL COLUMNS"]})

    def test_parse_relation_config_dict(self):
        relation_config = MagicMock()
        relation_config.config.extra = {
            "search_index": {
                "columns": ["col1"],
                "analyzer": "pattern_analyzer",
                "data_types": ["string"],
            }
        }

        config_dict = BigQuerySearchIndexConfig.parse_relation_config(relation_config)
        self.assertEqual(config_dict["columns"], ["col1"])
        self.assertEqual(config_dict["analyzer"], "PATTERN_ANALYZER")
        self.assertEqual(config_dict["data_types"], ["STRING"])

    def test_from_bq_results(self):
        index_row = {"index_name": "my_idx", "analyzer": "LOG_ANALYZER"}
        columns_rows = [
            {"index_column_name": "col1"},
            {"index_column_name": "col2"},
        ]
        options_rows = [
            {
                "index_column_name": None,
                "option_name": "data_types",
                "option_value": "['STRING', 'INT64']",
            },
            {
                "index_column_name": "col1",
                "option_name": "index_granularity",
                "option_value": "GLOBAL",
            },
        ]

        config = BigQuerySearchIndexConfig.from_bq_results(index_row, columns_rows, options_rows)

        self.assertIsNotNone(config)
        self.assertEqual(config.name, "my_idx")
        self.assertEqual(config.columns, frozenset(["col1", "col2"]))
        self.assertEqual(config.data_types, frozenset(["STRING", "INT64"]))
        self.assertEqual(config.column_options, {"col1": {"index_granularity": "GLOBAL"}})

    def test_config_change_detection(self):
        config_old = BigQuerySearchIndexConfig(
            columns=frozenset(["col1"]), analyzer="LOG_ANALYZER"
        )
        config_new = BigQuerySearchIndexConfig(
            columns=frozenset(["col1"]), analyzer="NO_OP_ANALYZER"
        )

        self.assertNotEqual(config_old, config_new)

        change = BigQuerySearchIndexConfigChange(
            action=RelationConfigChangeAction.alter, context=config_new
        )
        self.assertFalse(change.requires_full_refresh)
        self.assertEqual(change.context, config_new)

    def test_from_bq_results_all_columns(self):
        # BigQuery returns 'all_columns' (lowercase, underscore) in some views
        index_row = {"index_name": "my_idx", "analyzer": "LOG_ANALYZER"}
        columns_rows = [{"index_column_name": "all_columns"}]
        options_rows = []

        config = BigQuerySearchIndexConfig.from_bq_results(index_row, columns_rows, options_rows)

        self.assertIsNotNone(config)
        self.assertEqual(config.columns, frozenset(["ALL COLUMNS"]))

    def test_from_bq_results_analyzer_options_prioritization(self):
        # index_row has analyzer_options
        index_row = {
            "index_name": "my_idx",
            "analyzer": "LOG_ANALYZER",
            "analyzer_options": '{"source": "index_row"}',
        }
        columns_rows = [{"index_column_name": "col1"}]
        # options_rows also has analyzer_options (duplicate case)
        options_rows = [
            {
                "index_column_name": None,
                "option_name": "analyzer_options",
                "option_value": '{"source": "options_rows"}',
            }
        ]

        config = BigQuerySearchIndexConfig.from_bq_results(index_row, columns_rows, options_rows)

        self.assertIsNotNone(config)
        self.assertEqual(config.analyzer_options, '{"source": "index_row"}')
