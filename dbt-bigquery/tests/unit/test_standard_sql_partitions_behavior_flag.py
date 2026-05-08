from types import SimpleNamespace
from unittest.mock import Mock, patch, MagicMock

from dbt.adapters.bigquery.connections import BigQueryConnectionManager


class TestStandardSqlPartitionsConnectionManager:
    """Test that the connection manager uses the correct SQL based on use_standard_sql_for_partitions."""

    def _make_connection_manager(self, use_standard_sql):
        cm = Mock(spec=BigQueryConnectionManager)
        cm.use_standard_sql_for_partitions = use_standard_sql
        cm.get_partitions_metadata = BigQueryConnectionManager.get_partitions_metadata.__get__(cm)
        cm._add_query_comment = lambda sql: sql
        cm.get_table_from_response = Mock(return_value=Mock())

        mock_field = Mock()
        mock_field.name = "partition_id"
        mock_iterator = Mock()
        mock_iterator.schema = [mock_field]
        mock_iterator.__iter__ = Mock(return_value=iter([]))
        cm.raw_execute = Mock(return_value=(Mock(), mock_iterator))

        return cm

    def test_uses_standard_sql_when_flag_enabled(self):
        cm = self._make_connection_manager(use_standard_sql=True)
        table = SimpleNamespace(project="proj", dataset="ds", identifier="tbl")

        cm.get_partitions_metadata(table=table)

        sql = cm.raw_execute.call_args[0][0]
        assert "INFORMATION_SCHEMA.PARTITIONS" in sql
        assert cm.raw_execute.call_args[1]["use_legacy_sql"] is False

    def test_uses_legacy_sql_when_flag_disabled(self):
        cm = self._make_connection_manager(use_standard_sql=False)
        table = SimpleNamespace(project="proj", dataset="ds", identifier="tbl")

        cm.get_partitions_metadata(table=table)

        sql = cm.raw_execute.call_args[0][0]
        assert "__PARTITIONS_SUMMARY__" in sql
        assert cm.raw_execute.call_args[1]["use_legacy_sql"] is True
