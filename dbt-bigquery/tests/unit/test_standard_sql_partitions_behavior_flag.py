from types import SimpleNamespace
from unittest.mock import Mock, patch

from dbt.adapters.bigquery.impl import BigQueryAdapter


def _make_adapter_stub(flag_enabled: bool):
    """Create a minimal stub with the behavior flag and a mock connection manager."""
    mock_connections = Mock()
    mock_connections.get_partitions_metadata = Mock(return_value=Mock())
    return SimpleNamespace(
        behavior=SimpleNamespace(
            bigquery_use_standard_sql_for_partitions=SimpleNamespace(no_warn=flag_enabled),
        ),
        connections=mock_connections,
    )


class TestStandardSqlPartitionsBehaviorFlag:
    def test_uses_standard_sql_when_flag_enabled(self):
        stub = _make_adapter_stub(flag_enabled=True)
        table = Mock()

        BigQueryAdapter.get_partitions_metadata(stub, table)

        stub.connections.get_partitions_metadata.assert_called_once_with(
            table=table, use_standard_sql=True
        )

    def test_uses_legacy_sql_when_flag_disabled(self):
        stub = _make_adapter_stub(flag_enabled=False)
        table = Mock()

        BigQueryAdapter.get_partitions_metadata(stub, table)

        stub.connections.get_partitions_metadata.assert_called_once_with(
            table=table, use_standard_sql=False
        )
