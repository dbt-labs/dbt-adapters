"""
Unit tests for the safe_create_table_as macro in create_table_as.sql.

Tests the full macro end-to-end using jinja2.FileSystemLoader with stubbed
dbt context, following the pattern used in test_get_partition_batches.py.
"""

import os
from unittest import mock

import jinja2

_TABLE_DIR = os.path.normpath(
    os.path.join(
        os.path.dirname(__file__),
        os.pardir,
        os.pardir,
        "src",
        "dbt",
        "include",
        "athena",
        "macros",
        "materializations",
        "models",
        "table",
    )
)


def _render_safe_create_table_as(temporary, run_query_result="success"):
    """Load and render safe_create_table_as with stubbed dbt context.

    Args:
        temporary: Whether the relation is a tmp table.
        run_query_result: Value returned by run_query_with_partitions_limit_catching.

    Returns:
        Tuple of (macro_result, adapter_mock, context_dict).
    """
    result_holder = {}

    # Stub adapter.dispatch so create_table_as renders simple SQL
    # without needing the full athena__create_table_as template logic.
    adapter = mock.Mock()
    adapter.dispatch.return_value = mock.Mock(return_value="CREATE TABLE test")
    adapter.run_query_with_partitions_limit_catching.return_value = run_query_result
    adapter.get_columns_in_relation.return_value = []

    relation = mock.Mock()
    relation.__str__ = lambda self: '"test_schema"."test_table__dbt_tmp"'
    relation.identifier = "test_table__dbt_tmp"
    relation.schema = "test_schema"
    relation.database = "AwsDataCatalog"
    relation.s3_path_table_part = "test_table__dbt_tmp"

    mock_tmp_relation = mock.Mock()
    mock_tmp_relation.__str__ = (
        lambda self: '"test_schema"."test_table__dbt_tmp__tmp_not_partitioned"'
    )

    mock_config = mock.Mock()
    mock_config.get = lambda key, *args, **kwargs: (
        mock.Mock(enforced=False)
        if key == "contract"
        else {
            "materialized": "incremental",
            "external_location": None,
            "partitioned_by": ["date_col"],
            "bucketed_by": None,
            "bucket_count": None,
            "field_delimiter": None,
            "table_type": "hive",
            "format": "parquet",
            "write_compression": None,
            "s3_data_dir": "s3://test-bucket/data",
            "s3_data_naming": "table",
            "s3_tmp_table_dir": "s3://test-bucket/tmp",
            "table_properties": None,
            "native_drop": False,
        }.get(key, args[0] if args else kwargs.get("default"))
    )

    mock_target = mock.Mock()
    mock_target.s3_data_dir = "s3://test-bucket/data"
    mock_target.s3_data_naming = "table"
    mock_target.s3_tmp_table_dir = "s3://test-bucket/tmp"

    mock_api = mock.Mock()
    mock_api.Relation.create.return_value = mock_tmp_relation

    context = {
        "config": mock_config,
        "adapter": adapter,
        "target": mock_target,
        "api": mock_api,
        "run_query": mock.Mock(),
        "drop_relation": mock.Mock(),
        "get_partition_batches": mock.Mock(return_value=[]),
        "log": lambda *args, **kwargs: "",
        "return": lambda value: result_holder.update({"value": value}) or "",
        "exceptions": mock.Mock(),
    }

    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader(_TABLE_DIR),
        extensions=["jinja2.ext.do"],
    )

    template = env.get_template("create_table_as.sql", globals=context)
    template.module.safe_create_table_as(
        temporary=temporary,
        relation=relation,
        compiled_code="select 1 as id, cast('2023-01-01' as date) as date_col",
        language="sql",
    )

    return result_holder.get("value"), adapter, context


class TestSafeCreateTableAsPartitionHandling:
    """
    Tests that safe_create_table_as applies partition-aware creation logic
    regardless of whether temporary=True or temporary=False.

    Before the fix, temporary=True skipped run_query_with_partitions_limit_catching
    and always used skip_partitioning=True, making batch inserts O(N * full scan).
    After the fix, both paths go through the same partition-handling logic.
    """

    def test_temporary_true_calls_run_query_with_partitions_limit_catching(self):
        """
        With temporary=True, run_query_with_partitions_limit_catching must be called.
        Before the fix this code path was bypassed entirely.
        """
        result, adapter, _ = _render_safe_create_table_as(
            temporary=True, run_query_result="success"
        )

        adapter.run_query_with_partitions_limit_catching.assert_called_once()
        assert result == "success"

    def test_temporary_false_calls_run_query_with_partitions_limit_catching(self):
        """With temporary=False, run_query_with_partitions_limit_catching is called."""
        result, adapter, _ = _render_safe_create_table_as(
            temporary=False, run_query_result="success"
        )

        adapter.run_query_with_partitions_limit_catching.assert_called_once()
        assert result == "success"

    def test_temporary_true_too_many_partitions_falls_back_to_batch(self):
        """
        When TOO_MANY_OPEN_PARTITIONS is returned for a tmp table (temporary=True),
        create_table_as_with_partitions fallback must be triggered.
        Before the fix this path was unreachable for temporary=True.
        """
        result, _, context = _render_safe_create_table_as(
            temporary=True,
            run_query_result="TOO_MANY_OPEN_PARTITIONS",
        )

        # create_table_as_with_partitions calls get_partition_batches
        # to split the load into batches — verify it was invoked.
        context["get_partition_batches"].assert_called_once()
        assert result == '"test_schema"."test_table__dbt_tmp" with many partitions created'
