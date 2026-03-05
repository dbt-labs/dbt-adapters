"""
Unit tests for the create_table_as_with_partitions macro.

Tests that the tmp staging table is always created with table_type='hive'
regardless of the model's table_type config (e.g., 'iceberg'), preventing
ICEBERG_FILESYSTEM_ERROR on retries caused by non-empty S3 locations.
"""

import os
import re
from types import SimpleNamespace
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

_TMP_LOCATION = "s3://my-bucket/data/my_schema/my_model__tmp_not_partitioned"


class _MockRelation:
    def __init__(self, identifier, schema="my_schema", database="my_catalog"):
        self.identifier = identifier
        self.schema = schema
        self.database = database

    def __str__(self):
        return f'"{self.database}"."{self.schema}"."{self.identifier}"'


class _MockColumn:
    def __init__(self, name):
        self.name = name
        self.quoted = f'"{name}"'


def _extract_with_props(sql):
    """Parse WITH (...) properties from a CREATE TABLE statement.

    Returns a dict of property_name -> value extracted from WITH clause.
    """
    match = re.search(r"with\s*\((.*?)\)", sql, re.IGNORECASE | re.DOTALL)
    if not match:
        return {}
    props = {}
    for part in match.group(1).split(","):
        part = part.strip()
        kv = re.match(r"(\w+)\s*=\s*(.+)", part)
        if kv:
            props[kv.group(1).strip().lower()] = kv.group(2).strip().strip("'")
    return props


def _build_context(table_type="hive", work_group_output_location_enforced=False, num_batches=1):
    """Build a stubbed jinja2 context and capture containers for a macro run."""
    run_query_calls = []
    drop_relation_calls = []

    relation = _MockRelation("my_model")

    def mock_api_relation_create(**kwargs):
        return _MockRelation(
            identifier=kwargs.get("identifier", "tmp"),
            schema=kwargs.get("schema", "my_schema"),
            database=kwargs.get("database", "my_catalog"),
        )

    mock_adapter = mock.Mock()
    mock_adapter.generate_s3_location.return_value = _TMP_LOCATION
    mock_adapter.is_work_group_output_location_enforced.return_value = work_group_output_location_enforced
    mock_adapter.get_columns_in_relation.return_value = [_MockColumn("col1"), _MockColumn("col2")]

    config_data = {
        "table_type": table_type,
        "format": "parquet",
        "s3_data_dir": "s3://my-bucket/data",
        "s3_data_naming": "unique",
        "s3_tmp_table_dir": None,
        "external_location": None,
    }
    mock_config = mock.Mock()
    mock_config.get = lambda key, *args, **kwargs: config_data.get(
        key, args[0] if args else kwargs.get("default")
    )

    batches = [f"batch_{i}" for i in range(num_batches)]

    dispatch_calls = []

    def mock_dispatch(macro_name, package=None):
        def dispatched(*args, **kwargs):
            dispatch_calls.append({"macro": macro_name, "args": args})
            return f"-- dispatch:{macro_name}"

        return dispatched

    mock_adapter.dispatch = mock_dispatch

    context = {
        "api": SimpleNamespace(Relation=SimpleNamespace(create=mock_api_relation_create)),
        "config": mock_config,
        "adapter": mock_adapter,
        "target": SimpleNamespace(
            s3_data_dir="s3://my-bucket/data",
            s3_data_naming="unique",
            s3_tmp_table_dir=None,
        ),
        "log": lambda *args, **kwargs: None,
        "run_query": lambda sql: run_query_calls.append(sql),
        "drop_relation": lambda rel: drop_relation_calls.append(rel),
        "get_partition_batches": lambda **kwargs: batches,
        "return": lambda value: None,
    }

    return context, relation, {
        "run_query_calls": run_query_calls,
        "drop_relation_calls": drop_relation_calls,
        "dispatch_calls": dispatch_calls,
        "mock_adapter": mock_adapter,
    }


def _run_macro(table_type="hive", work_group_output_location_enforced=False, num_batches=1):
    context, relation, captures = _build_context(
        table_type=table_type,
        work_group_output_location_enforced=work_group_output_location_enforced,
        num_batches=num_batches,
    )
    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader(_TABLE_DIR),
        extensions=["jinja2.ext.do"],
    )
    template = env.get_template("create_table_as.sql", globals=context)
    template.module.create_table_as_with_partitions(
        temporary=False,
        relation=relation,
        compiled_code="select 1",
    )
    return captures


class TestTmpTableForcedHive:
    """The tmp staging table must always be created with table_type='hive'."""

    def test_iceberg_model_tmp_table_forced_to_hive(self):
        """Core bug fix: Iceberg model must still produce a Hive tmp table."""
        captures = _run_macro(table_type="iceberg")
        tmp_sql = captures["run_query_calls"][0]
        props = _extract_with_props(tmp_sql)
        assert props.get("table_type") == "hive"
        assert props.get("is_external") == "true"
        assert props.get("external_location") == _TMP_LOCATION
        assert props.get("format") == "parquet"

    def test_tmp_table_omits_location_when_workgroup_enforced(self):
        captures = _run_macro(table_type="iceberg", work_group_output_location_enforced=True)
        tmp_sql = captures["run_query_calls"][0]
        props = _extract_with_props(tmp_sql)
        assert "external_location" not in props


class TestS3Cleanup:
    """delete_from_s3 must be called with the tmp location before table creation."""

    def test_delete_from_s3_called_with_tmp_location(self):
        captures = _run_macro(table_type="iceberg")
        captures["mock_adapter"].delete_from_s3.assert_called_once_with(_TMP_LOCATION)

    def test_delete_from_s3_called_before_run_query(self):
        """S3 cleanup must happen before the tmp table is created."""
        call_order = []
        context, relation, _ = _build_context(table_type="iceberg")
        context["adapter"].delete_from_s3.side_effect = lambda loc: call_order.append("delete_from_s3")
        original_run_query = context["run_query"]

        def tracking_run_query(sql):
            call_order.append("run_query")
            original_run_query(sql)

        context["run_query"] = tracking_run_query

        env = jinja2.Environment(
            loader=jinja2.FileSystemLoader(_TABLE_DIR),
            extensions=["jinja2.ext.do"],
        )
        template = env.get_template("create_table_as.sql", globals=context)
        template.module.create_table_as_with_partitions(
            temporary=False,
            relation=relation,
            compiled_code="select 1",
        )

        assert call_order[0] == "delete_from_s3", (
            "delete_from_s3 must be called before run_query (tmp table creation)"
        )
        assert call_order[1] == "run_query"


class TestBatchProcessing:
    """Batch insert logic must remain intact after the tmp table fix."""

    def test_multi_batch_only_first_uses_create_table_as(self):
        captures = _run_macro(table_type="iceberg", num_batches=3)
        dispatched = [c for c in captures["dispatch_calls"] if c["macro"] == "create_table_as"]
        assert len(dispatched) == 1

    def test_multi_batch_subsequent_batches_use_insert(self):
        captures = _run_macro(table_type="iceberg", num_batches=3)
        insert_calls = [
            sql for sql in captures["run_query_calls"]
            if re.search(r"insert\s+into", sql, re.IGNORECASE)
        ]
        assert len(insert_calls) == 2

    def test_tmp_relation_dropped_before_and_after(self):
        captures = _run_macro(table_type="iceberg")
        drops = captures["drop_relation_calls"]
        assert len(drops) == 2
        assert all("tmp_not_partitioned" in r.identifier for r in drops)
