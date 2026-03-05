"""
Unit tests for the athena__create_hive_staging_table_as macro.

Tests that the macro always creates a Hive external table regardless of the
model's table_type config (e.g., 'iceberg'), preventing
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


def _extract_with_props(sql):
    """Parse WITH (...) properties from a CREATE TABLE statement."""
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


def _run_macro(table_type="hive", work_group_output_location_enforced=False):
    """Call athena__create_hive_staging_table_as and return (sql, mock_adapter)."""
    mock_adapter = mock.Mock()
    mock_adapter.generate_s3_location.return_value = _TMP_LOCATION
    mock_adapter.is_work_group_output_location_enforced.return_value = work_group_output_location_enforced

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

    context = {
        "config": mock_config,
        "adapter": mock_adapter,
        "target": SimpleNamespace(
            s3_data_dir="s3://my-bucket/data",
            s3_data_naming="unique",
            s3_tmp_table_dir=None,
        ),
        "log": lambda *args, **kwargs: None,
    }

    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader(_TABLE_DIR),
        extensions=["jinja2.ext.do"],
    )
    template = env.get_template("create_table_as.sql", globals=context)
    relation = _MockRelation("my_model__tmp_not_partitioned")
    sql = template.module.athena__create_hive_staging_table_as(
        relation=relation,
        compiled_code="select 1",
        temporary=False,
    )
    return sql, mock_adapter


class TestHiveStagingMacro:
    """Unit tests for athena__create_hive_staging_table_as."""

    def test_always_generates_hive_table(self):
        """Core bug fix: must produce Hive SQL regardless of model's table_type."""
        for table_type in ("hive", "iceberg"):
            sql, _ = _run_macro(table_type=table_type)
            props = _extract_with_props(sql)
            assert props.get("table_type") == "hive"
            assert props.get("is_external") == "true"
            assert props.get("external_location") == _TMP_LOCATION
            assert props.get("format") == "parquet"

    def test_omits_location_when_workgroup_enforced(self):
        sql, _ = _run_macro(work_group_output_location_enforced=True)
        assert "external_location" not in _extract_with_props(sql)

    def test_deletes_s3_location(self):
        _, mock_adapter = _run_macro(table_type="iceberg")
        mock_adapter.delete_from_s3.assert_called_once_with(_TMP_LOCATION)
