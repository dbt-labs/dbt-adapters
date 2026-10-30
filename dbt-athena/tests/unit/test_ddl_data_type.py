"""
Unit tests for the ddl_data_type macro, rendered with jinja2 like test_get_partition_batches.
"""

import os
import re
from types import SimpleNamespace

import jinja2
import pytest

_UTILS_DIR = os.path.normpath(
    os.path.join(
        os.path.dirname(__file__),
        os.pardir,
        os.pardir,
        "src",
        "dbt",
        "include",
        "athena",
        "macros",
        "utils",
    )
)


def _ddl_data_type(col_type, table_type):
    result_holder = {}
    context = {
        "modules": SimpleNamespace(re=re),
        "return": lambda value: result_holder.update({"value": value}) or "",
    }
    env = jinja2.Environment(loader=jinja2.FileSystemLoader(_UTILS_DIR))
    template = env.get_template("ddl_dml_data_type.sql", globals=context)
    template.module.ddl_data_type(col_type, table_type)
    return result_holder["value"]


@pytest.mark.parametrize(
    "col_type,table_type,expected",
    [
        # Scalar types keep their existing mapping.
        ("varchar", "hive", "string"),
        ("varchar(10)", "hive", "string"),
        ("character varying(10)", "hive", "string"),
        ("integer", "hive", "int"),
        ("bigint", "hive", "bigint"),
        ("timestamp", "hive", "timestamp"),
        ("timestamp(6)", "iceberg", "timestamp"),
        ("timestamp(3) with time zone", "iceberg", "timestamp"),
        ("varbinary", "iceberg", "binary"),
        ("decimal(10,2)", "iceberg", "decimal(10,2)"),
        ("array(integer)", "hive", "array<int>"),
        ("map(varchar,integer)", "hive", "map<string,int>"),
        # Complex types keep their structure.
        ("struct<n:int,created_at:timestamp>", "iceberg", "struct<n:int,created_at:timestamp>"),
        ("struct<raw:binary>", "iceberg", "struct<raw:binary>"),
        ("array(timestamp)", "iceberg", "array<timestamp>"),
        ("array(timestamp(6))", "iceberg", "array<timestamp>"),
        ("map<string,timestamp>", "iceberg", "map<string,timestamp>"),
        # Field names that contain a type name are left alone.
        ("struct<event_timestamp:string>", "iceberg", "struct<event_timestamp:string>"),
        ("struct<timestamp:string>", "iceberg", "struct<timestamp:string>"),
        ("struct<integer_count:bigint>", "hive", "struct<integer_count:bigint>"),
        ("struct<integer:bigint,n:integer>", "hive", "struct<integer:bigint,n:int>"),
        ("struct<varchar_col:string>", "hive", "struct<varchar_col:string>"),
    ],
)
def test_ddl_data_type(col_type, table_type, expected):
    assert _ddl_data_type(col_type, table_type) == expected
