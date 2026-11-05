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
        ("varchar(10)", "hive", "string"),
        ("integer", "hive", "int"),
        ("timestamp(6)", "iceberg", "timestamp"),
        ("array(integer)", "hive", "array<int>"),
        ("map(varchar,integer)", "hive", "map<string,int>"),
        ("map(varchar,array(bigint))", "hive", "map<string,array<bigint>>"),
        # Parameterized types nested in an array or map keep their parentheses.
        (
            "map<string,struct<n:bigint,w:decimal(38,0)>>",
            "hive",
            "map<string,struct<n:bigint,w:decimal(38,0)>>",
        ),
        ("map(varchar,decimal(38,0))", "hive", "map<string,decimal(38,0)>"),
        ("array(decimal(38,0))", "hive", "array<decimal(38,0)>"),
        ("array<decimal(38,0)>", "hive", "array<decimal(38,0)>"),
        ("map<string,char(3)>", "hive", "map<string,char(3)>"),
        # A Trino-style row nested in an array or map keeps its parentheses.
        ("array(row(n bigint,w integer))", "hive", "array<row(n bigint,w int)>"),
    ],
)
def test_ddl_data_type(col_type, table_type, expected):
    assert _ddl_data_type(col_type, table_type) == expected
