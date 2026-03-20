import pytest

from dbt.adapters.bigquery.column import (
    BigQueryColumn,
    get_nested_column_data_types,
    _parse_struct_fields,
)


@pytest.mark.parametrize(
    ["columns", "constraints", "expected_nested_columns"],
    [
        ({}, None, {}),
        ({}, {"not_in_columns": "unique"}, {}),
        # Flat column
        (
            {"a": {"name": "a", "data_type": "string"}},
            None,
            {"a": {"name": "a", "data_type": "string"}},
        ),
        # Flat column - missing data_type
        (
            {"a": {"name": "a"}},
            None,
            {"a": {"name": "a", "data_type": None}},
        ),
        # Flat column - with constraints
        (
            {"a": {"name": "a", "data_type": "string"}},
            {"a": "not null"},
            {"a": {"name": "a", "data_type": "string not null"}},
        ),
        # Flat column - with constraints + other keys
        (
            {"a": {"name": "a", "data_type": "string", "quote": True}},
            {"a": "not null"},
            {"a": {"name": "a", "data_type": "string not null", "quote": True}},
        ),
        # Single nested column, 1 level
        (
            {"b.nested": {"name": "b.nested", "data_type": "string"}},
            None,
            {"b": {"name": "b", "data_type": "struct<nested string>"}},
        ),
        # Single nested column, 1 level - missing data_type
        (
            {"b.nested": {"name": "b.nested"}},
            None,
            {"b": {"name": "b", "data_type": "struct<nested>"}},
        ),
        # Single nested column, 1 level - with constraints
        (
            {"b.nested": {"name": "b.nested", "data_type": "string"}},
            {"b.nested": "not null"},
            {"b": {"name": "b", "data_type": "struct<nested string not null>"}},
        ),
        # Single nested column, 1 level - with constraints, missing data_type (constraints not valid without data_type)
        (
            {"b.nested": {"name": "b.nested"}},
            {"b.nested": "not null"},
            {"b": {"name": "b", "data_type": "struct<nested>"}},
        ),
        # Single nested column, 1 level - with constraints + other keys
        (
            {"b.nested": {"name": "b.nested", "data_type": "string", "other": "unpreserved"}},
            {"b.nested": "not null"},
            {"b": {"name": "b", "data_type": "struct<nested string not null>"}},
        ),
        # Single nested column, 1 level - with corresponding parent column
        (
            {
                "b": {"name": "b", "data_type": "struct"},
                "b.nested": {"name": "b.nested", "data_type": "string"},
            },
            None,
            {"b": {"name": "b", "data_type": "struct<nested string>"}},
        ),
        # Single nested column, 1 level - with corresponding parent column specified last
        (
            {
                "b.nested": {"name": "b.nested", "data_type": "string"},
                "b": {"name": "b", "data_type": "struct"},
            },
            None,
            {"b": {"name": "b", "data_type": "struct<nested string>"}},
        ),
        # Single nested column, 1 level - with corresponding parent column + parent constraint
        (
            {
                "b": {"name": "b", "data_type": "struct"},
                "b.nested": {"name": "b.nested", "data_type": "string"},
            },
            {"b": "not null"},
            {"b": {"name": "b", "data_type": "struct<nested string> not null"}},
        ),
        # Single nested column, 1 level - with corresponding parent column as array
        (
            {
                "b": {"name": "b", "data_type": "array"},
                "b.nested": {"name": "b.nested", "data_type": "string"},
            },
            None,
            {"b": {"name": "b", "data_type": "array<struct<nested string>>"}},
        ),
        # Single nested column, 1 level - with corresponding parent column as array + constraint
        (
            {
                "b": {"name": "b", "data_type": "array"},
                "b.nested": {"name": "b.nested", "data_type": "string"},
            },
            {"b": "not null"},
            {"b": {"name": "b", "data_type": "array<struct<nested string>> not null"}},
        ),
        # Multiple nested columns, 1 level
        (
            {
                "b.nested": {"name": "b.nested", "data_type": "string"},
                "b.nested2": {"name": "b.nested2", "data_type": "int64"},
            },
            None,
            {"b": {"name": "b", "data_type": "struct<nested string, nested2 int64>"}},
        ),
        # Multiple nested columns, 1 level - with constraints
        (
            {
                "b.nested": {"name": "b.nested", "data_type": "string"},
                "b.nested2": {"name": "b.nested2", "data_type": "int64"},
            },
            {"b.nested": "not null"},
            {"b": {"name": "b", "data_type": "struct<nested string not null, nested2 int64>"}},
        ),
        # Multiple nested columns, 1 level - with constraints
        (
            {
                "b.nested": {"name": "b.nested", "data_type": "string"},
                "b.nested2": {"name": "b.nested2", "data_type": "int64"},
            },
            {"b.nested": "not null"},
            {"b": {"name": "b", "data_type": "struct<nested string not null, nested2 int64>"}},
        ),
        # Mix of flat and nested columns, 1 level
        (
            {
                "a": {"name": "a", "data_type": "string"},
                "b.nested": {"name": "b.nested", "data_type": "string"},
                "b.nested2": {"name": "b.nested2", "data_type": "int64"},
            },
            None,
            {
                "b": {"name": "b", "data_type": "struct<nested string, nested2 int64>"},
                "a": {"name": "a", "data_type": "string"},
            },
        ),
        # Nested columns, multiple levels
        (
            {
                "b.user.name.first": {
                    "name": "b.user.name.first",
                    "data_type": "string",
                },
                "b.user.name.last": {
                    "name": "b.user.name.last",
                    "data_type": "string",
                },
                "b.user.id": {"name": "b.user.id", "data_type": "int64"},
                "b.user.country": {"name": "b.user.country", "data_type": "string"},
            },
            None,
            {
                "b": {
                    "name": "b",
                    "data_type": "struct<user struct<name struct<first string, last string>, id int64, country string>>",
                },
            },
        ),
        # Nested columns, multiple levels - missing data_type
        (
            {
                "b.user.name.first": {
                    "name": "b.user.name.first",
                    "data_type": "string",
                },
                "b.user.name.last": {
                    "name": "b.user.name.last",
                    "data_type": "string",
                },
                "b.user.id": {"name": "b.user.id", "data_type": "int64"},
                "b.user.country": {"name": "b.user.country"},  # missing data_type
            },
            None,
            {
                "b": {
                    "name": "b",
                    "data_type": "struct<user struct<name struct<first string, last string>, id int64, country>>",
                },
            },
        ),
        # Nested columns, multiple levels - with constraints!
        (
            {
                "b.user.name.first": {
                    "name": "b.user.name.first",
                    "data_type": "string",
                },
                "b.user.name.last": {
                    "name": "b.user.name.last",
                    "data_type": "string",
                },
                "b.user.id": {"name": "b.user.id", "data_type": "int64"},
                "b.user.country": {"name": "b.user.country", "data_type": "string"},
            },
            {"b.user.name.first": "not null", "b.user.id": "unique"},
            {
                "b": {
                    "name": "b",
                    "data_type": "struct<user struct<name struct<first string not null, last string>, id int64 unique, country string>>",
                },
            },
        ),
        # Nested columns, multiple levels - with parent arrays and constraints!
        (
            {
                "b.user.names": {
                    "name": "b.user.names",
                    "data_type": "array",
                },
                "b.user.names.first": {
                    "name": "b.user.names.first",
                    "data_type": "string",
                },
                "b.user.names.last": {
                    "name": "b.user.names.last",
                    "data_type": "string",
                },
                "b.user.id": {"name": "b.user.id", "data_type": "int64"},
                "b.user.country": {"name": "b.user.country", "data_type": "string"},
            },
            {"b.user.names.first": "not null", "b.user.id": "unique"},
            {
                "b": {
                    "name": "b",
                    "data_type": "struct<user struct<names array<struct<first string not null, last string>>, id int64 unique, country string>>",
                },
            },
        ),
    ],
)
def test_get_nested_column_data_types(columns, constraints, expected_nested_columns):
    actual_nested_columns = get_nested_column_data_types(columns, constraints)
    assert expected_nested_columns == actual_nested_columns


@pytest.mark.parametrize(
    ["data_type", "expected_fields"],
    [
        ("INT64", None),
        ("STRING", None),
        (
            "struct<x INT64, y STRING>",
            [{"name": "x", "data_type": "INT64"}, {"name": "y", "data_type": "STRING"}],
        ),
        (
            "struct<a struct<b INT64, c STRING>, d INT64>",
            [
                {"name": "a", "data_type": "struct<b INT64, c STRING>"},
                {"name": "d", "data_type": "INT64"},
            ],
        ),
        (
            "struct<nested string not null, nested2 int64>",
            [
                {"name": "nested", "data_type": "string not null"},
                {"name": "nested2", "data_type": "int64"},
            ],
        ),
        # Trailing constraints after closing > must not corrupt inner field parsing
        (
            "struct<x INT64, y STRING> not null",
            [{"name": "x", "data_type": "INT64"}, {"name": "y", "data_type": "STRING"}],
        ),
        (
            "struct<a struct<b INT64>> not null",
            [{"name": "a", "data_type": "struct<b INT64>"}],
        ),
        # Malformed: missing outer closing > returns None
        ("struct<x INT64, y STRING", None),
        # Malformed: missing inner closing > returns None
        ("struct<a struct<b INT64, c STRING>", None),
    ],
)
def test_parse_struct_fields(data_type, expected_fields):
    assert _parse_struct_fields(data_type) == expected_fields


@pytest.mark.parametrize(
    ["column_name", "data_type", "expected"],
    [
        # Non-struct column returns name as-is
        ("col", "INT64", "col"),
        ("col", "STRING", "col"),
        # Simple struct - fields are explicitly selected, NULL struct preserved
        (
            "col",
            "struct<y INT64, x INT64>",
            "IF(col IS NULL, NULL, STRUCT(col.y AS y, col.x AS x)) AS col",
        ),
        # Struct with 3 fields
        (
            "struct_data",
            "struct<y INT64, x INT64, z INT64>",
            "IF(struct_data IS NULL, NULL, STRUCT(struct_data.y AS y, struct_data.x AS x, struct_data.z AS z)) AS struct_data",
        ),
        # Nested struct - NULL semantics preserved at each nesting level
        (
            "a",
            "struct<b struct<c STRING, d INT64>, e INT64>",
            "IF(a IS NULL, NULL, STRUCT(IF(a.b IS NULL, NULL, STRUCT(a.b.c AS c, a.b.d AS d)) AS b, a.e AS e)) AS a",
        ),
        # Deeply nested struct
        (
            "a",
            "struct<b struct<c struct<d STRING>>>",
            "IF(a IS NULL, NULL, STRUCT(IF(a.b IS NULL, NULL, STRUCT(IF(a.b.c IS NULL, NULL, STRUCT(a.b.c.d AS d)) AS c)) AS b)) AS a",
        ),
        # Array type - not a struct, returns as-is
        ("col", "ARRAY<INT64>", "col"),
        # Struct with constraints in field types
        (
            "col",
            "struct<x STRING not null, y INT64>",
            "IF(col IS NULL, NULL, STRUCT(col.x AS x, col.y AS y)) AS col",
        ),
        # Empty data_type - returns column name as-is (no struct to parse)
        ("col", "", "col"),
    ],
)
def test_get_struct_select_expression(column_name, data_type, expected):
    assert BigQueryColumn.get_struct_select_expression(column_name, data_type) == expected
