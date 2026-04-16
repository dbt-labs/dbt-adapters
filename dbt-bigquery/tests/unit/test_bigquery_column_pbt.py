"""
Property-based tests for _parse_struct_fields().

The function is a hand-rolled parser that tracks angle-bracket and parenthesis
depth simultaneously to split BigQuery STRUCT type strings into fields. This is
exactly the class of problem where Hypothesis finds off-by-ones and bracket-counting
errors that hand-written examples miss.
"""

import pytest
from hypothesis import assume, given, strategies as st

from dbt.adapters.bigquery.column import _parse_struct_fields


_FIELD_NAME = st.from_regex(r"[a-z][a-z0-9_]{0,10}", fullmatch=True)
_SIMPLE_TYPES = ["INT64", "STRING", "FLOAT64", "BOOL", "DATE", "TIMESTAMP"]
_PARAMETERISED_TYPES = ["NUMERIC(10, 2)", "NUMERIC(38, 9)", "BIGNUMERIC(76, 38)"]
_ALL_FIELD_TYPES = st.sampled_from(_SIMPLE_TYPES + _PARAMETERISED_TYPES)

_field_tuple = st.tuples(_FIELD_NAME, _ALL_FIELD_TYPES)


@given(fields=st.lists(_field_tuple, min_size=1, max_size=8))
def test_well_formed_struct_field_count(fields):
    """A well-formed struct<...> string always parses to exactly len(fields) entries."""
    struct_str = "struct<" + ", ".join(f"{name} {dtype}" for name, dtype in fields) + ">"
    result = _parse_struct_fields(struct_str)
    assert result is not None
    assert len(result) == len(fields)


@given(
    precision=st.integers(min_value=1, max_value=38),
    scale=st.integers(min_value=0, max_value=10),
    extra_fields=st.lists(_field_tuple, min_size=0, max_size=4),
)
def test_numeric_params_not_split_as_field_separator(precision, scale, extra_fields):
    """Commas inside NUMERIC(P,S) must not be treated as field separators."""
    assume(scale <= precision)
    fields = [("amount", f"NUMERIC({precision},{scale})")] + extra_fields
    struct_str = "struct<" + ", ".join(f"{n} {t}" for n, t in fields) + ">"
    result = _parse_struct_fields(struct_str)
    assert result is not None
    assert len(result) == len(fields)


@given(st.text())
def test_malformed_input_never_raises(type_str):
    """_parse_struct_fields must never raise — it returns None or a list for all inputs."""
    try:
        result = _parse_struct_fields(type_str)
        assert result is None or isinstance(result, list)
    except Exception as e:
        pytest.fail(f"Unexpected exception {type(e).__name__}: {e}")


@given(fields=st.lists(_field_tuple, min_size=1, max_size=8))
def test_field_names_preserved(fields):
    """Parsed field names must match the names in the input struct string."""
    struct_str = "struct<" + ", ".join(f"{name} {dtype}" for name, dtype in fields) + ">"
    result = _parse_struct_fields(struct_str)
    assert result is not None
    parsed_names = [f["name"] for f in result]
    input_names = [name for name, _ in fields]
    assert parsed_names == input_names


def test_non_struct_returns_none():
    """Non-struct type strings must return None, not raise."""
    for type_str in ["INT64", "STRING", "ARRAY<INT64>", "", "NUMERIC(10,2)"]:
        assert _parse_struct_fields(type_str) is None
