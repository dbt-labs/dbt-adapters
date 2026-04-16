"""
Property-based tests for Column.from_description().

The method parses raw type strings (e.g. "NUMERIC(10,2)", "VARCHAR(255)") into
Column objects using a regex. The combinatorial
input space and regex boundary cases make this an ideal PBT target.
"""

import pytest
from hypothesis import assume, given, strategies as st

from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.base.column import Column


_STRING_TYPES = ["varchar", "char", "character varying", "character"]
_NUMERIC_TYPES = ["numeric", "decimal"]


@given(
    dtype=st.sampled_from(_STRING_TYPES),
    size=st.integers(min_value=1, max_value=65535),
)
def test_string_type_char_size_roundtrip(dtype, size):
    """Parsing DTYPE(N) preserves N in char_size for all string types."""
    col = Column.from_description("col", f"{dtype}({size})")
    assert col.char_size == size


@given(
    dtype=st.sampled_from(_NUMERIC_TYPES),
    precision=st.integers(min_value=1, max_value=38),
    scale=st.integers(min_value=0, max_value=38),
)
def test_numeric_precision_scale_roundtrip(dtype, precision, scale):
    """Parsing DTYPE(P,S) preserves both precision and scale."""
    assume(scale <= precision)
    col = Column.from_description("col", f"{dtype}({precision},{scale})")
    assert col.numeric_precision == precision
    assert col.numeric_scale == scale


@given(st.text())
def test_from_description_never_raises_unexpected_exceptions(type_str):
    """from_description must only raise DbtRuntimeError — never AttributeError, IndexError, etc."""
    try:
        Column.from_description("col", type_str)
    except DbtRuntimeError:
        pass
    except Exception as e:
        pytest.fail(f"Unexpected exception {type(e).__name__}: {e}")


@given(
    dtype=st.sampled_from(_STRING_TYPES),
    size=st.integers(min_value=1, max_value=65535),
)
def test_string_type_has_no_numeric_fields(dtype, size):
    """String-typed columns must never populate numeric_precision or numeric_scale."""
    col = Column.from_description("col", f"{dtype}({size})")
    assert col.numeric_precision is None
    assert col.numeric_scale is None


@given(
    dtype=st.sampled_from(_NUMERIC_TYPES),
    precision=st.integers(min_value=1, max_value=38),
    scale=st.integers(min_value=0, max_value=38),
)
def test_numeric_type_has_no_char_size(dtype, precision, scale):
    """Numeric-typed columns must never populate char_size."""
    assume(scale <= precision)
    col = Column.from_description("col", f"{dtype}({precision},{scale})")
    assert col.char_size is None


@given(
    dtype=st.sampled_from(
        _STRING_TYPES + _NUMERIC_TYPES + ["integer", "float", "boolean", "text"]
    ),
)
def test_plain_type_without_params_always_parses(dtype):
    """Type strings with no parentheses must always produce a Column (never raise)."""
    col = Column.from_description("col", dtype)
    assert col.dtype == dtype
