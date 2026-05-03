"""
Property-based tests for merge_nested_fields and find_missing_fields in struct_utils.py.

Tests cover flat (non-RECORD) field lists to keep strategies simple while still
exercising the core merge/diff logic. RECORD nesting is intentionally out of scope
here — nested behaviour is covered by the functional tests.
"""

from hypothesis import assume, given, strategies as st

from dbt.adapters.bigquery.struct_utils import find_missing_fields, merge_nested_fields


_FIELD_NAME = st.from_regex(r"[a-z][a-z0-9_]{0,10}", fullmatch=True)
_FIELD_TYPE = st.sampled_from(["STRING", "INT64", "FLOAT64", "BOOL", "NUMERIC", "DATE"])

_flat_field = st.fixed_dictionaries({"name": _FIELD_NAME, "type": _FIELD_TYPE})

_flat_fields = st.lists(_flat_field, min_size=0, max_size=8, unique_by=lambda f: f["name"])


@given(fields=_flat_fields)
def test_find_missing_fields_identical_schemas_no_missing(fields):
    """find_missing_fields(source, source) must always return an empty dict."""
    missing = find_missing_fields(fields, fields)
    assert len(missing) == 0


@given(source=_flat_fields, target=_flat_fields)
def test_find_missing_fields_keys_are_source_names(source, target):
    """Every key returned by find_missing_fields must be a field name present in source."""
    source_names = {f["name"] for f in source}
    missing = find_missing_fields(source, target)
    for key in missing:
        assert key in source_names


@given(existing=_flat_fields, new_field=_flat_field)
def test_merge_adds_new_top_level_field(existing, new_field):
    """merge_nested_fields with a brand-new field must include it in the output."""
    assume(new_field["name"] not in {f["name"] for f in existing})
    additions = {new_field["name"]: new_field}
    merged = merge_nested_fields(existing, additions)
    merged_names = {f["name"] for f in merged}
    assert new_field["name"] in merged_names
    for f in existing:
        assert f["name"] in merged_names


@given(existing=_flat_fields, additions_list=_flat_fields)
def test_merge_idempotent(existing, additions_list):
    """Applying the same additions twice produces the same name→field mapping as applying once."""
    additions = {f["name"]: f for f in additions_list}
    result1 = merge_nested_fields(existing, additions)
    result2 = merge_nested_fields(result1, additions)
    result1_map = {f["name"]: f for f in result1}
    result2_map = {f["name"]: f for f in result2}
    assert result1_map == result2_map


@given(existing=_flat_fields, additions_list=_flat_fields)
def test_merge_then_find_missing_is_empty_for_additions(existing, additions_list):
    """After merging additions into existing, find_missing_fields reports no additions as missing."""
    additions = {f["name"]: f for f in additions_list}
    merged = merge_nested_fields(existing, additions)
    still_missing = find_missing_fields(additions_list, merged)
    assert len(still_missing) == 0
