import agate
import pytest

from dbt.adapters.snowflake.impl import SnowflakeAdapter
from dbt.adapters.snowflake.relation_configs.policies import SnowflakeRelationType


def _row(**overrides):
    base = {
        "database_name": "db",
        "schema_name": "sch",
        "name": "obj",
        "kind": "TABLE",
        "is_dynamic": "N",
        "is_iceberg": "N",
        "is_interactive": "N",
    }
    base.update(overrides)
    keys = list(base.keys())
    # Force Text typing: agate's default type inference reads single-row "Y"/"N"
    # values as Boolean, which production never hits because
    # normalize_show_objects_result always forces these columns to Text first.
    column_types = [agate.Text()] * len(keys)
    return agate.Table([[base[k] for k in keys]], keys, column_types=column_types).rows[0]


@pytest.mark.parametrize(
    "is_interactive,is_dynamic,expected",
    [
        ("Y", "N", SnowflakeRelationType.InteractiveTable),
        # A dynamic interactive table reports BOTH flags; interactive must win.
        ("Y", "Y", SnowflakeRelationType.InteractiveTable),
        ("YES", "N", SnowflakeRelationType.InteractiveTable),
        ("N", "Y", SnowflakeRelationType.DynamicTable),
        ("N", "N", SnowflakeRelationType.Table),
        # Absence markers must all degrade to "not interactive".
        (None, "Y", SnowflakeRelationType.DynamicTable),
        ("", "Y", SnowflakeRelationType.DynamicTable),
    ],
)
def test_discriminator_precedence(is_interactive, is_dynamic, expected):
    row = _row(is_interactive=is_interactive, is_dynamic=is_dynamic)
    assert SnowflakeAdapter._tabular_relation_type("TABLE", row) == expected


def test_missing_is_interactive_column_does_not_raise():
    """An account without the feature returns no is_interactive column at all."""
    keys = ["database_name", "schema_name", "name", "kind", "is_dynamic", "is_iceberg"]
    column_types = [agate.Text()] * len(keys)
    row = agate.Table(
        [["db", "sch", "obj", "TABLE", "Y", "N"]], keys, column_types=column_types
    ).rows[0]
    assert (
        SnowflakeAdapter._tabular_relation_type("TABLE", row) == SnowflakeRelationType.DynamicTable
    )


def test_non_table_kind_is_untouched_by_the_interactive_check():
    """The interactive/dynamic remap must only apply to kind=TABLE."""
    row = _row(kind="VIEW", is_interactive="Y")
    assert SnowflakeAdapter._tabular_relation_type("VIEW", row) == SnowflakeRelationType.View


def test_unknown_kind_still_falls_back_to_external():
    row = _row(kind="WIDGET")
    assert SnowflakeAdapter._tabular_relation_type("WIDGET", row) == SnowflakeRelationType.External
