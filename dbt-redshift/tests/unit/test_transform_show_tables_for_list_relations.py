import agate
import pytest

from dbt.adapters.redshift.impl import RedshiftAdapter


SHOW_TABLES_COLUMNS = [
    "database_name",
    "schema_name",
    "table_name",
    "table_type",
    "table_acl",
    "remarks",
    "owner",
    "last_altered_time",
    "last_modified_time",
    "dist_style",
    "table_subtype",
]

SHOW_TABLES_TYPES = [agate.Text()] * len(SHOW_TABLES_COLUMNS)


def _make_show_table(rows):
    return agate.Table(rows, column_names=SHOW_TABLES_COLUMNS, column_types=SHOW_TABLES_TYPES)


@pytest.fixture
def adapter(mocker):
    mock_config = mocker.MagicMock()
    mock_mp_context = mocker.MagicMock()
    return RedshiftAdapter(mock_config, mock_mp_context)


class TestTransformShowTablesForListRelations:
    def test_mixed_types(self, adapter):
        result = adapter.transform_show_tables_for_list_relations(
            _make_show_table(
                [
                    (
                        "dev",
                        "s1",
                        "test_table",
                        "TABLE",
                        "",
                        "",
                        "alice",
                        None,
                        None,
                        "AUTO (ALL)",
                        "REGULAR TABLE",
                    ),
                    (
                        "dev",
                        "s1",
                        "regular_view",
                        "VIEW",
                        "",
                        "",
                        "alice",
                        None,
                        None,
                        "",
                        "REGULAR VIEW",
                    ),
                    (
                        "dev",
                        "s1",
                        "late_binding_view",
                        "VIEW",
                        "",
                        "",
                        "alice",
                        None,
                        None,
                        "",
                        "LATE BINDING VIEW",
                    ),
                    (
                        "dev",
                        "s1",
                        "manual_mv",
                        "VIEW",
                        "",
                        "",
                        "alice",
                        None,
                        None,
                        "",
                        "MATERIALIZED VIEW",
                    ),
                ]
            )
        )
        assert list(result.column_names) == ["database", "name", "schema", "type"]
        assert len(result.rows) == 4
        assert result.rows[0]["type"] == "table"
        assert result.rows[1]["type"] == "view"
        assert result.rows[2]["type"] == "view"
        assert result.rows[3]["type"] == "materialized_view"

    def test_empty_result(self, adapter):
        result = adapter.transform_show_tables_for_list_relations(_make_show_table([]))
        assert len(result.rows) == 0
