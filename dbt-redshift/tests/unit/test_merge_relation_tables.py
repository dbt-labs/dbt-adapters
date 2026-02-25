import agate
import pytest

from dbt.adapters.redshift.impl import RedshiftAdapter


COLUMN_NAMES = ["database", "name", "schema", "type"]
COLUMN_TYPES = [agate.Text(), agate.Text(), agate.Text(), agate.Text()]


def _make_table(rows):
    return agate.Table(rows, column_names=COLUMN_NAMES, column_types=COLUMN_TYPES)


@pytest.fixture
def adapter(mocker):
    mock_config = mocker.MagicMock()
    mock_mp_context = mocker.MagicMock()
    adapter = RedshiftAdapter(mock_config, mock_mp_context)
    return adapter


class TestMergeRelationTables:
    def test_basic_merge(self, adapter):
        all_relations = _make_table(
            [
                ("db", "users", "public", "table"),
                ("db", "user_view", "public", "view"),
                ("db", "summary", "public", "table"),
            ]
        )
        materialized_views = _make_table(
            [
                ("db", "user_view", "public", "materialized_view"),
            ]
        )

        result = adapter.merge_relation_tables(all_relations, materialized_views)

        assert len(result.rows) == 3
        assert result.rows[0]["type"] == "table"
        assert result.rows[1]["type"] == "materialized_view"
        assert result.rows[2]["type"] == "table"

    def test_no_materialized_views(self, adapter):
        all_relations = _make_table(
            [
                ("db", "users", "public", "table"),
                ("db", "orders", "public", "view"),
            ]
        )
        materialized_views = _make_table([])

        result = adapter.merge_relation_tables(all_relations, materialized_views)

        assert len(result.rows) == 2
        assert result.rows[0]["type"] == "table"
        assert result.rows[1]["type"] == "view"

    def test_no_relations(self, adapter):
        all_relations = _make_table([])
        materialized_views = _make_table([])

        result = adapter.merge_relation_tables(all_relations, materialized_views)

        assert len(result.rows) == 0

    def test_all_views_are_materialized_views(self, adapter):
        all_relations = _make_table(
            [
                ("db", "mv1", "public", "view"),
                ("db", "mv2", "public", "view"),
                ("db", "mv3", "analytics", "view"),
            ]
        )
        materialized_views = _make_table(
            [
                ("db", "mv1", "public", "materialized_view"),
                ("db", "mv2", "public", "materialized_view"),
                ("db", "mv3", "analytics", "materialized_view"),
            ]
        )

        result = adapter.merge_relation_tables(all_relations, materialized_views)

        assert len(result.rows) == 3
        for row in result.rows:
            assert row["type"] == "materialized_view"

    def test_mv_with_no_match_is_ignored(self, adapter):
        all_relations = _make_table(
            [
                ("db", "users", "public", "table"),
            ]
        )
        materialized_views = _make_table(
            [
                ("db", "nonexistent", "public", "materialized_view"),
            ]
        )

        result = adapter.merge_relation_tables(all_relations, materialized_views)

        assert len(result.rows) == 1
        assert result.rows[0]["name"] == "users"
        assert result.rows[0]["type"] == "table"

    def test_cross_database_merge(self, adapter):
        all_relations = _make_table(
            [
                ("db1", "users", "public", "view"),
                ("db2", "users", "public", "view"),
            ]
        )
        materialized_views = _make_table(
            [
                ("db1", "users", "public", "materialized_view"),
            ]
        )

        result = adapter.merge_relation_tables(all_relations, materialized_views)

        assert result.rows[0]["type"] == "materialized_view"
        assert result.rows[1]["type"] == "view"

    def test_preserves_column_structure(self, adapter):
        all_relations = _make_table(
            [
                ("db", "users", "public", "table"),
            ]
        )
        materialized_views = _make_table([])

        result = adapter.merge_relation_tables(all_relations, materialized_views)

        assert result.column_names == all_relations.column_names
        assert len(result.column_types) == len(all_relations.column_types)
