import pytest

from dbt.adapters.redshift import RedshiftAdapter


def _make_adapter(mocker, default_database="dev"):
    mock_config = mocker.MagicMock()
    mock_config.credentials.query_group = None
    mock_config.credentials.database = default_database
    return RedshiftAdapter(mock_config, mocker.MagicMock())


def _make_relation(mocker, database):
    relation = mocker.MagicMock()
    relation.database = database
    return relation


class TestIsDifferentDatabase:
    """Unit tests for RedshiftAdapter._is_different_database."""

    def test_none(self, mocker):
        adapter = _make_adapter(mocker, default_database="dev")
        assert adapter._is_different_database(None) is False

    def test_same(self, mocker):
        adapter = _make_adapter(mocker, default_database="dev")
        assert adapter._is_different_database("dev") is False

    def test_same_case_insensitive(self, mocker):
        adapter = _make_adapter(mocker, default_database="dev")
        assert adapter._is_different_database("DEV") is False

    def test_same_quoted(self, mocker):
        adapter = _make_adapter(mocker, default_database="dev")
        assert adapter._is_different_database('"dev"') is False

    def test_different(self, mocker):
        adapter = _make_adapter(mocker, default_database="dev")
        assert adapter._is_different_database("other_db") is True


class TestSchemaOperations:
    """Unit tests for create_schema / drop_schema cross-database support."""

    def test_create_schema_cross_db_issues_use(self, mocker):
        adapter = _make_adapter(mocker, default_database="dev")
        relation = _make_relation(mocker, database="other_db")
        mock_execute = mocker.patch.object(adapter, "execute")
        mock_super = mocker.patch("dbt.adapters.sql.SQLAdapter.create_schema")

        adapter.create_schema(relation)

        mock_execute.assert_any_call('USE "other_db"')
        mock_super.assert_called_once_with(relation)
        mock_execute.assert_any_call("RESET USE")

    def test_create_schema_same_db_no_use(self, mocker):
        adapter = _make_adapter(mocker, default_database="dev")
        relation = _make_relation(mocker, database="dev")
        mock_execute = mocker.patch.object(adapter, "execute")
        mocker.patch("dbt.adapters.sql.SQLAdapter.create_schema")

        adapter.create_schema(relation)

        mock_execute.assert_not_called()

    def test_drop_schema_cross_db_issues_use(self, mocker):
        adapter = _make_adapter(mocker, default_database="dev")
        relation = _make_relation(mocker, database="other_db")
        mock_execute = mocker.patch.object(adapter, "execute")
        mock_super = mocker.patch("dbt.adapters.sql.SQLAdapter.drop_schema")

        adapter.drop_schema(relation)

        mock_execute.assert_any_call('USE "other_db"')
        mock_super.assert_called_once_with(relation)
        mock_execute.assert_any_call("RESET USE")

    def test_drop_schema_same_db_no_use(self, mocker):
        adapter = _make_adapter(mocker, default_database="dev")
        relation = _make_relation(mocker, database="dev")
        mock_execute = mocker.patch.object(adapter, "execute")
        mocker.patch("dbt.adapters.sql.SQLAdapter.drop_schema")

        adapter.drop_schema(relation)

        mock_execute.assert_not_called()

    def test_create_schema_resets_on_error(self, mocker):
        adapter = _make_adapter(mocker, default_database="dev")
        relation = _make_relation(mocker, database="other_db")
        mock_execute = mocker.patch.object(adapter, "execute")
        mocker.patch(
            "dbt.adapters.sql.SQLAdapter.create_schema",
            side_effect=RuntimeError("boom"),
        )

        with pytest.raises(RuntimeError, match="boom"):
            adapter.create_schema(relation)

        mock_execute.assert_any_call("RESET USE")

    def test_drop_schema_resets_on_error(self, mocker):
        adapter = _make_adapter(mocker, default_database="dev")
        relation = _make_relation(mocker, database="other_db")
        mock_execute = mocker.patch.object(adapter, "execute")
        mocker.patch(
            "dbt.adapters.sql.SQLAdapter.drop_schema",
            side_effect=RuntimeError("boom"),
        )

        with pytest.raises(RuntimeError, match="boom"):
            adapter.drop_schema(relation)

        mock_execute.assert_any_call("RESET USE")
