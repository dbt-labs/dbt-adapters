from dbt.adapters.redshift import RedshiftAdapter


def _make_adapter(mocker, default_query_group=None, default_database="dev"):
    mock_config = mocker.MagicMock()
    mock_config.credentials.query_group = default_query_group
    mock_config.credentials.database = default_database
    return RedshiftAdapter(mock_config, mocker.MagicMock())


class TestNeedsQueryGroupChange:
    """Unit tests for RedshiftAdapter._needs_query_group_change."""

    def test_no_model_no_default(self, mocker):
        adapter = _make_adapter(mocker, default_query_group=None)
        assert adapter._needs_query_group_change({}) is False

    def test_no_model_with_default(self, mocker):
        adapter = _make_adapter(mocker, default_query_group="profile_qg")
        assert adapter._needs_query_group_change({}) is False

    def test_model_no_default(self, mocker):
        adapter = _make_adapter(mocker, default_query_group=None)
        assert adapter._needs_query_group_change({"query_group": "model_qg"}) is True

    def test_model_differs_from_default(self, mocker):
        adapter = _make_adapter(mocker, default_query_group="profile_qg")
        assert adapter._needs_query_group_change({"query_group": "model_qg"}) is True

    def test_model_same_as_default(self, mocker):
        adapter = _make_adapter(mocker, default_query_group="shared_qg")
        assert adapter._needs_query_group_change({"query_group": "shared_qg"}) is False


class TestNeedsDatabaseChange:
    """Unit tests for RedshiftAdapter._needs_database_change."""

    def test_no_model_database(self, mocker):
        adapter = _make_adapter(mocker, default_database="dev")
        assert adapter._needs_database_change({}) is False

    def test_model_same_as_default(self, mocker):
        adapter = _make_adapter(mocker, default_database="dev")
        assert adapter._needs_database_change({"database": "dev"}) is False

    def test_model_same_as_default_case_insensitive(self, mocker):
        adapter = _make_adapter(mocker, default_database="dev")
        assert adapter._needs_database_change({"database": "DEV"}) is False

    def test_model_differs_from_default(self, mocker):
        adapter = _make_adapter(mocker, default_database="dev")
        assert adapter._needs_database_change({"database": "other_db"}) is True
