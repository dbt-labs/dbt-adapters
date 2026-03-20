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

    def test_model_quoted_same_as_default(self, mocker):
        adapter = _make_adapter(mocker, default_database="dev")
        assert adapter._needs_database_change({"database": '"dev"'}) is False

    def test_model_differs_from_default(self, mocker):
        adapter = _make_adapter(mocker, default_database="dev")
        assert adapter._needs_database_change({"database": "other_db"}) is True


class TestModelHooks:
    """Unit tests asserting pre/post_model_hook issue the correct SQL."""

    def test_pre_model_hook_sets_query_group(self, mocker):
        adapter = _make_adapter(mocker, default_query_group=None)
        mock_execute = mocker.patch.object(adapter, "execute")

        adapter.pre_model_hook({"query_group": "model_qg"})

        mock_execute.assert_called_once_with("SET query_group TO 'model_qg'")

    def test_pre_model_hook_uses_database(self, mocker):
        adapter = _make_adapter(mocker, default_database="dev")
        mock_execute = mocker.patch.object(adapter, "execute")

        adapter.pre_model_hook({"database": "other_db"})

        mock_execute.assert_called_once_with('USE "other_db"')

    def test_pre_model_hook_uses_quoted_database(self, mocker):
        adapter = _make_adapter(mocker, default_database="dev")
        mock_execute = mocker.patch.object(adapter, "execute")

        adapter.pre_model_hook({"database": '"other_db"'})

        mock_execute.assert_called_once_with('USE "other_db"')

    def test_pre_model_hook_sets_both(self, mocker):
        adapter = _make_adapter(mocker, default_query_group=None, default_database="dev")
        mock_execute = mocker.patch.object(adapter, "execute")

        adapter.pre_model_hook({"query_group": "model_qg", "database": "other_db"})

        assert mock_execute.call_count == 2
        mock_execute.assert_any_call("SET query_group TO 'model_qg'")
        mock_execute.assert_any_call('USE "other_db"')

    def test_pre_model_hook_no_change(self, mocker):
        adapter = _make_adapter(mocker, default_query_group="qg", default_database="dev")
        mock_execute = mocker.patch.object(adapter, "execute")

        adapter.pre_model_hook({"query_group": "qg", "database": "dev"})

        mock_execute.assert_not_called()

    def test_post_model_hook_resets_query_group(self, mocker):
        adapter = _make_adapter(mocker, default_query_group=None)
        mock_execute = mocker.patch.object(adapter, "execute")

        adapter.post_model_hook({"query_group": "model_qg"}, None)

        mock_execute.assert_called_once_with("RESET query_group")

    def test_post_model_hook_restores_query_group(self, mocker):
        adapter = _make_adapter(mocker, default_query_group="default_qg")
        mock_execute = mocker.patch.object(adapter, "execute")

        adapter.post_model_hook({"query_group": "model_qg"}, None)

        mock_execute.assert_called_once_with("SET query_group TO 'default_qg'")

    def test_post_model_hook_resets_database(self, mocker):
        adapter = _make_adapter(mocker, default_database="dev")
        mock_execute = mocker.patch.object(adapter, "execute")

        adapter.post_model_hook({"database": "other_db"}, None)

        mock_execute.assert_called_once_with("RESET USE")

    def test_post_model_hook_no_change(self, mocker):
        adapter = _make_adapter(mocker, default_query_group="qg", default_database="dev")
        mock_execute = mocker.patch.object(adapter, "execute")

        adapter.post_model_hook({"query_group": "qg", "database": "dev"}, None)

        mock_execute.assert_not_called()
