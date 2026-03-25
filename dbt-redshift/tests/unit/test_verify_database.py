import pytest

import dbt_common.exceptions
from dbt.adapters.redshift.impl import RedshiftAdapter


@pytest.fixture
def adapter(mocker):
    mock_config = mocker.MagicMock()
    mock_config.credentials.database = "dev"
    mock_config.credentials.ra3_node = False
    mock_mp_context = mocker.MagicMock()
    adapter = RedshiftAdapter(mock_config, mock_mp_context)
    adapter.use_show_apis = lambda: False
    return adapter


class TestVerifyDatabase:
    """Tests for verify_database cross-database reference gating."""

    def test_same_database_always_passes(self, adapter):
        assert adapter.verify_database("dev") == ""

    def test_same_database_case_insensitive(self, adapter):
        assert adapter.verify_database("DEV") == ""

    def test_quoted_database_strips_quotes(self, adapter):
        assert adapter.verify_database('"dev"') == ""

    def test_cross_db_blocked_by_default(self, adapter):
        with pytest.raises(dbt_common.exceptions.NotImplementedError, match="Cross-db"):
            adapter.verify_database("other_db")

    def test_cross_db_allowed_with_ra3_node(self, adapter):
        adapter.config.credentials.ra3_node = True
        assert adapter.verify_database("other_db") == ""

    def test_cross_db_allowed_with_use_show_apis(self, adapter):
        adapter.use_show_apis = lambda: True
        assert adapter.verify_database("other_db") == ""

    def test_cross_db_allowed_with_both_flags(self, adapter):
        adapter.config.credentials.ra3_node = True
        adapter.use_show_apis = lambda: True
        assert adapter.verify_database("other_db") == ""

    def test_cross_db_blocked_without_either_flag(self, adapter):
        adapter.config.credentials.ra3_node = False
        adapter.use_show_apis = lambda: False
        with pytest.raises(dbt_common.exceptions.NotImplementedError, match="Cross-db"):
            adapter.verify_database("other_db")
