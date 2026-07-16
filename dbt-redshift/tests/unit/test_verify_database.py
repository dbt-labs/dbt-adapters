import pytest

import dbt_common.exceptions
from dbt.adapters.redshift.impl import RedshiftAdapter


@pytest.fixture
def adapter(mocker):
    mock_config = mocker.MagicMock()
    mock_config.credentials.database = "dev"
    mock_config.credentials.ra3_node = False
    mock_config.credentials.datasharing = False
    mock_config.flags = {}
    mock_mp_context = mocker.MagicMock()
    return RedshiftAdapter(mock_config, mock_mp_context)


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

    def test_cross_db_allowed_with_datasharing(self, adapter):
        adapter.config.credentials.datasharing = True
        assert adapter.verify_database("other_db") == ""

    def test_cross_db_allowed_with_both_configs(self, adapter):
        adapter.config.credentials.ra3_node = True
        adapter.config.credentials.datasharing = True
        assert adapter.verify_database("other_db") == ""

    def test_cross_db_blocked_without_either_config(self, adapter):
        adapter.config.credentials.ra3_node = False
        adapter.config.credentials.datasharing = False
        with pytest.raises(dbt_common.exceptions.NotImplementedError, match="Cross-db"):
            adapter.verify_database("other_db")
