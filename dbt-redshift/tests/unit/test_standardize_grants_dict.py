import agate
import pytest

from dbt.adapters.redshift.impl import RedshiftAdapter


SHOW_GRANTS_COLUMNS = [
    "schema_name",
    "object_name",
    "object_type",
    "privilege_type",
    "identity_id",
    "identity_name",
    "identity_type",
    "admin_option",
    "privilege_scope",
    "database_name",
    "grantor_name",
]

SHOW_GRANTS_TYPES = [agate.Text()] * len(SHOW_GRANTS_COLUMNS)

LEGACY_COLUMNS = ["grantee", "privilege_type"]
LEGACY_TYPES = [agate.Text()] * len(LEGACY_COLUMNS)


def _make_show_grants_table(rows):
    return agate.Table(rows, column_names=SHOW_GRANTS_COLUMNS, column_types=SHOW_GRANTS_TYPES)


def _make_legacy_table(rows):
    return agate.Table(rows, column_names=LEGACY_COLUMNS, column_types=LEGACY_TYPES)


@pytest.fixture
def adapter(mocker):
    mock_config = mocker.MagicMock()
    mock_mp_context = mocker.MagicMock()
    return RedshiftAdapter(mock_config, mock_mp_context)


def _set_use_show_apis(adapter, mocker, enabled):
    mock_behavior = mocker.MagicMock()
    mock_behavior.redshift_use_show_apis.no_warn = enabled
    adapter._behavior = mock_behavior


class TestStandardizeGrantsDictShowApi:
    """Tests for standardize_grants_dict when redshift_use_show_apis is enabled."""

    def test_includes_all_privileges(self, adapter, mocker):
        _set_use_show_apis(adapter, mocker, enabled=True)
        table = _make_show_grants_table(
            [
                (
                    "public",
                    "t1",
                    "TABLE",
                    "SELECT",
                    "101",
                    "alice",
                    "user",
                    "f",
                    "TABLE",
                    "dev",
                    "admin",
                ),
                (
                    "public",
                    "t1",
                    "TABLE",
                    "DROP",
                    "101",
                    "alice",
                    "user",
                    "f",
                    "TABLE",
                    "dev",
                    "admin",
                ),
                (
                    "public",
                    "t1",
                    "TABLE",
                    "ALTER",
                    "101",
                    "alice",
                    "user",
                    "f",
                    "TABLE",
                    "dev",
                    "admin",
                ),
                (
                    "public",
                    "t1",
                    "TABLE",
                    "TRUNCATE",
                    "101",
                    "bob",
                    "user",
                    "f",
                    "TABLE",
                    "dev",
                    "admin",
                ),
            ]
        )
        result = adapter.standardize_grants_dict(table)
        assert result == {
            "select": ["user:alice"],
            "drop": ["user:alice"],
            "alter": ["user:alice"],
            "truncate": ["user:bob"],
        }

    def test_empty_table(self, adapter, mocker):
        _set_use_show_apis(adapter, mocker, enabled=True)
        table = _make_show_grants_table([])
        result = adapter.standardize_grants_dict(table)
        assert result == {}


class TestStandardizeGrantsDictLegacy:
    """Tests for standardize_grants_dict when redshift_use_show_apis is disabled."""

    def test_basic_grants(self, adapter, mocker):
        _set_use_show_apis(adapter, mocker, enabled=False)
        table = _make_legacy_table(
            [
                ("alice", "select"),
                ("alice", "insert"),
                ("bob", "select"),
            ]
        )
        result = adapter.standardize_grants_dict(table)
        assert result == {
            "select": ["alice", "bob"],
            "insert": ["alice"],
        }

    def test_empty_table(self, adapter, mocker):
        _set_use_show_apis(adapter, mocker, enabled=False)
        table = _make_legacy_table([])
        result = adapter.standardize_grants_dict(table)
        assert result == {}
