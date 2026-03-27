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

SVV_COLUMNS = ["identity_name", "identity_type", "privilege_type"]
SVV_TYPES = [agate.Text()] * len(SVV_COLUMNS)


def _make_show_grants_table(rows):
    return agate.Table(rows, column_names=SHOW_GRANTS_COLUMNS, column_types=SHOW_GRANTS_TYPES)


def _make_svv_table(rows):
    return agate.Table(rows, column_names=SVV_COLUMNS, column_types=SVV_TYPES)


@pytest.fixture
def adapter(mocker):
    mock_config = mocker.MagicMock()
    mock_config.credentials.datasharing = False
    mock_mp_context = mocker.MagicMock()
    return RedshiftAdapter(mock_config, mock_mp_context)


class TestStandardizeGrantsDictShowApi:
    """Tests for standardize_grants_dict when use_show_apis() is True (SHOW GRANTS path)."""

    def test_includes_all_privileges(self, adapter):
        adapter.config.credentials.datasharing = True
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
                    "INSERT",
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
                    "102",
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
            "insert": ["user:alice"],
            "truncate": ["user:bob"],
        }

    def test_group_detected_by_slash_prefix(self, adapter):
        """SHOW GRANTS reports groups as identity_type='role' with '/' prefix on identity_name."""
        adapter.config.credentials.datasharing = True
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
                    "SELECT",
                    "102",
                    "/readonly_group",
                    "role",
                    "f",
                    "TABLE",
                    "dev",
                    "admin",
                ),
                (
                    "public",
                    "t1",
                    "TABLE",
                    "SELECT",
                    "103",
                    "readonly_role",
                    "role",
                    "f",
                    "TABLE",
                    "dev",
                    "admin",
                ),
            ]
        )
        result = adapter.standardize_grants_dict(table)
        assert result == {
            "select": ["user:alice", "group:readonly_group", "role:readonly_role"],
        }

    def test_public_grants_skipped(self, adapter):
        """PUBLIC grants are not configurable via dbt and should be excluded."""
        adapter.config.credentials.datasharing = True
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
                    "SELECT",
                    "0",
                    "PUBLIC",
                    "public",
                    "f",
                    "TABLE",
                    "dev",
                    "admin",
                ),
            ]
        )
        result = adapter.standardize_grants_dict(table)
        assert result == {"select": ["user:alice"]}

    def test_empty_table(self, adapter):
        adapter.config.credentials.datasharing = True
        table = _make_show_grants_table([])
        result = adapter.standardize_grants_dict(table)
        assert result == {}


class TestStandardizeGrantsDictSvv:
    """Tests for standardize_grants_dict when use_show_apis() is False (SVV path)."""

    def test_distinguishes_users_groups_roles(self, adapter):
        table = _make_svv_table(
            [
                ("alice", "user", "SELECT"),
                ("readonly_group", "group", "SELECT"),
                ("readonly_role", "role", "SELECT"),
            ]
        )
        result = adapter.standardize_grants_dict(table)
        assert result == {
            "select": ["user:alice", "group:readonly_group", "role:readonly_role"],
        }

    def test_multiple_privileges(self, adapter):
        table = _make_svv_table(
            [
                ("alice", "user", "SELECT"),
                ("alice", "user", "INSERT"),
                ("bob", "user", "SELECT"),
            ]
        )
        result = adapter.standardize_grants_dict(table)
        assert result == {
            "select": ["user:alice", "user:bob"],
            "insert": ["user:alice"],
        }

    def test_public_grants_skipped(self, adapter):
        """PUBLIC grants are not configurable via dbt and should be excluded."""
        table = _make_svv_table(
            [
                ("alice", "user", "SELECT"),
                ("PUBLIC", "public", "SELECT"),
            ]
        )
        result = adapter.standardize_grants_dict(table)
        assert result == {"select": ["user:alice"]}

    def test_empty_table(self, adapter):
        table = _make_svv_table([])
        result = adapter.standardize_grants_dict(table)
        assert result == {}
