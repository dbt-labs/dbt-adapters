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

PG_USER_COLUMNS = ["grantee", "privilege_type"]
PG_USER_TYPES = [agate.Text()] * len(PG_USER_COLUMNS)


def _make_show_grants_table(rows):
    return agate.Table(rows, column_names=SHOW_GRANTS_COLUMNS, column_types=SHOW_GRANTS_TYPES)


def _make_svv_table(rows):
    return agate.Table(rows, column_names=SVV_COLUMNS, column_types=SVV_TYPES)


def _make_pg_user_table(rows):
    return agate.Table(rows, column_names=PG_USER_COLUMNS, column_types=PG_USER_TYPES)


@pytest.fixture
def adapter(mocker):
    mock_config = mocker.MagicMock()
    mock_config.credentials.datasharing = False
    mock_config.credentials.user = "dbt_runner"
    mock_config.flags = {}
    mock_mp_context = mocker.MagicMock()
    a = RedshiftAdapter(mock_config, mock_mp_context)
    # Explicit defaults: extended off, datasharing off.  Individual test classes override as needed.
    a.behavior.redshift_grants_extended = mocker.MagicMock(no_warn=False)
    return a


class TestStandardizeGrantsDictShowApi:
    """Extended path: grants_extended=True, use_show_apis=True (SHOW GRANTS)."""

    @pytest.fixture(autouse=True)
    def set_flags(self, adapter, mocker):
        adapter.behavior.redshift_grants_extended = mocker.MagicMock(no_warn=True)
        adapter.config.credentials.datasharing = True

    def test_includes_all_privileges(self, adapter):
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

    def test_reserved_roles_skipped(self, adapter):
        """Reserved roles (ds:*, sys:*) cannot be modified via GRANT/REVOKE."""
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
                    "200",
                    "ds:named_datashare",
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
                    "300",
                    "sys:dba",
                    "role",
                    "f",
                    "TABLE",
                    "dev",
                    "admin",
                ),
            ]
        )
        result = adapter.standardize_grants_dict(table)
        assert result == {"select": ["user:alice"]}

    def test_current_user_excluded(self, adapter):
        """The dbt runner is excluded to avoid spurious REVOKE-self drift."""
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
                    "DBT_RUNNER",  # case variation — filter is case-insensitive
                    "user",
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
        table = _make_show_grants_table([])
        result = adapter.standardize_grants_dict(table)
        assert result == {}


class TestStandardizeGrantsDictSvv:
    """Extended path: grants_extended=True, use_show_apis=False (svv_relation_privileges)."""

    @pytest.fixture(autouse=True)
    def set_flags(self, adapter, mocker):
        adapter.behavior.redshift_grants_extended = mocker.MagicMock(no_warn=True)
        adapter.config.credentials.datasharing = False

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

    def test_reserved_roles_skipped(self, adapter):
        """Reserved roles (ds:*, sys:*) cannot be modified via GRANT/REVOKE."""
        table = _make_svv_table(
            [
                ("alice", "user", "SELECT"),
                ("ds:named_datashare", "role", "SELECT"),
                ("sys:superuser", "role", "SELECT"),
            ]
        )
        result = adapter.standardize_grants_dict(table)
        assert result == {"select": ["user:alice"]}

    def test_empty_table(self, adapter):
        table = _make_svv_table([])
        result = adapter.standardize_grants_dict(table)
        assert result == {}


class TestStandardizeGrantsDictLegacyNoShowApis:
    """Legacy path: grants_extended=False, use_show_apis=False (pg_user + has_table_privilege).

    The SQL query returns a plain 'grantee' column; super() is called and returns
    plain usernames with no prefix.  Groups and roles are not visible in this path.
    """

    @pytest.fixture(autouse=True)
    def set_flags(self, adapter, mocker):
        adapter.behavior.redshift_grants_extended = mocker.MagicMock(no_warn=False)
        adapter.config.credentials.datasharing = False

    def test_returns_plain_names(self, adapter):
        table = _make_pg_user_table(
            [
                ("alice", "select"),
                ("bob", "select"),
                ("alice", "insert"),
            ]
        )
        result = adapter.standardize_grants_dict(table)
        assert result == {
            "select": ["alice", "bob"],
            "insert": ["alice"],
        }

    def test_empty_table(self, adapter):
        table = _make_pg_user_table([])
        result = adapter.standardize_grants_dict(table)
        assert result == {}


class TestStandardizeGrantsDictLegacyWithShowApis:
    """Legacy path: grants_extended=False, use_show_apis=True (SHOW GRANTS, plain names).

    Only user rows are returned (groups and roles are filtered out). Plain
    identity_name values are returned with no prefix, matching the legacy
    user-only behavior.
    """

    @pytest.fixture(autouse=True)
    def set_flags(self, adapter, mocker):
        adapter.behavior.redshift_grants_extended = mocker.MagicMock(no_warn=False)
        adapter.config.credentials.datasharing = True

    def test_returns_plain_identity_names(self, adapter):
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
            ]
        )
        result = adapter.standardize_grants_dict(table)
        # Groups (identity_type='role') are filtered out in legacy mode
        assert result == {"select": ["alice"]}

    def test_current_user_excluded(self, adapter):
        """The dbt runner (credentials.user) is excluded, matching pg_user path behaviour."""
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
                    "dbt_runner",
                    "user",
                    "f",
                    "TABLE",
                    "dev",
                    "admin",
                ),
            ]
        )
        result = adapter.standardize_grants_dict(table)
        assert result == {"select": ["alice"]}

    def test_non_users_filtered(self, adapter):
        """Groups and roles are not returned in the legacy show_apis path."""
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
                    "readonly_role",
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
                    "/readonly_group",
                    "role",
                    "f",
                    "TABLE",
                    "dev",
                    "admin",
                ),
            ]
        )
        result = adapter.standardize_grants_dict(table)
        assert result == {"select": ["alice"]}

    def test_empty_table(self, adapter):
        table = _make_show_grants_table([])
        result = adapter.standardize_grants_dict(table)
        assert result == {}
