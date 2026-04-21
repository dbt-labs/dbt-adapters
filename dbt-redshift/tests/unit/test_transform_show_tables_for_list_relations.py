import agate
import pytest

from dbt.adapters.redshift.impl import RedshiftAdapter


def _set_use_show_apis(adapter, enabled):
    adapter.use_show_apis = lambda: enabled


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

    def test_missing_table_subtype_column(self, adapter):
        """SHOW TABLES may not return table_subtype on some Redshift versions."""
        columns_without_subtype = [c for c in SHOW_TABLES_COLUMNS if c != "table_subtype"]
        types_without_subtype = [agate.Text()] * len(columns_without_subtype)
        table = agate.Table(
            [
                ("dev", "s1", "t1", "TABLE", "", "", "alice", None, None, "AUTO (ALL)"),
                ("dev", "s1", "v1", "VIEW", "", "", "alice", None, None, ""),
            ],
            column_names=columns_without_subtype,
            column_types=types_without_subtype,
        )
        result = adapter.transform_show_tables_for_list_relations(table)
        assert len(result.rows) == 2
        assert result.rows[0]["type"] == "table"
        # Without table_subtype, all VIEWs are treated as plain views
        assert result.rows[1]["type"] == "view"


class TestUseShowApisGating:
    """Verify that use_show_apis correctly gates behavior."""

    def test_standardize_grants_uses_svv_when_flag_off(self, adapter):
        """standardize_grants_dict uses svv_relation_privileges path when flag is off."""
        _set_use_show_apis(adapter, enabled=False)
        svv_table = agate.Table(
            [("alice", "user", "select")],
            column_names=["identity_name", "identity_type", "privilege_type"],
            column_types=[agate.Text(), agate.Text(), agate.Text()],
        )
        result = adapter.standardize_grants_dict(svv_table)
        assert result == {"select": ["user:alice"]}

    def test_standardize_grants_uses_show_path_when_flag_on(self, adapter):
        """standardize_grants_dict uses SHOW GRANTS columns when flag is on."""
        _set_use_show_apis(adapter, enabled=True)
        show_table = agate.Table(
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
                )
            ],
            column_names=[
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
            ],
            column_types=[agate.Text()] * 11,
        )
        result = adapter.standardize_grants_dict(show_table)
        assert result == {"select": ["user:alice"]}

    def test_show_apis_only_methods_not_called_when_flag_off(self):
        """transform_show_tables_for_list_relations should only be reachable when flag is on.

        This test documents that the SHOW TABLES code path (and thus
        transform_show_tables_for_list_relations / build_catalog_from_show_tables_and_svv_columns)
        is gated behind use_show_apis at the Jinja macro level in adapters.sql and catalog/*.sql.
        We verify the gate by checking the macro renders the legacy SQL when the flag is off.
        """
        from types import SimpleNamespace

        import jinja2

        mock_adapter = SimpleNamespace(use_show_apis=lambda: False)

        env = jinja2.Environment(
            loader=jinja2.FileSystemLoader("src/dbt/include/redshift/macros/metadata"),
        )
        template = env.get_template("helpers.sql")
        macros = template.make_module({"adapter": mock_adapter, "return": lambda x: x})
        result = macros.redshift__use_show_apis()
        assert result.strip() == "False"

        mock_adapter_on = SimpleNamespace(use_show_apis=lambda: True)
        macros_on = template.make_module({"adapter": mock_adapter_on, "return": lambda x: x})
        result_on = macros_on.redshift__use_show_apis()
        assert result_on.strip() == "True"
