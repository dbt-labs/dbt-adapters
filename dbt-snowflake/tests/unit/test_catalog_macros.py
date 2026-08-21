import os
import re
import unittest
from unittest import mock

from jinja2 import Environment, FileSystemLoader

MACROS_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "../../src/dbt/include/snowflake/macros")
)

INFORMATION_SCHEMA = '"my_database".information_schema'

# fragments unique to each projection, used to count how many times it was inlined
TABLES_PROJECTION_MARKER = '"stats:clustering_key:label"'
COLUMNS_PROJECTION_MARKER = '"column_index"'


class MockRelation:
    def __init__(self, schema, identifier=None):
        self.schema = schema
        self.identifier = identifier


class MockCompilerError(Exception):
    """Stands in for dbt's compiler error so that raising actually aborts rendering."""


class TestSnowflakeCatalogMacros(unittest.TestCase):
    def setUp(self):
        self.jinja_env = Environment(
            loader=FileSystemLoader(MACROS_DIR),
            extensions=["jinja2.ext.do"],
        )
        exceptions = mock.Mock()
        exceptions.raise_compiler_error.side_effect = MockCompilerError
        self.adapter = mock.Mock()
        self.set_scan_per_schema(True)
        self.default_context = {
            "exceptions": exceptions,
            "adapter": self.adapter,
            "return": lambda r: r,
        }
        self.template = self.jinja_env.get_template("catalog.sql", globals=self.default_context)

    def set_scan_per_schema(self, enabled):
        self.adapter.behavior.snowflake_catalog_scan_per_schema.no_warn = enabled

    def __run_macro(self, name, *args):
        value = getattr(self.template.module, name)(*args)
        return re.sub(r"\s+", " ", value.strip())

    # -- the per-schema scans -------------------------------------------------

    def test_schemas_emit_one_scan_per_schema(self):
        sql = self.__run_macro(
            "snowflake__pruned_catalog_scan_by_schemas_sql",
            INFORMATION_SCHEMA,
            "tables",
            {"foo", "bar"},
        )

        # one pruned scan per schema, unioned -- never a disjunction across schemas.
        # note the unquoted `table_schema`: see `test_pruned_scans_do_not_quote_columns`
        assert sql == (
            '( select * from "my_database".information_schema.tables '
            "where ((table_schema ilike 'BAR' and upper(table_schema) = upper('BAR') )) "
            "union all "
            'select * from "my_database".information_schema.tables '
            "where ((table_schema ilike 'FOO' and upper(table_schema) = upper('FOO') )) "
            ") as pruned_tables"
        )

    def test_single_schema_emits_no_union(self):
        sql = self.__run_macro(
            "snowflake__pruned_catalog_scan_by_schemas_sql", INFORMATION_SCHEMA, "tables", {"foo"}
        )
        assert "union" not in sql

    def test_schemas_deduplicated_case_insensitively(self):
        sql = self.__run_macro(
            "snowflake__pruned_catalog_scan_by_schemas_sql",
            INFORMATION_SCHEMA,
            "tables",
            ["foo", "FOO"],
        )
        assert "union" not in sql

    def test_relations_grouped_by_schema(self):
        relations = [
            MockRelation("foo", "table_a"),
            MockRelation("bar", "table_b"),
            MockRelation("foo", "table_c"),
        ]
        sql = self.__run_macro(
            "snowflake__pruned_catalog_scan_by_relations_sql",
            INFORMATION_SCHEMA,
            "columns",
            relations,
        )

        # two schemas -> two scans, and the schema match is a top-level conjunct in each
        assert sql.count("union all") == 1
        assert sql.count("information_schema.columns") == 2
        assert sql.count("ilike 'foo'") == 1
        assert sql.count("ilike 'bar'") == 1
        assert "table_a" in sql and "table_b" in sql and "table_c" in sql

    def test_relations_sql_does_not_depend_on_input_order(self):
        # `relations` reaches the macro as a set, so the emitted sql must not depend on ordering
        a = MockRelation("foo", "table_a")
        b = MockRelation("bar", "table_b")
        c = MockRelation("foo", "table_c")

        def render(relations):
            return self.__run_macro(
                "snowflake__pruned_catalog_scan_by_relations_sql",
                INFORMATION_SCHEMA,
                "tables",
                relations,
            )

        assert render([a, b, c]) == render([c, b, a])

    def test_pruned_scans_do_not_quote_columns(self):
        """
        The pruned scans filter the information_schema view directly, and that view's real
        columns are uppercase. `"table_schema"` only resolves on the outer select, where the
        projection has aliased it -- against the view it is `invalid identifier`, which fails
        the whole catalog query. So no predicate inside a pruned scan may be quoted.
        """
        relations = [MockRelation("foo", "table_a"), MockRelation("bar", "table_b")]
        rendered = [
            self.__run_macro(
                "snowflake__pruned_catalog_scan_by_schemas_sql",
                INFORMATION_SCHEMA,
                "tables",
                {"foo", "bar"},
            ),
            self.__run_macro(
                "snowflake__pruned_catalog_scan_by_relations_sql",
                INFORMATION_SCHEMA,
                "columns",
                relations,
            ),
        ]

        for sql in rendered:
            assert '"table_schema"' not in sql
            assert '"table_name"' not in sql
            assert "table_schema ilike" in sql

    # -- the where clause ----------------------------------------------------

    def test_relation_identifiers_share_one_schema_predicate(self):
        relations = [MockRelation("foo", "table_a"), MockRelation("foo", "table_c")]
        sql = self.__run_macro("snowflake__pruned_catalog_relations_where_clause_sql", relations)

        assert sql == (
            "where table_schema ilike 'foo' and upper(table_schema) = upper('foo') "
            "and ((table_name ilike 'table_a' and upper(table_name) = upper('table_a') ) "
            "or (table_name ilike 'table_c' and upper(table_name) = upper('table_c') ))"
        )

    def test_relation_without_identifier_selects_whole_schema(self):
        relations = [MockRelation("foo", "table_a"), MockRelation("foo")]
        sql = self.__run_macro("snowflake__pruned_catalog_relations_where_clause_sql", relations)

        assert "table_name" not in sql
        assert sql == "where table_schema ilike 'foo' and upper(table_schema) = upper('foo')"

    def test_relation_without_schema_raises(self):
        relations = [MockRelation(None, "table_a")]

        with self.assertRaises(MockCompilerError):
            self.__run_macro("snowflake__pruned_catalog_relations_where_clause_sql", relations)

        self.default_context["exceptions"].raise_compiler_error.assert_called_once_with(
            "`get_catalog_relations` requires a list of relations, each with a schema"
        )

    def test_relations_from_mixed_schemas_raise(self):
        # the where clause pins a single schema, so mixed input would silently drop the rest
        relations = [MockRelation("foo", "table_a"), MockRelation("bar", "table_b")]

        with self.assertRaises(MockCompilerError):
            self.__run_macro("snowflake__pruned_catalog_relations_where_clause_sql", relations)

        self.default_context["exceptions"].raise_compiler_error.assert_called_once_with(
            "`snowflake__pruned_catalog_relations_where_clause_sql` requires relations from a"
            " single schema, got: BAR, FOO"
        )

    # -- statement size ------------------------------------------------------

    def test_projection_is_not_repeated_per_schema(self):
        """
        Snowflake caps a statement at 1MB and the `tables` projection is ~1.5kB, so inlining it
        per schema would blow the limit at a few hundred schemas -- the exact case this flag
        targets. Only the scan may repeat.
        """
        schemas = {f"schema_{i}" for i in range(300)}

        tables = self.__run_macro(
            "snowflake__catalog_tables_by_schemas_sql", INFORMATION_SCHEMA, schemas
        )
        columns = self.__run_macro(
            "snowflake__catalog_columns_by_schemas_sql", INFORMATION_SCHEMA, schemas
        )

        assert tables.count(TABLES_PROJECTION_MARKER) == 1
        assert columns.count(COLUMNS_PROJECTION_MARKER) == 1
        assert tables.count("union all") == 299
        assert columns.count("union all") == 299

    def test_projection_is_not_repeated_per_relation_group(self):
        relations = [MockRelation(f"schema_{i}", f"table_{i}") for i in range(300)]

        tables = self.__run_macro(
            "snowflake__catalog_tables_by_relations_sql", INFORMATION_SCHEMA, relations
        )
        columns = self.__run_macro(
            "snowflake__catalog_columns_by_relations_sql", INFORMATION_SCHEMA, relations
        )

        assert tables.count(TABLES_PROJECTION_MARKER) == 1
        assert columns.count(COLUMNS_PROJECTION_MARKER) == 1
        assert tables.count("union all") == 299
        assert columns.count("union all") == 299

    def test_statement_size_stays_well_under_snowflake_limit(self):
        # 1000 schemas in one database is large but not unheard of; both ctes go in one statement
        schemas = {f"schema_{i}" for i in range(1000)}

        rendered = getattr(self.template.module, "snowflake__catalog_tables_by_schemas_sql")(
            INFORMATION_SCHEMA, schemas
        ) + getattr(self.template.module, "snowflake__catalog_columns_by_schemas_sql")(
            INFORMATION_SCHEMA, schemas
        )

        assert len(rendered) < 1_000_000

    # -- the projection macros still default to the information schema -------

    def test_projection_defaults_to_information_schema_view(self):
        tables = self.__run_macro("snowflake__get_catalog_tables_sql", INFORMATION_SCHEMA)
        columns = self.__run_macro("snowflake__get_catalog_columns_sql", INFORMATION_SCHEMA)

        assert tables.endswith('from "my_database".information_schema.tables')
        assert columns.endswith('from "my_database".information_schema.columns')

    def test_flag_does_not_change_the_projection(self):
        """
        The two ctes are joined on their projected columns, so the flag must only move the
        filtering -- never touch the select list.
        """
        for macro in (
            "snowflake__catalog_tables_by_schemas_sql",
            "snowflake__catalog_columns_by_schemas_sql",
        ):
            self.set_scan_per_schema(True)
            pruned = self.__run_macro(macro, INFORMATION_SCHEMA, {"foo", "bar"})
            self.set_scan_per_schema(False)
            legacy = self.__run_macro(macro, INFORMATION_SCHEMA, {"foo", "bar"})

            assert pruned.split(" from ")[0] == legacy.split(" from ")[0]

    # -- flag off: unchanged behaviour --------------------------------------

    def test_flag_disabled_keeps_single_scan_across_schemas(self):
        self.set_scan_per_schema(False)

        sql = self.__run_macro(
            "snowflake__catalog_tables_by_schemas_sql", INFORMATION_SCHEMA, ["foo", "bar"]
        )

        assert "union" not in sql
        # the legacy predicate stays quoted: it sits on the projection, whose aliases are
        # quoted lowercase, not on the information_schema view
        assert sql.endswith(
            'from "my_database".information_schema.tables '
            "where ((\"table_schema\" ilike 'foo' and upper(\"table_schema\") = upper('foo') ) "
            "or (\"table_schema\" ilike 'bar' and upper(\"table_schema\") = upper('bar') ))"
        )

    def test_flag_disabled_keeps_schema_identifier_pairs(self):
        self.set_scan_per_schema(False)
        relations = [MockRelation("foo", "table_a"), MockRelation("bar")]

        sql = self.__run_macro(
            "snowflake__catalog_columns_by_relations_sql", INFORMATION_SCHEMA, relations
        )

        # legacy path pairs each schema with its own identifier, so mixed schemas stay correct
        assert "union" not in sql
        assert sql.endswith(
            'from "my_database".information_schema.columns where ( ( '
            "\"table_schema\" ilike 'foo' and upper(\"table_schema\") = upper('foo') "
            "and \"table_name\" ilike 'table_a' and upper(\"table_name\") = upper('table_a') "
            ") or ( \"table_schema\" ilike 'bar' and upper(\"table_schema\") = upper('bar') ) )"
        )

    def test_flag_disabled_still_requires_a_schema(self):
        self.set_scan_per_schema(False)

        with self.assertRaises(MockCompilerError):
            self.__run_macro(
                "snowflake__catalog_tables_by_relations_sql",
                INFORMATION_SCHEMA,
                [MockRelation(None, "table_a")],
            )
