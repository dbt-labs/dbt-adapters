import ast
import os
import re
import unittest
from unittest import mock

from jinja2 import Environment, FileSystemLoader

MACROS_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "../../src/dbt/include/snowflake/macros")
)
RELATION = "test_db.test_schema.test_table"


class TestCreateTableReplaceGuard(unittest.TestCase):
    """Renders the real CTAS macros from create.sql with should_replace_on_create
    stubbed, and checks that the replace decision reaches the emitted DDL."""

    def setUp(self):
        self.jinja_env = Environment(
            loader=FileSystemLoader(MACROS_DIR), extensions=["jinja2.ext.do"]
        )
        self.catalog_relation = mock.Mock(
            is_transient=True,
            cluster_by=None,
            automatic_clustering=False,
            change_tracking=None,
            partition_by=None,
            external_volume=None,
            base_location=None,
            iceberg_version=None,
            storage_serialization_policy=None,
            max_data_extension_time_in_days=None,
            data_retention_time_in_days=None,
        )
        self.should_replace = mock.Mock(return_value=True)
        self.config_values = {"contract": mock.Mock(enforced=False)}
        self.context = {
            "adapter": mock.Mock(**{"build_catalog_relation.return_value": self.catalog_relation}),
            "config": mock.Mock(
                model=mock.Mock(),
                get=lambda key, default=None: self.config_values.get(key, default),
            ),
            "exceptions": mock.Mock(),
            # dbt's return() hands back the value; render None as "" so falsy checks hold.
            "return": lambda r: "" if r is None else r,
            "should_replace_on_create": self.should_replace,
            "resolve_model_name": lambda relation: RELATION,
        }
        optional_template = self.jinja_env.get_template("utils/optional.sql", globals=self.context)
        self.context["optional"] = optional_template.module.optional
        self.relation = mock.Mock()
        self.relation.__str__ = lambda self: RELATION

    def _render(self, macro_name, *args):
        template = self.jinja_env.get_template("relations/table/create.sql", globals=self.context)
        rendered = getattr(template.module, macro_name)(*args)
        return re.sub(r"\s+", " ", str(rendered)).strip()

    def test_transient_ctas_creates_without_replace(self):
        self.should_replace.return_value = False
        ddl = self._render("snowflake__create_table_info_schema_sql", self.relation, "select 1")
        self.assertIn(f"create transient table {RELATION}", ddl)
        self.assertNotIn("or replace", ddl)
        self.should_replace.assert_called_once_with(self.relation)

    def test_transient_ctas_keeps_replace(self):
        ddl = self._render("snowflake__create_table_info_schema_sql", self.relation, "select 1")
        self.assertIn(f"create or replace transient table {RELATION}", ddl)

    def test_permanent_ctas_creates_without_replace(self):
        self.catalog_relation.is_transient = False
        self.should_replace.return_value = False
        ddl = self._render("snowflake__create_table_info_schema_sql", self.relation, "select 1")
        self.assertIn(f"create table {RELATION}", ddl)
        self.assertNotIn("or replace", ddl)

    def test_built_in_iceberg_ctas_creates_without_replace(self):
        self.should_replace.return_value = False
        ddl = self._render("snowflake__create_table_built_in_sql", self.relation, "select 1")
        self.assertIn(f"create iceberg table {RELATION}", ddl)
        self.assertNotIn("or replace", ddl)
        self.should_replace.assert_called_once_with(self.relation)

    def test_built_in_iceberg_ctas_keeps_replace(self):
        ddl = self._render("snowflake__create_table_built_in_sql", self.relation, "select 1")
        self.assertIn(f"create or replace iceberg table {RELATION}", ddl)

    def test_temporary_ctas_does_not_consult_decision(self):
        ddl = self._render("snowflake__create_table_temporary_sql", self.relation, "select 1")
        self.assertIn(f"create or replace temporary table {RELATION}", ddl)
        self.should_replace.assert_not_called()

    def test_transient_tmp_relation_ctas_does_not_consult_decision(self):
        ddl = self._render("snowflake__create_table_transient_sql", self.relation, "select 1")
        self.assertIn(f"create or replace transient table {RELATION}", ddl)
        self.should_replace.assert_not_called()

    def test_python_write_errors_if_exists_when_not_replacing(self):
        self.should_replace.return_value = False
        code = self._render("py_write_table", "def model(dbt, session): pass", self.relation)
        self.assertIn('df.write.mode("errorifexists").save_as_table(', code)
        self.should_replace.assert_called_once_with(self.relation)

    def test_python_write_overwrites_when_replacing(self):
        code = self._render("py_write_table", "def model(dbt, session): pass", self.relation)
        self.assertIn('df.write.mode("overwrite").save_as_table(', code)

    def test_python_temporary_write_overwrites_without_consulting_decision(self):
        code = self._render("py_write_table", "def model(dbt, session): pass", self.relation, True)
        self.assertIn('df.write.mode("overwrite").save_as_table(', code)
        self.should_replace.assert_not_called()

    def _enable_copy_options(self, grants=True, tags=True):
        self.config_values.update({"copy_grants": grants, "copy_tags": tags})

    def test_plain_create_omits_copy_grants_and_copy_tags(self):
        """Snowflake rejects COPY GRANTS / COPY TAGS on a CTAS without OR REPLACE, and on this
        path there is no replaced table to copy from."""
        self._enable_copy_options()
        self.should_replace.return_value = False
        for macro_name in (
            "snowflake__create_table_info_schema_sql",
            "snowflake__create_table_built_in_sql",
        ):
            with self.subTest(macro=macro_name):
                ddl = self._render(macro_name, self.relation, "select 1")
                self.assertNotIn("or replace", ddl)
                self.assertNotIn("copy grants", ddl)
                self.assertNotIn("copy tags", ddl)

    def test_replace_keeps_copy_grants_and_copy_tags(self):
        self._enable_copy_options()
        for macro_name in (
            "snowflake__create_table_info_schema_sql",
            "snowflake__create_table_built_in_sql",
        ):
            with self.subTest(macro=macro_name):
                ddl = self._render(macro_name, self.relation, "select 1")
                self.assertIn("or replace", ddl)
                self.assertIn("copy grants", ddl)
                self.assertIn("copy tags", ddl)

    def test_replace_emits_only_the_enabled_copy_option(self):
        for grants, tags in ((True, False), (False, True)):
            self._enable_copy_options(grants=grants, tags=tags)
            with self.subTest(copy_grants=grants, copy_tags=tags):
                ddl = self._render(
                    "snowflake__create_table_info_schema_sql", self.relation, "select 1"
                )
                self.assertEqual("copy grants" in ddl, grants)
                self.assertEqual("copy tags" in ddl, tags)

    def test_python_model_code_starts_at_column_zero(self):
        """The global create_table_as indents the dispatched macro's output, so py_write_table
        must keep a newline before the model code; otherwise `def model` is indented and the
        stored procedure fails with IndentationError."""
        model_code = "def model(dbt, session):\n    return session.sql('select 1')"
        cases = ((False, False), (True, False), (True, True))  # (replace, temporary)
        for replace, temporary in cases:
            self.should_replace.return_value = replace
            with self.subTest(replace=replace, temporary=temporary):
                template = self.jinja_env.get_template(
                    "relations/table/create.sql", globals=self.context
                )
                raw = str(template.module.py_write_table(model_code, self.relation, temporary))
                generated = "\n    " + raw  # what the global create_table_as wrapper emits
                ast.parse(generated)
                self.assertIn("\ndef model(dbt, session):", generated)
