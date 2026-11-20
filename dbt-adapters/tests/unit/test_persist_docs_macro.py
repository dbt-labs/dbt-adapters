from unittest.mock import Mock
import pytest


class TestValidateDocColumnsMacro:
    @pytest.fixture
    def jinja_env(self):
        """Set up Jinja environment to load actual macro files."""
        from jinja2 import Environment, FileSystemLoader

        return Environment(
            loader=FileSystemLoader("src/dbt/include/global_project/macros/adapters"),
            extensions=["jinja2.ext.do"],
        )

    @pytest.fixture
    def mock_relation(self):
        relation = Mock()
        relation.render.return_value = '"my_schema"."my_table"'
        return relation

    def _render_validate_doc_columns(self, jinja_env, relation, column_dict, existing_columns, flags=None):
        exceptions = Mock()
        exceptions.warn = Mock(return_value="")
        adapter = Mock()

        captured_return = {}

        def _mock_return(val):
            captured_return["value"] = val
            return ""

        context = {
            "adapter": adapter,
            "exceptions": exceptions,
            "return": _mock_return,
        }
        if flags is not None:
            context["flags"] = flags

        template = jinja_env.get_template("persist_docs.sql", globals=context)
        template.module.validate_doc_columns(relation, column_dict, existing_columns)
        return captured_return.get("value"), exceptions

    def test_validate_doc_columns_warns_on_missing_column_during_run(self, jinja_env, mock_relation):
        flags = Mock()
        flags.WHICH = "run"
        column_dict = {
            "id": {"quote": False},
            "new_column": {"quote": False},
        }
        existing_columns = ["id"]

        filtered, exceptions = self._render_validate_doc_columns(
            jinja_env, mock_relation, column_dict, existing_columns, flags=flags
        )

        assert filtered == {"id": {"quote": False}}
        assert exceptions.warn.called
        warn_msg = exceptions.warn.call_args[0][0]
        assert "The following columns are specified in the schema but are not present in the database: new_column" in warn_msg

    def test_validate_doc_columns_suppresses_warning_during_clone(self, jinja_env, mock_relation):
        """During dbt clone, missing columns are expected when cloning from deferred state (resolves #2182)."""
        flags = Mock()
        flags.WHICH = "clone"
        column_dict = {
            "id": {"quote": False},
            "new_column": {"quote": False},
        }
        existing_columns = ["id"]

        filtered, exceptions = self._render_validate_doc_columns(
            jinja_env, mock_relation, column_dict, existing_columns, flags=flags
        )

        assert filtered == {"id": {"quote": False}}
        assert not exceptions.warn.called

    def test_validate_doc_columns_warns_when_flags_undefined(self, jinja_env, mock_relation):
        column_dict = {
            "id": {"quote": False},
            "missing_col": {"quote": False},
        }
        existing_columns = ["id"]

        filtered, exceptions = self._render_validate_doc_columns(
            jinja_env, mock_relation, column_dict, existing_columns, flags=None
        )

        assert filtered == {"id": {"quote": False}}
        assert exceptions.warn.called

    def test_validate_doc_columns_all_columns_present(self, jinja_env, mock_relation):
        flags = Mock()
        flags.WHICH = "run"
        column_dict = {
            "id": {"quote": False},
            "name": {"quote": False},
        }
        existing_columns = ["id", "name"]

        filtered, exceptions = self._render_validate_doc_columns(
            jinja_env, mock_relation, column_dict, existing_columns, flags=flags
        )

        assert filtered == column_dict
        assert not exceptions.warn.called
