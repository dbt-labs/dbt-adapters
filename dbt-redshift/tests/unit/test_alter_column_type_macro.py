from types import SimpleNamespace

import jinja2
import pytest

DEFAULT_PATH_MARKER = "default-add-copy-drop-rename"


def _render_alter_column_type(skip_transaction_statements, new_column_type):
    """Render ``redshift__alter_column_type`` against a stub adapter.

    Returns a ``(rendered, statements)`` tuple where ``statements`` holds the SQL
    of every ``statement()`` call the macro made (i.e. the native ALTER, if any).
    """
    statements = []

    def statement(name, caller=None):
        statements.append(" ".join(caller().split()))
        return ""

    adapter = SimpleNamespace(
        skip_transaction_statements=lambda: skip_transaction_statements,
        quote=lambda identifier: f'"{identifier}"',
    )
    relation = SimpleNamespace(render=lambda: "my_schema.my_table")

    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader("src/dbt/include/redshift/macros"),
        extensions=["jinja2.ext.do"],  # adapters.sql uses {% do %}, as dbt's Jinja env does
    )
    template = env.get_template("adapters.sql")
    macros = template.make_module(
        {
            "adapter": adapter,
            "statement": statement,
            "default__alter_column_type": lambda *args: DEFAULT_PATH_MARKER,
        }
    )
    rendered = macros.redshift__alter_column_type(relation, "name", new_column_type)
    return str(rendered).strip(), statements


@pytest.mark.parametrize(
    "new_column_type",
    ["varchar(256)", "VARCHAR(256)", "character varying(256)", "varbyte(100)"],
)
def test_native_alter_used_when_transaction_statements_are_skipped(new_column_type):
    rendered, statements = _render_alter_column_type(True, new_column_type)

    assert statements == [
        f'alter table my_schema.my_table alter column "name" type {new_column_type}'
    ]
    assert DEFAULT_PATH_MARKER not in rendered


@pytest.mark.parametrize(
    "new_column_type",
    ["varchar(256)", "character varying(256)", "varbyte(100)"],
)
def test_default_path_used_when_transaction_statements_are_not_skipped(new_column_type):
    # Covers the flag being off, and the flag being on with autocommit off: in both
    # cases dbt still wraps statements in BEGIN/COMMIT, so the native ALTER would fail
    # with "ALTER TABLE ALTER COLUMN cannot run inside a transaction block".
    rendered, statements = _render_alter_column_type(False, new_column_type)

    assert statements == []
    assert rendered == DEFAULT_PATH_MARKER


@pytest.mark.parametrize("skip_transaction_statements", [True, False])
@pytest.mark.parametrize("new_column_type", ["bigint", "integer", "numeric(18,4)"])
def test_default_path_used_for_unsupported_types(skip_transaction_statements, new_column_type):
    rendered, statements = _render_alter_column_type(skip_transaction_statements, new_column_type)

    assert statements == []
    assert rendered == DEFAULT_PATH_MARKER
