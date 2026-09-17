from types import SimpleNamespace

import jinja2


def _render_alter_column_type(column_name, new_column_type, skip_txn=False):
    """Render redshift__alter_column_type and return the SQL it would execute."""
    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader("src/dbt/include/redshift/macros"),
        extensions=["jinja2.ext.do"],
    )
    template = env.get_template("adapters.sql")

    statements = []

    # jinja passes a `{% call %}` block's body as `caller`, which is how we
    # capture the SQL the macro would have executed.
    def statement_macro(name, caller=None, **kwargs):
        statements.append(caller())
        return ""

    behavior = SimpleNamespace(
        redshift_skip_autocommit_transaction_statements=SimpleNamespace(no_warn=skip_txn)
    )
    adapter = SimpleNamespace(behavior=behavior, quote=lambda value: f'"{value}"')
    relation = SimpleNamespace(render=lambda: "my_db.my_schema.my_table")

    macros = template.make_module({"adapter": adapter, "statement": statement_macro})
    macros.redshift__alter_column_type(relation, column_name, new_column_type)
    return "\n".join(statements)


def test_varchar_uses_native_alter_when_skipping_transactions():
    rendered = _render_alter_column_type("my_col", "varchar(500)", skip_txn=True)
    assert "alter column" in rendered
    assert "type varchar(500)" in rendered
    assert "__dbt_alter" not in rendered
    assert "cast(" not in rendered


def test_varbyte_uses_native_alter_when_skipping_transactions():
    rendered = _render_alter_column_type("my_col", "varbyte(128)", skip_txn=True)
    assert "alter column" in rendered
    assert "__dbt_alter" not in rendered


def test_character_varying_uses_native_alter_when_skipping_transactions():
    rendered = _render_alter_column_type("my_col", "character varying(500)", skip_txn=True)
    assert "alter column" in rendered
    assert "__dbt_alter" not in rendered


def test_varchar_migrates_with_cast_when_not_skipping_transactions():
    # Native ALTER COLUMN TYPE cannot run inside a transaction block, so even
    # varchar takes the migration path when the behavior flag is off.
    rendered = _render_alter_column_type("my_col", "varchar(500)")
    assert 'add column "my_col__dbt_alter" varchar(500)' in rendered
    assert 'set "my_col__dbt_alter" = cast("my_col" as varchar(500))' in rendered


def test_cross_category_change_casts_explicitly():
    rendered = _render_alter_column_type("my_col", "bigint")
    assert 'set "my_col__dbt_alter" = cast("my_col" as bigint)' in rendered
    # the uncasted copy from default__alter_column_type is what #2159 fixes
    assert 'set "my_col__dbt_alter" = "my_col"' not in rendered


def test_multi_word_type_casts_explicitly():
    rendered = _render_alter_column_type("my_ts_col", "timestamp without time zone")
    assert (
        'set "my_ts_col__dbt_alter" = cast("my_ts_col" as timestamp without time zone)' in rendered
    )


def test_migration_emits_full_add_copy_drop_rename_sequence():
    rendered = _render_alter_column_type("my_col", "bigint")
    statements = [line.strip() for line in rendered.splitlines() if line.strip()]
    assert statements == [
        'alter table my_db.my_schema.my_table add column "my_col__dbt_alter" bigint;',
        'update my_db.my_schema.my_table set "my_col__dbt_alter" = cast("my_col" as bigint);',
        'alter table my_db.my_schema.my_table drop column "my_col" cascade;',
        'alter table my_db.my_schema.my_table rename column "my_col__dbt_alter" to "my_col"',
    ]
