from types import SimpleNamespace

import jinja2


def _render_create_table_as(temporary, dist="my_col", sort=["my_col"]):
    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader("src/dbt/include/redshift/macros"),
        extensions=["jinja2.ext.do"],
    )
    template = env.get_template("adapters.sql")

    config_values = {"dist": dist, "sort_type": None, "sort": sort, "sql_header": None, "backup": None}
    contract_config = SimpleNamespace(enforced=False)
    config = SimpleNamespace(
        get=lambda key, default=None, validator=None: (
            contract_config if key == "contract" else config_values.get(key, default)
        )
    )

    relation = SimpleNamespace(include=lambda database=True, schema=True: "my_rel")

    class _AnyValidator:
        def __getitem__(self, item):
            return None

    validation = SimpleNamespace(any=_AnyValidator())

    macros = template.make_module({"config": config, "validation": validation})
    return macros.redshift__create_table_as(temporary, relation, "select 1")


def test_dist_and_sort_omitted_for_temporary_tables():
    rendered = _render_create_table_as(temporary=True).lower()
    assert "distkey" not in rendered
    assert "sortkey" not in rendered


def test_dist_and_sort_applied_for_non_temporary_tables():
    rendered = _render_create_table_as(temporary=False).lower()
    assert "distkey" in rendered
    assert "sortkey" in rendered
