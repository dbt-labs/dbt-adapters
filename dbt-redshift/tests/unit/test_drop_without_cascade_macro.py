from types import SimpleNamespace

import jinja2


def _load_metadata_helpers(adapter, return_fn=lambda x: x):
    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader("src/dbt/include/redshift/macros/metadata"),
    )
    template = env.get_template("helpers.sql")
    return template.make_module({"adapter": adapter, "return": return_fn})


def _load_drop_macro(subdir, filename, helper_fn):
    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader(f"src/dbt/include/redshift/macros/relations/{subdir}"),
    )
    template = env.get_template(filename)
    return template.make_module({"redshift__drop_without_cascade": helper_fn})


def test_helper_macro_returns_true():
    adapter = SimpleNamespace(drop_without_cascade=lambda: True)
    macros = _load_metadata_helpers(adapter)
    assert macros.redshift__drop_without_cascade().strip() == "True"


def test_helper_macro_returns_false():
    adapter = SimpleNamespace(drop_without_cascade=lambda: False)
    macros = _load_metadata_helpers(adapter)
    assert macros.redshift__drop_without_cascade().strip() == "False"


# Drop macros: assert cascade is included/omitted based on the helper value.

DROP_MACROS = [
    ("table", "drop.sql", "redshift__drop_table", "drop table if exists my_rel"),
    ("view", "drop.sql", "redshift__drop_view", "drop view if exists my_rel"),
    (
        "materialized_view",
        "drop.sql",
        "redshift__drop_materialized_view",
        "drop materialized view if exists my_rel",
    ),
]


def _render(subdir, filename, macro_name, drop_without_cascade):
    macros = _load_drop_macro(subdir, filename, lambda: drop_without_cascade)
    return getattr(macros, macro_name)("my_rel")


def test_drop_macros_include_cascade_by_default():
    for subdir, filename, macro_name, prefix in DROP_MACROS:
        rendered = _render(subdir, filename, macro_name, False).strip()
        assert rendered == f"{prefix} cascade", rendered


def test_drop_macros_omit_cascade_when_opted_in():
    for subdir, filename, macro_name, prefix in DROP_MACROS:
        rendered = _render(subdir, filename, macro_name, True).strip()
        assert rendered == prefix, rendered
