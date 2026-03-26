from types import SimpleNamespace

import jinja2


def _load_redshift_metadata_helpers_macros(adapter, return_fn=lambda x: x):
    """Load the metadata/helpers.sql macros with the given adapter and return callable."""
    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader("src/dbt/include/redshift/macros/metadata"),
    )
    template = env.get_template("helpers.sql")
    return template.make_module({"adapter": adapter, "return": return_fn})


def test_use_show_apis_macro_returns_true():
    adapter = SimpleNamespace(
        use_show_apis=lambda: True,
    )

    macros = _load_redshift_metadata_helpers_macros(adapter)
    result = macros.redshift__use_show_apis()
    assert result.strip() == "True"


def test_use_show_apis_macro_returns_false():
    adapter = SimpleNamespace(
        use_show_apis=lambda: False,
    )

    macros = _load_redshift_metadata_helpers_macros(adapter)
    result = macros.redshift__use_show_apis()
    assert result.strip() == "False"
