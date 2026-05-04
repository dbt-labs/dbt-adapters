from types import SimpleNamespace

import jinja2


def _load_metadata_helpers(adapter, return_fn=lambda x: x):
    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader("src/dbt/include/redshift/macros/metadata"),
    )
    template = env.get_template("helpers.sql")
    return template.make_module({"adapter": adapter, "return": return_fn})


class _FakeConfig:
    """Minimal stand-in for dbt's `config` proxy in macros."""

    def __init__(self, values=None):
        self._values = values or {}

    def get(self, key, default=None):
        return self._values.get(key, default)


def _load_drop_macro(subdir, filename, helper_fn, config_values=None):
    """Load a drop macro and inject `config` plus the credentials-level helper."""
    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader(f"src/dbt/include/redshift/macros/relations/{subdir}"),
    )
    template = env.get_template(filename)
    return template.make_module(
        {
            "config": _FakeConfig(config_values),
            "redshift__drop_without_cascade": helper_fn,
        }
    )


def test_helper_macro_returns_true():
    adapter = SimpleNamespace(drop_without_cascade=lambda: True)
    macros = _load_metadata_helpers(adapter)
    assert macros.redshift__drop_without_cascade().strip() == "True"


def test_helper_macro_returns_false():
    adapter = SimpleNamespace(drop_without_cascade=lambda: False)
    macros = _load_metadata_helpers(adapter)
    assert macros.redshift__drop_without_cascade().strip() == "False"


# Drop macros: assert cascade is included/omitted based on the resolved value.

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


def _render(subdir, filename, macro_name, profile_value, model_value=None):
    config_values = {} if model_value is None else {"drop_without_cascade": model_value}
    macros = _load_drop_macro(subdir, filename, lambda: profile_value, config_values=config_values)
    return getattr(macros, macro_name)("my_rel")


def test_drop_macros_include_cascade_by_default():
    for subdir, filename, macro_name, prefix in DROP_MACROS:
        rendered = _render(subdir, filename, macro_name, profile_value=False).strip()
        assert rendered.startswith(prefix), rendered
        assert "cascade" in rendered, rendered


def test_drop_macros_omit_cascade_when_profile_opts_in():
    for subdir, filename, macro_name, prefix in DROP_MACROS:
        rendered = _render(subdir, filename, macro_name, profile_value=True).strip()
        assert rendered.startswith(prefix), rendered
        assert "cascade" not in rendered, rendered


def test_drop_macros_omit_cascade_when_model_opts_in():
    """Model-level config=True wins even when profile is False."""
    for subdir, filename, macro_name, prefix in DROP_MACROS:
        rendered = _render(
            subdir, filename, macro_name, profile_value=False, model_value=True
        ).strip()
        assert rendered.startswith(prefix), rendered
        assert "cascade" not in rendered, rendered


def test_drop_macros_keep_cascade_when_model_opts_out():
    """Model-level config=False overrides a profile-level True."""
    for subdir, filename, macro_name, prefix in DROP_MACROS:
        rendered = _render(
            subdir, filename, macro_name, profile_value=True, model_value=False
        ).strip()
        assert rendered.startswith(prefix), rendered
        assert "cascade" in rendered, rendered
