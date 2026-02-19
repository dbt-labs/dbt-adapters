from types import SimpleNamespace

from dbt_common.behavior_flags import BehaviorFlagRendered
from dbt.adapters.redshift.impl import REDSHIFT_USE_SHOW_APIS
import jinja2


def _load_redshift_metadata_helpers_macros(adapter, return_fn=lambda x: x):
    """Load the metadata/helpers.sql macros with the given adapter and return callable."""
    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader("src/dbt/include/redshift/macros/metadata"),
    )
    template = env.get_template("helpers.sql")
    return template.make_module({"adapter": adapter, "return": return_fn})


def test_use_show_apis_macro_returns_true_when_flag_enabled():
    adapter = SimpleNamespace(
        behavior=SimpleNamespace(
            redshift_use_show_apis=BehaviorFlagRendered(
                REDSHIFT_USE_SHOW_APIS,
                {REDSHIFT_USE_SHOW_APIS["name"]: True},
            )
        ),
    )
    assert adapter.behavior.redshift_use_show_apis.no_warn is True

    macros = _load_redshift_metadata_helpers_macros(adapter)
    result = macros.redshift__use_show_apis()
    assert result.strip() == "True"


def test_use_show_apis_macro_returns_false_when_flag_disabled():
    adapter = SimpleNamespace(
        behavior=SimpleNamespace(
            redshift_use_show_apis=BehaviorFlagRendered(REDSHIFT_USE_SHOW_APIS, {})
        ),
    )
    assert adapter.behavior.redshift_use_show_apis.no_warn is False

    macros = _load_redshift_metadata_helpers_macros(adapter)
    result = macros.redshift__use_show_apis()
    assert result.strip() == "False"
