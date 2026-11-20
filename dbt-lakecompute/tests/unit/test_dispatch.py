from types import SimpleNamespace

from dbt.adapters.factory import FACTORY, AdapterContainer, get_adapter_type_names


def make_container() -> AdapterContainer:
    container = AdapterContainer()
    container.load_plugin("lakecompute")
    return container


def test_lakecompute_dispatches_to_duckdb_before_default():
    assert make_container().get_adapter_type_names("lakecompute") == ["lakecompute", "duckdb"]


def test_module_level_helper_is_patched():
    FACTORY.load_plugin("lakecompute")
    assert get_adapter_type_names("lakecompute") == ["lakecompute", "duckdb"]


def test_other_adapters_are_unchanged():
    container = AdapterContainer()
    container.plugins["other"] = SimpleNamespace(  # type: ignore[assignment]
        adapter=SimpleNamespace(type=lambda: "other"), dependencies=[]
    )
    assert container.get_adapter_type_names("other") == ["other"]
