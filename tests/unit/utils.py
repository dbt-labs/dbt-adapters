from dbt.adapters.factory import FACTORY


def inject_adapter(adapter, plugin):
    """
    Inject the given adapter into the factory
    so that it will be available from get_adapter() as if dbt loaded it.
    """
    plugin_key = plugin.adapter.type()
    FACTORY.plugins[plugin_key] = plugin

    adapter_key = adapter.type()
    FACTORY.adapters[adapter_key] = adapter
