from dbt.adapters.athena.connections import AthenaConnectionManager, AthenaCredentials
from dbt.adapters.athena.impl import AthenaAdapter
from dbt.adapters.base import AdapterPlugin
from dbt.include import athena

Plugin: AdapterPlugin = AdapterPlugin(
    adapter=AthenaAdapter,  # type:ignore
    credentials=AthenaCredentials,
    include_path=athena.PACKAGE_PATH,
)

__all__ = [
    "AthenaConnectionManager",
    "AthenaCredentials",
    "AthenaAdapter",
    "Plugin",
]


# check to see if this package was imported indirectly via dbt-athena-community
try:
    # if this is successful, raise a warning to point users to use this package directly
    import dbt.adapters.athena_community
    import warnings

    warnings.warn(
        "dbt-athena-community will be deprecated in favor of dbt-athena following version 1.9.x. To continue using new features, please run `pip install dbt-athena` instead.",
        DeprecationWarning,
    )

except ImportError:
    # if the import was unsuccessful, dbt-athena was installed directly, which is the desired state
    pass
