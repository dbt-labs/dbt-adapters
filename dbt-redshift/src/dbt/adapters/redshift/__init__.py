from dbt.adapters.base import AdapterPlugin

from dbt.adapters.redshift.connections import (  # noqa: F401
    RedshiftConnectionManager,
    RedshiftCredentials,
)

from dbt.adapters.redshift.relation import RedshiftRelation  # noqa: F401
from dbt.adapters.redshift.impl import RedshiftAdapter
from dbt.include import redshift


Plugin: AdapterPlugin = AdapterPlugin(
    adapter=RedshiftAdapter,  # type: ignore
    credentials=RedshiftCredentials,
    include_path=redshift.PACKAGE_PATH,
    dependencies=["postgres"],
)
