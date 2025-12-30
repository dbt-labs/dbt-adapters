from dbt.adapters.base import AdapterPlugin

from dbt.adapters.hologres.column import HologresColumn
from dbt.adapters.hologres.connections import HologresConnectionManager, HologresCredentials
from dbt.adapters.hologres.impl import HologresAdapter
from dbt.adapters.hologres.relation import HologresRelation
from dbt.include import hologres


Plugin = AdapterPlugin(
    adapter=HologresAdapter,  # type: ignore
    credentials=HologresCredentials,
    include_path=hologres.PACKAGE_PATH,
)
