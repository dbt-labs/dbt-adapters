from dbt.adapters.base import AdapterPlugin

from dbt.adapters.postgres.column import PostgresColumn
from dbt.adapters.postgres.connections import PostgresConnectionManager, PostgresCredentials
from dbt.adapters.postgres.impl import PostgresAdapter
from dbt.adapters.postgres.relation import PostgresRelation
from dbt.include import postgres


Plugin = AdapterPlugin(
    adapter=PostgresAdapter,  # type: ignore
    credentials=PostgresCredentials,
    include_path=postgres.PACKAGE_PATH,
)
