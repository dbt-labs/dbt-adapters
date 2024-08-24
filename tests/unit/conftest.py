from contextlib import contextmanager
from multiprocessing import get_context
from types import SimpleNamespace
from typing import Any, ContextManager, Dict, List, Optional, Tuple

import agate
from dbt_common.behavior_flags import RawBehaviorFlag
import pytest

from dbt.adapters.base.column import Column
from dbt.adapters.base.connections import BaseConnectionManager
from dbt.adapters.base.impl import BaseAdapter
from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.contracts.connection import (
    AdapterRequiredConfig,
    AdapterResponse,
    Connection,
    ConnectionState,
    Credentials,
    QueryComment,
)


@pytest.fixture
def adapter(config, existing_relations, behavior_flags) -> BaseAdapter:

    adapter = BaseAdapterStub(config, get_context("spawn"))

    adapter.behavior = behavior_flags

    for relation in existing_relations:
        adapter.cache_added(BaseRelation.create(relation))

    return adapter


@pytest.fixture
def config(config_extra) -> AdapterRequiredConfig:
    raw_config = {
        "credentials": CredentialsStub("test_database", "test_schema"),
        "profile_name": "test_profile",
        "target_name": "test_target",
        "threads": 4,
        "project_name": "test_project",
        "query_comment": QueryComment(),
        "cli_vars": {},
        "target_path": "path/to/nowhere",
        "log_cache_events": False,
    }
    raw_config.update(config_extra)
    return SimpleNamespace(**raw_config)


@pytest.fixture
def config_extra() -> Dict[str, Any]:
    return {}


@pytest.fixture
def existing_relations() -> List[Dict[str, str]]:
    return []


@pytest.fixture
def behavior_flags() -> List[RawBehaviorFlag]:
    return []


class CredentialsStub(Credentials):
    def type(self) -> str:
        return "test"

    def _connection_keys(self):
        return {"database": self.database, "schema": self.schema}


class ConnectionManagerStub(BaseConnectionManager):

    @contextmanager
    def exception_handler(self, sql: str) -> ContextManager:  # type: ignore
        try:
            yield
        finally:
            pass

    def cancel_open(self) -> Optional[List[str]]:
        # there's no database, so there are no connections
        pass

    def open(cls, connection: Connection) -> Connection:
        # there's no database, so just change the state
        connection.state = ConnectionState.OPEN
        return connection

    def begin(self) -> None:
        # there's no database, so there are no transactions
        pass

    def commit(self) -> None:
        # there's no database, so there are no transactions
        pass

    def execute(
        self,
        sql: str,
        auto_begin: bool = False,
        fetch: bool = False,
        limit: Optional[int] = None,
    ) -> Tuple[AdapterResponse, agate.Table]:
        # there's no database, so just return the sql
        return AdapterResponse(code=sql), None


class BaseAdapterStub(BaseAdapter):

    ConnectionManager = ConnectionManagerStub

    ###
    # Abstract methods for database-specific values, attributes, and types
    ###
    @classmethod
    def date_function(cls) -> str:
        return "date_function"

    @classmethod
    def is_cancelable(cls) -> bool:
        return False

    def list_schemas(self, database: str) -> List[str]:
        return list(self.cache.schemas)

    ###
    # Abstract methods about relations
    ###
    def drop_relation(self, relation: BaseRelation) -> None:
        self.cache_dropped(relation)

    def truncate_relation(self, relation: BaseRelation) -> None:
        self.cache_dropped(relation)

    def rename_relation(self, from_relation: BaseRelation, to_relation: BaseRelation) -> None:
        self.cache_renamed(from_relation, to_relation)

    def get_columns_in_relation(self, relation: BaseRelation) -> List[Column]:
        # there's no database, so these need to be added as kwargs in the existing_relations fixture
        return relation.columns

    def expand_column_types(self, goal: BaseRelation, current: BaseRelation) -> None:
        # there's no database, so these need to be added as kwargs in the existing_relations fixture
        current.columns = goal.columns

    def list_relations_without_caching(self, schema_relation: BaseRelation) -> List[BaseRelation]:
        # there's no database, so use the cache as the database
        return self.cache.get_relations(schema_relation.database, schema_relation.schema)

    ###
    # ODBC FUNCTIONS -- these should not need to change for every adapter,
    #                   although some adapters may override them
    ###
    def create_schema(self, relation: BaseRelation):
        # there's no database, this happens implicitly by adding a relation to the cache
        pass

    def drop_schema(self, relation: BaseRelation):
        for each_relation in self.cache.get_relations(relation.database, relation.schema):
            self.cache_dropped(each_relation)

    @classmethod
    def quote(cls, identifier: str) -> str:
        quote_char = ""
        return f"{quote_char}{identifier}{quote_char}"

    ###
    # Conversions: These must be implemented by concrete implementations, for
    # converting agate types into their sql equivalents.
    ###
    @classmethod
    def convert_text_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "str"

    @classmethod
    def convert_number_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "float"

    @classmethod
    def convert_boolean_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "bool"

    @classmethod
    def convert_datetime_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "datetime"

    @classmethod
    def convert_date_type(cls, *args, **kwargs):
        return "date"

    @classmethod
    def convert_time_type(cls, *args, **kwargs):
        return "time"
