import abc
import time
from concurrent.futures import as_completed, Future
from contextlib import contextmanager
from datetime import datetime
from enum import Enum
from importlib import import_module
from multiprocessing.context import SpawnContext
from typing import (
    Any,
    Callable,
    Dict,
    FrozenSet,
    Iterable,
    Iterator,
    List,
    Mapping,
    Optional,
    Set,
    Tuple,
    Type,
    TypedDict,
    Union,
    TYPE_CHECKING,
)
import pytz
from dbt_common.clients.jinja import CallableMacroGenerator
from dbt_common.events.functions import fire_event, warn_or_error
from dbt_common.exceptions import (
    DbtInternalError,
    DbtRuntimeError,
    DbtValidationError,
    MacroArgTypeError,
    MacroResultError,
    NotImplementedError,
    UnexpectedNullError,
)
from dbt_common.utils import (
    AttrDict,
    cast_to_str,
    executor,
    filter_null_values,
)

from dbt.adapters.base.connection.manager import BaseConnectionManager
from dbt.adapters.base.dialect import BaseDialectDefinition
from dbt.adapters.base.dialect.relation import BaseRelation
from dbt.adapters.base.execution.manager import ExecutionManager
from dbt.adapters.base.meta import AdapterMeta
from dbt.adapters.contracts.connection import AdapterResponse
from dbt.adapters.protocol import AdapterConfig, MacroContextGeneratorCallable

if TYPE_CHECKING:
    import agate

GET_CATALOG_MACRO_NAME = "get_catalog"
GET_CATALOG_RELATIONS_MACRO_NAME = "get_catalog_relations"
GET_RELATION_LAST_MODIFIED_MACRO_NAME = "get_relation_last_modified"


def _parse_callback_empty_table(*args, **kwargs) -> Tuple[str, "agate.Table"]:
    # Lazy load agate_helper to avoid importing agate when it is not necessary.
    from dbt_common.clients.agate_helper import empty_table

    return "", empty_table()


def _expect_row_value(key: str, row: "agate.Row"):
    if key not in row.keys():
        raise DbtInternalError(
            'Got a row without "{}" column, columns: {}'.format(key, row.keys())
        )
    return row[key]


class BaseAdapter(metaclass=AdapterMeta):
    """The BaseAdapter provides an abstract base class for adapters.

    Adapters must implement the following methods and macros. Some of the
    methods can be safely overridden as a noop, where it makes sense
    (transactions on databases that don't support them, for instance). Those
    methods are marked with a (passable) in their docstrings. Check docstrings
    for type information, etc.

    To implement a macro, implement "${adapter_type}__${macro_name}" in the
    adapter's internal project.

    To invoke a method in an adapter macro, call it on the 'adapter' Jinja
    object using dot syntax.

    To invoke a method in model code, add the @available decorator atop a method
    declaration. Methods are invoked as macros.

    Methods:
        - exception_handler
        - date_function
        - list_schemas
        - drop_relation
        - truncate_relation
        - rename_relation
        - get_columns_in_relation
        - get_catalog_for_single_relation
        - get_column_schema_from_query
        - expand_column_types
        - list_relations_without_caching
        - is_cancelable
        - create_schema
        - drop_schema
        - quote
        - convert_text_type
        - convert_number_type
        - convert_boolean_type
        - convert_datetime_type
        - convert_date_type
        - convert_time_type
        - standardize_grants_dict

    Macros:
        - get_catalog
    """
    connection_manager: BaseConnectionManager
    dialect_definition: BaseDialectDefinition
    execution_manager: ExecutionManager

    # A set of clobber config fields accepted by this adapter
    # for use in materializations
    AdapterSpecificConfigs: Type[AdapterConfig] = AdapterConfig

    MAX_SCHEMA_METADATA_RELATIONS = 100

    def __init__(self, config, mp_context: SpawnContext, ) -> None:
        self.config = config
        self.connections = self.ConnectionManager(config, mp_context)
        self._macro_resolver: Optional[MacroResolverProtocol] = None
        self._macro_context_generator: Optional[MacroContextGeneratorCallable] = None
        self.behavior = DEFAULT_BASE_BEHAVIOR_FLAGS  # type: ignore

    ###
    # Methods to set / access a macro resolver
    ###


    ###
    # Methods that pass through to the connection manager
    ###
    def acquire_connection(self, name=None) -> Connection:
        return self.connections.set_connection_name(name)

    def release_connection(self) -> None:
        self.connections.release()

    def cleanup_connections(self) -> None:
        self.connections.cleanup_all()

    def clear_transaction(self) -> None:
        self.connections.clear_transaction()

    def commit_if_has_connection(self) -> None:
        self.connections.commit_if_has_connection()



    ###
    # Methods that should never be overridden
    ###
    @classmethod
    def type(cls) -> str:
        """Get the type of this adapter. Types must be class-unique and
        consistent.

        :return: The type name
        :rtype: str
        """
        return cls.ConnectionManager.TYPE









