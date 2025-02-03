from contextlib import contextmanager
from importlib import import_module
from typing import Optional, List, Dict, Any, Tuple

from dbt_common.clients.jinja import CallableMacroGenerator
from dbt_common.events.functions import fire_event
from dbt_common.exceptions import DbtRuntimeError, DbtInternalError
from dbt_common.utils import AttrDict, cast_to_str

from dbt.adapters.base.dialect import BaseDialectDefinition
from dbt.adapters.base.dialect.relation import BaseRelation, AdapterTrackingRelationInfo
from dbt.adapters.base.execution.caching.cache_manager import CacheManager
from dbt.adapters.base.execution.macro_handler import MacroHandler

from dbt.adapters.base.integration.connection_manager import BaseConnectionManager
from dbt.adapters.contracts.connection import AdapterRequiredConfig
from dbt.adapters.contracts.macros import MacroResolverProtocol
from dbt.adapters.contracts.relation import RelationConfig
from dbt.adapters.events.types import ListRelations
from dbt.adapters.reference_keys import _make_ref_key_dict


class ExecutionManager:
    cache: CacheManager
    dialect: BaseDialectDefinition
    connections: BaseConnectionManager
    macro_handler: MacroHandler

    def __init__(self, config: AdapterRequiredConfig, conn_name: str,
                 dialect: BaseDialectDefinition,
                 connection_manager: BaseConnectionManager,
                 macro_resolver: Optional[MacroResolverProtocol] = None,
                 ) -> None:
        self.cache = CacheManager(config, conn_name, dialect.get_list_relations_callable())
        self.config = config
        self.dialect = dialect
        self.connections = connection_manager
        self.macro_handler = MacroHandler()
        if macro_resolver is not None:
            self.macro_handler.set_macro_resolver(macro_resolver)

    @available.parse(_parse_callback_empty_table)
    def execute(
            self,
            sql: str,
            auto_begin: bool = False,
            fetch: bool = False,
            limit: Optional[int] = None,
    ) -> Tuple[AdapterResponse, "agate.Table"]:
        """Execute the given SQL. This is a thin wrapper around
        ConnectionManager.execute.

        :param str sql: The sql to execute.
        :param bool auto_begin: If set, and dbt is not currently inside a
            transaction, automatically begin one.
        :param bool fetch: If set, fetch results.
        :param Optional[int] limit: If set, only fetch n number of rows
        :return: A tuple of the query status and results (empty if fetch=False).
        :rtype: Tuple[AdapterResponse, "agate.Table"]
        """
        return self.connections.execute(sql=sql, auto_begin=auto_begin, fetch=fetch, limit=limit)

    def execute_macro(
            self,
            macro_name: str,
            macro_resolver: Optional[MacroResolverProtocol] = None,
            project: Optional[str] = None,
            context_override: Optional[Dict[str, Any]] = None,
            kwargs: Optional[Dict[str, Any]] = None,
            needs_conn: bool = False,
    ) -> AttrDict:
        """Look macro_name up in the manifest and execute its results.

        :param macro_name: The name of the macro to execute.
        :param manifest: The manifest to use for generating the base macro
            execution context. If none is provided, use the internal manifest.
        :param project: The name of the project to search in, or None for the
            first match.
        :param context_override: An optional dict to update() the macro
            execution context.
        :param kwargs: An optional dict of keyword args used to pass to the
            macro.
        : param needs_conn: A boolean that indicates whether the specified macro
            requires an open connection to execute. If needs_conn is True, a
            connection is expected and opened if necessary. Otherwise (and by default),
            no connection is expected prior to executing the macro.
        """

        if kwargs is None:
            kwargs = {}
        if context_override is None:
            context_override = {}

        resolver = macro_resolver or self.macro_resolver
        if resolver is None:
            raise DbtInternalError("Macro resolver was None when calling execute_macro!")

        if self._macro_context_generator is None:
            raise DbtInternalError("Macro context generator was None when calling execute_macro!")

        macro = resolver.find_macro_by_name(macro_name, self.config.project_name, project)
        if macro is None:
            if project is None:
                package_name = "any package"
            else:
                package_name = 'the "{}" package'.format(project)

            raise DbtRuntimeError(
                'dbt could not find a macro with the name "{}" in {}'.format(
                    macro_name, package_name
                )
            )

        macro_context = self._macro_context_generator(macro, self.config, resolver, project)
        macro_context.update(context_override)

        macro_function = CallableMacroGenerator(macro, macro_context)

        if needs_conn:
            connection = self.connections.get_thread_connection()
            self.connections.open(connection)

        with self.connections.exception_handler(f"macro {macro_name}"):
            result = macro_function(**kwargs)
        return result

    def list_relations(self, database: Optional[str], schema: str) -> List[BaseRelation]:
        if self.cache.is_cached(database, schema):
            return self.cache.get_relations(database, schema)

        schema_relation = self.dialect.Relation.create(
            database=database,
            schema=schema,
            identifier="",
            quote_policy=self.config.quoting,
        ).without_identifier()

        # we can't build the relations cache because we don't have a
        # manifest so we can't run any operations.
        relations = self.list_relations_without_caching(schema_relation)

        # if the cache is already populated, add this schema in
        # otherwise, skip updating the cache and just ignore
        if self.cache:
            for relation in relations:
                self.cache.add(relation)
            if not relations:
                # it's possible that there were no relations in some schemas. We want
                # to insert the schemas we query into the cache's `.schemas` attribute
                # so we can check it later
                self.cache.update_schemas([(database, schema)])

        fire_event(
            ListRelations(
                database=cast_to_str(database),
                schema=schema,
                relations=[_make_ref_key_dict(x) for x in relations],
            )
        )

        return relations

    def get_rows_different_sql(
            self,
            relation_a: BaseRelation,
            relation_b: BaseRelation,
            column_names: Optional[List[str]] = None,
            except_operator: str = "EXCEPT",
    ) -> str:
        """Generate SQL for a query that returns a single row with a two
        columns: the number of rows that are different between the two
        relations and the number of mismatched rows.
        """
        # This method only really exists for test reasons.
        names: List[str]
        if column_names is None:
            columns = self.connections.d(relation_a)
            names = sorted((self.quote(c.name) for c in columns))
        else:
            names = sorted((self.quote(n) for n in column_names))
        columns_csv = ", ".join(names)

        sql = self.dialect.get_columns_equal_sql(
            columns=columns_csv,
            relation_a=str(relation_a),
            relation_b=str(relation_b),
            except_op=except_operator,
        )

        return sql

    def debug_query(self) -> None:
        self.execute("select 1 as id")

    def nice_connection_name(self) -> str:
        conn = self.connections.get_if_exists()
        if conn is None or conn.name is None:
            return "<None>"
        return conn.name

    @contextmanager
    def connection_named(
            self, name: str, query_header_context: Any = None, should_release_connection=True
    ) -> Iterator[None]:
        try:
            if self.connections.query_header is not None:
                self.connections.query_header.set(name, query_header_context)
            self.acquire_connection(name)
            yield
        finally:
            if should_release_connection:
                self.release_connection()

            if self.connections.query_header is not None:
                self.connections.query_header.reset()

    @classmethod
    def get_adapter_run_info(cls, config: RelationConfig) -> AdapterTrackingRelationInfo:
        adapter_class_name, *_ = cls.__name__.split("Adapter")
        adapter_name = adapter_class_name.lower()

        if adapter_name == "base":
            adapter_version = ""
        else:
            adapter_version = import_module(f"dbt.adapters.{adapter_name}.__version__").version

        return AdapterTrackingRelationInfo(
            adapter_name=adapter_name,
            base_adapter_version=import_module("dbt.adapters.__about__").version,
            adapter_version=adapter_version,
            model_adapter_details=cls._get_adapter_specific_run_info(config),
        )

    @classmethod
    def _get_adapter_specific_run_info(cls, config) -> Dict[str, Any]:
        """
        Adapter maintainers should overwrite this method to return any run metadata that should be captured during a run.
        """
        return {}