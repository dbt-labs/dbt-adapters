from asyncio import Future, as_completed
from typing import FrozenSet, Tuple, List, Iterable, Set, Optional, Callable

from dbt.adapters.base import SchemaSearchMap, BaseRelation
from dbt.adapters.contracts.connection import AdapterRequiredConfig
from dbt.adapters.contracts.relation import RelationConfig
from dbt.adapters.events.types import CatalogGenerationError
from dbt_common.events.functions import warn_or_error
from dbt_common.utils import executor
from dbt_common.utils.executor import HasThreadingConfig


def catch_as_completed(
        futures,  # typing: List[Future["agate.Table"]]
) -> Tuple["agate.Table", List[Exception]]:
    from dbt_common.clients.agate_helper import merge_tables

    # catalogs: "agate.Table" =".Table(rows=[])
    tables: List["agate.Table"] = []
    exceptions: List[Exception] = []

    for future in as_completed(futures):
        exc = future.exception()
        # we want to re-raise on ctrl+c and BaseException
        if exc is None:
            catalog = future.result()
            tables.append(catalog)
        elif isinstance(exc, KeyboardInterrupt) or not isinstance(exc, Exception):
            raise exc
        else:
            warn_or_error(CatalogGenerationError(exc=str(exc)))
            # exc is not None, derives from Exception, and isn't ctrl+c
            exceptions.append(exc)
    return merge_tables(tables), exceptions


class ModelCatalog:
    config: HasThreadingConfig
    relations: AdapterRelationOperations

    def get_catalog(
            self,
            relation_configs: Iterable[RelationConfig],
            used_schemas: FrozenSet[Tuple[str, str]],
    ) -> Tuple["agate.Table", List[Exception]]:
        with executor(self.config) as tpe:
            futures: List[Future["agate.Table"]] = []
            schema_map: SchemaSearchMap = self._get_catalog_schemas(relation_configs)
            for info, schemas in schema_map.items():
                if len(schemas) == 0:
                    continue
                name = ".".join([str(info.database), "information_schema"])
                fut = tpe.submit_connected(
                    self, name, self._get_one_catalog, info, schemas, used_schemas
                )
                futures.append(fut)

        catalogs, exceptions = catch_as_completed(futures)
        return catalogs, exceptions

    def get_catalog_by_relations(
            self, used_schemas: FrozenSet[Tuple[str, str]], relations: Set[BaseRelation]
    ) -> Tuple["agate.Table", List[Exception]]:
        with executor(self.config) as tpe:
            futures: List[Future["agate.Table"]] = []
            relations_by_schema = self._get_catalog_relations_by_info_schema(relations)
            for info_schema in relations_by_schema:
                name = ".".join([str(info_schema.database), "information_schema"])
                relations = set(relations_by_schema[info_schema])
                fut = tpe.submit_connected(
                    self,
                    name,
                    self._get_one_catalog_by_relations,
                    info_schema,
                    relations,
                    used_schemas,
                )
                futures.append(fut)

            catalogs, exceptions = catch_as_completed(futures)
            return catalogs, exceptions

    @classmethod
    def _catalog_filter_table(
            cls, table: "agate.Table", used_schemas: FrozenSet[Tuple[str, str]]
    ) -> "agate.Table":
        """Filter the table as appropriate for catalog entries. Subclasses can
        override this to change filtering rules on a per-adapter basis.
        """
        from dbt_common.clients.agate_helper import table_from_rows

        # force database + schema to be strings
        table = table_from_rows(
            table.rows,
            table.column_names,
            text_only_columns=["table_database", "table_schema", "table_name"],
        )
        return table.where(_catalog_filter_schemas(used_schemas))

    def _get_one_catalog(
            self,
            information_schema: InformationSchema,
            schemas: Set[str],
            used_schemas: FrozenSet[Tuple[str, str]],
    ) -> "agate.Table":
        kwargs = {"information_schema": information_schema, "schemas": schemas}
        table = self.execute_macro(GET_CATALOG_MACRO_NAME, kwargs=kwargs)

        results = self._catalog_filter_table(table, used_schemas)  # type: ignore[arg-type]
        return results

    def _get_one_catalog_by_relations(
            self,
            information_schema: InformationSchema,
            relations: List[BaseRelation],
            used_schemas: FrozenSet[Tuple[str, str]],
    ) -> "agate.Table":
        kwargs = {
            "information_schema": information_schema,
            "relations": relations,
        }
        table = self.execute_macro(GET_CATALOG_RELATIONS_MACRO_NAME, kwargs=kwargs)

        results = self._catalog_filter_table(table, used_schemas)  # type: ignore[arg-type]
        return results

    def get_filtered_catalog(
            self,
            relation_configs: Iterable[RelationConfig],
            used_schemas: FrozenSet[Tuple[str, str]],
            relations: Optional[Set[BaseRelation]] = None,
    ):
        catalogs: "agate.Table"
        if (
                relations is None
                or len(relations) > self.MAX_SCHEMA_METADATA_RELATIONS
                or not self.supports(Capability.SchemaMetadataByRelations)
        ):
            # Do it the traditional way. We get the full catalog.
            catalogs, exceptions = self.get_catalog(relation_configs, used_schemas)
        else:
            # Do it the new way. We try to save time by selecting information
            # only for the exact set of relations we are interested in.
            catalogs, exceptions = self.get_catalog_by_relations(used_schemas, relations)

        if relations and catalogs:
            relation_map = {
                (
                    r.database.casefold() if r.database else None,
                    r.schema.casefold() if r.schema else None,
                    r.identifier.casefold() if r.identifier else None,
                )
                for r in relations
            }

            def in_map(row: "agate.Row"):
                d = _expect_row_value("table_database", row)
                s = _expect_row_value("table_schema", row)
                i = _expect_row_value("table_name", row)
                d = d.casefold() if d is not None else None
                s = s.casefold() if s is not None else None
                i = i.casefold() if i is not None else None
                return (d, s, i) in relation_map

            catalogs = catalogs.where(in_map)

        return catalogs, exceptions


def _catalog_filter_schemas(
        used_schemas: FrozenSet[Tuple[str, str]]
) -> Callable[["agate.Row"], bool]:
    """Return a function that takes a row and decides if the row should be
    included in the catalog output.
    """
    schemas = frozenset((d.lower(), s.lower()) for d, s in used_schemas)

    def test(row: "agate.Row") -> bool:
        table_database = _expect_row_value("table_database", row)
        table_schema = _expect_row_value("table_schema", row)
        # the schema may be present but None, which is not an error and should
        # be filtered out
        if table_schema is None:
            return False
        return (table_database.lower(), table_schema.lower()) in schemas

    return test
