from concurrent.futures import Future
from typing import Optional, Type, List, Dict, Callable, Iterable

from dbt.adapters.base import available
from dbt.adapters.base.dialect.relation import BaseRelation, InformationSchema, SchemaSearchMap
from dbt.adapters.cache import RelationsCache
from dbt.adapters.contracts.connection import AdapterRequiredConfig
from dbt.adapters.contracts.relation import RelationConfig
from dbt.adapters.events.types import CacheMiss
from dbt.adapters.exceptions import NullRelationCacheAttemptedError, NullRelationDropAttemptedError, \
    RenameToNoneAttemptedError
from dbt_common.events.functions import fire_event
from dbt_common.utils import cast_to_str, executor


def _relation_name(rel: Optional[BaseRelation]) -> str:
    if rel is None:
        return "null relation"
    else:
        return str(rel)


class CacheManager:
    relation: Type[BaseRelation]
    config: AdapterRequiredConfig
    name: str

    def __init__(self, config: AdapterRequiredConfig, conn_name: str,
                 list_relations_without_caching: Callable) -> None:
        self.cache = RelationsCache()
        self.config = config
        self.name = conn_name
        self.list_relations_without_caching = list_relations_without_caching

    def is_cached(self, database: Optional[str], schema: str) -> bool:
        """Check if the schema is cached, and by default logs if it is not."""

        if (database, schema) not in self.cache:
            fire_event(
                CacheMiss(
                    conn_name=self.name,
                    database=cast_to_str(database),
                    schema=schema,
                )
            )
            return False
        else:
            return True

    def _get_cache_schemas(self, relation_configs: Iterable[RelationConfig]) -> Set[BaseRelation]:
        """Get the set of schema relations that the cache logic needs to
        populate.
        """
        return {
            self.Relation.create_from(
                quoting=self.config, relation_config=relation_config
            ).without_identifier()
            for relation_config in relation_configs
        }

    def _get_catalog_schemas(self, relation_configs: Iterable[RelationConfig]) -> SchemaSearchMap:
        """Get a mapping of each node's "information_schema" relations to a
        set of all schemas expected in that information_schema.

        There may be keys that are technically duplicates on the database side,
        for example all of '"foo", 'foo', '"FOO"' and 'FOO' could coexist as
        databases, and values could overlap as appropriate. All values are
        lowercase strings.
        """
        info_schema_name_map = SchemaSearchMap()
        relations = self._get_catalog_relations(relation_configs)
        for relation in relations:
            info_schema_name_map.add(relation)
        # result is a map whose keys are information_schema Relations without
        # identifiers that have appropriate database prefixes, and whose values
        # are sets of lowercase schema names that are valid members of those
        # databases
        return info_schema_name_map

    def _get_catalog_relations_by_info_schema(
            self, relations
    ) -> Dict[InformationSchema, List[BaseRelation]]:
        relations_by_info_schema: Dict[InformationSchema, List[BaseRelation]] = dict()
        for relation in relations:
            info_schema = relation.information_schema_only()
            if info_schema not in relations_by_info_schema:
                relations_by_info_schema[info_schema] = []
            relations_by_info_schema[info_schema].append(relation)

        return relations_by_info_schema

    def _get_catalog_relations(
            self, relation_configs: Iterable[RelationConfig]
    ) -> List[BaseRelation]:
        relations = [
            self.Relation.create_from(quoting=self.config, relation_config=relation_config)
            for relation_config in relation_configs
        ]
        return relations

    def _relations_cache_for_schemas(
            self,
            relation_configs: Iterable[RelationConfig],
            cache_schemas: Optional[Set[BaseRelation]] = None,
    ) -> None:
        """Populate the relations cache for the given schemas. Returns an
        iterable of the schemas populated, as strings.
        """
        if not cache_schemas:
            cache_schemas = self._get_cache_schemas(relation_configs)
        with executor(self.config) as tpe:
            futures: List[Future[List[BaseRelation]]] = []
            for cache_schema in cache_schemas:
                fut = tpe.submit_connected(
                    self,
                    f"list_{cache_schema.database}_{cache_schema.schema}",
                    self.list_relations_without_caching,
                    cache_schema,
                )
                futures.append(fut)

            for future in as_completed(futures):
                # if we can't read the relations we need to just raise anyway,
                # so just call future.result() and let that raise on failure
                for relation in future.result():
                    self.cache.add(relation)

        # it's possible that there were no relations in some schemas. We want
        # to insert the schemas we query into the cache's `.schemas` attribute
        # so we can check it later
        cache_update: Set[Tuple[Optional[str], str]] = set()
        for relation in cache_schemas:
            if relation.schema:
                cache_update.add((relation.database, relation.schema))
        self.cache.update_schemas(cache_update)

    def set_relations_cache(
            self,
            relation_configs: Iterable[RelationConfig],
            clear: bool = False,
            required_schemas: Optional[Set[BaseRelation]] = None,
    ) -> None:
        """Run a query that gets a populated cache of the relations in the
        database and set the cache on this adapter.
        """
        with self.cache.lock:
            if clear:
                self.cache.clear()
            self._relations_cache_for_schemas(relation_configs, required_schemas)

    def cache_added(self, relation: Optional[BaseRelation]) -> str:
        """Cache a new relation in dbt. It will show up in `list relations`."""
        if relation is None:
            name = self.name()
            raise NullRelationCacheAttemptedError(name)
        self.cache.add(relation)
        # so jinja doesn't render things
        return ""

    def cache_dropped(self, relation: Optional[BaseRelation]) -> str:
        """Drop a relation in dbt. It will no longer show up in
        `list relations`, and any bound views will be dropped from the cache
        """
        if relation is None:
            raise NullRelationDropAttemptedError(self.name)
        self.cache.drop(relation)
        return ""

    @available
    def cache_renamed(
            self,
            from_relation: Optional[BaseRelation],
            to_relation: Optional[BaseRelation],
    ) -> str:
        """Rename a relation in dbt. It will show up with a new name in
        `list_relations`, but bound views will remain bound.
        """
        if from_relation is None or to_relation is None:
            name = self.nice_connection_name()
            src_name = _relation_name(from_relation)
            dst_name = _relation_name(to_relation)
            raise RenameToNoneAttemptedError(src_name, dst_name, name)

        self.cache.rename(from_relation, to_relation)
        return ""
