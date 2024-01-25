from dataclasses import dataclass
from datetime import datetime
from typing import Any, List, Optional, Set

from dbt.adapters.base import AdapterConfig, ConstraintSupport, available
from dbt.adapters.capability import (
    Capability,
    CapabilityDict,
    CapabilitySupport,
    Support,
)
from dbt.adapters.exceptions import (
    CrossDbReferenceProhibitedError,
    IndexConfigError,
    IndexConfigNotDictError,
    UnexpectedDbReferenceError,
)
from dbt.adapters.sql import SQLAdapter
from dbt_common.contracts.constraints import ConstraintType
from dbt_common.dataclass_schema import ValidationError, dbtClassMixin
from dbt_common.exceptions import DbtRuntimeError
from dbt_common.utils import encoding as dbt_encoding

from dbt.adapters.postgres.column import PostgresColumn
from dbt.adapters.postgres.connections import PostgresConnectionManager
from dbt.adapters.postgres.relation import PostgresRelation


GET_RELATIONS_MACRO_NAME = "postgres__get_relations"


@dataclass
class PostgresIndexConfig(dbtClassMixin):
    columns: List[str]
    unique: bool = False
    type: Optional[str] = None

    def render(self, relation):
        # We append the current timestamp to the index name because otherwise
        # the index will only be created on every other run. See
        # https://github.com/dbt-labs/dbt-core/issues/1945#issuecomment-576714925
        # for an explanation.
        now = datetime.utcnow().isoformat()
        inputs = self.columns + [relation.render(), str(self.unique), str(self.type), now]
        string = "_".join(inputs)
        return dbt_encoding.md5(string)

    @classmethod
    def parse(cls, raw_index) -> Optional["PostgresIndexConfig"]:
        if raw_index is None:
            return None
        try:
            cls.validate(raw_index)
            return cls.from_dict(raw_index)
        except ValidationError as exc:
            raise IndexConfigError(exc)
        except TypeError:
            raise IndexConfigNotDictError(raw_index)


@dataclass
class PostgresConfig(AdapterConfig):
    unlogged: Optional[bool] = None
    indexes: Optional[List[PostgresIndexConfig]] = None


class PostgresAdapter(SQLAdapter):
    Relation = PostgresRelation
    ConnectionManager = PostgresConnectionManager
    Column = PostgresColumn

    AdapterSpecificConfigs = PostgresConfig

    CONSTRAINT_SUPPORT = {
        ConstraintType.check: ConstraintSupport.ENFORCED,
        ConstraintType.not_null: ConstraintSupport.ENFORCED,
        ConstraintType.unique: ConstraintSupport.ENFORCED,
        ConstraintType.primary_key: ConstraintSupport.ENFORCED,
        ConstraintType.foreign_key: ConstraintSupport.ENFORCED,
    }

    CATALOG_BY_RELATION_SUPPORT = True

    _capabilities: CapabilityDict = CapabilityDict(
        {Capability.SchemaMetadataByRelations: CapabilitySupport(support=Support.Full)}
    )

    @classmethod
    def date_function(cls):
        return "now()"

    @available
    def verify_database(self, database):
        if database.startswith('"'):
            database = database.strip('"')
        expected = self.config.credentials.database
        if database.lower() != expected.lower():
            raise UnexpectedDbReferenceError(self.type(), database, expected)
        # return an empty string on success so macros can call this
        return ""

    @available
    def parse_index(self, raw_index: Any) -> Optional[PostgresIndexConfig]:
        return PostgresIndexConfig.parse(raw_index)

    def _link_cached_database_relations(self, schemas: Set[str]):
        """
        :param schemas: The set of schemas that should have links added.
        """
        database = self.config.credentials.database
        table = self.execute_macro(GET_RELATIONS_MACRO_NAME)

        for dep_schema, dep_name, refed_schema, refed_name in table:
            dependent = self.Relation.create(
                database=database, schema=dep_schema, identifier=dep_name
            )
            referenced = self.Relation.create(
                database=database, schema=refed_schema, identifier=refed_name
            )

            # don't record in cache if this relation isn't in a relevant
            # schema
            if refed_schema.lower() in schemas:
                self.cache.add_link(referenced, dependent)

    def _get_catalog_schemas(self, manifest):
        # postgres only allow one database (the main one)
        schema_search_map = super()._get_catalog_schemas(manifest)
        try:
            return schema_search_map.flatten()
        except DbtRuntimeError as exc:
            raise CrossDbReferenceProhibitedError(self.type(), exc.msg)

    def _link_cached_relations(self, manifest) -> None:
        schemas: Set[str] = set()
        relations_schemas = self._get_cache_schemas(manifest)
        for relation in relations_schemas:
            self.verify_database(relation.database)
            schemas.add(relation.schema.lower())  # type: ignore

        self._link_cached_database_relations(schemas)

    def _relations_cache_for_schemas(self, manifest, cache_schemas=None):
        super()._relations_cache_for_schemas(manifest, cache_schemas)
        self._link_cached_relations(manifest)

    def timestamp_add_sql(self, add_to: str, number: int = 1, interval: str = "hour") -> str:
        return f"{add_to} + interval '{number} {interval}'"

    def valid_incremental_strategies(self):
        """The set of standard builtin strategies which this adapter supports out-of-the-box.
        Not used to validate custom strategies defined by end users.
        """
        return ["append", "delete+insert", "merge"]

    def debug_query(self):
        self.execute("select 1 as id")
