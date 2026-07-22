from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from multiprocessing.context import SpawnContext
from typing import Any, Dict, List, Optional, Set

from dateutil.relativedelta import relativedelta

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
from dbt_common.behavior_flags import BehaviorFlag
from dbt_common.contracts.constraints import ConstraintType
from dbt_common.dataclass_schema import ValidationError, dbtClassMixin
from dbt_common.exceptions import DbtRuntimeError
from dbt_common.utils import encoding as dbt_encoding

from dbt.adapters.postgres.column import PostgresColumn
from dbt.adapters.postgres.connections import PostgresConnectionManager
from dbt.adapters.postgres.relation import PostgresRelation


GET_RELATIONS_MACRO_NAME = "postgres__get_relations"

POSTGRES_SKIP_AUTOCOMMIT_TRANSACTION_STATEMENTS = BehaviorFlag(
    name="postgres_skip_autocommit_transaction_statements",
    default=True,
    description=(
        "When autocommit is enabled, skip sending BEGIN/COMMIT/ROLLBACK statements "
        "since each statement is automatically committed. This reduces round-trips "
        "to the database and avoids unnecessary transaction overhead."
        "Setting this to False will preserve the legacy behavior of sending BEGIN/COMMIT/ROLLBACK statements."
    ),
)


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
        now = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
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


PARTITION_METHODS = ("range", "list", "hash")
RANGE_GRANULARITIES = ("hour", "day", "week", "month", "year")

_GRANULARITY_NAME_FMT = {
    "year": "%Y",
    "month": "%Y%m",
    "week": "%Y%m%d",
    "day": "%Y%m%d",
    "hour": "%Y%m%d%H",
}


def _as_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day)
    return datetime.fromisoformat(str(value))


def _floor_to_granularity(dt: datetime, granularity: str) -> datetime:
    if granularity == "year":
        return dt.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
    if granularity == "month":
        return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if granularity == "week":
        floored = dt.replace(hour=0, minute=0, second=0, microsecond=0)
        return floored - timedelta(days=floored.weekday())
    if granularity == "day":
        return dt.replace(hour=0, minute=0, second=0, microsecond=0)
    # hour
    return dt.replace(minute=0, second=0, microsecond=0)


def _granularity_step(granularity: str):
    return {
        "year": relativedelta(years=1),
        "month": relativedelta(months=1),
        "week": timedelta(weeks=1),
        "day": timedelta(days=1),
        "hour": timedelta(hours=1),
    }[granularity]


@dataclass
class PostgresPartitionConfig(dbtClassMixin):
    """
    Native PostgreSQL declarative partitioning config (issue #679).

    https://www.postgresql.org/docs/current/ddl-partitioning.html

    - fields: one or more columns/expressions that make up the partition key
    - method: `range`, `list`, or `hash`
    - granularity: for `range`, drives auto-management of partitions (bounds + names)
    - default_partition: create a DEFAULT partition to catch rows outside every partition
    - partitions: explicit static partition definitions, e.g.
        range: {"name": "p2024", "from": "'2024-01-01'", "to": "'2025-01-01'"}
        list:  {"name": "p_us", "values": ["'us'"]}
        hash:  {"name": "p0", "modulus": 2, "remainder": 0}
    """

    fields: List[str]
    method: str = "range"
    granularity: Optional[str] = None
    default_partition: bool = True
    partitions: Optional[List[Dict[str, Any]]] = None

    @property
    def render(self) -> str:
        """The `PARTITION BY ...` key clause, e.g. `range (created_at)`."""
        return f"{self.method} ({', '.join(self.fields)})"

    def _validate(self) -> None:
        if not self.fields:
            raise DbtRuntimeError(
                "partition_by requires at least one column in `fields`, but none were provided"
            )
        if self.method not in PARTITION_METHODS:
            raise DbtRuntimeError(
                f"Invalid partition_by method '{self.method}'. "
                f"Supported methods are: {', '.join(PARTITION_METHODS)}"
            )
        if self.granularity is not None and self.granularity not in RANGE_GRANULARITIES:
            raise DbtRuntimeError(
                f"Invalid partition_by granularity '{self.granularity}'. "
                f"Supported granularities are: {', '.join(RANGE_GRANULARITIES)}"
            )
        if self.granularity is not None and self.method != "range":
            raise DbtRuntimeError(
                "partition_by `granularity` is only supported for the `range` method"
            )

    @classmethod
    def parse(cls, raw_partition_by: Any) -> Optional["PostgresPartitionConfig"]:
        if raw_partition_by is None:
            return None
        try:
            cls.validate(raw_partition_by)
            partition_by: "PostgresPartitionConfig" = cls.from_dict(raw_partition_by)
        except ValidationError as exc:
            raise DbtRuntimeError(f"Could not parse partition_by config: {exc}")
        except TypeError:
            raise DbtRuntimeError(f"partition_by must be a dict, but got: {raw_partition_by}")
        partition_by._validate()
        return partition_by


@dataclass
class PostgresConfig(AdapterConfig):
    unlogged: Optional[bool] = None
    indexes: Optional[List[PostgresIndexConfig]] = None
    partition_by: Optional[PostgresPartitionConfig] = field(default=None)


class PostgresAdapter(SQLAdapter):
    Relation = PostgresRelation
    ConnectionManager = PostgresConnectionManager
    Column = PostgresColumn

    AdapterSpecificConfigs = PostgresConfig

    connections: PostgresConnectionManager

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

    def __init__(self, config, mp_context: SpawnContext) -> None:
        super().__init__(config, mp_context)
        self.connections.set_skip_transactions_checker(
            lambda: self.behavior.postgres_skip_autocommit_transaction_statements.no_warn
        )

    @property
    def _behavior_flags(self) -> List[BehaviorFlag]:
        return [POSTGRES_SKIP_AUTOCOMMIT_TRANSACTION_STATEMENTS]

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

    @available
    def parse_partition_by(self, raw_partition_by: Any) -> Optional[PostgresPartitionConfig]:
        return PostgresPartitionConfig.parse(raw_partition_by)

    @available
    def get_partition_bounds(
        self, minimum: Any, maximum: Any, granularity: str
    ) -> List[Dict[str, str]]:
        """
        Compute the range partitions needed to cover [minimum, maximum] at the given
        granularity. Returns a list of dicts: {"name": suffix, "from": literal, "to": literal},
        where the literals are quoted SQL timestamps for a `FOR VALUES FROM (..) TO (..)` clause.
        """
        if minimum is None or maximum is None:
            return []

        current = _floor_to_granularity(_as_datetime(minimum), granularity)
        end = _as_datetime(maximum)
        step = _granularity_step(granularity)

        bounds: List[Dict[str, str]] = []
        while current <= end:
            nxt = current + step
            bounds.append(
                {
                    "name": "p" + current.strftime(_GRANULARITY_NAME_FMT[granularity]),
                    "from": "'" + current.strftime("%Y-%m-%d %H:%M:%S") + "'",
                    "to": "'" + nxt.strftime("%Y-%m-%d %H:%M:%S") + "'",
                }
            )
            current = nxt
        return bounds

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
        return ["append", "delete+insert", "merge", "microbatch"]

    def debug_query(self):
        self.execute("select 1 as id")
