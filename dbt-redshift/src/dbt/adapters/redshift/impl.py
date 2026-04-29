import os
from contextlib import contextmanager
from dataclasses import dataclass
from multiprocessing.context import SpawnContext

import agate
from dbt_common.behavior_flags import BehaviorFlag
from dbt_common.contracts.constraints import ConstraintType
from datetime import datetime, timezone
from typing import List, Optional, Set, Any, Dict, Tuple, Type, Mapping
from collections import namedtuple
from dbt.adapters.base import PythonJobHelper
from dbt.adapters.base.impl import AdapterConfig, ConstraintSupport, FreshnessResponse
from dbt.adapters.base.meta import available
from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.capability import (
    Capability,
    CapabilityDict,
    CapabilitySupport,
    Support,
)
from dbt.adapters.protocol import MacroResolverProtocol
from dbt.adapters.sql import SQLAdapter
from dbt.adapters.contracts.connection import AdapterResponse
from dbt.adapters.events.logging import AdapterLogger


import dbt_common.exceptions

from dbt.adapters.redshift import RedshiftConnectionManager, RedshiftRelation

logger = AdapterLogger("Redshift")
packages = ["redshift_connector", "redshift_connector.core"]
if os.getenv("DBT_REDSHIFT_CONNECTOR_DEBUG_LOGGING"):
    level = "DEBUG"
else:
    level = "ERROR"
for package in packages:
    logger.debug(f"Setting {package} to {level}")
    logger.set_adapter_dependency_log_level(package, level)

GET_RELATIONS_MACRO_NAME = "redshift__get_relations"
SHOW_TABLES_FROM_SCHEMA_MACRO_NAME = "redshift__show_tables_from_schema"

REDSHIFT_SKIP_AUTOCOMMIT_TRANSACTION_STATEMENTS = BehaviorFlag(
    name="redshift_skip_autocommit_transaction_statements",
    default=False,
    description=(
        "When autocommit is enabled, skip sending BEGIN/COMMIT/ROLLBACK statements "
        "since each statement is automatically committed. This reduces round-trips "
        "to the database and avoids unnecessary transaction overhead. "
        "Setting this to True will enable the optimization."
    ),
)

REDSHIFT_GRANTS_EXTENDED = BehaviorFlag(
    name="redshift_grants_extended",
    default=False,
    description=(
        "Enable groups and roles support in dbt grants config. "
        "When enabled, grantee names must use 'user:', 'group:', or 'role:' prefixes. "
        "Unprefixed entries are treated as users for backward compatibility. "
        "When disabled (default), the legacy behavior is preserved: all grantees are "
        "treated as plain usernames and only user grants are detected."
    ),
)

CATALOG_COLUMNS = [
    "table_database",
    "table_schema",
    "table_name",
    "table_type",
    "table_comment",
    "table_owner",
    "column_name",
    "column_index",
    "column_type",
    "column_comment",
]

CATALOG_COLUMN_TYPES = [
    agate.Text(),
    agate.Text(),
    agate.Text(),
    agate.Text(),
    agate.Text(),
    agate.Text(),
    agate.Text(),
    agate.Number(),
    agate.Text(),
    agate.Text(),
]

_SHOW_TABLE_TYPE_MAP = {
    "TABLE": "BASE TABLE",
    "VIEW": "VIEW",
    "MATERIALIZED VIEW": "MATERIALIZED VIEW",
    "LATE BINDING VIEW": "LATE BINDING VIEW",
}


@dataclass
class RedshiftConfig(AdapterConfig):
    sort_type: Optional[str] = None
    dist: Optional[str] = None
    sort: Optional[str] = None
    bind: Optional[bool] = None
    backup: Optional[bool] = True
    auto_refresh: Optional[bool] = False
    query_group: Optional[str] = None


class RedshiftAdapter(SQLAdapter):
    Relation = RedshiftRelation
    ConnectionManager = RedshiftConnectionManager
    connections: RedshiftConnectionManager

    AdapterSpecificConfigs = RedshiftConfig

    CONSTRAINT_SUPPORT = {
        ConstraintType.check: ConstraintSupport.NOT_SUPPORTED,
        ConstraintType.not_null: ConstraintSupport.ENFORCED,
        ConstraintType.unique: ConstraintSupport.NOT_ENFORCED,
        ConstraintType.primary_key: ConstraintSupport.NOT_ENFORCED,
        ConstraintType.foreign_key: ConstraintSupport.NOT_ENFORCED,
    }

    _capabilities = CapabilityDict(
        {
            Capability.SchemaMetadataByRelations: CapabilitySupport(support=Support.Full),
            Capability.TableLastModifiedMetadata: CapabilitySupport(support=Support.Full),
            Capability.TableLastModifiedMetadataBatch: CapabilitySupport(support=Support.Full),
        }
    )

    def __init__(self, config, mp_context: SpawnContext) -> None:
        super().__init__(config, mp_context)
        # Pass behavior flag checker to connection manager for transaction optimization
        self.connections.set_skip_transactions_checker(
            lambda: self.behavior.redshift_skip_autocommit_transaction_statements.no_warn
        )

    @property
    def _behavior_flags(self) -> List[BehaviorFlag]:
        return [
            REDSHIFT_SKIP_AUTOCOMMIT_TRANSACTION_STATEMENTS,
            REDSHIFT_GRANTS_EXTENDED,
        ]

    @classmethod
    def date_function(cls):
        return "getdate()"

    def drop_relation(self, relation):
        """
        In Redshift, DROP TABLE ... CASCADE should not be used
        inside a transaction. Redshift doesn't prevent the CASCADE
        part from conflicting with concurrent transactions. If we do
        attempt to drop two tables with CASCADE at once, we'll often
        get the dreaded:

            table was dropped by a concurrent transaction

        So, we need to lock around calls to the underlying
        drop_relation() function.

        https://docs.aws.amazon.com/redshift/latest/dg/r_DROP_TABLE.html

        Users with no downstream views (no CASCADE side-effects) can opt out
        of the lock via `allow_concurrent_drops: true` in their profile credentials
        to allow DROP statements to run in parallel across threads. The DROP
        still runs inside a fresh transaction context — the lock is the only
        thing skipped.
        """
        if self.config.credentials.allow_concurrent_drops:
            with self.connections.fresh_transaction_without_lock():
                return super().drop_relation(relation)
        with self.connections.fresh_transaction():
            return super().drop_relation(relation)

    @classmethod
    def convert_text_type(cls, agate_table: "agate.Table", col_idx):
        column = agate_table.columns[col_idx]
        # `lens` must be a list, so this can't be a generator expression,
        # because max() raises ane exception if its argument has no members.
        lens = [len(d.encode("utf-8")) for d in column.values_without_nulls()]
        max_len = max(lens) if lens else 64
        return "varchar({})".format(max_len)

    @classmethod
    def convert_time_type(cls, agate_table: "agate.Table", col_idx):
        return "varchar(24)"

    @available
    def use_show_apis(self) -> bool:
        """Whether to use Redshift SHOW/SVV_* APIs for metadata queries.

        Returns True when the ``datasharing`` profile config is enabled.
        """
        return bool(self.config.credentials.datasharing)

    @available
    def use_grants_extended(self) -> bool:
        """Whether to use extended grants support for groups and roles."""
        return self.behavior.redshift_grants_extended.no_warn

    @available
    def verify_database(self, database):
        if database.startswith('"'):
            database = database.strip('"')
        expected = self.config.credentials.database
        ra3_node = self.config.credentials.ra3_node

        if database.lower() != expected.lower() and not ra3_node and not self.use_show_apis():
            raise dbt_common.exceptions.NotImplementedError(
                "Cross-db references allowed only in RA3.* node or with datasharing enabled. ({} vs {})".format(
                    database, expected
                )
            )
        # return an empty string on success so macros can call this
        return ""

    @available
    def transform_show_tables_for_list_relations(
        self, show_tables: "agate.Table"
    ) -> "agate.Table":
        """Transform SHOW TABLES FROM SCHEMA output into the relation format dbt expects.

        SHOW TABLES returns columns including database_name, schema_name, table_name,
        table_type (TABLE/VIEW), and table_subtype (REGULAR TABLE, REGULAR VIEW,
        LATE BINDING VIEW, MATERIALIZED VIEW).

        Returns an agate Table with columns: database, name, schema, type
        where type is one of: table, view, materialized_view.
        """
        new_rows = []
        # has_subtype is only needed until redshift patch 197 is everywhere
        has_subtype = "table_subtype" in show_tables.column_names
        for row in show_tables.rows:
            table_type = (row["table_type"] or "").strip().upper()
            if table_type == "VIEW":
                subtype = (row["table_subtype"] or "").strip().upper() if has_subtype else ""
                relation_type = "materialized_view" if subtype == "MATERIALIZED VIEW" else "view"
            else:
                relation_type = "table"

            new_rows.append(
                (
                    row["database_name"],
                    row["table_name"],
                    row["schema_name"],
                    relation_type,
                )
            )

        return agate.Table(
            new_rows,
            column_names=["database", "name", "schema", "type"],
            column_types=[agate.Text(), agate.Text(), agate.Text(), agate.Text()],
        )

    @available
    def build_catalog_from_show_tables_and_svv_columns(
        self,
        show_tables_results: List["agate.Table"],
        svv_columns: "agate.Table",
    ) -> "agate.Table":
        """Build the base catalog by joining SHOW TABLES metadata with SVV_REDSHIFT_COLUMNS."""
        if not show_tables_results or not svv_columns.rows:
            return agate.Table([], column_names=CATALOG_COLUMNS, column_types=CATALOG_COLUMN_TYPES)

        table_meta: Dict[tuple, tuple] = {}
        for show_table in show_tables_results:
            # has_subtype is only needed until redshift patch 197 is everywhere
            has_subtype = "table_subtype" in show_table.column_names
            for row in show_table.rows:
                table_type = (row["table_type"] or "").strip().upper()
                subtype = (row["table_subtype"] or "").strip().upper() if has_subtype else ""
                catalog_type = _SHOW_TABLE_TYPE_MAP.get(
                    subtype, _SHOW_TABLE_TYPE_MAP.get(table_type, "BASE TABLE")
                )

                key = (row["database_name"], row["schema_name"].lower(), row["table_name"].lower())
                table_meta[key] = (
                    row["database_name"],
                    row["schema_name"],
                    row["table_name"],
                    catalog_type,
                    row["remarks"],
                    row["owner"],
                )

        catalog_rows = []
        for row in svv_columns.rows:
            meta = table_meta.get(
                (row["database_name"], row["schema_name"].lower(), row["table_name"].lower())
            )
            if meta:
                catalog_rows.append(
                    meta
                    + (
                        row["column_name"],
                        row["ordinal_position"],
                        row["data_type"],
                        row["remarks"],
                    )
                )

        return agate.Table(
            catalog_rows, column_names=CATALOG_COLUMNS, column_types=CATALOG_COLUMN_TYPES
        )

    def standardize_grants_dict(self, grants_table: "agate.Table") -> dict:
        """Translate the result of a grants query to match the grants config format.

        When ``redshift_grants_extended`` is disabled (default), the legacy
        behavior is used: grantees are returned as plain names with no
        ``user:``/``group:``/``role:`` prefixes, and only user grants are
        detected (groups and roles are invisible to the legacy query).

        When ``redshift_grants_extended`` is enabled, grantees are returned
        with prefixes so that groups and roles can be distinguished and
        idempotently managed.  ``SHOW GRANTS`` is used for cross-database
        support when ``datasharing`` is enabled; ``svv_relation_privileges``
        is used otherwise.
        """

        grants_dict: Dict[str, List[str]] = {}
        current_user = self.config.credentials.user.lower()

        if not self.use_grants_extended():
            if not self.use_show_apis():
                # pg_user query returns a 'grantee' column — delegate to base.
                return super().standardize_grants_dict(grants_table)
            else:
                # SHOW GRANTS returns 'identity_name'; return plain names with
                # no prefix so the config comparison is unchanged from legacy.
                # Filter to users only and exclude the current dbt runner to
                # match the pg_user + has_table_privilege() path.
                for row in grants_table:
                    if row["identity_type"].lower() != "user":
                        continue
                    if row["identity_name"].lower() == current_user:
                        continue
                    grantee = row["identity_name"]
                    privilege = row["privilege_type"].lower()
                    if privilege in grants_dict:
                        grants_dict[privilege].append(grantee)
                    else:
                        grants_dict[privilege] = [grantee]
                return grants_dict

        if self.use_show_apis():
            # SHOW GRANTS path — need to detect groups from the / prefix
            for row in grants_table:
                identity_name = row["identity_name"]
                identity_type = row["identity_type"].lower()
                # Skip PUBLIC grants — not configurable via dbt grants
                if identity_type == "public":
                    continue
                # Skip Redshift reserved roles — these cannot be modified
                # via GRANT/REVOKE.  Includes datashare roles (ds:*) and
                # system-defined roles (sys:*).
                if identity_name.startswith(("ds:", "sys:")):
                    continue
                # Skip the dbt runner — matches pg_user and SVV paths which
                # exclude current_user to avoid spurious REVOKE-self drift.
                if identity_type == "user" and identity_name.lower() == current_user:
                    continue
                # SHOW GRANTS reports groups as identity_type='role' with a
                # '/' prefix on identity_name.  This is undocumented behavior.
                if identity_type == "role" and identity_name.startswith("/"):
                    grantee = f"group:{identity_name[1:]}"
                else:
                    grantee = f"{identity_type}:{identity_name}"
                privilege = row["privilege_type"].lower()
                if privilege in grants_dict:
                    grants_dict[privilege].append(grantee)
                else:
                    grants_dict[privilege] = [grantee]
        else:
            # svv_relation_privileges path — identity_type is accurate
            for row in grants_table:
                identity_name = row["identity_name"]
                identity_type = row["identity_type"].lower()
                # Skip PUBLIC grants — not configurable via dbt grants
                if identity_type == "public":
                    continue
                # Skip Redshift reserved roles — these cannot be modified
                # via GRANT/REVOKE.  Includes datashare roles (ds:*) and
                # system-defined roles (sys:*).
                if identity_name.startswith(("ds:", "sys:")):
                    continue
                grantee = f"{identity_type}:{identity_name}"
                privilege = row["privilege_type"].lower()
                if privilege in grants_dict:
                    grants_dict[privilege].append(grantee)
                else:
                    grants_dict[privilege] = [grantee]

        return grants_dict

    def calculate_freshness_from_metadata_batch(
        self,
        sources: List[BaseRelation],
        macro_resolver: Optional[MacroResolverProtocol] = None,
    ) -> Tuple[List[Optional[AdapterResponse]], Dict[BaseRelation, FreshnessResponse]]:
        if not self.use_show_apis():
            return super().calculate_freshness_from_metadata_batch(sources, macro_resolver)

        source_lookup = {
            (
                (source.database or "").lower(),
                (source.schema or "").lower(),
                (source.identifier or "").lower(),
            ): source
            for source in sources
        }

        sources_by_schema: Dict[Tuple[str, str], List[BaseRelation]] = {}
        for source in sources:
            sources_by_schema.setdefault((source.database or "", source.schema or ""), []).append(
                source
            )

        adapter_responses: List[Optional[AdapterResponse]] = []
        freshness_responses: Dict[BaseRelation, FreshnessResponse] = {}

        for (database, schema), schema_sources in sources_by_schema.items():
            result = self.execute_macro(
                SHOW_TABLES_FROM_SCHEMA_MACRO_NAME,
                kwargs={"database": database, "schema": schema},
                needs_conn=True,
            )
            adapter_response, table = result.response, result.table
            adapter_responses.append(adapter_response)

            requested_identifiers = {(s.identifier or "").lower() for s in schema_sources}
            snapshot_time = datetime.now(timezone.utc)

            for row in table:
                table_name = row["table_name"]
                if table_name.lower() not in requested_identifiers:
                    continue

                last_modified = row["last_modified_time"]

                lookup_key = (database.lower(), schema.lower(), table_name.lower())
                source = source_lookup[lookup_key]

                freshness_responses[source] = self._create_freshness_response(
                    last_modified, snapshot_time
                )

        return adapter_responses, freshness_responses

    def _get_catalog_schemas(self, manifest):
        # redshift(besides ra3) only allow one database (the main one)
        schemas = super(SQLAdapter, self)._get_catalog_schemas(manifest)
        allow_multiple_databases = self.config.credentials.ra3_node or self.use_show_apis()
        try:
            return schemas.flatten(allow_multiple_databases=allow_multiple_databases)
        except dbt_common.exceptions.DbtRuntimeError as exc:
            msg = f"Cross-db references allowed only in {self.type()} RA3.* node. Got {exc.msg}"
            raise dbt_common.exceptions.CompilationError(msg)

    def valid_incremental_strategies(self):
        """The set of standard builtin strategies which this adapter supports out-of-the-box.
        Not used to validate custom strategies defined by end users.
        """
        return ["append", "delete+insert", "merge", "microbatch"]

    def timestamp_add_sql(self, add_to: str, number: int = 1, interval: str = "hour") -> str:
        return f"{add_to} + interval '{number} {interval}'"

    def _link_cached_database_relations(self, schemas: Set[str]):
        """
        :param schemas: The set of schemas that should have links added.
        """
        database = self.config.credentials.database
        _Relation = namedtuple("_Relation", "database schema identifier")
        links = [
            (
                _Relation(database, dep_schema, dep_identifier),
                _Relation(database, ref_schema, ref_identifier),
            )
            for dep_schema, dep_identifier, ref_schema, ref_identifier in self.execute_macro(
                GET_RELATIONS_MACRO_NAME
            )
            # don't record in cache if this relation isn't in a relevant schema
            if ref_schema in schemas
        ]

        for dependent, referenced in links:
            self.cache.add_link(
                referenced=self.Relation.create(**referenced._asdict()),
                dependent=self.Relation.create(**dependent._asdict()),
            )

    def _link_cached_relations(self, manifest):
        schemas = set(
            relation.schema.lower()
            for relation in self._get_cache_schemas(manifest)
            if self.verify_database(relation.database) == ""
        )
        self._link_cached_database_relations(schemas)

    def _relations_cache_for_schemas(self, manifest, cache_schemas=None):
        super()._relations_cache_for_schemas(manifest, cache_schemas)
        self._link_cached_relations(manifest)

    # avoid non-implemented abstract methods warning
    # make it clear what needs to be implemented while still raising the error in super()
    # we can update these with Redshift-specific messages if needed
    @property
    def python_submission_helpers(self) -> Dict[str, Type[PythonJobHelper]]:
        return super().python_submission_helpers

    @property
    def default_python_submission_method(self) -> str:
        return super().default_python_submission_method

    def generate_python_submission_response(self, submission_result: Any) -> AdapterResponse:
        return super().generate_python_submission_response(submission_result)

    def debug_query(self):
        """Override for DebugTask method"""
        self.execute("select 1 as id")

    def _set_query_group(self, value: str) -> None:
        self.execute(f"SET query_group TO '{value}'")

    def _unset_query_group(self) -> None:
        self.execute("RESET query_group")

    def _apply_query_group(self, query_group: Optional[str]) -> None:
        if query_group is None:
            self._unset_query_group()
        else:
            self._set_query_group(query_group)

    def _needs_query_group_change(self, config: Mapping[str, Any]) -> bool:
        model_query_group = config.get("query_group")
        default_query_group = self.config.credentials.query_group
        return model_query_group is not None and model_query_group != default_query_group

    def _use_database(self, database: str) -> None:
        self.execute(f"USE {self.quote(database)}")

    def _reset_database(self) -> None:
        self.execute("RESET USE")

    @staticmethod
    def _normalize_database(database: str) -> str:
        return database.strip('"').lower()

    def _is_different_database(self, database: Optional[str]) -> bool:
        """Check if the given database differs from the default credentials database."""
        if database is None:
            return False
        return self._normalize_database(str(database)) != self._normalize_database(
            self.config.credentials.database
        )

    def _needs_database_change(self, config: Mapping[str, Any]) -> bool:
        return self.use_show_apis() and self._is_different_database(config.get("database"))

    def pre_model_hook(self, config: Mapping[str, Any]) -> Optional[str]:
        if self._needs_query_group_change(config):
            self._set_query_group(str(config.get("query_group")))
        if self._needs_database_change(config):
            self._use_database(self._normalize_database(str(config.get("database"))))
        return None

    def post_model_hook(self, config: Mapping[str, Any], context: Optional[str]) -> None:
        if self._needs_query_group_change(config):
            self._apply_query_group(self.config.credentials.query_group)
        if self._needs_database_change(config):
            self._reset_database()

    @contextmanager
    def _use_database_context(self, relation):
        """Issue USE <database> / RESET USE around cross-database operations."""
        needs_use = self.use_show_apis() and self._is_different_database(relation.database)
        if needs_use:
            self._use_database(self._normalize_database(str(relation.database)))
        try:
            yield
        finally:
            if needs_use:
                self._reset_database()

    def create_schema(self, relation) -> None:
        with self._use_database_context(relation):
            super().create_schema(relation)

    def drop_schema(self, relation) -> None:
        with self._use_database_context(relation):
            super().drop_schema(relation)
