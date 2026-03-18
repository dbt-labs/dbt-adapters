import os
from dataclasses import dataclass
from multiprocessing.context import SpawnContext

import agate
from dbt_common.behavior_flags import BehaviorFlag
from dbt_common.contracts.constraints import ConstraintType
from typing import List, Optional, Set, Any, Dict, Type, Mapping
from collections import namedtuple
from dbt.adapters.base import PythonJobHelper
from dbt.adapters.base.impl import AdapterConfig, ConstraintSupport
from dbt.adapters.base.meta import available
from dbt.adapters.capability import Capability, CapabilityDict, CapabilitySupport, Support
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

REDSHIFT_USE_SHOW_APIS = BehaviorFlag(
    name="redshift_use_show_apis",
    default=False,
    description=(
        "Use Redshift SVV_* system views instead of PostgreSQL catalog tables "
        "for metadata queries. Required for cross-database operations with Datasharing. "
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

        if self.config.credentials.ra3_node:
            logger.info(
                "The `ra3_node` configuration in profiles.yml is deprecated. "
                "Use the `redshift_use_show_apis` behavior flag instead. "
            )

    @property
    def _behavior_flags(self) -> List[BehaviorFlag]:
        return [
            REDSHIFT_SKIP_AUTOCOMMIT_TRANSACTION_STATEMENTS,
            REDSHIFT_USE_SHOW_APIS,
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
        """
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

        Returns True when the ``redshift_use_show_apis`` behavior flag.
        """
        return self.behavior.redshift_use_show_apis.no_warn

    @available
    def verify_database(self, database):
        if database.startswith('"'):
            database = database.strip('"')
        expected = self.config.credentials.database
        ra3_node = self.config.credentials.ra3_node

        if database.lower() != expected.lower() and not ra3_node and not self.use_show_apis():
            raise dbt_common.exceptions.NotImplementedError(
                "Cross-db references allowed only in RA3.* node. ({} vs {})".format(
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
        """Translate the result of `show grants` to match the grants config format.

        When ``redshift_use_show_apis`` is enabled, ``SHOW GRANTS ON TABLE``
        returns columns ``identity_name`` and ``privilege_type`` (uppercase).
        Otherwise the legacy query returns ``grantee`` and ``privilege_type``
        (lowercase).
        """
        if not self.use_show_apis():
            return super().standardize_grants_dict(grants_table)

        grants_dict: Dict[str, List[str]] = {}

        for row in grants_table:
            grantee = row["identity_name"]
            privilege = row["privilege_type"].lower()
            if privilege in grants_dict:
                grants_dict[privilege].append(grantee)
            else:
                grants_dict[privilege] = [grantee]

        return grants_dict

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

    def pre_model_hook(self, config: Mapping[str, Any]) -> Optional[str]:
        default_query_group = self.config.credentials.query_group
        model_query_group = config.get("query_group")

        if model_query_group == default_query_group or model_query_group is None:
            return None
        self._set_query_group(model_query_group)
        return None

    def post_model_hook(self, config: Mapping[str, Any], context: Optional[str]) -> None:
        default_query_group = self.config.credentials.query_group
        model_query_group = config.get("query_group")

        if model_query_group == default_query_group:
            return None
        elif default_query_group is None and model_query_group is not None:
            self._unset_query_group()
        else:
            self._set_query_group(default_query_group)
