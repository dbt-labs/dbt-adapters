import os
from dataclasses import dataclass

from dbt_common.contracts.constraints import ConstraintType
from typing import Optional, Set, Any, Dict, Type, TYPE_CHECKING
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

if TYPE_CHECKING:
    import agate


@dataclass
class RedshiftConfig(AdapterConfig):
    sort_type: Optional[str] = None
    dist: Optional[str] = None
    sort: Optional[str] = None
    bind: Optional[bool] = None
    backup: Optional[bool] = True
    auto_refresh: Optional[bool] = False


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
    def verify_database(self, database):
        if database.startswith('"'):
            database = database.strip('"')
        expected = self.config.credentials.database
        ra3_node = self.config.credentials.ra3_node

        if database.lower() != expected.lower() and not ra3_node:
            raise dbt_common.exceptions.NotImplementedError(
                "Cross-db references allowed only in RA3.* node. ({} vs {})".format(
                    database, expected
                )
            )
        # return an empty string on success so macros can call this
        return ""

    def _get_catalog_schemas(self, manifest):
        # redshift(besides ra3) only allow one database (the main one)
        schemas = super(SQLAdapter, self)._get_catalog_schemas(manifest)
        try:
            return schemas.flatten(allow_multiple_databases=self.config.credentials.ra3_node)
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
