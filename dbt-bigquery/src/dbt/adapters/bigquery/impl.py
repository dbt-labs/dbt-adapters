from dataclasses import dataclass
from datetime import datetime
from multiprocessing.context import SpawnContext
import threading
from typing import (
    Any,
    Dict,
    FrozenSet,
    Iterable,
    List,
    Optional,
    Tuple,
    TYPE_CHECKING,
    Type,
    Set,
    Union,
)

import google.api_core
import google.auth
import google.oauth2
import google.cloud.bigquery
from google.cloud.bigquery import AccessEntry, Client, SchemaField, Table as BigQueryTable
import google.cloud.exceptions
import pytz

from dbt_common.contracts.constraints import (
    ColumnLevelConstraint,
    ConstraintType,
    ModelLevelConstraint,
)
from dbt_common.dataclass_schema import dbtClassMixin
from dbt_common.events.functions import fire_event
import dbt_common.exceptions
import dbt_common.exceptions.base
from dbt_common.exceptions import DbtInternalError
from dbt_common.utils import filter_null_values
from dbt.adapters.base import (
    AdapterConfig,
    BaseAdapter,
    BaseRelation,
    ConstraintSupport,
    PythonJobHelper,
    RelationType,
    SchemaSearchMap,
    available,
)
from dbt.adapters.base.impl import FreshnessResponse
from dbt.adapters.cache import _make_ref_key_dict
from dbt.adapters.capability import Capability, CapabilityDict, CapabilitySupport, Support
from dbt.adapters.catalogs import CatalogRelation
from dbt.adapters.contracts.connection import AdapterResponse
from dbt.adapters.contracts.macros import MacroResolverProtocol
from dbt.adapters.contracts.relation import RelationConfig
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.events.types import SchemaCreation, SchemaDrop

from dbt.adapters.bigquery import constants, parse_model
from dbt.adapters.bigquery.catalogs import (
    BigLakeCatalogIntegration,
    BigQueryInfoSchemaCatalogIntegration,
    BigQueryCatalogRelation,
)
from dbt.adapters.bigquery.column import BigQueryColumn, get_nested_column_data_types
from dbt.adapters.bigquery.connections import BigQueryAdapterResponse, BigQueryConnectionManager
from dbt.adapters.bigquery.dataset import add_access_entry_to_dataset, is_access_entry_in_dataset
from dbt.adapters.bigquery.python_submissions import (
    ClusterDataprocHelper,
    ServerlessDataProcHelper,
    BigFramesHelper,
)
from dbt.adapters.bigquery.relation import BigQueryRelation
from dbt.adapters.bigquery.relation_configs import (
    BigQueryBaseRelationConfig,
    BigQueryMaterializedViewConfig,
    PartitionConfig,
)
from dbt.adapters.bigquery.utility import sql_escape

if TYPE_CHECKING:
    # Indirectly imported via agate_helper, which is lazy loaded further downfile.
    # Used by mypy for earlier type hints.
    import agate


logger = AdapterLogger("BigQuery")

# Write dispositions for bigquery.
WRITE_APPEND = google.cloud.bigquery.job.WriteDisposition.WRITE_APPEND
WRITE_TRUNCATE = google.cloud.bigquery.job.WriteDisposition.WRITE_TRUNCATE

CREATE_SCHEMA_MACRO_NAME = "create_schema"
_dataset_lock = threading.Lock()


@dataclass
class GrantTarget(dbtClassMixin):
    dataset: str
    project: str

    def render(self):
        return f"{self.project}.{self.dataset}"


@dataclass
class BigqueryConfig(AdapterConfig):
    cluster_by: Optional[Union[List[str], str]] = None
    partition_by: Optional[Dict[str, Any]] = None
    kms_key_name: Optional[str] = None
    labels: Optional[Dict[str, str]] = None
    partitions: Optional[List[str]] = None
    grant_access_to: Optional[List[Dict[str, str]]] = None
    hours_to_expiration: Optional[int] = None
    require_partition_filter: Optional[bool] = None
    partition_expiration_days: Optional[int] = None
    merge_update_columns: Optional[str] = None
    enable_refresh: Optional[bool] = None
    refresh_interval_minutes: Optional[int] = None
    max_staleness: Optional[str] = None
    enable_list_inference: Optional[bool] = None
    intermediate_format: Optional[str] = None
    submission_method: Optional[str] = None
    notebook_template_id: Optional[str] = None


class BigQueryAdapter(BaseAdapter):
    RELATION_TYPES = {
        "TABLE": RelationType.Table,
        "VIEW": RelationType.View,
        "MATERIALIZED_VIEW": RelationType.MaterializedView,
        "EXTERNAL": RelationType.External,
    }

    Relation = BigQueryRelation
    Column = BigQueryColumn
    ConnectionManager = BigQueryConnectionManager

    AdapterSpecificConfigs = BigqueryConfig

    CATALOG_INTEGRATIONS = [BigLakeCatalogIntegration, BigQueryInfoSchemaCatalogIntegration]
    CONSTRAINT_SUPPORT = {
        ConstraintType.check: ConstraintSupport.NOT_SUPPORTED,
        ConstraintType.not_null: ConstraintSupport.ENFORCED,
        ConstraintType.unique: ConstraintSupport.NOT_SUPPORTED,
        ConstraintType.primary_key: ConstraintSupport.NOT_ENFORCED,
        ConstraintType.foreign_key: ConstraintSupport.NOT_ENFORCED,
    }

    _capabilities: CapabilityDict = CapabilityDict(
        {
            Capability.TableLastModifiedMetadata: CapabilitySupport(support=Support.Full),
            Capability.SchemaMetadataByRelations: CapabilitySupport(support=Support.Full),
        }
    )

    def __init__(self, config, mp_context: SpawnContext) -> None:
        super().__init__(config, mp_context)
        self.connections: BigQueryConnectionManager = self.connections
        self.add_catalog_integration(constants.DEFAULT_INFO_SCHEMA_CATALOG)
        self.add_catalog_integration(constants.DEFAULT_ICEBERG_CATALOG)

    ###
    # Implementations of abstract methods
    ###

    @classmethod
    def date_function(cls) -> str:
        return "CURRENT_TIMESTAMP()"

    @classmethod
    def is_cancelable(cls) -> bool:
        return True

    def drop_relation(self, relation: BigQueryRelation) -> None:
        is_cached = self._schema_is_cached(relation.database, relation.schema)  # type:ignore
        if is_cached:
            self.cache_dropped(relation)

        conn = self.connections.get_thread_connection()

        table_ref = self.get_table_ref_from_relation(relation)

        # mimic "drop if exists" functionality that's ubiquitous in most sql implementations
        conn.handle.delete_table(table_ref, not_found_ok=True)

    def truncate_relation(self, relation: BigQueryRelation) -> None:
        raise dbt_common.exceptions.base.NotImplementedError(
            "`truncate` is not implemented for this adapter!"
        )

    def rename_relation(
        self, from_relation: BigQueryRelation, to_relation: BigQueryRelation
    ) -> None:
        conn = self.connections.get_thread_connection()
        client = conn.handle

        from_table_ref = self.get_table_ref_from_relation(from_relation)
        from_table = client.get_table(from_table_ref)
        if (
            from_table.table_type == "VIEW"
            or from_relation.type == RelationType.View
            or to_relation.type == RelationType.View
        ):
            raise dbt_common.exceptions.DbtRuntimeError(
                "Renaming of views is not currently supported in BigQuery"
            )

        to_table_ref = self.get_table_ref_from_relation(to_relation)

        self.cache_renamed(from_relation, to_relation)
        client.copy_table(from_table_ref, to_table_ref)
        client.delete_table(from_table_ref)

    @available
    def list_schemas(self, database: str) -> List[str]:
        return self.connections.list_dataset(database)

    @available.parse(lambda *a, **k: False)
    def check_schema_exists(self, database: str, schema: str) -> bool:
        conn = self.connections.get_thread_connection()
        client = conn.handle

        dataset_ref = self.connections.dataset_ref(database, schema)
        # try to do things with the dataset. If it doesn't exist it will 404.
        # we have to do it this way to handle underscore-prefixed datasets,
        # which appear in neither the information_schema.schemata view nor the
        # list_datasets method.
        try:
            next(iter(client.list_tables(dataset_ref, max_results=1)))
        except StopIteration:
            pass
        except google.api_core.exceptions.NotFound:
            # the schema does not exist
            return False
        return True

    @available.parse(lambda *a, **k: {})
    @classmethod
    def nest_column_data_types(
        cls,
        columns: Dict[str, Dict[str, Any]],
        constraints: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Dict[str, Optional[str]]]:
        return get_nested_column_data_types(columns, constraints)

    def get_columns_in_relation(self, relation: BigQueryRelation) -> List[BigQueryColumn]:
        try:
            table = self.connections.get_bq_table(
                database=relation.database, schema=relation.schema, identifier=relation.identifier
            )
            return self._get_dbt_columns_from_bq_table(table)

        except (ValueError, google.cloud.exceptions.NotFound) as e:
            logger.debug("get_columns_in_relation error: {}".format(e))
            return []

    @available.parse(lambda *a, **k: [])
    def add_time_ingestion_partition_column(self, partition_by, columns) -> List[BigQueryColumn]:
        """Add time ingestion partition column to columns list"""
        columns.append(
            self.Column(
                partition_by.insertable_time_partitioning_field(),
                partition_by.data_type,
                None,
                "NULLABLE",
            )
        )
        return columns

    def expand_column_types(self, goal: BigQueryRelation, current: BigQueryRelation) -> None:
        # This is a no-op on BigQuery
        pass

    def expand_target_column_types(
        self, from_relation: BigQueryRelation, to_relation: BigQueryRelation
    ) -> None:
        # This is a no-op on BigQuery
        pass

    @available.parse_list
    def list_relations_without_caching(
        self, schema_relation: BigQueryRelation
    ) -> List[BigQueryRelation]:
        connection = self.connections.get_thread_connection()
        client = connection.handle

        dataset_ref = self.connections.dataset_ref(
            schema_relation.database, schema_relation.schema
        )

        all_tables = client.list_tables(
            dataset_ref,
            # BigQuery paginates tables by alphabetizing them, and using
            # the name of the last table on a page as the key for the
            # next page. If that key table gets dropped before we run
            # list_relations, then this will 404. So, we avoid this
            # situation by making the page size sufficiently large.
            # see: https://github.com/dbt-labs/dbt/issues/726
            # TODO: cache the list of relations up front, and then we
            #       won't need to do this
            max_results=100000,
        )

        # This will 404 if the dataset does not exist. This behavior mirrors
        # the implementation of list_relations for other adapters
        try:
            return [self._bq_table_to_relation(table) for table in all_tables]  # type: ignore[misc]
        except google.api_core.exceptions.NotFound:
            return []
        except google.api_core.exceptions.Forbidden as exc:
            logger.debug("list_relations_without_caching error: {}".format(str(exc)))
            return []

    def get_relation(
        self, database: str, schema: str, identifier: str
    ) -> Optional[BigQueryRelation]:
        if self._schema_is_cached(database, schema):
            # if it's in the cache, use the parent's model of going through
            # the relations cache and picking out the relation
            return super().get_relation(database=database, schema=schema, identifier=identifier)

        try:
            table = self.connections.get_bq_table(database, schema, identifier)
        except google.api_core.exceptions.NotFound:
            table = None
        return self._bq_table_to_relation(table)

    # BigQuery added SQL support for 'create schema' + 'drop schema' in March 2021
    # Unfortunately, 'drop schema' runs into permissions issues during tests
    # Most of the value here comes from user overrides of 'create_schema'

    # TODO: the code below is copy-pasted from SQLAdapter.create_schema. Is there a better way?
    def create_schema(self, relation: BigQueryRelation) -> None:
        # use SQL 'create schema'
        relation = relation.without_identifier()

        fire_event(SchemaCreation(relation=_make_ref_key_dict(relation)))
        kwargs = {
            "relation": relation,
        }
        self.execute_macro(CREATE_SCHEMA_MACRO_NAME, kwargs=kwargs)
        self.commit_if_has_connection()
        # we can't update the cache here, as if the schema already existed we
        # don't want to (incorrectly) say that it's empty

    def drop_schema(self, relation: BigQueryRelation) -> None:
        # still use a client method, rather than SQL 'drop schema ... cascade'
        database = relation.database
        schema = relation.schema
        logger.debug('Dropping schema "{}.{}".', database, schema)  # in lieu of SQL
        fire_event(SchemaDrop(relation=_make_ref_key_dict(relation)))
        self.connections.drop_dataset(database, schema)
        self.cache.drop_schema(database, schema)

    @classmethod
    def quote(cls, identifier: str) -> str:
        return "`{}`".format(identifier)

    @classmethod
    def convert_text_type(cls, agate_table: "agate.Table", col_idx: int) -> str:
        return "string"

    @classmethod
    def convert_number_type(cls, agate_table: "agate.Table", col_idx: int) -> str:
        import agate

        decimals = agate_table.aggregate(agate.MaxPrecision(col_idx))
        return "float64" if decimals else "int64"

    @classmethod
    def convert_integer_type(cls, agate_table: "agate.Table", col_idx: int) -> str:
        return "int64"

    @classmethod
    def convert_boolean_type(cls, agate_table: "agate.Table", col_idx: int) -> str:
        return "bool"

    @classmethod
    def convert_datetime_type(cls, agate_table: "agate.Table", col_idx: int) -> str:
        return "datetime"

    @classmethod
    def convert_date_type(cls, agate_table: "agate.Table", col_idx: int) -> str:
        return "date"

    @classmethod
    def convert_time_type(cls, agate_table: "agate.Table", col_idx: int) -> str:
        return "time"

    ###
    # Implementation details
    ###
    def _make_match_kwargs(self, database: str, schema: str, identifier: str) -> Dict[str, str]:
        return filter_null_values(
            {
                "database": database,
                "identifier": identifier,
                "schema": schema,
            }
        )

    def _get_dbt_columns_from_bq_table(self, table) -> List[BigQueryColumn]:
        "Translates BQ SchemaField dicts into dbt BigQueryColumn objects"

        columns = []
        for col in table.schema:
            # BigQuery returns type labels that are not valid type specifiers
            dtype = self.Column.translate_type(col.field_type)
            column = self.Column(col.name, dtype, col.fields, col.mode)
            columns.append(column)

        return columns

    def _agate_to_schema(
        self, agate_table: "agate.Table", column_override: Dict[str, str]
    ) -> List[SchemaField]:
        """Convert agate.Table with column names to a list of bigquery schemas."""
        bq_schema = []
        for idx, col_name in enumerate(agate_table.column_names):
            inferred_type = self.convert_agate_type(agate_table, idx)
            type_ = column_override.get(col_name, inferred_type)
            bq_schema.append(SchemaField(col_name, type_))
        return bq_schema

    @available.parse(lambda *a, **k: "")
    def copy_table(self, source, destination, materialization):
        if materialization == "incremental":
            write_disposition = WRITE_APPEND
        elif materialization == "table":
            write_disposition = WRITE_TRUNCATE
        else:
            raise dbt_common.exceptions.CompilationError(
                'Copy table materialization must be "copy" or "table", but '
                f"config.get('copy_materialization', 'table') was "
                f"{materialization}"
            )

        self.connections.copy_bq_table(source, destination, write_disposition)

        return "COPY TABLE with materialization: {}".format(materialization)

    @available.parse(lambda *a, **k: [])
    def get_column_schema_from_query(self, sql: str) -> List[BigQueryColumn]:
        """Get a list of the column names and data types from the given sql.

        :param str sql: The sql to execute.
        :return: List[BigQueryColumn]
        """
        _, iterator = self.connections.raw_execute_with_comment(sql)
        columns = [self.Column.create_from_field(field) for field in iterator.schema]
        flattened_columns = []
        for column in columns:
            flattened_columns += column.flatten()
        return flattened_columns

    @available.parse(lambda *a, **k: False)
    def get_columns_in_select_sql(self, select_sql: str) -> List[BigQueryColumn]:
        try:
            conn = self.connections.get_thread_connection()
            client = conn.handle
            query_job, iterator = self.connections.raw_execute_with_comment(select_sql)
            query_table = client.get_table(query_job.destination)
            return self._get_dbt_columns_from_bq_table(query_table)

        except (ValueError, google.cloud.exceptions.NotFound) as e:
            logger.debug("get_columns_in_select_sql error: {}".format(e))
            return []

    def _bq_table_to_relation(self, bq_table) -> Union[BigQueryRelation, None]:
        if bq_table is None:
            return None

        return self.Relation.create(
            database=bq_table.project,
            schema=bq_table.dataset_id,
            identifier=bq_table.table_id,
            quote_policy={"schema": True, "identifier": True},
            type=self.RELATION_TYPES.get(
                bq_table.table_type, RelationType.External
            ),  # type:ignore
        )

    @classmethod
    def warning_on_hooks(cls, hook_type):
        msg = "{} is not supported in bigquery and will be ignored"
        logger.info(msg)

    @available
    def add_query(self, sql, auto_begin=True, bindings=None, abridge_sql_log=False):
        if self.nice_connection_name() in ["on-run-start", "on-run-end"]:
            self.warning_on_hooks(self.nice_connection_name())
        else:
            raise dbt_common.exceptions.base.NotImplementedError(
                "`add_query` is not implemented for this adapter!"
            )

    ###
    # Special bigquery adapter methods
    ###

    @staticmethod
    def _partitions_match(table, conf_partition: Optional[PartitionConfig]) -> bool:
        """
        Check if the actual and configured partitions for a table are a match.
        BigQuery tables can be replaced if:
        - Both tables are not partitioned, OR
        - Both tables are partitioned using the exact same configs

        If there is a mismatch, then the table cannot be replaced directly.
        """
        is_partitioned = table.range_partitioning or table.time_partitioning

        if not is_partitioned and not conf_partition:
            return True
        elif conf_partition and table.time_partitioning is not None:
            table_field = (
                table.time_partitioning.field.lower() if table.time_partitioning.field else None
            )

            table_granularity = table.partitioning_type
            conf_table_field = conf_partition.field
            return (
                table_field == conf_table_field.lower()
                or (conf_partition.time_ingestion_partitioning and table_field is not None)
            ) and table_granularity.lower() == conf_partition.granularity.lower()
        elif conf_partition and table.range_partitioning is not None:
            dest_part = table.range_partitioning
            conf_part = conf_partition.range or {}

            return (
                dest_part.field == conf_partition.field
                and dest_part.range_.start == conf_part.get("start")
                and dest_part.range_.end == conf_part.get("end")
                and dest_part.range_.interval == conf_part.get("interval")
            )
        else:
            return False

    @staticmethod
    def _clusters_match(table, conf_cluster) -> bool:
        """
        Check if the actual and configured clustering columns for a table
        are a match. BigQuery tables can be replaced if clustering columns
        match exactly.
        """
        if isinstance(conf_cluster, str):
            conf_cluster = [conf_cluster]

        return table.clustering_fields == conf_cluster

    @available.parse(lambda *a, **k: True)
    def is_replaceable(
        self, relation, conf_partition: Optional[PartitionConfig], conf_cluster
    ) -> bool:
        """
        Check if a given partition and clustering column spec for a table
        can replace an existing relation in the database. BigQuery does not
        allow tables to be replaced with another table that has a different
        partitioning spec. This method returns True if the given config spec is
        identical to that of the existing table.
        """
        if not relation:
            return True

        try:
            table = self.connections.get_bq_table(
                database=relation.database, schema=relation.schema, identifier=relation.identifier
            )
        except google.cloud.exceptions.NotFound:
            return True

        return all(
            (
                self._partitions_match(table, conf_partition),
                self._clusters_match(table, conf_cluster),
            )
        )

    @available
    def parse_partition_by(self, raw_partition_by: Any) -> Optional[PartitionConfig]:
        """
        dbt v0.16.0 expects `partition_by` to be a dictionary where previously
        it was a string. Check the type of `partition_by`, raise error
        or warning if string, and attempt to convert to dict.
        """
        return PartitionConfig.parse(raw_partition_by)

    def get_table_ref_from_relation(self, relation: BaseRelation):
        return self.connections.table_ref(relation.database, relation.schema, relation.identifier)

    def _update_column_dict(self, bq_column_dict, dbt_columns, parent=""):
        """
        Helper function to recursively traverse the schema of a table in the
        update_column_descriptions function below.

        bq_column_dict should be a dict as obtained by the to_api_repr()
        function of a SchemaField object.
        """
        if parent:
            dotted_column_name = "{}.{}".format(parent, bq_column_dict["name"])
        else:
            dotted_column_name = bq_column_dict["name"]

        if dotted_column_name in dbt_columns:
            column_config = dbt_columns[dotted_column_name]
            bq_column_dict["description"] = column_config.get("description")
            if bq_column_dict["type"] != "RECORD":
                bq_column_dict["policyTags"] = {"names": column_config.get("policy_tags", list())}

        new_fields = []
        for child_col_dict in bq_column_dict.get("fields", list()):
            new_child_column_dict = self._update_column_dict(
                child_col_dict, dbt_columns, parent=dotted_column_name
            )
            new_fields.append(new_child_column_dict)

        bq_column_dict["fields"] = new_fields

        return bq_column_dict

    @available.parse_none
    def update_columns(self, relation, columns):
        if len(columns) == 0:
            return

        conn = self.connections.get_thread_connection()
        table_ref = self.get_table_ref_from_relation(relation)
        table = conn.handle.get_table(table_ref)

        new_schema = []
        for bq_column in table.schema:
            bq_column_dict = bq_column.to_api_repr()
            new_bq_column_dict = self._update_column_dict(bq_column_dict, columns)
            new_schema.append(SchemaField.from_api_repr(new_bq_column_dict))

        new_table = google.cloud.bigquery.Table(table_ref, schema=new_schema)
        conn.handle.update_table(new_table, ["schema"])

    @available.parse_none
    def update_table_description(
        self, database: str, schema: str, identifier: str, description: str
    ):
        conn = self.connections.get_thread_connection()
        client = conn.handle

        table_ref = self.connections.table_ref(database, schema, identifier)
        table = client.get_table(table_ref)
        table.description = description
        client.update_table(table, ["description"])

    @available.parse_none
    def alter_table_add_columns(self, relation, columns):
        logger.debug('Adding columns ({}) to table {}".'.format(columns, relation))

        conn = self.connections.get_thread_connection()
        client = conn.handle

        table_ref = self.get_table_ref_from_relation(relation)
        table = client.get_table(table_ref)

        new_columns = [col.column_to_bq_schema() for col in columns]
        new_schema = table.schema + new_columns

        new_table = google.cloud.bigquery.Table(table_ref, schema=new_schema)
        client.update_table(new_table, ["schema"])

    @available.parse_none
    def load_dataframe(
        self,
        database: str,
        schema: str,
        table_name: str,
        agate_table: "agate.Table",
        column_override: Dict[str, str],
        field_delimiter: str,
    ) -> None:
        connection = self.connections.get_thread_connection()
        client: Client = connection.handle
        table_schema = self._agate_to_schema(agate_table, column_override)
        file_path = agate_table.original_abspath

        self.connections.write_dataframe_to_table(
            client,
            file_path,
            database,
            schema,
            table_name,
            table_schema,
            field_delimiter,
            fallback_timeout=300,
        )

    @available.parse_none
    def upload_file(
        self,
        local_file_path: str,
        database: str,
        table_schema: str,
        table_name: str,
        **kwargs,
    ) -> None:
        connection = self.connections.get_thread_connection()
        client: Client = connection.handle

        self.connections.write_file_to_table(
            client,
            local_file_path,
            database,
            table_schema,
            table_name,
            fallback_timeout=300,
            **kwargs,
        )

    @classmethod
    def _catalog_filter_table(
        cls, table: "agate.Table", used_schemas: FrozenSet[Tuple[str, str]]
    ) -> "agate.Table":
        table = table.rename(
            column_names={col.name: col.name.replace("__", ":") for col in table.columns}
        )
        return super()._catalog_filter_table(table, used_schemas)

    def _get_catalog_schemas(self, relation_config: Iterable[RelationConfig]) -> SchemaSearchMap:
        candidates = super()._get_catalog_schemas(relation_config)
        db_schemas: Dict[str, Set[str]] = {}
        result = SchemaSearchMap()

        for candidate, schemas in candidates.items():
            database = candidate.database
            if database not in db_schemas:
                db_schemas[database] = set(self.list_schemas(database))  # type:ignore
            if candidate.schema in db_schemas[database]:  # type:ignore
                result[candidate] = schemas
            else:
                logger.debug(
                    "Skipping catalog for {}.{} - schema does not exist".format(
                        database, candidate.schema
                    )
                )
        return result

    def calculate_freshness_from_metadata(
        self,
        source: BaseRelation,
        macro_resolver: Optional[MacroResolverProtocol] = None,
    ) -> Tuple[Optional[AdapterResponse], FreshnessResponse]:
        conn = self.connections.get_thread_connection()
        client: Client = conn.handle

        table_ref = self.get_table_ref_from_relation(source)
        table = client.get_table(table_ref)
        snapshot = datetime.now(tz=pytz.UTC)

        freshness = FreshnessResponse(
            max_loaded_at=table.modified,
            snapshotted_at=snapshot,
            age=(snapshot - table.modified).total_seconds(),
        )

        return None, freshness

    @available.parse(lambda *a, **k: {})
    def get_common_options(
        self, config: Dict[str, Any], node: Dict[str, Any], temporary: bool = False
    ) -> Dict[str, Any]:
        opts = {}

        if (config.get("hours_to_expiration") is not None) and (not temporary):
            expiration = f'TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL {config.get("hours_to_expiration")} hour)'
            opts["expiration_timestamp"] = expiration

        if config.persist_relation_docs() and "description" in node:  # type: ignore[attr-defined]
            description = sql_escape(node["description"])
            opts["description"] = '"""{}"""'.format(description)

        labels = config.get("labels") or {}

        if config.get("labels_from_meta"):
            meta = config.get("meta") or {}
            labels = {**meta, **labels}  # Merge with priority to labels

        if labels:
            opts["labels"] = list(labels.items())  # type: ignore[assignment]

        return opts

    @available.parse(lambda *a, **k: {})
    def get_table_options(
        self, config: Dict[str, Any], node: Dict[str, Any], temporary: bool
    ) -> Dict[str, Any]:
        opts = self.get_common_options(config, node, temporary)

        if config.get("kms_key_name") is not None:
            opts["kms_key_name"] = f"'{config.get('kms_key_name')}'"

        if temporary:
            opts["expiration_timestamp"] = "TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 12 hour)"
        else:
            # It doesn't apply the `require_partition_filter` option for a temporary table
            # so that we avoid the error by not specifying a partition with a temporary table
            # in the incremental model.
            if (
                config.get("require_partition_filter") is not None
                and config.get("partition_by") is not None
            ):
                opts["require_partition_filter"] = config.get("require_partition_filter")
            if config.get("partition_expiration_days") is not None:
                opts["partition_expiration_days"] = config.get("partition_expiration_days")

            relation_config = getattr(config, "model", None)
            if not temporary and (
                catalog_relation := self.build_catalog_relation(relation_config)
            ):
                if not isinstance(catalog_relation, BigQueryCatalogRelation):
                    raise DbtInternalError("Unexpected catalog relation")
                if catalog_relation.table_format == constants.ICEBERG_TABLE_FORMAT:
                    opts["table_format"] = f"'{catalog_relation.table_format}'"
                    opts["file_format"] = f"'{catalog_relation.file_format}'"
                    opts["storage_uri"] = f"'{catalog_relation.storage_uri}'"

        return opts

    @available.parse(lambda *a, **k: {})
    def get_view_options(self, config: Dict[str, Any], node: Dict[str, Any]) -> Dict[str, Any]:
        opts = self.get_common_options(config, node)
        return opts

    @available.parse(lambda *a, **k: True)
    def get_bq_table(self, relation: BigQueryRelation) -> Optional[BigQueryTable]:
        try:
            table = self.connections.get_bq_table(
                relation.database, relation.schema, relation.identifier
            )
        except google.cloud.exceptions.NotFound:
            table = None
        return table

    @available.parse(lambda *a, **k: True)
    def describe_relation(
        self, relation: BigQueryRelation
    ) -> Optional[BigQueryBaseRelationConfig]:
        if relation.type == RelationType.MaterializedView:
            bq_table = self.get_bq_table(relation)
            parser = BigQueryMaterializedViewConfig
        else:
            raise dbt_common.exceptions.DbtRuntimeError(
                f"The method `BigQueryAdapter.describe_relation` is not implemented "
                f"for the relation type: {relation.type}"
            )
        if bq_table:
            return parser.from_bq_table(bq_table)
        return None

    @available.parse_none
    def grant_access_to(self, entity, entity_type, role, grant_target_dict) -> None:
        """
        Given an entity, grants it access to a dataset.
        """
        conn: BigQueryConnectionManager = self.connections.get_thread_connection()
        client = conn.handle  # type:ignore
        GrantTarget.validate(grant_target_dict)
        grant_target = GrantTarget.from_dict(grant_target_dict)
        if entity_type == "view":
            entity = self.get_table_ref_from_relation(entity).to_api_repr()
        with _dataset_lock:
            dataset_ref = self.connections.dataset_ref(grant_target.project, grant_target.dataset)
            dataset = client.get_dataset(dataset_ref)
            access_entry = AccessEntry(role, entity_type, entity)
            # only perform update if access entry not in dataset
            if is_access_entry_in_dataset(dataset, access_entry):
                logger.warning(f"Access entry {access_entry} " f"already exists in dataset")
            else:
                dataset = add_access_entry_to_dataset(dataset, access_entry)
                client.update_dataset(dataset, ["access_entries"])

    @available.parse_none
    def get_dataset_location(self, relation):
        conn = self.connections.get_thread_connection()
        client = conn.handle
        dataset_ref = self.connections.dataset_ref(relation.project, relation.dataset)
        dataset = client.get_dataset(dataset_ref)
        return dataset.location

    def get_rows_different_sql(
        self,
        relation_a: BigQueryRelation,
        relation_b: BigQueryRelation,
        column_names: Optional[List[str]] = None,
        except_operator="EXCEPT DISTINCT",
    ) -> str:
        return super().get_rows_different_sql(
            relation_a=relation_a,
            relation_b=relation_b,
            column_names=column_names,
            except_operator=except_operator,
        )

    def timestamp_add_sql(self, add_to: str, number: int = 1, interval: str = "hour") -> str:
        return f"timestamp_add({add_to}, interval {number} {interval})"

    def string_add_sql(
        self,
        add_to: str,
        value: str,
        location="append",
    ) -> str:
        if location == "append":
            return f"concat({add_to}, '{value}')"
        elif location == "prepend":
            return f"concat('{value}', {add_to})"
        else:
            raise dbt_common.exceptions.DbtRuntimeError(
                f'Got an unexpected location value of "{location}"'
            )

    # This is used by the test suite
    @available
    def run_sql_for_tests(self, sql, fetch, conn=None):
        """For the testing framework.
        Run an SQL query on a bigquery adapter. No cursors, transactions,
        etc. to worry about"""

        do_fetch = fetch != "None"
        _, res = self.execute(sql, fetch=do_fetch)

        # convert dataframe to matrix-ish repr
        if fetch == "one":
            return res[0]
        else:
            return list(res)

    def generate_python_submission_response(self, submission_result) -> BigQueryAdapterResponse:
        return BigQueryAdapterResponse(_message="OK")

    @property
    def default_python_submission_method(self) -> str:
        if (
            hasattr(self.connections.profile.credentials, "submission_method")
            and self.connections.profile.credentials.submission_method
        ):
            return self.connections.profile.credentials.submission_method
        return "serverless"

    @property
    def python_submission_helpers(self) -> Dict[str, Type[PythonJobHelper]]:
        return {
            "cluster": ClusterDataprocHelper,
            "serverless": ServerlessDataProcHelper,
            "bigframes": BigFramesHelper,
        }

    @available
    @classmethod
    def render_raw_columns_constraints(cls, raw_columns: Dict[str, Dict[str, Any]]) -> List:
        rendered_constraints: Dict[str, str] = {}
        for raw_column in raw_columns.values():
            for con in raw_column.get("constraints", None):
                constraint = cls._parse_column_constraint(con)
                rendered_constraint = cls.process_parsed_constraint(
                    constraint, cls.render_column_constraint
                )

                if rendered_constraint:
                    column_name = raw_column["name"]
                    if column_name not in rendered_constraints:
                        rendered_constraints[column_name] = rendered_constraint
                    else:
                        rendered_constraints[column_name] += f" {rendered_constraint}"

        nested_columns = cls.nest_column_data_types(raw_columns, rendered_constraints)
        rendered_column_constraints = [
            f"{cls.quote(column['name']) if column.get('quote') else column['name']} {column['data_type']}"
            for column in nested_columns.values()
        ]
        return rendered_column_constraints

    @classmethod
    def render_column_constraint(cls, constraint: ColumnLevelConstraint) -> Optional[str]:
        c = super().render_column_constraint(constraint)
        if (
            constraint.type == ConstraintType.primary_key
            or constraint.type == ConstraintType.foreign_key
        ):
            return f"{c} not enforced" if c else None
        return c

    @classmethod
    def render_model_constraint(cls, constraint: ModelLevelConstraint) -> Optional[str]:
        c = super().render_model_constraint(constraint)
        if (
            constraint.type == ConstraintType.primary_key
            or constraint.type == ConstraintType.foreign_key
        ):
            return f"{c} not enforced" if c else None

        return c

    def debug_query(self):
        """Override for DebugTask method"""
        self.execute("select 1 as id")

    def validate_sql(self, sql: str) -> AdapterResponse:
        """Submit the given SQL to the engine for validation, but not execution.

        This submits the query with the `dry_run` flag set True.

        :param str sql: The sql to validate
        """
        return self.connections.dry_run(sql)

    @available
    def build_catalog_relation(self, model: RelationConfig) -> Optional[CatalogRelation]:
        """
        Builds a relation for a given configuration.

        This method uses the provided configuration to determine the appropriate catalog
        integration and config parser for building the relation. It defaults to the information schema
        catalog if none is provided in the configuration for backward compatibility.

        Args:
            model (RelationConfig): `config.model` (not `model`) from the jinja context

        Returns:
            Any: The constructed relation object generated through the catalog integration and parser
        """
        if catalog := parse_model.catalog_name(model):
            catalog_integration = self.get_catalog_integration(catalog)
            return catalog_integration.build_relation(model)
        return None
