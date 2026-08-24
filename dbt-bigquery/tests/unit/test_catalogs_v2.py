"""Unit tests for BigQueryAdapter bridge_v2_catalog hook methods."""

from datetime import date
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from dbt.adapters.base import BaseAdapter
from dbt.adapters.bigquery import constants
from dbt.adapters.bigquery.impl import BigQueryAdapter
from dbt.adapters.planning import (
    CatalogBindingState,
    CatalogFacts,
    CreateFromQueryFacts,
    ExistingRelationFacts,
    FormatFacts,
    MaterializationOperationKind,
    MaterializationStatementStrategy,
    MaterializationTransactionMode,
    RelationFacts,
    RuntimeFacts,
    TableMaterializationFacts,
)


def _v2_catalog(name, catalog_type, table_format_value, config=None):
    return SimpleNamespace(
        name=name,
        catalog_type=catalog_type,
        table_format=SimpleNamespace(value=table_format_value),
        config=config or {},
    )


class TestBigQueryV2ToV1Type:
    def setup_method(self):
        self.adapter = object.__new__(BigQueryAdapter)

    def test_biglake_metastore(self):
        assert self.adapter._v2_to_v1_type("biglake_metastore") == "biglake_metastore"

    def test_unknown_passthrough(self):
        assert self.adapter._v2_to_v1_type("custom_type") == "custom_type"


def _table_facts():
    relation = RelationFacts("project", "dataset", "table", "table")
    return TableMaterializationFacts(
        create=CreateFromQueryFacts(
            relation=relation,
            catalog=CatalogFacts(state=CatalogBindingState.UNBOUND),
            format=FormatFacts(),
            runtime=RuntimeFacts(engine="bigquery"),
        ),
        existing=ExistingRelationFacts(
            relation=relation,
            format=FormatFacts(),
            can_be_renamed=False,
            can_be_replaced=True,
            requires_drop_before_replace=False,
        ),
    )


class TestBigQueryTableMaterializationPlanning:
    def setup_method(self):
        self.adapter = object.__new__(BigQueryAdapter)

    def test_table_plan_uses_non_transactional_direct_replace(self):
        plan = self.adapter.plan_table_materialization(
            "macro.dbt_bigquery.materialization_table_bigquery",
            "sql",
        )

        assert plan.statement == MaterializationStatementStrategy.NO_AUTO_BEGIN

    def test_biglake_provider_and_iceberg_capabilities_are_explicit(self):
        catalog_relation = SimpleNamespace(
            catalog_type=constants.BIGLAKE_CATALOG_TYPE,
            table_format=constants.ICEBERG_TABLE_FORMAT,
        )
        self.adapter.build_catalog_relation = MagicMock(return_value=catalog_relation)
        model = MagicMock()

        assert (
            self.adapter.get_create_from_query_catalog_provider(catalog_relation, model)
            == "biglake"
        )
        facts = self.adapter.get_table_materialization_execution_facts(
            model,
            MagicMock(),
        )
        assert facts.transaction_mode == MaterializationTransactionMode.NONE
        assert "biglake_iceberg" in facts.capabilities

    def test_biglake_provider_is_retained_in_incremental_facts(self):
        catalog_relation = SimpleNamespace(
            catalog_type=constants.BIGLAKE_CATALOG_TYPE,
            catalog_name="analytics",
            catalog_database="lakehouse",
            table_format=constants.ICEBERG_TABLE_FORMAT,
            file_format="parquet",
            external_volume=None,
        )

        facts = self.adapter.build_incremental_mutation_facts(
            requested_strategy="merge",
            language="sql",
            unique_key="id",
            requested_temp_relation_type=None,
            catalog_relation=catalog_relation,
        )

        assert facts.catalog.catalog_provider == "biglake"
        assert facts.catalog.integration_name == "analytics"
        assert facts.format.table_provider == "parquet"

    def test_changed_partitioning_forces_drop_before_replace(self):
        existing = MagicMock()
        existing.is_table = True
        model = MagicMock()
        model.config = {"partition_by": {"field": "event_date"}, "cluster_by": ["id"]}
        self.adapter.parse_partition_by = MagicMock(return_value="parsed-partition")
        self.adapter.is_replaceable = MagicMock(return_value=False)

        with patch.object(
            BaseAdapter,
            "build_table_materialization_facts",
            return_value=_table_facts(),
        ):
            facts = self.adapter.build_table_materialization_facts(
                model,
                MagicMock(),
                existing,
            )

        self.adapter.is_replaceable.assert_called_once_with(
            existing,
            "parsed-partition",
            ["id"],
        )
        assert facts.existing is not None
        assert facts.existing.requires_drop_before_replace is True


class TestBigQueryIncrementalMaterializationPlanning:
    def setup_method(self):
        self.adapter = object.__new__(BigQueryAdapter)

    def test_specialized_sql_materialization_opts_into_typed_executor(self):
        plan = self.adapter.plan_incremental_materialization(
            "macro.dbt_bigquery.materialization_incremental_bigquery",
            "sql",
        )

        assert plan is not None
        assert plan.materialization_macro_id.endswith("materialization_incremental_bigquery")

    def test_partition_config_is_normalized_into_plan_facts(self):
        model = SimpleNamespace(
            config={
                "partition_by": {
                    "field": "event_ts",
                    "data_type": "timestamp",
                    "granularity": "day",
                    "time_ingestion_partitioning": True,
                    "copy_partitions": True,
                },
                "partitions": ["date('2026-08-22')"],
                "require_partition_filter": True,
            }
        )

        facts = self.adapter._incremental_partition_facts(model)

        assert facts is not None
        assert facts.field == "event_ts"
        assert facts.time_ingestion_partitioning is True
        assert facts.copy_partitions is True
        assert facts.require_partition_filter is True
        assert facts.static_partitions == ("date('2026-08-22')",)

    def test_merge_lifecycle_program_owns_staging_schema_and_mutation(self):
        model = SimpleNamespace(config={"partition_by": {"field": "event_date"}})
        self.adapter.build_table_materialization_facts = MagicMock(return_value=_table_facts())
        mutation = self.adapter.plan_incremental_mutation("merge", language="sql")

        lifecycle = self.adapter.resolve_incremental_lifecycle_plan(
            mutation,
            model,
            MagicMock(),
            MagicMock(),
            full_refresh=False,
            on_schema_change="append_new_columns",
            staging_is_temporary=True,
            contract_enforced=False,
        )

        assert [operation.kind for operation in lifecycle.operations] == [
            MaterializationOperationKind.RUN_HOOKS,
            MaterializationOperationKind.CREATE_FROM_QUERY,
            MaterializationOperationKind.PROCESS_SCHEMA_CHANGES,
            MaterializationOperationKind.EXECUTE_INCREMENTAL_MUTATION,
            MaterializationOperationKind.DROP_RELATION_IF_EXISTS,
            MaterializationOperationKind.RUN_HOOKS,
            MaterializationOperationKind.APPLY_GRANTS,
            MaterializationOperationKind.PERSIST_DOCUMENTATION,
        ]
        assert lifecycle.partition is not None
        assert lifecycle.partition.field == "event_date"

    def test_unknown_incremental_strategy_is_rejected_by_typed_planner(self):
        plan = self.adapter.plan_incremental_mutation("custom", language="sql")

        assert plan.strategy.value == "unsupported"
        assert "Expected one of" in (plan.reason or "")

    def test_copy_partitions_is_an_explicit_operation_not_renderer_side_effect(self):
        model = SimpleNamespace(
            config={
                "partition_by": {
                    "field": "event_date",
                    "copy_partitions": True,
                },
                "partitions": ["date('2026-08-23')"],
            }
        )
        self.adapter.build_table_materialization_facts = MagicMock(return_value=_table_facts())
        mutation = self.adapter.plan_incremental_mutation("insert_overwrite", language="sql")

        lifecycle = self.adapter.resolve_incremental_lifecycle_plan(
            mutation,
            model,
            MagicMock(),
            MagicMock(),
            full_refresh=False,
            on_schema_change="ignore",
            staging_is_temporary=True,
            contract_enforced=False,
        )

        assert MaterializationOperationKind.COPY_INCREMENTAL_PARTITIONS in {
            operation.kind for operation in lifecycle.operations
        }
        assert MaterializationOperationKind.EXECUTE_INCREMENTAL_MUTATION not in {
            operation.kind for operation in lifecycle.operations
        }

    def test_partition_copy_executes_from_typed_static_partition_facts(self):
        partition = self.adapter._incremental_partition_facts(
            SimpleNamespace(
                config={
                    "partition_by": {"field": "event_date", "copy_partitions": True},
                    "partitions": ["date('2026-08-23')"],
                }
            )
        )
        source = MagicMock(identifier="source")
        target = MagicMock(identifier="target")
        source_partition = object()
        target_partition = object()
        source.incorporate.return_value = source_partition
        target.incorporate.return_value = target_partition
        result_table = SimpleNamespace(
            columns=[SimpleNamespace(values=lambda: [date(2026, 8, 23)])]
        )
        self.adapter.execute = MagicMock(return_value=(None, result_table))
        self.adapter.copy_table = MagicMock()

        assert partition is not None
        self.adapter.execute_incremental_partition_copy(source, target, partition)

        source.incorporate.assert_called_once_with(path={"identifier": "source$20260823"})
        target.incorporate.assert_called_once_with(path={"identifier": "target$20260823"})
        self.adapter.copy_table.assert_called_once_with(
            source_partition, target_partition, "table"
        )

    def test_ingestion_time_partitioning_plans_create_then_insert(self):
        model = SimpleNamespace(
            config={
                "partition_by": {
                    "field": "event_date",
                    "time_ingestion_partitioning": True,
                }
            }
        )
        self.adapter.build_table_materialization_facts = MagicMock(return_value=_table_facts())
        mutation = self.adapter.plan_incremental_mutation("merge", language="sql")

        lifecycle = self.adapter.resolve_incremental_lifecycle_plan(
            mutation,
            model,
            MagicMock(),
            MagicMock(),
            full_refresh=False,
            on_schema_change="ignore",
            staging_is_temporary=True,
            contract_enforced=False,
        )

        kinds = [operation.kind for operation in lifecycle.operations]
        create_index = kinds.index(MaterializationOperationKind.CREATE_FROM_QUERY)
        assert kinds[create_index + 1] == MaterializationOperationKind.INSERT_FROM_QUERY

    def test_ingestion_time_insert_renders_only_from_typed_partition_facts(self):
        relation = MagicMock()
        relation.__str__.return_value = "`project.dataset.target`"
        self.adapter.get_columns_in_relation = MagicMock(
            return_value=[SimpleNamespace(name="id"), SimpleNamespace(name="payload")]
        )
        partition = self.adapter._incremental_partition_facts(
            SimpleNamespace(
                config={
                    "partition_by": {
                        "field": "event_date",
                        "time_ingestion_partitioning": True,
                    }
                }
            )
        )

        assert partition is not None
        sql = self.adapter.render_incremental_insert_from_query(
            relation,
            "select 1 as id, date('2026-08-23') as event_date, 'x' as payload",
            partition,
            None,
        )

        assert "insert into `project.dataset.target` (_PARTITIONTIME, `id`, `payload`)" in sql
        assert "TIMESTAMP(`event_date`) as _PARTITIONTIME" in sql
        assert "* EXCEPT(`event_date`)" in sql
