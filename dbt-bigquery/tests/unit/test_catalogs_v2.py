"""Unit tests for BigQueryAdapter bridge_v2_catalog hook methods."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from dbt.adapters.base import BaseAdapter
from dbt.adapters.bigquery.impl import BigQueryAdapter
from dbt.adapters.bigquery import constants
from dbt.adapters.planning import (
    CatalogBindingState,
    CatalogFacts,
    CreateFromQueryFacts,
    ExistingRelationFacts,
    FormatFacts,
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
