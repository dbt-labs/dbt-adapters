from dbt.adapters.planning import (
    MaterializationHookStrategy,
    MaterializationTransactionMode,
    MaterializationTransactionStrategy,
    TableReplacementStrategy,
)
from dbt.adapters.snowflake.impl import SnowflakeAdapter
from dbt.adapters.snowflake import constants
from dbt.adapters.snowflake.relation import SnowflakeRelation


def _adapter() -> SnowflakeAdapter:
    return object.__new__(SnowflakeAdapter)


def test_snowflake_sql_table_resolves_to_direct_replace() -> None:
    plan = SnowflakeAdapter.plan_table_materialization(
        _adapter(),
        "macro.dbt_snowflake.materialization_table_snowflake",
        "sql",
    )

    assert plan.replacement == TableReplacementStrategy.DIRECT_REPLACE
    assert plan.transaction == MaterializationTransactionStrategy.ADAPTER_MANAGED
    assert plan.hooks == MaterializationHookStrategy.IN_TRANSACTION
    assert plan.setup_macro == "set_query_tag"
    assert plan.teardown_macro == "unset_query_tag"


def test_snowflake_resolver_preserves_default_and_python_fallbacks() -> None:
    adapter = _adapter()

    default_plan = SnowflakeAdapter.plan_table_materialization(
        adapter,
        "macro.dbt.materialization_table_default",
        "sql",
    )
    python_plan = SnowflakeAdapter.plan_table_materialization(
        adapter,
        "macro.dbt_snowflake.materialization_table_snowflake",
        "python",
    )

    assert default_plan.replacement == TableReplacementStrategy.STAGE_AND_SWAP
    assert python_plan is None


def test_snowflake_target_relation_uses_resolved_catalog_format() -> None:
    adapter = _adapter()
    relation = SnowflakeRelation.create(
        database="analytics",
        schema="mart",
        identifier="orders",
    )
    adapter.build_catalog_relation = lambda model: type(
        "CatalogRelation", (), {"table_format": constants.ICEBERG_TABLE_FORMAT}
    )()

    target = SnowflakeAdapter.resolve_table_materialization_relation(adapter, object(), relation)

    assert target.type == "table"
    assert target.table_format == constants.ICEBERG_TABLE_FORMAT


def test_snowflake_glue_catalog_provider_is_explicit_in_runtime_facts() -> None:
    adapter = _adapter()
    catalog_relation = type(
        "CatalogRelation",
        (),
        {
            "catalog_type": constants.ICEBERG_REST_CATALOG_TYPE,
            "table_format": constants.ICEBERG_TABLE_FORMAT,
            "catalog_linked_database_type": "glue",
        },
    )()
    adapter.build_catalog_relation = lambda model: catalog_relation

    provider = adapter.get_create_from_query_catalog_provider(catalog_relation, object())
    facts = adapter.get_table_materialization_execution_facts(
        object(),
        SnowflakeRelation.create(
            database="analytics",
            schema="mart",
            identifier="orders",
        ),
    )

    assert provider == "glue"
    assert facts.transaction_mode == MaterializationTransactionMode.NONE
    assert facts.capabilities == (
        "create_or_replace",
        "query_tag_envelope",
        "iceberg",
        "glue_linked_catalog",
    )


def test_snowflake_glue_catalog_provider_is_retained_in_incremental_plan() -> None:
    adapter = _adapter()
    catalog_relation = type(
        "CatalogRelation",
        (),
        {
            "catalog_type": constants.ICEBERG_REST_CATALOG_TYPE,
            "catalog_name": "aws_glue_catalog",
            "catalog_database": "analytics",
            "catalog_linked_database_type": "glue",
            "table_format": constants.ICEBERG_TABLE_FORMAT,
            "file_format": constants.ICEBERG_TABLE_FORMAT,
            "external_volume": None,
        },
    )()

    facts = SnowflakeAdapter.build_incremental_mutation_facts(
        adapter,
        requested_strategy="merge",
        language="sql",
        unique_key="id",
        requested_temp_relation_type=None,
        catalog_relation=catalog_relation,
    )

    assert facts.catalog.integration_name == "aws_glue_catalog"
    assert facts.catalog.catalog_provider == "glue"
    assert facts.catalog.catalog_database == "analytics"
    assert facts.format.table_provider == constants.ICEBERG_TABLE_FORMAT.casefold()
