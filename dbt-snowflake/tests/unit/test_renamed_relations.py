from dbt.adapters.snowflake.relation import SnowflakeRelation
from dbt.adapters.snowflake.relation_configs import SnowflakeRelationType


def test_renameable_relation():
    relation = SnowflakeRelation.create(
        database="my_db",
        schema="my_schema",
        identifier="my_table",
        type=SnowflakeRelationType.Table,
    )
    assert relation.renameable_relations == frozenset(
        {
            SnowflakeRelationType.Table,
            SnowflakeRelationType.View,
            SnowflakeRelationType.DynamicTable,
            SnowflakeRelationType.InteractiveTable,
        }
    )


def test_interactive_table_is_renameable_and_replaceable():
    from dbt.adapters.snowflake.relation import SnowflakeRelation
    from dbt.adapters.snowflake.relation_configs.policies import SnowflakeRelationType

    assert SnowflakeRelationType.InteractiveTable == "interactive_table"

    # renameable_relations/replaceable_relations are dataclass fields with
    # default_factory -- accessing them on the CLASS yields a dataclasses.Field,
    # not a frozenset. They must be read off an INSTANCE.
    relation = SnowflakeRelation.create(
        database="db",
        schema="sch",
        identifier="tbl",
        type=SnowflakeRelationType.InteractiveTable,
    )
    assert SnowflakeRelationType.InteractiveTable in relation.renameable_relations
    assert SnowflakeRelationType.InteractiveTable in relation.replaceable_relations
    assert relation.can_be_renamed is True


def test_interactive_table_type_predicates_do_not_overlap_dynamic_table():
    from dbt.adapters.snowflake.relation import SnowflakeRelation
    from dbt.adapters.snowflake.relation_configs.policies import SnowflakeRelationType

    relation = SnowflakeRelation.create(
        database="db",
        schema="sch",
        identifier="tbl",
        type=SnowflakeRelationType.InteractiveTable,
    )
    assert relation.is_interactive_table is True
    assert relation.is_dynamic_table is False
    # is_materialized_view is dynamic_table's dispatch alias; interactive_table must not reuse it.
    assert relation.is_materialized_view is False
    assert relation.is_table is False
