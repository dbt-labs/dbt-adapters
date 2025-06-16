from dbt.adapters.contracts.relation import RelationType
from dbt.tests.util import get_connection
import pytest


class TestGetCatalog:
    @pytest.fixture(scope="class")
    def my_schema(self, project, adapter):
        schema = adapter.Relation.create(
            database=project.database,
            schema=project.test_schema,
            identifier="",
        )
        yield schema

    @pytest.fixture(scope="class")
    def my_seed(self, adapter, my_schema):
        relation = adapter.Relation.create(
            database=my_schema.database,
            schema=my_schema.schema,
            identifier="my_seed",
            type=RelationType.Table,
        )
        with get_connection(adapter):
            sql = f"""
            create table {relation.database}.{relation.schema}.{relation.identifier} (
                id integer,
                value integer,
                record_valid_date timestamp
            );
            insert into {relation.database}.{relation.schema}.{relation.identifier}
             (id, value, record_valid_date) values
                (1,100,'2023-01-01 00:00:00'),
                (2,200,'2023-01-02 00:00:00'),
                (3,300,'2023-01-02 00:00:00')
            ;
            """
            adapter.execute(sql)
        yield relation

    @pytest.fixture(scope="class")
    def my_table(self, adapter, my_schema, my_seed):
        relation = adapter.Relation.create(
            database=my_schema.database,
            schema=my_schema.schema,
            identifier="my_table",
            type=RelationType.Table,
        )
        with get_connection(adapter):
            sql = f"""
            create table {relation.database}.{relation.schema}.{relation.identifier} as
            select * from {my_seed.database}.{my_seed.schema}.{my_seed.identifier}
            ;
            """
            adapter.execute(sql)
        yield relation

    @pytest.fixture(scope="class")
    def my_view(self, adapter, my_schema, my_seed):
        relation = adapter.Relation.create(
            database=my_schema.database,
            schema=my_schema.schema,
            identifier="my_view",
            type=RelationType.View,
        )
        with get_connection(adapter):
            sql = f"""
            create view {relation.database}.{relation.schema}.{relation.identifier} as
            select * from {my_seed.database}.{my_seed.schema}.{my_seed.identifier}
            ;
            """
            adapter.execute(sql)
        yield relation

    @pytest.fixture(scope="class")
    def my_materialized_view(self, adapter, my_schema, my_seed):
        relation = adapter.Relation.create(
            database=my_schema.database,
            schema=my_schema.schema,
            identifier="my_materialized_view",
            type=RelationType.MaterializedView,
        )
        with get_connection(adapter):
            sql = f"""
            create materialized view {relation.database}.{relation.schema}.{relation.identifier} as
            select * from {my_seed.database}.{my_seed.schema}.{my_seed.identifier}
            ;
            """
            adapter.execute(sql)
        yield relation

    @pytest.fixture(scope="class")
    def my_information_schema(self, adapter, my_schema):
        yield adapter.Relation.create(
            database=my_schema.database,
            schema=my_schema.schema,
            identifier="INFORMATION_SCHEMA",
        ).information_schema()

    def test_get_one_catalog_by_relations(
        self,
        adapter,
        my_schema,
        my_seed,
        my_table,
        my_view,
        my_materialized_view,
        my_information_schema,
    ):
        my_schemas = frozenset({(my_schema.database, my_schema.schema)})
        my_relations = [my_seed, my_table, my_view, my_materialized_view]
        with get_connection(adapter):
            catalog = adapter._get_one_catalog_by_relations(
                information_schema=my_information_schema,
                relations=my_relations,
                used_schemas=my_schemas,
            )
        # my_seed, my_table, my_view, my_materialized_view each have 3 cols = 12 cols
        # my_materialized_view creates an underlying table with 2 additional = 5 cols
        # note the underlying table is missing as it's not in `my_relations`
        assert len(catalog) == 12

    def test_get_one_catalog_by_schemas(
        self,
        adapter,
        my_schema,
        my_seed,
        my_table,
        my_view,
        my_materialized_view,
        my_information_schema,
    ):
        my_schemas = frozenset({(my_schema.database, my_schema.schema)})
        with get_connection(adapter):
            catalog = adapter._get_one_catalog(
                information_schema=my_information_schema,
                schemas={my_schema.schema},
                used_schemas=my_schemas,
            )
        # my_seed, my_table, my_view, my_materialized_view each have 3 cols = 12 cols
        # my_materialized_view creates an underlying table with 2 additional = 5 cols
        assert len(catalog) == 17
