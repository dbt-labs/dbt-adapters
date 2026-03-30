from dbt.adapters.contracts.relation import RelationType
from dbt.adapters.redshift.impl import CATALOG_COLUMNS
from dbt.tests.util import get_connection
import pytest

from tests.functional.adapter.cross_database.conftest import (
    REDSHIFT_TEST_CROSS_DBNAME,
    CrossDatabaseMixin,
    skip_if_no_cross_db,
)


@skip_if_no_cross_db
class TestCrossDatabaseCatalog(CrossDatabaseMixin):
    """Test that catalog correctly introspects cross-database relations."""

    @pytest.fixture(scope="class")
    def cross_db_schema(self, project, adapter):
        return adapter.Relation.create(
            database=REDSHIFT_TEST_CROSS_DBNAME,
            schema=project.test_schema,
            identifier="",
        )

    @pytest.fixture(scope="class")
    def cross_db_table(self, adapter, cross_db_schema):
        relation = adapter.Relation.create(
            database=cross_db_schema.database,
            schema=cross_db_schema.schema,
            identifier="cross_db_catalog_table",
            type=RelationType.Table,
        )
        with get_connection(adapter):
            adapter.execute(
                f"create schema if not exists "
                f"{cross_db_schema.database}.{cross_db_schema.schema}"
            )
            adapter.execute(
                f"create table {relation.database}.{relation.schema}.{relation.identifier} "
                f"(id integer, name varchar(256), price numeric(10,2))"
            )
        yield relation
        with get_connection(adapter):
            adapter.execute(
                f"drop table if exists "
                f"{relation.database}.{relation.schema}.{relation.identifier}"
            )

    @pytest.fixture(scope="class")
    def cross_db_information_schema(self, adapter, cross_db_schema):
        return adapter.Relation.create(
            database=cross_db_schema.database,
            schema=cross_db_schema.schema,
            identifier="INFORMATION_SCHEMA",
        ).information_schema()

    def test_catalog_by_relations(
        self, adapter, cross_db_schema, cross_db_table, cross_db_information_schema
    ):
        used_schemas = frozenset({(cross_db_schema.database, cross_db_schema.schema)})
        with get_connection(adapter):
            catalog = adapter._get_one_catalog_by_relations(
                information_schema=cross_db_information_schema,
                relations=[cross_db_table],
                used_schemas=used_schemas,
            )
        # 1 table × 3 columns = 3 rows
        assert len(catalog) == 3
        for col in CATALOG_COLUMNS:
            assert col in catalog.column_names

    @pytest.mark.usefixtures("cross_db_table")
    def test_catalog_by_schemas(self, adapter, cross_db_schema, cross_db_information_schema):
        used_schemas = frozenset({(cross_db_schema.database, cross_db_schema.schema)})
        with get_connection(adapter):
            catalog = adapter._get_one_catalog(
                information_schema=cross_db_information_schema,
                schemas={cross_db_schema.schema},
                used_schemas=used_schemas,
            )
        # 1 table × 3 columns = 3 rows
        assert len(catalog) == 3
        for col in CATALOG_COLUMNS:
            assert col in catalog.column_names
