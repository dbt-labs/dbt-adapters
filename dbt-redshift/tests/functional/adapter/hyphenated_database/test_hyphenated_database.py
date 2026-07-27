"""Functional tests verifying that SHOW APIs handle hyphenated database names.

Redshift's SHOW TABLES / SHOW COLUMNS / SHOW SCHEMAS / SHOW GRANTS commands
require quoted identifiers when the database or schema name contains a hyphen.
These tests exercise those code paths end-to-end against a real Redshift
database whose name contains a hyphen (supplied via REDSHIFT_TEST_DBNAME_W_HYPHEN).

Run only when REDSHIFT_TEST_DBNAME_W_HYPHEN is set (see conftest.py).
"""

from dbt.adapters.contracts.relation import RelationType
from dbt.adapters.redshift.impl import CATALOG_COLUMNS
from dbt.tests.util import get_connection, run_dbt
import pytest

from tests.functional.adapter.hyphenated_database.fixtures import (
    REDSHIFT_TEST_DBNAME_W_HYPHEN,
    HyphenatedDatabaseMixin,
)


_SIMPLE_TABLE = """
{{ config(materialized='table') }}
select 1 as id, 'hello' as label
union all select 2, 'world'
"""


class TestHyphenatedDatabaseRun(HyphenatedDatabaseMixin):
    """dbt run succeeds against a database whose name contains a hyphen."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "simple_table.sql": _SIMPLE_TABLE,
        }

    @pytest.fixture(scope="class")
    def dbt_run_results(self, project):
        """Run dbt once per worker/class; other tests in this class use it as a prerequisite."""
        return run_dbt(["run"])

    def test_run(self, dbt_run_results):
        assert len(dbt_run_results) == 1

    @pytest.mark.usefixtures("dbt_run_results")
    def test_list_relations_uses_show_api(self, project):
        """list_relations_without_caching (SHOW TABLES) works with a hyphenated database."""
        with project.adapter.connection_named("_test"):
            schema_relation = project.adapter.Relation.create(
                database=REDSHIFT_TEST_DBNAME_W_HYPHEN,
                schema=project.test_schema,
            )
            relations = project.adapter.list_relations_without_caching(schema_relation)

        identifiers = {rel.identifier for rel in relations}
        assert "simple_table" in identifiers

    @pytest.mark.usefixtures("dbt_run_results")
    def test_check_schema_exists(self, project):
        """check_schema_exists (SHOW SCHEMAS) works with a hyphenated database."""
        with project.adapter.connection_named("_test"):
            exists = project.adapter.check_schema_exists(
                REDSHIFT_TEST_DBNAME_W_HYPHEN, project.test_schema
            )
        assert exists

    @pytest.mark.usefixtures("dbt_run_results")
    def test_list_schemas(self, project):
        """list_schemas (SHOW SCHEMAS) works with a hyphenated database."""
        with project.adapter.connection_named("_test"):
            schemas = project.adapter.list_schemas(REDSHIFT_TEST_DBNAME_W_HYPHEN)
        assert project.test_schema.lower() in [s.lower() for s in schemas]


class TestHyphenatedDatabaseCatalog(HyphenatedDatabaseMixin):
    """Catalog introspection (SHOW TABLES / SHOW COLUMNS) works with a hyphenated database."""

    @pytest.fixture(scope="class")
    def hyphen_db_schema(self, project, adapter):
        return adapter.Relation.create(
            database=REDSHIFT_TEST_DBNAME_W_HYPHEN,
            schema=project.test_schema,
            identifier="",
        )

    @pytest.fixture(scope="class")
    def hyphen_db_table(self, adapter, hyphen_db_schema):
        relation = adapter.Relation.create(
            database=hyphen_db_schema.database,
            schema=hyphen_db_schema.schema,
            identifier="hyphen_db_catalog_table",
            type=RelationType.Table,
        )
        with get_connection(adapter):
            adapter.execute(
                f"create schema if not exists "
                f'"{hyphen_db_schema.database}".{hyphen_db_schema.schema}'
            )
            adapter.execute(
                f'create table "{relation.database}".{relation.schema}.{relation.identifier} '
                f"(id integer, name varchar(256), value numeric(10,2))"
            )
        yield relation
        with get_connection(adapter):
            adapter.execute(
                f"drop table if exists "
                f'"{relation.database}".{relation.schema}.{relation.identifier}'
            )

    @pytest.fixture(scope="class")
    def hyphen_db_information_schema(self, adapter, hyphen_db_schema):
        return adapter.Relation.create(
            database=hyphen_db_schema.database,
            schema=hyphen_db_schema.schema,
            identifier="INFORMATION_SCHEMA",
        ).information_schema()

    def test_get_columns_in_relation(self, adapter, hyphen_db_table):
        """get_columns_in_relation (SHOW COLUMNS) works with a hyphenated database."""
        with get_connection(adapter):
            columns = adapter.get_columns_in_relation(hyphen_db_table)
        column_names = {col.name for col in columns}
        assert column_names == {"id", "name", "value"}

    def test_catalog_by_relations(
        self, adapter, hyphen_db_schema, hyphen_db_table, hyphen_db_information_schema
    ):
        """_get_one_catalog_by_relations (SHOW COLUMNS) works with a hyphenated database."""
        used_schemas = frozenset({(hyphen_db_schema.database, hyphen_db_schema.schema)})
        with get_connection(adapter):
            catalog = adapter._get_one_catalog_by_relations(
                information_schema=hyphen_db_information_schema,
                relations=[hyphen_db_table],
                used_schemas=used_schemas,
            )
        # 1 table × 3 columns = 3 rows
        assert len(catalog) == 3
        for col in CATALOG_COLUMNS:
            assert col in catalog.column_names

    @pytest.mark.usefixtures("hyphen_db_table")
    def test_catalog_by_schemas(self, adapter, hyphen_db_schema, hyphen_db_information_schema):
        """_get_one_catalog (SHOW TABLES) works with a hyphenated database."""
        used_schemas = frozenset({(hyphen_db_schema.database, hyphen_db_schema.schema)})
        with get_connection(adapter):
            catalog = adapter._get_one_catalog(
                information_schema=hyphen_db_information_schema,
                schemas={hyphen_db_schema.schema},
                used_schemas=used_schemas,
            )
        # 1 table × 3 columns = 3 rows
        assert len(catalog) == 3
        for col in CATALOG_COLUMNS:
            assert col in catalog.column_names
