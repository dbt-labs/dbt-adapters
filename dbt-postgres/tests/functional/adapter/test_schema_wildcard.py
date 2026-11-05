import pytest


class TestPostgresSchemaWildcard:
    @pytest.fixture(scope="class")
    def models(self):
        return {"model.sql": "select 1 as id"}

    def test_underscore_is_not_wildcard(self, project):
        adapter = project.adapter
        schema = project.test_schema + "_data"
        other_schema = project.test_schema + "Xdata"

        with adapter.connection_named("schema_wildcard_test"):
            adapter.execute(
                f'create schema if not exists "{schema}"',
                auto_begin=True,
            )
            adapter.execute(
                f'create schema if not exists "{other_schema}"',
                auto_begin=True,
            )
            adapter.execute(
                f'create table if not exists "{schema}".test_item (id int)',
                auto_begin=True,
            )
            adapter.execute(
                f'create table if not exists "{other_schema}".test_item (id int)',
                auto_begin=True,
            )
            adapter.connections.get_thread_connection().handle.commit()

            relation = adapter.Relation.create(
                database=project.database,
                schema=schema,
            )
            found = adapter.list_relations_without_caching(relation)

            assert all(r.schema.lower() == schema.lower() for r in found)
            assert any(r.identifier == "test_item" for r in found)
