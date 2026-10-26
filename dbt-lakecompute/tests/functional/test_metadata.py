"""`dbt compile` and `dbt show`, plus the schema and relation-listing macros underneath them.

The service does not grant CREATE_VIEW, so these build tables only.
"""

from uuid import uuid4

import pytest

from dbt.tests.util import run_dbt


model_simple_sql = """
select 1 as id, 'alice' as name
"""

model_ephemeral_sql = """
{{ config(materialized='ephemeral') }}
select 2 as id, 'bob' as name
"""

model_ref_sql = """
select * from {{ ref('model_ephemeral') }}
"""


class TestCompileAndShow:

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_simple.sql": model_simple_sql,
            "model_ephemeral.sql": model_ephemeral_sql,
            "model_ref.sql": model_ref_sql,
        }

    def test_compile(self, project):
        results = run_dbt(["compile"])
        # three models, but the ephemeral one is inlined rather than given its own result
        assert len(results) == 2
        assert {r.node.name for r in results} == {"model_simple", "model_ref"}

    def test_compile_inlines_ephemeral_ref(self, project):
        run_dbt(["compile"])
        with open(
            project.project_root / "target" / "compiled" / "test" / "models" / "model_ref.sql"
        ) as f:
            compiled = f.read()
        # the CTE is inlined, and the three-part name it selects from is unquoted
        assert "__dbt__cte__model_ephemeral" in compiled

    def test_show(self, project):
        results = run_dbt(["show", "--select", "model_simple"])
        agate_table = results.results[0].agate_table
        assert agate_table.column_names == ("id", "name")
        assert agate_table.rows[0][0] == 1
        assert agate_table.rows[0][1] == "alice"


class TestSchemaMetadata:
    @pytest.fixture(scope="class")
    def models(self):
        return {"model_simple.sql": model_simple_sql}

    @pytest.fixture
    def scratch_schema(self, project):
        """Allocate a separate namespace for each test, including concurrent test runs."""
        relation = project.adapter.Relation.create(
            database=project.database, schema=f"{project.test_schema}_{uuid4().hex}"
        )
        try:
            yield relation
        finally:
            with project.adapter.connection_named("cleanup_scratch_schema"):
                project.adapter.drop_schema(relation)

    def test_list_schemas_includes_the_test_schema(self, project):
        with project.adapter.connection_named("test"):
            schemas = project.adapter.list_schemas(project.database)
        assert project.test_schema in [s.lower() for s in schemas]

    def test_list_schemas_is_scoped_to_the_database(self, project):
        with project.adapter.connection_named("test"):
            schemas = project.adapter.list_schemas("no_such_database")
        assert list(schemas) == []

    def test_check_schema_exists(self, project):
        with project.adapter.connection_named("test"):
            assert project.adapter.check_schema_exists(project.database, project.test_schema)
            assert not project.adapter.check_schema_exists(
                project.database, "schema_that_is_not_there"
            )

    def test_check_schema_exists_does_not_treat_underscore_as_a_wildcard(self, project):
        """`ilike` would match here; `lower(x) = lower(y)` does not.

        dbt generates schema names like `test17_my_schema`, so a single-character wildcard in
        that position is a real collision, not a theoretical one.
        """
        wildcard = project.test_schema.replace("_", "x", 1)
        if wildcard == project.test_schema:  # pragma: no cover - schema always has an underscore
            pytest.skip("test schema has no underscore to substitute")
        with project.adapter.connection_named("test"):
            assert not project.adapter.check_schema_exists(project.database, wildcard)

    def test_drop_schema_never_emits_cascade(self, project, scratch_schema, monkeypatch):
        """The service rejects every `cascade` form, so the teardown has to be explicit.

        `drop schema ... cascade` fails with `DROP SCHEMA <schema_name> CASCADE is not
        supported for Iceberg schemas currently` -- even when the schema does not exist, since
        the planner rejects CASCADE before `if exists` can short-circuit. A bare
        `drop schema if exists` over a *populated* schema fails in Polaris instead.
        """
        sent = []
        original = project.adapter.connections.execute

        def recording_execute(sql, *args, **kwargs):
            sent.append(sql)
            return original(sql, *args, **kwargs)

        relation = scratch_schema
        with project.adapter.connection_named("setup"):
            project.adapter.create_schema(relation)
        project.run_sql(f"create or replace table {relation}.t as select 1 as id")

        with monkeypatch.context() as patcher:
            patcher.setattr(project.adapter.connections, "execute", recording_execute)
            with project.adapter.connection_named("test"):
                project.adapter.drop_schema(relation)

        assert sent, "no SQL was captured"
        assert not any("cascade" in sql.lower() for sql in sent)
        # the table is dropped, then the schema -- in one request
        teardown = sent[-1]
        assert teardown.index("drop table") < teardown.index("drop schema")
        with project.adapter.connection_named("check"):
            assert not project.adapter.check_schema_exists(project.database, relation.schema)

    def test_drop_schema_is_idempotent(self, project, scratch_schema):
        with project.adapter.connection_named("test"):
            project.adapter.drop_schema(scratch_schema)
            project.adapter.drop_schema(scratch_schema)

    def test_create_and_drop_schema(self, project, scratch_schema):
        relation = scratch_schema
        with project.adapter.connection_named("test"):
            project.adapter.create_schema(relation)
            assert project.adapter.check_schema_exists(project.database, relation.schema)
            # create is `if not exists`, so a second call is a no-op rather than an error
            project.adapter.create_schema(relation)

            project.adapter.drop_schema(relation)
            assert not project.adapter.check_schema_exists(project.database, relation.schema)


class TestRelationMetadata:
    @pytest.fixture(scope="class")
    def models(self):
        return {"model_simple.sql": model_simple_sql}

    def test_list_relations_reports_tables(self, project):
        project.run_sql(
            f"create or replace table {project.database}.{project.test_schema}.rel_t as select 1 as id"
        )

        schema_relation = project.adapter.Relation.create(
            database=project.database, schema=project.test_schema
        )
        with project.adapter.connection_named("test"):
            relations = project.adapter.list_relations_without_caching(schema_relation)

        by_name = {r.identifier: r for r in relations}
        assert "rel_t" in by_name
        # `BASE TABLE` is mapped in SQL to dbt's own name
        assert by_name["rel_t"].type == "table"

    def test_listed_relations_render_unquoted(self, project):
        """The reason `list_relations_without_caching` is overridden in Python.

        `SQLAdapter`'s version hard-codes an all-quoted policy, so the relation the cache hands
        a materialization would render as `"db"."schema"."name"`.
        """
        project.run_sql(
            f"create or replace table {project.database}.{project.test_schema}.rel_q as select 1 as id"
        )
        schema_relation = project.adapter.Relation.create(
            database=project.database, schema=project.test_schema
        )
        with project.adapter.connection_named("test"):
            relations = project.adapter.list_relations_without_caching(schema_relation)

        rendered = [r.render() for r in relations if r.identifier == "rel_q"]
        assert rendered == [f"{project.database}.{project.test_schema}.rel_q"]
        assert '"' not in rendered[0]

    def test_get_relation_resolves_a_relation(self, project):
        project.run_sql(
            f"create or replace table {project.database}.{project.test_schema}.rel_c "
            "as select 1 as id"
        )
        with project.adapter.connection_named("test"):
            found = project.adapter.get_relation(
                database=project.database, schema=project.test_schema, identifier="rel_c"
            )
        assert found is not None
        assert found.render() == f"{project.database}.{project.test_schema}.rel_c"


class TestRelationNameMatching:
    """Identifier casing, on a class of its own so the relation cache starts empty."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"model_simple.sql": model_simple_sql}

    def test_get_relation_matches_case_insensitively(self, project):
        """With quoting off, `_make_match_kwargs` lowercases database, schema and identifier
        before matching. The concern recorded in `relation.py` was that a service which
        preserves case would then never match. It does not apply: `_CachedRelation` lowercases
        both sides of every cache key too (dbt-adapters' `cache.py`), so a mixed-case relation
        is found by its lowercase name and keeps its original casing when rendered.
        """
        project.run_sql(
            f'create or replace table {project.database}.{project.test_schema}."MixedCase" '
            "as select 1 as id"
        )
        with project.adapter.connection_named("test"):
            found = project.adapter.get_relation(
                database=project.database, schema=project.test_schema, identifier="mixedcase"
            )
        assert found is not None
        assert found.identifier == "MixedCase"
