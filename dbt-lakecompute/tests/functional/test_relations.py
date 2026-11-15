"""Relation and column DDL: drops, truncation, `describe`, and the refusals for what the
service cannot do (`comment on`, grants)."""

import pytest


model_simple_sql = """
select 1 as id, 'alice' as name
"""


class TestRelationDdl:
    @pytest.fixture(scope="class")
    def models(self):
        return {"model_simple.sql": model_simple_sql}

    def test_drop_relation_emits_no_cascade(self, project):
        project.run_sql(
            f"create or replace table {project.database}.{project.test_schema}.rel_d as select 1 as id"
        )
        relation = project.adapter.Relation.create(
            database=project.database,
            schema=project.test_schema,
            identifier="rel_d",
            type="table",
        )
        with project.adapter.connection_named("test"):
            sql = project.adapter.execute_macro("get_drop_sql", kwargs={"relation": relation})
        assert (
            sql.strip() == f"drop table if exists {project.database}.{project.test_schema}.rel_d"
        )

        with project.adapter.connection_named("test"):
            project.adapter.drop_relation(relation)
        remaining = project.run_sql(
            "select count(*) from system.information_schema.tables "
            f"where lower(table_schema) = lower('{project.test_schema}') "
            "and lower(table_name) = 'rel_d'",
            fetch="all",
        )
        assert remaining[0][0] == 0

    def test_drop_view_also_emits_no_cascade(self, project):
        relation = project.adapter.Relation.create(
            database=project.database,
            schema=project.test_schema,
            identifier="rel_dv",
            type="view",
        )
        with project.adapter.connection_named("test"):
            sql = project.adapter.execute_macro("get_drop_sql", kwargs={"relation": relation})
        assert (
            sql.strip() == f"drop view if exists {project.database}.{project.test_schema}.rel_dv"
        )

    def test_truncate_relation(self, project):
        project.run_sql(
            f"create or replace table {project.database}.{project.test_schema}.rel_tr as select 1 as id"
        )
        relation = project.adapter.Relation.create(
            database=project.database,
            schema=project.test_schema,
            identifier="rel_tr",
            type="table",
        )
        with project.adapter.connection_named("test"):
            project.adapter.truncate_relation(relation)
        rows = project.run_sql(
            f"select count(*) from {project.database}.{project.test_schema}.rel_tr", fetch="all"
        )
        assert rows[0][0] == 0


class TestColumnMetadata:
    @pytest.fixture(scope="class")
    def models(self):
        return {"model_simple.sql": model_simple_sql}

    def test_get_columns_in_relation_uses_describe(self, project):
        project.run_sql(
            f"create or replace table {project.database}.{project.test_schema}.col_t as "
            "select 1::bigint as c_int, 'x' as c_str, 1.25::decimal(38,2) as c_dec, "
            "current_date as c_date"
        )
        relation = project.adapter.Relation.create(
            database=project.database, schema=project.test_schema, identifier="col_t", type="table"
        )
        with project.adapter.connection_named("test"):
            columns = project.adapter.get_columns_in_relation(relation)

        assert [c.name for c in columns] == ["c_int", "c_str", "c_dec", "c_date"]
        assert [c.data_type for c in columns] == [
            "BIGINT",
            "VARCHAR",
            "DECIMAL(38,2)",
            "DATE",
        ]

    def test_get_columns_in_relation_handles_nested_types(self, project):
        project.run_sql(
            f"create or replace table {project.database}.{project.test_schema}.col_nested as "
            "select {{'a': 1}} as c_struct, [1, 2] as c_list"
        )
        relation = project.adapter.Relation.create(
            database=project.database,
            schema=project.test_schema,
            identifier="col_nested",
            type="table",
        )
        with project.adapter.connection_named("test"):
            columns = project.adapter.get_columns_in_relation(relation)

        by_name = {c.name: c.data_type for c in columns}
        assert by_name["c_struct"].startswith("STRUCT")
        assert by_name["c_list"].endswith("[]")

    @pytest.mark.parametrize("schema_suffix", ["", "_missing"])
    def test_missing_relation_has_no_columns(self, project, schema_suffix):
        relation = project.adapter.Relation.create(
            database=project.database,
            schema=project.test_schema + schema_suffix,
            identifier="col_never_created",
            type="table",
        )
        with project.adapter.connection_named("test"):
            assert project.adapter.get_columns_in_relation(relation) == []


class TestPersistDocsIsIgnored:
    """As in v2: the service rejects `comment on`, so nothing is emitted."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"model_simple.sql": model_simple_sql}

    def test_comment_macros_render_nothing(self, project):
        relation = project.adapter.Relation.create(
            database=project.database, schema=project.test_schema, identifier="x", type="table"
        )
        with project.adapter.connection_named("test"):
            for macro, kwargs in (
                ("alter_relation_comment", {"relation_comment": "hi"}),
                ("alter_column_comment", {"column_dict": {"id": {"description": "hi"}}}),
            ):
                rendered = project.adapter.execute_macro(
                    macro, kwargs={"relation": relation, **kwargs}
                )
                assert rendered.strip() == ""


class TestTimestamps:
    @pytest.fixture(scope="class")
    def models(self):
        return {"model_simple.sql": model_simple_sql}

    def test_current_timestamp_is_now(self, project):
        with project.adapter.connection_named("test"):
            rendered = project.adapter.execute_macro("current_timestamp").strip()
        assert rendered == "now()"
        assert project.run_sql(f"select {rendered}", fetch="all")[0][0] is not None


class TestGrantsAreIgnored:
    @pytest.fixture(scope="class")
    def models(self):
        return {"model_simple.sql": model_simple_sql}

    def test_no_grant_config_is_a_silent_no_op(self, project):
        relation = project.adapter.Relation.create(
            database=project.database, schema=project.test_schema, identifier="x", type="table"
        )
        with project.adapter.connection_named("test"):
            for grant_config in ({}, None):
                rendered = project.adapter.execute_macro(
                    "apply_grants",
                    kwargs={"relation": relation, "grant_config": grant_config},
                )
                assert rendered.strip() == ""

    def test_a_grant_config_is_a_silent_no_op(self, project):
        relation = project.adapter.Relation.create(
            database=project.database, schema=project.test_schema, identifier="x", type="table"
        )
        with project.adapter.connection_named("test"):
            rendered = project.adapter.execute_macro(
                "apply_grants",
                kwargs={"relation": relation, "grant_config": {"select": ["someone"]}},
            )
        assert rendered.strip() == ""

    def test_grants_are_not_copied(self, project):
        with project.adapter.connection_named("test"):
            assert project.adapter.execute_macro("copy_grants") is False
