"""`dbt run` builds tables, and refuses what it cannot build."""

import pytest

from dbt.tests.util import run_dbt, run_dbt_and_capture, write_file


model_table_sql = """
{{ config(materialized='table') }}
select 1 as id, 'alice' as name
union all
select 2 as id, 'bob' as name
"""

model_ephemeral_sql = """
{{ config(materialized='ephemeral') }}
select 3 as id, 'carol' as name
"""

model_from_ephemeral_sql = """
{{ config(materialized='table') }}
select * from {{ ref('model_ephemeral') }}
"""


class TestTableMaterialization:
    @pytest.fixture(scope="class")
    def models(self):
        return {"model_table.sql": model_table_sql}

    def test_build_and_rebuild(self, project, captured_sql):
        results = run_dbt(["run"])
        assert len(results) == 1

        main = [s for s in captured_sql if "create or replace table" in s]
        assert len(main) == 1
        assert (
            main[0]
            .strip()
            .startswith(
                f"create or replace table {project.database}.{project.test_schema}.model_table as ("
            )
        )
        # `create or replace`, so no intermediate relation and nothing renamed
        assert not any("__dbt_tmp" in s or "__dbt_backup" in s for s in captured_sql)
        assert not any("rename" in s.lower() for s in captured_sql)

        rows = project.run_sql(
            f"select count(*) from {project.database}.{project.test_schema}.model_table",
            fetch="one",
        )
        assert rows[0] == 2

        # a second run replaces rather than appends or errors
        del captured_sql[:]
        assert len(run_dbt(["run"])) == 1
        assert sum("create or replace table" in s for s in captured_sql) == 1
        rows = project.run_sql(
            f"select count(*) from {project.database}.{project.test_schema}.model_table",
            fetch="one",
        )
        assert rows[0] == 2

    def test_statement_has_no_trailing_semicolon(self, project, captured_sql):
        run_dbt(["run"])
        main = [s for s in captured_sql if "create or replace table" in s]
        assert main and not main[0].strip().endswith(";")


class TestEphemeralIsInlined:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_ephemeral.sql": model_ephemeral_sql,
            "model_from_ephemeral.sql": model_from_ephemeral_sql,
        }

    def test_run(self, project, captured_sql):
        results = run_dbt(["run"])
        assert len(results) == 1

        main = [s for s in captured_sql if "create or replace table" in s]
        assert len(main) == 1
        assert f"{project.test_schema}.model_from_ephemeral as (" in main[0]
        assert "__dbt__cte__model_ephemeral as (" in main[0]
        assert f"{project.test_schema}.model_ephemeral" not in main[0]

        rows = project.run_sql(
            f"select name from {project.database}.{project.test_schema}.model_from_ephemeral",
            fetch="one",
        )
        assert rows[0] == "carol"


class TestUnsupportedMaterializations:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "inc.sql": "{{ config(materialized='incremental') }}\nselect 1 as id",
            "v.sql": "{{ config(materialized='view') }}\nselect 1 as id",
            "mv.sql": "{{ config(materialized='materialized_view') }}\nselect 1 as id",
            "fn.sql": "{{ config(materialized='function') }}\nselect 1 as id",
        }

    @pytest.mark.parametrize(
        "model,expected",
        [
            ("inc", "'incremental' materialization"),
            ("v", "'view' materialization"),
            ("mv", "'materialized_view' materialization"),
            ("fn", "'function' materialization"),
        ],
    )
    def test_refused(self, project, model, expected):
        _, log = run_dbt_and_capture(["run", "--select", model], expect_pass=False)
        assert expected in log
        assert f"model.test.{model}" in log


class TestSeedsAreRefused:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"s.csv": "id,name\n1,a\n"}

    def test_refused_before_anything_is_created(self, project):
        _, log = run_dbt_and_capture(["seed"], expect_pass=False)
        assert "'seed' materialization" in log
        assert "seed.test.s" in log
        relation = project.adapter.Relation.create(
            database=project.database, schema=project.test_schema, identifier="s"
        )
        with project.adapter.connection_named("check"):
            assert project.adapter.get_relation(**relation.path.to_dict()) is None


class TestPythonModelsAreRefused:
    """Rejected by dbt-core: no materialization here declares `python`."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "py_model.py": (
                "def model(dbt, session):\n"
                "    dbt.config(materialized='table')\n"
                "    return None\n"
            )
        }

    def test_refused(self, project):
        results = run_dbt(["run"], expect_pass=False)
        assert len(results) == 1
        assert "python" in str(results[0].message).lower()


class TestTemporaryTablesAreRefused:
    @pytest.fixture(scope="class")
    def models(self):
        return {"model_table.sql": model_table_sql}

    def test_create_table_as_temporary_raises(self, project):
        from dbt_common.exceptions import CompilationError

        relation = project.adapter.Relation.create(
            database=project.database, schema=project.test_schema, identifier="tmp_probe"
        )
        with project.adapter.connection_named("test"):
            with pytest.raises(CompilationError, match="no temporary tables"):
                project.adapter.execute_macro(
                    "lakecompute__create_table_as",
                    kwargs={
                        "temporary": True,
                        "relation": relation,
                        "compiled_code": "select 1 as id",
                    },
                )


contract_model_sql = """
{{ config(materialized='table') }}
select 1 as id, 'alice' as name
"""

contract_schema_yml = """
version: 2
models:
  - name: contract_model
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: integer
      - name: name
        data_type: varchar
"""

contract_schema_wrong_yml = contract_schema_yml.replace("data_type: varchar", "data_type: date")


class TestModelContracts:
    """Column names and types are enforced; constraints are not (see CONSTRAINT_SUPPORT)."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"contract_model.sql": contract_model_sql, "schema.yml": contract_schema_yml}

    def test_matching_contract_builds(self, project, captured_sql):
        assert len(run_dbt(["run"])) == 1
        main = [s for s in captured_sql if "create or replace table" in s]
        assert len(main) == 1
        assert "model_subq" in main[0]

    def test_mismatched_contract_fails(self, project):
        write_file(contract_schema_wrong_yml, project.project_root, "models", "schema.yml")
        _, log = run_dbt_and_capture(["run"], expect_pass=False)
        assert "contract" in log.lower()


class TestNestedModelContracts:
    @pytest.fixture(scope="class")
    def models(self):
        return {"nested_contract.sql": "select [1, 2] as value"}

    @pytest.mark.parametrize(
        "expression,declared_type,matching",
        [
            ("[1, 2]", "integer[]", True),
            ("[1, 2]", "varchar[]", False),
            ("{'a': 1}", "struct(a integer)", True),
            ("{'a': 1}", "struct(b integer)", False),
            ("{'a': 1}", "struct(a date)", False),
            ("map(['a'], [1])", "map(varchar, integer)", True),
            ("map(['a'], [1])", "map(varchar, date)", False),
            ("[{'a': [1, 2]}]", "struct(a varchar[])[]", False),
        ],
    )
    def test_nested_schema_is_enforced(
        self, project, captured_sql, expression, declared_type, matching
    ):
        write_file(
            f"select {expression} as value", project.project_root, "models", "nested_contract.sql"
        )
        write_file(
            f"""
version: 2
models:
  - name: nested_contract
    config:
      materialized: table
      contract:
        enforced: true
    columns:
      - name: value
        data_type: {declared_type}
""",
            project.project_root,
            "models",
            "schema.yml",
        )
        _, log = run_dbt_and_capture(["run"], expect_pass=matching)
        creates = [s for s in captured_sql if "create or replace table" in s]
        if matching:
            assert len(creates) == 1
        else:
            assert "This model has an enforced contract that failed" in log
            assert "data type mismatch" in log
            assert creates == []


contract_with_constraint_schema_yml = contract_schema_yml.replace(
    "data_type: integer", "data_type: integer\n        constraints:\n          - type: not_null"
)


class TestUnsupportedConstraintsWarn:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "contract_model.sql": contract_model_sql,
            "schema.yml": contract_with_constraint_schema_yml,
        }

    def test_warns_and_builds_without_the_constraint(self, project, captured_sql):
        _, log = run_dbt_and_capture(["run"])
        assert "The constraint type not_null is not supported by lakecompute" in " ".join(
            log.split()
        )
        main = [s for s in captured_sql if "create or replace table" in s]
        assert len(main) == 1
        assert "not null" not in main[0].lower()


persist_docs_schema_yml = """
version: 2
models:
  - name: model_table
    description: a table
    config:
      persist_docs:
        relation: true
        columns: true
      grants:
        select: ["someone"]
    columns:
      - name: id
        description: an id
"""


class TestPersistDocsAndGrantsAreIgnored:
    """As in v2: the table builds, with no `comment on`, no grant and no warning."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"model_table.sql": model_table_sql, "schema.yml": persist_docs_schema_yml}

    def test_builds_without_comments_or_grants(self, project, captured_sql):
        _, log = run_dbt_and_capture(["run"])
        assert any("create or replace table" in s for s in captured_sql)
        assert not any("comment on" in s.lower() for s in captured_sql)
        assert not any("grant" in s.lower() for s in captured_sql)
        assert "grants are not supported" not in log.lower()
