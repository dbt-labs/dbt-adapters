"""
Regression tests for https://github.com/dbt-labs/dbt-adapters/issues/1511: `insert overwrite
... select *` mapped values by position, but the target and tmp relation do not always agree
on column order.
"""

import pytest

from dbt.tests.util import run_dbt, write_file


def _model(select_list, tmp_relation_type="view", overwrite_columns=None):
    overwrite = f"    overwrite_columns={overwrite_columns!r},\n" if overwrite_columns else ""
    return f"""
{{{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    on_schema_change='append_new_columns',
    tmp_relation_type='{tmp_relation_type}',
{overwrite}) }}}}

select
{select_list}
"""


def _contract_yml(names, columns):
    models = "".join(
        f"""  - name: {name}
    config:
      contract:
        enforced: true
    columns:
"""
        + "".join(f"      - name: {c}\n        data_type: {t}\n" for c, t in columns)
        for name in names
    )
    return f"version: 2\nmodels:\n{models}"


# swapped vs the contract, same data type so a positional overwrite is silent, not a cast error
_SELECT_SWAPPED = """    '2025-01-01'::date as column1,
    'a'::varchar(10) as column2,
    44::number(38,0) as column4,
    33::number(38,0) as column3"""

# column2b sits mid-list in the contract, but `alter table add column` appends it to the end
_SELECT_WITH_NEW_COLUMN = """    '2025-01-01'::date as column1,
    'a'::varchar(10) as column2,
    22::number(38,0) as column2b,
    44::number(38,0) as column4,
    33::number(38,0) as column3"""

_COLUMNS_V1 = [
    ("column1", "date"),
    ("column2", "varchar(10)"),
    ("column3", "number(38,0)"),
    ("column4", "number(38,0)"),
]

_COLUMNS_V2 = [
    ("column1", "date"),
    ("column2", "varchar(10)"),
    ("column2b", "number(38,0)"),
    ("column3", "number(38,0)"),
    ("column4", "number(38,0)"),
]

_TMP_RELATION_TYPES = ("view", "table", "transient")


def _physical_column_order(project, model):
    return [
        row[0]
        for row in project.run_sql(
            "select column_name from information_schema.columns "
            f"where table_schema = upper('{project.test_schema}') "
            f"and table_name = upper('{model}') order by ordinal_position",
            fetch="all",
        )
    ]


class TestInsertOverwriteContractColumnOrder:
    """Contract order differs from model SQL order."""

    @pytest.fixture(scope="class")
    def models(self):
        models = {
            f"{tmp}_tmp.sql": _model(_SELECT_SWAPPED, tmp_relation_type=tmp)
            for tmp in _TMP_RELATION_TYPES
        }
        models["schema.yml"] = _contract_yml(
            [f"{tmp}_tmp" for tmp in _TMP_RELATION_TYPES], _COLUMNS_V1
        )
        return models

    def test_values_map_by_name(self, project):
        run_dbt(["run"])
        # the initial CREATE TABLE AS already maps by name
        for tmp in _TMP_RELATION_TYPES:
            assert project.run_sql(
                f"select column3, column4 from {project.test_schema}.{tmp}_tmp", fetch="one"
            ) == (33, 44)

        run_dbt(["run"])
        for tmp in _TMP_RELATION_TYPES:
            assert project.run_sql(
                f"select column3, column4 from {project.test_schema}.{tmp}_tmp", fetch="one"
            ) == (33, 44), f"tmp_relation_type={tmp} did not map by column name"


class TestInsertOverwriteAppendedColumnOrder:
    """A column added mid-contract is appended to the end of the target."""

    @pytest.fixture(scope="class")
    def models(self):
        models = {
            f"{tmp}_tmp.sql": _model(_SELECT_SWAPPED, tmp_relation_type=tmp)
            for tmp in _TMP_RELATION_TYPES
        }
        models["schema.yml"] = _contract_yml(
            [f"{tmp}_tmp" for tmp in _TMP_RELATION_TYPES], _COLUMNS_V1
        )
        return models

    def test_values_map_by_name_after_column_added(self, project):
        run_dbt(["run"])
        run_dbt(["run"])

        for tmp in _TMP_RELATION_TYPES:
            write_file(
                _model(_SELECT_WITH_NEW_COLUMN, tmp_relation_type=tmp),
                project.project_root,
                "models",
                f"{tmp}_tmp.sql",
            )
        write_file(
            _contract_yml([f"{tmp}_tmp" for tmp in _TMP_RELATION_TYPES], _COLUMNS_V2),
            project.project_root,
            "models",
            "schema.yml",
        )
        run_dbt(["run"])

        for tmp in _TMP_RELATION_TYPES:
            assert _physical_column_order(project, f"{tmp}_tmp")[-1] == "COLUMN2B", (
                f"COLUMN2B not appended last for tmp_relation_type={tmp}; "
                "this test no longer exercises the divergence"
            )
            assert project.run_sql(
                f"select column2b, column3, column4 from {project.test_schema}.{tmp}_tmp",
                fetch="one",
            ) == (22, 33, 44), f"tmp_relation_type={tmp} did not map by column name"


class TestInsertOverwriteColumnsConfig:
    """`overwrite_columns` keeps taking precedence."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "partial.sql": _model(
                _SELECT_SWAPPED, overwrite_columns=["column1", "column2", "column3"]
            ),
            "schema.yml": _contract_yml(["partial"], _COLUMNS_V1),
        }

    def test_overwrite_columns_precedence(self, project):
        run_dbt(["run"])
        run_dbt(["run"])
        # a column omitted from the list is NULLed: insert overwrite truncates first
        assert project.run_sql(
            f"select column3, column4 from {project.test_schema}.partial", fetch="one"
        ) == (33, None)


def _uncontracted_model(select_list, on_schema_change):
    return f"""
{{{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    on_schema_change='{on_schema_change}',
) }}}}

select {select_list}
"""


class TestInsertOverwriteRenamedColumn:
    """Intentional behaviour change: a rename used to write into the old column, silently."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"renamed.sql": _uncontracted_model("1 as a, 2 as b", "ignore")}

    def test_rename_is_surfaced(self, project):
        run_dbt(["run"])
        run_dbt(["run"])
        write_file(
            _uncontracted_model("1 as a, 2 as b2", "ignore"),
            project.project_root,
            "models",
            "renamed.sql",
        )
        results = run_dbt(["run"], expect_pass=False)
        assert str(results[0].status) == "error"
        # the tmp relation has no `b`, so the named select list fails to compile
        assert "invalid identifier 'b'" in results[0].message.lower(), results[0].message


class TestInsertOverwriteDroppedColumn:
    """Intentional behaviour change: the retained column is now NULLed, not a count error."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"dropped.sql": _uncontracted_model("1 as a, 2 as b, 3 as c", "append_new_columns")}

    def test_dropped_column_is_nulled(self, project):
        run_dbt(["run"])
        run_dbt(["run"])
        write_file(
            _uncontracted_model("1 as a, 2 as b", "append_new_columns"),
            project.project_root,
            "models",
            "dropped.sql",
        )
        run_dbt(["run"])
        assert project.run_sql(
            f"select a, b, c from {project.test_schema}.dropped", fetch="one"
        ) == (1, 2, None)


class TestInsertOverwriteAddedColumn:
    """Intentional behaviour change: under `ignore` a new column is dropped, not an error."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"added.sql": _uncontracted_model("1 as a, 2 as b", "ignore")}

    def test_added_column_is_dropped(self, project):
        run_dbt(["run"])
        run_dbt(["run"])
        write_file(
            _uncontracted_model("1 as a, 2 as b, 3 as c", "ignore"),
            project.project_root,
            "models",
            "added.sql",
        )
        run_dbt(["run"])
        assert project.run_sql(f"select a, b from {project.test_schema}.added", fetch="one") == (
            1,
            2,
        )
        assert "C" not in _physical_column_order(project, "added")
