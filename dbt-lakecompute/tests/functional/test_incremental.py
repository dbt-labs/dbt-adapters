"""Incremental models: `insert into ... by name` to append, else a full rebuild."""

import pytest

from dbt.tests.util import run_dbt, run_dbt_and_capture, write_file


model_append_sql = """
{{ config(materialized='incremental') }}
select {{ var('n', 1) }} as id, 'row {{ var('n', 1) }}' as name
{% if is_incremental() %}
where {{ var('n', 1) }} > (select max(id) from {{ this }})
{% endif %}
"""

# with this filter, an overwrite would replace the table with only the new rows
model_overwrite_sql = """
{{ config(materialized='incremental', incremental_strategy='insert_overwrite') }}
select range as id from range(1, {{ var('n', 1) }} + 1)
{% if is_incremental() %}
where range > (select max(id) from {{ this }})
{% endif %}
"""

model_columns_sql = """
{{ config(materialized='incremental') }}
select 1 as id, 'alice' as name
"""

contract_schema_yml = """
version: 2
models:
  - name: inc_contract
    config:
      contract:
        enforced: true
      on_schema_change: fail
    columns:
      - name: id
        data_type: integer
      - name: name
        data_type: varchar
"""


def _normalized(statements):
    return [" ".join(s.split()) for s in statements]


def _vars(n):
    return ["--vars", f"{{n: {n}}}"]


def _relation(project, identifier):
    return f"{project.database}.{project.test_schema}.{identifier}"


def _rows(project, identifier, columns="id, name"):
    sql = f"select {columns} from {_relation(project, identifier)} order by id"
    return [tuple(r) for r in project.run_sql(sql, fetch="all")]


def _ids(project, identifier):
    return [r[0] for r in _rows(project, identifier, "id")]


def _columns(project, identifier):
    sql = f"select column_name, column_type from (describe {_relation(project, identifier)})"
    return [tuple(r) for r in project.run_sql(sql, fetch="all")]


def _write_model(project, name, contents):
    write_file(contents, project.project_root, "models", name)


def _incremental(select, on_schema_change=None):
    config = "materialized='incremental'"
    if on_schema_change:
        config += f", on_schema_change='{on_schema_change}'"
    return f"{{{{ config({config}) }}}}\n{select}"


class TestIncrementalAppend:
    @pytest.fixture(scope="class")
    def models(self):
        return {"inc.sql": model_append_sql}

    def test_build_append_and_full_refresh(self, project, captured_sql):
        relation = _relation(project, "inc")

        assert len(run_dbt(["run", *_vars(1)])) == 1
        sent = _normalized(captured_sql)
        assert any(s.startswith(f"create or replace table {relation} as (") for s in sent)
        assert _rows(project, "inc") == [(1, "row 1")]

        del captured_sql[:]
        assert len(run_dbt(["run", *_vars(2)])) == 1
        sent = _normalized(captured_sql)
        assert (
            f"insert into {relation} by name ( select 2 as id, 'row 2' as name "
            f"where 2 > (select max(id) from {relation}) )"
        ) in sent
        assert not any("create or replace table" in s for s in sent)
        assert not any("__dbt_tmp" in s or "__dbt_backup" in s for s in sent)
        assert not any("rename" in s.lower() or s.startswith("drop") for s in sent)
        assert _rows(project, "inc") == [(1, "row 1"), (2, "row 2")]

        run_dbt(["run", *_vars(2)])
        assert len(_rows(project, "inc")) == 2

        del captured_sql[:]
        run_dbt(["run", "--full-refresh", *_vars(3)])
        sent = _normalized(captured_sql)
        assert any(s.startswith(f"create or replace table {relation} as (") for s in sent)
        assert not any(s.startswith("insert into") for s in sent)
        assert not any("select max(id)" in s for s in sent)
        assert _rows(project, "inc") == [(3, "row 3")]


class TestIncrementalAppendByName:
    @pytest.fixture(scope="class")
    def models(self):
        return {"inc_columns.sql": model_columns_sql}

    def test_columns_match_by_name(self, project):
        run_dbt(["run"])

        _write_model(project, "inc_columns.sql", _incremental("select 'bob' as name, 2 as id"))
        run_dbt(["run"])

        _write_model(project, "inc_columns.sql", _incremental("select 3 as id"))
        run_dbt(["run"])
        assert _rows(project, "inc_columns") == [(1, "alice"), (2, "bob"), (3, None)]

        # on_schema_change='ignore': a column the table lacks fails
        _write_model(
            project, "inc_columns.sql", _incremental("select 4 as id, 'x' as name, 1 as extra")
        )
        _, log = run_dbt_and_capture(["run"], expect_pass=False)
        assert "extra" in log
        assert len(_rows(project, "inc_columns")) == 3


class TestIncrementalInsertOverwrite:
    @pytest.fixture(scope="class")
    def models(self):
        return {"inc_overwrite.sql": model_overwrite_sql}

    def test_every_run_rebuilds_everything(self, project, captured_sql):
        run_dbt(["run", *_vars(1)])
        assert _ids(project, "inc_overwrite") == [1]
        run_dbt(["run", *_vars(2)])
        assert _ids(project, "inc_overwrite") == [1, 2]
        run_dbt(["run", *_vars(2)])
        assert _ids(project, "inc_overwrite") == [1, 2]

        sent = _normalized(captured_sql)
        assert sum(s.startswith("create or replace table") for s in sent) == 3
        assert not any(s.startswith("insert into") for s in sent)
        assert not any("select max(id)" in s for s in sent)


class TestUnsupportedIncrementalStrategies:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "inc_merge.sql": (
                "{{ config(materialized='incremental', incremental_strategy='merge', "
                "unique_key='id') }}\nselect 1 as id"
            ),
            "inc_delete_insert.sql": (
                "{{ config(materialized='incremental', incremental_strategy='delete+insert', "
                "unique_key='id') }}\nselect 1 as id"
            ),
        }

    @pytest.mark.parametrize(
        "model,strategy", [("inc_merge", "merge"), ("inc_delete_insert", "delete+insert")]
    )
    def test_refused_before_anything_is_sent(self, project, captured_sql, model, strategy):
        _, log = run_dbt_and_capture(["run", "--select", model], expect_pass=False)
        assert f"uses '{strategy}'" in log
        assert f"model.test.{model}" in log
        assert not any(model in s for s in captured_sql)


class TestOnSchemaChangeAppendNewColumns:
    @pytest.fixture(scope="class")
    def models(self):
        return {"inc_anc.sql": _incremental("select 1 as id, 'a' as name", "append_new_columns")}

    def test_adds_and_never_drops(self, project, captured_sql):
        relation = _relation(project, "inc_anc")
        run_dbt(["run"])

        _write_model(
            project,
            "inc_anc.sql",
            _incremental(
                "select 2 as id, 'b' as name, 2.5::decimal(18, 3) as amount, true as flag",
                "append_new_columns",
            ),
        )
        del captured_sql[:]
        run_dbt(["run"])
        assert (
            f'alter table {relation} add column "amount" DECIMAL(18,3); '
            f'alter table {relation} add column "flag" BOOLEAN'
        ) in _normalized(captured_sql)

        _write_model(
            project,
            "inc_anc.sql",
            _incremental("select 3 as id, true as flag", "append_new_columns"),
        )
        run_dbt(["run"])

        assert _columns(project, "inc_anc") == [
            ("id", "INTEGER"),
            ("name", "VARCHAR"),
            ("amount", "DECIMAL(18,3)"),
            ("flag", "BOOLEAN"),
        ]
        rows = _rows(project, "inc_anc", "id, name, amount, flag")
        assert [(r[0], r[1], r[3]) for r in rows] == [
            (1, "a", None),
            (2, "b", True),
            (3, None, True),
        ]
        assert [r[2] is None for r in rows] == [True, False, True]


class TestOnSchemaChangeSyncAllColumns:
    @pytest.fixture(scope="class")
    def models(self):
        return {"inc_sync.sql": _incremental("select 1 as id, 'a' as name", "sync_all_columns")}

    def test_adds_and_drops(self, project, captured_sql):
        relation = _relation(project, "inc_sync")
        run_dbt(["run"])

        _write_model(
            project,
            "inc_sync.sql",
            _incremental("select 2 as id, true as flag", "sync_all_columns"),
        )
        del captured_sql[:]
        run_dbt(["run"])
        sent = _normalized(captured_sql)
        add = f'alter table {relation} add column "flag" BOOLEAN'
        drop = f'alter table {relation} drop column "name"'
        insert = next(i for i, s in enumerate(sent) if s.startswith("insert into"))
        assert sent.index(add) < insert < sent.index(drop)
        assert _columns(project, "inc_sync") == [("id", "INTEGER"), ("flag", "BOOLEAN")]
        assert _rows(project, "inc_sync", "id, flag") == [(1, None), (2, True)]

    def test_type_change_fails_before_any_alter(self, project, captured_sql):
        _write_model(
            project,
            "inc_sync.sql",
            _incremental("select 1 as id, 'a' as name", "sync_all_columns"),
        )
        run_dbt(["run", "--full-refresh"])

        _write_model(
            project,
            "inc_sync.sql",
            _incremental("select 3.5::double as id, 'x' as extra", "sync_all_columns"),
        )
        del captured_sql[:]
        _, log = run_dbt_and_capture(["run"], expect_pass=False)
        assert "cannot change a column's type in place" in log
        assert "id: INTEGER in the table, DOUBLE in the model" in log
        sent = _normalized(captured_sql)
        assert not any(s.startswith(("alter table", "insert into")) for s in sent)
        assert _columns(project, "inc_sync") == [("id", "INTEGER"), ("name", "VARCHAR")]

    def test_dropped_column_still_readable_by_the_filter(self, project):
        _write_model(
            project,
            "inc_sync.sql",
            _incremental("select 1 as id, 1 as watermark", "sync_all_columns"),
        )
        run_dbt(["run", "--full-refresh"])

        _write_model(
            project,
            "inc_sync.sql",
            _incremental(
                "select 2 as id\n"
                "{% if is_incremental() %}"
                "where 2 > (select max(watermark) from {{ this }})"
                "{% endif %}",
                "sync_all_columns",
            ),
        )
        run_dbt(["run"])
        assert _columns(project, "inc_sync") == [("id", "INTEGER")]
        assert _ids(project, "inc_sync") == [1, 2]


class TestIncrementalContract:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "inc_contract.sql": model_columns_sql,
            "schema.yml": contract_schema_yml,
        }

    def test_append_is_checked(self, project, captured_sql):
        run_dbt(["--warn-error", "run"])
        del captured_sql[:]
        run_dbt(["run"])
        assert any(s.startswith("insert into") for s in _normalized(captured_sql))

        _write_model(
            project,
            "schema.yml",
            contract_schema_yml.replace("data_type: varchar", "data_type: date"),
        )
        del captured_sql[:]
        _, log = run_dbt_and_capture(["run"], expect_pass=False)
        assert "contract" in log.lower()
        assert not any(s.startswith("insert into") for s in _normalized(captured_sql))
        assert len(_rows(project, "inc_contract")) == 2

    @pytest.mark.parametrize("on_schema_change", ["fail", "append_new_columns"])
    def test_table_is_checked_too(self, project, captured_sql, on_schema_change):
        """Query and contract agree, but the table still has the old type."""
        schema_yml = contract_schema_yml.replace(
            "on_schema_change: fail", f"on_schema_change: {on_schema_change}"
        )
        _write_model(project, "schema.yml", schema_yml)
        _write_model(project, "inc_contract.sql", model_columns_sql)
        run_dbt(["run", "--full-refresh"])

        _write_model(
            project, "schema.yml", schema_yml.replace("data_type: integer", "data_type: double")
        )
        _write_model(
            project, "inc_contract.sql", _incremental("select 1.5::double as id, 'x' as name")
        )
        del captured_sql[:]
        _, log = run_dbt_and_capture(["run"], expect_pass=False)
        assert "id: INTEGER in the table, DOUBLE in the model" in log
        assert not any(s.startswith(("alter", "insert")) for s in _normalized(captured_sql))
        assert _rows(project, "inc_contract") == [(1, "alice")]
        assert _columns(project, "inc_contract")[0] == ("id", "INTEGER")

    def test_refused_before_any_alter(self, project, captured_sql):
        """A column the contract does not declare must not be added before the contract fails."""
        schema_yml = contract_schema_yml.replace(
            "on_schema_change: fail", "on_schema_change: append_new_columns"
        )
        _write_model(project, "schema.yml", schema_yml)
        _write_model(project, "inc_contract.sql", model_columns_sql)
        run_dbt(["run", "--full-refresh"])

        _write_model(
            project,
            "inc_contract.sql",
            _incremental("select 2 as id, 'b' as name, 9 as extra"),
        )
        del captured_sql[:]
        _, log = run_dbt_and_capture(["run"], expect_pass=False)
        assert "contract" in log.lower()
        assert not any(s.startswith(("alter", "insert")) for s in _normalized(captured_sql))
        assert [c[0] for c in _columns(project, "inc_contract")] == ["id", "name"]


class TestOnSchemaChangeFail:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "inc_fail.sql": _incremental(
                "select 1 as id, 'alice' as name, 2.5::decimal(18, 3) as amount", "fail"
            )
        }

    def test_unchanged_appends_and_changed_fails(self, project):
        run_dbt(["run"])
        run_dbt(["run"])
        assert len(_rows(project, "inc_fail")) == 2

        _write_model(project, "inc_fail.sql", _incremental("select 3 as id, 1 as extra", "fail"))
        _, log = run_dbt_and_capture(["run"], expect_pass=False)
        assert "extra: not in the table" in log
        assert "name: no longer selected by the model" in log
        assert len(_rows(project, "inc_fail")) == 2


class TestRebuildReplacesCachedKind:
    """A relation the listing typed as external is rebuilt, and the cache must say table after."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "inc_kind.sql": (
                "{{ config(materialized='incremental', post_hook=\"{{ log('cached kind: ' ~ "
                'load_cached_relation(this).type, info=True) }}") }}\nselect 1 as id'
            )
        }

    def test_post_hook_sees_a_table(self, project, monkeypatch):
        from dbt.adapters.base.relation import RelationType
        from dbt.adapters.lakecompute.impl import LakeComputeAdapter

        run_dbt(["run"])

        original = LakeComputeAdapter.list_relations_without_caching

        def listed_as_external(self, schema_relation):
            return [
                r.incorporate(type=RelationType.External) for r in original(self, schema_relation)
            ]

        monkeypatch.setattr(
            LakeComputeAdapter, "list_relations_without_caching", listed_as_external
        )
        _, log = run_dbt_and_capture(["run"])
        assert "cached kind: table" in log
        assert "cached kind: external" not in log


class TestSchemaCheckRunsAfterPreHooks:
    """The source exists only between the pre-hook and the post-hook."""

    @pytest.fixture(scope="class")
    def models(self):
        src = "{{ this.database }}.{{ this.schema }}.inc_hooked_src"
        return {
            "inc_hooked.sql": (
                "{{ config(materialized='incremental', on_schema_change='fail', "
                f'pre_hook="create or replace table {src} as select 1 as id", '
                f'post_hook="drop table if exists {src}") }}}}\n'
                f"select id from {src}"
            )
        }

    def test_append(self, project, captured_sql):
        run_dbt(["run"])
        del captured_sql[:]
        run_dbt(["run"])
        sent = _normalized(captured_sql)
        assert any("describe select" in s for s in sent)
        assert any(s.startswith("insert into") for s in sent)
        assert len(_ids(project, "inc_hooked")) == 2


class TestSchemaCheckSendsSqlHeader:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "inc_header.sql": _incremental(
                "{% call set_sql_header(config) %}"
                "create or replace temporary macro lc_twice(x) as x * 2;"
                "{% endcall %}\n"
                "select lc_twice(1) as id",
                "fail",
            )
        }

    def test_append(self, project, captured_sql):
        run_dbt(["run"])
        del captured_sql[:]
        run_dbt(["run"])
        checks = [s for s in _normalized(captured_sql) if "describe select" in s]
        assert len(checks) == 1
        assert checks[0].startswith("create or replace temporary macro lc_twice(x)")
        assert _ids(project, "inc_header") == [2, 2]


class TestIsIncrementalSkipsTheLookup:
    """Only the materialization's own lookup runs when is_incremental() is decided by config."""

    @pytest.fixture(scope="class")
    def models(self):
        guarded = "select 1 as id\n{% if is_incremental() %}where 1 = 1{% endif %}"
        return {
            "inc_skip.sql": _incremental(guarded),
            "inc_skip_overwrite.sql": (
                "{{ config(materialized='incremental', incremental_strategy='insert_overwrite') }}"
                "\n" + guarded
            ),
            "tbl_skip.sql": "{{ config(materialized='table') }}\n" + guarded,
        }

    @pytest.mark.parametrize(
        "model,args",
        [
            ("inc_skip", ["--full-refresh"]),
            ("inc_skip_overwrite", []),
            ("tbl_skip", []),
        ],
    )
    def test_one_lookup(self, project, monkeypatch, model, args):
        from dbt.adapters.lakecompute.impl import LakeComputeAdapter

        run_dbt(["run", "--select", model])

        lookups = []
        original = LakeComputeAdapter.get_relation

        def counting(self, database, schema, identifier):
            if identifier == model:
                lookups.append(identifier)
            return original(self, database, schema, identifier)

        monkeypatch.setattr(LakeComputeAdapter, "get_relation", counting)
        run_dbt(["run", "--select", model, *args])
        assert lookups == [model]
