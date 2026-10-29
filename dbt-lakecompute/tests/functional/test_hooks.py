"""All four hook phases run, and no bare `commit;` is sent."""

import pytest

from dbt.tests.util import run_dbt


model_with_hooks_sql = """
{{ config(
    materialized='table',
    pre_hook=[
        before_begin("select 'hook_before_begin' as marker"),
        "select 'hook_pre_in_transaction' as marker",
    ],
    post_hook=[
        "select 'hook_post_in_transaction' as marker",
        after_commit("select 'hook_after_commit' as marker"),
    ],
) }}
select 1 as id
"""


class TestModelHooks:
    @pytest.fixture(scope="class")
    def models(self):
        return {"model_hooks.sql": model_with_hooks_sql}

    def test_every_phase_runs(self, project, captured_sql):
        assert len(run_dbt(["run"])) == 1

        markers = [
            marker
            for marker in (
                "hook_before_begin",
                "hook_pre_in_transaction",
                "hook_post_in_transaction",
                "hook_after_commit",
            )
            if not any(marker in sql for sql in captured_sql)
        ]
        assert markers == [], f"hooks that never reached the wire: {markers}"

    def test_hooks_bracket_the_model(self, project, captured_sql):
        run_dbt(["run"])

        def index_of(fragment):
            return next(i for i, sql in enumerate(captured_sql) if fragment in sql)

        main = index_of("create or replace table")
        assert index_of("hook_before_begin") < main
        assert index_of("hook_pre_in_transaction") < main
        assert main < index_of("hook_post_in_transaction")
        assert index_of("hook_post_in_transaction") < index_of("hook_after_commit")


class TestRunHooksShadow:
    @pytest.fixture(scope="class")
    def models(self):
        return {"model_hooks.sql": model_with_hooks_sql}

    def test_no_bare_commit_statement(self, project, captured_sql):
        run_dbt(["run"])
        assert not any(sql.strip().rstrip(";").strip().lower() == "commit" for sql in captured_sql)

    def test_shadow_is_defined(self, project):
        manifest = run_dbt(["parse"])
        candidates = [
            macro
            for macro in manifest.macros.values()
            if macro.name == "run_hooks" and macro.package_name == "dbt_lakecompute"
        ]
        assert candidates, "dbt_lakecompute does not define run_hooks any more"
        assert "commit;" not in candidates[0].macro_sql


class TestPostHooksCanResolveCreatedTable:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_hooks.sql": """
{{ config(
    materialized='table',
    post_hook=[
        "{{ require_created_relation() }}",
        after_commit("{{ require_created_relation() }}"),
    ],
) }}
select 1 as id
"""
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "require_created_relation.sql": """
{% macro require_created_relation() %}
  {% if execute %}
    {% set relation = adapter.get_relation(
        database=this.database, schema=this.schema, identifier=this.identifier
    ) %}
    {% if relation is none %}
      {{ exceptions.raise_compiler_error('Post-hook could not resolve the created table') }}
    {% endif %}
    select count(*) from {{ relation }}
  {% endif %}
{% endmacro %}
"""
        }

    def test_first_build_resolves_the_table_in_both_hook_phases(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1
