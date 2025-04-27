import os
import pytest

from dbt.tests.util import run_dbt, run_dbt_and_capture

_MACROS__CREATE_DB = """
{% macro create_db_fake() %}

{% set database = "db_for_test__do_delete_if_you_see_this" %}

{# IF NOT EXISTS not avaiable but Redshift merely returns an error for trying to overwrite #}
{% set create_command %}
    CREATE DATABASE {{ database }}
{% endset %}

{{ log(create_command, info=True) }}

{% do run_query(create_command) %}

{{ log("Created redshift database " ~ database, info=True) }}

{% endmacro %}
"""

_MACROS__UPDATE_MY_MODEL = """
{% macro update_some_model(alert_ids, sent_at, table_name) %}
      {% set update_query %}
          UPDATE {{ ref('my_model') }} set status = 'sent'
      {% endset %}
      {% do run_query(update_query) %}
{% endmacro %}
"""

_MACROS__UPDATE_MY_SEED = """
{% macro update_my_seed() %}
update {{ ref("my_seed") }} set status = 'done'
{% endmacro %}
"""

_MODELS__MY_MODEL = """
{{ config(materialized="table") }}

select 1 as id, 'pending' as status
"""

_MODELS__AFTER_COMMIT = """
{{
  config(
    post_hook=after_commit("{{ update_my_seed() }}")
  )
}}

select 1 as id
"""

_SEEDS_MY_SEED = """
id,status
1,pending
""".lstrip()


class TestTransactionBlocksPreventCertainCommands:
    @pytest.fixture(scope="class")
    def macros(self):
        return {"macro.sql": _MACROS__CREATE_DB}

    def test_autocommit_deactivated_prevents_DDL(self, project):
        """Scenario: user has autocommit=True in their target to run macros with normally
        forbidden commands like CREATE DATABASE and VACUUM"""
        result, out = run_dbt_and_capture(["run-operation", "create_db_fake"], expect_pass=False)
        assert "CREATE DATABASE cannot run inside a transaction block" not in out


class TestAutocommitUnblocksDDLInTransactions:
    @pytest.fixture(scope="class")
    def dbt_profile_target(self):
        return {
            "type": "redshift",
            "threads": 1,
            "retries": 6,
            "host": os.getenv("REDSHIFT_TEST_HOST"),
            "port": int(os.getenv("REDSHIFT_TEST_PORT")),
            "user": os.getenv("REDSHIFT_TEST_USER"),
            "pass": os.getenv("REDSHIFT_TEST_PASS"),
            "dbname": os.getenv("REDSHIFT_TEST_DBNAME"),
            "autocommit": False,
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {"macro.sql": _MACROS__CREATE_DB}

    def test_default_setting_allows_DDL(self, project):
        """Monitor if status quo in Redshift connector changes"""
        result, out = run_dbt_and_capture(["run-operation", "create_db_fake"], expect_pass=False)
        assert "CREATE DATABASE cannot run inside a transaction block" in out


class TestUpdateDDLCommits:
    @pytest.fixture(scope="class")
    def macros(self):
        return {"macro.sql": _MACROS__UPDATE_MY_MODEL}

    @pytest.fixture(scope="class")
    def models(self):
        return {"my_model.sql": _MODELS__MY_MODEL}

    def test_update_will_go_through(self, project):
        run_dbt()
        run_dbt(["run-operation", "update_some_model"])
        _, out = run_dbt_and_capture(
            ["show", "--inline", "select * from {}.my_model".format(project.test_schema)]
        )
        assert "1 | sent" in out


class TestUpdateDDLDoesNotCommitWithoutAutocommit:
    @pytest.fixture(scope="class")
    def dbt_profile_target(self):
        return {
            "type": "redshift",
            "host": os.getenv("REDSHIFT_TEST_HOST"),
            "port": int(os.getenv("REDSHIFT_TEST_PORT")),
            "user": os.getenv("REDSHIFT_TEST_USER"),
            "pass": os.getenv("REDSHIFT_TEST_PASS"),
            "dbname": os.getenv("REDSHIFT_TEST_DBNAME"),
            "autocommit": False,
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {"macro.sql": _MACROS__UPDATE_MY_MODEL}

    @pytest.fixture(scope="class")
    def models(self):
        return {"my_model.sql": _MODELS__MY_MODEL}

    def test_update_will_not_go_through(self, project):
        run_dbt()
        run_dbt(["run-operation", "update_some_model"])
        _, out = run_dbt_and_capture(
            ["show", "--inline", "select * from {}.my_model".format(project.test_schema)]
        )
        assert "1 | pending" in out


class TestAfterCommitMacroTakesEffect:
    @pytest.fixture(scope="class")
    def macros(self):
        return {"macro.sql": _MACROS__UPDATE_MY_SEED}

    @pytest.fixture(scope="class")
    def models(self):
        return {"my_model.sql": _MODELS__AFTER_COMMIT}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"my_seed.csv": _SEEDS_MY_SEED}

    def test_update_happens_via_macro_in_config(self, project):
        run_dbt(["seed"])
        _, out = run_dbt_and_capture(
            ["show", "--inline", "select * from {}.my_seed".format(project.test_schema)]
        )
        assert "1 | pending" in out

        run_dbt()
        _, out = run_dbt_and_capture(
            ["show", "--inline", "select * from {}.my_seed".format(project.test_schema)]
        )
        assert "1 | done" in out
