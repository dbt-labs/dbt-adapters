import pytest
from dbt.tests.util import run_dbt_and_capture

_TEST_MODEL = """
                {{
                  config(
                    materialized = 'table',
                    )
                }}
                select 1 as test_col
            """

_MACROS = {
    "macro.sql": """
            {% macro transaction_macro() %}
                {% if execute %}
                    begin transaction;
                      --temp table
                      create temporary table trans_temp_table as (
                          select 1 as test_col
                      );

                      delete from {{target.schema}}.test_table where test_col in (select test_col from trans_temp_table);
                      insert into {{target.schema}}.test_table  (select * from trans_temp_table);
                      commit;
                    end transaction;

                    drop table trans_temp_table;
                {% endif %}
            {% endmacro %}
            """
}


class TestRetryOnRelationOidNotFound:
    """
    test based on bug report: https://github.com/dbt-labs/dbt-adapters/issues/642

    on-run-end macro is called twice because we will run the hooks concurrently ensuring we hit the exception

    """

    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target):
        outputs = {"default": dbt_profile_target}
        outputs["default"]["retries"] = 5
        outputs["default"]["threads"] = 4
        return outputs

    @pytest.fixture(scope="class")
    def models(self):
        return {"test_table.sql": _TEST_MODEL}

    @pytest.fixture(scope="class")
    def macros(self):
        return _MACROS

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "on-run-end": [
                "{{ transaction_macro() }}",
                "{{ transaction_macro() }}",
            ]
        }

    def test_retry_on_relation_oid_not_found(self, project):
        result, stdout = run_dbt_and_capture(["run", "--log-level=debug"])
        assert "could not open relation with OID" in stdout
        assert "Redshift adapter: Retrying query due to error: Database Error" in stdout
