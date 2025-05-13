import pytest
from dbt.tests.util import run_dbt_and_capture

_BAD_MODEL = """
                {{
                  config(
                    materialized = 'view',
                    )
                }}

SELECT 1 FROM non_existent_table
"""

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


@pytest.mark.skip("We can't reliably reproduce this error in the test environment.")
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


class TestRetryAll:

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "bad_model.sql": _BAD_MODEL,
        }

    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target):
        outputs = {"default": dbt_profile_target}
        outputs["default"]["retries"] = 2
        outputs["default"]["retry_all"] = True
        return outputs

    def test_running_bad_model_retries(self, project):
        result, log = run_dbt_and_capture(["run", "--log-level=debug"], expect_pass=False)
        assert "Retrying query due to error" in log
