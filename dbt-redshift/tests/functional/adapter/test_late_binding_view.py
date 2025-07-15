import pytest

from dbt.tests.util import run_dbt, run_sql_with_adapter

_MODEL_SQL = """{{
  config(
    materialized='view',
    bind=False
  )
}}
select * from {{ ref('seed') }}
"""

_SEED_CSV = """
id,first_name,email,ip_address,updated_at
1,Larry,lking0@miitbeian.gov.cn,69.135.206.194,2008-09-12 19:08:31
""".lstrip()


class TestLateBindingView:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model.sql": _MODEL_SQL,
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"seed.csv": _SEED_CSV}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seeds": {
                "quote_columns": False,
            }
        }

    def test_late_binding_view_query(self, project):
        seed_run_result = run_dbt(["seed"])
        assert len(seed_run_result) == 1
        run_result = run_dbt()
        assert len(run_result) == 1
        # drop the table. Use 'cascade' here so that if late-binding views
        # didn't work as advertised, the following dbt run will fail.
        drop_query = """drop table if exists {}.seed cascade""".format(project.test_schema)
        run_sql_with_adapter(project.adapter, drop_query)
        run_result = run_dbt()
        assert len(run_result) == 1
