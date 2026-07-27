import pytest
from dbt.tests.util import check_table_does_exist, run_dbt

_SEED_CSV = """
id,first_name,last_name,email,product_id
1,Jack,Hunter,jhunter0@foo.bar,1
2,Kathryn,Walker,kwalker1@foo.bar,1
3,Gerald,Ryan,gryan2@foo.bar,3
4,Jack,Hunter,jhunter1@foo.bar,4
5,Kathryn,Walker,kwalker2@foo.bar,5
6,Gerald,Ryan,gryan3@foo.bar,6
""".lstrip()


class TestClusterBy:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"seed.csv": _SEED_CSV}

    @pytest.fixture(scope="class")
    def models(self, dbt_profile_target):
        warehouse_name = dbt_profile_target["warehouse"]

        _DYNAMIC_TABLE_1_SQL = f"""
        {{{{ config(materialized='dynamic_table', snowflake_warehouse='{warehouse_name}', target_lag='1 minute') }}}}
        select * from {{{{ ref('seed') }}}}
        """.lstrip()

        _DYNAMIC_TABLE_2_SQL = f"""
        {{{{ config(materialized='dynamic_table', cluster_by=['last_name'], snowflake_warehouse='{warehouse_name}', target_lag='1 minute') }}}}
        select * from {{{{ ref('dynamic_table_1') }}}}
        """.lstrip()

        _DYNAMIC_TABLE_3_SQL = f"""
        {{{{ config(materialized='dynamic_table', cluster_by=['last_name', 'first_name'], snowflake_warehouse='{warehouse_name}', target_lag='1 minute') }}}}
        select
            last_name,
            first_name,
            count(*) as count
        from {{{{ ref('seed') }}}}
        group by 1, 2
        """.lstrip()

        _DYNAMIC_TABLE_4_SQL = f"""
        {{{{ config(materialized='dynamic_table', cluster_by=['last_name', 'product_id % 3'], snowflake_warehouse='{warehouse_name}', target_lag='1 minute') }}}}
        select
            last_name,
            first_name,
            product_id,
            count(*) as count
        from {{{{ ref('seed') }}}}
        group by 1, 2, 3
        """.lstrip()

        return {
            "dynamic_table_1.sql": _DYNAMIC_TABLE_1_SQL,
            "dynamic_table_2.sql": _DYNAMIC_TABLE_2_SQL,
            "dynamic_table_3.sql": _DYNAMIC_TABLE_3_SQL,
            "dynamic_table_4.sql": _DYNAMIC_TABLE_4_SQL,
        }

    def test_snowflake_dynamic_table_cluster_by(self, project):

        run_dbt(["seed"])

        db_with_schema = f"{project.database}.{project.test_schema}"

        check_table_does_exist(
            project.adapter, f"{db_with_schema}.{self._available_models_in_setup()['seed_table']}"
        )

        run_dbt()

        # Check that all dynamic tables exist
        check_table_does_exist(
            project.adapter,
            f"{db_with_schema}.{self._available_models_in_setup()['dynamic_table_1']}",
        )
        check_table_does_exist(
            project.adapter,
            f"{db_with_schema}.{self._available_models_in_setup()['dynamic_table_2']}",
        )
        check_table_does_exist(
            project.adapter,
            f"{db_with_schema}.{self._available_models_in_setup()['dynamic_table_3']}",
        )
        check_table_does_exist(
            project.adapter,
            f"{db_with_schema}.{self._available_models_in_setup()['dynamic_table_4']}",
        )

        with project.adapter.connection_named("__test"):
            # Check if cluster_by is applied to dynamic_table_2 (should cluster by last_name)
            cluster_by = self._get_dynamic_table_ddl(
                project, self._available_models_in_setup()["dynamic_table_2"]
            )
            assert "CLUSTER BY (LAST_NAME)" in cluster_by.upper()

            # Check if cluster_by is applied to dynamic_table_3 (should cluster by last_name, first_name)
            cluster_by = self._get_dynamic_table_ddl(
                project, self._available_models_in_setup()["dynamic_table_3"]
            )
            assert "CLUSTER BY (LAST_NAME, FIRST_NAME)" in cluster_by.upper()

            # Check if cluster_by is applied to dynamic_table_4 (should cluster by last_name, product_id % 3)
            cluster_by = self._get_dynamic_table_ddl(
                project, self._available_models_in_setup()["dynamic_table_4"]
            )
            assert "CLUSTER BY (LAST_NAME, PRODUCT_ID % 3)" in cluster_by.upper()

    def _get_dynamic_table_ddl(self, project, table_name: str) -> str:
        ddl_query = f"SELECT GET_DDL('DYNAMIC_TABLE', '{project.database}.{project.test_schema}.{table_name}')"
        ddl = project.run_sql(ddl_query, fetch="one")
        return ddl[0]

    def _available_models_in_setup(self) -> dict[str, str]:
        return dict(
            seed_table="SEED",
            dynamic_table_1="DYNAMIC_TABLE_1",
            dynamic_table_2="DYNAMIC_TABLE_2",
            dynamic_table_3="DYNAMIC_TABLE_3",
            dynamic_table_4="DYNAMIC_TABLE_4",
        )
