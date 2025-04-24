import pytest
import threading
import time
from dbt.tests.util import run_dbt
from dbt_common.exceptions import DbtRuntimeError


class TestExceptionHandling:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "concurrent_table.sql": """
                {{ config(materialized='table') }}

                select 1 as id, 'test' as name
            """
        }

    def test_concurrent_relation_access_error(self, project, adapter):
        """
        This test validates that we can induce an error from redshift when we try to access a relation
        while that relation is being modified. This is done by repeatedly dropping and recreating tables and granting
        access to them at the same time.
        """
        run_dbt(["run", "--select", "concurrent_table"])
        schema = project.adapter.config.credentials.schema
        table_name = f"{schema}.concurrent_table"
        table_script = f"""
                        begin;
                        drop table if exists {table_name}_tmp;
                        create table {table_name}_tmp as (select 1 as id, 'test' as name);
                        alter table {table_name} rename to concurrent_table_old;
                        alter table {table_name}_tmp rename to concurrent_table;
                        drop table if exists {table_name}_old;
                        commit;
                        """

        def drop_and_recreate_table(sql_script):
            for _ in range(5):  # Do this 5 times to increase chance of race condition
                with adapter.connection_named("test_concurrent_create"):
                    try:
                        adapter.execute(sql_script)
                        print("Table recreated successfully.")
                    except Exception as e:
                        print(f"Error in thread: {e}")
                    finally:
                        time.sleep(0.1)  # Small delay to increase chance of race condition

        thread_1 = threading.Thread(target=drop_and_recreate_table, args=(table_script,))

        thread_1.start()
        time.sleep(0.1)
        with pytest.raises(DbtRuntimeError) as excinfo:
            for _ in range(10):
                with adapter.connection_named("test_concurrent_pg_table"):
                    adapter.execute(f"grant select on all tables in schema {schema} to public")
                    result = adapter.execute("select * from pg_tables")
                    print(result)
        assert "could not open relation with OID" in str(excinfo.value)
        thread_1.join()
