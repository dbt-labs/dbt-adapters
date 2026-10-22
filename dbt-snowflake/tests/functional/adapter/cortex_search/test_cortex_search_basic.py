"""Live-account tests for the `cortex_search_service` materialization.

They need a Snowflake account with Cortex Search enabled and `SNOWFLAKE_TEST_WAREHOUSE`
set to a warehouse the service can refresh with.
"""

import pytest

from dbt.tests.util import (
    assert_message_in_logs,
    get_connection,
    relation_from_name,
    run_dbt,
    run_dbt_and_capture,
)


SEED = """
doc_id,doc_name,body,region
1,password,how to reset a password,us
2,billing,billing cycle explained,eu
3,export,exporting reports to csv,us
""".strip()


SERVICE = """
{{ config(
    materialized='cortex_search_service',
    search_column='body',
    attributes=['region'],
    primary_key=['doc_id'],
    snowflake_warehouse=target.warehouse,
    target_lag='1 hour',
    embedding_model='snowflake-arctic-embed-m-v1.5',
    refresh_mode='INCREMENTAL',
    initialize='ON_CREATE',
    full_index_build_interval_days=7,
    request_logging=false,
    auto_suspend=1800,
    comment='Support articles'
) }}
select doc_id::varchar as doc_id, doc_name, body, region from {{ ref('support_docs') }}
"""

# A service isn't queryable as a table, so `ref()` is passed to CORTEX_SEARCH_DATA_SCAN
# by name. This proves dependency ordering and name resolution.
DOWNSTREAM = """
{{ config(materialized='table') }}
select * from table(cortex_search_data_scan(service_name => '{{ ref('support_search') }}'))
"""


def service_created_on(project, name):
    relation = relation_from_name(project.adapter, name)
    sql = (
        f"show cortex search services like '{relation.identifier}' "
        f"in schema {relation.database}.{relation.schema}"
    )
    with get_connection(project.adapter):
        _, table = project.adapter.execute(sql, fetch=True)
    table = table.rename(column_names=[c.lower() for c in table.column_names])
    rows = [row for row in table if row["name"].upper() == relation.identifier.upper()]
    assert len(rows) == 1, f"expected one service named {relation.identifier}, got {rows}"
    return rows[0]["created_on"]


class TestCortexSearchServiceBasic:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"support_docs.csv": SEED}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "support_search.sql": SERVICE,
            "support_search_scan.sql": DOWNSTREAM,
        }

    @pytest.fixture(scope="class", autouse=True)
    def setup(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])
        yield

    def test_create(self, project):
        assert service_created_on(project, "support_search") is not None

    def test_rerun_keeps_service(self, project):
        created_on = service_created_on(project, "support_search")
        _, logs = run_dbt_and_capture(["--debug", "run", "--select", "support_search"])
        assert_message_in_logs("create cortex search service if not exists", logs)
        assert_message_in_logs("an existing service is kept unchanged", logs)
        assert service_created_on(project, "support_search") == created_on

    def test_full_refresh_replaces_service(self, project):
        created_on = service_created_on(project, "support_search")
        _, logs = run_dbt_and_capture(
            ["--debug", "run", "--select", "support_search", "--full-refresh"]
        )
        assert_message_in_logs("create or replace cortex search service", logs)
        assert service_created_on(project, "support_search") != created_on

    def test_downstream_ref(self, project):
        scan = relation_from_name(project.adapter, "support_search_scan")
        assert project.run_sql(f"select count(*) from {scan}", fetch="one")[0] == 3
