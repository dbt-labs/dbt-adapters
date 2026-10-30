"""Live Cortex Search tests: drift, stable sources, source replacement, and by-name refs."""

import time

import pytest

from dbt.tests.util import (
    assert_message_in_logs,
    get_connection,
    get_manifest,
    run_dbt,
    run_dbt_and_capture,
)
from tests.functional.utils import update_model
from dbt_common.exceptions import DbtDatabaseError

SEED = """
doc_id,doc_name,body,region
1,password,how to reset a password,us
2,billing,billing cycle explained,eu
3,export,exporting reports to csv,us
""".strip()

UPSTREAM = """
{{ config(materialized='incremental', unique_key='doc_id', event_time='event_at') }}
select doc_id::varchar as doc_id, doc_name, body, region,
       '2026-01-01'::timestamp_ntz as event_at
from {{ ref('support_docs') }}
"""

SERVICE = """
{{ config(
    materialized='cortex_search_service',
    search_column='body', attributes=['region'], primary_key=['doc_id'],
    snowflake_warehouse=target.warehouse, target_lag='1 hour',
    embedding_model='snowflake-arctic-embed-m-v1.5', refresh_mode='INCREMENTAL',
    initialize='ON_CREATE', full_index_build_interval_days=7,
    request_logging=false, auto_suspend=1800, comment='Support articles',
    event_time='event_at'
) }}
select doc_id, doc_name, body, region, event_at from {{ ref('support_docs_incremental') }}
"""

DOWNSTREAM = r"""
{{ config(materialized='table') }}
{% set service = ref('support_search').render() %}
select * from table(cortex_search_data_scan(
    service_name => {{ dbt.string_literal(dbt.escape_single_quotes(service | replace('\\', '\\\\'))) }}
))
"""


def relation(project, name):
    node = get_manifest(project.project_root).nodes["model.test." + name]
    return project.adapter.Relation.create_from(project.adapter.config, node)


def execute(project, sql):
    with get_connection(project.adapter):
        _, table = project.adapter.execute(sql, fetch=True)
    return table.rename(column_names=[c.lower() for c in table.column_names])


def service_state(project):
    table = execute(
        project, f'describe cortex search service {relation(project, "support_search").render()}'
    )
    assert len(table.rows) == 1
    return table.rows[0]


def indexed_body(project, doc_id="1"):
    name = relation(project, "support_search").render().replace("'", "''")
    rows = execute(
        project,
        f"select body from table(cortex_search_data_scan(service_name=>'{name}')) where doc_id='{doc_id}'",
    )
    return rows.rows[0][0] if rows.rows else None


def wait_for_body(project, expected):
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        if indexed_body(project) == expected:
            return
        state = service_state(project)
        assert not state["indexing_error"], state["indexing_error"]
        time.sleep(2)
    pytest.fail("Search index did not receive the source update within 90 seconds")


class CortexSearchBase:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"support_docs.csv": SEED}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "support_docs_incremental.sql": UPSTREAM,
            "support_search.sql": SERVICE,
            "support_search_scan.sql": DOWNSTREAM,
        }

    @pytest.fixture(scope="class", autouse=True)
    def setup(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])
        yield

    def check_by_name(self, project, options):
        results = run_dbt(["run", "--select", "support_search_scan", *options])
        node = results[0].node
        assert "(select * from" not in node.compiled_code.lower()
        assert relation(project, "support_search").render() in node.compiled_code
        assert "model.test.support_search" in node.depends_on.nodes


class TestCortexSearchServiceBasic(CortexSearchBase):
    def test_create(self, project):
        assert service_state(project)["created_on"] is not None

    def test_rerun_keeps_service(self, project):
        created_on = service_state(project)["created_on"]
        run_dbt(["run", "--select", "support_search"])
        assert service_state(project)["created_on"] == created_on

    def test_downstream_ref(self, project):
        scan = relation(project, "support_search_scan")
        assert project.run_sql(f"select count(*) from {scan.render()}", fetch="one")[0] == 3

    def test_cheap_option_change_alters_without_replacing(self, project):
        created_on = service_state(project)["created_on"]
        updated = (
            SERVICE.replace("target_lag='1 hour'", "target_lag='2 hours'")
            .replace("full_index_build_interval_days=7", "full_index_build_interval_days=3")
            .replace("request_logging=false", "request_logging=true")
            .replace("auto_suspend=1800", "auto_suspend=3600")
            .replace("comment='Support articles'", 'comment="Support team\'s articles"')
        )
        update_model(project, "support_search", updated)
        try:
            _, logs = run_dbt_and_capture(["--debug", "run", "--select", "support_search"])
            assert_message_in_logs("alter cortex search service", logs)
            state = service_state(project)
            assert state["target_lag"] == "2 hours"
            assert state["full_index_build_interval_days"] == 3
            assert str(state["request_logging"]).lower() == "true"
            assert state["auto_suspend"] == 3600
            assert state["comment"] == "Support team's articles"
            assert state["created_on"] == created_on
        finally:
            update_model(project, "support_search", SERVICE)
            run_dbt(["run", "--select", "support_search"])

    def test_query_only_change_waits_for_full_refresh(self, project):
        changed = SERVICE.replace(
            "select doc_id, doc_name,", "select doc_id, upper(doc_name) as doc_name,"
        )
        created_on = service_state(project)["created_on"]
        update_model(project, "support_search", changed)
        try:
            run_dbt(["run", "--select", "support_search"])
            assert service_state(project)["created_on"] == created_on
            assert "upper(doc_name)" not in service_state(project)["definition"].lower()
            run_dbt(["run", "--select", "support_search", "--full-refresh"])
            assert service_state(project)["created_on"] != created_on
            assert "upper(doc_name)" in service_state(project)["definition"].lower()
        finally:
            update_model(project, "support_search", SERVICE)
            run_dbt(["run", "--select", "support_search", "--full-refresh"])

    def test_index_configuration_change_replaces_with_current_query(self, project):
        created_on = service_state(project)["created_on"]
        changed = (
            SERVICE.replace("attributes=['region']", "attributes=['region', 'doc_name']")
            .replace("target_lag='1 hour'", "target_lag='2 hours'")
            .replace("select doc_id, doc_name,", "select doc_id, upper(doc_name) as doc_name,")
        )
        update_model(project, "support_search", changed)
        try:
            run_dbt(["run", "--select", "support_search"])
            state = service_state(project)
            assert state["created_on"] != created_on
            assert {name.strip() for name in state["attribute_columns"].split(",")} == {
                "REGION",
                "DOC_NAME",
            }
            assert state["target_lag"] == "2 hours"
            assert "upper(doc_name)" in state["definition"].lower()
        finally:
            update_model(project, "support_search", SERVICE)
            run_dbt(["run", "--select", "support_search"])

    def test_incremental_upstream_update_reaches_index(self, project):
        service_created = service_state(project)["created_on"]
        node = get_manifest(project.project_root).nodes["seed.test.support_docs"]
        seed = project.adapter.Relation.create_from(project.adapter.config, node)
        execute(
            project,
            f"update {seed.render()} set body='changed password instructions' where doc_id=1",
        )
        run_dbt(["run", "--select", "support_docs_incremental"])
        run_dbt(["run", "--select", "support_search"])
        execute(
            project,
            f'alter cortex search service {relation(project, "support_search").render()} refresh',
        )
        wait_for_body(project, "changed password instructions")
        assert service_state(project)["created_on"] == service_created

    def test_upstream_replacement_is_reported_and_recovery_rebuilds_service(self, project):
        created_on = service_state(project)["created_on"]
        expected_body = indexed_body(project)
        run_dbt(["run", "--select", "support_docs_incremental", "--full-refresh"])
        source = relation(project, "support_docs_incremental")
        state = execute(
            project,
            f"show tables like '{source.identifier}' in schema {source.include(identifier=False).render()}",
        )
        assert state.rows[0]["change_tracking"] == "OFF"
        with pytest.raises(DbtDatabaseError, match="refresh|Change tracking"):
            execute(
                project,
                f'alter cortex search service {relation(project, "support_search").render()} refresh',
            )
        _, logs = run_dbt_and_capture(["run", "--select", "support_search"], expect_pass=False)
        assert_message_in_logs("indexing error", logs)
        execute(project, f"alter table {source.render()} set change_tracking=true")
        run_dbt(["run", "--select", "support_search", "--full-refresh"])
        assert service_state(project)["created_on"] != created_on
        assert not service_state(project)["indexing_error"]
        assert indexed_body(project) == expected_body

    @pytest.mark.parametrize(
        "options",
        [
            ["--empty"],
            ["--sample", '{"start": "2026-01-01", "end": "2026-01-02"}'],
        ],
    )
    def test_limited_runs_resolve_service_by_name(self, project, options):
        self.check_by_name(project, options)


class TestQuotedCortexSearchRefs(CortexSearchBase):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"quoting": {"identifier": True}}

    @pytest.fixture(scope="class")
    def models(self):
        quoted = SERVICE.replace(
            "materialized='cortex_search_service',",
            "materialized='cortex_search_service', alias='Support Search',",
        )
        return {
            "support_docs_incremental.sql": UPSTREAM,
            "support_search.sql": quoted,
            "support_search_scan.sql": DOWNSTREAM,
        }

    @pytest.mark.parametrize(
        "options",
        [
            ["--empty"],
            ["--sample", '{"start": "2026-01-01", "end": "2026-01-02"}'],
        ],
    )
    def test_quoted_service_in_limited_runs(self, project, options):
        assert service_state(project)["name"] == "Support Search"
        self.check_by_name(project, options)
