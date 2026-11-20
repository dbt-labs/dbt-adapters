"""Functional tests for BigQuery Lakehouse runtime catalog support.

These run against a real, pre-provisioned lakehouse catalog and are gated on:

- BIGQUERY_TEST_LAKEHOUSE_DATABASE: the GCP project hosting the catalog
- BIGQUERY_TEST_LAKEHOUSE_SCHEMA: an existing `catalog.namespace` the caller may
  create and drop tables in
- BIGQUERY_TEST_LAKEHOUSE_LOCATION (optional): the catalog's location, so the
  framework's scratch dataset (also used for temp-relation spill) is colocated

Namespaces cannot be created from BigQuery, so unlike standard functional tests
these reuse one namespace and keep table names unique per run instead.
"""

import os
import time

import pytest

from dbt.tests.util import run_dbt

_DATABASE = os.getenv("BIGQUERY_TEST_LAKEHOUSE_DATABASE")
_SCHEMA = os.getenv("BIGQUERY_TEST_LAKEHOUSE_SCHEMA")
_LOCATION = os.getenv("BIGQUERY_TEST_LAKEHOUSE_LOCATION")
_SUFFIX = f"{os.getpid()}_{int(time.time())}"

pytestmark = pytest.mark.skipif(
    not (_DATABASE and _SCHEMA),
    reason="BIGQUERY_TEST_LAKEHOUSE_DATABASE and BIGQUERY_TEST_LAKEHOUSE_SCHEMA "
    "must point at a provisioned lakehouse catalog namespace",
)


class _LakehouseBase:
    @pytest.fixture(scope="class")
    def dbt_profile_target(self):
        target = {
            "type": "bigquery",
            "method": "oauth",
            "threads": 2,
            "job_retries": 2,
            "project": _DATABASE,
        }
        if _LOCATION:
            target["location"] = _LOCATION
        return target

    CLEANUP_ALIASES: tuple = ()

    @pytest.fixture(scope="class", autouse=True)
    def cleanup_lakehouse_tables(self, project):
        yield
        for alias in self.CLEANUP_ALIASES:
            project.run_sql(f"drop table if exists `{_DATABASE}.{_SCHEMA}.{alias}`")


_TABLE_ALIAS = f"zz_dbt_lk_tbl_{_SUFFIX}"

MODEL__TABLE = f"""
{{{{ config(
    materialized='table',
    schema='{_SCHEMA}',
    alias='{_TABLE_ALIAS}',
    partition_by={{'field': 'ds', 'data_type': 'date'}},
) }}}}
select 1 as id, date '2026-07-01' as ds
union all
select 2 as id, date '2026-07-02' as ds
"""

SOURCES__YML = f"""
version: 2
sources:
  - name: lakehouse
    database: {_DATABASE}
    schema: {_SCHEMA}
    freshness:
      warn_after: {{count: 480, period: hour}}
    tables:
      - name: {_TABLE_ALIAS}
"""


class TestLakehouseTable(_LakehouseBase):
    CLEANUP_ALIASES = (_TABLE_ALIAS,)

    @pytest.fixture(scope="class")
    def models(self):
        return {"lake_table.sql": MODEL__TABLE, "sources.yml": SOURCES__YML}

    def test_table_lifecycle(self, project):
        # first build
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].node.database == _DATABASE
        assert results[0].node.schema == _SCHEMA

        rows = project.run_sql(
            f"select count(*) as n from `{_DATABASE}.{_SCHEMA}.{_TABLE_ALIAS}`",
            fetch="one",
        )
        assert rows[0] == 2

        # steady-state rebuild: must be a same-spec `create or replace`,
        # never a drop (is_replaceable returns True for lakehouse)
        results = run_dbt(["run"])
        assert len(results) == 1

        # metadata source freshness (tables.get lastModifiedTime path)
        freshness = run_dbt(["source", "freshness"])
        assert freshness[0].status in ("pass", "warn")

        # docs generation skips database-derived enrichment for lakehouse relations
        run_dbt(["docs", "generate"])


_INC_ALIAS = f"zz_dbt_lk_inc_{_SUFFIX}"

MODEL__INCREMENTAL = f"""
{{{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='id',
    schema='{_SCHEMA}',
    alias='{_INC_ALIAS}',
    on_schema_change='append_new_columns',
) }}}}
select 1 as id, 'a' as val
{{% if var('extra_col', false) %}}, 'x' as extra{{% endif %}}
{{% if is_incremental() %}}
union all
select 2 as id, 'b' as val
{{% if var('extra_col', false) %}}, 'y' as extra{{% endif %}}
{{% endif %}}
"""


class TestLakehouseIncrementalMerge(_LakehouseBase):
    CLEANUP_ALIASES = (_INC_ALIAS,)

    @pytest.fixture(scope="class")
    def models(self):
        return {"lake_incremental.sql": MODEL__INCREMENTAL}

    def test_merge_and_schema_evolution(self, project):
        # initial build
        results = run_dbt(["run"])
        assert len(results) == 1
        rows = project.run_sql(
            f"select count(*) as n from `{_DATABASE}.{_SCHEMA}.{_INC_ALIAS}`",
            fetch="one",
        )
        assert rows[0] == 1

        # incremental run adding a row AND a column: exercises the MERGE DML,
        # the temp-relation spill into the scratch dataset, and the
        # on_schema_change ALTER TABLE ADD COLUMN DDL branch
        results = run_dbt(["run", "--vars", "{extra_col: true}"])
        assert len(results) == 1

        rows = project.run_sql(
            f"select count(*) as n, count(extra) as extras "
            f"from `{_DATABASE}.{_SCHEMA}.{_INC_ALIAS}`",
            fetch="one",
        )
        assert rows[0] == 2
        assert rows[1] >= 1


_IO_ALIAS = f"zz_dbt_lk_io_{_SUFFIX}"

MODEL__INSERT_OVERWRITE = f"""
{{{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={{'field': 'ds', 'data_type': 'date'}},
    partitions=["date('2026-07-01')", "date('2026-07-02')"],
    schema='{_SCHEMA}',
    alias='{_IO_ALIAS}',
) }}}}
select 1 as id, date '2026-07-01' as ds
union all
select 2 as id, date '2026-07-02' as ds
{{% if is_incremental() %}}
union all
select 3 as id, date '2026-07-02' as ds
{{% endif %}}
"""


_IOD_ALIAS = f"zz_dbt_lk_iod_{_SUFFIX}"

MODEL__INSERT_OVERWRITE_DYNAMIC = f"""
{{{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={{'field': 'ds', 'data_type': 'date'}},
    schema='{_SCHEMA}',
    alias='{_IOD_ALIAS}',
) }}}}
select 1 as id, date '2026-07-01' as ds
union all
select 2 as id, date '2026-07-02' as ds
{{% if is_incremental() %}}
union all
select 3 as id, date '2026-07-02' as ds
{{% endif %}}
"""


class TestLakehouseInsertOverwrite(_LakehouseBase):
    """insert_overwrite rides MERGE DML (dynamic partitions additionally ride a
    multi-statement script) — verify both live rather than leaving them in the
    gap between guarded and verified."""

    CLEANUP_ALIASES = (_IO_ALIAS, _IOD_ALIAS)

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "lake_insert_overwrite.sql": MODEL__INSERT_OVERWRITE,
            "lake_insert_overwrite_dynamic.sql": MODEL__INSERT_OVERWRITE_DYNAMIC,
        }

    def test_static_insert_overwrite(self, project):
        results = run_dbt(["run", "--select", "lake_insert_overwrite"])
        assert len(results) == 1
        rows = project.run_sql(
            f"select count(*) as n from `{_DATABASE}.{_SCHEMA}.{_IO_ALIAS}`",
            fetch="one",
        )
        assert rows[0] == 2

        # incremental run replaces the listed partitions with the new output
        results = run_dbt(["run", "--select", "lake_insert_overwrite"])
        assert len(results) == 1
        rows = project.run_sql(
            f"select count(*) as n from `{_DATABASE}.{_SCHEMA}.{_IO_ALIAS}`",
            fetch="one",
        )
        assert rows[0] == 3

    def test_dynamic_insert_overwrite(self, project):
        # Build this model independently; test order is not guaranteed.
        results = run_dbt(["run", "--select", "lake_insert_overwrite_dynamic"])
        assert len(results) == 1
        rows = project.run_sql(
            f"select count(*) as n from `{_DATABASE}.{_SCHEMA}.{_IOD_ALIAS}`",
            fetch="one",
        )
        assert rows[0] == 2

        # The incremental run executes the declare + temp table + merge script.
        results = run_dbt(["run", "--select", "lake_insert_overwrite_dynamic"])
        assert len(results) == 1
        rows = project.run_sql(
            f"select count(*) as n from `{_DATABASE}.{_SCHEMA}.{_IOD_ALIAS}`",
            fetch="one",
        )
        assert rows[0] == 3


_SYNC_ALIAS = f"zz_dbt_lk_sync_{_SUFFIX}"

MODEL__SYNC_ALL_COLUMNS = f"""
{{{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='id',
    schema='{_SCHEMA}',
    alias='{_SYNC_ALIAS}',
    on_schema_change='sync_all_columns',
) }}}}
select 1 as id, 'a' as val {{% if not var('drop_col', false) %}}, 'x' as extra{{% endif %}}
{{% if is_incremental() %}}
union all
select 2 as id, 'b' as val {{% if not var('drop_col', false) %}}, 'y' as extra{{% endif %}}
{{% endif %}}
"""


class TestLakehouseSyncAllColumns(_LakehouseBase):
    """sync_all_columns column REMOVAL rides the DDL DROP COLUMN path — the one
    schema-evolution branch the merge test does not reach."""

    CLEANUP_ALIASES = (_SYNC_ALIAS,)

    @pytest.fixture(scope="class")
    def models(self):
        return {"lake_sync.sql": MODEL__SYNC_ALL_COLUMNS}

    def test_sync_drops_removed_column(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1

        # source no longer emits `extra` -> ALTER TABLE ... DROP COLUMN via DDL
        results = run_dbt(["run", "--vars", "{drop_col: true}"])
        assert len(results) == 1

        rows = project.run_sql(
            f"select count(*) as n, count(val) as vals "
            f"from `{_DATABASE}.{_SCHEMA}.{_SYNC_ALIAS}`",
            fetch="one",
        )
        assert rows[0] == 2
        assert rows[1] == 2
        with pytest.raises(Exception):
            project.run_sql(
                f"select extra from `{_DATABASE}.{_SCHEMA}.{_SYNC_ALIAS}` limit 1",
                fetch="one",
            )


_RESPEC_ALIAS = f"zz_dbt_lk_respec_{_SUFFIX}"

MODEL__RESPEC = f"""
{{{{ config(
    materialized='table',
    schema='{_SCHEMA}',
    alias='{_RESPEC_ALIAS}',
    partition_by={{'field': 'ts', 'data_type': 'timestamp', 'granularity': var('grain', 'day')}},
) }}}}
select timestamp '2026-07-01 00:00:00' as ts, 1 as id
"""


class TestLakehouseFullRefreshRecovery(_LakehouseBase):
    """A changed partition spec fails CREATE OR REPLACE server-side, and
    --full-refresh must recover by pre-dropping the Lakehouse table."""

    CLEANUP_ALIASES = (_RESPEC_ALIAS,)

    @pytest.fixture(scope="class")
    def models(self):
        return {"lake_respec.sql": MODEL__RESPEC}

    def test_full_refresh_recovers_partition_spec_change(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1

        # spec change: day -> month; plain CREATE OR REPLACE must fail
        results = run_dbt(["run", "--vars", "{grain: month}"], expect_pass=False)
        assert "different partitioning spec" in results[0].message.lower()

        # --full-refresh pre-drops and recreates with the new spec
        results = run_dbt(["run", "--full-refresh", "--vars", "{grain: month}"])
        assert len(results) == 1
        rows = project.run_sql(
            f"select count(*) as n from `{_DATABASE}.{_SCHEMA}.{_RESPEC_ALIAS}`",
            fetch="one",
        )
        assert rows[0] == 1
