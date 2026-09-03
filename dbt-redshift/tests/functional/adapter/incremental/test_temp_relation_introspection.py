"""
How an incremental run describes its temp relation, in each datasharing mode.

redshift__make_temp_relation marks the relation is_temporary, and with `datasharing`
enabled RedshiftAdapter.get_columns_in_relation routes marked relations straight to the
driver. Before that, introspecting a temp relation ran information_schema."columns" and
then, when that came back empty, the legacy query -- whose unbound_views CTE calls
pg_get_late_binding_view_cols(), expanding every late-binding view in the session
(dbt-labs/dbt-adapters#2156). Measured on a datashare consumer, where neither query can ever
match a temp relation:

    information_schema."columns"             46.993s  -> 0 rows
    legacy w/ pg_get_late_binding_view_cols  44.596s  -> 0 rows
    select * from <tmp> limit 0               0.071s  -> the actual answer

With datasharing off the catalog can see temp relations, so information_schema answers on
the first query and the late-binding lookup is never reached. The marker is deliberately
not acted on there, and the test below pins that down: this change is meant to be confined
to datashare consumers, and a future refactor that quietly extended the driver path to
every connection should fail here rather than in someone's warehouse.

These tests do not need a datashare consumer database -- they assert which queries dbt
issues, which is decided by the `datasharing` config alone. The correctness tests that do
need one live in tests/functional/adapter/datashare_consumer/.
"""

import re

from dbt.tests.util import run_dbt, run_dbt_and_capture
import pytest

_MODEL = """
{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='sync_all_columns'
    )
}}

with source_data as (
    select 1 as id, 'aaa'::varchar(50) as field_1, 2.5::numeric(18,2) as field_2
    union all select 2 as id, 'bbb'::varchar(50) as field_1, 3.5::numeric(18,2) as field_2
)

select * from source_data

{% if is_incremental() %}
where id not in (select id from {{ this }})
{% endif %}
"""

_TEMP_IDENTIFIER = re.compile(r"create temporary table\s+\"?(\w*__dbt_tmp\d+)\"?", re.IGNORECASE)


class _TempRelationIntrospection:
    @pytest.fixture(scope="class")
    def models(self):
        return {"incremental_temp_introspection.sql": _MODEL}

    def _incremental_run(self, project):
        """Build the model, then run it again so it takes the incremental path.

        Returns the debug logs of the second run and the temp relation's identifier, so
        assertions can be scoped to the temp relation. The target relation is still
        described from the catalog in the same run, so asserting on query text alone would
        either pass trivially or fail spuriously.
        """
        run_dbt(["run", "--select", "incremental_temp_introspection"])
        results, logs = run_dbt_and_capture(
            ["--debug", "run", "--select", "incremental_temp_introspection"]
        )
        assert len(results) == 1

        match = _TEMP_IDENTIFIER.search(logs)
        assert match, "expected the incremental run to create a temp relation"
        return logs, match.group(1)

    def _lines_mentioning(self, logs, temp_identifier):
        return [line for line in logs.splitlines() if temp_identifier in line]

    def test_column_types_agree_with_the_target_across_runs(self, project):
        """Whatever describes the source, it has to name types the way the target's describer does.

        Otherwise a disagreement reads as a type change and dbt rewrites the column on every
        run. varchar and numeric are here specifically because their reported type carries a
        size. This has to hold in both modes, so it lives on the shared base.
        """
        run_dbt(["run", "--select", "incremental_temp_introspection"])

        for _ in range(2):
            _, logs = run_dbt_and_capture(
                ["--debug", "run", "--select", "incremental_temp_introspection"]
            )
            assert "Data types changed: []" in logs


class TestTempRelationIntrospectionWithDatasharing(_TempRelationIntrospection):
    """Datasharing on: the temp relation is described from the driver, catalog untouched."""

    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target, unique_schema):
        return {
            "test": {
                "outputs": {
                    "default": {**dbt_profile_target, "schema": unique_schema, "datasharing": True}
                },
                "target": "default",
            }
        }

    def test_temp_relation_is_described_from_the_driver(self, project):
        logs, temp_identifier = self._incremental_run(project)

        # The driver probe: cheap, and the only thing that can answer for a temp relation on
        # a datashare consumer.
        assert f'select * from "{temp_identifier}" limit 0' in logs

        # The queries the marker exists to skip.
        for line in self._lines_mentioning(logs, temp_identifier):
            assert "pg_get_late_binding_view_cols" not in line
            assert 'from information_schema."columns"' not in line
            assert "svv_columns" not in line
            assert "SHOW COLUMNS" not in line


class TestTempRelationIntrospectionWithoutDatasharing(_TempRelationIntrospection):
    """Datasharing off: unchanged behavior -- information_schema still describes the temp relation."""

    def test_temp_relation_is_described_from_information_schema(self, project):
        logs, temp_identifier = self._incremental_run(project)

        lines = self._lines_mentioning(logs, temp_identifier)
        assert any('from information_schema."columns"' in line for line in lines)

        # information_schema answers on the first query, so the expensive lookup that
        # motivated this change is still never reached here either.
        for line in lines:
            assert "pg_get_late_binding_view_cols" not in line
