"""
Incremental on_schema_change against a connection pointed directly at a datashare consumer
database -- the configuration that reproduces dbt-labs/dbt-adapters#1947 and #1991.

The existing coverage passes because it never exercises this arrangement:

    | test                                       | connection dbname | model database        |
    |--------------------------------------------|-------------------|-----------------------|
    | TestIncrementalOnSchemaChangeWithDatasharing | default           | same as connection    |
    | TestIncrementalCrossDatabase*                | default           | cross-db via +database|
    | these tests                                  | consumer database | same as connection    |

In the last row, temporary relations are created and stay queryable but are absent from
information_schema.columns, pg_attribute and svv_columns, so column introspection returns
nothing. Before the driver-based fallback, sync_all_columns read that as "the source has no
columns" and dropped every column in the target.
"""

from dbt.tests.adapter.incremental.test_incremental_on_schema_change import (
    BaseIncrementalOnSchemaChange,
)
from dbt.tests.util import get_connection, run_dbt
import pytest

from tests.functional.adapter.datashare_consumer.fixtures import DatashareConsumerMixin


class TestIncrementalOnSchemaChangeDatashareConsumer(
    DatashareConsumerMixin, BaseIncrementalOnSchemaChange
):
    """The full on_schema_change suite, run against a datashare consumer database."""


_MODEL_SYNC_ALL_COLUMNS = """
{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='sync_all_columns'
    )
}}

with source_data as (
    select 1 as id, 'aaa' as field_1, 2.5::numeric(18,2) as field_2, 'x'::char(5) as field_3
    union all select 2 as id, 'bbb' as field_1, 3.5::numeric(18,2) as field_2, 'y'::char(5) as field_3
)

select * from source_data

{% if is_incremental() %}
where id not in (select id from {{ this }})
{% endif %}
"""


class TestIncrementalTempRelationColumnsPreserved(DatashareConsumerMixin):
    """Targeted regression test: an incremental run must not drop the target's columns.

    Also guards the type mapping: because the source relation is described from the driver
    and the target from SHOW COLUMNS, any disagreement in reported data types shows up as a
    perpetual schema change. Running twice and asserting a stable column set catches that --
    char and numeric columns are included specifically because they are the ones whose
    reported type carries a size.
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {"incremental_sync_all_columns.sql": _MODEL_SYNC_ALL_COLUMNS}

    def _columns(self, project):
        relation = project.adapter.Relation.create(
            database=project.database,
            schema=project.test_schema,
            identifier="incremental_sync_all_columns",
        )
        with get_connection(project.adapter):
            return project.adapter.get_columns_in_relation(relation)

    def test_columns_survive_incremental_run(self, project):
        run_dbt(["run", "--select", "incremental_sync_all_columns"])
        before = self._columns(project)
        assert [c.name for c in before] == ["id", "field_1", "field_2", "field_3"]

        run_dbt(["run", "--select", "incremental_sync_all_columns"])
        after = self._columns(project)

        assert [c.name for c in after] == ["id", "field_1", "field_2", "field_3"]
        # Data types must be stable too, otherwise the driver-described source and the
        # SHOW COLUMNS-described target disagree and dbt alters the column type every run.
        assert [c.dtype for c in after] == [c.dtype for c in before]

    def test_third_run_is_still_stable(self, project):
        """A type-mapping mismatch would keep re-triggering; a third run pins that down."""
        run_dbt(["run", "--select", "incremental_sync_all_columns"])
        run_dbt(["run", "--select", "incremental_sync_all_columns"])
        first = self._columns(project)
        run_dbt(["run", "--select", "incremental_sync_all_columns"])
        assert [(c.name, c.dtype) for c in self._columns(project)] == [
            (c.name, c.dtype) for c in first
        ]
