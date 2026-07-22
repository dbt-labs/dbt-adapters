"""
Functional tests for PostgreSQL native table partitioning (issue #679).

This is the "PR 0" validation harness: it is landed test-first, so most of these
tests fail until the corresponding implementation PRs land. As each PR merges, more
of these turn green:

  * PR 1 (config/parsing) .......... TestPartitionConfigParsing
  * PR 2 (table build + swap) ...... TestRangePartitionedTable, TestListPartitionedTable,
                                     TestHashPartitionedTable, TestDefaultPartition,
                                     TestPartitionSwap
  * PR 3 (incremental lifecycle) ... TestIncrementalPartitionCreation,
                                     TestMicrobatchPartitioning, TestRepartitionGuard
  * PR 4 (contracts) ............... TestPartitionContract

Requires a live Postgres (see the docker command in the repo dev guide). Some
strategies (merge/microbatch) require Postgres >= 15.
"""

from dbt.tests.util import run_dbt, run_dbt_and_capture
import pytest


# -- helpers ----------------------------------------------------------------------


def _relkind(project, relname):
    """'p' == partitioned table, 'r' == ordinary table, None == missing."""
    sql = """
        select c.relkind
        from pg_class c
        join pg_namespace n on n.oid = c.relnamespace
        where c.relname = '{}' and n.nspname = '{}'
    """.format(
        relname, project.test_schema
    )
    result = project.run_sql(sql, fetch="one")
    return result[0] if result else None


def _child_partitions(project, parent):
    """Names of the child partitions attached to `parent`, sorted."""
    sql = """
        select c.relname
        from pg_inherits i
        join pg_class c on c.oid = i.inhrelid
        join pg_class p on p.oid = i.inhparent
        join pg_namespace n on n.oid = p.relnamespace
        where p.relname = '{}' and n.nspname = '{}'
        order by c.relname
    """.format(
        parent, project.test_schema
    )
    return [row[0] for row in project.run_sql(sql, fetch="all")]


def _rowcount(project, relname):
    sql = 'select count(*) from "{}"."{}"'.format(project.test_schema, relname)
    return project.run_sql(sql, fetch="one")[0]


def _relnames_like(project, pattern):
    sql = """
        select c.relname
        from pg_class c
        join pg_namespace n on n.oid = c.relnamespace
        where c.relname like '{}' and n.nspname = '{}'
    """.format(
        pattern, project.test_schema
    )
    return [row[0] for row in project.run_sql(sql, fetch="all")]


# -- fixtures / model SQL ---------------------------------------------------------

range_month_sql = """
{{ config(
    materialized='table',
    partition_by={
      "fields": ["created_at"],
      "method": "range",
      "granularity": "month",
      "default_partition": true
    }
) }}

select 1 as id, '2024-01-15'::timestamp as created_at
union all
select 2 as id, '2024-02-10'::timestamp as created_at
union all
select 3 as id, '2024-03-05'::timestamp as created_at
"""

list_sql = """
{{ config(
    materialized='table',
    partition_by={
      "fields": ["region"],
      "method": "list",
      "partitions": [
        {"name": "p_us", "values": ["'us'"]},
        {"name": "p_eu", "values": ["'eu'"]}
      ],
      "default_partition": true
    }
) }}

select 1 as id, 'us' as region
union all
select 2 as id, 'eu' as region
union all
select 3 as id, 'apac' as region
"""

hash_sql = """
{{ config(
    materialized='table',
    partition_by={
      "fields": ["id"],
      "method": "hash",
      "partitions": [
        {"name": "p0", "modulus": 2, "remainder": 0},
        {"name": "p1", "modulus": 2, "remainder": 1}
      ]
    }
) }}

select generate_series(1, 10) as id
"""


class TestPartitionConfigParsing:
    """PR 1: parsing/validation only — no run required for the error cases."""

    bad_method_sql = """
    {{ config(materialized='table',
              partition_by={"fields": ["id"], "method": "nonsense"}) }}
    select 1 as id
    """

    not_dict_sql = """
    {{ config(materialized='table', partition_by="created_at") }}
    select 1 as id, now() as created_at
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "bad_method.sql": self.bad_method_sql,
            "not_dict.sql": self.not_dict_sql,
        }

    def test_invalid_method_raises(self, project):
        _, out = run_dbt_and_capture(["run", "--select", "bad_method"], expect_pass=False)
        assert "method" in out.lower()

    def test_non_dict_partition_by_raises(self, project):
        _, out = run_dbt_and_capture(["run", "--select", "not_dict"], expect_pass=False)
        assert "partition" in out.lower()


class TestRangePartitionedTable:
    """PR 2."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"range_month.sql": range_month_sql}

    def test_creates_partitioned_table(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1

        # parent is a partitioned table
        assert _relkind(project, "range_month") == "p"

        # child partitions exist (one per month present + default)
        children = _child_partitions(project, "range_month")
        assert len(children) >= 3

        # all rows are routed and readable through the parent
        assert _rowcount(project, "range_month") == 3

    def test_rebuild_is_idempotent(self, project):
        run_dbt(["run"])
        run_dbt(["run"])
        assert _relkind(project, "range_month") == "p"
        assert _rowcount(project, "range_month") == 3


class TestListPartitionedTable:
    """PR 2."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"list_model.sql": list_sql}

    def test_list_partitions(self, project):
        assert len(run_dbt(["run"])) == 1
        assert _relkind(project, "list_model") == "p"
        # us + eu + default catches 'apac'
        assert _rowcount(project, "list_model") == 3


class TestHashPartitionedTable:
    """PR 2."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"hash_model.sql": hash_sql}

    def test_hash_partitions(self, project):
        assert len(run_dbt(["run"])) == 1
        assert _relkind(project, "hash_model") == "p"
        assert len(_child_partitions(project, "hash_model")) == 2
        assert _rowcount(project, "hash_model") == 10


class TestDefaultPartition:
    """PR 2: rows outside every declared partition land in DEFAULT, not an error."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"list_model.sql": list_sql}

    def test_overflow_row_routed_to_default(self, project):
        # 'apac' matches neither p_us nor p_eu; must not raise, must be retained
        assert len(run_dbt(["run"])) == 1
        got = project.run_sql(
            'select region from "{}"."list_model" where region = \'apac\''.format(
                project.test_schema
            ),
            fetch="all",
        )
        assert [r[0] for r in got] == ["apac"]


class TestPartitionSwap:
    """PR 2: after the intermediate->target rename, no __dbt_tmp children linger."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"range_month.sql": range_month_sql}

    def test_no_tmp_leftovers(self, project):
        run_dbt(["run"])
        run_dbt(["run"])  # second run exercises the backup/swap path
        assert _relnames_like(project, "%__dbt_tmp%") == []
        assert _relnames_like(project, "%__dbt_backup%") == []
        # every child belongs to the final target name
        for child in _child_partitions(project, "range_month"):
            assert child.startswith("range_month")


# -- incremental ------------------------------------------------------------------

incremental_range_sql = """
{{ config(
    materialized='incremental',
    incremental_strategy='append',
    partition_by={
      "fields": ["created_at"],
      "method": "range",
      "granularity": "month",
      "default_partition": true
    }
) }}

select id, created_at from "{{ this.schema }}"."seed_events"
{% if is_incremental() %}
  where created_at > (select max(created_at) from {{ this }})
{% endif %}
"""


class TestIncrementalPartitionCreation:
    """PR 3: a second batch that spans a new month auto-creates the partition."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"inc_model.sql": incremental_range_sql}

    def _seed(self, project, rows):
        schema = project.test_schema
        project.run_sql(
            'create table if not exists "{}"."seed_events" (id int, created_at timestamp)'.format(
                schema
            )
        )
        project.run_sql('truncate "{}"."seed_events"'.format(schema))
        values = ",".join("({}, '{}'::timestamp)".format(i, ts) for i, ts in rows)
        project.run_sql('insert into "{}"."seed_events" values {}'.format(schema, values))

    def test_new_partition_added_on_incremental(self, project):
        self._seed(project, [(1, "2024-01-05"), (2, "2024-01-20")])
        run_dbt(["run", "--select", "inc_model"])
        first = set(_child_partitions(project, "inc_model"))

        # a later batch in a new month
        self._seed(
            project,
            [(1, "2024-01-05"), (2, "2024-01-20"), (3, "2024-02-11")],
        )
        run_dbt(["run", "--select", "inc_model"])
        second = set(_child_partitions(project, "inc_model"))

        assert second - first, "expected a new month partition to be created"
        assert _rowcount(project, "inc_model") == 3  # no duplicates


microbatch_sql = """
{{ config(
    materialized='incremental',
    incremental_strategy='microbatch',
    event_time='created_at',
    batch_size='day',
    begin='2024-01-01',
    unique_key='id',
    partition_by={
      "fields": ["created_at"],
      "method": "range",
      "granularity": "day",
      "default_partition": true
    }
) }}

select id, created_at from "{{ this.schema }}"."seed_events"
"""


def _skip_if_pg_lt_15(project):
    version = int(project.run_sql("show server_version_num", fetch="one")[0])
    if version < 150000:
        pytest.skip("microbatch/merge requires Postgres >= 15")


class TestMicrobatchPartitioning:
    """PR 3: microbatch day-batches map onto day range partitions. Postgres >= 15."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"mb_model.sql": microbatch_sql}

    def test_microbatch_creates_day_partitions(self, project):
        _skip_if_pg_lt_15(project)
        schema = project.test_schema
        project.run_sql(
            'create table if not exists "{}"."seed_events" (id int, created_at timestamp)'.format(
                schema
            )
        )
        project.run_sql('truncate "{}"."seed_events"'.format(schema))
        project.run_sql(
            'insert into "{}"."seed_events" values '
            "(1, '2024-01-01'::timestamp), "
            "(2, '2024-01-02'::timestamp), "
            "(3, '2024-01-03'::timestamp)".format(schema)
        )
        run_dbt(["run", "--select", "mb_model"])
        assert _relkind(project, "mb_model") == "p"
        assert len(_child_partitions(project, "mb_model")) >= 3


class TestRepartitionGuard:
    """PR 3: changing partition_by on an existing incremental relation needs --full-refresh."""

    v1 = """
    {{ config(materialized='incremental', incremental_strategy='append',
              partition_by={"fields": ["created_at"], "method": "range",
                            "granularity": "month", "default_partition": true}) }}
    select 1 as id, '2024-01-15'::timestamp as created_at
    """
    v2 = """
    {{ config(materialized='incremental', incremental_strategy='append',
              partition_by={"fields": ["id"], "method": "hash",
                            "partitions": [{"name": "p0", "modulus": 1, "remainder": 0}]}) }}
    select 1 as id, '2024-01-15'::timestamp as created_at
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {"model.sql": self.v1}

    def test_changing_partition_by_requires_full_refresh(self, project, project_root):
        run_dbt(["run"])
        # rewrite the model to a different partition scheme
        with open(f"{project_root}/models/model.sql", "w") as f:
            f.write(self.v2)
        _, out = run_dbt_and_capture(["run"], expect_pass=False)
        assert "full-refresh" in out.lower() or "full refresh" in out.lower()
        # with a full refresh it rebuilds cleanly
        assert len(run_dbt(["run", "--full-refresh"])) == 1


class TestPartitionContract:
    """PR 4: an enforced contract PK must include every partition column."""

    schema_yml = """
version: 2
models:
  - name: contract_ok
    config:
      contract:
        enforced: true
    constraints:
      - type: primary_key
        columns: [id, created_at]
    columns:
      - name: id
        data_type: integer
      - name: created_at
        data_type: timestamp
  - name: contract_bad
    config:
      contract:
        enforced: true
    constraints:
      - type: primary_key
        columns: [id]
    columns:
      - name: id
        data_type: integer
      - name: created_at
        data_type: timestamp
"""

    model = """
{{ config(materialized='table',
          partition_by={"fields": ["created_at"], "method": "range",
                        "granularity": "month", "default_partition": true}) }}
select 1 as id, '2024-01-15'::timestamp as created_at
"""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": self.schema_yml,
            "contract_ok.sql": self.model,
            "contract_bad.sql": self.model,
        }

    def test_pk_including_partition_col_ok(self, project):
        assert len(run_dbt(["run", "--select", "contract_ok"])) == 1
        assert _relkind(project, "contract_ok") == "p"

    def test_pk_missing_partition_col_raises(self, project):
        _, out = run_dbt_and_capture(["run", "--select", "contract_bad"], expect_pass=False)
        assert "partition" in out.lower()
