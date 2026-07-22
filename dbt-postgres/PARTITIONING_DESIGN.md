# Design: PostgreSQL native table partitioning (issue #679)

Scope: full incremental lifecycle — create partitioned structure, and on subsequent
incremental runs ATTACH/create the partitions a batch of data needs.

## 1. Config surface

New `partition_by` config on the `table` and `incremental` materializations, parsed
like `PostgresIndexConfig` and exposed via an `@available` method.

```jinja
{{ config(
    materialized='table',
    partition_by={
      "fields": ["created_at"],       -- one or more columns/expressions
      "method": "range",              -- range | list | hash
      "granularity": "month",         -- range only: hour|day|month|year (drives auto bounds + names)
      "default_partition": true,      -- create a DEFAULT partition to catch overflow rows
      "partitions": [                 -- optional explicit static partitions
        {"name": "p2024", "from": "'2024-01-01'", "to": "'2025-01-01'"}
      ]
    }
) }}
```

```python
# src/dbt/adapters/postgres/impl.py
@dataclass
class PostgresPartitionConfig(dbtClassMixin):
    fields: List[str]
    method: str = "range"                    # validate against range|list|hash
    granularity: Optional[str] = None        # range auto-management
    default_partition: bool = True
    partitions: Optional[List[dict]] = None

    @classmethod
    def parse(cls, raw): ...                  # mirror PostgresIndexConfig.parse, raise PartitionConfigError

@dataclass
class PostgresConfig(AdapterConfig):
    unlogged: Optional[bool] = None
    indexes: Optional[List[PostgresIndexConfig]] = None
    partition_by: Optional[PostgresPartitionConfig] = None   # NEW

# PostgresAdapter
@available
def parse_partition_by(self, raw) -> Optional[PostgresPartitionConfig]:
    return PostgresPartitionConfig.parse(raw)
```

Add `PartitionConfigError` / `PartitionConfigNotDictError` alongside the existing
index errors.

## 2. The hard constraint

Postgres forbids `CREATE TABLE ... AS SELECT` combined with `PARTITION BY`. A
partitioned table must be built in three steps:

1. `CREATE TABLE t (<explicit columns>) PARTITION BY RANGE (created_at);`
2. Create child partitions (+ optional `DEFAULT`).
3. `INSERT INTO t SELECT ...`

So partitioning **must** route through the explicit-column-DDL branch that contract
enforcement already uses in `postgres__create_table_as`
(`get_table_columns_and_constraints()` + `get_select_subquery(sql)`), never the CTAS
branch.

Also: any PRIMARY KEY / UNIQUE constraint on a partitioned table must include every
partition column. When a contract is enforced with `partition_by`, validate this and
raise a clear compiler error.

## 3. New / changed macros

New file `src/dbt/include/postgres/macros/relations/table/partition.sql`:

- `postgres__partition_by_clause(partition_config)` → `partition by range (created_at)`
- `postgres__get_partition_bound_sql(partition, method)` → `for values from (..) to (..)`
  / `for values in (..)` / `for values with (modulus .., remainder ..)`
- `postgres__create_partition(parent_relation, partition)` →
  `create table <child> partition of <parent> for values ...`
- `postgres__create_default_partition(parent_relation)` →
  `create table <child>__default partition of <parent> default`
- `postgres__partition_name(parent_relation, partition)` — deterministic name with the
  63-char truncation logic from `postgres__make_relation_with_suffix` (the overflow bug
  that sank PR #78).
- `adapter.get_partition_range(...)` helper (Python `@available`) to compute
  auto range bounds + names from `granularity` over the batch's min/max of the
  partition column.

Modify `postgres__create_table_as` (`macros/adapters.sql`):

```jinja
{%- set partition_config = adapter.parse_partition_by(config.get('partition_by')) -%}
{% if partition_config is not none %}
  {{ postgres__create_partitioned_table_as(temporary, relation, sql, partition_config) }}
{% else %}
  ... existing behavior ...
{% endif %}
```

`postgres__create_partitioned_table_as`:
1. Render explicit columns (reuse contract column DDL; if no contract, derive columns
   by compiling `sql` into a temp/limit-0 relation via `get_columns_in_relation`).
2. `CREATE TABLE ... PARTITION BY <clause>`.
3. Create each declared partition; create DEFAULT partition if requested.
4. `INSERT INTO relation SELECT ... FROM (sql)`.

## 4. Rename / swap handling

The base `table` and `incremental` materializations build into an intermediate
`__dbt_tmp` relation then `rename_relation(intermediate, target)`. Child partitions are
independent tables whose names embed the intermediate name — after the parent rename
they keep stale `__dbt_tmp` names.

Override `postgres__rename_relation` (or add a post-rename hook) so that when the
relation is partitioned, each child partition is `ALTER TABLE ... RENAME`d from the
intermediate-derived name to the target-derived name. Because partition names are
generated deterministically from the parent relation via `postgres__partition_name`,
the rename set is computable from the parent's `pg_inherits` children.

## 5. Incremental lifecycle (full scope)

Postgres routes incremental through the **base** `incremental` materialization; the
per-strategy SQL lives in `postgres__get_incremental_*_sql`
(`macros/materializations/incremental_strategies.sql`). Two integration points:

1. **First run / full refresh** — `get_create_table_as_sql` already flows to
   `postgres__create_table_as`, so the partitioned-build branch (§3) handles it.

2. **Incremental run** — before appending/merging, ensure partitions exist for the
   incoming batch:
   - Add a `postgres__create_incremental_missing_partitions(target_relation, tmp_relation, partition_config)`
     macro that: queries the target's existing child partitions (`pg_inherits` /
     `pg_class`), computes the partitions the temp data spans (via
     `adapter.get_partition_range` for `range`/`granularity`, or distinct keys for
     `list`), and `CREATE TABLE ... PARTITION OF` for any missing ones. Idempotent.
   - Call it inside each `postgres__get_incremental_*_sql` (default/append/
     delete+insert/microbatch) right before the DML, or once in a small
     postgres-specific wrapper. **`microbatch` is the primary target** — its
     time-bounded batches map directly onto range partitions.
   - `delete+insert` on a partitioned parent works transparently (routed to children);
     no partition-swap optimization in v1, but leave a note for a future
     `insert_overwrite`-by-partition strategy using `DETACH/ATTACH`.

Guardrails:
- Disallow changing `partition_by` on an existing relation without `--full-refresh`
  (detect via describe; raise a clear error) — Postgres can't repartition in place.
- Auto-partition creation must be transactional with the insert so a failed run leaves
  no orphan empty partitions.

## 6. Tests

Unit (`tests/unit/`):
- `PostgresPartitionConfig.parse` happy/error paths (bad method, non-dict, missing
  fields, list without values).
- Bound SQL rendering for range/list/hash.
- Partition-name truncation at the 63-char boundary.

Functional (`tests/functional/`, needs Postgres — see docker cmd in CLAUDE.md):
- `table` materialized, range by month: assert `relkind='p'` in `pg_class`, child
  partitions exist, DEFAULT partition present, row routing correct.
- list and hash methods.
- Contract + partition_by: PK includes partition col passes; PK without it raises.
- Incremental append across two batches spanning a new month → new partition
  auto-created, no duplicates.
- microbatch across day partitions.
- Changing `partition_by` without `--full-refresh` raises; with it, rebuilds.
- Swap/rename: run twice, confirm child partition names track the target relation and
  no `__dbt_tmp` leftovers.

## 7. Capability / docs / changelog

- No new `Capability` enum entry required (it's a config, not a cross-core feature),
  but confirm nothing in dbt-core keys off partition support.
- `changie new` under **Features** referencing #679.
- Redshift extends `PostgresAdapter` but overrides `redshift__create_table_as`, so it
  never reaches the postgres partitioned build path. No guard is being added.

## 8. Git flow & PR sequencing

All work lands on a single integration branch (`feature/postgres-partitioning`) so it
can be tested and merged at once. Each numbered PR is a logical commit/stack on that
branch.

- **PR 0 — functional tests (test-first).** Land the full functional test suite for the
  partitioning behaviors up front. These fail initially and act as the validation
  harness: each later PR turns more of them green.
- **PR 1 — config + parsing** + `@available` + errors + unit tests (no behavior change).
- **PR 2 — `table` partitioned build** (create + default + explicit partitions) +
  rename/swap handling.
- **PR 3 — incremental missing-partition creation** (append/delete+insert/microbatch).
- **PR 4 — contract interaction** + changelog + docs.

## Key files

- `dbt-postgres/src/dbt/adapters/postgres/impl.py` — config, parse, @available
- `dbt-postgres/src/dbt/include/postgres/macros/adapters.sql` — `postgres__create_table_as`
- `dbt-postgres/src/dbt/include/postgres/macros/relations/table/partition.sql` — NEW
- `dbt-postgres/src/dbt/include/postgres/macros/materializations/incremental_strategies.sql`
- `dbt-adapters/.../materializations/models/{table,incremental/incremental}.sql` — reference only
- `dbt-redshift` `redshift__create_table_as` — guard
