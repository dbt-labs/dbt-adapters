resolves #[ISSUE_NUMBER]
[docs](https://github.com/dbt-labs/docs.getdbt.com/issues/new/choose) dbt-labs/docs.getdbt.com/#[DOCS_ISSUE_NUMBER]

### Problem

When a new column is added to a snapshot source, dbt correctly adds the column to the snapshot 
table via `ALTER TABLE ADD COLUMN`, but all historical rows remain with NULL values for the 
new column. This creates challenges for analytics engineers who need historical data populated 
with current values for query simplicity, without having to implement custom downstream joins.

Currently, users must either:
1. Accept NULL values in historical rows (limiting query usability)
2. Build downstream models that join current source values (added complexity)
3. Implement custom post-snapshot scripts (maintenance burden)

### Solution

This PR adds a configurable option to automatically backfill historical snapshot rows when 
new columns are detected, with comprehensive audit tracking for multiple backfill events over time.

**New Configuration Options:**
- `backfill_new_columns` (bool/string): Enable/disable backfill of historical rows
- `backfill_audit_column` (string): Column name for JSON audit dict (stored as TEXT)
- `var('dbt_snapshot_backfill_enabled')`: Global control via dbt var (behavior flag)

**Key Design Decisions:**
1. **Behavior flag gated**: Feature is disabled by default, requires explicit opt-in via behavior flag
2. **NULL handling**: If source value is NULL, historical row stays NULL but audit JSON is 
   still updated - this allows users to distinguish "never backfilled" vs "backfilled but source was NULL"
3. **JSON audit column**: Single TEXT column containing a JSON dictionary that tracks per-column 
   backfill timestamps in ISO8601 format:
   ```json
   {"tier": "2024-01-30T10:00:00Z", "region": "2024-02-28T10:00:00Z"}
   ```
   - Uses TEXT type (not native JSON) for cross-database compatibility
   - Each backfill merges new entries into existing JSON
   - Enables precise tracking of when each column was backfilled

**Critical Limitation (Documented):**
Backfilled data represents CURRENT source values, not historical point-in-time values. 
This trade-off is clearly documented and users must explicitly opt-in.

**Adapter Support:**
- Postgres, Snowflake, Redshift: Use UPDATE...FROM syntax
- BigQuery: MERGE syntax
- Spark/Databricks: MERGE INTO syntax (requires Delta/Iceberg/Hudi)
- Athena: MERGE syntax (Iceberg tables only)

**Files Changed:**

Core Implementation:
- `dbt-adapters/src/dbt/include/global_project/macros/materializations/snapshots/helpers.sql` - Added backfill macros
- `dbt-adapters/src/dbt/include/global_project/macros/materializations/snapshots/snapshot.sql` - Integrated backfill call

Adapter Overrides:
- `dbt-postgres/src/dbt/include/postgres/macros/materializations/snapshot_merge.sql`
- `dbt-snowflake/src/dbt/include/snowflake/macros/materializations/snapshot.sql`
- `dbt-bigquery/src/dbt/include/bigquery/macros/materializations/snapshot.sql`
- `dbt-redshift/src/dbt/include/redshift/macros/materializations/snapshot_merge.sql`
- `dbt-spark/src/dbt/include/spark/macros/materializations/snapshot.sql`
- `dbt-athena/src/dbt/include/athena/macros/materializations/snapshots/snapshot.sql`

Tests:
- `dbt-tests-adapter/src/dbt/tests/adapter/simple_snapshot/test_snapshot_backfill.py` - Base test classes
- Adapter-specific test files in each adapter's `tests/functional/adapter/` directory

Documentation:
- `docs/guides/snapshot-column-backfill.md` - Feature documentation

**Alternatives Considered:**
1. Dual audit columns (timestamp + comma-list) - rejected; JSON provides per-column timestamps
2. Separate audit table - rejected as overly complex for the use case
3. Per-column audit columns (e.g., `dbt_backfilled_at_tier`) - rejected due to schema proliferation
4. Native JSON column type - rejected due to inconsistent support across databases

### Checklist

- [ ] I have read [the contributing guide](https://github.com/dbt-labs/dbt-core/blob/main/CONTRIBUTING.md) and understand what's expected of me
- [ ] I have run this code in development and it appears to resolve the stated issue
- [ ] This PR includes tests, or tests are not required/relevant for this PR
- [ ] This PR has no interface changes (e.g. macros, cli, logs, json artifacts, config files, adapter interface, etc) or this PR has already received feedback and approval from Product or DX

### Test Plan

| Test ID | Scenario | Validation |
|---------|----------|------------|
| BF-001 | Single column added, backfill enabled | All historical rows have new column populated |
| BF-002 | Multiple columns added at once | All columns populated in single run |
| BF-003 | Sequential column additions | Each backfill only affects new columns |
| BF-004 | Audit JSON column | JSON contains per-column ISO8601 timestamps |
| BF-005 | Audit JSON merges correctly | New columns added to existing JSON dict |
| BF-006 | Composite unique key | Backfill joins correctly on multi-column key |
| BF-007 | Backfill disabled (default) | Historical rows remain NULL |
| BF-008 | Var-based enablement | `var('dbt_snapshot_backfill_enabled')` works |
| BF-009 | Config precedence | Snapshot config overrides var |
| BF-010 | Idempotency | Multiple runs don't cause issues |
| BF-011 | Source NULL values | NULL in source remains NULL, audit columns still updated |
| BF-012 | NULL vs never-backfilled | Can distinguish via audit columns |
| BF-013 | Behavior flag disabled | Feature does nothing when behavior flag is off |
