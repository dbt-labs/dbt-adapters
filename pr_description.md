### Problem

BigQuery materialized views scramble the `cluster_by` column order ([#586](https://github.com/dbt-labs/dbt-adapters/issues/586)).

`BigQueryClusterConfig.fields` uses `FrozenSet[str]`, which is unordered. When a user specifies `cluster_by=["portal", "cu_name", "rt_name", "ids_in_container"]`, converting to a `frozenset` loses the ordering, producing DDL like `CLUSTER BY rt_name, ids_in_container, portal, cu_name` instead of respecting the user-specified order.

### Solution

Changed `BigQueryClusterConfig.fields` from `FrozenSet[str]` to `Tuple[str, ...]` so that the user-specified column order is preserved through to DDL rendering. This follows the same pattern used by the Redshift adapter's `RedshiftSortConfig`, which already uses `Tuple[str]` for its `sortkey` field.

To avoid introducing a behavioral change in change detection (where `frozenset` compared order-insensitively but `tuple` would compare order-sensitively), a `_cluster_config_has_changed()` helper method was added to `BigQueryRelation` that uses `sorted()` comparison. This means reordering cluster columns alone won't trigger a needless full refresh — matching the previous `frozenset` behavior — while still preserving order in the generated SQL.

**Changes:**
- **`_cluster.py`**: `FrozenSet[str]` → `Tuple[str, ...]`, `frozenset()` → `tuple()` in both `parse_relation_config()` and `parse_bq_table()`
- **`relation.py`**: Added `_cluster_config_has_changed()` static method with sorted comparison; used it in `materialized_view_config_changeset()` instead of direct `!=`
- **Tests**: Updated `frozenset` assertions to `tuple` in functional tests; added unit tests covering the sorted comparison logic (same fields in different order, different fields, None transitions)
