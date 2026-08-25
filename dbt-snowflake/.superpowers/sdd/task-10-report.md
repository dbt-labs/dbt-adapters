# Task 10 Report: functional (dbt-YAML) tests for `interactive_table`

## Note on the brief file

`.superpowers/sdd/task-10-brief.md` does not exist in this worktree (`.superpowers/sdd/`
was empty at task start). The task prompt itself embedded the full scenario list inline
("Scenarios required" section), so that was treated as the authoritative brief. No other
brief content was found anywhere on disk.

## v2 (Fusion/Rust) scenario checklist and disposition

Source: `fs` repo, `origin/feat/snowflake-interactive-table`, file
`fs/sa/crates/dbt-loader/tests/materializations/interactive_table.rs` (880 lines, fetched
fresh via `git fetch` + `git show` — not read from any local cache).

| v2 Rust test | Ported? | Where / why |
|---|---|---|
| `no_existing_relation_creates_the_interactive_table` | Yes | `TestBasic` |
| `full_refresh_replaces_rather_than_alters` | Yes (implicitly) | `Changes.test_full_refresh_is_always_successful` covers `--full-refresh` always replacing regardless of on_configuration_change |
| `existing_relation_of_another_type_is_replaced` | **Not ported** | Cross-type relation-type-change (table → interactive_table) is generic relation-swap plumbing already exercised by this repo's own `tests/functional/relation_tests/test_relation_type_change.py` parametrized suite. Adding `interactive_table` to that suite's model matrix is a natural follow-up, but that file is outside Task 10's explicit file list (only `interactive_table_tests/*` + `utils.py` were in scope), so it's deliberately deferred rather than touched here. |
| `existing_interactive_table_with_changes_is_altered` | Yes | `TestTargetLagValueChange` |
| `cluster_by_change_is_replaced_not_altered` | Yes | `TestClusterByChange` |
| `newly_set_target_lag_is_replaced_not_altered` (static→dynamic) | Yes | `TestTargetLagTransitions.test_static_to_dynamic_forces_replace`, plus the symmetric dynamic→static direction added on top (v2's file only tests one direction; the Python-side changeset code comments confirm both 001422 and 001420 are rejected live, so both directions are asserted here) |
| `on_configuration_change_fail_raises` | Yes | `TestChangesFail` |
| `on_configuration_change_continue_skips_the_build` | Yes | `TestChangesContinue` |
| `no_changes_records_a_skip` | Yes | `TestStaticNoOpIdempotency` (static) + implicitly by every `Changes`-family test's `setup_method` calling `assert_changes_are_not_applied` before mutating |
| `warehouse_association_runs_on_a_no_op_build` | Yes | `TestWarehouseAttachDetach.test_adding_warehouse_attaches` runs against an already-existing, otherwise-unchanged table |
| `warehouse_association_does_not_store_a_result` | **Not ported** | Rust-harness-specific mechanic: introspects whether the Jinja `store_result` function was invoked during the association loop. Python's `dbt.tests.util` functional harness has no equivalent hook into `store_result` call counts without mocking internals; this is a unit-test-shaped assertion already covered at that layer by `tests/unit/test_interactive_table_warehouses.py`. |
| `warehouse_association_runs_on_a_create` | Yes | Covered by `TestWarehouseAttachDetach` (attach path) — the create path is exercised by `TestBasic` and is structurally identical for warehouse-sync purposes since the macro runs unconditionally after `interactive_table_get_build_sql`. |
| `no_configured_warehouses_emits_no_association` | Yes | `TestStaticNoOpIdempotency` asserts no `alter warehouse` when `snowflake_interactive_warehouses` is unset |
| `warehouse_association_detaches_a_removed_warehouse` | Yes | `TestWarehouseAttachDetach.test_removing_warehouse_detaches` |
| `warehouse_association_emits_no_detach_when_nothing_is_currently_attached` | Yes (implicit) | `TestWarehouseAttachDetach.test_adding_warehouse_attaches` starts from a table with no warehouses attached and asserts only the attach log line, not a detach one |
| `warehouse_association_detach_is_case_insensitive_against_config` | **Not ported** | This is a property of `describe_interactive_table_warehouses`'s case-folding, already unit-tested exhaustively in `tests/unit/test_interactive_table_warehouses.py::test_case_insensitive_match` and the Jinja-level `TestSyncInteractiveWarehouses::test_case_insensitive_match_does_not_detach_then_reattach`. A live functional re-assertion would require knowing Snowflake's exact casing echo for a lower-cased warehouse identifier, which is unverified and not worth guessing at for this task; deferred to Task 11 if warranted. |
| `warehouse_association_detach_does_not_store_a_result` | **Not ported** | Same `store_result` mechanic as above — not applicable outside the Rust macro-test harness. |
| `no_path_emits_transient_or_refresh` | **Not ported** | Rust-harness-specific global sanity check (scans every executed statement across 4 render paths for the literal substrings `transient`/`refresh`). This is a static SQL-shape check with no live-warehouse dependency; it's better suited to a unit/macro-rendering test than a functional (live-execution) one, and no equivalent macro-rendering harness exists in this Python codebase's functional test layer. Not ported; flagged as a possible unit-test addition outside this task's scope. |

New scenarios in the Python suite **not** present in the v2 Rust file (this repo's own
V1-specific requirements from the brief, not cross-checked against v2 since v2 has no
equivalent):
- `TestCompileValidation` (4 compile-time validations — v2's Rust test harness doesn't
  test model-config validation raising at all; this is new coverage for Task 2's Python
  `parse_relation_config` checks).
- `TestRefreshWarehouseChange` (refresh_warehouse-only alter, distinct from `snowflake_warehouse`).
- `TestInitializationWarehouseChanges` (create/alter/unset trio, mirroring dynamic_table_tests).
- `TestStaticTableNoDiffRegression` (Task 1's `is_dynamic` gate regression, specific to this
  codebase's V1 history — the bug being regression-tested doesn't exist in v2 since v2 was
  built with the gate already in place).

## CompilationError test approach

No existing functional-test precedent in `dbt-snowflake` asserts a `CompilationError` from
a model-config validation specifically, but a directly analogous pattern already exists and
was reused as-is:

- `tests/functional/warehouse_test/test_warehouses.py::TestInvalidConfigWarehouse` —
  `result = run_dbt(["run", ...], expect_pass=False); assert "..." in result[0].message`
- `tests/functional/relation_tests/test_relation_type_change.py` and
  `tests/functional/relation_tests/dynamic_table_tests/test_configuration_changes.py::TestChangesFail`
  both use `run_dbt([...], expect_pass=False)` for an exception raised *inside* a
  materialization macro body during node execution (not a top-level parse-time exception).

Confirmed via `dbt.tests.util.run_dbt` source: it re-raises `res.exception` only when the
*top-level* `dbtRunner.invoke()` call itself raised (e.g. a parse error before any node
runs). A `CompilationError` raised from `SnowflakeInteractiveTableConfig.parse_relation_config`
happens inside `relation.from_config(config.model)`, called from the `create`/`replace`
Jinja macros during a specific node's execution — this is caught per-node by dbt-core's
node executor and surfaces as a failed `RunResult` with `.message` set to the exception
text, not a top-level `res.exception`. This is exactly the same shape as `TestChangesFail`'s
`raise_fail_fast_error` raise, which already works with `expect_pass=False` in this codebase.

Chosen approach: `results = run_dbt(["run", "--select", "<model>"], expect_pass=False)` then
`assert "<substring of the raised message>" in results[0].message`. Each of the 4 (5,
counting the missing/blank cluster_by split into two tests) validations gets its own
`--select`ed model within one `TestCompileValidation` class, so one failing validation
doesn't block collection/execution of the others.

## Files

- `tests/functional/relation_tests/interactive_table_tests/__init__.py` — empty, matches `dynamic_table_tests/__init__.py`.
- `tests/functional/relation_tests/interactive_table_tests/models.py` — model-fixture strings: `SEED`, `INTERACTIVE_TABLE_STATIC`, `INTERACTIVE_TABLE_DYNAMIC` (+ target_lag/refresh_warehouse/cluster_by/init-warehouse/interactive-warehouses variants), and 5 compile-validation-failure models.
- `tests/functional/relation_tests/interactive_table_tests/test_basic.py` — all test classes (see below).
- `tests/functional/utils.py` — added `describe_interactive_table(project, name)`, mirroring `describe_dynamic_table` exactly (calls `snowflake__describe_interactive_table`, indexes `results["interactive_table"]`, returns `SnowflakeInteractiveTableConfig.from_relation_results(results)`).

### `test_basic.py` class summary

| Class | Tests | What it checks |
|---|---|---|
| `TestBasic` | 1 | Smoke: dynamic + static interactive tables both create with `relation_type == "interactive_table"`. |
| `TestCompileValidation` | 5 | Missing/blank `cluster_by`, `table_format: iceberg`, `transient: true`, `target_lag` without a resolvable warehouse — each a `CompilationError` at `dbt run`. |
| `TestTargetLagValueChange` | 1 | `target_lag` value→value change is ALTERed, not replaced; log + `describe_interactive_table` both confirm. |
| `TestRefreshWarehouseChange` | 1 | `refresh_warehouse`-only change is ALTERed, not replaced. |
| `TestInitializationWarehouseChanges` | 3 | Create-with-value / alter-to-new-value / unset-to-none for `snowflake_initialization_warehouse`, all via ALTER. |
| `TestClusterByChange` | 1 | `cluster_by` change forces `create or replace interactive table`, never ALTER. |
| `TestTargetLagTransitions` | 2 | Both dynamic→static and static→dynamic force `create or replace`, never ALTER. |
| `TestStaticTableNoDiffRegression` | 1 | Task 1 regression: project-wide `snowflake_initialization_warehouse` + a static table produces "No configuration changes were identified" — no phantom ALTER/REPLACE. |
| `TestWarehouseAttachDetach` | 2 | Adding a warehouse to `snowflake_interactive_warehouses` fires `alter warehouse ... add tables`; removing it fires `alter warehouse ... drop tables`. |
| `TestStaticNoOpIdempotency` | 1 | Static table + nothing changed + no warehouses configured → second run emits no `create or replace`/`alter interactive table`/`alter warehouse` at all. |
| `Changes` (base, not collected) / `TestChangesApply` / `TestChangesContinue` / `TestChangesFail` | 2/1/1 | `on_configuration_change` apply/continue/fail modes, combining an alterable change (`target_lag`) and a full-refresh-only change (`cluster_by`) in one changeset, mirroring `dynamic_table_tests.test_configuration_changes.Changes` structurally. |

Total: **24 tests collected** (verified via `pytest --collect-only`).

## Verification results

- `HATCH_PYTHON=3.12 hatch run pytest --collect-only tests/functional/relation_tests/interactive_table_tests/` → **24 tests collected**, no errors.
- `HATCH_PYTHON=3.12 hatch run pytest --collect-only tests/functional/` (whole functional suite, sanity check that the `utils.py` edit didn't break anything else) → **495 tests collected**, no errors.
- `HATCH_PYTHON=3.12 hatch run unit-tests` → **353 passed**, 12 warnings (pre-existing `RequestsDependencyWarning`, unrelated). Matches expected baseline.
- `HATCH_PYTHON=3.12 hatch run code-quality` → **black / flake8 / mypy all passed**, along with the pre-commit hooks (yaml check, end-of-file, trailing whitespace, case conflicts, "no dbt-core usage in adapter").

None of the new functional tests were executed against live Snowflake, per instructions —
that is Task 11.

## Concerns / judgment calls

1. **`SNOWFLAKE_TEST_INTERACTIVE_WAREHOUSE` env var is new** — introduced in
   `test_basic.py` (default `"DBT_TESTING_INTERACTIVE"`) for the warehouse attach/detach
   tests, since no such warehouse or env var convention previously existed anywhere in this
   codebase (checked `test.env.example`, `tests/conftest.py`, and all `SNOWFLAKE_TEST_*`
   usages). `test.env.example` was **not** modified, since it wasn't in Task 10's explicit
   file list — Task 11 (live execution) will need either to export this env var pointing at
   a real `WAREHOUSE_TYPE = INTERACTIVE` warehouse, or the default name must exist in the
   test account.
2. **`existing_relation_of_another_type_is_replaced` was not ported** — see table above.
   Extending `test_relation_type_change.py`'s parametrized model matrix to include
   `interactive_table` would be the natural home for this, but that file wasn't in Task 10's
   scope.
3. **Log message text for interactive_table's ALTER path is `"Applying ALTER to:"`**, not
   `"Applying UPDATE SCHEDULER to:"` like dynamic_table's — confirmed by reading
   `src/dbt/include/snowflake/macros/relations/interactive_table/alter.sql` directly (there
   is no scheduler concept on interactive tables). All log-message assertions in the new
   tests were built from reading the actual macro source, not assumed by analogy to
   dynamic_table.
4. All raw-string comparisons against `describe_interactive_table(...)` results (e.g.
   `target_lag == "2 hours"`) follow the exact convention already established in
   `dynamic_table_tests/test_configuration_changes.py` (raw string equality, not the
   internal `_normalized` properties) — this is unverified against a live warehouse for the
   interactive-table-specific readback format (same caveat already documented in
   `relation_configs/interactive_table.py`'s `_normalize_cluster_by` docstring), so a
   mismatch here is plausible and exactly what Task 11 is for.
