# Snowflake `interactive_table` (v1/Python) — Finish the retry PR

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to
> implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Take the `worktree-snowflake-interactive-table-v1` branch (10 commits, config/changeset
layer only, no macros, no adapter describe method) to a PR-ready state: adapter method, macros,
validations, tests. Live-Snowflake verification and PR creation are explicitly **excluded** from
subagent execution — both are supervised/human steps, see Tasks 11 and 13.

**Architecture:** Mirror the existing `dynamic_table` materialization's file layout and dispatch
pattern (`materializations/interactive_table.sql` + `relations/interactive_table/*.sql`), but
without an ALTER path — Snowflake's `ALTER TABLE` support for interactive tables does not cover
`CLUSTER BY`, `TARGET_LAG`, or warehouse changes (confirmed against
`docs.snowflake.com/en/sql-reference/sql/alter-table`, "Usage notes: General" — supported
operations for interactive tables are limited to rename, column comments, several policy types,
storage lifecycle policy, and tags). Every meaningful config change other than rename requires
`CREATE OR REPLACE`, matching the original community PR (#1798 → #2042)'s design, not
`dynamic_table`'s ALTER-capable one.

**Tech Stack:** Python (dbt-adapters `SQLAdapter` framework), Jinja macros, `agate` for Snowflake
`SHOW`-result parsing, `pytest` for unit and functional tests.

## Global Constraints

- **`ALTER INTERACTIVE TABLE` is a real, separate statement not documented on Snowflake's general
  `ALTER TABLE` reference page** — confirmed live against account `ktb38830` on 2026-08-25 (see
  `dbt-adapters-snowflake-interactive-table-v1-retry.md`'s progress log for the full transcript).
  Do not trust the `ALTER TABLE` doc page's interactive-table coverage; it is incomplete. Confirmed
  behavior:
  - `ALTER INTERACTIVE TABLE t SET TARGET_LAG = '<duration>'` — **succeeds** for a value-to-value
    change on an already-dynamic table.
  - `ALTER INTERACTIVE TABLE t SET WAREHOUSE = <wh>` — **succeeds**, updates the `refresh_warehouse`
    readback column. The property name is `WAREHOUSE` (matching the `CREATE` clause name), NOT
    `REFRESH_WAREHOUSE` — tried, rejected with "invalid property 'REFRESH_WAREHOUSE' for
    'INTERACTIVE_TABLE'" (001420).
  - `ALTER INTERACTIVE TABLE t REFRESH` and `SUSPEND`/`RESUME` — all succeed.
  - `ALTER INTERACTIVE TABLE t CLUSTER BY (...)` — **real syntax error** ("unexpected 'CLUSTER'").
    No ALTER path for `cluster_by`; every change requires `CREATE OR REPLACE`.
  - `ALTER INTERACTIVE TABLE t UNSET TARGET_LAG` (dynamic → static): rejected, "invalid value
    'null' for property 'TARGET_LAG'", **error 001422**.
  - `ALTER INTERACTIVE TABLE t SET TARGET_LAG = ...` on an already-static table (static →
    dynamic): rejected, "invalid property 'TARGET_LAG' for 'TABLE'", **error 001420**.
  - Net rule: **`target_lag` and `refresh_warehouse` value-to-value changes ALTER in place;
    `cluster_by` changes and any dynamic↔static transition require `CREATE OR REPLACE`.** This
    matches the config layer's existing `requires_full_refresh` values exactly as already written
    (Task 1 below does NOT need to change `SnowflakeInteractiveTableRefreshWarehouseConfigChange` —
    it was already correct) — only fix the two error-code citations in the existing comments
    (dynamic→static is 001422, not 001420 as currently written) and build the ALTER macro that was
    previously missing.
- **`cluster_by` is mandatory.** Snowflake's `CREATE INTERACTIVE TABLE` docs state "the CLUSTER BY
  clause is required for all interactive tables." Must be validated at compile time, not left to a
  runtime Snowflake syntax error.
- **`initialization_warehouse` is not a controllable config.** Per `SHOW INTERACTIVE TABLES`'s own
  column documentation: "Warehouse used for the initial population of a dynamic interactive table.
  **Returns an empty string after initial population is complete.**" There is no
  `INITIALIZATION_WAREHOUSE` clause anywhere in `CREATE INTERACTIVE TABLE`'s syntax. It must never
  be compared for changes — doing so would show a false "changed" diff on every run after the
  first, forever.
- **`transient=true` and `table_format=iceberg` are not supported** — absent from
  `CREATE INTERACTIVE TABLE`'s documented syntax entirely. Must be rejected with a clear
  compile-time error (matching the `fs`/v2 Fusion port's equivalent validation), not left to fail
  with an opaque Snowflake syntax error at execution time.
- **`target_lag` requires a resolvable warehouse** (`refresh_warehouse` or `snowflake_warehouse`) —
  validate at compile time.
- **Warehouse attachment (`snowflake_interactive_warehouses`) must support both attach and
  detach**, not attach-only (the original PR #2042 never detaches; the v2/Fusion port does both).
  This never contributes to the main config changeset — attaching/detaching never triggers a
  rebuild.
- **Model tiers for every subagent dispatch in this plan**: `haiku` for read/fetch/mechanical
  edits (single-file, pattern-copy work), `sonnet` for integration/judgment tasks (multi-file
  coordination, reconciling existing code), `opus` for every review — both per-task and the final
  whole-branch review.
- Every task must run the affected test file(s) and report **actual pass counts** (`N passed`),
  never a bare exit code — `hatch run unit-tests` in this repo silently exits 0 having run zero
  tests unless `HATCH_PYTHON=3.12` is set (the default env's `ddtrace==2.3.0` fails to build on
  newer Python and the failure is swallowed). Always run as:
  `HATCH_PYTHON=3.12 hatch run unit-tests -- <path> -v` from the `dbt-snowflake/` directory.

---

### Task 1: Fix the missing `is_dynamic` gate on `initialization_warehouse`, and correct two error-code comments

**Files:**
- Modify: `dbt-snowflake/src/dbt/adapters/snowflake/relation_configs/interactive_table.py:304-306` (comment only)
- Modify: `dbt-snowflake/src/dbt/adapters/snowflake/relation.py:251-260` (`interactive_table_config_changeset`)
- Modify: `dbt-snowflake/tests/unit/test_interactive_table_config.py`

**Context:** Two live probes against account `ktb38830` (2026-08-25) — the second specifically to
correct a gap in the first (the first probe only observed the *default*, never-explicitly-set
behavior of `initialization_warehouse`, which happened to read back empty and led to the wrong
conclusion that the field isn't real; the second explicitly set it via both `CREATE ...
INITIALIZATION_WAREHOUSE = wh` and `ALTER INTERACTIVE TABLE t SET/UNSET INITIALIZATION_WAREHOUSE`,
both of which succeeded and persisted correctly on readback) — settled the following:
1. `SnowflakeInteractiveTableRefreshWarehouseConfigChange.requires_full_refresh -> False`
   (interactive_table.py:328) and `SnowflakeInteractiveTableInitializationWarehouseConfigChange
   .requires_full_refresh -> False` (interactive_table.py:337) are **both already correct** —
   `ALTER INTERACTIVE TABLE t SET WAREHOUSE = ...` and `SET/UNSET INITIALIZATION_WAREHOUSE` both
   succeed live. No change needed to either class. `snowflake_initialization_warehouse` stays a
   real, user-settable config — do NOT remove it from `parse_relation_config` or the changeset.
2. The **real bug**: `relation.py`'s `interactive_table_config_changeset` gates the
   `refresh_warehouse` comparison on `new.target_lag_normalized is not None` (lines 233-243,
   already correct — a static table has no refresh warehouse to compare) but does **not** apply
   the same gate to the `snowflake_initialization_warehouse` comparison a few lines below (251-260,
   compares unconditionally). Since a static interactive table presumably rejects
   `INITIALIZATION_WAREHOUSE` the same way it rejects `WAREHOUSE`/`TARGET_LAG` (matching v2's own
   `initialization_warehouse_is_inert` gate in
   `fs/sa/crates/dbt-adapter/src/relation/snowflake/config/components/interactive_table_initialization_warehouse.rs`,
   which drops the value whenever `target_lag.is_none()`), a static table with a project-wide
   `snowflake_initialization_warehouse` set would show a phantom "changed" diff on every run,
   forever, without this gate. Fix: mirror `refresh_warehouse`'s exact gating pattern.
3. The existing code comments cite the wrong error code for the dynamic→static transition:
   `ALTER INTERACTIVE TABLE t UNSET TARGET_LAG` on a live dynamic table returned **001422**
   ("invalid value 'null' for property 'TARGET_LAG'"), not 001420 as
   `SnowflakeInteractiveTableTargetLagConfigChange`'s comment (interactive_table.py:304-306)
   currently states. 001420 is what the static→dynamic direction returns instead
   (`SET TARGET_LAG` on an already-static table: "invalid property 'TARGET_LAG' for 'TABLE'").
   Fix the comment to cite the correct code for each direction; no behavior change needed since
   both directions already correctly force full refresh.

**Interfaces:**
- Consumes: nothing new.
- Produces: `SnowflakeInteractiveTableConfigChangeset` keeps all 4 fields unchanged. The only
  behavioral change is that `snowflake_initialization_warehouse` no longer produces a spurious
  changeset entry for a static table.

- [ ] **Step 1: Fix the error-code comment on `SnowflakeInteractiveTableTargetLagConfigChange`**

```python
@dataclass(frozen=True, eq=True, unsafe_hash=True)
class SnowflakeInteractiveTableTargetLagConfigChange(RelationConfigChange):
    context: Optional[str] = None

    @property
    def requires_full_refresh(self) -> bool:
        # Only a value-to-value change is alterable via `ALTER INTERACTIVE TABLE
        # ... SET TARGET_LAG`. Both transitions must rebuild: unsetting a lag
        # (dynamic -> static) is rejected with "invalid value 'null' for
        # property 'TARGET_LAG'" (001422); setting one on an already-static
        # table (static -> dynamic) is rejected with "invalid property
        # 'TARGET_LAG' for 'TABLE'" (001420). Both confirmed live against
        # ktb38830, 2026-08-25.
        return self.action != RelationConfigChangeAction.alter
```

- [ ] **Step 2: Gate the `snowflake_initialization_warehouse` comparison on `is_dynamic`, matching
  `refresh_warehouse`'s existing pattern exactly**

Current (buggy — no gate):

```python
        if (
            new.snowflake_initialization_warehouse_normalized
            != existing.snowflake_initialization_warehouse_normalized
        ):
            changeset.snowflake_initialization_warehouse = (
                SnowflakeInteractiveTableInitializationWarehouseConfigChange(
                    action=RelationConfigChangeAction.alter,  # type:ignore
                    context=new.snowflake_initialization_warehouse,
                )
            )
```

Fixed, mirroring the `refresh_warehouse` block immediately above it (lines 233-249):

```python
        # Snowflake only accepts (and only reports back) an initialization
        # warehouse when the table is dynamic -- same reasoning as
        # `refresh_warehouse` above. Gate on the desired side, not `existing`,
        # which must stay whatever Snowflake reported.
        if new.target_lag_normalized is not None:
            desired_init_warehouse = new.snowflake_initialization_warehouse
            desired_init_warehouse_normalized = new.snowflake_initialization_warehouse_normalized
        else:
            desired_init_warehouse = None
            desired_init_warehouse_normalized = None

        if desired_init_warehouse_normalized != existing.snowflake_initialization_warehouse_normalized:
            changeset.snowflake_initialization_warehouse = (
                SnowflakeInteractiveTableInitializationWarehouseConfigChange(
                    action=RelationConfigChangeAction.alter,  # type:ignore
                    context=desired_init_warehouse,
                )
            )
```

- [ ] **Step 3: Write the failing test first**

```python
def test_static_table_with_project_wide_init_warehouse_produces_no_change():
    # A static table (no target_lag) has no initialization warehouse concept;
    # a project-wide `snowflake_initialization_warehouse` must not phantom-diff it.
    existing = SnowflakeInteractiveTableConfig(target_lag=None, snowflake_initialization_warehouse=None)
    new = SnowflakeInteractiveTableConfig(target_lag=None, snowflake_initialization_warehouse="some_wh")
    changeset = SnowflakeRelation.interactive_table_config_changeset(..., ...)  # adapt to the real call signature used elsewhere in this test file
    assert changeset is None or changeset.snowflake_initialization_warehouse is None
```

Run it first and confirm it currently FAILS (produces a spurious change) before applying Step 2 —
this is the regression test proving the bug existed. Adapt the exact construction pattern
(`relation_results`/`relation_config` fixtures) from whatever the neighboring
`refresh_warehouse`-gating test in this same file already uses — do not invent a new fixture
shape.

- [ ] **Step 4: Run tests and report actual pass count**

```bash
cd dbt-snowflake && HATCH_PYTHON=3.12 hatch run unit-tests -- tests/unit/test_interactive_table_config.py -v
```
Expected: all tests pass, with an explicit `N passed` line quoted in the report.

- [ ] **Step 5: Commit**

```bash
git add dbt-snowflake/src/dbt/adapters/snowflake/relation_configs/interactive_table.py \
        dbt-snowflake/src/dbt/adapters/snowflake/relation.py \
        dbt-snowflake/tests/unit/test_interactive_table_config.py
git commit -m "fix: gate initialization_warehouse comparison on is_dynamic; correct error-code comments"
```

---

### Task 2: Add compile-time validations

**Files:**
- Modify: `dbt-snowflake/src/dbt/adapters/snowflake/relation_configs/interactive_table.py:251-264` (`parse_relation_config`)
- Modify: `dbt-snowflake/tests/unit/test_interactive_table_config.py`

**Context:** Four real Snowflake constraints are currently unenforced, meaning a bad config only
fails with an opaque Snowflake syntax error at execution time instead of a clear dbt compile error:
(1) `cluster_by` is required and non-blank; (2) `table_format: iceberg` isn't supported;
(3) `transient: true` isn't supported; (4) `target_lag` requires a resolvable warehouse. All four
are already validated in the v2/Fusion port (`fs` PR #12664) for the same underlying reasons —
this task ports the same checks into the Python parse step, which is where PR #2042 originally did
equivalent validation (see `parse_relation_config`'s docstring history) before this retry's version
deferred it.

**Interfaces:**
- Consumes: `RelationConfig.config.extra`, `RelationConfig.compiled_code` — same as
  `parse_relation_config` already does.
- Produces: raises `dbt.adapters.exceptions.CompilationError` (import path: check what
  `dbt-adapters/src/dbt/adapters/exceptions/` actually exports — likely
  `dbt.adapters.exceptions.compilation.RelationConfigNotSupportedError` or similar; grep the base
  framework for the exact class dynamic_table or another existing validation uses, e.g.
  `dbt-snowflake/.../relation_configs/dynamic_table.py`'s own validations if any exist, and reuse
  the same import for consistency — do not introduce a second exception type for the same purpose).

- [ ] **Step 1: Locate the existing exception convention**

```bash
grep -rn "CompilationError\|raise_compiler_error" dbt-snowflake/src/dbt/adapters/snowflake/relation_configs/
```
Use whatever class the codebase already raises for equivalent parse-time config errors in this
same directory. If nothing precedents this in `relation_configs/`, use
`dbt.adapters.exceptions.compilation.CompilationError` (the general adapters-level compile error
type) — confirm the exact import path by checking `dbt-adapters/src/dbt/adapters/exceptions/__init__.py`.

- [ ] **Step 2: Add the four validations to `parse_relation_config`**

```python
@classmethod
def parse_relation_config(cls, relation_config: RelationConfig) -> dict:
    extra = relation_config.config.extra if relation_config.config else {}

    cluster_by = parse_model.cluster_by(relation_config)
    if not cluster_by or not str(cluster_by).strip():
        raise CompilationError(
            f"Interactive tables require a non-empty `cluster_by` config: "
            f"{relation_config.identifier}"
        )

    if str(extra.get("table_format", "")).strip().casefold() == "iceberg":
        raise CompilationError(
            f"Interactive tables do not support `table_format: iceberg`: "
            f"{relation_config.identifier}"
        )

    if extra.get("transient"):
        raise CompilationError(
            f"Interactive tables do not support `transient: true`: "
            f"{relation_config.identifier}"
        )

    target_lag = extra.get("target_lag")
    warehouse = extra.get("refresh_warehouse") or extra.get("snowflake_warehouse")
    if target_lag and str(target_lag).strip().casefold() not in _ABSENT and not warehouse:
        raise CompilationError(
            f"Interactive tables with `target_lag` set require a warehouse "
            f"(`refresh_warehouse` or `snowflake_warehouse`): {relation_config.identifier}"
        )

    return {
        "name": relation_config.identifier,
        "schema_name": relation_config.schema,
        "database_name": relation_config.database,
        "query": relation_config.compiled_code,
        "cluster_by": cluster_by,
        "target_lag": target_lag,
        "snowflake_warehouse": extra.get("snowflake_warehouse"),
        "refresh_warehouse": extra.get("refresh_warehouse"),
    }
```

(Replace `CompilationError` with whatever exact class Step 1 identifies.)

- [ ] **Step 3: Write the failing tests first**

```python
def test_missing_cluster_by_raises():
    relation_config = _relation_config(cluster_by=None)
    with pytest.raises(CompilationError, match="cluster_by"):
        SnowflakeInteractiveTableConfig.parse_relation_config(relation_config)


def test_iceberg_table_format_raises():
    relation_config = _relation_config(extra={"table_format": "iceberg"})
    with pytest.raises(CompilationError, match="iceberg"):
        SnowflakeInteractiveTableConfig.parse_relation_config(relation_config)


def test_transient_raises():
    relation_config = _relation_config(extra={"transient": True})
    with pytest.raises(CompilationError, match="transient"):
        SnowflakeInteractiveTableConfig.parse_relation_config(relation_config)


def test_target_lag_without_warehouse_raises():
    relation_config = _relation_config(extra={"target_lag": "1 hour"})
    with pytest.raises(CompilationError, match="warehouse"):
        SnowflakeInteractiveTableConfig.parse_relation_config(relation_config)


def test_target_lag_with_warehouse_does_not_raise():
    relation_config = _relation_config(extra={"target_lag": "1 hour", "snowflake_warehouse": "wh"})
    SnowflakeInteractiveTableConfig.parse_relation_config(relation_config)  # should not raise
```

Run them first and confirm they fail with `AttributeError`/no-raise (not yet implemented), per
Task 4's report contract in the subagent-driven-development skill — do not skip this check.

- [ ] **Step 4: Run full file, report pass count, commit**

```bash
cd dbt-snowflake && HATCH_PYTHON=3.12 hatch run unit-tests -- tests/unit/test_interactive_table_config.py -v
git add dbt-snowflake/src/dbt/adapters/snowflake/relation_configs/interactive_table.py \
        dbt-snowflake/tests/unit/test_interactive_table_config.py
git commit -m "feat: validate cluster_by/iceberg/transient/target_lag-warehouse at compile time"
```

---

### Task 3: `describe_interactive_table` adapter method

**Files:**
- Modify: `dbt-snowflake/src/dbt/adapters/snowflake/impl.py` (add new method near `describe_dynamic_table`, impl.py:686-736)
- Test: `dbt-snowflake/tests/unit/test_interactive_table_listing.py` (extend) or a new
  `tests/unit/test_describe_interactive_table.py` if the existing file's fixtures don't fit —
  read `test_interactive_table_listing.py` first to decide which is the better fit.

**Context:** This method does not exist anywhere in the worktree yet. It's the single biggest
missing piece — every other Python-layer test in this worktree exercises `relation_results` dicts
built by hand, not this method. `describe_dynamic_table` (impl.py:686-736) is the direct structural
template: connect, run a `SHOW ... LIKE` query, lower-case columns, select a fixed column list,
return a dict. Two differences from `describe_dynamic_table`: (1) `SHOW ... LIKE` does pattern
matching (a general Snowflake `SHOW` behavior, not specific to this feature), so an exact-match
filter is required — a table named `orders` could otherwise false-match `orders_backup`;
`describe_dynamic_table` itself gets away without this only because it trusts a single result row,
which this method should not assume; (2) the column list must be exactly `INTERACTIVE_TABLE_COLUMNS`
(`relation_configs/interactive_table.py:26-35`), which is already the declared "one source of
truth" for this exact reason.

**Interfaces:**
- Consumes: `SnowflakeRelation` (has `.quote_policy`, `.schema`, `.database`, `.identifier`).
- Produces: `Dict[str, Any]` shaped `{"interactive_table": <agate.Table>}`, which
  `SnowflakeInteractiveTableConfig.parse_relation_results` (interactive_table.py:266-295) already
  expects and knows how to read.

- [ ] **Step 1: Add the method**

```python
@available
def describe_interactive_table(self, relation: SnowflakeRelation) -> Dict[str, Any]:
    """Get all relevant metadata about an interactive table.

    SHOW INTERACTIVE TABLES LIKE uses pattern matching, so results are
    filtered to an exact name match after fetching, respecting quote policy
    (unquoted identifiers are stored upper-case in Snowflake metadata).
    """
    quoting = relation.quote_policy
    schema = f'"{relation.schema}"' if quoting.schema else relation.schema
    database = f'"{relation.database}"' if quoting.database else relation.database
    show_sql = (
        f"show interactive tables like '{relation.identifier}' in schema {database}.{schema}"
    )
    res, tables_table = self.execute(show_sql, fetch=True)
    if res.code != "SUCCESS":
        raise DbtRuntimeError(f"Could not get interactive table metadata: {show_sql} failed")

    tables_table = tables_table.rename(
        column_names=[name.lower() for name in tables_table.column_names]
    )

    if quoting.identifier:
        exact_match = tables_table.where(lambda row: row.get("name") == relation.identifier)
    else:
        identifier_upper = (relation.identifier or "").upper()
        exact_match = tables_table.where(
            lambda row: (row.get("name") or "").upper() == identifier_upper
        )
    if len(exact_match.rows) == 0:
        raise DbtRuntimeError(f"Could not find interactive table: {relation.identifier}")

    available_columns = [c.lower() for c in exact_match.column_names]
    select_columns = [c for c in INTERACTIVE_TABLE_COLUMNS if c in available_columns]
    selected = exact_match.select(select_columns)

    return {"interactive_table": selected}
```

Add `from dbt.adapters.snowflake.relation_configs.interactive_table import INTERACTIVE_TABLE_COLUMNS`
to `impl.py`'s imports (check it isn't already imported under a different alias first).

- [ ] **Step 2: Write unit tests** (read `test_interactive_table_listing.py` first for the fixture
  pattern this worktree already uses for mocking `self.execute`/`agate.Table` — reuse it, don't
  invent a new mocking approach)

At minimum, cover: (a) happy path returns the right dict shape; (b) `SHOW` returning multiple
pattern-matched rows correctly filters to the exact match; (c) quoted vs unquoted identifier
matching; (d) no matching row raises `DbtRuntimeError`; (e) a `SHOW` result missing a column (e.g.
an account without a given feature flag) doesn't KeyError — it's just omitted from `select_columns`.

- [ ] **Step 3: Run tests, report pass count, commit.**

---

### Task 4: `describe` wrapper macro

**Files:**
- Create: `dbt-snowflake/src/dbt/include/snowflake/macros/relations/interactive_table/describe.sql`

**Interfaces:**
- Consumes: `adapter.describe_interactive_table(relation)` (Task 3).
- Produces: `snowflake__describe_interactive_table(relation)` macro, callable the same way
  `snowflake__describe_dynamic_table` is, for parity/testability with `dynamic_table` and so
  `tests/functional/utils.py` can gain a `describe_interactive_table` test helper mirroring
  `describe_dynamic_table` (utils.py:101-113) in Task 10.

- [ ] **Step 1: Read the template**

```bash
cat dbt-snowflake/src/dbt/include/snowflake/macros/relations/dynamic_table/describe.sql
```

- [ ] **Step 2: Write the file**

```sql
{% macro snowflake__describe_interactive_table(relation) %}
    {%- set _interactive_table = adapter.describe_interactive_table(relation) -%}
    {% do return(_interactive_table) %}
{% endmacro %}
```

(Match the exact macro-call/return shape of `dynamic_table/describe.sql` — if it differs from the
above sketch, mirror it exactly rather than this guess.)

- [ ] **Step 3: Commit.** No test needed standalone — this is exercised by Task 10's functional
  tests once the materialization calls it.

---

### Task 5: Core macros — `create`, `replace`, `drop`, `rename`, and the materialization

**Files:**
- Create: `dbt-snowflake/src/dbt/include/snowflake/macros/materializations/interactive_table.sql`
- Create: `dbt-snowflake/src/dbt/include/snowflake/macros/relations/interactive_table/create.sql`
- Create: `dbt-snowflake/src/dbt/include/snowflake/macros/relations/interactive_table/replace.sql`
- Create: `dbt-snowflake/src/dbt/include/snowflake/macros/relations/interactive_table/alter.sql`
- Create: `dbt-snowflake/src/dbt/include/snowflake/macros/relations/interactive_table/drop.sql`
- Create: `dbt-snowflake/src/dbt/include/snowflake/macros/relations/interactive_table/rename.sql`

**Context:** Do not reference PR #2042 (the reverted original) for this task — per explicit user
direction, treat the v2/Fusion port (`fs` PR #12664) as the correct reference implementation
(assumed correct until reviewed otherwise), and build the actual Jinja/Python shape by following
this v1 codebase's own existing conventions (`dynamic_table.sql` and its `relations/dynamic_table/*`
files) rather than any prior v1 attempt. Where v1's existing macro conventions and v2's confirmed
Snowflake behavior would lead to different code shapes, decide independently based on what's
idiomatic for *this* codebase rather than copying either source mechanically. Two things this
means concretely: (1) `cluster_by` can stay an unconditional, always-present clause in the DDL
because Task 2 guarantees it's validated non-blank before compilation ever reaches macro
rendering; (2) a real `ALTER INTERACTIVE TABLE` path exists and is now confirmed live (see Global
Constraints) — `target_lag`/`refresh_warehouse`/`snowflake_initialization_warehouse` value-to-value
changes must route through ALTER, not REPLACE, matching `configuration_changes.requires_full_refresh`
(already correctly computed by the existing config layer). Only `cluster_by` changes and
dynamic↔static transitions replace. Do not port `dynamic_table.sql`'s scheduler/REFRESH trigger
step (materializations/dynamic_table.sql:18-23)
— that's a dynamic-table-specific mechanism (Snowflake's scheduler flag) with no interactive-table
analog found in any source read for this plan.

**Interfaces:**
- Consumes: `existing_relation.interactive_table_config_changeset(adapter.describe_interactive_table(existing_relation), config.model)` (relation.py:204, already implemented) → returns `SnowflakeInteractiveTableConfigChangeset` or `None`.
- Produces: registers the `interactive_table` materialization; `{'relations': [target_relation]}`.

- [ ] **Step 1: `materializations/interactive_table.sql`**

```sql
{% materialization interactive_table, adapter='snowflake' %}

    {% set query_tag = set_query_tag() %}

    {% set existing_relation = load_cached_relation(this) %}
    {% set target_relation = this.incorporate(type=this.InteractiveTable) %}

    {{ run_hooks(pre_hooks) }}

    {% set build_sql = interactive_table_get_build_sql(existing_relation, target_relation) %}

    {% if build_sql == '' %}
        {{ interactive_table_execute_no_op(target_relation) }}
    {% else %}
        {{ interactive_table_execute_build_sql(build_sql, existing_relation, target_relation) }}
    {% endif %}

    {# runs every run (build or no-op) so the warehouse attachment list stays authoritative #}
    {{ snowflake__sync_interactive_warehouses(target_relation) }}

    {{ run_hooks(post_hooks) }}

    {% do unset_query_tag(query_tag) %}

    {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}


{% macro interactive_table_get_build_sql(existing_relation, target_relation) %}

    {% set full_refresh_mode = should_full_refresh() %}

    {% if existing_relation is none %}
        {% set build_sql = get_create_sql(target_relation, sql) %}
    {% elif full_refresh_mode or not existing_relation.is_interactive_table %}
        {% set build_sql = get_replace_sql(existing_relation, target_relation, sql) %}
    {% else %}

        {% set on_configuration_change = config.get('on_configuration_change') %}
        {% set configuration_changes = snowflake__get_interactive_table_configuration_changes(existing_relation, config) %}

        {% if configuration_changes is none %}
            {% set build_sql = '' %}
            {{ exceptions.warn("No configuration changes were identified on: `" ~ target_relation ~ "`. Continuing.") }}

        {% elif on_configuration_change == 'apply' %}
            {% if configuration_changes.requires_full_refresh %}
                {# cluster_by change, or a dynamic<->static transition -- both confirmed to reject ALTER live, see Global Constraints #}
                {% set build_sql = get_replace_sql(existing_relation, target_relation, sql) %}
            {% else %}
                {# target_lag/refresh_warehouse/snowflake_initialization_warehouse value-to-value change -- confirmed ALTERable live #}
                {% set build_sql = snowflake__get_alter_interactive_table_as_sql(existing_relation, configuration_changes, target_relation, sql) %}
            {% endif %}
        {% elif on_configuration_change == 'continue' %}
            {% set build_sql = '' %}
            {{ exceptions.warn("Configuration changes were identified and `on_configuration_change` was set to `continue` for `" ~ target_relation ~ "`") }}
        {% elif on_configuration_change == 'fail' %}
            {{ exceptions.raise_fail_fast_error("Configuration changes were identified and `on_configuration_change` was set to `fail` for `" ~ target_relation ~ "`") }}
        {% else %}
            {{ exceptions.raise_compiler_error("Unexpected configuration scenario: `" ~ on_configuration_change ~ "`") }}
        {% endif %}

    {% endif %}

    {% do return(build_sql) %}

{% endmacro %}


{% macro interactive_table_execute_no_op(relation) %}
    {% do store_raw_result(
        name="main",
        message="skip " ~ relation,
        code="skip",
        rows_affected="-1"
    ) %}
{% endmacro %}


{% macro interactive_table_execute_build_sql(build_sql, existing_relation, target_relation) %}

    {% set grant_config = config.get('grants') %}

    {% call statement(name="main") %}
        {{ build_sql }}
    {% endcall %}

    {% set should_revoke = should_revoke(existing_relation, full_refresh_mode=True) %}
    {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

    {% do persist_docs(target_relation, model) %}

{% endmacro %}


{% macro snowflake__get_interactive_table_configuration_changes(existing_relation, new_config) -%}
    {% set _existing_interactive_table = adapter.describe_interactive_table(existing_relation) %}
    {% set _configuration_changes = existing_relation.interactive_table_config_changeset(_existing_interactive_table, new_config.model) %}
    {% do return(_configuration_changes) %}
{%- endmacro %}
```

Note: there's deliberately no `{% elif config.get('target_lag') is none %}` special-case forcing
static tables to always replace — the changeset (post Task 1's fix) already computes `has_changes`
correctly for static tables, so a static table with no changes correctly no-ops instead of
rebuilding every run, matching ordinary idempotent-materialization behavior. Verify this holds in
Task 10/11; if it doesn't, that's a real regression to fix, not something to route around with a
special-case.

- [ ] **Step 2: `relations/interactive_table/create.sql`**

```sql
{% macro snowflake__get_create_interactive_table_as_sql(relation, sql) -%}

    {%- set interactive_table = relation.from_config(config.model) -%}
    {{ snowflake__create_interactive_table_sql(interactive_table, relation, sql) }}

{%- endmacro %}


{% macro snowflake__create_interactive_table_sql(interactive_table, relation, sql) -%}
{#-
    https://docs.snowflake.com/en/sql-reference/sql/create-interactive-table
    No COPY GRANTS, no iceberg/table_format variant -- neither appears in the
    documented CREATE INTERACTIVE TABLE syntax.
-#}

    create interactive table {{ relation }}
        cluster by ({{ interactive_table.cluster_by }})
        {% if interactive_table.target_lag is not none %}target_lag = '{{ interactive_table.target_lag }}'{% endif %}
        {{ optional('warehouse', interactive_table.warehouse_parameter, equals_char='= ') }}
        {{ optional('initialization_warehouse', interactive_table.snowflake_initialization_warehouse, equals_char='= ') }}
        as (
            {{ sql }}
        )

{%- endmacro %}
```

Note: uses `interactive_table.warehouse_parameter` (relation_configs/interactive_table.py:210-230),
not `.snowflake_warehouse` directly — that property already implements the
refresh_warehouse-overrides-snowflake_warehouse fallback the config layer built. Using the raw
`.snowflake_warehouse` field here instead would silently ignore an explicit `refresh_warehouse`
override.
`INITIALIZATION_WAREHOUSE = <wh>` is a real `CREATE INTERACTIVE TABLE` clause, confirmed live
2026-08-25 — Snowflake's public docs don't mention it (same doc-lag pattern as `ALTER INTERACTIVE
TABLE` itself; don't trust the doc's syntax block as exhaustive here either).

- [ ] **Step 3: `relations/interactive_table/replace.sql`**

```sql
{% macro snowflake__get_replace_interactive_table_sql(relation, sql) -%}

    {%- set interactive_table = relation.from_config(config.model) -%}

    create or replace interactive table {{ relation }}
        cluster by ({{ interactive_table.cluster_by }})
        {% if interactive_table.target_lag is not none %}target_lag = '{{ interactive_table.target_lag }}'{% endif %}
        {{ optional('warehouse', interactive_table.warehouse_parameter, equals_char='= ') }}
        {{ optional('initialization_warehouse', interactive_table.snowflake_initialization_warehouse, equals_char='= ') }}
        as (
            {{ sql }}
        )

{%- endmacro %}
```

- [ ] **Step 3.5: `relations/interactive_table/alter.sql`** (new — PR #2042 never built this since
  it assumed no ALTER path existed; confirmed live on 2026-08-25 that one does for
  `target_lag`/`refresh_warehouse`/`snowflake_initialization_warehouse` value changes)

**Do not derive this from scratch.** `fs`'s v2/Fusion port already has a shipped, live-tested
equivalent — read it in full and translate field names, don't reinvent the shape:
```bash
cd /Users/naman/Projects/dbt/fs && git show origin/feat/snowflake-interactive-table:sa/crates/dbt-loader/src/dbt_macro_assets/dbt-snowflake/macros/relations/interactive_table/alter.sql
```
That macro combines `target_lag`/`warehouse`/`initialization_warehouse` into **one**
`alter interactive table t set ...` statement (space-separated assignments, not semicolon-chained
— confirmed live: Snowflake accepts multiple property assignments in one `SET` clause), plus a
separate `unset initialization_warehouse` statement when that field is being cleared rather than
changed to a new value (the only one of the three that can be cleared while the table stays
dynamic — `target_lag`/`refresh_warehouse` can't be cleared without a dynamic↔static transition,
which replaces instead of reaching this macro). Translated to this codebase's Python field names:

```sql
{% macro snowflake__get_alter_interactive_table_as_sql(existing_relation, configuration_changes, target_relation, sql) -%}
    {{ log('Applying ALTER to: ' ~ existing_relation) }}
    {{ snowflake__alter_interactive_table_sql(existing_relation, configuration_changes, target_relation) }}
{%- endmacro %}


{% macro snowflake__alter_interactive_table_sql(existing_relation, configuration_changes, target_relation) %}
{#-
    Only reached when `configuration_changes.requires_full_refresh` is False.
    Mirrors fs's (v2/Fusion) `snowflake__get_alter_interactive_table_as_sql`
    statement shape exactly -- confirmed live against ktb38830, 2026-08-25.
-#}
    {%- set interactive_table = target_relation.from_config(config.model) -%}
    {%- set target_lag = configuration_changes.target_lag -%}
    {%- set refresh_warehouse = configuration_changes.refresh_warehouse -%}
    {%- set init_warehouse = configuration_changes.snowflake_initialization_warehouse -%}

    {#- `.context` distinguishes a new value from a cleared one for
        snowflake_initialization_warehouse specifically -- see the fs source
        this is translated from for why the other two fields don't need this
        guard. -#}
    {%- set has_set_changes = target_lag or refresh_warehouse or (init_warehouse and init_warehouse.context) -%}

    {% if has_set_changes -%}
alter interactive table {{ existing_relation }} set
        {% if target_lag %}target_lag = '{{ interactive_table.target_lag }}'{% endif %}
        {% if refresh_warehouse %}warehouse = {{ interactive_table.warehouse_parameter }}{% endif %}
        {% if init_warehouse and init_warehouse.context %}initialization_warehouse = {{ interactive_table.snowflake_initialization_warehouse }}{% endif %}
    {%- endif %}

    {%- if init_warehouse and not init_warehouse.context %}
    {% if has_set_changes %};{% endif %}
alter interactive table {{ existing_relation }} unset initialization_warehouse
    {%- endif %}
{% endmacro %}
```

Verify the exact `.context` semantics against `RelationConfigChange` (base framework, not
Snowflake-specific) before assuming this translation is byte-correct — the fs source uses its own
Rust `ComponentConfigChange` enum, which is a different type with possibly-different truthiness
rules than this codebase's `RelationConfigChange` dataclass.

- [ ] **Step 4: `relations/interactive_table/drop.sql`**

```sql
{% macro snowflake__get_drop_interactive_table_sql(relation) %}
    drop table if exists {{ relation }}
{% endmacro %}
```

(Plain `DROP TABLE`, not a special keyword — interactive tables report `kind=TABLE` in Snowflake
metadata, per `impl.py`'s own classification logic at impl.py:310-315.)

- [ ] **Step 5: `relations/interactive_table/rename.sql`**

```sql
{%- macro snowflake__get_rename_interactive_table_sql(relation, new_name) -%}
    alter table {{ relation }} rename to {{ new_name }}
{%- endmacro -%}
```

(Plain `ALTER TABLE ... RENAME TO` — confirmed supported for interactive tables per the Global
Constraints doc citation.)

- [ ] **Step 6: Wire the dispatch points**

In each of `relations/create.sql`, `relations/drop.sql`, `relations/replace.sql`,
`relations/rename.sql`, add an `{% elif relation.is_interactive_table %}` branch dispatching to the
macros above, positioned the same way the existing `{% elif relation.is_dynamic_table %}` branch
is in each file. Read each file first — do not guess the branch structure; match it exactly.
`alter.sql` is **not** wired into any of these generic dispatch files — it's called directly from
`materializations/interactive_table.sql`'s own apply branch (Step 1), the same way dynamic_table's
alter path is invoked from its own materialization rather than through `relations/alter.sql`
(there is no generic multi-type `alter` dispatcher in this codebase; confirm this by checking
whether `relations/alter.sql` exists at all before assuming otherwise).

- [ ] **Step 7: No automated test for this step alone** — exercised by Task 10's functional tests.
  Commit once Step 6 compiles (`dbt parse` against a scratch project referencing an
  `interactive_table` model does not raise) — a quick manual `dbt compile` sanity check, not a full
  test suite run, is sufficient gate for this task; full behavioral verification is Task 10/11.

- [ ] **Step 8: Commit.**

---

### Task 6: Warehouse attach/detach (`snowflake_interactive_warehouses`)

**Files:**
- Modify: `dbt-snowflake/src/dbt/adapters/snowflake/impl.py` (new `describe_interactive_table_warehouses` method)
- Create: `dbt-snowflake/src/dbt/include/snowflake/macros/relations/interactive_table/warehouses.sql`
- Test: new `dbt-snowflake/tests/unit/test_interactive_table_warehouses.py`

**Context:** Neither PR #2042 (attach-only, no detach) nor the current worktree (nothing at all)
has full attach/detach. The v2/Fusion port (`fs` PR #12664) does both, diffing the desired
`snowflake_interactive_warehouses` config against `adapter.describe_interactive_table_warehouses()`
every run, and — critically — **never routes this through the main config changeset**, so
attaching/detaching a warehouse never triggers a rebuild. Read the actual working Rust
implementation as the reference for query mechanics before writing this task's code — do not
guess the SHOW/query shape:

```bash
cd /Users/naman/Projects/dbt/fs && git show origin/feat/snowflake-interactive-table:sa/crates/dbt-adapter/src/relation/snowflake/config/components/interactive_table_warehouse.rs
```

Read that file in full first. It will show exactly how v2 enumerates which warehouses currently
have a given table attached (likely a `SHOW TABLES IN WAREHOUSE <wh>`-style query per warehouse in
the desired list, or an `INFORMATION_SCHEMA`-based query — confirm from the actual source rather
than assuming). Translate the mechanism, not the Rust syntax, to Python/Jinja.

**Interfaces:**
- Consumes: `config.get('snowflake_interactive_warehouses')` (a string or list of warehouse
  identifiers — check v2's `snowflake_interactive_warehouses` component for the exact accepted
  shape rather than assuming).
- Produces: `snowflake__sync_interactive_warehouses(relation)` macro (referenced by Task 5's
  materialization), calling `ALTER WAREHOUSE <wh> ADD TABLES (<relation>)` for warehouses newly in
  the desired list and `ALTER WAREHOUSE <wh> DROP TABLES (<relation>)` for warehouses no longer
  desired.

- [ ] **Step 1: Read the v2 reference implementation** (command above). Report back in the
  implementation notes exactly what query/mechanism it uses to enumerate current attachments,
  since this determines Step 2's SQL.

- [ ] **Step 2: Add `describe_interactive_table_warehouses` to `impl.py`**, following whatever
  mechanism Step 1 found, structured the same way `describe_interactive_table` (Task 3) is:
  execute, check `res.code == "SUCCESS"`, return a data structure the macro can iterate (e.g. a
  list of warehouse names currently attached).

- [ ] **Step 3: Write `warehouses.sql`**

```sql
{% macro snowflake__sync_interactive_warehouses(relation) %}
    {%- set desired = config.get('snowflake_interactive_warehouses') -%}
    {%- set desired = ([desired] if desired is string else (desired or [])) -%}
    {%- set current = adapter.describe_interactive_table_warehouses(relation) -%}

    {%- for warehouse in desired if warehouse not in current -%}
        {%- call statement('attach_interactive_warehouse_' ~ loop.index) -%}
            alter warehouse {{ warehouse }} add tables ({{ relation }})
        {%- endcall -%}
    {%- endfor -%}

    {%- for warehouse in current if warehouse not in desired -%}
        {%- call statement('detach_interactive_warehouse_' ~ loop.index) -%}
            alter warehouse {{ warehouse }} drop tables ({{ relation }})
        {%- endcall -%}
    {%- endfor -%}
{% endmacro %}
```

(Adjust the `not in` membership checks for case-sensitivity/quoting once Step 1's real mechanism
is known — warehouse identifiers may need the same casefold-for-comparison /
verbatim-for-DDL split established in the existing `_normalize_warehouse` helper, Task 1's
neighboring code. Do not compare raw strings if Step 1 reveals Snowflake echoes back
differently-cased names.)

- [ ] **Step 4: Unit tests** covering: nothing configured + nothing attached → no statements;
  attach-only; detach-only; both attach and detach in the same run; case-insensitive match (a
  desired `my_wh` matching a current `MY_WH` should not spuriously detach-then-reattach).

- [ ] **Step 5: Run tests, report pass count, commit.**

---

### Task 7: Comment-DDL dispatch (`adapters.sql`)

**Files:**
- Modify: `dbt-snowflake/src/dbt/include/snowflake/macros/adapters.sql`

**Context:** `snowflake__alter_relation_comment`, `snowflake__alter_column_comment`, and any
persist_docs-related macro that branches on relation type each need an
`{% elif relation.is_interactive_table %}` branch setting `relation_type = 'table'` for comment DDL
purposes (interactive tables use plain `COMMENT ON TABLE` syntax) — mechanical, mirrors the
existing `is_dynamic_table` branches exactly. Grep first to find every such branch before editing,
don't rely on line numbers from a stale read.

- [ ] **Step 1:**
```bash
grep -n "is_dynamic_table" dbt-snowflake/src/dbt/include/snowflake/macros/adapters.sql
```
- [ ] **Step 2:** Add a matching `{% elif relation.is_interactive_table %}` branch next to each hit
  found, setting the same value the `is_dynamic_table` branch sets, adapted to `'table'` where the
  comment mechanics differ (interactive tables have no special comment DDL keyword).
- [ ] **Step 3:** No standalone test — covered by Task 10's `persist_docs` functional test.
  Commit.

---

### Task 8: Test helper — `query_relation_type`

**Files:**
- Modify: `dbt-snowflake/tests/functional/utils.py:56-75`

**Context:** The shared functional-test helper's relation-type classification SQL has no branch
for interactive tables at all — every dynamic_table functional test relies on this helper, and any
interactive_table functional test (Task 10) will too.

- [ ] **Step 1: Read current implementation**, then update:

```sql
case table_type
    when 'BASE TABLE' then
        iff(is_interactive = 'YES', 'interactive_table', iff(is_dynamic = 'YES', 'dynamic_table', 'table'))
    when 'VIEW' then 'view'
    when 'EXTERNAL TABLE' then 'external_table'
end as relation_type
```

Interactive-before-dynamic, matching the same ordering rule already established in `impl.py`'s
`_tabular_relation_type` (impl.py:455-471) — a dynamic interactive table sets both flags.

**Caveat**: confirm the actual column is named `is_interactive` in whatever system view this query
runs against (`INFORMATION_SCHEMA.TABLES` per the `impl.py` classification code) — read the
existing query's source (`INFORMATION_SCHEMA.TABLES` vs `SHOW`) before editing, since the column
name/values (`'YES'`/`'NO'` vs `Y`/`N`) differ between the two per
`interactive-tables-snowflake` wiki findings.

- [ ] **Step 2: No standalone test** (this is test infrastructure) — exercised by Task 10.
  Commit.

---

### Task 9: Remaining unit test gaps

**Files:**
- Modify: `dbt-snowflake/tests/unit/test_renamed_relations.py` if any comment-DDL relation-type
  mock needs `is_interactive_table` added (check `test_alter_relation_comment_macro.py`-equivalent
  file in this codebase, if one exists, for Task 7's coverage).

- [ ] **Step 1:** Grep for existing tests that mock `is_dynamic_table` and check whether an
  equivalent `is_interactive_table` mock is missing anywhere Task 7 touched.
- [ ] **Step 2:** Add missing coverage.
- [ ] **Step 3:** Run full `dbt-snowflake` unit suite, report pass count, commit.

```bash
cd dbt-snowflake && HATCH_PYTHON=3.12 hatch run unit-tests -v
```

---

### Task 10: Functional (dbt-YAML) tests

**Files:**
- Create: `dbt-snowflake/tests/functional/relation_tests/interactive_table_tests/models.py`
- Create: `dbt-snowflake/tests/functional/relation_tests/interactive_table_tests/test_basic.py`
- Modify: `dbt-snowflake/tests/functional/utils.py` — add a `describe_interactive_table` helper
  mirroring `describe_dynamic_table` (utils.py:101-113), wrapping Task 4's macro.

**Context:** Do not reference PR #2042's functional tests for this task — per explicit user
direction, build these fresh, structurally following this codebase's own existing
`dynamic_table_tests/test_basic.py` pattern (no `dbt-tests-adapter` shared mixins are used for
dynamic_table, so don't introduce them here either), and cross-checking *scenario coverage* (not
code) against v2/Fusion's real functional test file
(`fs/sa/crates/dbt-loader/tests/materializations/interactive_table.rs` in the `fs` repo) to make
sure nothing behaviorally important is missed — v2 is the assumed-correct reference for what
Snowflake actually does, this codebase's own dynamic_table tests are the reference for *how a v1
Python/pytest test should be structured*. Scenarios to cover, written natively for this codebase:
1. ALTER-vs-REPLACE test classes modeled on `dynamic_table_tests/test_basic.py`'s
   `TestSchedulerConfigChange` (the best template for "assert ALTER happened, not REPLACE"): one
   asserting `target_lag`-value-only, `refresh_warehouse`-only, and
   `snowflake_initialization_warehouse`-only (both set-to-new-value and cleared-to-none) changes
   each emit `ALTER INTERACTIVE TABLE ... SET/UNSET ...` (confirmed live 2026-08-25, see Global
   Constraints), and one asserting `cluster_by` changes and dynamic↔static `target_lag`
   transitions still emit `CREATE OR REPLACE`. Also add a static-table test confirming a
   project-wide `snowflake_initialization_warehouse` produces no diff at all (the Task 1
   regression test).
2. Test coverage for the 4 compile-time validations from Task 2: missing/blank `cluster_by`,
   `table_format: iceberg`, `transient: true`, `target_lag` set without a resolvable warehouse —
   each should assert a `CompilationError` at `dbt compile`/`dbt run`, never reaching SQL.
3. Test coverage for attach AND detach (Task 6): one test that adds a warehouse to
   `snowflake_interactive_warehouses` and confirms `ALTER WAREHOUSE ... ADD TABLES` fires, one that
   then removes it on a subsequent run and confirms `ALTER WAREHOUSE ... DROP TABLES` fires.
4. A no-op idempotency test for a **static** interactive table specifically — confirms Task 5's
   materialization correctly no-ops a static table with nothing changed, rather than rebuilding it
   unconditionally every run.

- [ ] **Step 1:** Read `dynamic_table_tests/test_basic.py` and `dynamic_table_tests/models.py` in
  full for the exact fixture/class structure to follow. Read
  `fs/sa/crates/dbt-loader/tests/materializations/interactive_table.rs` (in the `fs` repo) for the
  scenario list v2 already validated live — use it as a checklist, not a code source.
- [ ] **Step 2:** Write `models.py` fixtures for the 4 scenario groups above, following
  `dynamic_table_tests/models.py`'s existing fixture-string conventions.
- [ ] **Step 3:** Write `test_basic.py` test classes, one per scenario, following
  `dynamic_table_tests/test_basic.py`'s exact structural pattern (class-scoped `seeds`/`models`
  fixtures, `run_dbt`/`run_dbt_and_capture`, `assert_message_in_logs`, `query_relation_type`).
- [ ] **Step 4:** These require a live Snowflake connection to actually execute (functional tests,
  not unit tests) — do NOT attempt to run them in this task. Confirm only that they **collect**
  without error:
```bash
cd dbt-snowflake && HATCH_PYTHON=3.12 hatch run pytest --collect-only tests/functional/relation_tests/interactive_table_tests/
```
  Report the collected test count. Actual execution against a live warehouse is Task 11.
- [ ] **Step 5:** Commit.

---

### Task 11: Live-Snowflake verification (SUPERVISED — do not delegate to an autonomous subagent)

This task is run together, live, in this session — not dispatched as an unattended subagent task,
per an explicit decision to keep hands-on control over anything that writes to a real warehouse.

**Checklist** (using the `ktb38830` account, in a dedicated throwaway schema, dropped `CASCADE`
afterward with a 0-rows-from-a-fresh-connection confirmation, matching the cleanup discipline
already established in [[snowflake-live-probe-account]] and the v2 PR's own live-test method):

- [ ] Run Task 10's functional test suite for real: `hatch run pytest tests/functional/relation_tests/interactive_table_tests/ -v`
- [ ] `CREATE INTERACTIVE TABLE` DDL accepted, both static and dynamic, with the full clause set
      (cluster_by + target_lag + warehouse + initialization_warehouse)
- [ ] A static table with a project-wide `snowflake_initialization_warehouse` set produces no
      diff and no rebuild across repeated no-op runs (the Task 1 fix's real-world proof)
- [ ] Confirm `cluster_by` readback shape from `SHOW INTERACTIVE TABLES` specifically (already
      independently confirmed bare-parens, no `LINEAR` prefix, per prior probe — recheck against
      this task's actual DDL rather than assuming the earlier probe's project still applies)
- [ ] `target_lag`-value-only and `refresh_warehouse`-only changes emit
      `ALTER INTERACTIVE TABLE ... SET ...` (property name `WAREHOUSE`, not `REFRESH_WAREHOUSE`);
      `cluster_by` changes and dynamic↔static `target_lag` transitions emit `CREATE OR REPLACE` —
      confirm against this task's own macros, not just the ad hoc probe from plan-writing time
- [ ] A no-op run on an unchanged static interactive table does NOT rebuild
- [ ] `snowflake_interactive_warehouses` attach and detach both fire the correct
      `ALTER WAREHOUSE` statements
- [ ] `RENAME` preserves warehouse attachment (matches v2's confirmed finding for dynamic
      interactive tables — verify it holds for this Python implementation too)
- [ ] Cross-type replace, `table` ⇄ `interactive_table`, works both directions
- [ ] The 4 compile-time validations (Task 2) actually block `dbt run`/`dbt compile` with the
      expected error message, without ever issuing SQL
- [ ] Cleanup: `DROP SCHEMA ... CASCADE`, confirm 0 rows via a separate connection

- [ ] **If anything here contradicts an assumption baked into Tasks 1-10** (e.g., `cluster_by`
  readback format, warehouse attach/detach query mechanism, rename behavior), stop and fix the
  code — do not just note the discrepancy in a test comment and move on.

---

### Task 12: Final whole-branch review

Dispatch on `opus`. Use `scripts/review-package` (from the subagent-driven-development skill
directory) against `MERGE_BASE..HEAD` where `MERGE_BASE = git merge-base main HEAD`, covering all
commits from Task 1 through Task 10 (and any fixes from Task 11). Pay particular attention to:
whether the ALTER-vs-REPLACE branch in `materializations/interactive_table.sql` (Task 5) correctly
matches `configuration_changes.requires_full_refresh` — `target_lag`/`refresh_warehouse`
value-to-value changes must ALTER, `cluster_by` changes and dynamic↔static transitions must
REPLACE, confirmed live 2026-08-25 (see Global Constraints) — whether the warehouse attach/detach
diff (Task 6) could ever spuriously flip-flop on casing, and whether the 4 new validations
(Task 2) have functional test coverage proving they actually block `dbt run` (not just unit-level
`parse_relation_config` coverage).

---

### Task 13: PR draft (NOT delegated — done directly, human hands off the actual `gh pr create`)

Once Tasks 1-12 are complete and reviewed clean:
- [ ] Draft PR title and body (linking back to the reverted #2042 and the v2/Fusion port #12664 for
  context, summarizing what's different this time per the wiki's
  [[dbt-adapters-snowflake-interactive-table-v1-retry]] page).
- [ ] Print the exact `gh pr create --title ... --body ...` command.
- [ ] Stop. Do not run it — hand it to the user to review and execute themselves.

---

## Progress tracking

Mirror task completion in the Obsidian wiki at
`~/Projects/dbt/wiki/sources/dbt-adapters-snowflake-interactive-table-v1-retry.md`'s "Progress log"
and "Not yet done" sections after each task's review passes — that page is the live,
human-readable progress record; `.superpowers/sdd/progress.md` (git-ignored) is the
machine-recovery ledger for resuming this plan across context resets. Update both, not just one.
