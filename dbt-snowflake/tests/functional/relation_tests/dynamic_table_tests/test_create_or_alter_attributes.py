"""
Parameterized attribute-behavior tests for the CREATE OR ALTER dynamic table path.

For a FULL + scheduler='disable' native dynamic table with the behavior flag on, a COA
query edit re-declares the definition. This module pins down how each dynamic-table
attribute behaves across that COA, per Snowflake's CREATE OR ALTER semantics:

  - governance (masking / row-access policies, tags) is PRESERVED,
  - operational attributes set out of band (data_retention, comment) RESET to
    default/inherited -- the same as CREATE OR REPLACE (--full-refresh),
  - dbt config-surface attributes (cluster_by, warehouse, ...) are APPLIED from config,

and a combined multi-attribute case confirms they behave independently in one COA.

Test flow (shared by every test in this module):
  1. ARRANGE -- the `fresh_dt` fixture builds a fresh dynamic table per test
     (`seed` + baseline model + `run --full-refresh`); the flag is on via
     `project_config_update`. Axis A then sets the attribute out of band with
     `run_sql` (masking policy, tag, retention, ...) because these have no dbt
     config surface, and asserts the setup landed (`before == expected`).
  2. ACT -- edit the model and `dbt run` WITHOUT --full-refresh. Axis A uses a
     query-only edit (add a column) purely to trigger a COA; Axis B changes a
     dbt config key (warehouse, cluster_by, ...) to see it applied in place.
  3. ASSERT -- read the attribute back and check preserved vs. reset (Axis A) or
     applied (Axis B). Reads go through `describe_dynamic_table` (SHOW output) for
     dbt-managed attrs, or a catalog query for the rest -- policies/tags via
     `information_schema.*_references`, retention/comment via `.tables`,
     parameters via `SHOW PARAMETERS` (see the `_policies`/`_tags`/`_table_field`/
     `_param` read helpers below).
  4. TEARDOWN -- `fresh_dt` drops the schema so each parametrized case starts clean.

Axis A is parametrized over `ATTR_SPECS`, Axis B over `CONFIG_SPECS`; each spec is
one attribute's (how to set / how to read / expected outcome).

Requires a live Snowflake connection.
"""

import pytest

from dbt.tests.util import run_dbt

from tests.functional.utils import describe_dynamic_table, update_model


FLAG = {"flags": {"snowflake_dynamic_table_create_or_alter": True}}

SEED = "id,value\n1,alice\n2,bob\n3,carol\n"

DT_BASE = """
{{ config(materialized='dynamic_table', snowflake_warehouse='DBT_TESTING',
          refresh_mode='FULL', scheduler='disable') }}
select id, value from {{ ref('my_seed') }}
"""

# A COA-compatible query edit (add a column at the end) that triggers a COA.
DT_QUERY_EDIT = """
{{ config(materialized='dynamic_table', snowflake_warehouse='DBT_TESTING',
          refresh_mode='FULL', scheduler='disable') }}
select id, value, id * 10 as id_x10 from {{ ref('my_seed') }}
"""


# --------------------------------------------------------------------------- reads


def _fqn(project):
    return f"{project.database}.{project.test_schema}.dt_coa"


def _policies(project):
    rows = project.run_sql(
        f"select policy_name from table({project.database}.information_schema.policy_references("
        f"ref_entity_name => '{_fqn(project)}', ref_entity_domain => 'table'))",
        fetch="all",
    )
    return sorted(r[0].upper() for r in rows)


def _tags(project):
    rows = project.run_sql(
        f"select tag_name from table({project.database}.information_schema.tag_references("
        f"'{_fqn(project)}', 'table'))",
        fetch="all",
    )
    return sorted(r[0].upper() for r in rows)


def _table_field(project, field):
    rows = project.run_sql(
        f"select {field} from {project.database}.information_schema.tables "
        f"where table_schema ilike '{project.test_schema}' and table_name ilike 'dt_coa'",
        fetch="all",
    )
    return rows[0][0] if rows else None


def _param(project, name):
    # SHOW PARAMETERS returns rows directly (key, value, default, level, ...)
    rows = project.run_sql(f"show parameters like '{name}' in table {_fqn(project)}", fetch="all")
    return rows[0][1] if rows else None


# --------------------------------------------------------------------------- specs

# Each spec: how to set the attribute out of band, how to read it, and the expected
# behavior after a COA query edit. `read` returns a comparable value.
PRESERVED = "preserved"
RESET = "reset"

ATTR_SPECS = [
    pytest.param(
        {
            "apply": [
                "create masking policy if not exists mp as (v varchar) returns varchar -> '***'",
                "alter table {fqn} modify column value set masking policy mp",
            ],
            "read": lambda p: "MP" in _policies(p),
            "before": True,
            "kind": PRESERVED,
        },
        id="masking_policy",
    ),
    pytest.param(
        {
            "apply": [
                "create row access policy if not exists rap as (i int) returns boolean -> true",
                "alter dynamic table {fqn} add row access policy rap on (id)",
            ],
            "read": lambda p: "RAP" in _policies(p),
            "before": True,
            "kind": PRESERVED,
        },
        id="row_access_policy",
    ),
    pytest.param(
        {
            "apply": [
                "create tag if not exists mytag",
                "alter dynamic table {fqn} set tag mytag = 'v1'",
            ],
            "read": lambda p: "MYTAG" in _tags(p),
            "before": True,
            "kind": PRESERVED,
        },
        id="tag",
    ),
    pytest.param(
        {
            "apply": [
                "create projection policy if not exists pp as () returns projection_constraint "
                "-> projection_constraint(allow => true)",
                "alter table {fqn} modify column value set projection policy pp",
            ],
            "read": lambda p: "PP" in _policies(p),
            "before": True,
            "kind": PRESERVED,
        },
        id="projection_policy",
    ),
    pytest.param(
        {
            "apply": [
                "create aggregation policy if not exists ap as () returns aggregation_constraint "
                "-> aggregation_constraint(min_group_size => 2)",
                "alter dynamic table {fqn} set aggregation policy ap",
            ],
            "read": lambda p: "AP" in _policies(p),
            "before": True,
            "kind": PRESERVED,
        },
        id="aggregation_policy",
    ),
    pytest.param(
        {
            "apply": ["alter dynamic table {fqn} set max_data_extension_time_in_days = 5"],
            "read": lambda p: str(_param(p, "MAX_DATA_EXTENSION_TIME_IN_DAYS")),
            "before": "5",
            "kind": RESET,
        },
        id="max_data_extension",
    ),
    pytest.param(
        {
            "apply": ["alter dynamic table {fqn} set data_retention_time_in_days = 3"],
            "read": lambda p: str(_table_field(p, "retention_time")),
            "before": "3",
            "kind": RESET,
        },
        id="data_retention",
    ),
    pytest.param(
        {
            "apply": ["alter dynamic table {fqn} set comment = 'keepme'"],
            "read": lambda p: (_table_field(p, "comment") or ""),
            "before": "keepme",
            "kind": RESET,
        },
        id="comment",
    ),
]


class TestCoaAttributeBehavior:
    """One fresh DT per case: set the attribute out of band, do a COA query edit, assert."""

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        yield {"my_seed.csv": SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {"dt_coa.sql": DT_BASE}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return FLAG

    @pytest.fixture(scope="function", autouse=True)
    def fresh_dt(self, project):
        run_dbt(["seed"])
        update_model(project, "dt_coa", DT_BASE)
        run_dbt(["run", "--full-refresh"])
        yield
        project.run_sql(f"drop schema if exists {project.test_schema} cascade")

    @pytest.mark.parametrize("spec", ATTR_SPECS)
    def test_attribute_behavior_across_coa(self, project, spec):
        fqn = _fqn(project)
        for stmt in spec["apply"]:
            project.run_sql(stmt.format(fqn=fqn))

        before = spec["read"](project)
        assert (
            before == spec["before"]
        ), f"setup failed: got {before!r}, expected {spec['before']!r}"

        # trigger a COA via a query-only edit
        update_model(project, "dt_coa", DT_QUERY_EDIT)
        run_dbt(["run"])
        assert "id_x10" in (describe_dynamic_table(project, "dt_coa").query or "").lower()

        after = spec["read"](project)
        if spec["kind"] == PRESERVED:
            assert after == before, f"expected preserved, got {after!r}"
        else:
            assert after != before, f"expected reset, still {after!r}"


def _model(cfg):
    return (
        "{{ config(materialized='dynamic_table', refresh_mode='FULL', scheduler='disable', "
        + cfg
        + ") }}\nselect id, value from {{ ref('my_seed') }}"
    )


# Axis B: dbt config keys our COA emits -> a config change is applied in place via COA.
DT_CFG_BASE = _model("snowflake_warehouse='DBT_TESTING'")

CONFIG_SPECS = [
    pytest.param(
        {
            "v2": _model("snowflake_warehouse='DBT_TESTING_ALT'"),
            "check": lambda dt: dt.snowflake_warehouse == "DBT_TESTING_ALT",
        },
        id="warehouse",
    ),
    pytest.param(
        {
            "v2": _model(
                "snowflake_warehouse='DBT_TESTING', snowflake_initialization_warehouse='DBT_TESTING_ALT'"
            ),
            "check": lambda dt: dt.snowflake_initialization_warehouse == "DBT_TESTING_ALT",
        },
        id="initialization_warehouse",
    ),
    pytest.param(
        {
            "v2": _model("snowflake_warehouse='DBT_TESTING', cluster_by=['id']"),
            "check": lambda dt: "ID" in str(dt.cluster_by or "").upper(),
        },
        id="cluster_by",
    ),
]


class TestCoaConfigChangeApplied:
    """Axis B: changing a dbt-supported config key is applied in place via COA (no --full-refresh)."""

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        yield {"my_seed.csv": SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {"dt_coa.sql": DT_CFG_BASE}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return FLAG

    @pytest.fixture(scope="function", autouse=True)
    def fresh_dt(self, project):
        run_dbt(["seed"])
        update_model(project, "dt_coa", DT_CFG_BASE)
        run_dbt(["run", "--full-refresh"])
        yield
        project.run_sql(f"drop schema if exists {project.test_schema} cascade")

    @pytest.mark.parametrize("spec", CONFIG_SPECS)
    def test_config_change_applied_via_coa(self, project, spec):
        update_model(project, "dt_coa", spec["v2"])
        run_dbt(["run"])  # config-only change -> COA applies it in place
        dt = describe_dynamic_table(project, "dt_coa")
        assert spec["check"](dt), f"config change not applied via COA: {dt}"


class TestCoaMultipleAttributes:
    """Governance + operational + config attrs together: one COA query edit, independent outcomes."""

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        yield {"my_seed.csv": SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        # cluster_by is a config-surface attr -> emitted by COA -> should be applied/kept
        yield {
            "dt_coa.sql": """
{{ config(materialized='dynamic_table', snowflake_warehouse='DBT_TESTING',
          refresh_mode='FULL', scheduler='disable', cluster_by=['id']) }}
select id, value from {{ ref('my_seed') }}
"""
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return FLAG

    @pytest.fixture(scope="function", autouse=True)
    def setup(self, project):
        run_dbt(["seed"])
        run_dbt(["run", "--full-refresh"])
        yield
        project.run_sql(f"drop schema if exists {project.test_schema} cascade")

    def test_mixed_attributes_across_one_coa(self, project):
        fqn = _fqn(project)
        # governance (should be preserved) + operational (should reset)
        project.run_sql(
            "create masking policy if not exists mp as (v varchar) returns varchar -> '***'"
        )
        project.run_sql(f"alter table {fqn} modify column value set masking policy mp")
        project.run_sql("create tag if not exists mytag")
        project.run_sql(f"alter dynamic table {fqn} set tag mytag = 'v1'")
        project.run_sql(f"alter dynamic table {fqn} set data_retention_time_in_days = 3")

        assert "MP" in _policies(project)
        assert "MYTAG" in _tags(project)
        assert str(_table_field(project, "retention_time")) == "3"
        cluster_before = describe_dynamic_table(project, "dt_coa").cluster_by

        # one COA query edit
        update_model(
            project,
            "dt_coa",
            """
{{ config(materialized='dynamic_table', snowflake_warehouse='DBT_TESTING',
          refresh_mode='FULL', scheduler='disable', cluster_by=['id']) }}
select id, value, id * 10 as id_x10 from {{ ref('my_seed') }}
""",
        )
        run_dbt(["run"])

        after = describe_dynamic_table(project, "dt_coa")
        assert "id_x10" in (after.query or "").lower()  # query evolved
        assert "MP" in _policies(project)  # governance preserved
        assert "MYTAG" in _tags(project)  # governance preserved
        assert after.cluster_by == cluster_before  # config-surface kept
        assert str(_table_field(project, "retention_time")) != "3"  # operational reset
