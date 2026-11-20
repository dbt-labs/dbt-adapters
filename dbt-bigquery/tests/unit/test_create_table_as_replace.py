"""Render the BigQuery CTAS macros and check when they emit `create or replace table`.

`create or replace` is kept for every caller except the ones that already know the target does
not exist (incremental first build, snapshot first build). Those emit `create table` so an inconsistent
relation cache fails loudly instead of silently replacing a live table.
"""

import re
from types import SimpleNamespace
from unittest import mock

import pytest
from dbt_common.clients.jinja import MaterializationExtension
from jinja2 import Environment, FileSystemLoader

MACROS_DIR = "src/dbt/include/bigquery/macros"
RELATION = "`proj`.`dataset`.`my_table`"
SQL = "select 1 as id"


def _normalize(sql):
    return re.sub(r"\s+", " ", str(sql)).strip()


@pytest.fixture
def jinja_env():
    # MaterializationExtension parses the `{% materialization %}` block in incremental.sql while
    # keeping macro names unprefixed, so sibling macros resolve as they do in dbt.
    return Environment(
        loader=FileSystemLoader(MACROS_DIR),
        extensions=[MaterializationExtension, "jinja2.ext.do"],
    )


@pytest.fixture
def context():
    config = mock.Mock()
    config.get = lambda key, default=None, **kwargs: (
        {"enforced": False} if key == "contract" else default
    )
    adapter = mock.Mock()
    adapter.parse_partition_by.return_value = SimpleNamespace(time_ingestion_partitioning=False)
    adapter.build_catalog_relation.return_value = SimpleNamespace(table_format="default")
    return {
        "config": config,
        "adapter": adapter,
        "model": {"resource_type": "model"},
        "partition_by": lambda partition_config: "",
        "cluster_by": lambda raw_cluster_by: "",
        "bigquery_table_options": lambda config, model, temporary: "",
        "create_table_as": mock.Mock(return_value="<global create_table_as>"),
        "return": lambda value: value,
    }


def _adapters_module(jinja_env, context):
    return jinja_env.get_template("adapters.sql", globals=context).module


class TestBigQueryCreateTableAs:
    def test_replaces_by_default(self, jinja_env, context):
        module = _adapters_module(jinja_env, context)
        sql = _normalize(module.bigquery__create_table_as(False, RELATION, SQL))
        assert sql.startswith(f"create or replace table {RELATION}")

    def test_replace_false_emits_create_table(self, jinja_env, context):
        module = _adapters_module(jinja_env, context)
        sql = _normalize(module.bigquery__create_table_as(False, RELATION, SQL, replace=False))
        assert sql.startswith(f"create table {RELATION}")

    def test_snapshot_target_is_never_replaced(self, jinja_env, context):
        # The default snapshot materialization only builds its non-temporary target on the
        # first build, when the relation cache says the target does not exist.
        context["model"] = {"resource_type": "snapshot"}
        module = _adapters_module(jinja_env, context)
        sql = _normalize(module.bigquery__create_table_as(False, RELATION, SQL))
        assert sql.startswith(f"create table {RELATION}")

    def test_snapshot_staging_table_still_replaces(self, jinja_env, context):
        context["model"] = {"resource_type": "snapshot"}
        module = _adapters_module(jinja_env, context)
        sql = _normalize(module.bigquery__create_table_as(True, RELATION, SQL))
        assert sql.startswith(f"create or replace table {RELATION}")

    def test_without_model_replaces(self, jinja_env, context):
        # `model` is undefined outside a node, e.g. in run-operation and on-run hooks.
        del context["model"]
        module = _adapters_module(jinja_env, context)
        sql = _normalize(module.bigquery__create_table_as(False, RELATION, SQL))
        assert sql.startswith(f"create or replace table {RELATION}")


class TestBqCreateTableAsWithReplace:
    def test_replace_true_goes_through_create_table_as(self, jinja_env, context):
        module = _adapters_module(jinja_env, context)
        sql = _normalize(module.bq_create_table_as_with_replace(False, RELATION, SQL, "sql"))
        assert sql == "<global create_table_as>"
        context["create_table_as"].assert_called_once_with(False, RELATION, SQL, "sql")

    def test_replace_false_emits_create_table(self, jinja_env, context):
        module = _adapters_module(jinja_env, context)
        sql = _normalize(
            module.bq_create_table_as_with_replace(False, RELATION, SQL, "sql", replace=False)
        )
        assert sql.startswith(f"create table {RELATION}")
        context["create_table_as"].assert_not_called()


class TestBqCreateTableAs:
    @pytest.fixture
    def incremental_module(self, jinja_env, context):
        adapters = _adapters_module(jinja_env, context)
        context.update(
            {
                "bq_create_table_as_with_replace": adapters.bq_create_table_as_with_replace,
                "declare_dbt_max_partition": lambda this, partition_by, sql, language: "",
                "this": RELATION,
            }
        )
        return jinja_env.get_template("materializations/incremental.sql", globals=context).module

    @pytest.fixture
    def partition_by(self):
        return SimpleNamespace(time_ingestion_partitioning=False)

    def test_replaces_by_default(self, incremental_module, context, partition_by):
        sql = _normalize(incremental_module.bq_create_table_as(partition_by, False, RELATION, SQL))
        assert sql == "<global create_table_as>"
        context["create_table_as"].assert_called_once_with(False, RELATION, SQL, "sql")

    def test_forwards_replace_false(self, incremental_module, context, partition_by):
        sql = _normalize(
            incremental_module.bq_create_table_as(
                partition_by, False, RELATION, SQL, "sql", replace=False
            )
        )
        assert sql.startswith(f"create table {RELATION}")
        context["create_table_as"].assert_not_called()
