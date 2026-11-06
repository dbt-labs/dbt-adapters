"""Unit tests for the macros behind the Spark `microbatch` incremental strategy.

They render the real macro files with a minimal Jinja context, so they check the
SQL dbt-spark would send for each batch without needing a Spark cluster.
"""

import re
from unittest import mock

import pytest
from jinja2 import Environment, FileSystemLoader

from dbt.adapters.spark import SparkRelation

MACROS_DIR = "src/dbt/include/spark/macros"


class _Return(Exception):
    """Mimic dbt's `return()`, which stops the macro and hands back a value."""

    def __init__(self, value):
        self.value = value


def _raise_return(value):
    raise _Return(value)


class _CompilerError(Exception):
    pass


def _raise_compiler_error(msg):
    raise _CompilerError(msg)


def _squash(sql):
    return re.sub(r"\s+", " ", str(sql)).strip()


@pytest.fixture
def config():
    return {}


@pytest.fixture
def model():
    m = mock.Mock()
    m.batch = None
    return m


@pytest.fixture
def context(config, model):
    adapter = mock.Mock()
    adapter.get_columns_in_relation.return_value = [
        mock.Mock(quoted="`id`"),
        mock.Mock(quoted="`event_time`"),
        mock.Mock(quoted="`date_day`"),
    ]
    cfg = mock.Mock()
    cfg.get = lambda key, default=None, **kwargs: config.get(key, default)
    target = mock.Mock(endpoint=None)
    return {
        "adapter": adapter,
        "config": cfg,
        "model": model,
        "target": target,
        "validation": mock.Mock(),
        "exceptions": mock.Mock(raise_compiler_error=_raise_compiler_error),
        "return": _raise_return,
    }


@pytest.fixture
def env():
    return Environment(loader=FileSystemLoader(MACROS_DIR), extensions=["jinja2.ext.do"])


def _call(module, macro_name, *args):
    """Call a macro; return its `return()` value, or its rendered output."""
    try:
        return getattr(module, macro_name)(*args)
    except _Return as r:
        return r.value


@pytest.fixture
def macros(env, context):
    """Load the incremental macro files with cross-file macros wired together."""
    adapters = env.get_template("adapters.sql", globals=context).module
    context["partition_cols"] = lambda label, required=False: _call(
        adapters, "spark__partition_cols", label, required
    )
    loaded = {
        name: env.get_template(f"materializations/incremental/{name}.sql", globals=context).module
        for name in ("strategies", "validate", "tmp_relation")
    }
    context["get_insert_overwrite_sql"] = lambda *a: _call(
        loaded["strategies"], "get_insert_overwrite_sql", *a
    )
    return loaded


def _relation(identifier="my_model", is_iceberg=False):
    return SparkRelation.create(schema="analytics", identifier=identifier, is_iceberg=is_iceberg)


def _microbatch_sql(macros, source, target, existing):
    return _squash(
        _call(
            macros["strategies"],
            "dbt_spark_get_incremental_sql",
            "microbatch",
            source,
            target,
            existing,
            None,
            None,
        )
    )


# --- temp relation per batch -------------------------------------------------


@pytest.mark.parametrize(
    "batch_id, expected",
    [
        ("20200102", "my_model__dbt_tmp_20200102"),  # daily batch
        ("2020010213", "my_model__dbt_tmp_2020010213"),  # hourly batch
        ("202001", "my_model__dbt_tmp_202001"),  # monthly batch
    ],
)
def test_tmp_relation_is_unique_per_batch(macros, model, batch_id, expected):
    model.batch = mock.Mock(id=batch_id)
    tmp = _call(
        macros["tmp_relation"], "dbt_spark_get_incremental_tmp_relation", _relation(), "sql"
    )
    assert tmp.render() == expected


def test_tmp_relations_of_concurrent_batches_do_not_collide(macros, model):
    names = set()
    for batch_id in ("20200101", "20200102", "20200103"):
        model.batch = mock.Mock(id=batch_id)
        tmp = _call(
            macros["tmp_relation"], "dbt_spark_get_incremental_tmp_relation", _relation(), "sql"
        )
        names.add(tmp.render())
    assert len(names) == 3


def test_tmp_relation_for_iceberg_model_is_a_plain_view(macros, model):
    # the temp relation is a session view, never an Iceberg table in the catalog
    model.batch = mock.Mock(id="20200102")
    target = _relation(is_iceberg=True)
    tmp = _call(macros["tmp_relation"], "dbt_spark_get_incremental_tmp_relation", target, "sql")
    assert tmp.render() == "my_model__dbt_tmp_20200102"


# --- microbatch strategy SQL -------------------------------------------------


def test_microbatch_parquet_overwrites_the_batch_partition(macros, config):
    config["partition_by"] = ["date_day"]
    target = _relation()
    source = _relation("my_model__dbt_tmp_20200102").include(database=False, schema=False)
    sql = _microbatch_sql(macros, source, target, existing=target)
    assert sql == (
        "insert overwrite table analytics.my_model partition (date_day) "
        "select `id`, `event_time`, `date_day` from my_model__dbt_tmp_20200102"
    )


def test_microbatch_iceberg_uses_dynamic_overwrite_without_partition_clause(macros, config):
    config["partition_by"] = ["date_day"]
    target = _relation(is_iceberg=True)
    source = _relation("my_model__dbt_tmp_20200102").include(database=False, schema=False)
    sql = _microbatch_sql(macros, source, target, existing=target)
    assert sql == (
        "insert overwrite analytics.my_model "
        "select `id`, `event_time`, `date_day` from my_model__dbt_tmp_20200102"
    )


def test_microbatch_iceberg_supports_partition_transforms(macros, config):
    # Iceberg hidden partitioning: the partition spec is not repeated in the insert
    config["partition_by"] = ["days(event_time)"]
    target = _relation(is_iceberg=True)
    source = _relation("my_model__dbt_tmp_20200102").include(database=False, schema=False)
    sql = _microbatch_sql(macros, source, target, existing=target)
    assert "partition" not in sql
    assert sql.startswith("insert overwrite analytics.my_model select")


def test_microbatch_requires_partition_by(macros):
    target = _relation()
    with pytest.raises(_CompilerError, match="requires a `partition_by` config"):
        _microbatch_sql(macros, _relation("tmp"), target, existing=target)


# --- strategy validation -----------------------------------------------------


@pytest.mark.parametrize("file_format", ["parquet", "delta", "iceberg", "hudi"])
def test_microbatch_is_a_valid_strategy(macros, file_format):
    strategy = _call(
        macros["validate"],
        "dbt_spark_validate_get_incremental_strategy",
        "microbatch",
        file_format,
    )
    assert strategy == "microbatch"


def test_microbatch_is_rejected_on_sql_endpoint(macros, context):
    context["target"].endpoint = "my-endpoint"
    with pytest.raises(
        _CompilerError, match="cannot use this strategy when connecting via endpoint"
    ):
        _call(
            macros["validate"],
            "dbt_spark_validate_get_incremental_strategy",
            "microbatch",
            "iceberg",
        )


def test_microbatch_iceberg_from_config_when_relation_format_unknown(macros, config):
    # within one run, batches after the first load the relation dbt cached when the
    # first batch created it; that relation carries no format metadata (is_iceberg=None)
    config["partition_by"] = ["days(event_time)"]
    config["file_format"] = "iceberg"
    target = SparkRelation.create(schema="analytics", identifier="my_model")
    assert target.is_iceberg is None
    source = _relation("my_model__dbt_tmp_20200102").include(database=False, schema=False)
    sql = _microbatch_sql(macros, source, target, existing=target)
    assert sql == (
        "insert overwrite analytics.my_model "
        "select `id`, `event_time`, `date_day` from my_model__dbt_tmp_20200102"
    )


def test_microbatch_trusts_relation_metadata_over_config(macros, config):
    # a known non-Iceberg table keeps the Hive-style overwrite even if the config
    # (not yet applied with --full-refresh) says iceberg
    config["partition_by"] = ["date_day"]
    config["file_format"] = "iceberg"
    target = _relation(is_iceberg=False)
    source = _relation("my_model__dbt_tmp_20200102").include(database=False, schema=False)
    sql = _microbatch_sql(macros, source, target, existing=target)
    assert sql.startswith("insert overwrite table analytics.my_model partition (date_day)")
