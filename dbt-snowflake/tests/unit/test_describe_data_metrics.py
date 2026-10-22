from types import SimpleNamespace
from unittest import mock

import pytest

from dbt.adapters.snowflake.impl import SnowflakeAdapter
from dbt.adapters.snowflake.relation_configs import SnowflakeDataMetricsConfig


@pytest.mark.parametrize(
    "database,quote_database,expected",
    [
        ("my_db", False, "my_db"),
        ("My Db", True, '"My Db"'),
        ('my"db', True, '"my""db"'),
        ('my""db', True, '"my""""db"'),
    ],
)
def test_database_identifier_is_escaped(database, quote_database, expected):
    relation = SimpleNamespace(
        database=database,
        type="table",
        quote_policy=SimpleNamespace(database=quote_database),
        render=lambda: "my_db.my_schema.my_model",
    )
    references = object()
    adapter = SimpleNamespace(
        execute=mock.Mock(return_value=(SimpleNamespace(code="SUCCESS"), references))
    )

    results = SnowflakeAdapter.describe_data_metrics(
        adapter, relation, SnowflakeDataMetricsConfig(functions=())
    )

    adapter.execute.assert_called_once_with(
        "select metric_database_name, metric_schema_name, metric_name, ref_arguments "
        f"from table({expected}.information_schema.data_metric_function_references("
        "ref_entity_name => 'my_db.my_schema.my_model', ref_entity_domain => 'table'))",
        fetch=True,
    )
    assert results == {"function_references": references}
