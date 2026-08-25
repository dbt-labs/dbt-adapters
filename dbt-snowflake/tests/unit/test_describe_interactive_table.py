from unittest import mock

import agate
import pytest

from dbt.adapters.snowflake.impl import SnowflakeAdapter
from dbt.adapters.snowflake.relation import SnowflakeRelation
from dbt.adapters.snowflake.relation_configs import SnowflakeQuotePolicy
from dbt.adapters.snowflake.relation_configs.interactive_table import INTERACTIVE_TABLE_COLUMNS
from dbt_common.exceptions import DbtRuntimeError


def _show_result(rows):
    """Build an agate.Table mimicking a `SHOW INTERACTIVE TABLES` result set.

    Column types are forced to Text, matching the pattern in
    test_interactive_table_listing.py: agate's default type inference misreads
    small tables of string-ish values (e.g. as Number/Boolean) in ways
    production never hits, since real SHOW results are always normalized to
    Text before any comparison happens.
    """
    keys = list(rows[0].keys())
    column_types = [agate.Text()] * len(keys)
    data = [[row.get(k) for k in keys] for row in rows]
    return agate.Table(data, keys, column_types=column_types)


def _relation(identifier="orders", quote_identifier=False):
    return SnowflakeRelation.create(
        database="my_db",
        schema="my_schema",
        identifier=identifier,
        quote_policy=SnowflakeQuotePolicy(
            database=False, schema=False, identifier=quote_identifier
        ),
    )


def _fake_adapter(show_table):
    fake_response = mock.Mock(code="SUCCESS")
    fake_adapter = mock.Mock()
    fake_adapter.execute.return_value = (fake_response, show_table)
    return fake_adapter


def _describe(fake_adapter, relation):
    return SnowflakeAdapter.describe_interactive_table(fake_adapter, relation)


def test_happy_path_returns_expected_dict_shape():
    show_table = _show_result(
        [
            {
                "name": "ORDERS",
                "schema_name": "MY_SCHEMA",
                "database_name": "MY_DB",
                "text": "create interactive table orders ...",
                "target_lag": "1 hour",
                "refresh_warehouse": "WH1",
                "initialization_warehouse": "WH1",
                "cluster_by": "LINEAR(ID)",
            }
        ]
    )
    relation = _relation(identifier="orders", quote_identifier=False)

    result = _describe(_fake_adapter(show_table), relation)

    assert set(result.keys()) == {"interactive_table"}
    table = result["interactive_table"]
    assert list(table.column_names) == list(INTERACTIVE_TABLE_COLUMNS)
    row = table.rows[0]
    assert row["name"] == "ORDERS"
    assert row["cluster_by"] == "LINEAR(ID)"


def test_multi_row_show_result_is_filtered_to_exact_match():
    """SHOW ... LIKE 'orders' pattern-matches, so a false-positive like
    `orders_backup` must be filtered out, leaving only the exact match."""
    show_table = _show_result(
        [
            {
                "name": "ORDERS",
                "schema_name": "MY_SCHEMA",
                "database_name": "MY_DB",
                "text": "t1",
                "target_lag": None,
                "refresh_warehouse": None,
                "initialization_warehouse": None,
                "cluster_by": "(ID)",
            },
            {
                "name": "ORDERS_BACKUP",
                "schema_name": "MY_SCHEMA",
                "database_name": "MY_DB",
                "text": "t2",
                "target_lag": None,
                "refresh_warehouse": None,
                "initialization_warehouse": None,
                "cluster_by": "(ID)",
            },
        ]
    )
    relation = _relation(identifier="orders")

    result = _describe(_fake_adapter(show_table), relation)

    table = result["interactive_table"]
    assert len(table.rows) == 1
    assert table.rows[0]["name"] == "ORDERS"


def test_unquoted_identifier_matches_case_insensitively():
    """Unquoted identifiers are stored upper-case by Snowflake, so the model's
    lower/mixed-case identifier must still match the upper-case SHOW result."""
    show_table = _show_result(
        [
            {
                "name": "ORDERS",
                "schema_name": "MY_SCHEMA",
                "database_name": "MY_DB",
                "text": "t1",
                "target_lag": None,
                "refresh_warehouse": None,
                "initialization_warehouse": None,
                "cluster_by": "(ID)",
            }
        ]
    )
    relation = _relation(identifier="Orders", quote_identifier=False)

    result = _describe(_fake_adapter(show_table), relation)

    table = result["interactive_table"]
    assert len(table.rows) == 1
    assert table.rows[0]["name"] == "ORDERS"


def test_quoted_identifier_requires_exact_case_match():
    """A quoted identifier is case-sensitive: a differently-cased row in the
    SHOW result must NOT match."""
    show_table = _show_result(
        [
            {
                "name": "MixedCase",
                "schema_name": "MY_SCHEMA",
                "database_name": "MY_DB",
                "text": "t1",
                "target_lag": None,
                "refresh_warehouse": None,
                "initialization_warehouse": None,
                "cluster_by": "(ID)",
            },
            {
                "name": "MIXEDCASE",
                "schema_name": "MY_SCHEMA",
                "database_name": "MY_DB",
                "text": "t2",
                "target_lag": None,
                "refresh_warehouse": None,
                "initialization_warehouse": None,
                "cluster_by": "(ID)",
            },
        ]
    )
    relation = _relation(identifier="MixedCase", quote_identifier=True)

    result = _describe(_fake_adapter(show_table), relation)

    table = result["interactive_table"]
    assert len(table.rows) == 1
    assert table.rows[0]["name"] == "MixedCase"


def test_no_matching_row_raises_dbt_runtime_error():
    show_table = _show_result(
        [
            {
                "name": "SOME_OTHER_TABLE",
                "schema_name": "MY_SCHEMA",
                "database_name": "MY_DB",
                "text": "t1",
                "target_lag": None,
                "refresh_warehouse": None,
                "initialization_warehouse": None,
                "cluster_by": "(ID)",
            }
        ]
    )
    relation = _relation(identifier="orders")

    with pytest.raises(DbtRuntimeError):
        _describe(_fake_adapter(show_table), relation)


def test_missing_column_is_omitted_not_a_keyerror():
    """An account without a given feature flag may not return every column
    (e.g. `initialization_warehouse`); this must be silently omitted from the
    selected columns rather than raising a KeyError."""
    show_table = _show_result(
        [
            {
                "name": "ORDERS",
                "schema_name": "MY_SCHEMA",
                "database_name": "MY_DB",
                "text": "t1",
                "target_lag": None,
                "refresh_warehouse": None,
                "cluster_by": "(ID)",
                # no "initialization_warehouse" column at all
            }
        ]
    )
    relation = _relation(identifier="orders")

    result = _describe(_fake_adapter(show_table), relation)

    table = result["interactive_table"]
    assert "initialization_warehouse" not in table.column_names
    assert set(table.column_names) == set(INTERACTIVE_TABLE_COLUMNS) - {"initialization_warehouse"}
