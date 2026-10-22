from unittest import mock

import agate
import pytest

from dbt.adapters.snowflake.impl import SnowflakeAdapter
from dbt.adapters.snowflake.relation import SnowflakeRelation


@pytest.mark.parametrize(
    "identifier,quoted,names,expected",
    [
        ("support_search", False, ["SUPPORT_SEARCH"], "SUPPORT_SEARCH"),
        ("SupportSearch", True, ["SupportSearch"], "SupportSearch"),
        ("SupportSearch", True, ["SUPPORTSEARCH"], None),
        ("support_search", False, ["SUPPORTXSEARCH"], None),
        ("support_search", False, [], None),
        ("support_search", False, ["SUPPORTXSEARCH", "SUPPORT_SEARCH"], "SUPPORT_SEARCH"),
    ],
)
def test_service_discovery(identifier, quoted, names, expected):
    adapter = mock.Mock(spec=SnowflakeAdapter)
    adapter.Relation = SnowflakeRelation
    adapter.execute_macro.return_value = agate.Table(
        [[name] for name in names], ["NAME"], column_types=[agate.Text()]
    )
    # A table may exist with the same name, but service lookup must ignore it.
    adapter.get_relation.return_value = SnowflakeRelation.create(
        database="db", schema="sch", identifier=identifier, type=SnowflakeRelation.Table
    )
    relation = SnowflakeRelation.create(
        database="db",
        schema="sch",
        identifier=identifier,
        quote_policy={"identifier": quoted},
    )
    result = SnowflakeAdapter.get_cortex_search_service(adapter, relation)
    adapter.get_relation.assert_not_called()
    adapter.execute_macro.assert_called_once_with(
        "snowflake__show_cortex_search_service", kwargs={"relation": relation.as_case_sensitive()}
    )
    if expected is None:
        assert result is None
    else:
        assert result.identifier == expected
        assert result.is_cortex_search_service
