from types import SimpleNamespace
from unittest import mock

import agate
import pytest
from dbt_common.exceptions import DbtRuntimeError

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


@pytest.mark.parametrize("indexing_error", [None, "Change tracking is not enabled on DOCS"])
def test_describe_service_configuration(indexing_error):
    adapter = mock.Mock(spec=SnowflakeAdapter)
    adapter.execute_macro.return_value = agate.Table(
        [["BODY", "REGION", "WH", "1 hour", indexing_error]],
        ["SEARCH_COLUMN", "ATTRIBUTE_COLUMNS", "WAREHOUSE", "TARGET_LAG", "INDEXING_ERROR"],
        column_types=[agate.Text()] * 5,
    )
    values = {"search_column": "body", "attributes": ["region"], "target_lag": "2 hours"}
    model = SimpleNamespace(config=SimpleNamespace(get=values.get))
    relation = SnowflakeRelation.create(database="db", schema="sch", identifier="search")
    if indexing_error:
        with pytest.raises(
            DbtRuntimeError, match="indexing error.*change tracking.*--full-refresh"
        ):
            SnowflakeAdapter.get_cortex_search_service_configuration_changes(
                adapter, relation, model, "WH"
            )
    else:
        assert SnowflakeAdapter.get_cortex_search_service_configuration_changes(
            adapter, relation, model, "WH"
        ) == {"alter": {"target_lag": "2 hours"}, "replace": []}
    adapter.execute_macro.assert_called_once_with(
        "snowflake__describe_cortex_search_service", kwargs={"relation": relation}
    )
