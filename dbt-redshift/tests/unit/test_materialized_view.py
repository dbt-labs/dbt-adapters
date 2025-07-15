from unittest.mock import Mock

import agate
import pytest

from dbt.adapters.redshift.relation_configs import RedshiftMaterializedViewConfig


@pytest.mark.parametrize("bool_value", [True, False, "True", "False", "true", "false"])
def test_redshift_materialized_view_config_handles_all_valid_bools(bool_value):
    config = RedshiftMaterializedViewConfig(
        database_name="somedb",
        schema_name="public",
        mv_name="someview",
        query="select * from sometable",
    )
    model_node = Mock()
    model_node.config.extra.get = lambda x, y=None: (
        bool_value if x in ["auto_refresh", "backup"] else "someDistValue"
    )
    config_dict = config.parse_relation_config(model_node)
    assert isinstance(config_dict["autorefresh"], bool)
    assert isinstance(config_dict["backup"], bool)


@pytest.mark.parametrize("bool_value", [1])
def test_redshift_materialized_view_config_throws_expected_exception_with_invalid_types(
    bool_value,
):
    config = RedshiftMaterializedViewConfig(
        database_name="somedb",
        schema_name="public",
        mv_name="someview",
        query="select * from sometable",
    )
    model_node = Mock()
    model_node.config.extra.get = lambda x, y=None: (
        bool_value if x in ["auto_refresh", "backup"] else "someDistValue"
    )
    with pytest.raises(TypeError):
        config.parse_relation_config(model_node)


def test_redshift_materialized_view_config_throws_expected_exception_with_invalid_str():
    config = RedshiftMaterializedViewConfig(
        database_name="somedb",
        schema_name="public",
        mv_name="someview",
        query="select * from sometable",
    )
    model_node = Mock()
    model_node.config.extra.get = lambda x, y=None: (
        "notABool" if x in ["auto_refresh", "backup"] else "someDistValue"
    )
    with pytest.raises(ValueError):
        config.parse_relation_config(model_node)


def test_redshift_materialized_view_parse_relation_results_handles_multiples_sort_key():
    materialized_view = agate.Table.from_object(
        [],
        [
            "database",
            "schema",
            "table",
            "diststyle",
            "sortkey1",
            "autorefresh",
        ],
    )

    column_descriptor = agate.Table.from_object(
        [
            {
                "column": "my_column",
                "is_dist_key": True,
                "sort_key_position": 1,
            },
            {
                "column": "my_column2",
                "is_dist_key": True,
                "sort_key_position": 2,
            },
            {
                "column": "my_column5",
                "is_dist_key": False,
                "sort_key_position": 0,
            },
        ],
    )

    query = agate.Table.from_object(
        [
            {
                "definition": "create materialized view my_view as (select 1 as my_column, 'value' as my_column2)"
            }
        ]
    )

    relation_results = {
        "materialized_view": materialized_view,
        "columns": column_descriptor,
        "query": query,
    }

    config_dict = RedshiftMaterializedViewConfig.parse_relation_results(relation_results)

    assert isinstance(config_dict["sort"], dict)
    assert config_dict["sort"]["sortkey"][0] == "my_column"
    assert config_dict["sort"]["sortkey"][1] == "my_column2"
