import boto3
import pytest
from pydantic import ValidationError
from tests.unit.constants import AWS_REGION, DATA_CATALOG_NAME, DATABASE_NAME

import dbt.adapters.athena.lakeformation as lakeformation
from dbt.adapters.athena.lakeformation import (
    FilterConfig,
    LfTagsConfig,
    LfTagsManager,
)
from dbt.adapters.athena.relation import AthenaRelation


# TODO: add more tests for lakeformation once moto library implements required methods:
# https://docs.getmoto.org/en/latest/docs/services/lakeformation.html
# get_resource_lf_tags
# create_data_cells_filter, delete_data_cells_filter, update_data_cells_filter
class TestLfTagsManager:
    @pytest.mark.parametrize(
        "response,identifier,columns,lf_tags,verb,expected",
        [
            pytest.param(
                {
                    "Failures": [
                        {
                            "LFTag": {
                                "CatalogId": "test_catalog",
                                "TagKey": "test_key",
                                "TagValues": ["test_values"],
                            },
                            "Error": {"ErrorCode": "test_code", "ErrorMessage": "test_err_msg"},
                        }
                    ]
                },
                "tbl_name",
                ["column1", "column2"],
                {"tag_key": "tag_value"},
                "add",
                None,
                id="lf_tag error",
                marks=pytest.mark.xfail,
            ),
            pytest.param(
                {"Failures": []},
                "tbl_name",
                None,
                {"tag_key": "tag_value"},
                "add",
                "Success: add LF tags {'tag_key': 'tag_value'} to test_dbt_athena.tbl_name",
                id="add lf_tag",
            ),
            pytest.param(
                {"Failures": []},
                None,
                None,
                {"tag_key": "tag_value"},
                "add",
                "Success: add LF tags {'tag_key': 'tag_value'} to test_dbt_athena",
                id="add lf_tag_to_database",
            ),
            pytest.param(
                {"Failures": []},
                "tbl_name",
                None,
                {"tag_key": ["tag_value"]},
                "remove",
                "Success: remove LF tags {'tag_key': ['tag_value']} to test_dbt_athena.tbl_name",
                id="remove lf_tag",
            ),
            pytest.param(
                {"Failures": []},
                "tbl_name",
                ["c1", "c2"],
                {"tag_key": "tag_value"},
                "add",
                "Success: add LF tags {'tag_key': 'tag_value'} to test_dbt_athena.tbl_name for columns ['c1', 'c2']",
                id="lf_tag database table and columns",
            ),
        ],
    )
    def test__parse_lf_response(
        self, dbt_debug_caplog, response, identifier, columns, lf_tags, verb, expected
    ):
        relation = AthenaRelation.create(
            database=DATA_CATALOG_NAME, schema=DATABASE_NAME, identifier=identifier
        )
        lf_client = boto3.client("lakeformation", region_name=AWS_REGION)
        manager = LfTagsManager(lf_client, relation, LfTagsConfig())
        manager._parse_and_log_lf_response(response, columns, lf_tags, verb)
        assert expected in dbt_debug_caplog.getvalue()

    @pytest.mark.parametrize(
        "lf_tags_columns,lf_inherited_tags,expected",
        [
            pytest.param(
                [
                    {
                        "Name": "my_column",
                        "LFTags": [{"TagKey": "inherited", "TagValues": ["oh-yes-i-am"]}],
                    }
                ],
                {"inherited"},
                {},
                id="retains-inherited-tag",
            ),
            pytest.param(
                [
                    {
                        "Name": "my_column",
                        "LFTags": [{"TagKey": "not-inherited", "TagValues": ["oh-no-im-not"]}],
                    }
                ],
                {},
                {"not-inherited": {"oh-no-im-not": ["my_column"]}},
                id="removes-non-inherited-tag",
            ),
            pytest.param(
                [
                    {
                        "Name": "my_column",
                        "LFTags": [
                            {"TagKey": "not-inherited", "TagValues": ["oh-no-im-not"]},
                            {"TagKey": "inherited", "TagValues": ["oh-yes-i-am"]},
                        ],
                    }
                ],
                {"inherited"},
                {"not-inherited": {"oh-no-im-not": ["my_column"]}},
                id="removes-non-inherited-tag-among-inherited",
            ),
            pytest.param([], {}, {}, id="handles-empty"),
        ],
    )
    def test__column_tags_to_remove(self, lf_tags_columns, lf_inherited_tags, expected):
        assert (
            lakeformation.LfTagsManager._column_tags_to_remove(lf_tags_columns, lf_inherited_tags)
            == expected
        )

    @pytest.mark.parametrize(
        "lf_tags_table,lf_tags,lf_inherited_tags,expected",
        [
            pytest.param(
                [
                    {"TagKey": "not-inherited", "TagValues": ["oh-no-im-not"]},
                    {"TagKey": "inherited", "TagValues": ["oh-yes-i-am"]},
                ],
                {"not-inherited": "some-preexisting-value"},
                {"inherited"},
                {},
                id="retains-being-set-and-inherited",
            ),
            pytest.param(
                [
                    {"TagKey": "not-inherited", "TagValues": ["oh-no-im-not"]},
                    {"TagKey": "inherited", "TagValues": ["oh-yes-i-am"]},
                ],
                {},
                {"inherited"},
                {"not-inherited": ["oh-no-im-not"]},
                id="removes-preexisting-not-being-set",
            ),
            pytest.param(
                [{"TagKey": "inherited", "TagValues": ["oh-yes-i-am"]}],
                {},
                {"inherited"},
                {},
                id="retains-inherited",
            ),
            pytest.param([], None, {}, {}, id="handles-empty"),
        ],
    )
    def test__table_tags_to_remove(self, lf_tags_table, lf_tags, lf_inherited_tags, expected):
        assert (
            lakeformation.LfTagsManager._table_tags_to_remove(
                lf_tags_table, lf_tags, lf_inherited_tags
            )
            == expected
        )


class TestFilterConfig:
    @pytest.mark.parametrize(
        "config_data",
        [
            pytest.param(
                {"row_filter": "col1=1", "all_rows": True}, id="both row_filter and all_rows"
            ),
            pytest.param({}, id="neither row_filter nor all_rows"),
            pytest.param({"row_filter": None, "all_rows": False}, id="None row_filter"),
            pytest.param({"row_filter": " ", "all_rows": False}, id="empty row_filter"),
        ],
    )
    def test_filter_config_validation_fails(self, config_data):
        """Tests that validator fails when row filter logic is invalid."""
        with pytest.raises(ValidationError):
            FilterConfig(**config_data)

    @pytest.mark.parametrize(
        "config_data",
        [
            pytest.param({"row_filter": "col1=1"}, id="row_filter only"),
            pytest.param({"all_rows": True}, id="all_rows only"),
            pytest.param(
                {"row_filter": "col1=1", "all_rows": False}, id="row_filter and all_rows=False"
            ),
        ],
    )
    def test_filter_config_validation_succeeds(self, config_data):
        """Tests that validator succeeds when row filter logic is valid."""
        FilterConfig(**config_data)  # No exception should be raised

    def test_to_api_repr(self):
        """Tests the `to_api_repr` method for all row and column combinations."""
        base_args = ("cat_id", "db", "tbl", "filter_name")

        # Case 1: All rows, All columns
        cfg = FilterConfig(all_rows=True)
        expected = {
            "TableCatalogId": "cat_id",
            "DatabaseName": "db",
            "TableName": "tbl",
            "Name": "filter_name",
            "RowFilter": {"AllRowsWildcard": {}},
            "ColumnWildcard": {"ExcludedColumnNames": []},
        }
        assert cfg.to_api_repr(*base_args) == expected

        # Case 2: Filter expression, All columns
        cfg = FilterConfig(row_filter="col1=1")
        expected["RowFilter"] = {"FilterExpression": "col1=1"}
        assert cfg.to_api_repr(*base_args) == expected

        # Case 3: Filter expression, Specific columns
        cfg = FilterConfig(row_filter="col1=1", column_names=["col1", "col2"])
        expected.pop("ColumnWildcard")
        expected["ColumnNames"] = ["col1", "col2"]
        assert cfg.to_api_repr(*base_args) == expected

        # Case 4: Filter expression, Excluded columns
        cfg = FilterConfig(row_filter="col1=1", excluded_column_names=["col3"])
        expected.pop("ColumnNames")
        expected["ColumnWildcard"] = {"ExcludedColumnNames": ["col3"]}
        assert cfg.to_api_repr(*base_args) == expected

    @pytest.mark.parametrize(
        "config,existing,should_update",
        [
            # Row filter changes
            pytest.param(
                FilterConfig(all_rows=True),
                {
                    "RowFilter": {"FilterExpression": "c=1"},
                    "ColumnWildcard": {"ExcludedColumnNames": []},
                },
                True,
                id="row_filter -> all_rows",
            ),
            pytest.param(
                FilterConfig(row_filter="c=1"),
                {
                    "RowFilter": {"AllRowsWildcard": {}},
                    "ColumnWildcard": {"ExcludedColumnNames": []},
                },
                True,
                id="all_rows -> row_filter",
            ),
            pytest.param(
                FilterConfig(row_filter="c=2"),
                {
                    "RowFilter": {"FilterExpression": "c=1"},
                    "ColumnWildcard": {"ExcludedColumnNames": []},
                },
                True,
                id="row_filter -> different row_filter",
            ),
            # Column changes
            pytest.param(
                FilterConfig(row_filter="c=1", column_names=["c1"]),
                {
                    "RowFilter": {"FilterExpression": "c=1"},
                    "ColumnWildcard": {"ExcludedColumnNames": []},
                },
                True,
                id="all_cols (wildcard) -> specific_cols",
            ),
            pytest.param(
                FilterConfig(row_filter="c=1", excluded_column_names=["c2"]),
                {"RowFilter": {"FilterExpression": "c=1"}, "ColumnNames": ["c1"]},
                True,
                id="specific_cols -> excluded_cols",
            ),
            pytest.param(
                FilterConfig(row_filter="c=1"),
                {"RowFilter": {"FilterExpression": "c=1"}, "ColumnNames": ["c1"]},
                True,
                id="specific_cols -> all_cols (wildcard)",
            ),
            pytest.param(
                FilterConfig(row_filter="c=1", column_names=["c1", "c2"]),
                {"RowFilter": {"FilterExpression": "c=1"}, "ColumnNames": ["c1"]},
                True,
                id="specific_cols -> different specific_cols",
            ),
            pytest.param(
                FilterConfig(row_filter="c=1", excluded_column_names=["c3"]),
                {
                    "RowFilter": {"FilterExpression": "c=1"},
                    "ColumnWildcard": {"ExcludedColumnNames": ["c2"]},
                },
                True,
                id="excluded_cols -> different excluded_cols",
            ),
            # No changes
            pytest.param(
                FilterConfig(row_filter="c=1"),
                {
                    "RowFilter": {"FilterExpression": "c=1"},
                    "ColumnWildcard": {"ExcludedColumnNames": []},
                },
                False,
                id="no change: row_filter, all_cols",
            ),
            pytest.param(
                FilterConfig(all_rows=True),
                {
                    "RowFilter": {"AllRowsWildcard": {}},
                    "ColumnWildcard": {"ExcludedColumnNames": []},
                },
                False,
                id="no change: all_rows, all_cols",
            ),
            pytest.param(
                FilterConfig(row_filter="c=1", column_names=["c1", "c2"]),
                {"RowFilter": {"FilterExpression": "c=1"}, "ColumnNames": ["c1", "c2"]},
                False,
                id="no change: row_filter, specific_cols",
            ),
            pytest.param(
                FilterConfig(row_filter="c=1", excluded_column_names=["c3"]),
                {
                    "RowFilter": {"FilterExpression": "c=1"},
                    "ColumnWildcard": {"ExcludedColumnNames": ["c3"]},
                },
                False,
                id="no change: row_filter, excluded_cols",
            ),
        ],
    )
    def test_to_update(self, config, existing, should_update):
        """Tests the `to_update` logic against various existing filter states."""
        assert config.to_update(existing) == should_update
