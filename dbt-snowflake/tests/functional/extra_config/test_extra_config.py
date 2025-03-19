import os

import pytest
import re

from dbt.tests.util import run_dbt


def get_cleanded_model_ddl_from_file(file_name: str) -> str:
    with open(f"target/run/test/models/{file_name}", "r") as ddl_file:
        return re.sub(r"\s+", " ", ddl_file.read())


_MODELS_TABLE_ROW_ACCESS_POLICY = """
{{ config(
    materialized = 'table',
    row_access_policy = 'row_access_policy_name on (id)',
) }}

select 1 as id
"""
_DDL_MODELS_TABLE_ROW_ACCESS_POLICY = (
    """.table_row_access_policy with row access policy row_access_policy_name on (id) as"""
)

_MODELS_TABLE_TAG = """
{{ config(
    materialized = 'table',
    table_tag = "tag_name = 'tag_value'",
) }}

select 1 as id
"""
_DDL_MODELS_TABLE_TAG = """.table_tag with tag (tag_name = 'tag_value')"""

_MODELS_TABLE_TAG_AND_ROW_ACCESS_POLICY = """
{{ config(
    materialized = 'table',
    table_tag = "tag_name = 'tag_value'",
    row_access_policy = 'row_access_policy_name on (id)',
) }}

select 1 as id
"""
_DDL_MODELS_TABLE_TAG_AND_ROW_ACCESS_POLICY = """.table_tag_and_row_access_policy with row access policy row_access_policy_name on (id) with tag (tag_name = 'tag_value') as"""

# View model templates
_MODELS_VIEW_ROW_ACCESS_POLICY = """
{{ config(
    materialized = 'view',
    row_access_policy = 'row_access_policy_name on (id)',
) }}

select 1 as id
"""
_DDL_MODELS_VIEW_ROW_ACCESS_POLICY = (
    """.view_row_access_policy with row access policy row_access_policy_name on (id) as"""
)

_MODELS_VIEW_TAG = """
{{ config(
    materialized = 'view',
    table_tag = "tag_name = 'tag_value'",
) }}

select 1 as id
"""
_DDL_MODELS_VIEW_TAG = """.view_tag with tag (tag_name = 'tag_value')"""

_MODELS_VIEW_TAG_AND_ROW_ACCESS_POLICY = """
{{ config(
    materialized = 'view',
    table_tag = "tag_name = 'tag_value'",
    row_access_policy = 'row_access_policy_name on (id)',
) }}

select 1 as id
"""
_DDL_MODELS_VIEW_TAG_AND_ROW_ACCESS_POLICY = """.view_tag_and_row_access_policy with row access policy row_access_policy_name on (id) with tag (tag_name = 'tag_value') as"""

# Incremental model templates
_MODELS_INCREMENTAL_ROW_ACCESS_POLICY = """
{{ config(
    materialized = 'incremental',
    row_access_policy = 'row_access_policy_name on (id)',
) }}

select 1 as id
"""
_DDL_MODELS_INCREMENTAL_ROW_ACCESS_POLICY = (
    """.incremental_row_access_policy with row access policy row_access_policy_name on (id) as"""
)

_MODELS_INCREMENTAL_TAG = """
{{ config(
    materialized = 'incremental',
    table_tag = "tag_name = 'tag_value'",
) }}

select 1 as id
"""
_DDL_MODELS_INCREMENTAL_TAG = """.incremental_tag with tag (tag_name = 'tag_value')"""

_MODELS_INCREMENTAL_TAG_AND_ROW_ACCESS_POLICY = """
{{ config(
    materialized = 'incremental',
    table_tag = "tag_name = 'tag_value'",
    row_access_policy = 'row_access_policy_name on (id)',
) }}

select 1 as id
"""
_DDL_MODELS_INCREMENTAL_TAG_AND_ROW_ACCESS_POLICY = """.incremental_tag_and_row_access_policy with row access policy row_access_policy_name on (id) with tag (tag_name = 'tag_value') as"""

# Dynamic table model templates
_MODELS_DYNAMIC_TABLE_ROW_ACCESS_POLICY = (
    """
{{ config(
    target_lag = '7 days',
    snowflake_warehouse = '"""
    + os.getenv("SNOWFLAKE_TEST_WAREHOUSE", "")
    + """',
    materialized = 'dynamic_table',
    row_access_policy = 'row_access_policy_name on (id)',
) }}

select 1 as id
"""
)
_DDL_MODELS_DYNAMIC_TABLE_ROW_ACCESS_POLICY = (
    r"\.dynamic_table_row_access_policy.*with row access policy row_access_policy_name on \(id\)"
)

_MODELS_DYNAMIC_TABLE_TAG = (
    """
{{ config(
    target_lag = '7 days',
    snowflake_warehouse = '"""
    + os.getenv("SNOWFLAKE_TEST_WAREHOUSE", "")
    + """',
    materialized = 'dynamic_table',
    table_tag = "tag_name = 'tag_value'",
) }}

select 1 as id
"""
)
_DDL_MODELS_DYNAMIC_TABLE_TAG = r"\.dynamic_table_tag.*with tag \(tag_name = 'tag_value'\)"

_MODELS_DYNAMIC_TABLE_TAG_AND_ROW_ACCESS_POLICY = (
    """
{{ config(
    target_lag = '7 days',
    snowflake_warehouse = '"""
    + os.getenv("SNOWFLAKE_TEST_WAREHOUSE", "")
    + """',
    materialized = 'dynamic_table',
    table_tag = "tag_name = 'tag_value'",
    row_access_policy = 'row_access_policy_name on (id)',
) }}

select 1 as id
"""
)
_DDL_MODELS_DYNAMIC_TABLE_TAG_AND_ROW_ACCESS_POLICY = r"\.dynamic_table_tag_and_row_access_policy.*with row access policy row_access_policy_name on \(id\).*with tag \(tag_name = 'tag_value'\)"


class TestExtraConfig:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            # Table models
            "table_row_access_policy.sql": _MODELS_TABLE_ROW_ACCESS_POLICY,
            "table_tag.sql": _MODELS_TABLE_TAG,
            "table_tag_and_row_access_policy.sql": _MODELS_TABLE_TAG_AND_ROW_ACCESS_POLICY,
            # View models
            "view_row_access_policy.sql": _MODELS_VIEW_ROW_ACCESS_POLICY,
            "view_tag.sql": _MODELS_VIEW_TAG,
            "view_tag_and_row_access_policy.sql": _MODELS_VIEW_TAG_AND_ROW_ACCESS_POLICY,
            # Incremental models
            "incremental_row_access_policy.sql": _MODELS_INCREMENTAL_ROW_ACCESS_POLICY,
            "incremental_tag.sql": _MODELS_INCREMENTAL_TAG,
            "incremental_tag_and_row_access_policy.sql": _MODELS_INCREMENTAL_TAG_AND_ROW_ACCESS_POLICY,
            # Dynamic table models
            "dynamic_table_row_access_policy.sql": _MODELS_DYNAMIC_TABLE_ROW_ACCESS_POLICY,
            "dynamic_table_tag.sql": _MODELS_DYNAMIC_TABLE_TAG,
            "dynamic_table_tag_and_row_access_policy.sql": _MODELS_DYNAMIC_TABLE_TAG_AND_ROW_ACCESS_POLICY,
        }

    def test_extra_config_table(self, project):
        # depending on the Snowflake edition tags and row access policies are supported so we check the DDL sent
        results = run_dbt(["run", "--select", "table_*"], expect_pass=None)
        assert len(results) == 3

        assert _DDL_MODELS_TABLE_ROW_ACCESS_POLICY in get_cleanded_model_ddl_from_file(
            "table_row_access_policy.sql"
        )
        assert _DDL_MODELS_TABLE_TAG in get_cleanded_model_ddl_from_file("table_tag.sql")
        assert _DDL_MODELS_TABLE_TAG_AND_ROW_ACCESS_POLICY in get_cleanded_model_ddl_from_file(
            "table_tag_and_row_access_policy.sql"
        )

    def test_extra_config_view(self, project):
        # Test view models with row access policy and tags
        results = run_dbt(["run", "--select", "view_*"], expect_pass=None)
        assert len(results) == 3

        assert _DDL_MODELS_VIEW_ROW_ACCESS_POLICY in get_cleanded_model_ddl_from_file(
            "view_row_access_policy.sql"
        )
        assert _DDL_MODELS_VIEW_TAG in get_cleanded_model_ddl_from_file("view_tag.sql")
        assert _DDL_MODELS_VIEW_TAG_AND_ROW_ACCESS_POLICY in get_cleanded_model_ddl_from_file(
            "view_tag_and_row_access_policy.sql"
        )

    def test_extra_config_incremental(self, project):
        # Test incremental models with row access policy and tags
        results = run_dbt(["run", "--select", "incremental_*"], expect_pass=None)
        assert len(results) == 3

        assert _DDL_MODELS_INCREMENTAL_ROW_ACCESS_POLICY in get_cleanded_model_ddl_from_file(
            "incremental_row_access_policy.sql"
        )
        assert _DDL_MODELS_INCREMENTAL_TAG in get_cleanded_model_ddl_from_file(
            "incremental_tag.sql"
        )
        assert (
            _DDL_MODELS_INCREMENTAL_TAG_AND_ROW_ACCESS_POLICY
            in get_cleanded_model_ddl_from_file("incremental_tag_and_row_access_policy.sql")
        )

    def test_extra_config_dynamic_table(self, project):
        # Test dynamic table models with row access policy and tags
        results = run_dbt(["run", "--select", "dynamic_table_*"], expect_pass=None)
        assert len(results) == 3

        assert re.search(
            _DDL_MODELS_DYNAMIC_TABLE_ROW_ACCESS_POLICY,
            get_cleanded_model_ddl_from_file("dynamic_table_row_access_policy.sql"),
        )
        assert re.search(
            _DDL_MODELS_DYNAMIC_TABLE_TAG,
            get_cleanded_model_ddl_from_file("dynamic_table_tag.sql"),
        )
        assert re.search(
            _DDL_MODELS_DYNAMIC_TABLE_TAG_AND_ROW_ACCESS_POLICY,
            get_cleanded_model_ddl_from_file("dynamic_table_tag_and_row_access_policy.sql"),
        )

    def test_extra_config_dynamic_table_full_refresh(self, project):
        # Test dynamic table models with row access policy and tags using full refresh
        results = run_dbt(
            ["run", "--select", "dynamic_table_*", "--full-refresh"], expect_pass=None
        )
        assert len(results) == 3

        assert re.search(
            _DDL_MODELS_DYNAMIC_TABLE_ROW_ACCESS_POLICY,
            get_cleanded_model_ddl_from_file("dynamic_table_row_access_policy.sql"),
        )
        assert re.search(
            _DDL_MODELS_DYNAMIC_TABLE_TAG,
            get_cleanded_model_ddl_from_file("dynamic_table_tag.sql"),
        )
        assert re.search(
            _DDL_MODELS_DYNAMIC_TABLE_TAG_AND_ROW_ACCESS_POLICY,
            get_cleanded_model_ddl_from_file("dynamic_table_tag_and_row_access_policy.sql"),
        )
