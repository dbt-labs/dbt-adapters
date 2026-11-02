import re

import pytest

from dbt.tests.util import run_dbt


def get_cleaned_model_ddl_from_file(file_name: str) -> str:
    with open(f"target/run/test/models/{file_name}", "r") as ddl_file:
        return re.sub(r"\s+", " ", ddl_file.read())


_MODEL_TABLE_COPY_TAGS = """
{{ config(
    materialized = 'table',
    copy_tags = true,
) }}

select 1 as id
"""

_MODEL_TABLE_COPY_TAGS_AND_GRANTS = """
{{ config(
    materialized = 'table',
    copy_grants = true,
    copy_tags = true,
) }}

select 1 as id
"""

_MODEL_TABLE_NO_COPY_TAGS = """
{{ config(
    materialized = 'table',
) }}

select 1 as id
"""

_MODEL_INCREMENTAL_COPY_TAGS = """
{{ config(
    materialized = 'incremental',
    copy_tags = true,
) }}

select 1 as id
"""

_DDL_COPY_TAGS = "copy tags"
_DDL_COPY_GRANTS = "copy grants"


class TestCopyTags:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "table_copy_tags.sql": _MODEL_TABLE_COPY_TAGS,
            "table_copy_tags_and_grants.sql": _MODEL_TABLE_COPY_TAGS_AND_GRANTS,
            "table_no_copy_tags.sql": _MODEL_TABLE_NO_COPY_TAGS,
            "incremental_copy_tags.sql": _MODEL_INCREMENTAL_COPY_TAGS,
        }

    def test_copy_tags(self, project):
        results = run_dbt(["run"])
        assert len(results) == 4

        table_copy_tags_ddl = get_cleaned_model_ddl_from_file("table_copy_tags.sql")
        assert _DDL_COPY_TAGS in table_copy_tags_ddl
        assert _DDL_COPY_GRANTS not in table_copy_tags_ddl

        table_both_ddl = get_cleaned_model_ddl_from_file("table_copy_tags_and_grants.sql")
        assert _DDL_COPY_TAGS in table_both_ddl
        assert _DDL_COPY_GRANTS in table_both_ddl

        table_no_copy_tags_ddl = get_cleaned_model_ddl_from_file("table_no_copy_tags.sql")
        assert _DDL_COPY_TAGS not in table_no_copy_tags_ddl

        incremental_copy_tags_ddl = get_cleaned_model_ddl_from_file("incremental_copy_tags.sql")
        assert _DDL_COPY_TAGS in incremental_copy_tags_ddl

    def test_copy_tags_full_refresh(self, project):
        results = run_dbt(["run", "--select", "table_copy_tags", "--full-refresh"])
        assert len(results) == 1

        table_copy_tags_ddl = get_cleaned_model_ddl_from_file("table_copy_tags.sql")
        assert _DDL_COPY_TAGS in table_copy_tags_ddl
