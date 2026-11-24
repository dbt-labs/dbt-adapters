import pytest

from dbt.tests.adapter.caching.test_caching import (
    BaseCachingLowercaseModel,
    BaseCachingSelectedSchemaOnly,
    BaseCachingUppercaseModel,
    BaseNoPopulateCache,
)
from dbt.tests.util import run_dbt


class TestCachingLowerCaseModel(BaseCachingLowercaseModel):
    pass


class TestCachingUppercaseModel(BaseCachingUppercaseModel):
    pass


class TestCachingSelectedSchemaOnly(BaseCachingSelectedSchemaOnly):
    pass


class TestNoPopulateCache(BaseNoPopulateCache):
    pass


# Builds a table without looking up `this` first, so the schema has not been listed when dbt
# adds the returned relation to the cache.
_MATERIALIZATION_WITHOUT_LOOKUP = """
{% materialization table_without_lookup, default %}
  {%- set target_relation = this.incorporate(type='table') -%}
  {% do adapter.drop_relation(target_relation) %}
  {% call statement('main') -%}
    {{ get_create_table_as_sql(False, target_relation, sql) }}
  {%- endcall %}
  {% do adapter.commit() %}
  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
"""

_MODEL_WITHOUT_LOOKUP = """
{{ config(materialized='table_without_lookup') }}
select 1 as id
"""

_INCREMENTAL_MODEL = """
-- depends_on: {{ ref('model_without_lookup') }}
{{ config(materialized='incremental') }}
select 1 as id
"""


class TestNoPopulateCacheUnlistedSchema:
    """With --no-populate-cache, adding the relation that a materialization returns must not
    mark its schema as listed, or later lookups in that schema miss existing relations
    (dbt-labs/dbt-adapters#2310)."""

    @pytest.fixture(scope="class")
    def macros(self):
        return {"table_without_lookup.sql": _MATERIALIZATION_WITHOUT_LOOKUP}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_without_lookup.sql": _MODEL_WITHOUT_LOOKUP,
            "incremental_model.sql": _INCREMENTAL_MODEL,
        }

    def test_incremental_model_finds_existing_table(self, project):
        run_dbt(["run"])

        # model_without_lookup runs first and its relation is added to the cache. The incremental
        # model must still find its existing table and append to it, rather than take the
        # first-run path and try to create a table that already exists.
        results = run_dbt(["--no-populate-cache", "run"])
        assert len(results) == 2

        row_count = project.run_sql(
            "select count(*) from {schema}.incremental_model", fetch="one"
        )[0]
        assert row_count == 2
