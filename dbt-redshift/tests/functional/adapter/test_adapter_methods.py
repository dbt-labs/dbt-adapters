import pytest

from dbt.tests.util import run_dbt, check_relations_equal
from dbt.tests.fixtures.project import write_project_files


tests__get_relation_invalid = """
{% set upstream = ref('upstream') %}
{% set relations = adapter.get_relation(database=upstream.database, schema=upstream.schema, identifier="doesnotexist") %}
{% set limit_query = 0 %}
{% if relations.identifier %}
    {% set limit_query = 1 %}
{% endif %}

select 1 as id limit {{ limit_query }}

"""

models__upstream_sql = """
select 1 as id

"""

models__expected_sql = """
select 1 as valid_relation

"""

models__model_sql = """

{% set upstream = ref('upstream') %}

select * from {{ upstream }}

"""

models__call_get_relation = """

{% set model = ref('model') %}

{% set relation = adapter.get_relation(database=model.database, schema=model.schema, identifier=model.identifier) %}
{% if relation.identifier == model.identifier %}

select 1 as valid_relation

{% else %}

select 0 as valid_relation

{% endif %}

"""

models__get_relation_type = """

{% set base_view = ref('base_view') %}

{% set relation = adapter.get_relation(database=base_view.database, schema=base_view.schema, identifier=base_view.identifier) %}
{% if relation.type == 'view' %}

select 1 as valid_type

{% else %}

select 0 as valid_type

{% endif %}

"""


class RedshiftAdapterMethod:
    @pytest.fixture(scope="class")
    def tests(self):
        return {"get_relation_invalid.sql": tests__get_relation_invalid}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "upstream.sql": models__upstream_sql,
            "expected.sql": models__expected_sql,
            "model.sql": models__model_sql,
            "call_get_relation.sql": models__call_get_relation,
            "base_view.sql": "{{ config(bind=True) }} select * from {{ ref('model') }}",
            "get_relation_type.sql": models__get_relation_type,
            "expected_type.sql": "select 1 as valid_type",
        }

    def project_files(
        self,
        project_root,
        tests,
        models,
    ):
        write_project_files(project_root, "tests", tests)
        write_project_files(project_root, "models", models)

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "adapter_methods",
        }

    def test_adapter_methods(self, project):
        run_dbt(["compile"])  # trigger any compile-time issues
        result = run_dbt()
        assert len(result) == 7

        run_dbt(["test"])
        check_relations_equal(project.adapter, ["call_get_relation", "expected"])
        check_relations_equal(project.adapter, ["get_relation_type", "expected_type"])


class TestRedshiftAdapterMethod(RedshiftAdapterMethod):
    pass
