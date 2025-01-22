import pytest
from dbt.tests.util import run_dbt

_MODEL_SQL = """
{{ dispatch_to_parent() }}
select 1 as id
"""

_MACRO_SQL = """
{% macro do_something2(foo2, bar2) %}

    select
        '{{ foo2 }}' as foo2,
        '{{ bar2 }}' as bar2

{% endmacro %}

{% macro with_ref() %}

    {{ ref('table_model') }}

{% endmacro %}

{% macro dispatch_to_parent() %}
    {% set macro = adapter.dispatch('dispatch_to_parent') %}
    {{ macro() }}
{% endmacro %}

{% macro default__dispatch_to_parent() %}
    {% set msg = 'No default implementation of dispatch_to_parent' %}
    {{ exceptions.raise_compiler_error(msg) }}
{% endmacro %}

{% macro postgres__dispatch_to_parent() %}
    {{ return('') }}
{% endmacro %}
"""


class TestRedshift:
    @pytest.fixture(scope="class")
    def macros(self):
        return {"macro.sql": _MACRO_SQL}

    @pytest.fixture(scope="class")
    def models(self):
        return {"model.sql": _MODEL_SQL}

    def test_inherited_macro(self, project):
        run_dbt()
